from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from copy import deepcopy
from decimal import Decimal
from typing import Any, Protocol

import structlog
from sqlalchemy import text

from crypto_momentum_lab.next_gen.models import Order, OrderStatus, PositionBatch

log = structlog.get_logger("next_gen_persistence")


def compute_advisory_lock_key(environment: str, account_label: str) -> str:
    """生成 PostgreSQL 会话级咨询锁键 (如 live:a1)"""
    return f"{environment}:{account_label}"


class TradingRepositoryProtocol(Protocol):
    """下一代极简单仓储协议"""

    @asynccontextmanager
    def acquire_dedicated_advisory_lock(self, lock_key: str) -> AsyncIterator[None]: ...

    async def async_save_order(self, order: Order) -> None: ...
    async def async_save_batch(self, batch: PositionBatch) -> None: ...
    async def async_save_order_and_batch(
        self, order: Order, batch: PositionBatch | None
    ) -> None: ...
    async def async_get_batch(
        self, environment: str, account_label: str, batch_id: str
    ) -> PositionBatch | None: ...
    async def async_get_active_batches(
        self, environment: str, account_label: str
    ) -> list[PositionBatch]: ...
    async def async_get_order(
        self, environment: str, account_label: str, client_order_id: str
    ) -> Order | None: ...
    async def async_get_recent_orders(
        self, environment: str, account_label: str, limit: int = 50
    ) -> list[Order]: ...


class InMemoryTradingRepository:
    """测试与内存模拟专用的仓储实现，支持模拟数据库瞬态异常与重试校验"""

    def __init__(self) -> None:
        self.orders: dict[tuple[str, str, str], Order] = {}
        self.batches: dict[tuple[str, str, str], PositionBatch] = {}
        self._fail_writes_remaining: int = 0
        self._held_locks: set[str] = set()

    @asynccontextmanager
    async def acquire_dedicated_advisory_lock(
        self, lock_key: str
    ) -> AsyncIterator[None]:
        if lock_key in self._held_locks:
            raise RuntimeError(f"获取会话咨询锁失败: [{lock_key}] 已被另一进程持有！")
        self._held_locks.add(lock_key)
        try:
            yield
        finally:
            self._held_locks.discard(lock_key)

    def fail_next_n_writes(self, count: int) -> None:
        """设置接下来 N 次写入抛出模拟数据库异常"""
        self._fail_writes_remaining = count

    def _check_and_decrement_failure(self) -> None:
        if self._fail_writes_remaining > 0:
            self._fail_writes_remaining -= 1
            raise ConnectionError("Simulated database connection timeout/error")

    async def async_save_order(self, order: Order) -> None:
        self._check_and_decrement_failure()
        key = (order.environment, order.account_label, order.client_order_id)
        self.orders[key] = deepcopy(order)

    async def async_save_batch(self, batch: PositionBatch) -> None:
        self._check_and_decrement_failure()
        key = (batch.environment, batch.account_label, batch.batch_id)
        self.batches[key] = deepcopy(batch)

    async def async_save_order_and_batch(
        self, order: Order, batch: PositionBatch | None
    ) -> None:
        self._check_and_decrement_failure()
        order_key = (order.environment, order.account_label, order.client_order_id)
        self.orders[order_key] = deepcopy(order)
        if batch is not None:
            batch_key = (batch.environment, batch.account_label, batch.batch_id)
            self.batches[batch_key] = deepcopy(batch)

    def get_order(
        self, environment: str, account_label: str, client_order_id: str
    ) -> Order | None:
        return self.orders.get((environment, account_label, client_order_id))

    def get_batch(
        self, environment: str, account_label: str, batch_id: str
    ) -> PositionBatch | None:
        return self.batches.get((environment, account_label, batch_id))

    async def async_get_order(
        self, environment: str, account_label: str, client_order_id: str
    ) -> Order | None:
        return self.get_order(environment, account_label, client_order_id)

    async def async_get_batch(
        self, environment: str, account_label: str, batch_id: str
    ) -> PositionBatch | None:
        return self.get_batch(environment, account_label, batch_id)

    async def async_get_active_batches(
        self, environment: str, account_label: str
    ) -> list[PositionBatch]:
        return [
            b
            for (env, acc, _), b in self.batches.items()
            if env == environment
            and acc == account_label
            and b.status in ("ACTIVE", "EXITING")
        ]

    async def async_get_recent_orders(
        self, environment: str, account_label: str, limit: int = 50
    ) -> list[Order]:
        orders = [
            o
            for (env, acc, _), o in self.orders.items()
            if env == environment and acc == account_label
        ]
        orders.sort(key=lambda o: o.created_at, reverse=True)
        return orders[:limit]


class PostgresTradingRepository:
    """基于单一轻量 AsyncEngine 的直写仓储实现
    (支持 pg_try_advisory_lock 排他锁与 2 表直写)。
    """

    def __init__(self, async_engine: Any) -> None:
        self._engine = async_engine

    @asynccontextmanager
    async def acquire_dedicated_advisory_lock(
        self, lock_key: str
    ) -> AsyncIterator[None]:
        """专用物理连接获取会话级 PostgreSQL Advisory Lock (防多开互斥)"""
        async with self._engine.connect() as conn:
            # 执行 SELECT pg_try_advisory_lock(hashtext(:lock_key))
            res = await conn.execute(
                text("SELECT pg_try_advisory_lock(hashtext(:key)) AS acquired"),
                {"key": lock_key},
            )
            row = res.mappings().first()
            acquired = row["acquired"] if row else False
            if not acquired:
                raise RuntimeError(
                    f"获取 PostgreSQL 会话咨询锁失败: [{lock_key}] "
                    "已被另一进程持有！拒绝启动以防多实例竞态！"
                )
            log.info("advisory_lock_acquired", key=lock_key)
            try:
                yield
            finally:
                try:
                    await conn.execute(
                        text("SELECT pg_advisory_unlock(hashtext(:key))"),
                        {"key": lock_key},
                    )
                    log.info("advisory_lock_released", key=lock_key)
                except Exception as e:
                    log.warning("advisory_unlock_error", key=lock_key, err=str(e))

    async def async_save_order(self, order: Order) -> None:
        """初始发单落库 (INSERT ... ON CONFLICT DO UPDATE)"""
        sql = """
        INSERT INTO live_orders (
            environment, account_label, client_order_id, exchange_order_id,
            batch_id, symbol, side, position_side, order_type,
            quantity, price, status, filled_quantity, average_price,
            created_at, updated_at
        ) VALUES (
            :environment, :account_label, :client_order_id, :exchange_order_id,
            :batch_id, :symbol, :side, :position_side, :order_type,
            :quantity, :price, :status, :filled_quantity, :average_price,
            :created_at, :updated_at
        )
        ON CONFLICT (environment, account_label, client_order_id)
        DO UPDATE SET
            exchange_order_id = COALESCE(
                EXCLUDED.exchange_order_id, live_orders.exchange_order_id
            ),
            status = EXCLUDED.status,
            filled_quantity = EXCLUDED.filled_quantity,
            average_price = EXCLUDED.average_price,
            updated_at = EXCLUDED.updated_at;
        """
        params = {
            "environment": order.environment,
            "account_label": order.account_label,
            "client_order_id": order.client_order_id,
            "exchange_order_id": order.exchange_order_id,
            "batch_id": order.batch_id,
            "symbol": order.symbol,
            "side": order.side,
            "position_side": order.position_side,
            "order_type": order.order_type,
            "quantity": order.quantity,
            "price": order.price,
            "status": order.status.name,
            "filled_quantity": order.filled_qty,
            "average_price": order.avg_price,
            "created_at": order.created_at,
            "updated_at": order.updated_at,
        }
        async with self._engine.begin() as conn:
            await conn.execute(text(sql), params)

    async def async_save_batch(self, batch: PositionBatch) -> None:
        """批次更新落库 (INSERT ... ON CONFLICT DO UPDATE)"""
        sql = """
        INSERT INTO live_position_batches (
            environment, account_label, batch_id, symbol,
            position_side, quantity, entry_price, entry_bar_time,
            exit_order_submitted_at, status, realized_pnl,
            exiting_quantity, updated_at
        ) VALUES (
            :environment, :account_label, :batch_id, :symbol,
            :position_side, :quantity, :entry_price, :entry_bar_time,
            :exit_order_submitted_at, :status, :realized_pnl,
            :exiting_quantity, :updated_at
        )
        ON CONFLICT (environment, account_label, batch_id)
        DO UPDATE SET
            quantity = EXCLUDED.quantity,
            entry_price = EXCLUDED.entry_price,
            exit_order_submitted_at = COALESCE(
                EXCLUDED.exit_order_submitted_at,
                live_position_batches.exit_order_submitted_at
            ),
            status = EXCLUDED.status,
            realized_pnl = EXCLUDED.realized_pnl,
            exiting_quantity = EXCLUDED.exiting_quantity,
            updated_at = EXCLUDED.updated_at;
        """
        params = {
            "environment": batch.environment,
            "account_label": batch.account_label,
            "batch_id": batch.batch_id,
            "symbol": batch.symbol,
            "position_side": batch.position_side,
            "quantity": batch.quantity,
            "entry_price": batch.entry_price,
            "entry_bar_time": batch.entry_bar_time,
            "exit_order_submitted_at": batch.exit_order_submitted_at,
            "status": batch.status,
            "realized_pnl": batch.realized_pnl,
            "exiting_quantity": batch.exiting_quantity,
            "updated_at": batch.updated_at,
        }
        async with self._engine.begin() as conn:
            await conn.execute(text(sql), params)

    async def async_save_order_and_batch(
        self, order: Order, batch: PositionBatch | None
    ) -> None:
        """原子单事务双表直写"""
        async with self._engine.begin() as conn:
            # 1. 保存订单
            await conn.execute(
                text(
                    """
                    INSERT INTO live_orders (
                        environment, account_label, client_order_id, exchange_order_id,
                        batch_id, symbol, side, position_side, order_type,
                        quantity, price, status, filled_quantity, average_price,
                        created_at, updated_at
                    ) VALUES (
                        :environment, :account_label,
                        :client_order_id, :exchange_order_id,
                        :batch_id, :symbol, :side, :position_side, :order_type,
                        :quantity, :price, :status, :filled_quantity, :average_price,
                        :created_at, :updated_at
                    )
                    ON CONFLICT (environment, account_label, client_order_id)
                    DO UPDATE SET
                        exchange_order_id = COALESCE(
                            EXCLUDED.exchange_order_id, live_orders.exchange_order_id
                        ),
                        status = EXCLUDED.status,
                        filled_quantity = EXCLUDED.filled_quantity,
                        average_price = EXCLUDED.average_price,
                        updated_at = EXCLUDED.updated_at;
                    """
                ),
                {
                    "environment": order.environment,
                    "account_label": order.account_label,
                    "client_order_id": order.client_order_id,
                    "exchange_order_id": order.exchange_order_id,
                    "batch_id": order.batch_id,
                    "symbol": order.symbol,
                    "side": order.side,
                    "position_side": order.position_side,
                    "order_type": order.order_type,
                    "quantity": order.quantity,
                    "price": order.price,
                    "status": order.status.name,
                    "filled_quantity": order.filled_qty,
                    "average_price": order.avg_price,
                    "created_at": order.created_at,
                    "updated_at": order.updated_at,
                },
            )
            # 2. 保存批次 (若存在)
            if batch is not None:
                await conn.execute(
                    text(
                        """
                        INSERT INTO live_position_batches (
                            environment, account_label, batch_id, symbol,
                            position_side, quantity, entry_price, entry_bar_time,
                            exit_order_submitted_at, status, realized_pnl,
                            exiting_quantity, updated_at
                        ) VALUES (
                            :environment, :account_label, :batch_id, :symbol,
                            :position_side, :quantity, :entry_price, :entry_bar_time,
                            :exit_order_submitted_at, :status, :realized_pnl,
                            :exiting_quantity, :updated_at
                        )
                        ON CONFLICT (environment, account_label, batch_id)
                        DO UPDATE SET
                            quantity = EXCLUDED.quantity,
                            entry_price = EXCLUDED.entry_price,
                            exit_order_submitted_at = COALESCE(
                                EXCLUDED.exit_order_submitted_at,
                                live_position_batches.exit_order_submitted_at
                            ),
                            status = EXCLUDED.status,
                            realized_pnl = EXCLUDED.realized_pnl,
                            exiting_quantity = EXCLUDED.exiting_quantity,
                            updated_at = EXCLUDED.updated_at;
                        """
                    ),
                    {
                        "environment": batch.environment,
                        "account_label": batch.account_label,
                        "batch_id": batch.batch_id,
                        "symbol": batch.symbol,
                        "position_side": batch.position_side,
                        "quantity": batch.quantity,
                        "entry_price": batch.entry_price,
                        "entry_bar_time": batch.entry_bar_time,
                        "exit_order_submitted_at": batch.exit_order_submitted_at,
                        "status": batch.status,
                        "realized_pnl": batch.realized_pnl,
                        "exiting_quantity": batch.exiting_quantity,
                        "updated_at": batch.updated_at,
                    },
                )

    async def async_get_batch(
        self, environment: str, account_label: str, batch_id: str
    ) -> PositionBatch | None:
        sql = """
        SELECT environment, account_label, batch_id, symbol, position_side,
               quantity, entry_price, entry_bar_time, exit_order_submitted_at,
               status, realized_pnl, exiting_quantity, updated_at
        FROM live_position_batches
        WHERE environment = :env AND account_label = :acc AND batch_id = :bid;
        """
        async with self._engine.connect() as conn:
            res = await conn.execute(
                text(sql),
                {"env": environment, "acc": account_label, "bid": batch_id},
            )
            row = res.mappings().first()
            if not row:
                return None
            return PositionBatch(
                environment=row["environment"],
                account_label=row["account_label"],
                batch_id=row["batch_id"],
                symbol=row["symbol"],
                position_side=row["position_side"],
                quantity=Decimal(str(row["quantity"])),
                entry_price=Decimal(str(row["entry_price"])),
                entry_bar_time=row["entry_bar_time"],
                exit_order_submitted_at=row["exit_order_submitted_at"],
                status=row["status"],
                realized_pnl=Decimal(str(row["realized_pnl"])),
                exiting_quantity=Decimal(str(row["exiting_quantity"])),
                updated_at=row["updated_at"],
            )

    async def async_get_active_batches(
        self, environment: str, account_label: str
    ) -> list[PositionBatch]:
        sql = """
        SELECT environment, account_label, batch_id, symbol, position_side,
               quantity, entry_price, entry_bar_time, exit_order_submitted_at,
               status, realized_pnl, exiting_quantity, updated_at
        FROM live_position_batches
        WHERE environment = :env AND account_label = :acc
          AND status IN ('ACTIVE', 'EXITING')
        ORDER BY entry_bar_time ASC;
        """
        async with self._engine.connect() as conn:
            res = await conn.execute(
                text(sql), {"env": environment, "acc": account_label}
            )
            rows = res.mappings().all()
            return [
                PositionBatch(
                    environment=r["environment"],
                    account_label=r["account_label"],
                    batch_id=r["batch_id"],
                    symbol=r["symbol"],
                    position_side=r["position_side"],
                    quantity=Decimal(str(r["quantity"])),
                    entry_price=Decimal(str(r["entry_price"])),
                    entry_bar_time=r["entry_bar_time"],
                    exit_order_submitted_at=r["exit_order_submitted_at"],
                    status=r["status"],
                    realized_pnl=Decimal(str(r["realized_pnl"])),
                    exiting_quantity=Decimal(str(r["exiting_quantity"])),
                    updated_at=r["updated_at"],
                )
                for r in rows
            ]

    async def async_get_order(
        self, environment: str, account_label: str, client_order_id: str
    ) -> Order | None:
        sql = """
        SELECT environment, account_label, client_order_id, exchange_order_id,
               batch_id, symbol, side, position_side, order_type, quantity, price,
               status, filled_quantity, average_price, created_at, updated_at
        FROM live_orders
        WHERE environment = :env AND account_label = :acc AND client_order_id = :cid;
        """
        async with self._engine.connect() as conn:
            res = await conn.execute(
                text(sql),
                {"env": environment, "acc": account_label, "cid": client_order_id},
            )
            row = res.mappings().first()
            if not row:
                return None
            return Order(
                environment=row["environment"],
                account_label=row["account_label"],
                client_order_id=row["client_order_id"],
                batch_id=row["batch_id"],
                symbol=row["symbol"],
                side=row["side"],
                position_side=row["position_side"],
                order_type=row["order_type"],
                quantity=Decimal(str(row["quantity"])),
                price=Decimal(str(row["price"])) if row["price"] is not None else None,
                status=OrderStatus[row["status"]],
                exchange_order_id=row["exchange_order_id"],
                filled_qty=Decimal(str(row["filled_quantity"])),
                avg_price=Decimal(str(row["average_price"])),
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )

    async def async_get_recent_orders(
        self, environment: str, account_label: str, limit: int = 50
    ) -> list[Order]:
        sql = """
        SELECT environment, account_label, client_order_id, exchange_order_id,
               batch_id, symbol, side, position_side, order_type, quantity, price,
               status, filled_quantity, average_price, created_at, updated_at
        FROM live_orders
        WHERE environment = :env AND account_label = :acc
        ORDER BY created_at DESC
        LIMIT :limit;
        """
        async with self._engine.connect() as conn:
            res = await conn.execute(
                text(sql), {"env": environment, "acc": account_label, "limit": limit}
            )
            rows = res.mappings().all()
            return [
                Order(
                    environment=r["environment"],
                    account_label=r["account_label"],
                    client_order_id=r["client_order_id"],
                    batch_id=r["batch_id"],
                    symbol=r["symbol"],
                    side=r["side"],
                    position_side=r["position_side"],
                    order_type=r["order_type"],
                    quantity=Decimal(str(r["quantity"])),
                    price=Decimal(str(r["price"])) if r["price"] is not None else None,
                    status=OrderStatus[r["status"]],
                    exchange_order_id=r["exchange_order_id"],
                    filled_qty=Decimal(str(r["filled_quantity"])),
                    avg_price=Decimal(str(r["average_price"])),
                    created_at=r["created_at"],
                    updated_at=r["updated_at"],
                )
                for r in rows
            ]
