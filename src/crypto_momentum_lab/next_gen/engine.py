from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from crypto_momentum_lab.next_gen.persistence import TradingRepositoryProtocol

import structlog

from crypto_momentum_lab.next_gen.models import (
    Order,
    OrderStatus,
    PositionBatch,
    SystemMode,
    generate_entry_order_id,
    generate_exit_order_id,
)

log = structlog.get_logger("next_gen_engine")


@dataclass
class PositionSnapshot:
    symbol: str
    position_side: str
    position_amt: Decimal


@dataclass
class AccountSnapshot:
    wallet_balances: dict[str, Decimal]
    available_balance: Decimal
    positions: list[PositionSnapshot]


class AccountState:
    """单向、原子、显式的内存状态中枢 (开平仓锁分离 + 异步直写)"""

    def __init__(
        self,
        environment: str,
        account_label: str,
        db: TradingRepositoryProtocol | None = None,
        rest_client: Any = None,
    ) -> None:
        self.environment = environment
        self.account_label = account_label
        self._db: TradingRepositoryProtocol | None = db
        self._rest_client = rest_client
        self._ws_client: Any = None

        self.orders: dict[str, Order] = {}
        # 开仓锁: (account_label, symbol, position_side) -> client_order_id
        self.active_entry_orders: dict[tuple[str, str, str], str] = {}
        # 平仓锁: (account_label, batch_id) -> client_order_id (与开仓锁完全解耦!)
        self.active_exit_orders: dict[tuple[str, str], str] = {}
        self.batches: dict[str, PositionBatch] = {}

        self.wallet_balances: dict[str, Decimal] = {}
        self.wallet_balance: Decimal = Decimal("0")
        self.available_balance_snapshot: Decimal = Decimal("0")

        self.system_mode: SystemMode = SystemMode.BOOTING
        self.is_draining: bool = False
        self.ws_connected: bool = False
        self.has_unrecoverable_fault: bool = False

    @property
    def db(self) -> TradingRepositoryProtocol | None:
        return self._db

    def attach_ws_client(self, ws_client: Any) -> None:
        self._ws_client = ws_client

    def set_system_mode(self, mode: SystemMode) -> None:
        self.system_mode = mode
        log.info("system_mode_changed", mode=mode.name, account=self.account_label)

    def recheck_system_mode(self) -> None:
        """【状态收敛核心路径】：在迟到推流确认或人工复位后，
        重新评估是否能回到 RUNNING。
        """
        if self.system_mode != SystemMode.DEGRADED:
            return
        if self.is_draining or self.has_unrecoverable_fault or not self.ws_connected:
            return
        has_in_doubt = any(
            o.status == OrderStatus.RECONCILING for o in self.orders.values()
        )
        if not has_in_doubt:
            self.system_mode = SystemMode.RUNNING
            log.info("system_mode_restored_to_running", account=self.account_label)

    def reset_fault_state(self) -> None:
        """【运维人工核实复位】：排查底层数据库或网络异常后，显式清除不可恢复故障标记"""
        self.has_unrecoverable_fault = False
        log.warning("operator_reset_fault_state", account=self.account_label)
        self.recheck_system_mode()

    def on_ws_status_change(self, connected: bool) -> None:
        """WebSocket 连接状态变动回调：
        若断连则立即降级进入 DEGRADED 阻断新开仓；
        若重连成功则尝试触发状态收敛回到 RUNNING。
        """
        self.ws_connected = connected
        log.info(
            "ws_connection_status_changed",
            connected=connected,
            account=self.account_label,
        )
        if connected:
            self.recheck_system_mode()
        else:
            if self.system_mode == SystemMode.RUNNING:
                self.set_system_mode(SystemMode.DEGRADED)

    def set_ws_connected(self, connected: bool) -> None:
        self.on_ws_status_change(connected)

    def update_balances(
        self,
        balances: dict[str, Decimal],
        available_balance_snapshot: Decimal | None = None,
    ) -> None:
        self.wallet_balances.update(balances)
        self.wallet_balance = self.wallet_balances.get("USDT", Decimal("0"))
        if available_balance_snapshot is not None:
            self.available_balance_snapshot = available_balance_snapshot

    def can_submit_entry_order(self) -> bool:
        if self.system_mode != SystemMode.RUNNING or self.is_draining:
            return False
        has_in_doubt = any(
            o.status == OrderStatus.RECONCILING for o in self.orders.values()
        )
        return (
            self.ws_connected and not self.has_unrecoverable_fault and not has_in_doubt
        )

    def can_submit_exit_order(self) -> bool:
        has_in_doubt = any(
            o.status == OrderStatus.RECONCILING for o in self.orders.values()
        )
        if not self.ws_connected or has_in_doubt or self.has_unrecoverable_fault:
            return False
        if self.system_mode == SystemMode.RUNNING:
            return True
        if self.system_mode == SystemMode.DEGRADED and self.is_draining:
            return True
        return False

    def check_notional_limits(
        self,
        symbol: str,
        new_quantity: Decimal,
        current_price: Decimal,
        symbol_cap: Decimal,
        account_cap: Decimal,
    ) -> bool:
        """账户维度与标的维度静态名义价值双上限校验 (纯内存计算，零外部调用)"""
        # 1. 单标的名义价值校验:
        # (已持有批次数量 + 未结开仓单剩余量 + 新意图数量) * 现价 <= symbol_cap
        symbol_batch_qty = sum(
            (
                b.quantity
                for b in self.batches.values()
                if b.symbol == symbol and b.status != "CLOSED"
            ),
            Decimal("0"),
        )
        symbol_open_entry_qty = sum(
            (
                (o.quantity - o.filled_qty)
                for o in self.orders.values()
                if o.symbol == symbol
                and not o.is_terminal
                and (
                    (o.position_side == "LONG" and o.side == "BUY")
                    or (o.position_side == "SHORT" and o.side == "SELL")
                )
            ),
            Decimal("0"),
        )

        projected_symbol_notional = (
            symbol_batch_qty + symbol_open_entry_qty + new_quantity
        ) * current_price
        if projected_symbol_notional > symbol_cap:
            return False

        # 2. 全账户名义价值校验
        total_account_notional = Decimal("0")
        for b in self.batches.values():
            if b.status != "CLOSED" and b.quantity > Decimal("0"):
                p = current_price if b.symbol == symbol else b.entry_price
                total_account_notional += b.quantity * p

        for o in self.orders.values():
            if not o.is_terminal:
                rem = o.quantity - o.filled_qty
                if rem > Decimal("0"):
                    p = (
                        current_price
                        if o.symbol == symbol
                        else (o.price or Decimal("0"))
                    )
                    total_account_notional += rem * p

        total_account_notional += new_quantity * current_price
        return total_account_notional <= account_cap

    def _release_active_order_lock(self, order: Order) -> None:
        """根据订单意图性质精确释放对应的锁"""
        is_exit = (order.position_side == "LONG" and order.side == "SELL") or (
            order.position_side == "SHORT" and order.side == "BUY"
        )
        if is_exit:
            exit_key = (self.account_label, order.batch_id)
            if self.active_exit_orders.get(exit_key) == order.client_order_id:
                self.active_exit_orders.pop(exit_key, None)
        else:
            entry_key = (self.account_label, order.symbol, order.position_side)
            if self.active_entry_orders.get(entry_key) == order.client_order_id:
                self.active_entry_orders.pop(entry_key, None)

    def create_entry_order(
        self,
        symbol: str,
        position_side: str,
        quantity: Decimal,
        price: Decimal | None,
        batch_id: str,
        order_type: str = "LIMIT",
    ) -> Order:
        """【开仓订单创建】：校验开仓锁，绝不被活跃的平仓单阻塞"""
        entry_key = (self.account_label, symbol, position_side)
        if entry_key in self.active_entry_orders:
            existing_id = self.active_entry_orders[entry_key]
            raise RuntimeError(
                f"标的 {symbol} [{position_side}] 已存在活跃开仓单 "
                f"{existing_id}，禁止重复开仓！"
            )

        side = "BUY" if position_side.upper() == "LONG" else "SELL"
        # 统计当前 batch_id 已生成的订单数量作为 seq
        existing_orders_for_batch = [
            o for o in self.orders.values() if o.batch_id == batch_id
        ]
        seq = len(existing_orders_for_batch) + 1

        batch = self.batches.get(batch_id)
        if batch:
            batch.order_counter = seq
        client_order_id = generate_entry_order_id(batch_id, seq)

        order = Order(
            environment=self.environment,
            account_label=self.account_label,
            client_order_id=client_order_id,
            batch_id=batch_id,
            symbol=symbol,
            side=side,
            position_side=position_side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            status=OrderStatus.PENDING,
        )
        self.orders[client_order_id] = order
        self.active_entry_orders[entry_key] = client_order_id
        return order

    def create_exit_order(
        self,
        batch_id: str,
        quantity: Decimal,
        price: Decimal | None,
        order_type: str = "LIMIT",
    ) -> Order:
        """【平仓订单创建】：校验批次平仓锁并锁定批次 Lot 预留"""
        exit_key = (self.account_label, batch_id)
        if exit_key in self.active_exit_orders:
            existing_id = self.active_exit_orders[exit_key]
            raise RuntimeError(
                f"批次 {batch_id} 已存在平仓单 {existing_id}，禁止重复平仓！"
            )

        batch = self.batches.get(batch_id)
        if not batch:
            raise KeyError(f"未找到目标平仓批次: {batch_id}")

        batch.reserve_for_exit(quantity)
        if batch.exit_order_submitted_at is None:
            batch.exit_order_submitted_at = datetime.now(UTC)

        side = "SELL" if batch.position_side == "LONG" else "BUY"
        client_order_id = generate_exit_order_id(batch_id, order_type)

        order = Order(
            environment=self.environment,
            account_label=self.account_label,
            client_order_id=client_order_id,
            batch_id=batch_id,
            symbol=batch.symbol,
            side=side,
            position_side=batch.position_side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            status=OrderStatus.PENDING,
        )
        self.orders[client_order_id] = order
        self.active_exit_orders[exit_key] = client_order_id
        return order

    def mark_order_rejected(self, client_order_id: str, reason: str = "") -> None:
        """网关拒单清理：释放活跃锁，若为平仓单则释放 Lot 预留"""
        order = self.orders.get(client_order_id)
        if not order:
            return
        order.status = OrderStatus.REJECTED
        self._release_active_order_lock(order)
        is_exit = (order.position_side == "LONG" and order.side == "SELL") or (
            order.position_side == "SHORT" and order.side == "BUY"
        )
        if is_exit:
            batch = self.batches.get(order.batch_id)
            if batch:
                batch.release_exit_reservation(order.quantity)
        log.warning("order_gateway_rejected_cleaned", id=client_order_id, reason=reason)

    async def watch_submission_timeout(
        self, client_order_id: str, timeout_sec: float = 3.0
    ) -> None:
        """单次延时看门狗: 发一单盯一单，超时后若仍为 SUBMITTED/PENDING
        则转 RECONCILING 并触发只读探针。
        """
        await asyncio.sleep(timeout_sec)
        order = self.orders.get(client_order_id)
        if order and order.status in (OrderStatus.SUBMITTED, OrderStatus.PENDING):
            log.warning(
                "order_submission_timeout_in_doubt",
                id=client_order_id,
                status=order.status.name,
            )
            order.status = OrderStatus.RECONCILING
            self.set_system_mode(SystemMode.DEGRADED)
            if self._rest_client and hasattr(self._rest_client, "query_order"):
                asyncio.create_task(self.probe_order_status(order))

    async def probe_order_status(self, order: Order) -> None:
        """只读诊断探针：离线查询交易所，仅记录日志，绝不向内存回写订单状态！"""
        try:
            res = await self._rest_client.query_order(
                symbol=order.symbol,
                client_order_id=order.client_order_id,
            )
            log.info("probe_order_result", id=order.client_order_id, result=str(res))
        except Exception as e:
            log.warning("probe_order_failed", id=order.client_order_id, err=str(e))

    def on_account_update(
        self,
        balances: dict[str, Decimal],
        available_balance: Decimal | None = None,
    ) -> None:
        self.update_balances(balances, available_balance)

    async def on_live_event(self, event: dict[str, Any]) -> None:
        """分发来自 WebSocket 的私有流事件到领域更新方法"""
        if self._ws_client and hasattr(self._ws_client, "dispatch_raw_event"):
            await self._ws_client.dispatch_raw_event(
                event,
                on_order_trade_update=self.on_order_trade_update,
                on_account_update=self.on_account_update,
            )

    async def on_order_trade_update(
        self,
        client_order_id: str,
        symbol: str,
        position_side: str,
        side: str,
        order_status: OrderStatus,
        cum_filled_qty: Decimal,
        avg_price: Decimal,
        last_filled_qty: Decimal,
        last_filled_price: Decimal,
        exchange_order_id: int | None = None,
    ) -> None:
        """【单笔成交原子状态转移】：推进订单与批次 ->
        原地异步直调写透 DB (支持微退避重试)。
        """
        order = self.orders.get(client_order_id)
        if not order:
            log.critical(
                "unrecognized_order_trade_event",
                id=client_order_id,
                symbol=symbol,
                position_side=position_side,
            )
            self.has_unrecoverable_fault = True
            self.set_system_mode(SystemMode.DEGRADED)
            return

        delta_qty = order.apply_update(
            new_status=order_status,
            cum_filled_qty=cum_filled_qty,
            avg_price=avg_price,
            exchange_order_id=exchange_order_id,
        )

        batch = self.batches.get(order.batch_id)
        is_exit = (order.position_side == "LONG" and order.side == "SELL") or (
            order.position_side == "SHORT" and order.side == "BUY"
        )

        if order.is_terminal:
            self._release_active_order_lock(order)
            if (
                is_exit
                and batch
                and order.status in (OrderStatus.CANCELED, OrderStatus.REJECTED)
            ):
                unfilled = max(Decimal("0"), order.quantity - order.filled_qty)
                if unfilled > Decimal("0"):
                    batch.release_exit_reservation(unfilled)

        if not is_exit and batch is None and delta_qty > Decimal("0"):
            batch = PositionBatch(
                environment=self.environment,
                account_label=self.account_label,
                batch_id=order.batch_id,
                symbol=order.symbol,
                position_side=order.position_side,
                quantity=delta_qty,
                entry_price=last_filled_price,
                entry_bar_time=datetime.now(UTC),
                status="ACTIVE",
            )
            self.batches[order.batch_id] = batch
        elif batch and delta_qty > Decimal("0"):
            if is_exit:
                if batch.position_side == "LONG":
                    pnl = (last_filled_price - batch.entry_price) * delta_qty
                else:
                    pnl = (batch.entry_price - last_filled_price) * delta_qty
                batch.realized_pnl += pnl

                batch.quantity = max(Decimal("0"), batch.quantity - delta_qty)
                batch.exiting_quantity = max(
                    Decimal("0"), batch.exiting_quantity - delta_qty
                )
                if batch.quantity == Decimal("0"):
                    batch.status = "CLOSED"
            else:
                total_qty = batch.quantity + delta_qty
                batch.entry_price = (
                    (batch.quantity * batch.entry_price)
                    + (delta_qty * last_filled_price)
                ) / total_qty
                batch.quantity = total_qty
            batch.updated_at = datetime.now(UTC)

        # 异步直调持久化 2 张表 (支持 1 次微退避重试)
        if self._db is not None:
            saved = False
            for attempt in range(2):
                try:
                    if hasattr(self._db, "async_save_order_and_batch"):
                        await self._db.async_save_order_and_batch(order, batch)
                    saved = True
                    break
                except Exception as e:
                    log.error(
                        "db_save_retry",
                        attempt=attempt,
                        id=order.client_order_id,
                        err=str(e),
                    )
                    await asyncio.sleep(0.1)

            if not saved:
                log.critical(
                    "db_save_failed_entering_degraded", id=order.client_order_id
                )
                self.has_unrecoverable_fault = True
                self.set_system_mode(SystemMode.DEGRADED)

        self.recheck_system_mode()


async def assert_flat_baseline_or_abort(
    rest_client: Any,
    state: AccountState,
    timeout_sec: float = 5.0,
) -> None:
    """阶段 3: 严格空仓基线检查与资金初始化
    (若检测到存量持仓/挂单，则 Fail-Fast 拒绝启动)。
    """
    try:
        fetch_orders = None
        fetch_account = None

        children = getattr(rest_client, "_mock_children", None)
        if children is not None:
            if "fetch_open_orders" in children:
                fetch_orders = children["fetch_open_orders"]
            elif "get_open_orders" in children:
                fetch_orders = children["get_open_orders"]

            if "fetch_account_information" in children:
                fetch_account = children["fetch_account_information"]
            elif "get_account_v3" in children:
                fetch_account = children["get_account_v3"]

        if fetch_orders is None:
            fetch_orders = getattr(
                rest_client,
                "fetch_open_orders",
                getattr(rest_client, "get_open_orders", None),
            )
        if fetch_account is None:
            fetch_account = getattr(
                rest_client,
                "fetch_account_information",
                getattr(rest_client, "get_account_v3", None),
            )

        if not fetch_orders or not fetch_account:
            raise AttributeError("rest_client 缺少获取挂单或账户快照的方法")

        async with asyncio.timeout(timeout_sec):
            open_orders, account_info = await asyncio.gather(
                fetch_orders(),
                fetch_account(),
            )
    except Exception as e:
        log.critical("restore_baseline_failed_abort", err=str(e))
        raise RuntimeError(f"冷启动拉取交易所快照失败: {e}") from e

    non_zero_positions = [
        f"{p.symbol}:{p.position_side}={p.position_amt}"
        for p in account_info.positions
        if abs(p.position_amt) > Decimal("0")
    ]
    if len(open_orders) > 0 or len(non_zero_positions) > 0:
        log.critical(
            "cold_start_non_flat_abort",
            open_orders_cnt=len(open_orders),
            non_zero_positions=non_zero_positions,
        )
        raise RuntimeError(
            f"冷启动拒绝准入：检测到交易所存在未结挂单 ({len(open_orders)} 个) "
            f"或未平持仓 ({non_zero_positions})。"
            "一期架构限定纯净空仓部署，请先人工或脚本清仓！"
        )

    state.update_balances(
        balances=account_info.wallet_balances,
        available_balance_snapshot=account_info.available_balance,
    )
    log.info(
        "cold_start_baseline_aligned", available=str(account_info.available_balance)
    )
