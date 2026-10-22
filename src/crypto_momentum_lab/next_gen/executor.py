from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import structlog

from crypto_momentum_lab.next_gen.client import (
    BinanceOrderRejectedError,
    BinanceRestClient,
)
from crypto_momentum_lab.next_gen.engine import AccountState
from crypto_momentum_lab.next_gen.models import Order, OrderStatus, SystemMode
from crypto_momentum_lab.next_gen.persistence import TradingRepositoryProtocol
from crypto_momentum_lab.next_gen.symbol_rules import SymbolRulesRegistry

log = structlog.get_logger("order_executor")


class OrderExecutor:
    """下一代极简发单器 (直调 REST API + 预写库 + 延时看门狗)"""

    def __init__(
        self,
        rest_client: BinanceRestClient,
        state: AccountState,
        db: TradingRepositoryProtocol,
        read_client: BinanceRestClient | None = None,
        entry_leverage: int = 5,
        margin_type: str = "CROSSED",
    ) -> None:
        self._rest = rest_client
        self._read = read_client or rest_client
        self._state = state
        self._db = db
        self._entry_leverage = entry_leverage
        self._margin_type = margin_type
        self._configured_symbols: set[str] = set()
        self._settings_failed_symbols: set[str] = set()
        self._watchdog_tasks: set[asyncio.Task[Any]] = set()
        self._inflight_submissions = 0
        self._submissions_idle = asyncio.Event()
        self._submissions_idle.set()

    async def record_exit_decision(
        self,
        batch_id: str,
        symbol: str,
        reason: str,
        observed_at: datetime,
    ) -> None:
        await self._db.async_save_exit_decision(
            self._state.environment,
            self._state.account_label,
            observed_at,
            {"batch_id": batch_id, "symbol": symbol, "reason": reason},
        )

    @property
    def rest_client(self) -> BinanceRestClient:
        return self._rest

    async def close(self) -> None:
        """取消所有正在运行的延时看门狗任务"""
        for task in list(self._watchdog_tasks):
            task.cancel()
        if self._watchdog_tasks:
            await asyncio.gather(*self._watchdog_tasks, return_exceptions=True)

    async def _ensure_symbol_settings(self, symbol: str) -> None:
        """Configure margin/leverage once per symbol, lazily before entry.

        Setting these for every tradable symbol at startup fails on symbols
        whose maximum leverage is below the strategy value (Binance -4028).
        Configure only symbols that actually become entries; cache failures so
        a permanently unsupported symbol does not hammer REST on every signal.
        """
        if symbol in self._configured_symbols:
            return
        if symbol in self._settings_failed_symbols:
            raise RuntimeError(f"Symbol settings previously failed: {symbol}")
        try:
            await self._rest.set_margin_type(symbol, self._margin_type)
            await self._rest.set_leverage(symbol, leverage=self._entry_leverage)
        except BinanceOrderRejectedError:
            # Permanent exchange rejection for this symbol (for example -4028).
            # Cache it; a transient network/429 failure must be retried.
            self._settings_failed_symbols.add(symbol)
            raise
        self._configured_symbols.add(symbol)

    async def cancel_and_confirm(self, order: Order, timeout_sec: float = 2.0) -> bool:
        """Cancel once and confirm the final cumulative fill before replacing it."""
        if order.is_terminal:
            return True
        try:
            await self._rest.cancel_order(
                symbol=order.symbol,
                client_order_id=order.client_order_id,
            )
        except Exception as error:
            log.warning("cancel_request_failed_waiting_for_ws", error=str(error))
        if not order.is_terminal:
            await self._state.wait_for_terminal(order, timeout_sec)
        if not order.is_terminal:
            order.status = OrderStatus.RECONCILING
            self._state.set_system_mode(SystemMode.DEGRADED)
        return order.is_terminal

    async def load_active_state(self) -> None:
        """Restore outstanding orders and their batches, without replaying history."""
        env, account = self._state.environment, self._state.account_label
        self._state.orders.clear()
        self._state.active_entry_orders.clear()
        self._state.active_exit_orders.clear()
        batches = await self._db.async_get_active_batches(env, account)
        active_orders = await self._db.async_get_active_orders(env, account)
        self._state.batches = {b.batch_id: deepcopy(b) for b in batches}
        for order in active_orders:
            self._state.orders[order.client_order_id] = order
            if order.batch_id not in self._state.batches:
                batch = await self._db.async_get_batch(env, account, order.batch_id)
                if batch is not None:
                    self._state.batches[batch.batch_id] = deepcopy(batch)
        for batch in self._state.batches.values():
            orders = await self._db.async_get_batch_orders(env, account, batch.batch_id)
            self._state.orders.update({o.client_order_id: o for o in orders})
            batch.exiting_quantity = Decimal("0")
            for order in orders:
                if order.client_order_id.startswith(("fl_", "fm_")):
                    sequence = int(order.client_order_id.rsplit("_", 1)[-1])
                    batch.exit_order_counter = max(batch.exit_order_counter, sequence)
            if batch.quantity > 0:
                batch.status = "ACTIVE"
        for order in self._state.orders.values():
            if order.is_terminal:
                continue
            is_exit = (order.position_side == "LONG" and order.side == "SELL") or (
                order.position_side == "SHORT" and order.side == "BUY"
            )
            if is_exit:
                key = (account, order.batch_id)
                if key in self._state.active_exit_orders:
                    raise ValueError("Multiple outstanding exits for one batch")
                self._state.active_exit_orders[key] = order.client_order_id
                batch = self._state.batches[order.batch_id]
                batch.exiting_quantity += order.quantity - order.filled_qty
                batch.status = "EXITING"
            else:
                entry_key = (account, order.symbol, order.position_side)
                if entry_key in self._state.active_entry_orders:
                    raise ValueError("Multiple outstanding entries for one symbol")
                self._state.active_entry_orders[entry_key] = order.client_order_id

    async def restore_startup_state(self) -> None:
        """Recover stored responsibility once, before admitting strategy orders."""
        if self._state.system_mode != SystemMode.BOOTING:
            raise RuntimeError("REST recovery is only allowed during startup")
        mapping = {
            "NEW": OrderStatus.OPEN,
            "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
            "FILLED": OrderStatus.FILLED,
            "CANCELED": OrderStatus.CANCELED,
            "EXPIRED": OrderStatus.CANCELED,
            "EXPIRED_IN_MATCH": OrderStatus.CANCELED,
            "REJECTED": OrderStatus.REJECTED,
        }
        for order in list(self._state.orders.values()):
            if order.is_terminal:
                continue
            row = await self._read.fetch_startup_order(
                order.symbol, order.client_order_id
            )
            if (
                row["clientOrderId"] != order.client_order_id
                or row["symbol"] != order.symbol
                or row["side"] != order.side
                or row["positionSide"] != order.position_side
            ):
                raise RuntimeError(
                    "Startup order identity does not match stored command"
                )
            quantity, average = Decimal(row["executedQty"]), Decimal(row["avgPrice"])
            if (
                not quantity.is_finite()
                or not average.is_finite()
                or quantity > order.quantity
            ):
                raise RuntimeError("Invalid startup order execution values")
            if quantity < order.filled_qty:
                # The connected stream has already advanced this order.
                continue
            status = mapping[row["status"]]
            if quantity > 0 and average <= 0:
                raise RuntimeError("Startup filled order has no execution price")
            if status == OrderStatus.FILLED and quantity != order.quantity:
                raise RuntimeError(
                    "Startup FILLED order has incomplete execution quantity"
                )
            await self._state.on_order_trade_update(
                order.client_order_id,
                order.symbol,
                order.position_side,
                order.side,
                status,
                quantity,
                average,
                quantity - order.filled_qty,
                average,
                exchange_order_id=int(row["orderId"]),
                trade_time=datetime.fromtimestamp(row["updateTime"] / 1000, tz=UTC),
            )
        snapshot, open_orders = await asyncio.gather(
            self._read.fetch_account_information(),
            self._read.fetch_open_orders(),
        )
        expected: dict[tuple[str, str], Decimal] = {}
        for batch in self._state.batches.values():
            if batch.quantity > 0:
                key = (batch.symbol, batch.position_side)
                expected[key] = expected.get(key, Decimal("0")) + batch.quantity
        actual = {
            (p.symbol, p.position_side): abs(p.position_amt)
            for p in snapshot.positions
            if p.position_amt != 0
        }
        exchange_ids = {row["clientOrderId"] for row in open_orders}
        local_ids = {
            order.client_order_id
            for order in self._state.orders.values()
            if not order.is_terminal
        }
        if actual != expected or exchange_ids != local_ids:
            raise RuntimeError("启动基线不一致：检测到交易所存在未结挂单或持仓差异")
        self._state.update_balances(
            snapshot.wallet_balances, snapshot.available_balance
        )
        if self._state.has_unrecoverable_fault or self._state.private_stream_gap:
            raise RuntimeError("Startup private state is not trustworthy")

    async def drain(self, rules: SymbolRulesRegistry) -> int:
        """Close loaded batches; only private WebSocket events confirm execution."""
        if not self._state.ws_connected:
            raise RuntimeError("Drain requires a connected private WebSocket")
        for order in list(self._state.orders.values()):
            if not order.is_terminal and not await self.cancel_and_confirm(order):
                raise RuntimeError("Drain cancellation was not confirmed by WebSocket")
        count = 0
        for batch in list(self._state.batches.values()):
            if batch.quantity <= 0:
                continue
            quantity = rules.round_quantity(batch.symbol, batch.quantity)
            if quantity <= 0 or quantity != batch.quantity:
                raise RuntimeError("Exchange quantity does not align with symbol rules")
            await self.record_exit_decision(
                batch.batch_id,
                batch.symbol,
                "operator_drain",
                datetime.now(UTC),
            )
            order = self._state.create_exit_order(
                batch.batch_id, quantity, None, order_type="MARKET"
            )
            await self.submit_order(order)
            await self._state.wait_for_terminal(order, timeout_sec=10.0)
            if not order.is_terminal or batch.quantity != 0:
                self._state.set_system_mode(SystemMode.DEGRADED)
                raise RuntimeError("Drain market exit was not confirmed by WebSocket")
            count += 1
        return count

    async def wait_for_submissions(self) -> None:
        await self._submissions_idle.wait()

    async def submit_order(self, order: Order) -> dict[str, Any]:
        self._inflight_submissions += 1
        self._submissions_idle.clear()
        try:
            return await self._submit_order(order)
        except Exception:
            # Preparation or a safety gate failed before any order was sent.
            if order.status == OrderStatus.PENDING:
                self._state.rollback_unsubmitted_order(order)
            raise
        finally:
            self._inflight_submissions -= 1
            if self._inflight_submissions == 0:
                self._submissions_idle.set()

    async def _submit_order(self, order: Order) -> dict[str, Any]:
        """提交订单：
        1. 幂等与状态校验：必须处于 PENDING；
        2. 执行发单安全门与名义价值上限校验；
        3. 预写库落库为 SUBMITTED (失败则回滚并进入 DEGRADED，绝不外呼)；
        4. 启动 3.0s 超时探针看门狗；
        5. 发送 REST 请求。明确业务拒单 (4xx) 原子更新为 REJECTED 并释放活跃锁；
        6. 网络超时/5xx 服务端错误保持 SUBMITTED 与锁定，交由看门狗触发存疑查证。
        """
        # 1. 状态幂等检查
        if order.status != OrderStatus.PENDING:
            raise RuntimeError(
                f"只能提交处于 PENDING 状态的订单: {order.client_order_id}, "
                f"当前状态: {order.status.name}"
            )

        # 2. 发单安全门与名义价值上限校验
        if order.client_order_id.startswith("e_"):
            if not self._state.can_submit_entry_order():
                raise RuntimeError(
                    f"安全门阻断开仓单: {order.client_order_id}, "
                    f"mode={self._state.system_mode.name}, "
                    f"draining={self._state.is_draining}"
                )
            price = order.price or Decimal("0")
            if price > Decimal("0") and not self._state.check_notional_limits(
                symbol=order.symbol,
                new_quantity=order.quantity,
                current_price=price,
                exclude_order_id=order.client_order_id,
            ):
                raise RuntimeError(
                    f"名义价值上限阻断开仓单: {order.client_order_id}, "
                    f"symbol={order.symbol}"
                )
            await self._ensure_symbol_settings(order.symbol)
            if not self._state.can_submit_entry_order():
                raise RuntimeError(
                    "Symbol preparation completed after entry gate closed"
                )
        else:
            if not self._state.can_submit_exit_order():
                raise RuntimeError(
                    f"安全门阻断平仓单: {order.client_order_id}, "
                    f"mode={self._state.system_mode.name}"
                )

        # 3. 预写库防御：落库失败则立即回滚内存状态并释放锁
        # 绝不向交易所发送不可审计订单
        order.apply_update(OrderStatus.SUBMITTED)
        try:
            await self._db.async_save_order(order)
        except Exception as e:
            log.critical(
                "db_prewrite_failed_rolling_back_order",
                order_id=order.client_order_id,
                err=str(e),
            )
            self._state.rollback_unsubmitted_order(order)
            self._state.has_unrecoverable_fault = True
            self._state.set_system_mode(SystemMode.DEGRADED)
            raise RuntimeError(
                f"订单预落库失败，拒绝向交易所发单并进入安全降级: {e}"
            ) from e

        # 启动单次延时看门狗：发一单盯一单，3.0s 超时未收到 WS 推流则转 RECONCILING
        watchdog_task = asyncio.create_task(
            self._state.watch_submission_timeout(order.client_order_id, timeout_sec=3.0)
        )
        self._watchdog_tasks.add(watchdog_task)
        watchdog_task.add_done_callback(self._watchdog_tasks.discard)

        try:
            res = await self._rest.post_order(
                symbol=order.symbol,
                side=order.side,
                position_side=order.position_side,
                order_type=order.order_type,
                quantity=order.quantity,
                price=order.price,
                client_order_id=order.client_order_id,
            )
            return res
        except BinanceOrderRejectedError as e:
            # 业务明确拒单（如 -2010/-2019 等）：交易所未建单，原子清理活跃锁与预留
            self._state.mark_order_rejected(order.client_order_id, e.message)
            await self._db.async_save_order(order)
            raise
        except Exception as e:
            # 网络超时/5xx：订单处于存疑状态，绝不清理活跃锁，由看门狗转入 RECONCILING
            log.warning(
                "order_submission_in_doubt_due_to_network_failure",
                order_id=order.client_order_id,
                error=str(e),
            )
            raise
