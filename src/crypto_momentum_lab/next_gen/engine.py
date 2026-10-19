from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from crypto_momentum_lab.next_gen.persistence import TradingRepositoryProtocol

import structlog

from crypto_momentum_lab.domain.account.models import AccountFillEvent
from crypto_momentum_lab.domain.risk.models import RiskConfigSnapshot
from crypto_momentum_lab.live_rollout.scheduled_risk_window import (
    ScheduledRiskWindowConfig,
)
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
        self.entry_halted = False
        self.scheduled_risk_window: ScheduledRiskWindowConfig | None = None
        self.scheduled_flatten_pending = False
        self.ws_connected: bool = False
        self.has_unrecoverable_fault: bool = False
        self.private_stream_gap = False
        self._update_lock = asyncio.Lock()
        self._terminal_waiters: dict[str, asyncio.Event] = {}
        self.risk_config: RiskConfigSnapshot | None = None
        self.latest_prices: dict[str, Decimal] = {}
        self.daily_realized_pnl = Decimal("0")
        self.risk_day = datetime.now(UTC).date()
        self.daily_pnl_ready = False
        self.income_cutoff: datetime | None = None

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
        if (
            self.is_draining
            or self.has_unrecoverable_fault
            or not self.ws_connected
            or self.private_stream_gap
        ):
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
        self.private_stream_gap = False
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
            if any(not order.is_terminal for order in self.orders.values()) or any(
                batch.quantity > 0 for batch in self.batches.values()
            ):
                self.private_stream_gap = True
                for order in self.orders.values():
                    if not order.is_terminal:
                        order.status = OrderStatus.RECONCILING
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
        if (
            self.system_mode != SystemMode.RUNNING
            or self.is_draining
            or self.risk_config is None
            or self.entry_halted
            or self.scheduled_entry_blocked()
        ):
            return False
        has_in_doubt = any(
            o.status == OrderStatus.RECONCILING for o in self.orders.values()
        )
        return (
            self.ws_connected
            and not self.has_unrecoverable_fault
            and not has_in_doubt
            and not self.private_stream_gap
        )

    def scheduled_entry_blocked(self, now: datetime | None = None) -> bool:
        return self.scheduled_flatten_pending or (
            self.scheduled_risk_window is not None
            and not self.scheduled_risk_window.is_entry_allowed(
                now or datetime.now(UTC)
            )
        )

    def can_submit_exit_order(self) -> bool:
        has_in_doubt = any(
            o.status == OrderStatus.RECONCILING for o in self.orders.values()
        )
        if (
            has_in_doubt
            or self.has_unrecoverable_fault
            or self.private_stream_gap
            or self.system_mode in (SystemMode.BOOTING, SystemMode.STOPPED)
        ):
            return False
        if self.is_draining:
            # Draining is not an excuse to send exits without a live private
            # stream: every cancel/fill must still be confirmed by WS.
            return self.ws_connected
        if not self.ws_connected:
            return False
        if self.system_mode in (SystemMode.RUNNING, SystemMode.DEGRADED):
            return True
        return False

    def check_notional_limits(
        self,
        symbol: str,
        new_quantity: Decimal,
        current_price: Decimal,
        latest_prices: dict[str, Decimal] | None = None,
        exclude_order_id: str | None = None,
    ) -> bool:
        """Apply the persisted account limits, including pending entry exposure."""
        config = self.risk_config
        if config is None:
            return False
        prices = dict(self.latest_prices if latest_prices is None else latest_prices)
        prices[symbol] = current_price
        notional = new_quantity * current_price
        if (
            config.max_order_notional is not None
            and notional > config.max_order_notional
        ):
            return False
        gross = Decimal("0")
        unrealized = Decimal("0")
        occupied_symbols = set()
        for batch in self.batches.values():
            if batch.quantity <= 0:
                continue
            price = prices.get(batch.symbol)
            if price is None:
                return False
            occupied_symbols.add(batch.symbol)
            gross += batch.quantity * price
            direction = Decimal("1") if batch.position_side == "LONG" else Decimal("-1")
            unrealized += direction * (price - batch.entry_price) * batch.quantity
        for order in self.orders.values():
            if order.is_terminal or not order.client_order_id.startswith("e_"):
                continue
            if order.client_order_id == exclude_order_id:
                continue
            price = prices.get(order.symbol)
            if price is None:
                return False
            occupied_symbols.add(order.symbol)
            gross += (order.quantity - order.filled_qty) * price
        if (
            config.max_gross_notional is not None
            and gross + notional > config.max_gross_notional
        ):
            return False
        if config.max_open_positions is not None and symbol not in occupied_symbols:
            if len(occupied_symbols) >= config.max_open_positions:
                return False
        today = datetime.now(UTC).date()
        if today != self.risk_day:
            self.risk_day = today
            self.daily_realized_pnl = Decimal("0")
            self.daily_pnl_ready = True
        if config.max_daily_loss is not None:
            if not self.daily_pnl_ready:
                return False
            if self.daily_realized_pnl + unrealized <= -config.max_daily_loss:
                return False
        return True

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
        batch.exit_order_counter += 1
        seq = batch.exit_order_counter
        if batch.exit_order_submitted_at is None:
            batch.exit_order_submitted_at = datetime.now(UTC)

        side = "SELL" if batch.position_side == "LONG" else "BUY"
        client_order_id = generate_exit_order_id(batch_id, order_type, seq=seq)

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

    def rollback_unsubmitted_order(self, order: Order) -> None:
        """预写库失败回滚：从 orders 移除未提交订单，释放锁，释放批次预留"""
        self.orders.pop(order.client_order_id, None)
        self._release_active_order_lock(order)
        is_exit = (order.position_side == "LONG" and order.side == "SELL") or (
            order.position_side == "SHORT" and order.side == "BUY"
        )
        if is_exit:
            batch = self.batches.get(order.batch_id)
            if batch:
                order.release_batch_reservation(batch)
        log.warning(
            "order_unsubmitted_rolled_back",
            order_id=order.client_order_id,
            account=self.account_label,
        )

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
                order.release_batch_reservation(batch)
        log.warning("order_gateway_rejected_cleaned", id=client_order_id, reason=reason)

    async def watch_submission_timeout(
        self, client_order_id: str, timeout_sec: float = 3.0
    ) -> None:
        """单次延时看门狗: 发一单盯一单，超时后若仍为 SUBMITTED/PENDING
        则转 RECONCILING，等待 WebSocket 确认，不发起 REST 查询。
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

    async def wait_for_terminal(self, order: Order, timeout_sec: float) -> None:
        # An in-memory terminal status can precede the transaction commit.
        # Wait for the private-event handler before observing completion.
        async with self._update_lock:
            if order.is_terminal:
                return
            event = self._terminal_waiters.setdefault(
                order.client_order_id, asyncio.Event()
            )
        try:
            await asyncio.wait_for(event.wait(), timeout_sec)
        except TimeoutError:
            pass
        finally:
            self._terminal_waiters.pop(order.client_order_id, None)

    async def on_account_update(
        self,
        balances: dict[str, Decimal],
        available_balance: Decimal | None = None,
        positions: list[dict[str, Any]] | None = None,
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
        trade_time: datetime | None = None,
        exchange_realized_pnl: Decimal | None = None,
        trade_payload: dict[str, Any] | None = None,
    ) -> None:
        async with self._update_lock:
            if trade_payload is not None and self._db is not None:
                await self._db.async_save_fill(
                    AccountFillEvent(
                        environment=self.environment,
                        account_label=self.account_label,
                        symbol=trade_payload["s"],
                        trade_id=str(trade_payload["t"]),
                        order_id=str(trade_payload["i"]),
                        side=trade_payload["S"],
                        price=Decimal(str(trade_payload["L"])),
                        quantity=Decimal(str(trade_payload["l"])),
                        realized_pnl=Decimal(str(trade_payload["rp"])),
                        fee=Decimal(str(trade_payload["n"])),
                        fee_asset=trade_payload["N"],
                        trade_at=datetime.fromtimestamp(
                            trade_payload["T"] / 1000, tz=UTC
                        ),
                        raw_payload=trade_payload,
                    )
                )
            await self._apply_order_trade_update(
                client_order_id,
                symbol,
                position_side,
                side,
                order_status,
                cum_filled_qty,
                avg_price,
                last_filled_qty,
                last_filled_price,
                exchange_order_id,
                trade_time,
                exchange_realized_pnl,
            )

    async def _apply_order_trade_update(
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
        trade_time: datetime | None = None,
        exchange_realized_pnl: Decimal | None = None,
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

        previous_quantity, previous_average = order.filled_qty, order.avg_price
        delta_qty = order.apply_update(
            new_status=order_status,
            cum_filled_qty=cum_filled_qty,
            avg_price=avg_price,
            exchange_order_id=exchange_order_id,
        )
        if delta_qty > 0 and avg_price > 0:
            # Derive the new fill notional from the cumulative average.
            last_filled_price = (
                order.filled_qty * avg_price - previous_quantity * previous_average
            ) / delta_qty

        if (
            delta_qty > 0
            and exchange_realized_pnl is not None
            and trade_time is not None
        ):
            if trade_time.date() > self.risk_day:
                self.risk_day = trade_time.date()
                self.daily_realized_pnl = Decimal("0")
                self.daily_pnl_ready = True
            if trade_time.date() == self.risk_day and (
                self.income_cutoff is None or trade_time > self.income_cutoff
            ):
                self.daily_realized_pnl += exchange_realized_pnl

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
                order.release_batch_reservation(batch)

        if not is_exit and batch is None and delta_qty > Decimal("0"):
            batch = PositionBatch(
                environment=self.environment,
                account_label=self.account_label,
                batch_id=order.batch_id,
                symbol=order.symbol,
                position_side=order.position_side,
                quantity=delta_qty,
                entry_price=last_filled_price,
                entry_bar_time=trade_time or datetime.now(UTC),
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
                elif batch.exiting_quantity == 0 and order.is_terminal:
                    batch.status = "ACTIVE"
            else:
                total_qty = batch.quantity + delta_qty
                batch.entry_price = (
                    (batch.quantity * batch.entry_price)
                    + (delta_qty * last_filled_price)
                ) / total_qty
                batch.quantity = total_qty
                if trade_time is not None:
                    batch.entry_bar_time = max(batch.entry_bar_time, trade_time)
                if batch.status == "CLOSED":
                    log.warning(
                        "late_entry_fill_on_closed_batch_reopening",
                        batch_id=batch.batch_id,
                        delta_qty=str(delta_qty),
                    )
                    batch.status = "ACTIVE"
                    batch.entry_bar_time = trade_time or datetime.now(UTC)
            batch.updated_at = datetime.now(UTC)

        # 异步直调持久化 2 张表 (支持 1 次微退避重试)
        if self._db is not None:
            saved = False
            for attempt in range(2):
                try:
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
        if order.is_terminal and not self.has_unrecoverable_fault:
            waiter = self._terminal_waiters.get(order.client_order_id)
            if waiter is not None:
                waiter.set()
