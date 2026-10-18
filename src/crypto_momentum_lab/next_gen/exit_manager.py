from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from functools import partial
from typing import Any

import structlog

from crypto_momentum_lab.domain.market.closed_candle import ClosedCandle15m
from crypto_momentum_lab.domain.strategy.models import StrategySide
from crypto_momentum_lab.domain.strategy.position_exit import (
    PositionExitPolicy,
    position_exit_reason,
)
from crypto_momentum_lab.next_gen.client import BinanceRestClient
from crypto_momentum_lab.next_gen.engine import AccountState
from crypto_momentum_lab.next_gen.executor import OrderExecutor
from crypto_momentum_lab.next_gen.models import Order, OrderStatus, SystemMode
from crypto_momentum_lab.next_gen.symbol_rules import SymbolRulesRegistry

log = structlog.get_logger("exit_manager")


class ExitManager:
    """批次退出管理器：管理宽限期限价退出与超时市价清扫 (Sweep)"""

    def __init__(
        self,
        rest_client: BinanceRestClient,
        state: AccountState,
        executor: OrderExecutor | None = None,
        symbol_rules: SymbolRulesRegistry | None = None,
        *,
        grace_period_sec: float,
        decision_profit_pct: Decimal,
        grace_profit_pct: Decimal,
        eval_interval_sec: float = 1.0,
    ) -> None:
        self._rest = rest_client
        self._state = state
        self.symbol_rules = symbol_rules or SymbolRulesRegistry()
        self.grace_period_sec = grace_period_sec
        self.decision_profit_pct = decision_profit_pct
        self.grace_profit_pct = grace_profit_pct
        self.eval_interval_sec = eval_interval_sec
        self._active_exit_tasks: dict[str, asyncio.Task[Any]] = {}

        if executor is not None:
            self._executor = executor
        elif state.db is not None:
            self._executor = OrderExecutor(rest_client, state, state.db)
        else:
            raise ValueError("必须提供 executor 或 state.db 不能为空")

    async def execute_batch_exit(
        self,
        batch_id: str,
        limit_price: Decimal,
        grace_period_sec: float | None = None,
        immediate: bool = False,
    ) -> None:
        """执行单个批次平仓退出：
        1. 发送限价平仓单 (LIMIT fl_{batch_id})；
        2. 经过 grace_period_sec 宽限期；
        3. 若批次仍有剩余持仓，发起撤销限价单；
        4. 严格等待 WebSocket 事件通知该限价单达到终态并核算准确剩余净量；
        5. 若仍有未平仓量，以市价单 (MARKET fm_{batch_id}) 扫平剩余持仓。
        """
        grace_sec = (
            grace_period_sec if grace_period_sec is not None else self.grace_period_sec
        )
        batch = self._state.batches.get(batch_id)
        if not batch or batch.quantity <= Decimal("0"):
            return

        # 0. 先确保撤销本批次所有尚未成交的开仓挂单，防止边平仓边被撮合成交
        for order in list(self._state.orders.values()):
            if order.batch_id == batch_id and not order.is_terminal:
                is_entry = (order.position_side == "LONG" and order.side == "BUY") or (
                    order.position_side == "SHORT" and order.side == "SELL"
                )
                if is_entry:
                    if not await self._executor.cancel_and_confirm(order):
                        return

        batch = self._state.batches.get(batch_id)
        if not batch or batch.quantity <= 0:
            return

        # 1. 提交限价退出单 (按标的规则对齐价格与数量)
        rounded_limit_price = self.symbol_rules.round_price(batch.symbol, limit_price)
        rounded_exit_qty = self.symbol_rules.round_quantity(
            batch.symbol, batch.quantity
        )
        if rounded_exit_qty <= Decimal("0") or rounded_limit_price <= Decimal("0"):
            return

        limit_order = self._state.create_exit_order(
            batch_id=batch_id,
            quantity=rounded_exit_qty,
            price=None if immediate else rounded_limit_price,
            order_type="MARKET" if immediate else "LIMIT",
        )
        await self._executor.submit_order(limit_order)
        if immediate:
            return

        await self._complete_limit_exit(limit_order, grace_sec)

    async def _complete_limit_exit(self, limit_order: Order, grace_sec: float) -> None:
        batch_id = limit_order.batch_id
        batch = self._state.batches[batch_id]
        # Keep the original persisted deadline across process restarts.
        submitted_at = batch.exit_order_submitted_at or datetime.now(UTC)
        elapsed = (datetime.now(UTC) - submitted_at).total_seconds()
        await asyncio.sleep(max(0, grace_sec - elapsed))

        # 3. 检查批次是否仍有敞口
        current_batch = self._state.batches.get(batch_id)
        if not current_batch or current_batch.quantity <= Decimal("0"):
            return
        # Replacement requires the final private WebSocket observation.
        if not await self._executor.cancel_and_confirm(limit_order):
            return

        remaining_batch = self._state.batches.get(batch_id)
        if (
            remaining_batch
            and remaining_batch.quantity > Decimal("0")
            and self._state.can_submit_exit_order()
        ):
            sweep_qty = self.symbol_rules.round_quantity(
                remaining_batch.symbol, remaining_batch.quantity
            )
            if sweep_qty > Decimal("0"):
                market_order = self._state.create_exit_order(
                    batch_id=batch_id,
                    quantity=sweep_qty,
                    price=None,
                    order_type="MARKET",
                )
                await self._executor.submit_order(market_order)

    def evaluate_active_batches(
        self,
        now: datetime | None = None,
        current_prices: dict[str, Decimal] | None = None,
        closed_candle: ClosedCandle15m | None = None,
    ) -> list[str]:
        """评估当前所有活跃批次，触发到期退出或 Drain 排空退出"""
        if not self._state.can_submit_exit_order():
            return []

        eval_now = now or datetime.now(UTC)
        scheduled_batch_ids: list[str] = []
        prices = current_prices or {}

        for batch in list(self._state.batches.values()):
            if batch.status == "CLOSED" or batch.quantity <= Decimal("0"):
                continue

            exit_key = (self._state.account_label, batch.batch_id)
            if (
                exit_key in self._state.active_exit_orders
                or batch.batch_id in self._active_exit_tasks
            ):
                # 批次已存在活跃平仓挂单，不重复调度
                continue

            should_exit = False
            exit_reason = ""

            # 规则 1: 管理员下发受控排空 (Drain)
            if self._state.is_draining:
                should_exit = True
                exit_reason = "drain_active"
            else:
                # Only a formal, fully closed 15m candle can trigger normal exit.
                reason = position_exit_reason(
                    held_until=eval_now,
                    opened_at=batch.entry_bar_time,
                    symbol=batch.symbol,
                    side=StrategySide(batch.position_side.lower()),
                    policy=PositionExitPolicy(max_holding_seconds=None),
                    closed_candle=closed_candle,
                )
                if reason is not None:
                    should_exit = True
                    exit_reason = reason

            if should_exit:
                if batch.symbol not in prices:
                    continue
                reference_price = prices[batch.symbol]
                direction = (
                    Decimal("1") if batch.position_side == "LONG" else Decimal("-1")
                )
                gross_return = (
                    direction
                    * (reference_price - batch.entry_price)
                    / batch.entry_price
                )
                immediate = (
                    self._state.is_draining
                    or self.grace_period_sec == 0
                    or self.grace_profit_pct == 0
                    or gross_return >= self.decision_profit_pct
                )
                limit_price = batch.entry_price * (
                    1 + direction * self.grace_profit_pct
                )
                log.info(
                    "scheduling_batch_exit",
                    batch_id=batch.batch_id,
                    symbol=batch.symbol,
                    reason=exit_reason,
                    limit_price=str(limit_price),
                )
                task = asyncio.create_task(
                    self.execute_batch_exit(
                        batch_id=batch.batch_id,
                        limit_price=limit_price,
                        immediate=immediate,
                    )
                )
                self._active_exit_tasks[batch.batch_id] = task
                task.add_done_callback(partial(self._exit_task_done, batch.batch_id))
                scheduled_batch_ids.append(batch.batch_id)

        return scheduled_batch_ids

    def resume_active_exits(self) -> None:
        for order in self._state.orders.values():
            if order.is_terminal or not order.client_order_id.startswith("fl_"):
                continue
            if order.batch_id in self._active_exit_tasks:
                continue
            task = asyncio.create_task(
                self._complete_limit_exit(order, self.grace_period_sec)
            )
            self._active_exit_tasks[order.batch_id] = task
            task.add_done_callback(partial(self._exit_task_done, order.batch_id))

        # Graceful shutdown cancels exchange orders, but not the batch's exit
        # decision. Resume that responsibility with its original deadline.
        for batch in self._state.batches.values():
            if batch.quantity <= 0 or batch.exit_order_submitted_at is None:
                continue
            if (
                batch.batch_id in self._active_exit_tasks
                or (self._state.account_label, batch.batch_id)
                in self._state.active_exit_orders
            ):
                continue
            previous = max(
                (
                    order
                    for order in self._state.orders.values()
                    if order.batch_id == batch.batch_id
                    and order.client_order_id.startswith(("fl_", "fm_"))
                ),
                key=lambda order: order.created_at,
                default=None,
            )
            if (
                previous is None
                or previous.status is not OrderStatus.CANCELED
                or previous.price is None
            ):
                continue
            expired = (
                datetime.now(UTC) - batch.exit_order_submitted_at
            ).total_seconds() >= self.grace_period_sec
            task = asyncio.create_task(
                self.execute_batch_exit(
                    batch.batch_id,
                    previous.price,
                    immediate=expired,
                )
            )
            self._active_exit_tasks[batch.batch_id] = task
            task.add_done_callback(partial(self._exit_task_done, batch.batch_id))

    async def run(
        self,
        stop_event: asyncio.Event | None = None,
        price_provider: Callable[[str], Decimal | None] | None = None,
    ) -> None:
        """持续盯盘协程 (由 TaskGroup 结构化调度)"""
        self.resume_active_exits()
        log.info("exit_manager_loop_started")
        while True:
            if stop_event and stop_event.is_set():
                break
            try:
                if stop_event is not None:
                    try:
                        await asyncio.wait_for(
                            stop_event.wait(), timeout=self.eval_interval_sec
                        )
                        break
                    except TimeoutError:
                        pass
                else:
                    await asyncio.sleep(self.eval_interval_sec)

                prices: dict[str, Decimal] = {}
                if price_provider is not None:
                    for batch in self._state.batches.values():
                        if batch.status == "ACTIVE" and batch.symbol not in prices:
                            p = price_provider(batch.symbol)
                            if p is not None:
                                prices[batch.symbol] = p

                self.evaluate_active_batches(current_prices=prices)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error("exit_manager_loop_error", err=str(e))

        log.info("exit_manager_loop_stopped")

    def _exit_task_done(self, batch_id: str, task: asyncio.Task[Any]) -> None:
        self._active_exit_tasks.pop(batch_id, None)
        if not task.cancelled() and task.exception() is not None:
            log.error(
                "batch_exit_failed", batch_id=batch_id, error=str(task.exception())
            )
            self._state.has_unrecoverable_fault = True
            self._state.set_system_mode(SystemMode.DEGRADED)

    async def close(self) -> None:
        """平稳取消并等待所有活跃平仓任务完成"""
        tasks = list(self._active_exit_tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await self._executor.close()
