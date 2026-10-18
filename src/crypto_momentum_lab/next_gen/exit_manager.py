from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import structlog

from crypto_momentum_lab.next_gen.client import (
    BinanceOrderRejectedError,
    BinanceRestClient,
)
from crypto_momentum_lab.next_gen.engine import AccountState
from crypto_momentum_lab.next_gen.executor import OrderExecutor
from crypto_momentum_lab.next_gen.models import Order, OrderStatus
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
        grace_period_sec: float = 7200.0,
        max_holding_sec: float = 900.0,
        eval_interval_sec: float = 1.0,
    ) -> None:
        self._rest = rest_client
        self._state = state
        self.symbol_rules = symbol_rules or SymbolRulesRegistry()
        self.grace_period_sec = grace_period_sec
        self.max_holding_sec = max_holding_sec
        self.eval_interval_sec = eval_interval_sec
        self._active_exit_tasks: set[asyncio.Task[Any]] = set()

        if executor is not None:
            self._executor = executor
        elif state.db is not None:
            self._executor = OrderExecutor(rest_client, state, state.db)
        else:
            raise ValueError("必须提供 executor 或 state.db 不能为空")

    async def _wait_for_order_terminal(
        self, order: Order, timeout_sec: float = 5.0
    ) -> None:
        """等待 WebSocket 推流将订单推进至终态 (CANCELED/FILLED/REJECTED)"""
        start = asyncio.get_running_loop().time()
        while not order.is_terminal:
            if asyncio.get_running_loop().time() - start >= timeout_sec:
                log.warning(
                    "timeout_waiting_for_order_terminal_from_ws",
                    order_id=order.client_order_id,
                    status=order.status.name,
                )
                break
            await asyncio.sleep(0.05)

    async def execute_batch_exit(
        self,
        batch_id: str,
        limit_price: Decimal,
        grace_period_sec: float | None = None,
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
            price=rounded_limit_price,
            order_type="LIMIT",
        )
        await self._executor.submit_order(limit_order)

        # 2. 宽限期等待
        await asyncio.sleep(grace_sec)

        # 3. 检查批次是否仍有敞口
        batch = self._state.batches.get(batch_id)
        if not batch or batch.quantity <= Decimal("0"):
            return
        if limit_order.is_terminal and limit_order.status == OrderStatus.FILLED:
            return

        # 4. 宽限期超时仍未完全成交：向交易所发出撤单请求
        try:
            await self._rest.cancel_order(
                symbol=batch.symbol,
                client_order_id=limit_order.client_order_id,
            )
        except BinanceOrderRejectedError as e:
            # 交易所明确拒单 (如 -2011 订单不存在或已完成)，说明订单在撤单前已 FILLED
            log.info(
                "cancel_order_rejected_order_likely_already_filled",
                order_id=limit_order.client_order_id,
                code=e.code,
                msg=e.message,
            )
        except Exception as e:
            log.warning(
                "exit_manager_cancel_limit_order_network_error",
                order_id=limit_order.client_order_id,
                err=str(e),
            )
            # 网络故障时不盲目发市价单，保持现状由看门狗查证
            return

        # 5. 严格等待 WebSocket 事件将 limit_order 推进至终态
        # (吸收撤单期间可能发生的在途成交)
        await self._wait_for_order_terminal(limit_order, timeout_sec=2.0)

        # 若由于 WS 延迟未推来终态，主动调用 REST 探针查证真实订单状态
        if not limit_order.is_terminal:
            try:
                order_info = await self._rest.query_order(
                    symbol=limit_order.symbol,
                    client_order_id=limit_order.client_order_id,
                )
                if isinstance(order_info, dict):
                    raw_status = order_info.get("status")
                    binance_status = (
                        str(raw_status) if isinstance(raw_status, str) else "CANCELED"
                    )

                    def _parse_dec(val: Any) -> Decimal:
                        if isinstance(val, (int, float, str, Decimal)):
                            try:
                                return Decimal(str(val))
                            except Exception:
                                return Decimal("0")
                        return Decimal("0")

                    executed_qty = _parse_dec(order_info.get("executedQty"))
                    avg_price = _parse_dec(order_info.get("avgPrice"))
                    status_mapping = {
                        "NEW": OrderStatus.OPEN,
                        "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
                        "FILLED": OrderStatus.FILLED,
                        "CANCELED": OrderStatus.CANCELED,
                        "EXPIRED": OrderStatus.CANCELED,
                        "REJECTED": OrderStatus.REJECTED,
                    }
                    mapped_status = status_mapping.get(
                        binance_status, OrderStatus.CANCELED
                    )
                    if mapped_status in (
                        OrderStatus.CANCELED,
                        OrderStatus.FILLED,
                        OrderStatus.REJECTED,
                    ):
                        delta_qty = max(
                            Decimal("0"), executed_qty - limit_order.filled_qty
                        )
                        await self._state.on_order_trade_update(
                            client_order_id=limit_order.client_order_id,
                            symbol=limit_order.symbol,
                            position_side=limit_order.position_side,
                            side=limit_order.side,
                            order_status=mapped_status,
                            cum_filled_qty=executed_qty,
                            avg_price=avg_price,
                            last_filled_qty=delta_qty,
                            last_filled_price=avg_price,
                        )
                else:
                    await self._state.on_order_trade_update(
                        client_order_id=limit_order.client_order_id,
                        symbol=limit_order.symbol,
                        position_side=limit_order.position_side,
                        side=limit_order.side,
                        order_status=OrderStatus.CANCELED,
                        cum_filled_qty=limit_order.filled_qty,
                        avg_price=limit_order.avg_price,
                        last_filled_qty=Decimal("0"),
                        last_filled_price=limit_order.avg_price,
                    )
            except Exception as e:
                log.warning(
                    "exit_manager_failed_to_query_ground_truth_order",
                    order_id=limit_order.client_order_id,
                    err=str(e),
                )

        # 6. 市价清扫剩余未结数量 (根据 WS/REST 真实终态确认后的确切 batch.quantity)
        batch = self._state.batches.get(batch_id)
        if (
            batch
            and batch.quantity > Decimal("0")
            and limit_order.is_terminal
            and self._state.can_submit_exit_order()
        ):
            sweep_qty = self.symbol_rules.round_quantity(batch.symbol, batch.quantity)
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
    ) -> list[str]:
        """评估当前所有活跃批次，触发到期退出或 Drain 排空退出"""
        if not self._state.can_submit_exit_order():
            return []

        eval_now = now or datetime.now(UTC)
        scheduled_batch_ids: list[str] = []
        prices = current_prices or {}

        for batch in list(self._state.batches.values()):
            if batch.status != "ACTIVE" or batch.quantity <= Decimal("0"):
                continue

            exit_key = (self._state.account_label, batch.batch_id)
            if exit_key in self._state.active_exit_orders:
                # 批次已存在活跃平仓挂单，不重复调度
                continue

            should_exit = False
            exit_reason = ""

            # 规则 1: 管理员下发受控排空 (Drain)
            if self._state.is_draining:
                should_exit = True
                exit_reason = "drain_active"
            else:
                # 规则 2: 最大持仓周期到期退出 (15m = 900s)
                holding_sec = (eval_now - batch.entry_bar_time).total_seconds()
                if holding_sec >= self.max_holding_sec:
                    should_exit = True
                    exit_reason = f"max_holding_expired_{holding_sec:.1f}s"

            if should_exit:
                limit_price = prices.get(batch.symbol, batch.entry_price)
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
                    )
                )
                self._active_exit_tasks.add(task)
                task.add_done_callback(self._active_exit_tasks.discard)
                scheduled_batch_ids.append(batch.batch_id)

        return scheduled_batch_ids

    async def run(
        self,
        stop_event: asyncio.Event | None = None,
        price_provider: Callable[[str], Decimal | None] | None = None,
    ) -> None:
        """持续盯盘协程 (由 TaskGroup 结构化调度)"""
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

    async def close(self) -> None:
        """平稳取消并等待所有活跃平仓任务完成"""
        for task in list(self._active_exit_tasks):
            task.cancel()
        if self._active_exit_tasks:
            await asyncio.gather(*self._active_exit_tasks, return_exceptions=True)
        await self._executor.close()
