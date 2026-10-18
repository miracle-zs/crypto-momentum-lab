from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

import structlog

from crypto_momentum_lab.next_gen.engine import AccountState
from crypto_momentum_lab.next_gen.executor import OrderExecutor
from crypto_momentum_lab.next_gen.models import generate_batch_id

log = structlog.get_logger("momentum_strategy_engine")


@dataclass(frozen=True, slots=True)
class Bar15s:
    """15 秒 K 线数据载荷"""

    symbol: str
    close_time: datetime
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: Decimal = Decimal("0")


class MomentumStrategyEngine:
    """下一代 15s 周期动量计算与开仓决策引擎"""

    def __init__(
        self,
        state: AccountState,
        order_executor: OrderExecutor,
        symbols: list[str] | None = None,
        min_warmup_bars: int = 140,
        momentum_window: int = 140,
        top_n: int = 1,
        min_momentum_threshold: Decimal = Decimal("0.0005"),
        batch_notional: Decimal = Decimal("100.0"),
        symbol_cap: Decimal = Decimal("1000.0"),
        account_cap: Decimal = Decimal("5000.0"),
        eval_interval_sec: float = 15.0,
    ) -> None:
        self.state = state
        self.order_executor = order_executor
        self.symbols = [s.upper() for s in (symbols or ["BTCUSDT", "ETHUSDT"])]
        self.min_warmup_bars = min_warmup_bars
        self.momentum_window = momentum_window
        self.top_n = top_n
        self.min_momentum_threshold = min_momentum_threshold
        self.batch_notional = batch_notional
        self.symbol_cap = symbol_cap
        self.account_cap = account_cap
        self.eval_interval_sec = eval_interval_sec

        # 内存中维护各标的历史 15s Bar 环形缓冲区
        self.bar_buffers: dict[str, list[Bar15s]] = defaultdict(list)
        self.latest_prices: dict[str, Decimal] = {}

    def on_bar(self, bar: Bar15s) -> None:
        """接收并缓存新的 15s Bar"""
        sym = bar.symbol.upper()
        buf = self.bar_buffers[sym]
        buf.append(bar)
        # 保留最近 2 倍窗口的 Bar 深度
        if len(buf) > self.min_warmup_bars * 2:
            buf.pop(0)
        self.latest_prices[sym] = bar.close_price

    def feed_bars(self, bars: list[Bar15s]) -> None:
        """批量预热注入 Bar"""
        for b in bars:
            self.on_bar(b)

    async def warmup_market_data(self) -> None:
        """阶段 2 策略行情预热：校验各标的历史数据深度 >= 140 Bar"""
        for sym in self.symbols:
            count = len(self.bar_buffers.get(sym, []))
            if count < self.min_warmup_bars:
                log.warning(
                    "strategy_warmup_depth_insufficient",
                    symbol=sym,
                    current=count,
                    required=self.min_warmup_bars,
                )
            else:
                log.info(
                    "strategy_warmup_complete",
                    symbol=sym,
                    depth=count,
                )

    def calculate_momentum(self, symbol: str) -> Decimal | None:
        """计算指定标的在给定窗口内的收益率动量"""
        buf = self.bar_buffers.get(symbol, [])
        if len(buf) < self.momentum_window:
            return None
        past_price = buf[-self.momentum_window].close_price
        current_price = buf[-1].close_price
        if past_price <= Decimal("0"):
            return None
        return (current_price - past_price) / past_price

    async def evaluate_and_generate_orders(self) -> list[str]:
        """执行单轮 15s 动量评估并下发开仓单"""
        if not self.state.can_submit_entry_order():
            return []

        # 1. 计算所有标的动量收益率: (symbol, momentum, current_price)
        scores: list[tuple[str, Decimal, Decimal]] = []
        for sym in self.symbols:
            mom = self.calculate_momentum(sym)
            if mom is not None and mom >= self.min_momentum_threshold:
                cur_price = self.latest_prices.get(sym)
                if cur_price and cur_price > Decimal("0"):
                    scores.append((sym, mom, cur_price))

        # 按动量从高到低排序，选取前 Top N
        scores.sort(key=lambda x: x[1], reverse=True)
        top_candidates = scores[: self.top_n]

        submitted_order_ids: list[str] = []

        # 2. 依次评估选出的最优标的
        for sym, mom, price in top_candidates:
            # 检查当前标的是否已有活跃开仓挂单
            entry_key = (self.state.account_label, sym, "LONG")
            if entry_key in self.state.active_entry_orders:
                log.debug("skip_entry_already_has_active_order", symbol=sym)
                continue

            # 计算拟开仓数量
            raw_qty = self.batch_notional / price
            # 币安精度规则粗约保留 3 位小数
            qty = Decimal(f"{raw_qty:.3f}")
            if qty <= Decimal("0"):
                continue

            # 3. 严格全账户与单标的名义价值上限校验 (Fail-Closed)
            if not self.state.check_notional_limits(
                symbol=sym,
                new_quantity=qty,
                current_price=price,
                symbol_cap=self.symbol_cap,
                account_cap=self.account_cap,
            ):
                log.warning(
                    "strategy_entry_rejected_by_notional_cap",
                    symbol=sym,
                    qty=str(qty),
                    price=str(price),
                )
                continue

            # 4. 生成批次与开仓单并提交
            batch_id = generate_batch_id(
                symbol=sym,
                position_side="LONG",
                account_label=self.state.account_label,
            )
            order = self.state.create_entry_order(
                symbol=sym,
                position_side="LONG",
                quantity=qty,
                price=price,
                batch_id=batch_id,
                order_type="LIMIT",
            )
            try:
                await self.order_executor.submit_order(order)
                submitted_order_ids.append(order.client_order_id)
                log.info(
                    "strategy_entry_submitted",
                    symbol=sym,
                    batch_id=batch_id,
                    order_id=order.client_order_id,
                    momentum=f"{mom:.4f}",
                )
            except Exception as e:
                log.error(
                    "strategy_entry_submission_failed",
                    order_id=order.client_order_id,
                    err=str(e),
                )

        return submitted_order_ids

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        """15s 周期评估主协程 (由 TaskGroup 结构化调度)"""
        log.info("momentum_strategy_engine_started", symbols=self.symbols)
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

                await self.evaluate_and_generate_orders()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error("strategy_eval_loop_error", err=str(e))

        log.info("momentum_strategy_engine_stopped")
