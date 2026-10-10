"""Runtime loops for the independent reduce-only exit channels."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, Callable, Collection
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

import structlog

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.live_rollout.exit_channel_ports import ExitChannelProcessor
from crypto_momentum_lab.live_rollout.exit_failure_policy import (
    ORDER_IDENTITY_CONFLICT_REASON,
    is_pending_context_refresh,
    is_pending_exit_evaluation,
)
from crypto_momentum_lab.live_rollout.market_cache import (
    LatestMarketQuoteCache,
    LatestMarketStateCache,
)

if TYPE_CHECKING:
    from crypto_momentum_lab.domain.market.models import RealtimeMarketQuote
    from crypto_momentum_lab.live_rollout.closed_candle_feed import ClosedCandle15mEvent

log = structlog.get_logger()
_MAX_RETAINED_CANDLES = 4096


def _never_order_identity_conflict(_error: Exception) -> bool:
    return False


class LiveExitChannelRuntime:
    """Own retry and backoff policy for quote, candle, and grace exits."""

    def __init__(
        self,
        *,
        daemon: ExitChannelProcessor,
        latest_market_quotes: LatestMarketQuoteCache,
        latest_market_states: LatestMarketStateCache,
        is_transient_error: Callable[[Exception], bool],
        is_order_identity_conflict: Callable[[Exception], bool] | None = None,
        on_exit_failure: Callable[[str, str | None], None] | None = None,
        on_order_identity_conflict: Callable[[str], None] | None = None,
        closed_candle_expires_at: Callable[[ClosedCandle15mEvent], datetime | None]
        | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._daemon = daemon
        self._latest_market_quotes = latest_market_quotes
        self._latest_market_states = latest_market_states
        self._is_transient_error = is_transient_error
        self._is_order_identity_conflict = (
            is_order_identity_conflict or _never_order_identity_conflict
        )
        self._on_exit_failure = on_exit_failure
        self._on_order_identity_conflict = on_order_identity_conflict
        self._closed_candle_expires_at = closed_candle_expires_at
        self._clock = clock
        self._candle_facts_changed = asyncio.Event()
        self._quote_facts_changed = asyncio.Event()
        self._grace_facts_changed = asyncio.Event()
        self._facts_generation = 0
        self._symbol_facts_generation: dict[str, int] = {}
        self._quote_retries: dict[str, tuple[float, float]] = {}
        self._grace_retries: dict[str, tuple[float, float]] = {}
        self._candle_retries: dict[str, tuple[float, float]] = {}
        self._sync_waits: dict[str, set[str]] = {
            "quote": set(),
            "grace": set(),
            "candle": set(),
        }
        self._quote_ready: set[str] = set()
        self._candle_ready: set[str] = set()
        self._pending_candles: dict[tuple[str, datetime], ClosedCandle15mEvent] = {}
        self._evaluated_candles: dict[tuple[str, datetime], None] = {}

    async def run_quote_channel(
        self,
        *,
        source: AsyncIterable[RealtimeMarketQuote],
    ) -> None:
        retries = self._quote_retries
        iterator = aiter(source)
        next_quote = asyncio.ensure_future(anext(iterator))
        changed = asyncio.create_task(self._quote_facts_changed.wait())
        try:
            while True:
                done, _ = await asyncio.wait(
                    {next_quote, changed}, return_when=asyncio.FIRST_COMPLETED
                )
                incoming = None
                exhausted = False
                if next_quote in done:
                    try:
                        incoming = next_quote.result()
                    except StopAsyncIteration:
                        exhausted = True
                    else:
                        next_quote = asyncio.ensure_future(anext(iterator))
                        if incoming.symbol not in self._daemon.managed_position_symbols:
                            self._clear_retry(incoming.symbol, retries)
                        self._latest_market_quotes.observe(incoming)
                ready = set()
                if changed in done:
                    self._quote_facts_changed.clear()
                    changed = asyncio.create_task(self._quote_facts_changed.wait())
                    ready, self._quote_ready = self._quote_ready, set()
                if incoming is not None:
                    ready.discard(incoming.symbol)
                    await self._evaluate_quote(incoming, retries)
                for symbol in sorted(ready):
                    quotes = self._latest_market_quotes.for_symbols((symbol,))
                    for quote in quotes:
                        await self._evaluate_quote(quote, retries)
                if exhausted:
                    return
        finally:
            for task in (next_quote, changed):
                task.cancel()
            await asyncio.gather(next_quote, changed, return_exceptions=True)

    async def _evaluate_quote(
        self,
        quote: RealtimeMarketQuote,
        retries: dict[str, tuple[float, float]],
    ) -> None:
        loop_time = asyncio.get_running_loop().time()
        if quote.symbol not in self._daemon.managed_position_symbols:
            self._clear_retry(quote.symbol, retries)
        if loop_time < retries.get(quote.symbol, (0.0, 1.0))[0]:
            return
        for state in self._latest_market_states.for_symbols((quote.symbol,)):
            generation = self._evaluation_generation(quote.symbol)
            try:
                failure = await self._daemon.process_market_quote(quote, state)
            except Exception as error:
                order_identity_conflict = self._is_order_identity_conflict(error)
                if not (self._is_transient_error(error) or order_identity_conflict):
                    raise
                failure = (
                    ORDER_IDENTITY_CONFLICT_REASON
                    if order_identity_conflict
                    else type(error).__name__
                )
                if order_identity_conflict:
                    if self._on_order_identity_conflict is not None:
                        self._on_order_identity_conflict(quote.symbol)
                    if self._on_exit_failure is not None:
                        self._on_exit_failure(quote.symbol, failure)
                log.warning(
                    "live_market_quote_processing_degraded",
                    symbol=quote.symbol,
                    error_type=type(error).__name__,
                    reason=failure,
                )
                continue
            self._record_result(
                quote.symbol,
                failure,
                retries,
                channel="quote",
                evaluation_generation=generation,
            )

    def _clear_retry(
        self, symbol: str, retries: dict[str, tuple[float, float]]
    ) -> None:
        channel = "quote" if retries is self._quote_retries else "grace"
        self._sync_waits[channel].discard(symbol)
        if retries.pop(symbol, None) is not None and self._on_exit_failure is not None:
            self._on_exit_failure(symbol, None)

    def _record_result(
        self,
        symbol: str,
        failure: str | None,
        retries: dict[str, tuple[float, float]],
        *,
        channel: str,
        evaluation_generation: tuple[int, int] | None = None,
    ) -> None:
        if is_pending_context_refresh(failure):
            self._sync_waits[channel].add(symbol)
            if (
                evaluation_generation is not None
                and evaluation_generation != self._evaluation_generation(symbol)
            ):
                self._wake_sync_waits((symbol,))
            return
        if failure is None:
            if (
                evaluation_generation is not None
                and evaluation_generation != self._evaluation_generation(symbol)
            ):
                # An older evaluation cannot clear a newer protection state.
                self._sync_waits[channel].add(symbol)
                self._wake_sync_waits((symbol,))
                return
            self._sync_waits[channel].discard(symbol)
            retries.pop(symbol, None)
            if self._on_exit_failure is not None:
                self._on_exit_failure(symbol, None)
            return
        pending = is_pending_exit_evaluation(failure)
        if pending:
            self._sync_waits[channel].add(symbol)
        else:
            self._sync_waits[channel].discard(symbol)
        if not pending and self._on_exit_failure is not None:
            self._on_exit_failure(symbol, failure)
        delay = retries.get(symbol, (0.0, 1.0))[1]
        retries[symbol] = (
            asyncio.get_running_loop().time() + delay,
            min(delay * 2, 60.0),
        )
        if (
            pending
            and evaluation_generation is not None
            and evaluation_generation != self._evaluation_generation(symbol)
        ):
            self._wake_sync_waits((symbol,))
        log.warning(
            "live_exit_evaluation_deferred" if pending else "live_exit_retry_scheduled",
            symbol=symbol,
            channel=channel,
            reason=failure,
            retry_delay_seconds=delay,
        )

    def note_account_facts_changed(
        self,
        symbols: Collection[str] | None = None,
    ) -> None:
        """Wake fact waits after publication; preserve network-failure backoff."""
        if symbols is None:
            self._facts_generation += 1
        else:
            for symbol in symbols:
                self._symbol_facts_generation[symbol] = (
                    self._symbol_facts_generation.get(symbol, 0) + 1
                )
        self._candle_facts_changed.set()
        self._wake_sync_waits(symbols)

    def _evaluation_generation(self, symbol: str) -> tuple[int, int]:
        return self._facts_generation, self._symbol_facts_generation.get(symbol, 0)

    def _wake_sync_waits(self, symbols: Collection[str] | None) -> None:
        requested = None if symbols is None else frozenset(symbols)
        for channel, retries in (
            ("quote", self._quote_retries),
            ("grace", self._grace_retries),
            ("candle", self._candle_retries),
        ):
            waiting = self._sync_waits[channel]
            ready = set(waiting) if requested is None else waiting & requested
            for symbol in ready:
                retries.pop(symbol, None)
                waiting.discard(symbol)
            if channel == "quote" and ready:
                self._quote_ready.update(ready)
                self._quote_facts_changed.set()
            elif channel == "grace" and ready:
                self._grace_facts_changed.set()
            elif channel == "candle" and ready:
                self._candle_ready.update(ready)
                self._candle_facts_changed.set()

    async def run_closed_candle_channel(
        self,
        *,
        source: AsyncIterable[ClosedCandle15mEvent],
    ) -> None:
        iterator = aiter(source)
        next_event: asyncio.Future[ClosedCandle15mEvent] | None = asyncio.ensure_future(
            anext(iterator)
        )
        changed = asyncio.create_task(self._candle_facts_changed.wait())
        try:
            while next_event is not None or self._pending_candles:
                loop_time = asyncio.get_running_loop().time()
                earliest_retry: float | None = None
                for symbol, (retry_time, _) in self._candle_retries.items():
                    if any(k[0] == symbol for k in self._pending_candles):
                        if earliest_retry is None or retry_time < earliest_retry:
                            earliest_retry = retry_time

                timeout: float | None = None
                if earliest_retry is not None:
                    timeout = max(0.0, earliest_retry - loop_time)
                if self._closed_candle_expires_at is not None:
                    for event in self._pending_candles.values():
                        expires_at = self._closed_candle_expires_at(event)
                        if expires_at is not None:
                            remaining = max(
                                0.0, (expires_at - self._clock()).total_seconds()
                            )
                            timeout = (
                                remaining
                                if timeout is None
                                else min(timeout, remaining)
                            )

                waiting: set[
                    asyncio.Future[ClosedCandle15mEvent]
                    | asyncio.Task[bool]
                    | asyncio.Task[None]
                ] = {changed}
                if next_event is not None:
                    waiting.add(next_event)

                timer_task: asyncio.Task[None] | None = None
                if timeout is not None:
                    timer_task = asyncio.create_task(asyncio.sleep(timeout))
                    waiting.add(timer_task)

                try:
                    done, _ = await asyncio.wait(
                        waiting, return_when=asyncio.FIRST_COMPLETED
                    )
                finally:
                    if timer_task is not None:
                        timer_task.cancel()

                evaluate_keys: list[tuple[str, datetime]] = []

                if changed in done:
                    self._candle_facts_changed.clear()
                    changed = asyncio.create_task(self._candle_facts_changed.wait())
                    self._candle_ready.clear()

                if next_event is not None and next_event in done:
                    try:
                        event = next_event.result()
                    except StopAsyncIteration:
                        next_event = None
                    else:
                        next_event = asyncio.ensure_future(anext(iterator))
                        key = (event.candle.symbol, event.candle.candle_start)
                        if (
                            key not in self._evaluated_candles
                            and key not in self._pending_candles
                        ):
                            if len(self._pending_candles) >= _MAX_RETAINED_CANDLES:
                                raise RuntimeError(
                                    "unevaluated closed-candle capacity exceeded"
                                )
                            self._pending_candles[key] = event
                            evaluate_keys.append(key)

                loop_time = asyncio.get_running_loop().time()
                for key in sorted(
                    self._pending_candles, key=lambda k: (k[1], k[0])
                ):
                    symbol = key[0]
                    event = self._pending_candles[key]
                    expires_at = (
                        self._closed_candle_expires_at(event)
                        if self._closed_candle_expires_at is not None
                        else None
                    )
                    if expires_at is not None and self._clock() >= expires_at:
                        if key not in evaluate_keys:
                            evaluate_keys.append(key)
                        continue
                    if symbol in self._sync_waits["candle"]:
                        continue
                    if symbol in self._candle_retries:
                        if (
                            loop_time >= self._candle_retries[symbol][0]
                            and key not in evaluate_keys
                        ):
                            evaluate_keys.append(key)
                    elif key not in evaluate_keys:
                        evaluate_keys.append(key)

                for key in evaluate_keys:
                    await self._evaluate_candle(key)
        finally:
            tasks = [changed] + ([] if next_event is None else [next_event])
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _evaluate_candle(self, key: tuple[str, datetime]) -> None:
        if key not in self._pending_candles:
            return
        event = self._pending_candles[key]
        failure: str | None = None
        loop_time = asyncio.get_running_loop().time()
        generation = self._evaluation_generation(event.candle.symbol)
        for attempt in range(3):
            try:
                expires_at = (
                    self._closed_candle_expires_at(event)
                    if self._closed_candle_expires_at is not None
                    else None
                )
                if expires_at is not None and self._clock() >= expires_at:
                    failure = f"closed_candle_evaluation_expired:{event.candle.symbol}"
                    break
                failure = await self._daemon.process_closed_candle(
                    event,
                    latest_quote=next(
                        iter(
                            self._latest_market_quotes.for_symbols(
                                (event.candle.symbol,)
                            )
                        ),
                        None,
                    ),
                )
                if is_pending_context_refresh(failure) and attempt < 2:
                    # A projection fence invalidated the context. Reevaluate
                    # the original event, never the old order allocation.
                    continue
                break
            except Exception as error:
                if self._is_order_identity_conflict(error):
                    failure = ORDER_IDENTITY_CONFLICT_REASON
                    if self._on_order_identity_conflict is not None:
                        self._on_order_identity_conflict(event.candle.symbol)
                    break
                if not self._is_transient_error(error) or attempt == 2:
                    raise
                delay = self._candle_retries.get(event.candle.symbol, (0.0, 1.0))[1]
                self._candle_retries[event.candle.symbol] = (
                    loop_time + delay,
                    min(delay * 2, 60.0),
                )
                log.warning(
                    "live_closed_candle_retry_scheduled",
                    symbol=event.candle.symbol,
                    reason=type(error).__name__,
                    retry_delay_seconds=delay,
                )
                return

        if failure is None:
            del self._pending_candles[key]
            self._evaluated_candles[key] = None
            self._candle_retries.pop(event.candle.symbol, None)
            self._sync_waits["candle"].discard(event.candle.symbol)
            if len(self._evaluated_candles) > _MAX_RETAINED_CANDLES:
                del self._evaluated_candles[next(iter(self._evaluated_candles))]
            if self._on_exit_failure is not None and not any(
                pending[0] == event.candle.symbol for pending in self._pending_candles
            ):
                self._on_exit_failure(event.candle.symbol, None)
        elif (
            is_pending_exit_evaluation(failure)
            or failure == ORDER_IDENTITY_CONFLICT_REASON
        ):
            self._sync_waits["candle"].add(event.candle.symbol)
            self._candle_retries.pop(event.candle.symbol, None)
            if (
                failure == ORDER_IDENTITY_CONFLICT_REASON
                and self._on_exit_failure is not None
            ):
                self._on_exit_failure(event.candle.symbol, failure)
            if generation != self._evaluation_generation(event.candle.symbol):
                self._wake_sync_waits((event.candle.symbol,))
            log.warning(
                "live_closed_candle_position_sync_pending"
                if failure != ORDER_IDENTITY_CONFLICT_REASON
                else "live_closed_candle_exit_degraded",
                symbol=event.candle.symbol,
                reason=failure,
            )
        elif failure.startswith("closed_candle_evaluation_expired"):
            del self._pending_candles[key]
            self._evaluated_candles[key] = None
            self._candle_retries.pop(event.candle.symbol, None)
            self._sync_waits["candle"].discard(event.candle.symbol)
            if len(self._evaluated_candles) > _MAX_RETAINED_CANDLES:
                del self._evaluated_candles[next(iter(self._evaluated_candles))]
            if self._on_exit_failure is not None:
                self._on_exit_failure(event.candle.symbol, failure)
            log.warning(
                "live_closed_candle_evaluation_expired",
                symbol=event.candle.symbol,
                reason=failure,
            )
        else:
            if self._on_exit_failure is not None:
                self._on_exit_failure(event.candle.symbol, failure)
            delay = self._candle_retries.get(event.candle.symbol, (0.0, 1.0))[1]
            self._candle_retries[event.candle.symbol] = (
                loop_time + delay,
                min(delay * 2, 60.0),
            )
            log.error(
                "live_closed_candle_exit_degraded",
                symbol=event.candle.symbol,
                reason=failure,
            )

    async def run_grace_timeout_channel(self, *, interval_seconds: float = 1.0) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        retries = self._grace_retries
        while True:
            now = self._clock()
            loop_time = asyncio.get_running_loop().time()
            managed_symbols = self._daemon.managed_position_symbols
            for symbol in tuple(retries):
                if symbol not in managed_symbols:
                    self._clear_retry(symbol, retries)
            for symbol in sorted(managed_symbols):
                if loop_time < retries.get(symbol, (0.0, 1.0))[0]:
                    continue
                quote = next(
                    iter(self._latest_market_quotes.for_symbols((symbol,))), None
                )
                states = self._latest_market_states.for_symbols((symbol,))
                if not states:
                    # A recovered/open position can outlive its market-state
                    # subscription. The wall-clock exit still owns its deadline,
                    # so give the processor a symbol-scoped execution context.
                    states = (_grace_timeout_market_state(symbol, now, quote),)
                for state in states:
                    generation = self._evaluation_generation(symbol)
                    try:
                        failure = await self._daemon.process_grace_timeout(
                            state,
                            now=now,
                            latest_quote=quote,
                        )
                    except Exception as error:
                        if self._is_order_identity_conflict(error):
                            failure = ORDER_IDENTITY_CONFLICT_REASON
                            if self._on_order_identity_conflict is not None:
                                self._on_order_identity_conflict(symbol)
                            self._record_result(
                                symbol, failure, retries, channel="grace"
                            )
                            continue
                        if not self._is_transient_error(error):
                            raise
                        log.warning(
                            "live_grace_timeout_processing_degraded",
                            symbol=symbol,
                            error_type=type(error).__name__,
                        )
                        continue
                    self._record_result(
                        symbol,
                        failure,
                        retries,
                        channel="grace",
                        evaluation_generation=generation,
                    )
            sleeping = asyncio.create_task(asyncio.sleep(interval_seconds))
            changed = asyncio.create_task(self._grace_facts_changed.wait())
            try:
                done, _ = await asyncio.wait(
                    {sleeping, changed}, return_when=asyncio.FIRST_COMPLETED
                )
                for task in done:
                    task.result()
                self._grace_facts_changed.clear()
            finally:
                sleeping.cancel()
                changed.cancel()
                await asyncio.gather(sleeping, changed, return_exceptions=True)


def _grace_timeout_market_state(
    symbol: str,
    now: datetime,
    quote: RealtimeMarketQuote | None,
) -> MarketState15s:
    bucket_end = now.replace(
        second=now.second - now.second % 15,
        microsecond=0,
    )
    bid = quote.bid_price if quote is not None else None
    ask = quote.ask_price if quote is not None else None
    midpoint = (
        (bid + ask) / Decimal("2")
        if bid is not None and ask is not None
        else None
    )
    return MarketState15s(
        schema_version=1,
        exchange=quote.exchange if quote is not None else "binance-usdm",
        environment=quote.environment if quote is not None else "live",
        symbol=symbol,
        bucket_start=bucket_end - timedelta(seconds=15),
        bucket_end=bucket_end,
        open_price=None,
        high_price=None,
        low_price=None,
        close_price=None,
        trade_count=0,
        trade_notional=Decimal("0"),
        aggressive_buy_notional=Decimal("0"),
        aggressive_sell_notional=Decimal("0"),
        last_bid_price=bid,
        last_ask_price=ask,
        spread=ask - bid if bid is not None and ask is not None else None,
        midpoint=midpoint,
        liquidation_count=0,
        liquidation_notional=Decimal("0"),
        mark_price=midpoint,
        closed_kline_count=0,
        source_event_count=0,
        first_received_at=None,
        last_received_at=None,
        data_complete=False,
    )


__all__ = [
    "LiveExitChannelRuntime",
]
