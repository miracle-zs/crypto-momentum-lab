"""Runtime loops for the independent reduce-only exit channels."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import structlog

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
        self._candle_facts_changed = asyncio.Event()
        self._pending_candles: dict[tuple[str, datetime], ClosedCandle15mEvent] = {}
        self._evaluated_candles: dict[tuple[str, datetime], None] = {}


    async def run_quote_channel(
        self,
        *,
        source: AsyncIterable[RealtimeMarketQuote],
    ) -> None:
        retries: dict[str, tuple[float, float]] = {}
        async for quote in source:
            loop_time = asyncio.get_running_loop().time()
            managed_symbols = self._daemon.managed_position_symbols
            self._latest_market_quotes.observe(quote)
            if quote.symbol not in managed_symbols:
                self._clear_retry(quote.symbol, retries)
            if loop_time < retries.get(quote.symbol, (0.0, 1.0))[0]:
                continue
            for state in self._latest_market_states.for_symbols((quote.symbol,)):
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
                self._record_result(quote.symbol, failure, retries, channel="quote")

    def _clear_retry(self, symbol: str, retries: dict[str, tuple[float, float]]) -> None:
        if retries.pop(symbol, None) is not None and self._on_exit_failure is not None:
            self._on_exit_failure(symbol, None)

    def _record_result(
        self, symbol: str, failure: str | None,
        retries: dict[str, tuple[float, float]], *, channel: str,
    ) -> None:
        if is_pending_context_refresh(failure):
            return
        if failure is None:
            retries.pop(symbol, None)
            if self._on_exit_failure is not None:
                self._on_exit_failure(symbol, None)
            return
        pending = is_pending_exit_evaluation(failure)
        if not pending and self._on_exit_failure is not None:
            self._on_exit_failure(symbol, failure)
        delay = retries.get(symbol, (0.0, 1.0))[1]
        retries[symbol] = (asyncio.get_running_loop().time() + delay, min(delay * 2, 60.0))
        log.warning("live_exit_evaluation_deferred" if pending else "live_exit_retry_scheduled",
                    symbol=symbol, channel=channel, reason=failure, retry_delay_seconds=delay)

    def note_account_facts_changed(self) -> None:
        """Wake candle evaluation only after committed account facts are published."""
        self._candle_facts_changed.set()

    async def run_closed_candle_channel(
        self,
        *,
        source: AsyncIterable[ClosedCandle15mEvent],
    ) -> None:
        iterator = aiter(source)
        next_event: asyncio.Future[ClosedCandle15mEvent] | None = asyncio.ensure_future(anext(iterator))
        changed = asyncio.create_task(self._candle_facts_changed.wait())
        try:
            while next_event is not None or self._pending_candles:
                waiting: set[asyncio.Future[ClosedCandle15mEvent] | asyncio.Task[bool]] = {changed}
                if next_event is not None:
                    waiting.add(next_event)
                done, _ = await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED)
                if changed in done:
                    self._candle_facts_changed.clear()
                    changed = asyncio.create_task(self._candle_facts_changed.wait())
                    for key in sorted(self._pending_candles, key=lambda k: (k[1], k[0])):
                        await self._evaluate_candle(key)
                if next_event is not None and next_event in done:
                    try:
                        event = next_event.result()
                    except StopAsyncIteration:
                        next_event = None
                    else:
                        next_event = asyncio.ensure_future(anext(iterator))
                        key = (event.candle.symbol, event.candle.candle_start)
                        if key in self._evaluated_candles or key in self._pending_candles:
                            continue
                        # Bounded retention fails explicitly instead of discarding
                        # an unevaluated official closing event.
                        if len(self._pending_candles) >= _MAX_RETAINED_CANDLES:
                            raise RuntimeError("unevaluated closed-candle capacity exceeded")
                        self._pending_candles[key] = event
                        await self._evaluate_candle(key)
        finally:
            tasks = [changed] + ([] if next_event is None else [next_event])
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _evaluate_candle(self, key: tuple[str, datetime]) -> None:
        event = self._pending_candles[key]
        failure: str | None = None
        for attempt in range(3):
            try:
                failure = await self._daemon.process_closed_candle(
                    event,
                    latest_quote=next(iter(self._latest_market_quotes.for_symbols(
                        (event.candle.symbol,)
                    )), None),
                )
                if is_pending_context_refresh(failure) and attempt < 2:
                    # A projection fence invalidated the context. Reevaluate
                    # the original event, never the old order allocation.
                    continue
                break
            except asyncio.CancelledError:
                raise
            except Exception as error:
                if self._is_order_identity_conflict(error):
                    failure = ORDER_IDENTITY_CONFLICT_REASON
                    if self._on_order_identity_conflict is not None:
                        self._on_order_identity_conflict(event.candle.symbol)
                    break
                if not self._is_transient_error(error) or attempt == 2:
                    raise
                await asyncio.sleep(float(2**attempt))
        if failure is None:
            del self._pending_candles[key]
            self._evaluated_candles[key] = None
            if len(self._evaluated_candles) > _MAX_RETAINED_CANDLES:
                del self._evaluated_candles[next(iter(self._evaluated_candles))]
            if self._on_exit_failure is not None and not any(
                pending[0] == event.candle.symbol for pending in self._pending_candles
            ):
                self._on_exit_failure(event.candle.symbol, None)
        elif is_pending_exit_evaluation(failure):
            log.warning("live_closed_candle_position_sync_pending",
                        symbol=event.candle.symbol, reason=failure)
        else:
            if self._on_exit_failure is not None:
                self._on_exit_failure(event.candle.symbol, failure)
            log.error("live_closed_candle_exit_degraded",
                      symbol=event.candle.symbol, reason=failure)

    async def run_grace_timeout_channel(self, *, interval_seconds: float = 1.0) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        retries: dict[str, tuple[float, float]] = {}
        while True:
            now = datetime.now(tz=UTC)
            loop_time = asyncio.get_running_loop().time()
            managed_symbols = self._daemon.managed_position_symbols
            for symbol in tuple(retries):
                if symbol not in managed_symbols:
                    self._clear_retry(symbol, retries)
            for state in self._latest_market_states.for_symbols(
                tuple(sorted(managed_symbols))
            ):
                if loop_time < retries.get(state.symbol, (0.0, 1.0))[0]:
                    continue
                quote = next(
                    iter(self._latest_market_quotes.for_symbols((state.symbol,))),
                    None,
                )
                try:
                    failure = await self._daemon.process_grace_timeout(
                        state,
                        now=now,
                        latest_quote=quote,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    if self._is_order_identity_conflict(error):
                        failure = ORDER_IDENTITY_CONFLICT_REASON
                        if self._on_order_identity_conflict is not None:
                            self._on_order_identity_conflict(state.symbol)
                        self._record_result(state.symbol, failure, retries, channel="grace")
                        continue
                    if not self._is_transient_error(error):
                        raise
                    log.warning(
                        "live_grace_timeout_processing_degraded",
                        symbol=state.symbol,
                        error_type=type(error).__name__,
                    )
                    continue
                self._record_result(state.symbol, failure, retries, channel="grace")
            await asyncio.sleep(interval_seconds)


__all__ = [
    "LiveExitChannelRuntime",
]
