"""Runtime loops for the independent reduce-only exit channels."""

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime

import structlog

from crypto_momentum_lab.live_rollout.closed_candle_feed import (
    BinanceClosedCandle15mFeed,
)
from crypto_momentum_lab.live_rollout.daemon import LiveStrategyDaemon
from crypto_momentum_lab.live_rollout.market_cache import (
    LatestMarketQuoteCache,
    LatestMarketStateCache,
)
from crypto_momentum_lab.market_data.quote_hub import WebSocketMarketQuoteSource

log = structlog.get_logger()

DEFAULT_PENDING_POSITION_RETRY_DELAYS_SECONDS = (
    0.25,
    0.5,
    1.0,
    2.0,
    4.0,
    8.0,
    16.0,
    32.0,
)
ORDER_IDENTITY_CONFLICT_REASON = "order_identity_conflict"


def _never_order_identity_conflict(_error: Exception) -> bool:
    return False


def is_pending_position_sync_failure(failure: str | None) -> bool:
    return bool(
        failure is not None
        and failure.startswith("pending_live_positions:")
    )


def promote_pending_position_failure(failure: str) -> str:
    if not is_pending_position_sync_failure(failure):
        return failure
    return failure.replace(
        "pending_live_positions:",
        "unmanaged_live_positions:",
        1,
    )


class LiveExitChannelRuntime:
    """Own retry and backoff policy for quote, candle, and grace exits."""

    def __init__(
        self,
        *,
        daemon: LiveStrategyDaemon,
        latest_market_quotes: LatestMarketQuoteCache,
        latest_market_states: LatestMarketStateCache,
        is_transient_error: Callable[[Exception], bool],
        is_order_identity_conflict: Callable[[Exception], bool] | None = None,
        on_exit_failure: Callable[[str, str | None], None] | None = None,
        pending_position_retry_delays: tuple[float, ...] = (
            DEFAULT_PENDING_POSITION_RETRY_DELAYS_SECONDS
        ),
    ) -> None:
        if not pending_position_retry_delays:
            raise ValueError("pending_position_retry_delays must not be empty")
        if any(delay <= 0 for delay in pending_position_retry_delays):
            raise ValueError("pending position retry delays must be positive")
        self._daemon = daemon
        self._latest_market_quotes = latest_market_quotes
        self._latest_market_states = latest_market_states
        self._is_transient_error = is_transient_error
        self._is_order_identity_conflict = (
            is_order_identity_conflict or _never_order_identity_conflict
        )
        self._on_exit_failure = on_exit_failure
        self._pending_position_retry_delays = pending_position_retry_delays

    async def run_quote_channel(
        self,
        *,
        source: WebSocketMarketQuoteSource,
    ) -> None:
        retry_at_by_symbol: dict[str, float] = {}
        retry_delay_by_symbol: dict[str, float] = {}
        async for quote in source:
            loop_time = asyncio.get_running_loop().time()
            if loop_time < retry_at_by_symbol.get(quote.symbol, 0.0):
                continue
            self._latest_market_quotes.observe(quote)
            for state in self._latest_market_states.for_symbols((quote.symbol,)):
                try:
                    failure = await self._daemon.process_market_quote(quote, state)
                except Exception as error:
                    order_identity_conflict = self._is_order_identity_conflict(
                        error
                    )
                    if not (
                        self._is_transient_error(error)
                        or order_identity_conflict
                    ):
                        raise
                    failure = (
                        ORDER_IDENTITY_CONFLICT_REASON
                        if order_identity_conflict
                        else type(error).__name__
                    )
                    if (
                        order_identity_conflict
                        and self._on_exit_failure is not None
                    ):
                        self._on_exit_failure(quote.symbol, failure)
                    log.warning(
                        "live_market_quote_processing_degraded",
                        symbol=quote.symbol,
                        error_type=type(error).__name__,
                        reason=failure,
                    )
                    continue
                if failure is not None:
                    pending_position_sync = is_pending_position_sync_failure(
                        failure
                    )
                    if (
                        not pending_position_sync
                        and self._on_exit_failure is not None
                    ):
                        self._on_exit_failure(quote.symbol, failure)
                    if pending_position_sync:
                        log.warning(
                            "live_market_quote_position_sync_pending",
                            symbol=quote.symbol,
                            reason=failure,
                        )
                    delay = min(
                        retry_delay_by_symbol.get(quote.symbol, 1.0),
                        60.0,
                    )
                    retry_delay_by_symbol[quote.symbol] = min(delay * 2, 60.0)
                    retry_at_by_symbol[quote.symbol] = (
                        asyncio.get_running_loop().time() + delay
                    )
                    log.error(
                        "live_market_quote_exit_retry_scheduled",
                        symbol=quote.symbol,
                        reason=failure,
                        retry_delay_seconds=delay,
                    )
                    continue
                retry_at_by_symbol.pop(quote.symbol, None)
                retry_delay_by_symbol.pop(quote.symbol, None)
                if self._on_exit_failure is not None:
                    self._on_exit_failure(quote.symbol, None)

    async def run_closed_candle_channel(
        self,
        *,
        source: BinanceClosedCandle15mFeed,
    ) -> None:
        async for event in source:
            quote = next(
                iter(
                    self._latest_market_quotes.for_symbols((event.candle.symbol,))
                ),
                None,
            )
            failure: str | None = None
            for attempt in range(3):
                try:
                    failure = await self._daemon.process_closed_candle(
                        event,
                        latest_quote=quote,
                    )
                    break
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    if self._is_order_identity_conflict(error):
                        failure = ORDER_IDENTITY_CONFLICT_REASON
                        break
                    if not self._is_transient_error(error):
                        raise
                    if attempt == 2:
                        raise
                    delay_seconds = float(2**attempt)
                    log.warning(
                        "live_closed_candle_processing_retry",
                        symbol=event.candle.symbol,
                        candle_start=event.candle.candle_start.isoformat(),
                        attempt=attempt + 1,
                        retry_delay_seconds=delay_seconds,
                        error_type=type(error).__name__,
                    )
                    await asyncio.sleep(delay_seconds)
            if failure is not None:
                if is_pending_position_sync_failure(failure):
                    for attempt, delay in enumerate(
                        self._pending_position_retry_delays,
                        start=1,
                    ):
                        log.warning(
                            "live_closed_candle_position_sync_retry",
                            symbol=event.candle.symbol,
                            attempt=attempt,
                            delay_seconds=delay,
                            reason=failure,
                        )
                        await asyncio.sleep(delay)
                        try:
                            failure = await self._daemon.process_closed_candle(
                                event,
                                latest_quote=quote,
                            )
                        except asyncio.CancelledError:
                            raise
                        except Exception as error:
                            if not self._is_transient_error(error):
                                raise
                            log.warning(
                                "live_closed_candle_position_sync_degraded",
                                symbol=event.candle.symbol,
                                attempt=attempt,
                                error_type=type(error).__name__,
                            )
                            continue
                        if not is_pending_position_sync_failure(failure):
                            break
                    failure = (
                        promote_pending_position_failure(failure)
                        if failure is not None
                        else None
                    )
                if failure is not None and self._on_exit_failure is not None:
                    self._on_exit_failure(event.candle.symbol, failure)
                log.error(
                    "live_closed_candle_exit_degraded",
                    symbol=event.candle.symbol,
                    reason=failure,
                )
                continue
            if self._on_exit_failure is not None:
                self._on_exit_failure(event.candle.symbol, None)

    async def run_grace_timeout_channel(self, *, interval_seconds: float = 1.0) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        retry_at_by_symbol: dict[str, float] = {}
        retry_delay_by_symbol: dict[str, float] = {}
        while True:
            now = datetime.now(tz=UTC)
            loop_time = asyncio.get_running_loop().time()
            for state in self._latest_market_states.for_symbols(
                tuple(sorted(self._daemon.managed_position_symbols))
            ):
                if loop_time < retry_at_by_symbol.get(state.symbol, 0.0):
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
                        if self._on_exit_failure is not None:
                            self._on_exit_failure(state.symbol, failure)
                        retry_delay = min(
                            retry_delay_by_symbol.get(state.symbol, 1.0),
                            60.0,
                        )
                        retry_delay_by_symbol[state.symbol] = min(
                            retry_delay * 2,
                            60.0,
                        )
                        retry_at_by_symbol[state.symbol] = (
                            asyncio.get_running_loop().time() + retry_delay
                        )
                        log.warning(
                            "live_grace_timeout_processing_degraded",
                            symbol=state.symbol,
                            error_type=type(error).__name__,
                            reason=failure,
                            retry_delay_seconds=retry_delay,
                        )
                        continue
                    if not self._is_transient_error(error):
                        raise
                    log.warning(
                        "live_grace_timeout_processing_degraded",
                        symbol=state.symbol,
                        error_type=type(error).__name__,
                    )
                    continue
                if failure is not None:
                    pending_position_sync = is_pending_position_sync_failure(
                        failure
                    )
                    if (
                        not pending_position_sync
                        and self._on_exit_failure is not None
                    ):
                        self._on_exit_failure(state.symbol, failure)
                    if pending_position_sync:
                        log.warning(
                            "live_grace_timeout_position_sync_pending",
                            symbol=state.symbol,
                            reason=failure,
                        )
                    delay = min(
                        retry_delay_by_symbol.get(state.symbol, 1.0),
                        60.0,
                    )
                    retry_delay_by_symbol[state.symbol] = min(delay * 2, 60.0)
                    retry_at_by_symbol[state.symbol] = (
                        asyncio.get_running_loop().time() + delay
                    )
                    log.error(
                        "live_grace_timeout_exit_retry_scheduled",
                        symbol=state.symbol,
                        reason=failure,
                        retry_delay_seconds=delay,
                    )
                    continue
                retry_at_by_symbol.pop(state.symbol, None)
                retry_delay_by_symbol.pop(state.symbol, None)
                if self._on_exit_failure is not None:
                    self._on_exit_failure(state.symbol, None)
            await asyncio.sleep(interval_seconds)


__all__ = [
    "DEFAULT_PENDING_POSITION_RETRY_DELAYS_SECONDS",
    "ORDER_IDENTITY_CONFLICT_REASON",
    "LiveExitChannelRuntime",
    "is_pending_position_sync_failure",
    "promote_pending_position_failure",
]
