"""Market pipeline and background channel assembly for live rollout.

This module owns the assembly and lifecycle registration of market data
producers: 24h quote volume, 15m closed candle feeds, startup market state
buffering, and multi-channel WebSocket sources.
"""

from __future__ import annotations

import asyncio
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
)
from dataclasses import dataclass
from datetime import UTC, datetime

import structlog

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.market.runtime_state_models import RuntimeStateCursor
from crypto_momentum_lab.domain.strategy.position_exit import PositionExitMode
from crypto_momentum_lab.domain.strategy.runtime import RuntimeStrategy
from crypto_momentum_lab.execution_account.hub import (
    WebSocketAccountEventSource,
)
from crypto_momentum_lab.execution_account.risk_control_hub import (
    RiskControlEvent,
    WebSocketRiskControlSource,
)
from crypto_momentum_lab.live_rollout.closed_candle_feed import (
    BinanceClosedCandle15mFeed,
    ClosedCandle15mFeedConfig,
)
from crypto_momentum_lab.live_rollout.hub_cursor import LiveHubCursorState
from crypto_momentum_lab.live_rollout.market_cache import LatestMarketStateCache
from crypto_momentum_lab.live_rollout.market_timing import LiveMarketTimingTracker
from crypto_momentum_lab.live_rollout.postgres_runtime import poll_live_market_states
from crypto_momentum_lab.live_rollout.runtime_config import _LIVE_STARTUP_BUFFER_LIMIT
from crypto_momentum_lab.live_rollout.runtime_session import ResourceOwnershipRegistry
from crypto_momentum_lab.live_rollout.startup_market_buffer import (
    StartupMarketStateBuffer,
)
from crypto_momentum_lab.live_rollout.startup_recovery import (
    strategy_last_processed_at_by_symbol,
)
from crypto_momentum_lab.live_rollout.stream_recovery import (
    resilient_market_state_stream,
    resilient_risk_control_stream,
)
from crypto_momentum_lab.live_rollout.volume import WebSocketQuoteVolumeProvider
from crypto_momentum_lab.market_data.candle_source import (
    BinanceRestClosedCandle15mSource,
    ClosedCandleEmaProvider,
)
from crypto_momentum_lab.market_data.hub import (
    MarketStateBatch,
    WebSocketMarketStateSource,
)
from crypto_momentum_lab.market_data.quote_hub import (
    WebSocketMarketQuoteSource,
    WebSocketMarketQuoteVolumeSource,
)
from crypto_momentum_lab.persistence.postgres.runtime_state_repository import (
    PostgresRuntimeMarketStateRepository,
)

log = structlog.get_logger()


@dataclass(frozen=True, slots=True)
class LiveCandleAssembly:
    """15-minute closed candle feeds and EMA calculation providers."""

    candle_source: BinanceRestClosedCandle15mSource | None
    closed_candle_feed: BinanceClosedCandle15mFeed | None
    ema_provider: ClosedCandleEmaProvider | None


class LiveStartupMarketAssembly:
    """Startup market state buffer, producer task, and connection observer."""

    def __init__(
        self,
        *,
        buffer: StartupMarketStateBuffer | None,
        hub_source: WebSocketMarketStateSource | None,
        task: asyncio.Task[None] | None,
        timing_tracker: LiveMarketTimingTracker | None = None,
    ) -> None:
        self.buffer = buffer
        self.hub_source = hub_source
        self.task = task
        self.timing_tracker = timing_tracker
        self._control_plane_listener: (
            Callable[[bool, str | None], None] | None
        ) = None

    def set_control_plane_listener(
        self,
        listener: Callable[[bool, str | None], None] | None,
    ) -> None:
        self._control_plane_listener = listener

    def on_connection_change(self, available: bool, reason: str | None) -> None:
        if self.buffer is not None:
            self.buffer.observe_connection_change(available, reason)
        if self._control_plane_listener is not None:
            self._control_plane_listener(available, reason)


@dataclass(slots=True)
class LiveChannelSources:
    """Active background WebSocket sources for live runtime channels."""

    hub_source: WebSocketMarketStateSource | None
    quote_source: WebSocketMarketQuoteSource | None
    account_source: WebSocketAccountEventSource
    risk_control_source: WebSocketRiskControlSource | None

    def stop_all(self) -> None:
        """Stop all instantiated background sources in fail-safe order."""
        if self.hub_source is not None:
            self.hub_source.stop()
        if self.quote_source is not None:
            self.quote_source.stop()
        self.account_source.stop()
        if self.risk_control_source is not None:
            self.risk_control_source.stop()


async def assemble_live_quote_volume(
    *,
    market_quote_volume_hub_url: str,
    market_environment: str,
    session_id: str,
    ownership_registry: ResourceOwnershipRegistry,
) -> WebSocketQuoteVolumeProvider:
    """Assemble and start the 24-hour quote volume cache provider."""
    volume_source = WebSocketMarketQuoteVolumeSource(
        url=market_quote_volume_hub_url,
        environment=market_environment,
        consumer_id=f"live-volume:{session_id}",
    )
    volume_cache = WebSocketQuoteVolumeProvider(volume_source)
    ownership_registry.register("volume_cache", volume_cache.stop)
    try:
        await volume_cache.start()
    except Exception:
        try:
            await volume_cache.stop()
        except Exception:
            pass
        raise
    return volume_cache


def assemble_live_candle_sources(
    *,
    exit_mode: PositionExitMode,
    base_url: str,
    market_websocket_url: str,
    market_environment: str,
    session_id: str,
    require_price_above_ema5: bool,
    require_price_above_ema10: bool,
    ownership_registry: ResourceOwnershipRegistry,
) -> LiveCandleAssembly:
    """Assemble 15m candle feeds and EMA calculation providers."""
    candle_source: BinanceRestClosedCandle15mSource | None = None
    closed_candle_feed: BinanceClosedCandle15mFeed | None = None
    ema_provider: ClosedCandleEmaProvider | None = None

    if exit_mode is PositionExitMode.CANDLE_15M:
        candle_source = BinanceRestClosedCandle15mSource(base_url)
        ownership_registry.register("candle_source", candle_source.close)
        closed_candle_feed = BinanceClosedCandle15mFeed(
            config=ClosedCandle15mFeedConfig(
                websocket_url=market_websocket_url,
                environment=market_environment,
                consumer_id=f"live-exit-candles:{session_id}",
            ),
            backfill_source=candle_source,
        )
        ownership_registry.register(
            "closed_candle_feed", closed_candle_feed.stop
        )

    if require_price_above_ema5 or require_price_above_ema10:
        if candle_source is None:
            candle_source = BinanceRestClosedCandle15mSource(base_url)
            ownership_registry.register("candle_source", candle_source.close)
        ema_provider = ClosedCandleEmaProvider(candle_source)

    return LiveCandleAssembly(
        candle_source=candle_source,
        closed_candle_feed=closed_candle_feed,
        ema_provider=ema_provider,
    )


def assemble_live_startup_market_buffer(
    *,
    market_state_source: str,
    market_state_hub_url: str,
    market_environment: str,
    session_id: str,
    hub_cursor_state: LiveHubCursorState,
    max_states: int = _LIVE_STARTUP_BUFFER_LIMIT,
) -> LiveStartupMarketAssembly:
    """Assemble the startup market buffer and Hub consumer task."""
    if market_state_source != "hub":
        return LiveStartupMarketAssembly(
            buffer=None,
            hub_source=None,
            task=None,
        )

    buffer = StartupMarketStateBuffer(
        max_states=max_states,
        on_state_skipped=hub_cursor_state.acknowledge_state,
    )
    timing_tracker = LiveMarketTimingTracker()
    assembly = LiveStartupMarketAssembly(
        buffer=buffer,
        hub_source=None,
        task=None,
        timing_tracker=timing_tracker,
    )

    def observe_batch(batch: MarketStateBatch) -> None:
        # Keep cursor acknowledgement and diagnostic correlation independent:
        # neither becomes a prerequisite for market-state delivery.
        hub_cursor_state.observe_batch(batch)
        timing_tracker.observe_batch(
            batch,
            received_at=datetime.now(tz=UTC),
        )

    hub_source = WebSocketMarketStateSource(
        url=market_state_hub_url,
        environment=market_environment,
        consumer_id=f"live-strategy:{session_id}",
        on_connection_change=assembly.on_connection_change,
        on_batch=observe_batch,
        fail_on_replay_unavailable=True,
        preserve_sequence_on_overflow=True,
    )
    if hub_cursor_state.has_cursor:
        hub_source.set_resume_cursor(
            stream_id=hub_cursor_state.stream_id,
            sequence=hub_cursor_state.sequence,
        )

    task = asyncio.create_task(
        collect_startup_market_states(
            source=hub_source,
            buffer=buffer,
        ),
        name=f"live-startup-market-buffer:{session_id}",
    )
    assembly.hub_source = hub_source
    assembly.task = task
    return assembly


def build_live_market_state_stream(
    *,
    market_state_source: str,
    startup_market_buffer: StartupMarketStateBuffer | None,
    strategy: RuntimeStrategy,
    state_repository: PostgresRuntimeMarketStateRepository,
    market_environment: str,
    max_runtime_seconds: float,
    poll_interval_seconds: float,
    market_cursor: RuntimeStateCursor | None,
) -> AsyncIterable[MarketState15s]:
    """Build the market state stream (Hub buffer replay or DB poll)."""
    if market_state_source == "hub" and startup_market_buffer is not None:
        return startup_market_buffer.stream(
            skip_through=strategy_last_processed_at_by_symbol(strategy)
        )
    return poll_live_market_states(
        repository=state_repository,
        environment=market_environment,
        max_runtime_seconds=float(max_runtime_seconds),
        poll_interval_seconds=poll_interval_seconds,
        cursor=market_cursor,
    )


def assemble_live_channel_sources(
    *,
    market_state_source: str,
    market_quote_hub_url: str,
    market_environment: str,
    account_event_hub_url: str,
    account_label: str,
    session_id: str,
    on_account_recovery: Callable[[str], None],
    hub_source: WebSocketMarketStateSource | None = None,
    risk_control_enabled: bool = False,
    risk_control_hub_url: str | None = None,
    strategy_name: str | None = None,
    on_risk_control_connection_change: (
        Callable[[bool, str | None], None] | None
    ) = None,
) -> LiveChannelSources:
    """Assemble all real-time WebSocket sources for live channels."""
    quote_source: WebSocketMarketQuoteSource | None = None
    if market_state_source == "hub":
        quote_source = WebSocketMarketQuoteSource(
            url=market_quote_hub_url,
            environment=market_environment,
            consumer_id=f"live-exit-quotes:{session_id}",
        )

    account_source = WebSocketAccountEventSource(
        url=account_event_hub_url,
        environment="live",
        account_label=account_label,
        consumer_id=f"live-exit:{session_id}",
        on_recovery=on_account_recovery,
    )

    risk_control_source: WebSocketRiskControlSource | None = None
    if risk_control_enabled and risk_control_hub_url:
        risk_control_source = WebSocketRiskControlSource(
            url=risk_control_hub_url,
            environment="live",
            account_label=account_label,
            strategy_name=strategy_name or "",
            session_id=session_id,
            consumer_id=f"live-risk-control:{session_id}",
            on_connection_change=on_risk_control_connection_change,
        )

    return LiveChannelSources(
        hub_source=hub_source,
        quote_source=quote_source,
        account_source=account_source,
        risk_control_source=risk_control_source,
    )


async def collect_startup_market_states(
    *,
    source: AsyncIterable[MarketState15s],
    buffer: StartupMarketStateBuffer,
) -> None:
    """Consume Hub data during DB warmup and hand the same stream forward."""
    try:
        async for state in resilient_market_state_stream(source):
            await buffer.append(state)
    except Exception as error:
        buffer.close(error)
        raise
    else:
        buffer.close()


async def observe_market_states(
    states: AsyncIterable[MarketState15s],
    cache: LatestMarketStateCache,
    *,
    on_observed: Callable[..., None] | None = None,
    strategy: object | None = None,
    entry_universe_count: Callable[[datetime], int] | None = None,
) -> AsyncIterator[MarketState15s]:
    """Observe incoming market states into local cache and readiness metrics."""
    async for state in states:
        cache.observe(state)
        if on_observed is not None and strategy is not None:
            count = (
                0
                if entry_universe_count is None
                else entry_universe_count(state.bucket_start)
            )
            on_observed(
                state,
                strategy=strategy,
                entry_universe_count=count,
            )
        yield state


async def run_risk_control_channel(
    *,
    source: AsyncIterable[RiskControlEvent],
    on_event: Callable[[RiskControlEvent], Awaitable[None]],
) -> None:
    """Forward risk control events through resilient reconnection."""
    async for event in resilient_risk_control_stream(source):
        await on_event(event)


__all__ = [
    "LiveCandleAssembly",
    "LiveChannelSources",
    "LiveStartupMarketAssembly",
    "assemble_live_candle_sources",
    "assemble_live_channel_sources",
    "assemble_live_quote_volume",
    "assemble_live_startup_market_buffer",
    "build_live_market_state_stream",
    "collect_startup_market_states",
    "observe_market_states",
    "run_risk_control_channel",
]
