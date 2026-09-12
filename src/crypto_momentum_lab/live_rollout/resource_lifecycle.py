"""Ordered cleanup for resources owned by one live worker."""

from __future__ import annotations

from typing import Protocol

import structlog
from sqlalchemy.ext.asyncio import AsyncEngine

from crypto_momentum_lab.execution_account.binance import BinanceUsdMTradeClient
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionCoordinator,
)
from crypto_momentum_lab.health import LocalHealthWriter
from crypto_momentum_lab.live_rollout.closed_candle_feed import (
    BinanceClosedCandle15mFeed,
)
from crypto_momentum_lab.live_rollout.entry_orders import LiveLimitOrderLifecycle
from crypto_momentum_lab.live_rollout.entry_runtime import LiveEntryRuntime
from crypto_momentum_lab.live_rollout.signal_recorder import (
    LiveStrategySignalRecorder,
)
from crypto_momentum_lab.live_rollout.telemetry import LiveRuntimeTelemetry
from crypto_momentum_lab.market_data.binance.rest import BinanceUsdMRestClient
from crypto_momentum_lab.strategy_runner.candle_source import (
    BinanceRestClosedCandle15mSource,
)

log = structlog.get_logger()


class _StoppableVolumeCache(Protocol):
    async def stop(self) -> None: ...


class LiveResourceLifecycle:
    """Close live resources in the order required by the execution safety model."""

    def __init__(
        self,
        *,
        entry_runtime: LiveEntryRuntime | None,
        entry_order_lifecycle: LiveLimitOrderLifecycle | None,
        execution_coordinator: OrderExecutionCoordinator | None,
        client: BinanceUsdMTradeClient | None,
        closed_candle_feed: BinanceClosedCandle15mFeed | None,
        candle_source: BinanceRestClosedCandle15mSource | None,
        ema_candle_source: BinanceRestClosedCandle15mSource | None,
        signal_recorder: LiveStrategySignalRecorder | None,
        telemetry: LiveRuntimeTelemetry | None,
        volume_cache: _StoppableVolumeCache | None,
        volume_rest_client: BinanceUsdMRestClient | None,
        execution_engine: AsyncEngine | None,
        market_engine: AsyncEngine | None,
        observability_engine: AsyncEngine | None,
        checkpoint_engine: AsyncEngine | None,
        heartbeat_engine: AsyncEngine | None,
        health: LocalHealthWriter | None,
    ) -> None:
        self._entry_runtime = entry_runtime
        self._entry_order_lifecycle = entry_order_lifecycle
        self._execution_coordinator = execution_coordinator
        self._client = client
        self._closed_candle_feed = closed_candle_feed
        self._candle_source = candle_source
        self._ema_candle_source = ema_candle_source
        self._signal_recorder = signal_recorder
        self._telemetry = telemetry
        self._volume_cache = volume_cache
        self._volume_rest_client = volume_rest_client
        self._execution_engine = execution_engine
        self._market_engine = market_engine
        self._observability_engine = observability_engine
        self._checkpoint_engine = checkpoint_engine
        self._heartbeat_engine = heartbeat_engine
        self._health = health

    async def close(self) -> None:
        """Stop producers before transports, then dispose database engines."""

        if self._entry_runtime is not None:
            await self._entry_runtime.stop()
        if self._entry_order_lifecycle is not None:
            await self._entry_order_lifecycle.stop()
        if self._execution_coordinator is not None:
            await self._execution_coordinator.aclose()
        if self._client is not None:
            await self._client.aclose()
        if self._closed_candle_feed is not None:
            await self._closed_candle_feed.stop()
        closed_candle_sources: set[int] = set()
        for source in (self._candle_source, self._ema_candle_source):
            if source is None or id(source) in closed_candle_sources:
                continue
            source.close()
            closed_candle_sources.add(id(source))
        if self._signal_recorder is not None:
            await self._signal_recorder.stop()
        if self._telemetry is not None:
            await self._telemetry.stop()
        if self._volume_cache is not None:
            await self._volume_cache.stop()
        if self._volume_rest_client is not None:
            await self._volume_rest_client.aclose()
        if self._execution_engine is not None:
            await self._execution_engine.dispose()
        if self._market_engine is not None:
            await self._market_engine.dispose()
        if self._observability_engine is not None:
            await self._observability_engine.dispose()
        if self._checkpoint_engine is not None:
            await self._checkpoint_engine.dispose()
        if self._heartbeat_engine is not None:
            await self._heartbeat_engine.dispose()
        if self._health is not None:
            try:
                self._health.stopped()
            except Exception:
                log.exception("live_health_stop_marker_failed")


__all__ = ["LiveResourceLifecycle"]
