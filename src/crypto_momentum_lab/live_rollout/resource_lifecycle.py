"""Ordered cleanup for resources owned by one live worker."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from time import perf_counter
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

_DEFAULT_SHUTDOWN_TIMEOUT_SECONDS = 15.0
_RESOURCE_CLOSE_TIMEOUT_SECONDS = 5.0


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
        shutdown_timeout_seconds: float = _DEFAULT_SHUTDOWN_TIMEOUT_SECONDS,
    ) -> None:
        if shutdown_timeout_seconds <= 0:
            raise ValueError("shutdown_timeout_seconds must be positive")
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
        self._shutdown_timeout_seconds = shutdown_timeout_seconds

    async def close(self) -> None:
        """Stop producers before transports, then dispose database engines."""

        try:
            async with asyncio.timeout(self._shutdown_timeout_seconds):
                await self._close_impl()
        except TimeoutError:
            log.warning(
                "live_resource_shutdown_timed_out",
                timeout_seconds=self._shutdown_timeout_seconds,
            )
        finally:
            if self._health is not None:
                try:
                    self._health.stopped()
                except Exception:
                    log.exception("live_health_stop_marker_failed")

    async def _close_impl(self) -> None:
        """Run safety-critical closes first, then independent cleanup together."""

        if self._entry_runtime is not None:
            await self._close_resource("entry_runtime", self._entry_runtime.stop)
        if self._entry_order_lifecycle is not None:
            await self._close_resource(
                "entry_order_lifecycle",
                self._entry_order_lifecycle.stop,
            )
        if self._execution_coordinator is not None:
            await self._close_resource(
                "execution_coordinator",
                self._execution_coordinator.aclose,
            )
        if self._client is not None:
            await self._close_resource("trade_client", self._client.aclose)

        independent_resources: list[Awaitable[None]] = []
        if self._closed_candle_feed is not None:
            independent_resources.append(
                self._close_resource(
                    "closed_candle_feed",
                    self._closed_candle_feed.stop,
                )
            )
        if self._candle_source is not None or self._ema_candle_source is not None:
            independent_resources.append(
                self._close_resource(
                    "closed_candle_sources",
                    self._close_candle_sources,
                )
            )
        if self._signal_recorder is not None:
            independent_resources.append(
                self._close_resource(
                    "signal_recorder",
                    self._signal_recorder.stop,
                )
            )
        if self._telemetry is not None:
            independent_resources.append(
                self._close_resource("telemetry", self._telemetry.stop)
            )
        if self._volume_cache is not None:
            independent_resources.append(
                self._close_resource("volume_cache", self._volume_cache.stop)
            )
        if self._volume_rest_client is not None:
            independent_resources.append(
                self._close_resource(
                    "volume_rest_client",
                    self._volume_rest_client.aclose,
                )
            )
        if independent_resources:
            await asyncio.gather(*independent_resources)

        engines = (
            ("execution_engine", self._execution_engine),
            ("market_engine", self._market_engine),
            ("observability_engine", self._observability_engine),
            ("checkpoint_engine", self._checkpoint_engine),
            ("heartbeat_engine", self._heartbeat_engine),
        )
        await asyncio.gather(
            *(
                self._close_resource(label, engine.dispose)
                for label, engine in engines
                if engine is not None
            )
        )

    async def _close_candle_sources(self) -> None:
        closed_candle_sources: set[int] = set()
        for source in (self._candle_source, self._ema_candle_source):
            if source is None or id(source) in closed_candle_sources:
                continue
            source.close()
            closed_candle_sources.add(id(source))

    async def _close_resource(
        self,
        label: str,
        operation: Callable[[], Awaitable[None]],
    ) -> None:
        started_at = perf_counter()
        try:
            async with asyncio.timeout(_RESOURCE_CLOSE_TIMEOUT_SECONDS):
                await operation()
        except TimeoutError:
            log.warning(
                "live_resource_close_timed_out",
                resource=label,
                timeout_seconds=_RESOURCE_CLOSE_TIMEOUT_SECONDS,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("live_resource_close_failed", resource=label)
        else:
            log.info(
                "live_resource_closed",
                resource=label,
                duration_seconds=round(perf_counter() - started_at, 3),
            )


__all__ = ["LiveResourceLifecycle"]
