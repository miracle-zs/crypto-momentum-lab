"""Process-local health monitoring for a live worker."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable

import structlog

from crypto_momentum_lab.health import LocalHealthWriter
from crypto_momentum_lab.health.resources import ProcessResourceSampler

log = structlog.get_logger(__name__)


Sleep = Callable[[float], Awaitable[None]]


class LiveHealthMonitor:
    """Refresh local liveness while degrading on failed critical tasks."""

    def __init__(
        self,
        *,
        health: LocalHealthWriter,
        interval_seconds: float,
        is_degraded: Callable[[], bool],
        sleep: Sleep | None = None,
        resource_snapshot: Callable[[], dict[str, int | float | None]] | None = None,
        resource_interval_seconds: float = 60.0,
        monotonic: Callable[[], float] = time.monotonic,
        resource_dimensions: dict[str, str] | None = None,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        if resource_interval_seconds <= 0:
            raise ValueError("resource_interval_seconds must be positive")
        self._health = health
        self._interval_seconds = interval_seconds
        self._is_degraded = is_degraded
        self._sleep = sleep or asyncio.sleep
        self._resource_snapshot = (
            ProcessResourceSampler().snapshot
            if resource_snapshot is None
            else resource_snapshot
        )
        self._resource_interval_seconds = resource_interval_seconds
        self._monotonic = monotonic
        self._resource_dimensions = resource_dimensions or {}

    async def run(self) -> None:
        """Publish health markers until the task is cancelled."""

        next_resource_snapshot_at = self._monotonic() + self._resource_interval_seconds
        while True:
            await self._sleep(self._interval_seconds)
            try:
                if self._is_degraded():
                    self._health.degraded()
                else:
                    self._health.heartbeat()
            except Exception:
                log.exception("live_health_marker_failed")
            if self._monotonic() < next_resource_snapshot_at:
                continue
            next_resource_snapshot_at = (
                self._monotonic() + self._resource_interval_seconds
            )
            try:
                log.info(
                    "process_resource_snapshot",
                    service="live_strategy",
                    **self._resource_dimensions,
                    **self._resource_snapshot(),
                )
            except Exception:
                log.exception(
                    "process_resource_snapshot_failed",
                    service="live_strategy",
                )


__all__ = ["LiveHealthMonitor"]
