"""Process-local health monitoring for a live worker."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import structlog

from crypto_momentum_lab.health import LocalHealthWriter

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
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        self._health = health
        self._interval_seconds = interval_seconds
        self._is_degraded = is_degraded
        self._sleep = sleep or asyncio.sleep

    async def run(self) -> None:
        """Publish health markers until the task is cancelled."""

        while True:
            await self._sleep(self._interval_seconds)
            try:
                if self._is_degraded():
                    self._health.degraded()
                else:
                    self._health.heartbeat()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("live_health_marker_failed")


__all__ = ["LiveHealthMonitor"]
