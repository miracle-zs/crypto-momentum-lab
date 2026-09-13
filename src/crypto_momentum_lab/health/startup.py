"""Reusable structured timing for long-running service startup phases."""

from __future__ import annotations

from time import perf_counter
from typing import Protocol


class StartupPhaseLogger(Protocol):
    """The small logger surface needed by :class:`StartupPhaseTimer`."""

    def info(self, event: str, **kwargs: object) -> object:
        """Write one structured informational event."""


class StartupPhaseTimer:
    """Log monotonic elapsed time between named startup phase boundaries."""

    def __init__(
        self,
        logger: StartupPhaseLogger,
        *,
        event: str,
        **context: object,
    ) -> None:
        if not event.strip():
            raise ValueError("event must not be empty")
        self._logger = logger
        self._event = event
        self._context = context
        self._started_at = perf_counter()
        self._last_phase_at = self._started_at

    def mark(self, phase: str, **fields: object) -> None:
        """Log a completed phase and advance the timing boundary."""

        if not phase.strip():
            raise ValueError("phase must not be empty")
        now = perf_counter()
        values = {
            **self._context,
            "phase": phase,
            "phase_elapsed_ms": round((now - self._last_phase_at) * 1000, 3),
            "total_elapsed_ms": round((now - self._started_at) * 1000, 3),
            **fields,
        }
        self._logger.info(self._event, **values)
        self._last_phase_at = now
