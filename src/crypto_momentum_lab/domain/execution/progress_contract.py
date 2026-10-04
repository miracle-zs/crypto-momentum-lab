"""Domain models and pure services for execution readiness and progress contracts."""

from __future__ import annotations

from enum import StrEnum


class ExecutionReadiness(StrEnum):
    """Execution readiness states enforcing non-blocking degraded operations."""

    INDEPENDENT_EXECUTABLE = "independent_executable"
    PROGRESS_LAGGING = "progress_lagging"
    STALLED = "stalled"


__all__ = ["ExecutionReadiness"]
