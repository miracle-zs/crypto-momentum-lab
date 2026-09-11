"""Fail-closed enable/disable control for live reduce-only exits."""

from __future__ import annotations

import structlog

log = structlog.get_logger()


class LiveExitControlGate:
    """Own the operator-controlled exit enable state."""

    def __init__(self, *, run_id: str) -> None:
        if not run_id.strip():
            raise ValueError("run_id must not be empty")
        self._run_id = run_id
        self._enabled = True

    @property
    def enabled(self) -> bool:
        return self._enabled

    def set_enabled(self, enabled: bool, *, reason: str) -> None:
        if not isinstance(enabled, bool):
            raise TypeError("enabled must be a bool")
        if self._enabled == enabled:
            return
        self._enabled = enabled
        log.warning(
            "live_exit_lane_state_changed",
            enabled=enabled,
            reason=reason,
            run_id=self._run_id,
        )


__all__ = ["LiveExitControlGate"]
