"""Single live entry control; reconciliation is diagnostic."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import structlog

from crypto_momentum_lab.live_rollout.scheduled_risk_window import (
    ScheduledRiskWindowConfig,
)

if TYPE_CHECKING:
    from crypto_momentum_lab.execution_account.orders.coordinator import (
        CoordinatedOrderExecutionPort,
    )

log = structlog.get_logger()


class LiveEntryControlGate:
    """Own entry prerequisites, manual controls, and scheduled limits."""

    def __init__(
        self,
        *,
        run_id: str,
        state_machine: CoordinatedOrderExecutionPort,
        scheduled_risk_window: ScheduledRiskWindowConfig | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        is_symbol_warmed: Callable[[str], bool] | None = None,
        on_unwarmed_symbol: Callable[[str], None] | None = None,
    ) -> None:
        if not run_id.strip():
            raise ValueError("run_id must not be empty")
        self._run_id = run_id
        self._state_machine = state_machine
        self._schedule = scheduled_risk_window
        self._clock = clock
        self._is_symbol_warmed = is_symbol_warmed
        self._on_unwarmed_symbol = on_unwarmed_symbol
        self._entry_enabled = True
        self._entry_enabled_reason = "initializing"
        self._risk_control_entry_blocked = False
        self._risk_control_entry_block_reason = "risk_control_clear"
        self._scheduled_entry_blocked = False
        self._scheduled_entry_block_reason = "outside_scheduled_risk_window"
        self._entry_filter_cache_ready = True

    @property
    def entry_enabled(self) -> bool:
        return (
            self._entry_enabled
            and not self._risk_control_entry_blocked
            and not self._scheduled_entry_blocked
            and self._outside_scheduled_window()
        )

    @property
    def entry_enabled_reason(self) -> str:
        if self._risk_control_entry_blocked:
            return self._risk_control_entry_block_reason
        if self._scheduled_entry_blocked:
            return self._scheduled_entry_block_reason
        if not self._outside_scheduled_window():
            return "scheduled_risk_window"
        return self._entry_enabled_reason

    def is_symbol_entry_allowed(self, symbol: str) -> tuple[bool, str]:
        """Check whether entry is allowed for a specific symbol."""
        if not self.entry_enabled:
            return False, self.entry_enabled_reason
        if self._is_symbol_warmed is not None and not self._is_symbol_warmed(symbol):
            if self._on_unwarmed_symbol is not None:
                self._on_unwarmed_symbol(symbol)
            return False, f"symbol_not_prewarmed:{symbol}"
        return True, "entry_allowed"

    def _outside_scheduled_window(self) -> bool:
        # This read is synchronous even when cancellation/verification awaits I/O.
        return self._schedule is None or self._schedule.is_entry_allowed(self._clock())

    def set_entry_enabled(self, enabled: bool, *, reason: str) -> None:
        if not isinstance(enabled, bool):
            raise TypeError("enabled must be a bool")
        if not reason.strip():
            raise ValueError("reason must not be empty")
        if self._entry_enabled == enabled and self._entry_enabled_reason == reason:
            return
        state_changed = self._entry_enabled != enabled
        self._entry_enabled = enabled
        self._entry_enabled_reason = reason
        log.warning(
            "live_entry_lane_state_changed",
            enabled=enabled,
            state_changed=state_changed,
            reason=reason,
            run_id=self._run_id,
        )

    def set_entry_filter_cache_ready(self, ready: bool) -> None:
        """Set whether the configured entry filter cache can admit entries."""

        if not isinstance(ready, bool):
            raise TypeError("ready must be a bool")
        self._entry_filter_cache_ready = ready

    def refresh_entry_prerequisites(
        self,
        *,
        session_draining: bool,
        market_state_available: bool,
        market_state_unavailable_reason: str,
        strategy_warmup_ready: bool,
        strategy_warmup_reason: str = "strategy_warmup_ready",
    ) -> None:
        """Apply external live prerequisites in their fail-closed priority."""

        for value, field_name in (
            (session_draining, "session_draining"),
            (strategy_warmup_ready, "strategy_warmup_ready"),
            (market_state_available, "market_state_available"),
        ):
            if not isinstance(value, bool):
                raise TypeError(f"{field_name} must be a bool")
        if not strategy_warmup_reason.strip():
            raise ValueError("strategy_warmup_reason must not be empty")
        if not market_state_unavailable_reason.strip():
            raise ValueError("market_state_unavailable_reason must not be empty")

        if session_draining:
            self.set_entry_enabled(False, reason="session_draining")
        elif not strategy_warmup_ready:
            self.set_entry_enabled(False, reason=strategy_warmup_reason)
        elif not market_state_available:
            self.set_entry_enabled(
                False,
                reason=market_state_unavailable_reason,
            )
        elif not self._entry_filter_cache_ready:
            self.set_entry_enabled(False, reason="entry_cache_warming")
        else:
            self.set_entry_enabled(
                True,
                reason="live_entry_prerequisites_ready",
            )

    def set_risk_control_entry_blocked(
        self,
        blocked: bool,
        *,
        reason: str,
    ) -> None:
        """Apply the durable-control gate without touching other gates."""

        self._validate_gate_update(blocked, reason)
        if (
            self._risk_control_entry_blocked == blocked
            and self._risk_control_entry_block_reason == reason
        ):
            return
        self._risk_control_entry_blocked = blocked
        self._risk_control_entry_block_reason = reason
        if blocked:
            self._set_coordinator_entry_gate(blocked=True)
        elif not self._scheduled_entry_blocked:
            self._set_coordinator_entry_gate(blocked=False)
        log.warning(
            "live_risk_control_entry_gate_changed",
            blocked=blocked,
            reason=reason,
            run_id=self._run_id,
        )

    def set_scheduled_entry_blocked(
        self,
        blocked: bool,
        *,
        reason: str,
    ) -> None:
        """Apply the schedule gate while keeping reopen fail-closed."""

        self._validate_gate_update(blocked, reason)
        if (
            self._scheduled_entry_blocked == blocked
            and self._scheduled_entry_block_reason == reason
        ):
            return
        state_changed = self._scheduled_entry_blocked != blocked
        self._scheduled_entry_blocked = blocked
        self._scheduled_entry_block_reason = reason
        self._set_coordinator_entry_gate(
            blocked=blocked or self._risk_control_entry_blocked,
        )
        log.warning(
            "live_scheduled_entry_gate_changed",
            blocked=blocked,
            state_changed=state_changed,
            reason=reason,
            run_id=self._run_id,
        )

    def _validate_gate_update(self, blocked: bool, reason: str) -> None:
        if not isinstance(blocked, bool):
            raise TypeError("blocked must be a bool")
        if not reason.strip():
            raise ValueError("reason must not be empty")

    def _set_coordinator_entry_gate(self, *, blocked: bool) -> None:
        if blocked:
            self._state_machine.block_entry_submissions()
        else:
            self._state_machine.unblock_entry_submissions()


__all__ = ["LiveEntryControlGate"]
