"""Fail-closed composition of live entry control gates."""

from __future__ import annotations

from collections.abc import Collection

import structlog

log = structlog.get_logger()


class LiveEntryControlGate:
    """Own prerequisite, risk, schedule, and position-sync entry gates."""

    def __init__(self, *, run_id: str, state_machine: object) -> None:
        if not run_id.strip():
            raise ValueError("run_id must not be empty")
        self._run_id = run_id
        self._state_machine = state_machine
        self._entry_enabled = True
        self._entry_enabled_reason = "initializing"
        self._risk_control_entry_blocked = False
        self._risk_control_entry_block_reason = "risk_control_clear"
        self._scheduled_entry_blocked = False
        self._scheduled_entry_block_reason = "outside_scheduled_risk_window"
        self._pending_position_symbols: frozenset[str] = frozenset()

    @property
    def entry_enabled(self) -> bool:
        return (
            self._entry_enabled
            and not self._risk_control_entry_blocked
            and not self._scheduled_entry_blocked
            and not self._pending_position_symbols
        )

    @property
    def entry_enabled_reason(self) -> str:
        if self._risk_control_entry_blocked:
            return self._risk_control_entry_block_reason
        if self._scheduled_entry_blocked:
            return self._scheduled_entry_block_reason
        if self._pending_position_symbols:
            symbols = ",".join(sorted(self._pending_position_symbols))
            return f"account_position_sync_pending:{symbols}"
        return self._entry_enabled_reason

    def set_pending_position_symbols(
        self,
        symbols: Collection[str],
    ) -> None:
        self._pending_position_symbols = frozenset(symbols)

    def set_entry_enabled(self, enabled: bool, *, reason: str) -> None:
        if not isinstance(enabled, bool):
            raise TypeError("enabled must be a bool")
        if not reason.strip():
            raise ValueError("reason must not be empty")
        if (
            self._entry_enabled == enabled
            and self._entry_enabled_reason == reason
        ):
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
        if blocked:
            # Close the daemon gate before asking the coordinator to drain.
            self._scheduled_entry_blocked = True
            self._scheduled_entry_block_reason = reason
            self._set_coordinator_entry_gate(blocked=True)
        else:
            # Do not reopen the daemon gate if the coordinator rejected the
            # transition; the next schedule poll can retry it safely.
            if not self._set_coordinator_entry_gate(blocked=False):
                return
            self._scheduled_entry_blocked = False
            self._scheduled_entry_block_reason = reason
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

    def _set_coordinator_entry_gate(self, *, blocked: bool) -> bool:
        method_name = (
            "block_entry_submissions"
            if blocked
            else "unblock_entry_submissions"
        )
        method = getattr(self._state_machine, method_name, None)
        if not callable(method):
            return True
        try:
            method()
        except Exception as error:
            log.exception(
                "live_coordinator_entry_gate_update_failed",
                run_id=self._run_id,
                blocked=blocked,
                error_type=type(error).__name__,
            )
            if not blocked:
                self._scheduled_entry_blocked = True
                self._scheduled_entry_block_reason = (
                    "scheduled_entry_gate_update_failed"
                )
            return False
        return True


__all__ = ["LiveEntryControlGate"]
