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
        self._entry_filter_cache_ready = True
        self._exit_failure_by_symbol: dict[str, str] = {}

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

    def set_exit_failure(
        self,
        symbol: str,
        failure: str | None,
    ) -> None:
        """Remember an exit failure until that symbol recovers."""

        if not symbol.strip():
            raise ValueError("symbol must not be empty")
        if failure is None:
            self._exit_failure_by_symbol.pop(symbol, None)
            return
        if not failure.strip():
            raise ValueError("failure must not be empty when present")
        self._exit_failure_by_symbol[symbol] = failure

    def set_entry_filter_cache_ready(self, ready: bool) -> None:
        """Set whether the configured entry filter cache can admit entries."""

        if not isinstance(ready, bool):
            raise TypeError("ready must be a bool")
        self._entry_filter_cache_ready = ready

    def refresh_entry_prerequisites(
        self,
        *,
        lease_heartbeat_degraded: bool,
        session_draining: bool,
        market_state_available: bool,
        market_state_unavailable_reason: str,
        account_snapshot_available: bool,
        strategy_warmup_ready: bool = True,
        strategy_warmup_reason: str = "strategy_warmup_ready",
    ) -> None:
        """Apply external live prerequisites in their fail-closed priority."""

        for value, field_name in (
            (lease_heartbeat_degraded, "lease_heartbeat_degraded"),
            (session_draining, "session_draining"),
            (strategy_warmup_ready, "strategy_warmup_ready"),
            (market_state_available, "market_state_available"),
            (account_snapshot_available, "account_snapshot_available"),
        ):
            if not isinstance(value, bool):
                raise TypeError(f"{field_name} must be a bool")
        if not strategy_warmup_reason.strip():
            raise ValueError("strategy_warmup_reason must not be empty")
        if not market_state_unavailable_reason.strip():
            raise ValueError("market_state_unavailable_reason must not be empty")

        if lease_heartbeat_degraded:
            self.set_entry_enabled(
                False,
                reason="lease_heartbeat_degraded",
            )
        elif session_draining:
            self.set_entry_enabled(False, reason="session_draining")
        elif self._exit_failure_by_symbol:
            symbol, failure = next(iter(self._exit_failure_by_symbol.items()))
            self.set_entry_enabled(
                False,
                reason=f"exit_failure:{symbol}:{failure}",
            )
        elif not strategy_warmup_ready:
            self.set_entry_enabled(False, reason=strategy_warmup_reason)
        elif not market_state_available:
            self.set_entry_enabled(
                False,
                reason=market_state_unavailable_reason,
            )
        elif not account_snapshot_available:
            self.set_entry_enabled(
                False,
                reason="account_snapshot_recovering",
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
