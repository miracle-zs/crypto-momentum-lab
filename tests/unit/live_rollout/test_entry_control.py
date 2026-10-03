import pytest

from crypto_momentum_lab.live_rollout.entry_control import LiveEntryControlGate


class _StateMachine:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.fail_unblock = False

    def block_entry_submissions(self) -> None:
        self.calls.append("block")

    def unblock_entry_submissions(self) -> None:
        self.calls.append("unblock")
        if self.fail_unblock:
            raise RuntimeError("coordinator unavailable")


def test_entry_control_composes_gate_priority_and_pending_positions() -> None:
    state_machine = _StateMachine()
    gate = LiveEntryControlGate(run_id="run-1", state_machine=state_machine)

    assert gate.entry_enabled is True
    assert gate.entry_enabled_reason == "initializing"

    gate.set_pending_position_symbols({"BTCUSDT"})
    # Account-wide lane remains open for other symbols
    assert gate.entry_enabled is True
    # BTCUSDT is locally protected
    assert gate.is_symbol_entry_allowed("BTCUSDT") == (
        False,
        "account_position_sync_pending:BTCUSDT",
    )
    # ETHUSDT continues running
    assert gate.is_symbol_entry_allowed("ETHUSDT") == (True, "entry_allowed")

    gate.set_scheduled_entry_blocked(True, reason="scheduled_risk_window")
    gate.set_risk_control_entry_blocked(
        True,
        reason="risk_control_state_recovering",
    )
    assert gate.entry_enabled is False
    assert gate.entry_enabled_reason == "risk_control_state_recovering"
    assert gate.is_symbol_entry_allowed("BTCUSDT") == (
        False,
        "risk_control_state_recovering",
    )
    assert gate.is_symbol_entry_allowed("ETHUSDT") == (
        False,
        "risk_control_state_recovering",
    )
    assert state_machine.calls == ["block", "block"]

    gate.set_risk_control_entry_blocked(False, reason="risk_control_clear")
    assert gate.entry_enabled_reason == "scheduled_risk_window"
    gate.set_scheduled_entry_blocked(
        False,
        reason="scheduled_risk_window_complete",
    )
    # Account lane reopened, but BTCUSDT pending position sync remains protected
    assert gate.entry_enabled is True
    assert gate.is_symbol_entry_allowed("BTCUSDT") == (
        False,
        "account_position_sync_pending:BTCUSDT",
    )
    assert gate.is_symbol_entry_allowed("ETHUSDT") == (True, "entry_allowed")
    assert state_machine.calls == ["block", "block", "unblock"]

    gate.set_pending_position_symbols(set())
    assert gate.is_symbol_entry_allowed("BTCUSDT") == (True, "entry_allowed")


def test_entry_control_keeps_reopen_fail_closed_until_coordinator_recovers() -> None:
    state_machine = _StateMachine()
    gate = LiveEntryControlGate(run_id="run-1", state_machine=state_machine)
    gate.set_scheduled_entry_blocked(True, reason="scheduled_risk_window")

    state_machine.fail_unblock = True
    gate.set_scheduled_entry_blocked(
        False,
        reason="scheduled_risk_window_complete",
    )
    assert gate.entry_enabled is False
    assert gate.entry_enabled_reason == "scheduled_entry_gate_update_failed"

    state_machine.fail_unblock = False
    gate.set_scheduled_entry_blocked(
        False,
        reason="scheduled_risk_window_complete",
    )
    assert gate.entry_enabled is True
    assert gate.entry_enabled_reason == "initializing"


def test_entry_control_validates_gate_inputs() -> None:
    gate = LiveEntryControlGate(run_id="run-1", state_machine=_StateMachine())

    with pytest.raises(TypeError):
        gate.set_entry_enabled("yes", reason="test")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        gate.set_entry_enabled(False, reason=" ")


def test_entry_control_owns_external_prerequisite_priority() -> None:
    gate = LiveEntryControlGate(run_id="run-1", state_machine=_StateMachine())
    gate.set_entry_filter_cache_ready(False)
    gate.refresh_entry_prerequisites(
        lease_heartbeat_degraded=False,
        session_draining=False,
        market_state_available=True,
        market_state_unavailable_reason="market_state_hub_ready",
        account_snapshot_available=True,
        strategy_warmup_ready=True,
    )
    assert gate.entry_enabled is False
    assert gate.entry_enabled_reason == "entry_cache_warming"

    gate.set_exit_failure("BTCUSDT", "exit request failed")
    gate.refresh_entry_prerequisites(
        lease_heartbeat_degraded=False,
        session_draining=False,
        market_state_available=True,
        market_state_unavailable_reason="market_state_hub_ready",
        account_snapshot_available=True,
        strategy_warmup_ready=True,
    )
    # Account level gate is blocked by cache warming
    assert gate.entry_enabled_reason == "entry_cache_warming"
    assert gate.is_symbol_entry_allowed("BTCUSDT") == (False, "entry_cache_warming")

    # Clear cache warming
    gate.set_entry_filter_cache_ready(True)
    gate.refresh_entry_prerequisites(
        lease_heartbeat_degraded=False,
        session_draining=False,
        market_state_available=True,
        market_state_unavailable_reason="market_state_hub_ready",
        account_snapshot_available=True,
        strategy_warmup_ready=True,
    )
    # Account level is enabled, but BTCUSDT has exit failure while ETHUSDT is allowed
    assert gate.entry_enabled is True
    assert gate.is_symbol_entry_allowed("BTCUSDT") == (
        False,
        "exit_failure:BTCUSDT:exit request failed",
    )
    assert gate.is_symbol_entry_allowed("ETHUSDT") == (True, "entry_allowed")

    gate.set_exit_failure("BTCUSDT", None)
    assert gate.is_symbol_entry_allowed("BTCUSDT") == (True, "entry_allowed")
    gate.refresh_entry_prerequisites(
        lease_heartbeat_degraded=False,
        session_draining=False,
        market_state_available=False,
        market_state_unavailable_reason="market_state_consumer_lagged",
        account_snapshot_available=True,
        strategy_warmup_ready=True,
    )
    assert gate.entry_enabled_reason == "market_state_consumer_lagged"

    gate.refresh_entry_prerequisites(
        lease_heartbeat_degraded=False,
        session_draining=False,
        market_state_available=True,
        market_state_unavailable_reason="market_state_hub_ready",
        account_snapshot_available=True,
        strategy_warmup_ready=True,
    )
    assert gate.entry_enabled is True


def test_entry_control_stays_closed_until_strategy_warmup_is_ready() -> None:
    gate = LiveEntryControlGate(run_id="run-1", state_machine=_StateMachine())

    gate.refresh_entry_prerequisites(
        lease_heartbeat_degraded=False,
        session_draining=False,
        market_state_available=True,
        market_state_unavailable_reason="market_state_hub_ready",
        account_snapshot_available=True,
        strategy_warmup_ready=False,
        strategy_warmup_reason="strategy_warmup_incomplete:gaps=BTCUSDT",
    )

    assert gate.entry_enabled is False
    assert gate.entry_enabled_reason == "strategy_warmup_incomplete:gaps=BTCUSDT"


def test_entry_control_validates_external_prerequisites() -> None:
    gate = LiveEntryControlGate(run_id="run-1", state_machine=_StateMachine())

    with pytest.raises(TypeError):
        gate.set_entry_filter_cache_ready("yes")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        gate.set_exit_failure("BTCUSDT", " ")
    with pytest.raises(ValueError):
        gate.refresh_entry_prerequisites(
            lease_heartbeat_degraded=False,
            session_draining=False,
            market_state_available=True,
            market_state_unavailable_reason=" ",
            account_snapshot_available=True,
            strategy_warmup_ready=True,
        )


def test_entry_control_set_exit_failure_change_tracking() -> None:
    gate = LiveEntryControlGate(run_id="run-1", state_machine=_StateMachine())

    # Clearing a failure that wasn't present returns False
    assert gate.set_exit_failure("BTCUSDT", None) is False

    # Recording a new failure returns True
    assert gate.set_exit_failure("BTCUSDT", "timeout") is True

    # Recording identical failure returns False
    assert gate.set_exit_failure("BTCUSDT", "timeout") is False

    # Changing failure message returns True
    assert gate.set_exit_failure("BTCUSDT", "api_error") is True

    # Clearing an existing failure returns True
    assert gate.set_exit_failure("BTCUSDT", None) is True

    # Clearing again returns False
    assert gate.set_exit_failure("BTCUSDT", None) is False


def test_exit_failure_blocks_entry_immediately_without_prerequisite_refresh() -> None:
    gate = LiveEntryControlGate(run_id="run-1", state_machine=object())
    gate.set_entry_enabled(True, reason="ready")
    gate.set_exit_failure("BTCUSDT", "recovery_failed")
    # Global entry lane stays enabled for other symbols
    assert gate.entry_enabled is True
    # BTCUSDT is immediately blocked
    assert gate.is_symbol_entry_allowed("BTCUSDT") == (
        False,
        "exit_failure:BTCUSDT:recovery_failed",
    )
    # ETHUSDT remains allowed
    assert gate.is_symbol_entry_allowed("ETHUSDT") == (True, "entry_allowed")

    # Clearing exit failure allows BTCUSDT again
    gate.set_exit_failure("BTCUSDT", None)
    assert gate.is_symbol_entry_allowed("BTCUSDT") == (True, "entry_allowed")


def test_schedule_reopen_does_not_release_active_risk_block():
    state_machine = _StateMachine()
    gate = LiveEntryControlGate(run_id="run-1", state_machine=state_machine)
    gate.set_scheduled_entry_blocked(True, reason="scheduled_risk_window")
    gate.set_risk_control_entry_blocked(True, reason="risk_halt")
    gate.set_scheduled_entry_blocked(False, reason="scheduled_risk_window_complete")
    assert not gate.entry_enabled
    assert gate.entry_enabled_reason == "risk_halt"
    assert "unblock" not in state_machine.calls
    gate.set_risk_control_entry_blocked(False, reason="risk_clear")
    assert gate.entry_enabled
    assert state_machine.calls[-1] == "unblock"


def test_unwarmed_symbol_blocks_entry_and_triggers_initialization() -> None:
    warmed_symbols = {"BTCUSDT"}
    unwarmed_triggered: list[str] = []

    gate = LiveEntryControlGate(
        run_id="run-1",
        state_machine=_StateMachine(),
        is_symbol_warmed=lambda s: s in warmed_symbols,
        on_unwarmed_symbol=lambda s: unwarmed_triggered.append(s),
    )
    gate.set_entry_enabled(True, reason="ready")

    # Warmed symbol is allowed
    assert gate.is_symbol_entry_allowed("BTCUSDT") == (True, "entry_allowed")
    assert unwarmed_triggered == []

    # Unwarmed symbol is blocked and triggers background initialization
    assert gate.is_symbol_entry_allowed("SOLUSDT") == (
        False,
        "symbol_not_prewarmed:SOLUSDT",
    )
    assert unwarmed_triggered == ["SOLUSDT"]

    # Once warmed, symbol becomes allowed
    warmed_symbols.add("SOLUSDT")
    assert gate.is_symbol_entry_allowed("SOLUSDT") == (True, "entry_allowed")

