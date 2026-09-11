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
    assert gate.entry_enabled is False
    assert gate.entry_enabled_reason == (
        "account_position_sync_pending:BTCUSDT"
    )

    gate.set_scheduled_entry_blocked(True, reason="scheduled_risk_window")
    gate.set_risk_control_entry_blocked(
        True,
        reason="risk_control_state_recovering",
    )
    assert gate.entry_enabled_reason == "risk_control_state_recovering"
    assert state_machine.calls == ["block", "block"]

    gate.set_risk_control_entry_blocked(False, reason="risk_control_clear")
    assert gate.entry_enabled_reason == "scheduled_risk_window"
    gate.set_scheduled_entry_blocked(
        False,
        reason="scheduled_risk_window_complete",
    )
    assert gate.entry_enabled is False
    assert gate.entry_enabled_reason == (
        "account_position_sync_pending:BTCUSDT"
    )
    assert state_machine.calls == ["block", "block", "unblock"]


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
