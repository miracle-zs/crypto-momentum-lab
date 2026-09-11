import pytest

from crypto_momentum_lab.live_rollout.exit_control import LiveExitControlGate


def test_exit_control_gate_is_enabled_by_default_and_can_be_disabled() -> None:
    gate = LiveExitControlGate(run_id="run-1")

    assert gate.enabled is True
    gate.set_enabled(False, reason="operator_pause")
    assert gate.enabled is False
    gate.set_enabled(True, reason="operator_resume")
    assert gate.enabled is True


def test_exit_control_gate_rejects_non_boolean_state() -> None:
    gate = LiveExitControlGate(run_id="run-1")

    with pytest.raises(TypeError, match="enabled must be a bool"):
        gate.set_enabled("yes", reason="invalid")  # type: ignore[arg-type]
