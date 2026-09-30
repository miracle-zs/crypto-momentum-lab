import pytest

from crypto_momentum_lab.live_rollout.order_identity_errors import (
    is_durable_order_identity_conflict,
    is_runtime_order_identity_conflict,
)


@pytest.mark.parametrize("nested", [False, True])
def test_runtime_reservation_conflict_does_not_expand_exit_policy(nested: bool) -> None:
    class ReservationConflictError(RuntimeError):
        pass

    error: Exception = ReservationConflictError("occupied")
    if nested:
        outer = RuntimeError("outer")
        outer.__cause__ = error
        error = outer
    assert is_runtime_order_identity_conflict(error)
    assert not is_durable_order_identity_conflict(error)


@pytest.mark.parametrize(
    "message",
    [
        "is in non-dispatchable state",
        "Execution command was not durably accepted",
        "conflicts with its durable identity",
    ],
)
def test_both_policies_recognize_nested_durable_conflicts(message: str) -> None:
    error = RuntimeError("outer")
    error.__cause__ = ValueError(message)
    assert is_runtime_order_identity_conflict(error)
    assert is_durable_order_identity_conflict(error)
