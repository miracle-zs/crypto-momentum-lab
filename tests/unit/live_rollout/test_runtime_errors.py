import pytest
from sqlalchemy.exc import SQLAlchemyError

from crypto_momentum_lab.live_rollout.runtime_errors import is_transient_runtime_error
from crypto_momentum_lab.live_rollout.startup_resilience import (
    is_retryable_live_startup_error,
)


@pytest.mark.parametrize(
    "error", [SQLAlchemyError("db"), TimeoutError(), ConnectionError(), OSError()]
)
def test_transient_dependencies_are_shared_with_startup(error: Exception) -> None:
    assert is_transient_runtime_error(error)
    assert is_retryable_live_startup_error(error)


@pytest.mark.parametrize(
    "error",
    [
        ValueError("invalid"),
        RuntimeError("invalid"),
        RuntimeError("live gate blocked:active_risk_halt"),
    ],
)
def test_runtime_does_not_retry_configuration_or_gate_errors(error: Exception) -> None:
    assert not is_transient_runtime_error(error)
    assert is_retryable_live_startup_error(error) == str(error).startswith(
        "live gate blocked:"
    )


def test_runtime_does_not_classify_wrapped_transient_cause() -> None:
    error = RuntimeError("application failure")
    error.__cause__ = ConnectionError()
    assert not is_transient_runtime_error(error)
    assert not is_retryable_live_startup_error(error)
