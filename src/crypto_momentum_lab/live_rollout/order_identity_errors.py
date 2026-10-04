"""Distinct durable-exit and runtime-channel identity conflict policies."""

from crypto_momentum_lab.domain.execution.execution_coordinator import (
    ReservationConflictError,
)
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderPreSubmissionError,
)

_DURABLE_CONFLICT_MESSAGES = (
    "already bound to a different order",
    "is in non-dispatchable state",
    "Execution command was not durably accepted",
    "conflicts with its durable identity",
)


def is_runtime_order_identity_conflict(error: Exception) -> bool:
    msg = str(error)
    if any(message in msg for message in _DURABLE_CONFLICT_MESSAGES):
        return True
    if isinstance(error, ReservationConflictError):
        return True
    if isinstance(error, OrderPreSubmissionError) and (
        "already exists" in msg
        or "non-dispatchable" in msg
        or "durably accepted" in msg
    ):
        return True
    cause = error.__cause__
    if isinstance(cause, Exception):
        return is_runtime_order_identity_conflict(cause)
    return False


def is_durable_order_identity_conflict(error: Exception) -> bool:
    message = str(error)
    if any(marker in message for marker in _DURABLE_CONFLICT_MESSAGES):
        return True
    cause = error.__cause__
    return isinstance(cause, Exception) and is_durable_order_identity_conflict(cause)
