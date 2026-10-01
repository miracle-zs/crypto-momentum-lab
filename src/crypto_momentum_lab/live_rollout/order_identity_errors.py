"""Distinct durable-exit and runtime-channel identity conflict policies."""

_ORDER_IDENTITY_CONFLICT_MESSAGE = (
    "client order ID is already bound to a different order"
)


def is_runtime_order_identity_conflict(error: Exception) -> bool:
    if isinstance(error, ValueError) and str(error) == _ORDER_IDENTITY_CONFLICT_MESSAGE:
        return True
    msg = str(error)
    if (
        "already exists in terminal status" in msg
        or "already bound to a different order" in msg
        or "is in non-dispatchable state" in msg
        or "Execution command was not durably accepted" in msg
        or "conflicts with its durable identity" in msg
    ):
        return True
    if "ReservationConflictError" in type(error).__name__:
        return True
    if "OrderPreSubmissionError" in type(error).__name__ and (
        "already exists" in msg
        or "non-dispatchable" in msg
        or "durably accepted" in msg
    ):
        return True
    cause = getattr(error, "__cause__", None)
    if cause is not None and isinstance(cause, Exception):
        return is_runtime_order_identity_conflict(cause)
    return False


def is_durable_order_identity_conflict(error: Exception) -> bool:
    message = str(error)
    if (
        "already exists in terminal status" in message
        or "already bound to a different order" in message
        or "is in non-dispatchable state" in message
        or "Execution command was not durably accepted" in message
        or "conflicts with its durable identity" in message
    ):
        return True
    cause = error.__cause__
    return isinstance(cause, Exception) and is_durable_order_identity_conflict(cause)
