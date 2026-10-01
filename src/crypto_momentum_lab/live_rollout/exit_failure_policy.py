"""Shared failure classification and retry defaults for live exit channels."""

ORDER_IDENTITY_CONFLICT_REASON = "order_identity_conflict"


def is_pending_position_sync_failure(failure: str | None) -> bool:
    return bool(failure is not None and failure.startswith("pending_live_positions:"))


def is_pending_context_refresh(failure: str | None) -> bool:
    return bool(failure is not None and failure.startswith("pending_live_context:"))


def is_pending_exit_evaluation(failure: str | None) -> bool:
    """An exit awaits current account/context facts, without an execution fault."""
    return (
        is_pending_position_sync_failure(failure)
        or is_pending_context_refresh(failure)
        or bool(
            failure is not None and failure.startswith("pending_exit_order_recovery:")
        )
    )
