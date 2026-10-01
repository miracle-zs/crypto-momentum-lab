"""Shared failure classification and retry defaults for live exit channels."""

ORDER_IDENTITY_CONFLICT_REASON = "order_identity_conflict"



def is_pending_position_sync_failure(failure: str | None) -> bool:
    return bool(failure is not None and failure.startswith("pending_live_positions:"))


def is_pending_candle_evaluation(failure: str | None) -> bool:
    """A closing event waits for position facts or an unresolved exit receipt."""
    return is_pending_position_sync_failure(failure) or bool(
        failure is not None and failure.startswith("pending_exit_order_recovery:")
    )
