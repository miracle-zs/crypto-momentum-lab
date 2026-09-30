"""Shared failure classification and retry defaults for live exit channels."""

DEFAULT_PENDING_POSITION_RETRY_DELAYS_SECONDS = (
    0.25,
    0.5,
    1.0,
    2.0,
    4.0,
    8.0,
    16.0,
    32.0,
)
ORDER_IDENTITY_CONFLICT_REASON = "order_identity_conflict"



def is_pending_position_sync_failure(failure: str | None) -> bool:
    return bool(failure is not None and failure.startswith("pending_live_positions:"))


def promote_pending_position_failure(failure: str) -> str:
    if not is_pending_position_sync_failure(failure):
        return failure
    return failure.replace(
        "pending_live_positions:",
        "unmanaged_live_positions:",
        1,
    )

