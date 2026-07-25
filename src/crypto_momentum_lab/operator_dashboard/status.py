from datetime import datetime
from enum import StrEnum


class OperationalStatus(StrEnum):
    UNKNOWN = "UNKNOWN"
    FRESH = "FRESH"
    STALE = "STALE"
    READY = "READY"
    HALTED = "HALTED"
    SHADOW = "SHADOW"
    LIVE = "LIVE"
    DOWN = "DOWN"
    NO_DATA = "NO DATA"


def freshness_status(
    *,
    now: datetime,
    observed_at: datetime | None,
    stale_after_seconds: float,
) -> OperationalStatus:
    if observed_at is None:
        return OperationalStatus.UNKNOWN
    if (now - observed_at).total_seconds() > stale_after_seconds:
        return OperationalStatus.STALE
    return OperationalStatus.FRESH
