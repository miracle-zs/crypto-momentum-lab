from datetime import UTC, datetime, timedelta

from crypto_momentum_lab.operator_dashboard.status import (
    OperationalStatus,
    freshness_status,
)

NOW = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)


def test_status_unknown_when_timestamp_missing() -> None:
    assert (
        freshness_status(now=NOW, observed_at=None, stale_after_seconds=30)
        is OperationalStatus.UNKNOWN
    )


def test_status_stale_when_age_exceeds_threshold() -> None:
    assert (
        freshness_status(
            now=NOW,
            observed_at=NOW - timedelta(seconds=31),
            stale_after_seconds=30,
        )
        is OperationalStatus.STALE
    )
