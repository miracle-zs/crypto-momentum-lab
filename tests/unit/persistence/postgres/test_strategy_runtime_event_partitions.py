"""Unit tests for strategy runtime-event daily partition helpers."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from crypto_momentum_lab.persistence.postgres.strategy_runtime_event_partitions import (
    EVENT_PARTITION_PREFIX,
    event_partition_name,
    floor_event_partition_start,
)


def test_floor_event_partition_start_floors_to_utc_midnight() -> None:
    value = datetime(2026, 9, 16, 15, 42, 13, 999000, tzinfo=UTC)
    assert floor_event_partition_start(value) == datetime(
        2026, 9, 16, 0, 0, tzinfo=UTC
    )


def test_event_partition_name_uses_compact_day() -> None:
    value = datetime(2026, 9, 16, 23, 59, 59, tzinfo=UTC)
    assert event_partition_name(value) == f"{EVENT_PARTITION_PREFIX}20260916"


def test_floor_rejects_naive_datetime() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        floor_event_partition_start(datetime(2026, 9, 16, 12, 0, 0))
