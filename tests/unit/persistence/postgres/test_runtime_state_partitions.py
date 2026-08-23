from datetime import UTC, datetime

import pytest

from crypto_momentum_lab.persistence.postgres.runtime_state_partitions import (
    floor_runtime_state_partition_start,
    runtime_state_partition_name,
)


def test_runtime_state_partition_start_uses_utc_six_hour_boundaries() -> None:
    value = datetime(2026, 8, 23, 17, 42, 9, tzinfo=UTC)

    assert floor_runtime_state_partition_start(value) == datetime(
        2026,
        8,
        23,
        12,
        tzinfo=UTC,
    )


def test_runtime_state_partition_name_is_deterministic() -> None:
    assert runtime_state_partition_name(
        datetime(2026, 8, 23, 17, 42, tzinfo=UTC)
    ) == "runtime_market_states_15s_p_20260823_1200"


def test_runtime_state_partition_start_rejects_naive_datetime() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        floor_runtime_state_partition_start(datetime(2026, 8, 23, 17))
