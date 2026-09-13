import tracemalloc
from unittest.mock import patch

import crypto_momentum_lab.health.memory as memory
from crypto_momentum_lab.health.memory import (
    configure_tracemalloc,
    tracemalloc_memory_snapshot,
)


def test_tracemalloc_is_disabled_by_default() -> None:
    tracemalloc.stop()

    assert configure_tracemalloc({}) is False
    assert tracemalloc_memory_snapshot() == {
        "tracemalloc_enabled": False,
        "tracemalloc_current_bytes": None,
        "tracemalloc_peak_bytes": None,
    }


def test_tracemalloc_can_be_enabled_with_bounded_frame_count() -> None:
    tracemalloc.stop()
    try:
        assert configure_tracemalloc(
            {
                "CML_TRACEMALLOC": "true",
                "CML_TRACEMALLOC_FRAMES": "999",
            }
        ) is True

        snapshot = tracemalloc_memory_snapshot()
        assert snapshot["tracemalloc_enabled"] is True
        assert isinstance(snapshot["tracemalloc_current_bytes"], int)
        assert isinstance(snapshot["tracemalloc_peak_bytes"], int)
        assert tracemalloc.get_traceback_limit() == 25
    finally:
        tracemalloc.stop()


def test_cgroup_memory_snapshot_reads_current_and_limit(tmp_path) -> None:
    current = tmp_path / "current"
    limit = tmp_path / "limit"
    current.write_text("123\n")
    limit.write_text("456\n")

    with (
        patch.object(memory, "_CGROUP_MEMORY_CURRENT_PATHS", (current,)),
        patch.object(memory, "_CGROUP_MEMORY_LIMIT_PATHS", (limit,)),
    ):
        assert memory.cgroup_memory_snapshot() == {
            "cgroup_memory_current_bytes": 123,
            "cgroup_memory_limit_bytes": 456,
        }
