import asyncio
import time
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from crypto_momentum_lab.market_data.hub import MarketStateBatch
from crypto_momentum_lab.research_collector.models import (
    CollectionBatch,
    CollectorConfig,
    CollectorPaused,
    SourceKind,
)
from crypto_momentum_lab.research_collector.selection import StaticSymbolSelector
from crypto_momentum_lab.research_collector.service import ResearchStateCollector
from crypto_momentum_lab.research_collector.storage import (
    CapacityGuard,
    CapacitySnapshot,
    CapacityState,
)
from tests.unit.persistence.postgres.test_runtime_state_repository import (
    fixture_state,
)


def _batch(state, sequence: int) -> CollectionBatch:
    return CollectionBatch(
        batch=MarketStateBatch(
            sequence=sequence,
            published_at=state.bucket_end,
            environment=state.environment,
            states=(state,),
            stream_id="test-stream",
        )
    )


class _DummyUsage:
    def __init__(self, free: int) -> None:
        self.free = free


def test_capacity_guard_cached_snapshot_and_incremental_writes(tmp_path: Path) -> None:
    guard = CapacityGuard(
        root=tmp_path,
        soft_limit_bytes=1000,
        hard_limit_bytes=2000,
        global_warning_free_bytes=500,
        global_pause_free_bytes=100,
        disk_usage_fn=lambda p: _DummyUsage(free=10000),
        max_snapshot_age_seconds=10.0,
    )
    # First snapshot triggers scan
    snap = guard.snapshot()
    assert snap.state is CapacityState.HEALTHY
    assert snap.collector_bytes == 0
    assert snap.observed_at is not None
    assert not snap.is_degraded

    # Incrementally record writes
    guard.record_written_bytes(1200)
    current = guard.current_snapshot()
    assert current.collector_bytes == 1200
    assert current.state is CapacityState.WARNING  # >= soft_limit 1000

    guard.record_written_bytes(900)
    current2 = guard.current_snapshot()
    assert current2.collector_bytes == 2100
    assert current2.state is CapacityState.PAUSED  # >= hard_limit 2000

    with pytest.raises(CollectorPaused, match="collector_bytes=2100"):
        guard.ensure_writable()


def test_capacity_guard_snapshot_expiry_degradation(tmp_path: Path) -> None:
    guard = CapacityGuard(
        root=tmp_path,
        soft_limit_bytes=1000,
        hard_limit_bytes=2000,
        global_warning_free_bytes=500,
        global_pause_free_bytes=100,
        disk_usage_fn=lambda p: _DummyUsage(free=10000),
        max_snapshot_age_seconds=1.0,
    )
    guard.scan()
    now = time.monotonic()

    # Normal age (< 1.0s) -> HEALTHY
    snap = guard.current_snapshot(now=now + 0.5)
    assert snap.state is CapacityState.HEALTHY
    assert not snap.is_degraded

    # Expired (> 1.0s) -> WARNING with degradation flag
    snap_exp = guard.current_snapshot(now=now + 1.5)
    assert snap_exp.state is CapacityState.WARNING
    assert snap_exp.is_degraded
    assert "expired" in snap_exp.degraded_reason

    # Critically expired (> 3.0s) -> PAUSED
    snap_crit = guard.current_snapshot(now=now + 3.5)
    assert snap_crit.state is CapacityState.PAUSED
    assert snap_crit.is_degraded
    assert "critically expired" in snap_crit.degraded_reason

    with pytest.raises(CollectorPaused, match="critically expired"):
        guard.ensure_writable(now=now + 3.5)


async def test_collector_ingest_does_not_scan_directory(tmp_path: Path) -> None:
    """F10 verification: ingest() uses cached capacity snapshot without directory scan."""
    config = CollectorConfig(
        environment="research",
        root=tmp_path,
        soft_limit_bytes=1024**2,
        hard_limit_bytes=2 * 1024**2,
        global_warning_free_bytes=2,
        global_pause_free_bytes=1,
        window_seconds=15,
        late_tolerance_seconds=0,
        capacity_check_interval_seconds=60.0,
    )

    collector = ResearchStateCollector(
        config=config,
        source=MagicMock(),
        selector=StaticSymbolSelector(frozenset({"BTCUSDT"})),
    )
    await collector.initialize()

    # Spy on _capacity.scan
    scan_count = 0
    orig_scan = collector._capacity.scan

    def mock_scan():
        nonlocal scan_count
        scan_count += 1
        return orig_scan()

    collector._capacity.scan = mock_scan

    # Ingest 5 batches rapidly
    for i in range(5):
        state = fixture_state("BTCUSDT", i)
        await collector.ingest(_batch(state, i + 1))

    # scan() should NOT have been called during ingest (it was called once in initialize)
    assert scan_count == 0, f"Expected 0 scans during ingest, got {scan_count}"
    await collector.stop()


def test_capacity_guard_concurrent_writes_during_scan_do_not_double_count(tmp_path: Path) -> None:
    import crypto_momentum_lab.research_collector.storage as storage_mod

    guard = CapacityGuard(
        root=tmp_path,
        soft_limit_bytes=10000,
        hard_limit_bytes=20000,
        global_warning_free_bytes=500,
        global_pause_free_bytes=100,
        disk_usage_fn=lambda p: _DummyUsage(free=100000),
    )
    guard.scan()

    orig_dir_size = storage_mod._directory_size

    def simulated_scan_with_concurrent_write(path):
        # A file of 350 bytes is written to disk and recorded during directory traversal
        (tmp_path / "concurrent.dat").write_bytes(b"x" * 350)
        guard.record_written_bytes(350)
        return orig_dir_size(path)

    storage_mod._directory_size = simulated_scan_with_concurrent_write
    try:
        guard.scan()
    finally:
        storage_mod._directory_size = orig_dir_size

    current = guard.current_snapshot()
    # The 350 bytes written during traversal must be reported as 350 bytes, never double-counted to 700
    assert current.collector_bytes == 350

    # Subsequent incremental writes after scan are properly tracked under lock
    guard.record_written_bytes(150)
    current_after = guard.current_snapshot()
    assert current_after.collector_bytes == 500

