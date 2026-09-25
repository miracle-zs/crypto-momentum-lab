from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.market_data.hub import (
    MarketStateBatch,
    MarketStateHubReplayUnavailable,
)
from crypto_momentum_lab.research_collector.models import (
    CollectionBatch,
    CollectorCheckpoint,
    CollectorConfig,
)
from crypto_momentum_lab.research_collector.selection import (
    StaticSymbolSelector,
)
from crypto_momentum_lab.research_collector.service import (
    ResearchStateCollector,
)
from crypto_momentum_lab.research_collector.storage import CheckpointStore
from tests.unit.persistence.postgres.test_runtime_state_repository import (
    fixture_state,
)


async def _empty_batches():
    if False:
        yield MarketStateBatch(
            sequence=1,
            published_at=datetime.now(UTC),
            environment="research",
            states=(),
        )


class _IdleSource:
    def batches(self):
        return _empty_batches()


class _RecoverySource(_IdleSource):
    def __init__(self) -> None:
        self.resume_calls: list[tuple[str, int]] = []

    def set_resume_cursor(
        self,
        *,
        stream_id: str | None,
        sequence: int | None,
    ) -> None:
        return None

    def resume_after_recovery(self, *, stream_id: str, sequence: int) -> None:
        self.resume_calls.append((stream_id, sequence))


@dataclass
class _BackfillSource:
    state: MarketState15s
    calls: list[tuple[object, object]]

    async def latest_bucket(self) -> datetime:
        return self.state.bucket_start

    async def batches_after(self, cursor, *, until):
        self.calls.append((cursor, until))
        yield MarketStateBatch(
            sequence=0,
            published_at=self.state.bucket_end,
            environment=self.state.environment,
            states=(self.state,),
            stream_id=None,
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


async def test_collector_filters_and_checkpoints_after_window_flush(
    tmp_path: Path,
) -> None:
    config = CollectorConfig(
        environment="research",
        root=tmp_path,
        soft_limit_bytes=1024**2,
        hard_limit_bytes=2 * 1024**2,
        global_warning_free_bytes=2,
        global_pause_free_bytes=1,
        window_seconds=15,
        late_tolerance_seconds=0,
        max_spool_bytes=1024**2,
    )
    collector = ResearchStateCollector(
        config=config,
        source=_IdleSource(),
        selector=StaticSymbolSelector(frozenset({"BTCUSDT"})),
    )
    first = fixture_state("BTCUSDT", 0)
    second = fixture_state("BTCUSDT", 1)
    skipped = fixture_state("ETHUSDT", 1)

    first_receipt = await collector.ingest(_batch(first, 1))
    skipped_receipt = await collector.ingest(_batch(skipped, 2))
    second_receipt = await collector.ingest(_batch(second, 3))

    health = await collector.health()
    assert first_receipt.selected_rows == 1
    assert skipped_receipt.selected_rows == 0
    assert skipped_receipt.skipped_rows == 1
    assert second_receipt.selected_rows == 1
    assert health.last_sequence == 2
    assert health.pending_spool_files == 1
    await collector.stop()
    health = await collector.health()
    assert health.last_sequence == 3
    assert health.pending_spool_files == 0
    assert health.last_persisted_bucket == second.bucket_start
    assert len(tuple(tmp_path.joinpath("parquet").rglob("*.parquet"))) == 2


async def test_collector_replays_pending_spool_after_restart(tmp_path: Path) -> None:
    config = CollectorConfig(
        environment="research",
        root=tmp_path,
        soft_limit_bytes=1024**2,
        hard_limit_bytes=2 * 1024**2,
        global_warning_free_bytes=2,
        global_pause_free_bytes=1,
        max_spool_bytes=1024**2,
    )
    first_collector = ResearchStateCollector(
        config=config,
        source=_IdleSource(),
        selector=StaticSymbolSelector(frozenset({"BTCUSDT"})),
    )
    state = fixture_state("BTCUSDT", 0)
    await first_collector.ingest(_batch(state, 10))

    second_collector = ResearchStateCollector(
        config=config,
        source=_IdleSource(),
        selector=StaticSymbolSelector(frozenset({"BTCUSDT"})),
    )
    try:
        await second_collector.initialize()
        health = await second_collector.health()

        assert health.last_sequence == 10
        assert health.pending_spool_files == 0
        assert health.last_persisted_bucket == state.bucket_start
    finally:
        await first_collector.stop()
        await second_collector.stop()


async def test_collector_recovers_after_hub_stream_reset(tmp_path: Path) -> None:
    config = CollectorConfig(
        environment="research",
        root=tmp_path,
        soft_limit_bytes=1024**2,
        hard_limit_bytes=2 * 1024**2,
        global_warning_free_bytes=2,
        global_pause_free_bytes=1,
        max_spool_bytes=1024**2,
    )
    first = fixture_state("BTCUSDT", 0)
    recovered = fixture_state("BTCUSDT", 1)
    CheckpointStore(tmp_path / "checkpoints" / "research.json").save(
        CollectorCheckpoint(
            environment="research",
            stream_id="stream-a",
            last_sequence=7,
            last_bucket_start=first.bucket_start,
            last_symbol=first.symbol,
        )
    )
    source = _RecoverySource()
    backfill = _BackfillSource(recovered, [])
    collector = ResearchStateCollector(
        config=config,
        source=source,
        selector=StaticSymbolSelector(frozenset({"BTCUSDT"})),
        backfill_source=backfill,
    )
    try:
        await collector.initialize()

        await collector._recover_replay_gap(
            MarketStateHubReplayUnavailable(
                "market-state replay is unavailable: Hub stream reset",
                requested_sequence=7,
                latest_sequence=1,
                stream_id="stream-b",
            )
        )

        health = await collector.health()
        assert len(backfill.calls) == 1
        assert source.resume_calls == [("stream-b", 1)]
        assert health.last_sequence == 1
        assert health.last_persisted_bucket == recovered.bucket_start
        assert health.pending_spool_files == 0
    finally:
        await collector.stop()


async def test_collector_records_market_state_time_gap(tmp_path: Path) -> None:
    config = CollectorConfig(
        environment="research",
        root=tmp_path,
        soft_limit_bytes=1024**2,
        hard_limit_bytes=2 * 1024**2,
        global_warning_free_bytes=2,
        global_pause_free_bytes=1,
        window_seconds=15,
        late_tolerance_seconds=0,
        max_spool_bytes=1024**2,
    )
    collector = ResearchStateCollector(
        config=config,
        source=_IdleSource(),
        selector=StaticSymbolSelector(frozenset({"BTCUSDT"})),
    )
    try:
        first = fixture_state("BTCUSDT", 0)
        after_gap = fixture_state("BTCUSDT", 2)
        await collector.ingest(_batch(first, 1))
        await collector.ingest(_batch(after_gap, 2))

        health = await collector.health()
        assert health.market_state_gap_count == 1
        assert health.last_market_state_gap_start == first.bucket_start + timedelta(
            seconds=15
        )
        assert health.last_market_state_gap_end == first.bucket_start + timedelta(
            seconds=15
        )
        assert health.last_market_state_gap_buckets == 1
        assert health.selected_rows == 2
    finally:
        await collector.stop()


async def test_health_marker_tracks_durable_progress_pause_and_stop(tmp_path):
    import pytest

    from crypto_momentum_lab.research_collector.health import check_health
    from crypto_momentum_lab.research_collector.models import CollectorPaused

    config = CollectorConfig(
        environment="research",
        root=tmp_path,
        soft_limit_bytes=1024**2,
        hard_limit_bytes=2 * 1024**2,
        global_warning_free_bytes=2,
        global_pause_free_bytes=1,
        window_seconds=15,
        late_tolerance_seconds=0,
    )
    collector = ResearchStateCollector(
        config=config,
        source=_IdleSource(),
        selector=StaticSymbolSelector(frozenset({"BTCUSDT"})),
    )
    assert not check_health(tmp_path, "research")[0]
    state = fixture_state("BTCUSDT", 0)
    await collector.ingest(_batch(state, 1))
    assert check_health(tmp_path, "research")[0]
    # A failed capacity guard must never publish a fresh successful checkpoint.
    (tmp_path / "quota-fill").write_bytes(b"x" * (2 * 1024**2))
    await collector.refresh_capacity()
    with pytest.raises(CollectorPaused):
        await collector.ingest(_batch(state, 2))
    assert not check_health(tmp_path, "research")[0]
    (tmp_path / "quota-fill").unlink()
    await collector.refresh_capacity()
    await collector.ingest(_batch(state, 2))
    assert check_health(tmp_path, "research")[0]
    await collector.stop()
    assert not check_health(tmp_path, "research")[0]


async def test_dual_progress_crash_recovery_and_journal_backpressure(
    tmp_path: Path,
) -> None:
    import pytest

    from crypto_momentum_lab.research_collector.models import CollectorPaused

    # Set a small journal max_bytes to test backpressure
    config = CollectorConfig(
        environment="research",
        root=tmp_path,
        soft_limit_bytes=10 * 1024**2,
        hard_limit_bytes=20 * 1024**2,
        global_warning_free_bytes=2,
        global_pause_free_bytes=1,
        window_seconds=15,
        late_tolerance_seconds=100,  # Ensure no automatic window flush
        max_spool_bytes=3500,  # Fits 2 records (2652 bytes) before backpressure
    )
    collector = ResearchStateCollector(
        config=config,
        source=_IdleSource(),
        selector=StaticSymbolSelector(frozenset({"BTCUSDT"})),
    )
    s1 = fixture_state("BTCUSDT", 0)
    s2 = fixture_state("BTCUSDT", 1)
    s3 = fixture_state("BTCUSDT", 2)

    restarted = None
    try:
        await collector.ingest(_batch(s1, 1))
        await collector.ingest(_batch(s2, 2))

        health = await collector.health()
        # At this point, batches are in journal but unmaterialized due to late tolerance
        assert health.accepted_sequence == 2
        assert health.materialized_sequence is None
        assert health.pending_spool_files == 2
        assert health.pending_spool_bytes > 0

        # Ingesting s3 triggers journal byte limit (2000 bytes)
        with pytest.raises(CollectorPaused):
            await collector.ingest(_batch(s3, 3))

        # Simulate crash by dropping collector without calling stop()!
        # A fresh collector restarts against the same root directory.
        restarted = ResearchStateCollector(
            config=config,
            source=_IdleSource(),
            selector=StaticSymbolSelector(frozenset({"BTCUSDT"})),
        )
        # On initialize, recovered records should be staged and flushed durably!
        await restarted.initialize()

        restarted_health = await restarted.health()
        # Checkpoint has advanced to include recovered sequences
        assert restarted_health.accepted_sequence == 2
        assert restarted_health.materialized_sequence == 2
        assert restarted_health.last_sequence == 2
        assert restarted_health.pending_spool_files == 0
        assert restarted_health.pending_spool_bytes == 0

        # Backpressure is now relieved because pending bytes dropped to 0!
        # Ingesting s3 now succeeds!
        r3 = await restarted.ingest(_batch(s3, 3))
        assert r3.selected_rows == 1
        assert (await restarted.health()).accepted_sequence == 3
    finally:
        await collector.stop()
        if restarted is not None:
            await restarted.stop()


async def test_empty_selection_receipt_survives_restart_recovery(
    tmp_path: Path,
) -> None:
    config = CollectorConfig(
        environment="research",
        root=tmp_path,
        soft_limit_bytes=10 * 1024**2,
        hard_limit_bytes=20 * 1024**2,
        global_warning_free_bytes=2,
        global_pause_free_bytes=1,
        window_seconds=15,
        late_tolerance_seconds=100,  # Ensure no automatic window flush before crash
    )
    # Only select BTCUSDT
    collector = ResearchStateCollector(
        config=config,
        source=_IdleSource(),
        selector=StaticSymbolSelector(frozenset({"BTCUSDT"})),
    )
    restarted = None
    try:
        s_btc = fixture_state("BTCUSDT", 0)
        s_eth = fixture_state("ETHUSDT", 0)

        # 1. Ingest BTC batch -> selected
        r1 = await collector.ingest(_batch(s_btc, 1))
        assert r1.selected_rows == 1

        # 2. Ingest ETH batch -> empty selection, writes empty receipt to journal
        r2 = await collector.ingest(_batch(s_eth, 2))
        assert r2.selected_rows == 0

        health = await collector.health()
        assert health.pending_spool_files == 2

        # 3. Simulate crash before materialization: create fresh collector
        restarted = ResearchStateCollector(
            config=config,
            source=_IdleSource(),
            selector=StaticSymbolSelector(frozenset({"BTCUSDT"})),
        )
        # On initialize, recovery must not fail with empty batch error
        await restarted.initialize()

        restarted_health = await restarted.health()
        assert restarted_health.accepted_sequence == 2
        assert restarted_health.materialized_sequence == 2
        assert restarted_health.pending_spool_files == 0

        # Ingest sequence 3 after recovery
        s_btc2 = fixture_state("BTCUSDT", 1)
        r3 = await restarted.ingest(_batch(s_btc2, 3))
        assert r3.selected_rows == 1
        assert (await restarted.health()).accepted_sequence == 3
    finally:
        await collector.stop()
        if restarted is not None:
            await restarted.stop()


async def test_recovered_journal_sequence_gap_raises(tmp_path: Path) -> None:
    import pytest

    from crypto_momentum_lab.research_collector.models import CollectorSequenceGap

    config = CollectorConfig(
        environment="research",
        root=tmp_path,
        soft_limit_bytes=10 * 1024**2,
        hard_limit_bytes=20 * 1024**2,
        global_warning_free_bytes=2,
        global_pause_free_bytes=1,
        window_seconds=15,
        late_tolerance_seconds=100,
    )
    collector = ResearchStateCollector(
        config=config,
        source=_IdleSource(),
        selector=StaticSymbolSelector(frozenset({"BTCUSDT"})),
    )
    restarted = None
    try:
        s1 = fixture_state("BTCUSDT", 0)
        s2 = fixture_state("BTCUSDT", 1)
        s3 = fixture_state("BTCUSDT", 2)

        await collector.ingest(_batch(s1, 1))
        await collector.ingest(_batch(s2, 2))
        await collector.ingest(_batch(s3, 3))

        # Artificially remove sequence 2 from the pending journal on disk
        pending_files = list((tmp_path / "journal" / "pending" / "hub").glob("**/*.json"))
        deleted = False
        for f in pending_files:
            import json

            data = json.loads(f.read_text())
            if data.get("sequence") == 2:
                f.unlink()
                deleted = True
                break
        assert deleted

        # Restart: initialize should detect gap 1 -> 3 in pending journal
        restarted = ResearchStateCollector(
            config=config,
            source=_IdleSource(),
            selector=StaticSymbolSelector(frozenset({"BTCUSDT"})),
        )
        with pytest.raises(
            CollectorSequenceGap, match="recovered journal has sequence gap"
        ):
            await restarted.initialize()
    finally:
        await collector.stop()
        if restarted is not None:
            await restarted.stop()


async def test_collector_pipeline_decoupled_ingress_and_queue_backpressure(
    tmp_path: Path,
) -> None:
    import asyncio
    import time

    import pytest

    from crypto_momentum_lab.research_collector.models import CollectorPaused


    config = CollectorConfig(
        environment="research",
        root=tmp_path,
        soft_limit_bytes=10 * 1024**2,
        hard_limit_bytes=20 * 1024**2,
        global_warning_free_bytes=2,
        global_pause_free_bytes=1,
        window_seconds=15,
        late_tolerance_seconds=1,  # Short backpressure timeout
        max_queue_batches=2,       # Small queue capacity to test backpressure
    )
    collector = ResearchStateCollector(
        config=config,
        source=_IdleSource(),
        selector=StaticSymbolSelector(frozenset({"BTCUSDT"})),
    )
    await collector.initialize()

    # Block the materializer from consuming by intercepting stage_record
    stage_hold = asyncio.Event()
    real_stage = collector._materializer.stage_record

    def holding_stage(record):
        # Synchronously block until released
        while not stage_hold.is_set():
            time.sleep(0.01)
        return real_stage(record)

    collector._materializer.stage_record = holding_stage  # type: ignore[assignment]

    s1 = fixture_state("BTCUSDT", 0)
    s2 = fixture_state("BTCUSDT", 1)
    s3 = fixture_state("BTCUSDT", 2)

    # 1. Ingest batch 1: accepted to journal, worker gets it and blocks in stage_record
    r1 = await collector.ingest(_batch(s1, 1))
    assert r1.durable_receipt is not None
    assert r1.durable_receipt.sequence == 1
    assert r1.durable_receipt.record_id != ""

    # Ingest batch 2 and 3: fill the queue of maxsize=2
    r2 = await collector.ingest(_batch(s2, 2))
    assert r2.durable_receipt is not None

    # Wait for queue to have pending batch
    await asyncio.sleep(0.05)

    # Ingest batch 3: should fill the queue (since worker is busy with batch 1)
    # The queue now holds batch 2 and batch 3. Attempting to ingest batch 4
    # times out and raises CollectorPaused!
    s4 = fixture_state("BTCUSDT", 3)
    await collector.ingest(_batch(s3, 3))

    with pytest.raises(CollectorPaused, match="Materializer queue backpressure"):
        await collector.ingest(_batch(s4, 4))

    # Release materializer hold
    stage_hold.set()

    # Drain queue
    await collector.drain_queue(timeout_seconds=5.0)
    health = await collector.health()
    assert health.queued_batches == 0
    assert health.accepted_sequence == 3

    # Now that queue is drained, batch 4 can be ingested without backpressure
    r4 = await collector.ingest(_batch(s4, 4))
    assert r4.durable_receipt is not None
    assert r4.durable_receipt.sequence == 4

    await collector.stop()

