from dataclasses import dataclass
from datetime import UTC, datetime
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
    await second_collector.initialize()
    health = await second_collector.health()

    assert health.last_sequence == 10
    assert health.pending_spool_files == 0
    assert health.last_persisted_bucket == state.bucket_start


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
    with pytest.raises(CollectorPaused):
        await collector.ingest(_batch(state, 2))
    assert not check_health(tmp_path, "research")[0]
    (tmp_path / "quota-fill").unlink()
    await collector.ingest(_batch(state, 2))
    assert check_health(tmp_path, "research")[0]
    await collector.stop()
    assert not check_health(tmp_path, "research")[0]
