from datetime import UTC, datetime
from pathlib import Path

from crypto_momentum_lab.market_data.hub import MarketStateBatch
from crypto_momentum_lab.research_collector.models import (
    CollectionBatch,
    CollectorConfig,
)
from crypto_momentum_lab.research_collector.selection import (
    StaticSymbolSelector,
)
from crypto_momentum_lab.research_collector.service import (
    ResearchStateCollector,
)
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
