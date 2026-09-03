from dataclasses import replace
from datetime import datetime
from decimal import Decimal

import pyarrow.parquet as pq
import pytest

from crypto_momentum_lab.market_data.hub import MarketStateBatch
from crypto_momentum_lab.research_collector.models import (
    CollectionBatch,
    CollectorStateConflict,
    SelectedSymbol,
    SelectionSnapshot,
)
from crypto_momentum_lab.research_collector.storage import (
    LocalBatchSpool,
    ParquetWindowSink,
)
from tests.unit.persistence.postgres.test_runtime_state_repository import (
    fixture_state,
)


def _selection(symbol: str, observed_at: datetime) -> SelectionSnapshot:
    return SelectionSnapshot(
        observed_at=observed_at,
        symbols=(SelectedSymbol(symbol=symbol, reason="test"),),
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


def test_local_spool_round_trips_selected_batch(tmp_path) -> None:
    state = fixture_state("BTCUSDT", 0)
    spool = LocalBatchSpool(tmp_path / "spool", max_bytes=1024**2)
    selection = _selection(state.symbol, state.bucket_start)

    record = spool.write(_batch(state, 1), selection, (state,))
    loaded = spool.pending_records()

    assert len(loaded) == 1
    assert loaded[0].path == record.path
    assert loaded[0].collection_batch.states == (state,)
    assert loaded[0].selection == selection


def test_parquet_sink_deduplicates_and_detects_payload_conflict(tmp_path) -> None:
    state = fixture_state("BTCUSDT", 0)
    selection = _selection(state.symbol, state.bucket_start)
    sink = ParquetWindowSink(
        tmp_path / "parquet",
        window_seconds=15,
        late_tolerance_seconds=0,
    )

    first = sink.append(_batch(state, 1), selection)
    committed = sink.flush_all()
    duplicate = sink.append(_batch(state, 2), selection)

    assert first.selected_rows == 1
    assert committed.committed_rows == 1
    assert duplicate.selected_rows == 0
    assert duplicate.duplicate_rows == 1

    changed = replace(state, close_price=Decimal("102"))
    with pytest.raises(CollectorStateConflict, match="different payloads"):
        sink.append(_batch(changed, 3), selection)

    paths = tuple(tmp_path.joinpath("parquet").rglob("*.parquet"))
    assert len(paths) == 1
    assert pq.ParquetFile(paths[0]).metadata.num_rows == 1
