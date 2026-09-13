from dataclasses import replace
from datetime import datetime
from decimal import Decimal

import pyarrow.parquet as pq

from crypto_momentum_lab.market_data.hub import MarketStateBatch
from crypto_momentum_lab.research_collector.models import (
    CollectionBatch,
    SelectedSymbol,
    SelectionSnapshot,
    SourceKind,
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


def _batch(
    state,
    sequence: int,
    source_kind: SourceKind = SourceKind.HUB,
) -> CollectionBatch:
    return CollectionBatch(
        batch=MarketStateBatch(
            sequence=sequence,
            published_at=state.bucket_end,
            environment=state.environment,
            states=(state,),
            stream_id="test-stream",
        ),
        source_kind=source_kind,
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


def test_parquet_sink_deduplicates_and_keeps_existing_on_payload_conflict(
    tmp_path,
) -> None:
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

    # A republished bucket with a different payload must not stop collection:
    # the already-archived row wins and the mismatch is counted, not raised.
    changed = replace(state, close_price=Decimal("102"))
    conflicting = sink.append(_batch(changed, 3), selection)
    assert conflicting.selected_rows == 0
    assert conflicting.duplicate_rows == 0
    assert conflicting.conflicting_rows == 1

    paths = tuple(tmp_path.joinpath("parquet").rglob("*.parquet"))
    assert len(paths) == 1
    # The original row is still the only one stored for that key.
    rows = pq.ParquetFile(paths[0]).read().to_pylist()
    assert len(rows) == 1
    assert Decimal(rows[0]["close_price"]) == state.close_price


def test_parquet_sink_backfill_supersedes_hub_row(tmp_path) -> None:
    """A durable backfill copy supersedes the thinner hub copy."""

    state = fixture_state("BTCUSDT", 0)
    selection = _selection(state.symbol, state.bucket_start)
    sink = ParquetWindowSink(
        tmp_path / "parquet",
        window_seconds=15,
        late_tolerance_seconds=0,
    )

    # The hub copy lands first (1s realtime closure) and is already flushed.
    sink.append(_batch(state, 1, SourceKind.HUB), selection)
    sink.flush_all()

    # Gap recovery then replays the durable row (3s closure) for the same key
    # with a fuller payload; it must win.
    recovered = replace(
        state,
        close_price=Decimal("102"),
        trade_count=state.trade_count + 3,
    )
    upgraded = sink.append(
        _batch(recovered, 2, SourceKind.POSTGRES_BACKFILL),
        selection,
    )
    assert upgraded.upgraded_rows == 1
    assert upgraded.conflicting_rows == 0
    sink.flush_all()

    paths = tuple(tmp_path.joinpath("parquet").rglob("*.parquet"))
    assert len(paths) == 1
    rows = pq.ParquetFile(paths[0]).read().to_pylist()
    assert len(rows) == 1
    assert Decimal(rows[0]["close_price"]) == Decimal("102")
    assert rows[0]["source_kind"] == SourceKind.POSTGRES_BACKFILL.value

    # The reverse direction must NOT overwrite the durable row.
    thinner = replace(state, close_price=Decimal("99"))
    rejected = sink.append(_batch(thinner, 3, SourceKind.HUB), selection)
    assert rejected.conflicting_rows == 1
    assert rejected.upgraded_rows == 0
    assert len(pq.ParquetFile(paths[0]).read().to_pylist()) == 1
