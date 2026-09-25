from pathlib import Path

import pyarrow.parquet as pq

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.market_data.hub import MarketStateBatch
from crypto_momentum_lab.research_collector.journal import ArchiveJournal
from crypto_momentum_lab.research_collector.materializer import (
    WindowMaterializer,
)
from crypto_momentum_lab.research_collector.models import (
    CollectionBatch,
    SelectedSymbol,
    SelectionSnapshot,
    SourceKind,
)
from crypto_momentum_lab.research_collector.storage import ParquetWindowSink
from tests.unit.persistence.postgres.test_runtime_state_repository import (
    fixture_state,
)


def _batch(state: MarketState15s, sequence: int) -> CollectionBatch:
    return CollectionBatch(
        batch=MarketStateBatch(
            sequence=sequence,
            published_at=state.bucket_end,
            environment=state.environment,
            states=(state,),
            stream_id="test-stream",
        ),
        source_kind=SourceKind.HUB,
    )


def test_materializer_stages_and_flushes_to_parquet(tmp_path: Path) -> None:
    journal = ArchiveJournal(
        tmp_path / "journal",
        environment="research",
        max_bytes=1024 * 1024,
    )
    sink = ParquetWindowSink(
        tmp_path / "parquet",
        window_seconds=15,
        late_tolerance_seconds=0,
    )
    materializer = WindowMaterializer(sink=sink, journal=journal)

    s1 = fixture_state("BTCUSDT", 0)
    s2 = fixture_state("BTCUSDT", 1)
    selection = SelectionSnapshot(
        observed_at=s1.bucket_start,
        symbols=(SelectedSymbol(symbol="BTCUSDT", reason="test"),),
    )

    # Accept in journal
    b1 = _batch(s1, 1)
    journal.accept(b1, selection, (s1,))

    # Empty batch
    b2_empty = _batch(s2, 2)
    journal.accept(b2_empty, selection, ())

    b3 = _batch(s2, 3)
    journal.accept(b3, selection, (s2,))

    pending = journal.pending_records()
    assert len(pending) == 3

    # Stage all 3 records into materializer
    for record in pending:
        materializer.stage_record(record)

    # Flush all
    flush_result = materializer.flush_all()
    assert flush_result.committed_rows == 2
    assert flush_result.files_written == 2
    assert len(flush_result.committed_receipts) == 3
    assert journal.materialized_sequence == 3
    assert len(journal.pending_records()) == 0

    # Verify Parquet files exist
    parquet_files = list(tmp_path.joinpath("parquet").rglob("*.parquet"))
    assert len(parquet_files) == 2

    # Read back table and assert rows
    rows = pq.ParquetFile(parquet_files[0]).read().to_pylist()
    assert len(rows) == 1
    assert rows[0]["symbol"] == "BTCUSDT"


def test_materializer_does_not_commit_unwritten_conflicting_revision(tmp_path: Path) -> None:
    from dataclasses import replace
    from decimal import Decimal

    journal = ArchiveJournal(
        tmp_path / "journal",
        environment="research",
        max_bytes=1024 * 1024,
    )
    sink = ParquetWindowSink(
        tmp_path / "parquet",
        window_seconds=15,
        late_tolerance_seconds=0,
    )
    materializer = WindowMaterializer(sink=sink, journal=journal)

    s1 = fixture_state("BTCUSDT", 0)
    s2_conflict = replace(s1, close_price=Decimal("102"))
    selection = SelectionSnapshot(
        observed_at=s1.bucket_start,
        symbols=(SelectedSymbol(symbol="BTCUSDT", reason="test"),),
    )

    # 1. Accept first revision (close=100)
    b1 = _batch(s1, 1)
    journal.accept(b1, selection, (s1,))

    # 2. Accept conflicting revision with same key but different payload (close=102)
    b2 = _batch(s2_conflict, 2)
    journal.accept(b2, selection, (s2_conflict,))

    pending = journal.pending_records()
    assert len(pending) == 2

    # Stage both
    materializer.stage_record(pending[0])
    append_res2 = materializer.stage_record(pending[1])
    assert append_res2 is not None
    assert append_res2.conflicting_rows == 1  # Rejected by sink due to same priority

    # Flush all
    flush_result = materializer.flush_all()

    # The unwritten revision b2 must NOT be in committed_receipts
    committed_seqs = {r.sequence for r in flush_result.committed_receipts}
    assert committed_seqs == {1}
    assert 2 not in committed_seqs

    # Journal must not advance past sequence 1
    assert journal.materialized_sequence == 1
    # Unwritten record 2 must remain pending in journal
    remaining_pending = journal.pending_records()
    assert len(remaining_pending) == 1
    assert remaining_pending[0].receipt.sequence == 2

    # The parquet file must contain close=100
    parquet_files = list(tmp_path.joinpath("parquet").rglob("*.parquet"))
    assert len(parquet_files) == 1
    rows = pq.ParquetFile(parquet_files[0]).read().to_pylist()
    assert len(rows) == 1
    assert rows[0]["close_price"] == "100"

