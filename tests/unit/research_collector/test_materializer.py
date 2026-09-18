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
