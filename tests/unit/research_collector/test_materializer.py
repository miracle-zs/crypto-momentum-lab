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


def test_materializer_handles_conflicting_revision_without_hanging_pending(tmp_path: Path) -> None:
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
    assert len(append_res2.accepted_version_keys) == 0
    assert len(append_res2.dropped_version_keys) == 1

    # Flush all
    flush_result = materializer.flush_all()

    # Both receipts must be committed so rejected receipts do not hang pending forever
    committed_seqs = {r.sequence for r in flush_result.committed_receipts}
    assert committed_seqs == {1, 2}

    # Journal advances without being blocked by dropped revision
    assert journal.materialized_sequence == 2
    assert len(journal.pending_records()) == 0

    # Durable resolutions on disk prove whether each revision was materialized or rejected
    resolutions = journal.read_resolutions()
    assert len(resolutions) == 2
    res_map = {r["sequence"]: r for r in resolutions}
    assert res_map[1]["status"] == "materialized"
    assert res_map[2]["status"] == "rejected"
    assert res_map[2]["reason"] == "conflict_dropped"
    assert res_map[2]["dropped_keys_count"] == 1

    # The parquet file retains close=100
    parquet_files = list(tmp_path.joinpath("parquet").rglob("*.parquet"))
    assert len(parquet_files) == 1
    rows = pq.ParquetFile(parquet_files[0]).read().to_pylist()
    assert len(rows) == 1
    assert rows[0]["close_price"] == "100"


def test_materializer_commits_upgraded_backfill_revision(tmp_path: Path) -> None:
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
    selection = SelectionSnapshot(
        observed_at=s1.bucket_start,
        symbols=(SelectedSymbol(symbol="BTCUSDT", reason="test"),),
    )

    # 1. Accept hub revision
    b1 = _batch(s1, 1)
    journal.accept(b1, selection, (s1,))

    # 2. Accept backfill revision with higher priority
    s2_backfill = replace(s1, close_price=Decimal("105"), trade_count=s1.trade_count + 10)
    b2 = CollectionBatch(
        batch=MarketStateBatch(
            sequence=2,
            published_at=s2_backfill.bucket_end,
            environment=s2_backfill.environment,
            states=(s2_backfill,),
            stream_id="test-stream",
        ),
        source_kind=SourceKind.POSTGRES_BACKFILL,
    )
    journal.accept(b2, selection, (s2_backfill,))

    pending = journal.pending_records()
    assert len(pending) == 2

    # Stage both
    materializer.stage_record(pending[0])
    append_res2 = materializer.stage_record(pending[1])
    assert append_res2 is not None
    assert append_res2.upgraded_rows == 1
    assert len(append_res2.accepted_version_keys) == 1

    # Flush all
    flush_result = materializer.flush_all()
    committed_seqs = {r.sequence for r in flush_result.committed_receipts}
    assert committed_seqs == {1, 2}
    assert len(journal.pending_records()) == 0

    # The parquet file contains upgraded row close=105 and source_kind=postgres_backfill
    parquet_files = list(tmp_path.joinpath("parquet").rglob("*.parquet"))
    assert len(parquet_files) == 1
    rows = pq.ParquetFile(parquet_files[0]).read().to_pylist()
    assert len(rows) == 1
    assert rows[0]["close_price"] == "105"
    assert rows[0]["source_kind"] == SourceKind.POSTGRES_BACKFILL.value


def test_materializer_resolutions_persisted_before_journal_unlink(tmp_path: Path) -> None:
    journal = ArchiveJournal(
        tmp_path / "journal",
        environment="research",
        max_bytes=1024 * 1024,
    )
    s1 = fixture_state("BTCUSDT", 0)
    selection = SelectionSnapshot(
        observed_at=s1.bucket_start,
        symbols=(SelectedSymbol(symbol="BTCUSDT", reason="test"),),
    )
    b1 = _batch(s1, 1)
    r1 = journal.accept(b1, selection, (s1,))

    resolutions_seen_before_unlink = []
    orig_unlink = Path.unlink

    def mock_unlink(self_path):
        resolutions_file = tmp_path / "journal" / "resolutions.jsonl"
        resolutions_seen_before_unlink.append(resolutions_file.exists())
        orig_unlink(self_path)

    import unittest.mock
    with unittest.mock.patch.object(Path, "unlink", mock_unlink):
        journal.commit_materialization(
            [r1],
            resolutions=[{"sequence": 1, "status": "materialized", "reason": "accepted"}],
        )

    assert len(resolutions_seen_before_unlink) == 1
    assert resolutions_seen_before_unlink[0] is True
    assert len(journal.read_resolutions()) == 1



