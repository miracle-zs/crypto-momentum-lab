import json
from pathlib import Path

import pytest

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.market_data.hub import (
    MarketStateBatch,
    market_state_to_payload,
)
from crypto_momentum_lab.research_collector.journal import ArchiveJournal
from crypto_momentum_lab.research_collector.models import (
    CollectionBatch,
    CollectorPaused,
    CollectorStateConflict,
    SelectionSnapshot,
    SourceKind,
)
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


def test_journal_accepts_non_empty_and_empty_batches(tmp_path: Path) -> None:
    journal = ArchiveJournal(
        tmp_path / "journal",
        environment="research",
        max_bytes=1024 * 1024,
    )
    state = fixture_state("BTCUSDT", 0)
    batch1 = _batch(state, 1)
    selection = SelectionSnapshot(
        observed_at=state.bucket_start,
        symbols=(),
    )

    # 1. Accept normal batch
    receipt1 = journal.accept(batch1, selection, (state,))
    assert not receipt1.is_empty
    assert receipt1.sequence == 1
    assert receipt1.stream_id == "test-stream"
    assert journal.accepted_sequence == 1
    assert journal.pending_bytes > 0
    initial_bytes = journal.pending_bytes

    # 2. Accept empty selection batch
    empty_batch = _batch(state, 2)
    receipt2 = journal.accept(empty_batch, selection, ())
    assert receipt2.is_empty
    assert receipt2.sequence == 2
    assert journal.accepted_sequence == 2
    assert journal.pending_bytes > initial_bytes

    pending = journal.pending_records()
    assert len(pending) == 2
    assert pending[0].receipt == receipt1
    assert pending[1].receipt == receipt2


def test_journal_enforces_capacity(tmp_path: Path) -> None:
    journal = ArchiveJournal(
        tmp_path / "journal",
        environment="research",
        max_bytes=200,  # Very small quota
    )
    state = fixture_state("BTCUSDT", 0)
    batch = _batch(state, 1)
    selection = SelectionSnapshot(observed_at=state.bucket_start, symbols=())

    with pytest.raises(CollectorPaused, match="journal limit reached"):
        journal.accept(batch, selection, (state,))


def test_journal_commit_materialization_and_advances_sequence(
    tmp_path: Path,
) -> None:
    journal = ArchiveJournal(
        tmp_path / "journal",
        environment="research",
        max_bytes=1024 * 1024,
    )
    s1 = fixture_state("BTCUSDT", 0)
    s2 = fixture_state("BTCUSDT", 1)
    b1 = _batch(s1, 1)
    b2 = _batch(s2, 2)
    sel = SelectionSnapshot(observed_at=s1.bucket_start, symbols=())

    r1 = journal.accept(b1, sel, (s1,))
    r2 = journal.accept(b2, sel, (s2,))

    assert len(journal.pending_records()) == 2
    assert journal.accepted_sequence == 2
    assert journal.materialized_sequence is None

    # Commit first batch
    journal.commit_materialization(
        [r1],
        last_bucket_start=s1.bucket_start,
        last_symbol=s1.symbol,
    )
    assert len(journal.pending_records()) == 1
    assert journal.materialized_sequence == 1
    assert journal.last_materialized_bucket == s1.bucket_start
    assert journal.last_materialized_symbol == s1.symbol

    # Commit second batch
    journal.commit_materialization(
        [r2],
        last_bucket_start=s2.bucket_start,
        last_symbol=s2.symbol,
    )
    assert len(journal.pending_records()) == 0
    assert journal.materialized_sequence == 2
    assert journal.pending_bytes == 0


def test_journal_recovery_cleans_tmp_and_reads_legacy_spool(
    tmp_path: Path,
) -> None:
    journal_dir = tmp_path / "journal"
    legacy_spool_dir = tmp_path / "spool" / "pending" / "hub"
    legacy_spool_dir.mkdir(parents=True, exist_ok=True)

    # 1. Write an unfinished tmp file in journal
    tmp_file = journal_dir / "pending" / "hub" / ".part.12345.tmp"
    tmp_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_file.write_text("partial data", encoding="utf-8")

    # 2. Write a legacy spool file (schema_version 1)
    state = fixture_state("BTCUSDT", 0)
    legacy_record = {
        "schema_version": 1,
        "source_kind": "hub",
        "sequence": 5,
        "stream_id": "legacy-stream",
        "published_at": state.bucket_end.isoformat(),
        "environment": "research",
        "states": [market_state_to_payload(state)],
        "selection": {"observed_at": state.bucket_start.isoformat(), "symbols": []},
    }
    import json

    (legacy_spool_dir / "legacy.json").write_text(
        json.dumps(legacy_record), encoding="utf-8"
    )

    # 3. Recover journal
    journal = ArchiveJournal(
        journal_dir,
        environment="research",
        max_bytes=1024 * 1024,
    )
    recovered = journal.recover(legacy_spool_root=tmp_path / "spool" / "pending")

    # Verify tmp file was deleted
    assert not tmp_file.exists()

    # Verify legacy record was cleanly recovered
    assert len(recovered) == 1
    assert recovered[0].receipt.sequence == 5
    assert recovered[0].receipt.stream_id == "legacy-stream"
    assert journal.accepted_sequence == 5
    assert journal.pending_bytes > 0


def test_postgres_backfill_batches_have_unique_record_ids_and_selective_commit(
    tmp_path: Path,
) -> None:
    journal_dir = tmp_path / "journal"
    journal = ArchiveJournal(
        journal_dir,
        environment="research",
        max_bytes=1024 * 1024,
    )
    s1 = fixture_state("BTCUSDT", 0)
    s2 = fixture_state("ETHUSDT", 0)

    # Two PostgreSQL backfill batches have sequence=0, stream_id=None
    b1 = CollectionBatch(
        batch=MarketStateBatch(
            sequence=0,
            published_at=s1.bucket_end,
            environment="research",
            states=(s1,),
            stream_id=None,
        ),
        source_kind=SourceKind.POSTGRES_BACKFILL,
    )
    b2 = CollectionBatch(
        batch=MarketStateBatch(
            sequence=0,
            published_at=s2.bucket_end,
            environment="research",
            states=(s2,),
            stream_id=None,
        ),
        source_kind=SourceKind.POSTGRES_BACKFILL,
    )
    sel = SelectionSnapshot(observed_at=s1.bucket_start, symbols=())

    r1 = journal.accept(b1, sel, (s1,))
    r2 = journal.accept(b2, sel, (s2,))

    assert r1.record_id != ""
    assert r2.record_id != ""
    assert r1.record_id != r2.record_id
    assert len(journal.pending_records()) == 2

    # Commit only the first backfill receipt
    journal.commit_materialization([r1])

    # The second backfill receipt must NOT have been deleted! (P1-A fix)
    pending = journal.pending_records()
    assert len(pending) == 1
    assert pending[0].receipt.record_id == r2.record_id

    # Restart / recover journal from disk
    new_journal = ArchiveJournal(
        journal_dir,
        environment="research",
        max_bytes=1024 * 1024,
    )
    recovered = new_journal.recover()
    assert len(recovered) == 1
    assert recovered[0].receipt.record_id == r2.record_id


def test_out_of_order_commit_advances_only_contiguous_materialized_sequence(
    tmp_path: Path,
) -> None:
    journal = ArchiveJournal(
        tmp_path / "journal",
        environment="research",
        max_bytes=1024 * 1024,
    )
    s1 = fixture_state("BTCUSDT", 0)
    s2 = fixture_state("BTCUSDT", 1)
    s3 = fixture_state("BTCUSDT", 2)
    b1 = _batch(s1, 1)
    b2 = _batch(s2, 2)
    b3 = _batch(s3, 3)
    sel = SelectionSnapshot(observed_at=s1.bucket_start, symbols=())

    r1 = journal.accept(b1, sel, (s1,))
    r2 = journal.accept(b2, sel, (s2,))
    r3 = journal.accept(b3, sel, (s3,))

    assert journal.accepted_sequence == 3
    assert journal.materialized_sequence is None

    # 1. Commit batch 1 -> materialized_sequence advances to 1
    journal.commit_materialization([r1])
    assert journal.materialized_sequence == 1

    # 2. Commit batch 3 while batch 2 is still pending!
    # Contiguous prefix must remain 1 because batch 2 has not materialized!
    journal.commit_materialization([r3])
    assert journal.materialized_sequence == 1

    # 3. Commit batch 2 -> contiguous prefix now covers all up to 3!
    journal.commit_materialization([r2])
    assert journal.materialized_sequence == 3


def test_recover_fails_closed_on_corrupted_manifest(tmp_path: Path) -> None:
    journal_dir = tmp_path / "journal"
    journal = ArchiveJournal(
        journal_dir,
        environment="research",
        max_bytes=1024 * 1024,
    )
    # 1. Corrupted JSON (syntax error)
    manifest_file = journal_dir / "manifest.json"
    manifest_file.write_text('{"environment": "research", "highest_committed_sequence": ', encoding="utf-8")
    with pytest.raises(CollectorStateConflict, match="cannot read collector manifest"):
        journal.recover()

    # 2. Corrupted schema (not a dict)
    manifest_file.write_text("[1, 2, 3]", encoding="utf-8")
    with pytest.raises(CollectorStateConflict, match="corrupted collector manifest.*expected dict"):
        journal.recover()

    # 3. Corrupted values (non-integer sequence, float, boolean)
    manifest_file.write_text('{"environment": "research", "highest_committed_sequence": "invalid"}', encoding="utf-8")
    with pytest.raises(CollectorStateConflict, match="must be an integer"):
        journal.recover()

    manifest_file.write_text('{"environment": "research", "highest_committed_sequence": 1.9}', encoding="utf-8")
    with pytest.raises(CollectorStateConflict, match="must be an integer"):
        journal.recover()

    manifest_file.write_text('{"environment": "research", "highest_committed_sequence": true}', encoding="utf-8")
    with pytest.raises(CollectorStateConflict, match="must be an integer"):
        journal.recover()

    # 4. Environment mismatch
    manifest_file.write_text('{"environment": "production", "highest_committed_sequence": 10}', encoding="utf-8")
    with pytest.raises(CollectorStateConflict, match="collector manifest environment mismatch"):
        journal.recover()


def test_recover_and_read_resolutions_fails_closed_on_corrupted_resolutions(tmp_path: Path) -> None:
    journal_dir = tmp_path / "journal"
    journal = ArchiveJournal(
        journal_dir,
        environment="research",
        max_bytes=1024 * 1024,
    )
    res_file = journal_dir / "resolutions.jsonl"

    # 1. Corrupted JSON syntax on a line
    res_file.write_text('{"record_id": "rec-1", "sequence": 1}\n{"corrupted": line\n', encoding="utf-8")
    with pytest.raises(CollectorStateConflict, match="corrupted materialization resolution.*line 2"):
        journal.recover()
    with pytest.raises(CollectorStateConflict, match="corrupted materialization resolution.*line 2"):
        journal.read_resolutions()

    # 2. Non-dict JSON on a line
    res_file.write_text('["not", "a", "dict"]\n', encoding="utf-8")
    with pytest.raises(CollectorStateConflict, match="expected dict"):
        journal.recover()

    # 3. Non-integer sequence on a line (string, float 1.9, boolean)
    res_file.write_text('{"record_id": "rec-1", "sequence": "not-an-int"}\n', encoding="utf-8")
    with pytest.raises(CollectorStateConflict, match="must be an integer"):
        journal.recover()

    res_file.write_text('{"record_id": "rec-1", "sequence": 1.9}\n', encoding="utf-8")
    with pytest.raises(CollectorStateConflict, match="must be an integer"):
        journal.recover()

    res_file.write_text('{"record_id": "rec-1", "sequence": true}\n', encoding="utf-8")
    with pytest.raises(CollectorStateConflict, match="must be an integer"):
        journal.recover()


def test_recover_deformed_float_sequence_resolution_does_not_delete_pending_record(tmp_path: Path) -> None:
    """Ensure deformed sequence=1.9 resolution does NOT get truncated to int(1) and delete sequence=1 record."""
    journal_dir = tmp_path / "journal"
    journal = ArchiveJournal(
        journal_dir,
        environment="research",
        max_bytes=1024 * 1024,
    )
    state = fixture_state("BTCUSDT", 0)
    batch = _batch(state, sequence=1)
    selection = SelectionSnapshot(observed_at=state.bucket_start, symbols=())

    # 1. Accept valid pending record sequence=1
    receipt = journal.accept(batch, selection, (state,))
    assert len(journal.pending_records()) == 1
    pending_files = list((journal_dir / "pending").rglob("*.json"))
    assert len(pending_files) == 1
    pending_file = pending_files[0]
    assert pending_file.exists()

    # 2. Introduce deformed resolution with sequence=1.9 on same stream
    res_file = journal_dir / "resolutions.jsonl"
    res_file.write_text(
        json.dumps({
            "source_kind": "hub",
            "stream_id": "test-stream",
            "sequence": 1.9,
            "status": "materialized",
        }) + "\n",
        encoding="utf-8",
    )

    # 3. Recovery must FAIL CLOSED with CollectorStateConflict rather than truncating 1.9 -> 1
    new_journal = ArchiveJournal(
        journal_dir,
        environment="research",
        max_bytes=1024 * 1024,
    )
    with pytest.raises(CollectorStateConflict, match="must be an integer, got float"):
        new_journal.recover()

    # 4. Critical: The legitimate pending record for sequence=1 MUST NOT HAVE BEEN DELETED!
    assert pending_file.exists()


