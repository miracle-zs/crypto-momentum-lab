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
