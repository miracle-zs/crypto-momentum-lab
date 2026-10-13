"""Incremental journal persistence: only append-only facts are re-sent."""

from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
from crypto_momentum_lab.domain.execution.execution_book import (
    ExecutionBook,
)
from crypto_momentum_lab.domain.execution.observation_models import (
    Applied,
    Duplicate,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.ports import ExecutionHeadSnapshot
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    ExitOrderSubmissionFact,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.recovery_models import JournalPersistResult
from crypto_momentum_lab.persistence.postgres.account_journal_store import (
    PostgresAccountJournalStore,
)

START = datetime(2026, 9, 29, tzinfo=UTC)


def _journal() -> tuple[AccountFactStreamScope, AccountJournal]:
    key = PositionKey(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="trade-source", stream_epoch="one"
    )
    return scope, AccountJournal(key, stream_scope=scope)


def _fill(index: int) -> AccountFillEvent:
    return AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id=str(index),
        order_id=f"order-{index}",
        side="BUY",
        price=Decimal("65000.25"),
        quantity=Decimal("0.001"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=START + timedelta(seconds=index),
        raw_payload={"positionSide": "BOTH"},
    )


def _snapshot(second: int) -> AccountPositionSnapshot:
    return AccountPositionSnapshot(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side="BOTH",
        position_amt=Decimal("1"),
        entry_price=Decimal("65000"),
        mark_price=Decimal("65001"),
        unrealized_pnl=Decimal("1"),
        notional=Decimal("65001"),
        leverage=1,
        margin_type="isolated",
        observed_at=START + timedelta(seconds=second),
        raw_payload={"positionSide": "BOTH"},
    )


class _RecordingResult:
    def __init__(self, rowcount: int) -> None:
        self.rowcount = rowcount


class _RecordingSession:
    """Minimal AsyncSession stand-in that records the bound INSERT rows."""

    def __init__(self) -> None:
        self.kinds: list[str] = []

    async def execute(self, statement: Any) -> _RecordingResult:
        rows = _bound_rows(statement)
        self.kinds.extend(row["event_kind"] for row in rows)
        return _RecordingResult(len(rows))


def _bound_rows(statement: Any) -> list[dict[str, Any]]:
    values = getattr(statement, "_values", None)
    if values is not None:
        groups = (values,)
    else:
        groups = getattr(statement, "_multi_values", ((),))[0]
    rows: list[dict[str, Any]] = []
    for group in groups:
        row: dict[str, Any] = {}
        for key, bound in group.items():
            name = key.name if hasattr(key, "name") else str(key)
            row[name] = getattr(bound, "value", bound)
        rows.append(row)
    return rows


def test_pending_delta_tracks_appends_until_persisted() -> None:
    _, journal = _journal()

    assert journal.pending_fact_delta().fills == ()
    journal.append_fill(_fill(1))
    journal.record_snapshot(_snapshot(2))

    delta = journal.pending_fact_delta()
    assert delta.fills == (_fill(1),)
    assert delta.snapshots == (_snapshot(2),)

    journal.mark_facts_persisted()

    cleared = journal.pending_fact_delta()
    assert cleared.fills == ()
    assert cleared.snapshots == ()
    # The full history stays available for projection and state-carrying rows.
    assert len(journal.read_cut().fills) == 1


def test_duplicate_fill_is_not_queued_twice() -> None:
    _, journal = _journal()

    assert journal.append_fill(_fill(1)) is True
    assert journal.append_fill(_fill(1)) is False

    assert journal.pending_fact_delta().fills == (_fill(1),)


def test_candidate_copy_owns_its_pending_delta() -> None:
    _, journal = _journal()
    journal.append_fill(_fill(1))
    journal.mark_facts_persisted()

    candidate = journal.copy_for_transaction()
    candidate.append_fill(_fill(2))

    assert candidate.pending_fact_delta().fills == (_fill(2),)
    assert journal.pending_fact_delta().fills == ()


async def test_persist_sends_only_the_state_row_when_delta_is_drained() -> None:
    scope, journal = _journal()
    for index in range(3):
        journal.append_fill(_fill(index))
    journal.record_snapshot(_snapshot(10))
    facts = journal.read_cut()

    first = _RecordingSession()
    await PostgresAccountJournalStore().persist_facts_in_session(
        first,
        scope=scope,
        facts=facts,
        revision=journal.revision,
        delta=journal.pending_fact_delta(),
    )
    journal.mark_facts_persisted()

    second = _RecordingSession()
    await PostgresAccountJournalStore().persist_facts_in_session(
        second,
        scope=scope,
        facts=facts,
        revision=journal.revision,
        delta=journal.pending_fact_delta(),
    )

    assert first.kinds.count("fill") == 3
    assert first.kinds.count("snapshot") == 1
    # A drained delta still writes the state-carrying row, nothing else.
    assert second.kinds == ["facts_state"]


async def test_persist_without_delta_keeps_the_full_history_write() -> None:
    scope, journal = _journal()
    for index in range(3):
        journal.append_fill(_fill(index))
    facts = journal.read_cut()

    session = _RecordingSession()
    await PostgresAccountJournalStore().persist_facts_in_session(
        session,
        scope=scope,
        facts=facts,
        revision=journal.revision,
    )

    assert session.kinds.count("fill") == 3
    assert session.kinds.count("facts_state") == 1


async def test_retry_resends_a_delta_that_was_never_cleared() -> None:
    scope, journal = _journal()
    journal.append_fill(_fill(1))
    journal.mark_facts_persisted()
    facts = journal.read_cut()

    journal.append_fill(_fill(2))
    pending = journal.pending_fact_delta()

    # Two attempts without a successful publish both re-send the same delta.
    for _ in range(2):
        session = _RecordingSession()
        await PostgresAccountJournalStore().persist_facts_in_session(
            session,
            scope=scope,
            facts=facts,
            revision=journal.revision,
            delta=pending,
        )
        assert session.kinds.count("fill") == 1


class _FakeTx:
    """Transaction stand-in that records the deltas it was asked to persist."""

    def __init__(self) -> None:
        self.persist_calls: list[dict[str, Any]] = []
        self.head: ExecutionHeadSnapshot | None = None

    async def load_head(self, key: Any) -> ExecutionHeadSnapshot | None:
        return self.head

    async def load_checkpoint_by_id(self, **kwargs: Any) -> None:
        return None

    async def record_evidence(self, **kwargs: Any) -> bool:
        return True

    async def record_trade(self, **kwargs: Any) -> bool:
        return True

    async def persist_watermark(self, **kwargs: Any) -> None:
        return None

    async def persist_head(self, **kwargs: Any) -> int:
        revision = kwargs["expected_revision"] + 1
        self.head = ExecutionHeadSnapshot(
            revision=revision,
            stream_id=kwargs["stream_id"],
            stream_epoch=kwargs["stream_epoch"],
            projection_version=kwargs["projection_version"],
            state_payload=kwargs["state_payload"],
        )
        return revision

    async def persist_facts(self, **kwargs: Any) -> JournalPersistResult:
        self.persist_calls.append(kwargs)
        return JournalPersistResult(
            inserted_count=1,
            duplicate_count=0,
            conflict_count=0,
            revision=kwargs["revision"],
        )


class _FakeUow:
    def __init__(self) -> None:
        self.tx = _FakeTx()

    @asynccontextmanager
    async def transaction(self, key: Any):
        yield self.tx


async def test_observe_persists_only_facts_recorded_since_the_last_commit() -> None:
    uow = _FakeUow()
    book = ExecutionBook(execution_unit_of_work=uow)
    book._persistence_failed = False
    scope = ExecutionScope(
        environment="live", account_label="primary", symbol="BTCUSDT"
    )

    first = await book.observe(
        ExecutionEvidence(
            evidence_id="ev-1",
            scope=scope,
            observed_at=START,
            fill=_fill(1),
            stream_id="trade-source",
            stream_epoch="one",
            sequence=1,
        )
    )
    assert isinstance(first, Applied)
    assert [call["delta"].fills for call in uow.tx.persist_calls] == [(_fill(1),)]

    second = await book.observe(
        ExecutionEvidence(
            evidence_id="ev-2",
            scope=scope,
            observed_at=START + timedelta(seconds=1),
            fill=_fill(2),
            stream_id="trade-source",
            stream_epoch="one",
            sequence=2,
        )
    )
    assert isinstance(second, Applied)

    # The published journal is the delta owner, so the published history was
    # cleared by _publish_candidate and only the new fill is re-sent.
    deltas = [call["delta"].fills for call in uow.tx.persist_calls]
    assert deltas == [(_fill(1),), (_fill(2),)]
    calls_before = len(uow.tx.persist_calls)
    dup = await book.observe(
        ExecutionEvidence(
            evidence_id="ev-3",
            scope=scope,
            observed_at=START + timedelta(seconds=2),
            fill=_fill(2),
            stream_id="trade-source",
            stream_epoch="one",
            sequence=3,
        )
    )
    assert isinstance(dup, Duplicate)
    assert len(uow.tx.persist_calls) == calls_before
    assert [call["delta"].fills for call in uow.tx.persist_calls] == deltas


def test_duplicate_snapshot_is_idempotent_and_different_payload_is_not_ignored() -> (
    None
):
    _, journal = _journal()
    snap = _snapshot(1)

    journal.record_snapshot(snap)
    assert journal.revision == 1
    assert journal.pending_fact_delta().snapshots == (snap,)
    assert len(journal.read_cut().snapshots) == 1

    # Equal value in distinct instance: revision does not increase, delta does not grow
    journal.record_snapshot(replace(snap))
    assert journal.revision == 1
    assert journal.pending_fact_delta().snapshots == (snap,)
    assert len(journal.read_cut().snapshots) == 1

    # Different payload: not treated as duplicate, appended
    different = replace(snap, position_amt=Decimal("2"))
    journal.record_snapshot(different)
    assert journal.revision == 2
    assert journal.pending_fact_delta().snapshots == (snap, different)
    assert len(journal.read_cut().snapshots) == 2


def test_duplicate_boundary_is_idempotent_and_different_payload_is_not_ignored() -> (
    None
):
    _, journal = _journal()
    boundary = ExitOrderSubmissionFact(
        order_id="boundary-1",
        submitted_at=START + timedelta(seconds=1),
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
        client_order_id="client-1",
        target_batch_id="batch-1",
    )

    journal.record_boundary(boundary)
    assert journal.revision == 1
    assert journal.pending_fact_delta().exit_boundaries == (boundary,)
    assert len(journal.read_cut().exit_boundaries) == 1

    # Equal value in distinct instance: revision does not increase, delta does not grow
    journal.record_boundary(replace(boundary))
    assert journal.revision == 1
    assert journal.pending_fact_delta().exit_boundaries == (boundary,)
    assert len(journal.read_cut().exit_boundaries) == 1

    # Different payload: not treated as duplicate, appended
    different = replace(boundary, target_batch_id="batch-2")
    journal.record_boundary(different)
    assert journal.revision == 2
    assert journal.pending_fact_delta().exit_boundaries == (boundary, different)
    assert len(journal.read_cut().exit_boundaries) == 2


def test_restoring_cut_rebuilds_indices_and_exact_distinct_instance_repeat_noops() -> (
    None
):
    _, journal = _journal()
    snap = _snapshot(1)
    boundary = ExitOrderSubmissionFact(
        order_id="boundary-1",
        submitted_at=START + timedelta(seconds=1),
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
        client_order_id="client-1",
        target_batch_id="batch-1",
    )

    journal.record_snapshot(snap)
    journal.record_boundary(boundary)

    # Package into a durable cut and restore to a fresh journal instance
    facts = journal.read_cut()
    from crypto_momentum_lab.domain.execution.recovery_models import DurableJournalCut

    cut = DurableJournalCut(
        scope=journal.stream_scope,
        as_of=START + timedelta(seconds=2),
        revision=journal.revision,
        facts=facts,
    )
    restored = AccountJournal.from_durable_cut(cut)

    # Distinct instance with exact equality is a no-op against rebuilt indices
    rev_before = restored.revision
    restored.record_snapshot(replace(snap))
    restored.record_boundary(replace(boundary))

    assert restored.revision == rev_before
    assert restored.pending_fact_delta().snapshots == ()
    assert restored.pending_fact_delta().exit_boundaries == ()
    assert len(restored.read_cut().snapshots) == 1
    assert len(restored.read_cut().exit_boundaries) == 1


def test_candidate_rollback_leaves_original_indices_and_delta_unaffected() -> None:
    _, journal = _journal()
    snap = _snapshot(1)
    boundary = ExitOrderSubmissionFact(
        order_id="boundary-1",
        submitted_at=START + timedelta(seconds=1),
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
        client_order_id="client-1",
        target_batch_id="batch-1",
    )
    journal.record_snapshot(snap)
    journal.record_boundary(boundary)
    journal.mark_facts_persisted()

    original_rev = journal.revision
    candidate = journal.copy_for_transaction()

    # Candidate adds a new payload variant for the same keys
    candidate_snap_var = replace(snap, position_amt=Decimal("5"))
    candidate_boundary_var = replace(boundary, target_batch_id="candidate-batch")
    candidate.record_snapshot(candidate_snap_var)
    candidate.record_boundary(candidate_boundary_var)

    assert candidate.revision == original_rev + 2
    assert candidate.pending_fact_delta().snapshots == (candidate_snap_var,)
    assert candidate.pending_fact_delta().exit_boundaries == (candidate_boundary_var,)

    # Rollback discard: original was untouched and contains no candidate variants
    assert journal.revision == original_rev
    assert journal.pending_fact_delta().snapshots == ()
    assert journal.pending_fact_delta().exit_boundaries == ()
    assert snap.observed_at in journal._snapshots_by_key
    assert len(journal._snapshots_by_key[snap.observed_at]) == 1
    assert boundary.order_id in journal._boundaries_by_key
    assert len(journal._boundaries_by_key[boundary.order_id]) == 1

    # Later, original independently appends a distinct new payload variant
    orig_snap_var = replace(snap, position_amt=Decimal("9"))
    orig_boundary_var = replace(boundary, target_batch_id="orig-batch")
    journal.record_snapshot(orig_snap_var)
    journal.record_boundary(orig_boundary_var)

    assert journal.revision == original_rev + 2
    assert journal.pending_fact_delta().snapshots == (orig_snap_var,)
    assert journal.pending_fact_delta().exit_boundaries == (orig_boundary_var,)
    assert len(journal.read_cut().snapshots) == 2
    assert len(journal.read_cut().exit_boundaries) == 2
    assert len(journal._snapshots_by_key[snap.observed_at]) == 2
    assert len(journal._boundaries_by_key[boundary.order_id]) == 2
