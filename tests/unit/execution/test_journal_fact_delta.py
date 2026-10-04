"""Incremental journal persistence: only append-only facts are re-sent."""

from contextlib import asynccontextmanager
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
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
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

    async def load_head(self, key: Any) -> None:
        return None

    async def load_checkpoint_by_id(self, **kwargs: Any) -> None:
        return None

    async def record_evidence(self, **kwargs: Any) -> bool:
        return True

    async def record_trade(self, **kwargs: Any) -> bool:
        return True

    async def persist_watermark(self, **kwargs: Any) -> None:
        return None

    async def persist_head(self, **kwargs: Any) -> int:
        return int(kwargs.get("revision") or 0)

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
    await book.observe(ExecutionEvidence(
        evidence_id="ev-3", scope=scope, observed_at=START + timedelta(seconds=2),
        fill=_fill(2), stream_id="trade-source", stream_epoch="one", sequence=3,
    ))
    assert uow.tx.persist_calls[-1]["delta"].fills == ()
