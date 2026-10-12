"""Phase-D acceptance: pruning must not change what recovery produces.

Builds a real multi-checkpoint journal through the durable store, records the
recovered facts, runs the real retention passes with every row eligible, and
asserts recovery afterwards yields the identical facts hash, batches, total
quantity and health status from the retained checkpoint plus its suffix.
"""

import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import asyncpg  # type: ignore[import-untyped]
import pytest
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
from crypto_momentum_lab.domain.execution.position_book import PositionBook
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFillLoadProvenance,
    CoverageEvidence,
    compose_fact_coverage,
)
from crypto_momentum_lab.domain.execution.position_recovery import (
    create_verified_recovery_checkpoint,
)
from crypto_momentum_lab.persistence.postgres.account_journal_store import (
    PostgresAccountJournalStore,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
    ExecutionBookHeadRow,
)
from crypto_momentum_lab.persistence.postgres.position_fact_journal_models import (
    PositionFactJournalEventRow,
    PositionRecoveryCheckpointRow,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)
from crypto_momentum_lab.tools import prune_position_recovery_history as pruner
from tests.unit.execution.test_position_recovery import _key, _scope, _time
from tests.unit.persistence.postgres.test_checkpoint_snapshot_recovery import (
    _checkpoint,
    _snapshot,
)

pytestmark = pytest.mark.integration

SCANS = 3


def _asyncpg_url() -> str:
    url = os.environ.get(
        "CML_TEST_ASYNC_DATABASE_URL",
        "postgresql+asyncpg://cml:cml@localhost:54329/cml_test",
    )
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)


@pytest.fixture
async def journal_sessions(
    async_database_url: str,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    key = _key()

    async def _clear() -> None:
        async with factory() as session:
            async with session.begin():
                for model in (
                    PositionFactJournalEventRow,
                    PositionRecoveryCheckpointRow,
                    ExecutionBookHeadRow,
                ):
                    await session.execute(
                        delete(model).where(
                            model.account_label == key.account_label
                        )
                    )

    await _clear()
    yield factory
    await _clear()
    await engine.dispose()


async def _persist_scans(factory: async_sessionmaker[AsyncSession]) -> None:
    """Persist facts plus one verified checkpoint per scan, as live does."""
    store = PostgresAccountJournalStore()
    initial = _checkpoint()
    scope = initial.stream_scope
    async with factory() as session, session.begin():
        await store.save_checkpoint_in_session(session, initial)
    for index in range(1, SCANS + 1):
        async with factory() as session, session.begin():
            # The checkpoint row records the wall-clock save time, so the read
            # as-of must be in the present; the facts themselves stay at _time.
            cut = await store.load_recovery_in_session(
                session, scope=scope, as_of=datetime.now(UTC) + timedelta(seconds=1)
            )
            journal = AccountJournal.from_durable_cut(cut)
            parent = cut.checkpoint
            end = initial.event_cut + timedelta(seconds=10 * index)
            provenance = AccountFillLoadProvenance(
                stream_scope=scope,
                load_id=f"scan-{index}",
                scan_origin_from_id=None,
                scan_origin_start_time_ms=int(parent.event_cut.timestamp() * 1000),
                request_from_id=None,
                next_from_id=None,
                page_count=1,
                page_exhausted=True,
                truncated=False,
                checked_through=end,
                observed_at=end,
                source_anchor_id=parent.checkpoint_id,
                source_anchor_event_cut=parent.event_cut,
                source_anchor_kind="recovery_checkpoint",
            )
            proof = CoverageEvidence(
                fill_cursor_id=f"scan-{index}",
                fill_load_start=parent.event_cut,
                fill_checked_through=end,
                checkpoint_id=f"proof-{index}",
                checkpoint_event_cut=end,
                stream_scope=scope,
                evidence_observed_at=end,
                page_exhausted=True,
                not_truncated=True,
                load_provenance=provenance,
            )
            journal.record_snapshot(_snapshot(initial.key, end, "2", "10"))
            journal.record_fill_load_provenance(provenance)
            journal.set_coverage(
                compose_fact_coverage(
                    proof, start=parent.event_cut, end=end, expected_scope=scope
                )
            )
            checkpoint = create_verified_recovery_checkpoint(
                key=initial.key,
                scope=scope,
                journal=journal,
                proof=proof,
                provenance=provenance,
                adoption=None,
                adopting_epoch=False,
                # One checkpoint per scan: every superseded one must be
                # prunable while the newest still recovers the same facts.
                force_checkpoint=True,
            )
            assert checkpoint is not None
            journal.set_recovery_checkpoint(checkpoint)
            await store.persist_facts_in_session(
                session,
                scope=scope,
                facts=journal.read_cut(),
                revision=journal.revision,
                delta=journal.pending_fact_delta(),
            )


async def _recover(factory: async_sessionmaker[AsyncSession]):
    store = PostgresAccountJournalStore()
    async with factory() as session:
        return await store.load_recovery_in_session(
            session,
            scope=_scope(_key()),
            as_of=datetime.now(UTC) + timedelta(seconds=1),
        )


async def _run_pruner_fully_eligible() -> None:
    """Every recorded row is older than the (future) cutoff."""
    connection = await asyncpg.connect(_asyncpg_url())
    try:
        cutoff = datetime.now(UTC) + timedelta(hours=1)
        await pruner._prepare_protected_checkpoints(connection)
        await pruner._materialize_checkpoint_candidates(
            connection, cutoff=cutoff, limit=1000
        )
        await pruner._delete_checkpoint_batches(connection, batch_size=10)
        await pruner._prepare_retained_checkpoint_cuts(connection)
        await pruner._materialize_state_event_candidates(
            connection, cutoff=cutoff, limit=1000
        )
        await pruner._delete_state_event_batches(connection, batch_size=10)
    finally:
        await connection.close()


async def test_pruning_preserves_recovered_facts_and_quantities(
    journal_sessions: async_sessionmaker[AsyncSession],
) -> None:
    factory = journal_sessions
    await _persist_scans(factory)
    before_cut = await _recover(factory)
    before_journal = AccountJournal.from_durable_cut(before_cut)
    before_view = PositionBook(before_journal).get_view(
        now=_time(0) + timedelta(seconds=120)
    )
    before_hash = before_journal.read_cut().compute_facts_hash()

    async with factory() as session:
        checkpoints_before = set(
            (
                await session.scalars(
                    select(PositionRecoveryCheckpointRow.checkpoint_id).where(
                        PositionRecoveryCheckpointRow.account_label
                        == _key().account_label
                    )
                )
            ).all()
        )
        events_before = await session.scalar(
            select(func.count())
            .select_from(PositionFactJournalEventRow)
            .where(
                PositionFactJournalEventRow.account_label == _key().account_label
            )
        )

    assert len(checkpoints_before) == SCANS + 1

    await _run_pruner_fully_eligible()

    after_cut = await _recover(factory)
    after_journal = AccountJournal.from_durable_cut(after_cut)
    after_view = PositionBook(after_journal).get_view(
        now=_time(0) + timedelta(seconds=120)
    )

    async with factory() as session:
        checkpoints_after = set(
            (
                await session.scalars(
                    select(PositionRecoveryCheckpointRow.checkpoint_id).where(
                        PositionRecoveryCheckpointRow.account_label
                        == _key().account_label
                    )
                )
            ).all()
        )
        events_after = await session.scalar(
            select(func.count())
            .select_from(PositionFactJournalEventRow)
            .where(
                PositionFactJournalEventRow.account_label == _key().account_label
            )
        )

    # History actually shrank, but recovery is byte-for-byte identical.
    assert len(checkpoints_after) == 1
    assert events_after < events_before
    assert before_cut.checkpoint is not None
    assert after_cut.checkpoint is not None
    # The checkpoint recovery already used is the one retention kept.
    assert after_cut.checkpoint.checkpoint_id == before_cut.checkpoint.checkpoint_id
    assert checkpoints_after == {after_cut.checkpoint.checkpoint_id}
    assert after_cut.revision == before_cut.revision
    assert after_journal.read_cut().compute_facts_hash() == before_hash
    assert after_view.batches == before_view.batches
    assert after_view.total_quantity == before_view.total_quantity
    assert after_view.health_status == before_view.health_status
