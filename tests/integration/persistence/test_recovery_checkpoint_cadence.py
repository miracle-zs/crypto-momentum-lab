from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select

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
from crypto_momentum_lab.persistence.postgres.position_fact_journal_models import (
    PositionFactJournalEventRow,
    PositionRecoveryCheckpointRow,
)
from tests.integration.persistence.test_shared_policy_evidence import (
    evidence_sessions,  # noqa: F401
)
from tests.unit.persistence.postgres.test_checkpoint_snapshot_recovery import (
    _checkpoint,
    _snapshot,
)

pytestmark = pytest.mark.integration


async def test_repeated_scans_bound_checkpoints_and_restore_every_observation(
    evidence_sessions,  # noqa: F811 - imported pytest fixture
):
    store = PostgresAccountJournalStore()
    initial = _checkpoint()
    scope = initial.stream_scope
    async with evidence_sessions() as session, session.begin():
        await store.save_checkpoint_in_session(session, initial)
    # Restart from PostgreSQL on every step, preventing an in-memory timer or
    # cache from hiding missing durable evidence or resetting the budget.
    for index in range(1, 36):
        async with evidence_sessions() as session, session.begin():
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
            )
            if checkpoint is not None:
                journal.set_recovery_checkpoint(checkpoint)
            before = PositionBook(journal).get_view(now=end)
            assert before.health_status.value == "READY"
            await store.persist_facts_in_session(
                session,
                scope=scope,
                facts=journal.read_cut(),
                revision=journal.revision,
                delta=journal.pending_fact_delta(),
            )
        async with evidence_sessions() as session:
            restored = await store.load_recovery_in_session(
                session, scope=scope, as_of=datetime.now(UTC) + timedelta(seconds=1)
            )
            after = PositionBook(AccountJournal.from_durable_cut(restored)).get_view(
                now=end
            )
            assert after.batches == before.batches
            assert after.coverage == before.coverage
            assert after.event_cut == before.event_cut
            assert after.health_status == before.health_status
    async with evidence_sessions() as session:
        checkpoint_count = await session.scalar(
            select(func.count()).select_from(PositionRecoveryCheckpointRow)
        )
        assert 2 <= checkpoint_count <= 5
        for kind in ("snapshot", "coverage", "fill_load_provenance"):
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(PositionFactJournalEventRow)
                    .where(PositionFactJournalEventRow.event_kind == kind)
                )
                == 35
            )
