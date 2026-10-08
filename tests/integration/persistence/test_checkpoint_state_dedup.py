from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select

from crypto_momentum_lab.domain.execution.recovery_models import AccountFacts
from crypto_momentum_lab.persistence.postgres.account_journal_store import (
    PostgresAccountJournalStore,
)
from crypto_momentum_lab.persistence.postgres.position_fact_journal_models import (
    PositionFactJournalEventRow,
)
from tests.integration.persistence.test_shared_policy_evidence import (
    evidence_sessions,  # noqa: F401
)
from tests.unit.persistence.postgres.test_checkpoint_snapshot_recovery import (
    _checkpoint,
)

pytestmark = pytest.mark.integration


async def test_repeated_checkpoint_cuts_restore_without_duplicate_state_rows(
    evidence_sessions,  # noqa: F811 - imported pytest fixture
):
    store = PostgresAccountJournalStore()
    base = replace(
        _checkpoint(), has_late_events=True, integrity_issues=("source uncertain",)
    )
    for index in range(100):
        checkpoint = replace(
            base,
            checkpoint_id=f"cut-{index}",
            source_revision=base.source_revision + index,
        )
        facts = AccountFacts(
            position_key=checkpoint.key,
            stream_scope=checkpoint.stream_scope,
            recovery_checkpoint=checkpoint,
            has_late_events=True,
            integrity_issues=checkpoint.integrity_issues,
        )
        async with evidence_sessions() as session, session.begin():
            await store.persist_facts_in_session(
                session,
                scope=checkpoint.stream_scope,
                facts=facts,
                revision=checkpoint.source_revision,
            )
    async with evidence_sessions() as session:
        assert (
            await session.scalar(
                select(func.count())
                .select_from(PositionFactJournalEventRow)
                .where(PositionFactJournalEventRow.event_kind == "facts_state")
            )
            == 0
        )
        for include_prefix in (False, True):
            cut = await store.load_recovery_in_session(
                session,
                scope=checkpoint.stream_scope,
                as_of=datetime.now(UTC) + timedelta(seconds=1),
                include_checkpoint_prefix=include_prefix,
            )
            assert cut.checkpoint.checkpoint_id == "cut-99"
            assert cut.facts.has_late_events is True
            assert cut.facts.integrity_issues == ("source uncertain",)
            assert cut.checkpoint.projection == base.projection
