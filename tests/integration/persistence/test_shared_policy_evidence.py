import importlib.util
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import delete, select, text, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from crypto_momentum_lab.domain.market.revision_models import DecisionTrace
from crypto_momentum_lab.persistence.postgres.decision_trace_repository import (
    _load_policy_evidence,
)
from crypto_momentum_lab.persistence.postgres.models import (
    DecisionPolicyEvidenceRow,
    DecisionTraceRow,
    MarketRevisionRefRow,
)
from crypto_momentum_lab.tools.share_decision_policy_evidence import (
    backfill_batch,
    collect_batch,
)
from tests.unit.decision.test_decision_engine import _make_market_envelope
from tests.unit.decision.test_decision_state_storage import large_payload

pytestmark = pytest.mark.integration

@pytest.fixture
async def evidence_sessions(async_database_url):
    admin = create_async_engine(async_database_url)
    schema = "evidence_test_" + uuid4().hex
    async with admin.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_async_engine(
        async_database_url, connect_args={"server_settings": {"search_path": schema}}
    )
    try:
        async with engine.connect() as connection:
            await connection.exec_driver_sql(
                "CREATE TABLE position_fact_journal_events (id integer PRIMARY KEY)"
            )
            await connection.exec_driver_sql(
                "CREATE TABLE position_recovery_checkpoints (id integer PRIMARY KEY)"
            )
            for table in (
                DecisionPolicyEvidenceRow.__table__,
                DecisionTraceRow.__table__,
                MarketRevisionRefRow.__table__,
            ):
                await connection.run_sync(table.create)
            await connection.commit()

            def migrate(sync_connection):
                from alembic.migration import MigrationContext
                from alembic.operations import Operations

                path = (
                    Path(__file__).parents[3]
                    / "alembic"
                    / "versions"
                    / "20261008_0055_shared_decision_policy_evidence.py"
                )
                spec = importlib.util.spec_from_file_location("shared_migration", path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                context = MigrationContext.configure(sync_connection)
                module.op = Operations(context)
                with context.begin_transaction():
                    module.upgrade()

            await connection.run_sync(migrate)
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()
        async with admin.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await admin.dispose()

def trace():
    t0 = datetime.now(UTC)
    ref, _ = _make_market_envelope("BTCUSDT", t0, Decimal("100"))
    payload = large_payload()
    payload["market_state"] = {"symbol": "BTCUSDT"}
    return DecisionTrace(
        decision_id="first",
        strategy_name="test",
        account_label="primary",
        decision_time=t0,
        evaluated_market_refs=(ref,),
        intent_produced=False,
        rejection_reason="no_candidate",
        input_hash="input",
        frame_digest="frame",
        trace_payload=payload,
    )

async def test_backfill_is_lossless_and_gc_respects_reader_locks_and_live_refs(
    evidence_sessions,
):
    decision = trace()
    async with evidence_sessions() as session, session.begin():
        session.add(
            DecisionTraceRow(
                decision_id="legacy",
                strategy_name="test",
                account_label="primary",
                decision_time=decision.decision_time,
                intent_produced=False,
                intent_id=None,
                rejection_reason="no_candidate",
                evaluated_revision_ids=[],
                trace_payload=decision.trace_payload,
                created_at=decision.decision_time,
            )
        )
    async with evidence_sessions() as session, session.begin():
        assert await backfill_batch(session, 10) == (1, 1)
        await session.execute(
            update(DecisionPolicyEvidenceRow).values(
                created_at=datetime.now(UTC) - timedelta(days=10)
            )
        )
    async with evidence_sessions() as session, session.begin():
        assert await collect_batch(session, 10) == 0  # still referenced
        await session.execute(delete(DecisionTraceRow))
    async with evidence_sessions() as writer, writer.begin():
        digest = await writer.scalar(select(DecisionPolicyEvidenceRow.state_digest))
        assert len(await _load_policy_evidence(writer, {digest})) == 1
        async with evidence_sessions() as collector, collector.begin():
            assert await collect_batch(collector, 10) == 0  # key-share lock wins
    async with evidence_sessions() as session, session.begin():
        assert await collect_batch(session, 10) == 1
