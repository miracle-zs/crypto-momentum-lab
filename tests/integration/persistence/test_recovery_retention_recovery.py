"""Phase-D acceptance: pruning must not break recovery evidence.

Seeds one scope with several superseded checkpoints plus compactable and
immutable journal events, runs the real retention passes against PostgreSQL,
and asserts what survives: the newest checkpoint per scope, the checkpoint a
head still binds, one old row per scope and kind, and every immutable kind.
"""

import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import asyncpg  # type: ignore[import-untyped]
import pytest
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

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

NOW = datetime(2026, 10, 12, 18, 0, tzinfo=UTC)
OLD = NOW - timedelta(hours=96)
ACCOUNT = "retention-account"
SYMBOL = "BTCUSDT"
SIDE = "LONG"
STREAM_ID = "account_event_hub"
STREAM_EPOCH = "epoch-1"

_COMPACTABLE = ("facts_state", "snapshot")
_IMMUTABLE = ("fill", "exit_boundary")


def _asyncpg_url() -> str:
    url = os.environ.get(
        "CML_TEST_ASYNC_DATABASE_URL",
        "postgresql+asyncpg://cml:cml@localhost:54329/cml_test",
    )
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)


def _scope_values() -> dict[str, str]:
    return {
        "environment": "live",
        "account_label": ACCOUNT,
        "symbol": SYMBOL,
        "position_side": SIDE,
        "stream_id": STREAM_ID,
        "stream_epoch": STREAM_EPOCH,
    }


@pytest.fixture
async def retention_database(
    async_database_url: str,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async def _clear() -> None:
        async with factory() as session:
            async with session.begin():
                await session.execute(
                    delete(PositionFactJournalEventRow).where(
                        PositionFactJournalEventRow.account_label == ACCOUNT
                    )
                )
                await session.execute(
                    delete(PositionRecoveryCheckpointRow).where(
                        PositionRecoveryCheckpointRow.account_label == ACCOUNT
                    )
                )
                await session.execute(
                    delete(ExecutionBookHeadRow).where(
                        ExecutionBookHeadRow.account_label == ACCOUNT
                    )
                )

    await _clear()
    yield factory
    await _clear()
    await engine.dispose()


async def _seed(factory: async_sessionmaker[AsyncSession]) -> None:
    scope = _scope_values()
    async with factory() as session:
        async with session.begin():
            # Three superseded checkpoints, oldest first, plus the newest.
            for index, revision in enumerate((1, 2, 3, 4)):
                session.add(
                    PositionRecoveryCheckpointRow(
                        **scope,
                        checkpoint_id=f"cp-{index}",
                        schema_version=3,
                        event_cut=OLD + timedelta(minutes=index),
                        source_revision=revision,
                        facts_hash=f"facts-{index}",
                        payload_hash=f"payload-{index}",
                        recorded_at=OLD + timedelta(minutes=index),
                        payload={},
                    )
                )
            # A head still binds the second checkpoint: it must survive too.
            session.add(
                ExecutionBookHeadRow(
                    environment="live",
                    account_label=ACCOUNT,
                    symbol=SYMBOL,
                    position_side=SIDE,
                    stream_id=STREAM_ID,
                    stream_epoch=STREAM_EPOCH,
                    revision=5,
                    projection_version="pv-5",
                    state_payload={
                        "recovery_checkpoint": {"checkpoint_id": "cp-1"},
                        "journal_revision": 12,
                    },
                    updated_at=NOW,
                )
            )
            for kind in _COMPACTABLE + _IMMUTABLE:
                for revision in (1, 2, 3):
                    session.add(
                        PositionFactJournalEventRow(
                            event_record_id=f"evt-{kind}-{revision}",
                            **scope,
                            event_kind=kind,
                            event_id=f"exchange-{kind}-{revision}",
                            payload_hash=f"hash-{kind}-{revision}",
                            source_revision=revision,
                            occurred_at=OLD + timedelta(minutes=revision),
                            recorded_at=OLD + timedelta(minutes=revision),
                            payload={},
                        )
                    )


async def _run_pruner() -> None:
    connection = await asyncpg.connect(_asyncpg_url())
    try:
        cutoff = datetime.now(UTC) - timedelta(hours=72)
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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pruning_keeps_active_references_and_one_row_per_kind(
    retention_database: async_sessionmaker[AsyncSession],
) -> None:
    factory = retention_database
    await _seed(factory)
    await _run_pruner()

    async with factory() as session:
        checkpoints = set(
            (
                await session.scalars(
                    select(PositionRecoveryCheckpointRow.checkpoint_id).where(
                        PositionRecoveryCheckpointRow.account_label == ACCOUNT
                    )
                )
            ).all()
        )
        events = {
            row.event_kind: row.remaining
            for row in (
                await session.execute(
                    select(
                        PositionFactJournalEventRow.event_kind,
                        func.count().label("remaining"),
                    )
                    .where(PositionFactJournalEventRow.account_label == ACCOUNT)
                    .group_by(PositionFactJournalEventRow.event_kind)
                )
            ).all()
        }
        head = await session.scalar(
            select(ExecutionBookHeadRow).where(
                ExecutionBookHeadRow.account_label == ACCOUNT
            )
        )
        orders_untouched = head is not None and head.revision == 5

    # Newest per scope and the head-bound checkpoint both survive.
    assert checkpoints == {"cp-1", "cp-3"}
    assert orders_untouched
    # Compactable kinds keep exactly one retained row per scope and kind;
    # immutable facts are never selected for deletion.
    assert {kind: events.get(kind, 0) for kind in _COMPACTABLE} == {
        kind: 1 for kind in _COMPACTABLE
    }
    assert {kind: events.get(kind, 0) for kind in _IMMUTABLE} == {
        kind: 3 for kind in _IMMUTABLE
    }
