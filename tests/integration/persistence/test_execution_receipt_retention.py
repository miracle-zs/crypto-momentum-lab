import asyncio
from argparse import Namespace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from sqlalchemy import event, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
from crypto_momentum_lab.domain.execution.ports import (
    DecisionCommitConflict,
    ExecutionEvidenceIdentity,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
)
from crypto_momentum_lab.persistence.postgres.execution_receipt_retention import (
    PostgresExecutionReceiptRetention,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work import (
    ExecutionTransaction,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
    ExecutionBookHeadRow,
    ExecutionEvidenceReceiptRow,
    ExecutionRetiredStreamRow,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)

NOW = datetime(2026, 10, 7, tzinfo=UTC)
BEFORE = NOW - timedelta(hours=72)


@pytest.fixture
async def seeded(async_database_url):
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    key = ExecutionScope(
        environment="live", account_label=f"ret-{uuid4().hex}", symbol="BTCUSDT"
    ).to_position_key()
    fields = dict(
        environment=key.environment,
        account_label=key.account_label,
        symbol=key.symbol,
        position_side=key.position_side.value,
    )
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="source", stream_epoch="old"
    )
    async with factory() as session, session.begin():
        session.add(
            ExecutionBookHeadRow(
                **fields,
                stream_id="source",
                stream_epoch="new",
                revision=2,
                projection_version="projection",
                state_payload={},
                updated_at=NOW,
            )
        )
        session.add(
            ExecutionRetiredStreamRow(
                **fields,
                stream_id="source",
                stream_epoch="old",
                retired_at=BEFORE - timedelta(hours=1),
            )
        )
        for epoch, name, sequence, at in (
            ("old", "old-1", 1, BEFORE - timedelta(hours=2)),
            ("old", "old-2", 2, BEFORE - timedelta(hours=2)),
            ("old", "unsequenced", None, BEFORE - timedelta(hours=2)),
            ("old", "recent", 3, NOW),
            ("new", "current", 1, BEFORE - timedelta(hours=2)),
        ):
            session.add(
                ExecutionEvidenceReceiptRow(
                    **fields,
                    stream_id="source",
                    stream_epoch=epoch,
                    evidence_id=name,
                    sequence=sequence,
                    payload_digest="digest",
                    accepted_at=at,
                )
            )
    yield factory, key, scope
    await engine.dispose()


async def identities(factory, key):
    async with factory() as session:
        return set(
            await session.scalars(
                select(ExecutionEvidenceReceiptRow.evidence_id).where(
                    ExecutionEvidenceReceiptRow.account_label == key.account_label
                )
            )
        )


async def test_dry_run_is_non_destructive(seeded, tmp_path):
    factory, key, scope = seeded
    result = await PostgresExecutionReceiptRetention(
        factory, tmp_path
    ).prune_retired_stream(
        scope,
        before=BEFORE,
    )
    assert result["candidates"] == 2
    assert result["deleted"] == 0
    assert len(await identities(factory, key)) == 5
    assert not list(tmp_path.iterdir())


async def test_archive_keeps_current_unsequenced_and_recent_receipts(seeded, tmp_path):
    factory, key, scope = seeded
    retention = PostgresExecutionReceiptRetention(factory, tmp_path)
    result = await retention.prune_retired_stream(scope, before=BEFORE, apply=True)
    assert result["deleted"] == 2
    assert await asyncio.to_thread(Path(result["archive"]).exists)
    assert await identities(factory, key) == {"unsequenced", "recent", "current"}
    # A fresh transaction after pruning must still reject the archived event.
    async with factory() as session, session.begin():
        tx = ExecutionTransaction(
            session,
            journal_store=None,
            command_repository=None,
            reservation_repository=None,
        )
        with pytest.raises(DecisionCommitConflict, match="retired"):
            await tx.record_evidence(
                key=key,
                stream_id="source",
                stream_epoch="old",
                evidence=ExecutionEvidenceIdentity("old-1", "digest", BEFORE, 1),
            )


async def test_archive_failure_rolls_back_every_delete(seeded, tmp_path, monkeypatch):
    factory, key, scope = seeded

    def fail(*args):
        raise OSError("archive unavailable")

    monkeypatch.setattr(
        "crypto_momentum_lab.persistence.postgres.execution_receipt_retention.archive_receipts",
        fail,
    )
    with pytest.raises(OSError, match="archive unavailable"):
        await PostgresExecutionReceiptRetention(factory, tmp_path).prune_retired_stream(
            scope,
            before=BEFORE,
            apply=True,
        )
    assert len(await identities(factory, key)) == 5


async def test_current_epoch_cannot_be_pruned(seeded, tmp_path):
    factory, key, scope = seeded
    current = AccountFactStreamScope.for_position_key(
        key, stream_id="source", stream_epoch="new"
    )
    result = await PostgresExecutionReceiptRetention(
        factory, tmp_path
    ).prune_retired_stream(
        current,
        before=BEFORE,
        apply=True,
    )
    assert result["status"] == "protected"
    assert len(await identities(factory, key)) == 5


async def test_live_position_lock_is_not_waited_on(seeded, tmp_path):
    factory, key, scope = seeded
    async with factory() as session, session.begin():
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:key))"),
            {"key": f"execution_position:{key.canonical_id}"},
        )
        result = await PostgresExecutionReceiptRetention(
            factory, tmp_path
        ).prune_retired_stream(
            scope,
            before=BEFORE,
            apply=True,
        )
    assert result["status"] == "busy"
    assert len(await identities(factory, key)) == 5


async def test_missing_head_fails_closed(seeded, tmp_path):
    factory, key, scope = seeded
    async with factory() as session, session.begin():
        head = await session.get(
            ExecutionBookHeadRow,
            (key.environment, key.account_label, key.symbol, key.position_side.value),
        )
        await session.delete(head)
    result = await PostgresExecutionReceiptRetention(
        factory, tmp_path
    ).prune_retired_stream(
        scope,
        before=BEFORE,
        apply=True,
    )
    assert result["status"] == "protected"
    assert len(await identities(factory, key)) == 5


async def test_archiving_does_not_hold_live_position_lock(
    seeded, tmp_path, monkeypatch, async_database_url
):
    from crypto_momentum_lab.persistence.postgres import (
        execution_receipt_retention as module,
    )

    factory, key, scope = seeded
    archive = module.archive_receipts

    def checked_archive(root, records):
        with psycopg.connect(async_database_url.replace("+asyncpg", "")) as connection:
            locked = connection.execute(
                "SELECT pg_try_advisory_xact_lock(hashtext(%s))",
                (f"execution_position:{key.canonical_id}",),
            ).fetchone()[0]
            assert locked
        return archive(root, records)

    monkeypatch.setattr(module, "archive_receipts", checked_archive)
    result = await PostgresExecutionReceiptRetention(
        factory, tmp_path
    ).prune_retired_stream(
        scope,
        before=BEFORE,
        apply=True,
    )
    assert result["deleted"] == 2


async def test_archive_to_delete_race_revalidates_exact_records(
    seeded, tmp_path, monkeypatch, async_database_url
):
    from crypto_momentum_lab.persistence.postgres import (
        execution_receipt_retention as module,
    )

    factory, key, scope = seeded
    archive = module.archive_receipts

    def change_after_archive(root, records):
        path = archive(root, records)
        with psycopg.connect(async_database_url.replace("+asyncpg", "")) as connection:
            connection.execute(
                "UPDATE execution_evidence_receipts SET payload_digest='changed' "
                "WHERE account_label=%s AND evidence_id='old-1'",
                (key.account_label,),
            )
        return path

    monkeypatch.setattr(module, "archive_receipts", change_after_archive)
    result = await PostgresExecutionReceiptRetention(
        factory, tmp_path
    ).prune_retired_stream(
        scope,
        before=BEFORE,
        apply=True,
    )
    assert result["status"] == "changed"
    assert result["deleted"] == 0
    assert len(await identities(factory, key)) == 5


async def test_database_delete_failure_keeps_receipts_and_archive(seeded, tmp_path):
    factory, key, scope = seeded
    engine = factory.kw["bind"].sync_engine

    def fail_delete(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("DELETE FROM execution_evidence_receipts"):
            raise RuntimeError("injected delete failure")

    event.listen(engine, "before_cursor_execute", fail_delete)
    try:
        with pytest.raises(RuntimeError, match="injected delete failure"):
            await PostgresExecutionReceiptRetention(
                factory, tmp_path
            ).prune_retired_stream(
                scope,
                before=BEFORE,
                apply=True,
            )
    finally:
        event.remove(engine, "before_cursor_execute", fail_delete)
    assert len(await identities(factory, key)) == 5
    assert await asyncio.to_thread(lambda: len(list(tmp_path.glob("*.jsonl.zst")))) == 1


async def test_head_rollover_and_retirement_rollback_together(seeded):
    factory, key, scope = seeded
    with pytest.raises(RuntimeError, match="rollback"):
        async with factory() as session, session.begin():
            tx = ExecutionTransaction(
                session,
                journal_store=None,
                command_repository=None,
                reservation_repository=None,
            )
            await tx.persist_head(
                key=key,
                stream_id="source",
                stream_epoch="next",
                expected_revision=2,
                projection_version="next",
                state_payload={},
                updated_at=NOW,
                is_flat_adoption=True,
            )
            await session.flush()
            raise RuntimeError("rollback")
    async with factory() as session:
        identity = (
            key.environment,
            key.account_label,
            key.symbol,
            key.position_side.value,
        )
        assert (await session.get(ExecutionBookHeadRow, identity)).stream_epoch == "new"
        assert (
            await session.get(ExecutionRetiredStreamRow, (*identity, "source", "new"))
            is None
        )


async def test_cli_uses_execution_database_and_dry_run(
    seeded, tmp_path, monkeypatch, async_database_url
):
    from crypto_momentum_lab.tools.archive_execution_receipts import run

    factory, key, scope = seeded
    monkeypatch.setenv("CML_DATABASE_URL", async_database_url)
    await run(
        Namespace(
            minimum_age_hours=72,
            batch_size=1,
            max_scopes=2,
            max_runtime_seconds=45,
            archive_root=tmp_path,
            apply=False,
        )
    )
    assert len(await identities(factory, key)) == 5
    assert not list(tmp_path.iterdir())


async def test_completion_keeps_unsequenced_identities(seeded, tmp_path):
    factory, key, scope = seeded
    result = await PostgresExecutionReceiptRetention(
        factory, tmp_path
    ).prune_retired_stream(
        scope,
        before=NOW + timedelta(hours=1),
        apply=True,
    )
    assert result["status"] == "completed"
    assert result["deleted"] == 3
    assert await identities(factory, key) == {"unsequenced", "current"}
    async with factory() as session:
        identity = (
            key.environment,
            key.account_label,
            key.symbol,
            key.position_side.value,
            "source",
            "old",
        )
        assert (
            await session.get(ExecutionRetiredStreamRow, identity)
        ).receipts_archived_at is not None
