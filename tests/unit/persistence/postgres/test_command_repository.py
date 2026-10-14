from __future__ import annotations

import pytest

from crypto_momentum_lab.persistence.postgres.command_repository import (
    PostgresCommandRepository,
)
from crypto_momentum_lab.persistence.postgres.models import ExecutionCommandRow


@pytest.mark.asyncio
async def test_in_session_outbox_insert_preserves_caller_transaction():
    from datetime import UTC, datetime
    from unittest.mock import AsyncMock, Mock

    session = AsyncMock()
    session.get.return_value = None
    session.add = Mock()
    repository = PostgresCommandRepository(None)
    await repository.upsert_execution_command_in_session(
        session,
        command_id="command-1",
        client_order_id="command-1",
        command="exit",
        status="prepared",
        requested_at=datetime(2026, 9, 30, tzinfo=UTC),
        details={"reservations": ["r1"]},
    )
    session.get.assert_awaited_once_with(
        ExecutionCommandRow, "command-1", with_for_update=True
    )
    inserted = session.add.call_args.args[0]
    assert inserted.command_id == "command-1"
    assert inserted.details == {"reservations": ["r1"]}
    session.commit.assert_not_awaited()
    session.rollback.assert_not_awaited()


@pytest.mark.asyncio
async def test_in_session_outbox_identity_conflict_does_not_mutate_row():
    from datetime import UTC, datetime
    from unittest.mock import AsyncMock

    existing = ExecutionCommandRow(
        command_id="command-1",
        client_order_id="other-order",
        command="exit",
        status="prepared",
        details={},
    )
    session = AsyncMock()
    session.get.return_value = existing
    with pytest.raises(ValueError, match="durable identity"):
        await PostgresCommandRepository(None).upsert_execution_command_in_session(
            session,
            command_id="command-1",
            client_order_id="command-1",
            command="exit",
            status="unknown",
            requested_at=datetime(2026, 9, 30, tzinfo=UTC),
            details={},
        )
    assert existing.status == "prepared"
    session.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_execution_transaction_uses_command_repository_with_same_session():
    from datetime import UTC, datetime
    from unittest.mock import AsyncMock

    from crypto_momentum_lab.persistence.postgres.execution_unit_of_work import (
        ExecutionTransaction,
    )

    session, commands = AsyncMock(), AsyncMock()
    tx = ExecutionTransaction(
        session,
        command_repository=commands,
        reservation_repository=AsyncMock(),
    )
    values = dict(
        command_id="command-1",
        client_order_id="command-1",
        command="exit",
        status="unknown",
        requested_at=datetime(2026, 9, 30, tzinfo=UTC),
        details={},
    )
    await tx.upsert_outbox(**values)
    commands.upsert_execution_command_in_session.assert_awaited_once_with(
        session, **values
    )
    session.commit.assert_not_awaited()
