from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    PositionKey,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work import (
    AsyncPostgresExecutionUnitOfWork,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
    ExecutionBookHeadRow,
)

NOW = datetime(2026, 10, 10, tzinfo=UTC)


class _Rows:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _SessionContext:
    def __init__(self, session):
        self._session = session

    async def __aenter__(self):
        return self._session

    async def __aexit__(self, exc_type, exc, traceback):
        return None


@pytest.mark.asyncio
async def test_bulk_recovery_enumerates_current_heads_without_scanning_journal():
    key = PositionKey(
        "live", "primary", "BTCUSDT", FuturesPositionSide.BOTH
    )
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="account-events", stream_epoch="epoch-1"
    )
    head = ExecutionBookHeadRow(
        environment=key.environment,
        account_label=key.account_label,
        symbol=key.symbol,
        position_side=key.position_side.value,
        stream_id=scope.stream_id,
        stream_epoch=scope.stream_epoch,
        revision=1,
        projection_version="projection-1",
        state_payload={},
        updated_at=NOW,
    )
    statements = []

    async def scalars(statement):
        statements.append(statement)
        return _Rows([head] if "execution_book_heads" in str(statement) else [])

    session = SimpleNamespace(
        get_bind=Mock(
            return_value=SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
        ),
        get=AsyncMock(return_value=head),
        scalars=AsyncMock(side_effect=scalars),
    )
    journal_store = SimpleNamespace(
        list_scopes_in_session=AsyncMock(return_value=(scope,)),
        load_recovery_in_session=AsyncMock(return_value=Mock()),
    )
    unit_of_work = AsyncPostgresExecutionUnitOfWork(
        Mock(return_value=_SessionContext(session)),
        journal_store=journal_store,
        command_repository=AsyncMock(),
        reservation_repository=AsyncMock(),
    )

    states = await unit_of_work.load_positions(
        environment=key.environment,
        account_label=key.account_label,
        as_of=NOW,
    )

    assert len(states) == 1
    assert states[0].scope == scope
    assert states[0].head is not None
    assert states[0].head.revision == 1
    journal_store.list_scopes_in_session.assert_not_awaited()
    assert "execution_book_heads" in str(statements[0])


@pytest.mark.asyncio
async def test_bulk_recovery_uses_journal_for_headless_legacy_account():
    key = PositionKey("live", "legacy", "BTCUSDT", FuturesPositionSide.BOTH)
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="account-events", stream_epoch="epoch-1"
    )

    async def scalars(_statement):
        return _Rows([])

    session = SimpleNamespace(
        get_bind=Mock(
            return_value=SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
        ),
        get=AsyncMock(return_value=None),
        scalars=AsyncMock(side_effect=scalars),
    )
    journal_store = SimpleNamespace(
        list_scopes_in_session=AsyncMock(return_value=(scope,)),
        load_recovery_in_session=AsyncMock(return_value=Mock()),
    )
    unit_of_work = AsyncPostgresExecutionUnitOfWork(
        Mock(return_value=_SessionContext(session)),
        journal_store=journal_store,
        command_repository=AsyncMock(),
        reservation_repository=AsyncMock(),
    )

    states = await unit_of_work.load_positions(
        environment=key.environment,
        account_label=key.account_label,
        as_of=NOW,
    )

    assert len(states) == 1
    assert states[0].scope == scope
    assert states[0].head is None
    journal_store.list_scopes_in_session.assert_awaited_once()

