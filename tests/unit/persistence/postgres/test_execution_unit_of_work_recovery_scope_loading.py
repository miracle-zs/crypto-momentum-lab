from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.execution_head import (
    build_execution_head_payload,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.ports import DecisionCommitConflict
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.recovery_codec import PositionRecoveryCodec
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work import (
    AsyncPostgresExecutionUnitOfWork,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
    ExecutionBookHeadRow,
    ExecutionOrderWatermarkRow,
    ExecutionTradeIdentityRow,
)
from crypto_momentum_lab.persistence.postgres.models import (
    ExecutionCommandRow,
    PositionReservationRow,
)

NOW = datetime(2026, 10, 10, tzinfo=UTC)


class _Rows:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows

    def first(self):
        return self._rows[0] if self._rows else None


class _SessionContext:
    def __init__(self, session):
        self._session = session

    async def __aenter__(self):
        return self._session

    async def __aexit__(self, exc_type, exc, traceback):
        return None


class _FakeSession:
    def __init__(
        self,
        *,
        heads=(),
        reservations=(),
        commands=(),
        rows_by_model=None,
        position_key=None,
    ):
        self._heads = tuple(heads)
        self._reservations = tuple(reservations)
        self._commands = tuple(commands)
        self._rows_by_model = rows_by_model or {}
        self._position_key = position_key
        self.get_bind = Mock(
            return_value=SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
        )
        self.get = AsyncMock(side_effect=self._get)
        self.scalars = AsyncMock(side_effect=self._scalars)
        self.scalar = AsyncMock(return_value=None)
        self.execute = AsyncMock(side_effect=self._execute)

    async def _get(self, model, identity, **_kwargs):
        if model is not ExecutionBookHeadRow:
            return None
        for head in self._heads:
            if (
                head.environment,
                head.account_label,
                head.symbol,
                head.position_side,
            ) == tuple(identity):
                return head
        return None

    async def _scalars(self, statement):
        entity = statement.column_descriptions[0].get("entity")
        if entity is ExecutionBookHeadRow:
            return _Rows(list(self._heads))
        if entity is PositionReservationRow:
            return _Rows(list(self._reservations))
        if entity is ExecutionCommandRow:
            return _Rows(list(self._commands))
        return _Rows(list(self._rows_by_model.get(entity, ())))

    async def _execute(self, statement):
        entity = statement.column_descriptions[0].get("entity")
        if entity is not None:
            rows = list(self._rows_by_model.get(entity, ()))
            if self._position_key is not None:
                rows = [
                    row
                    for row in rows
                    if row
                    == (
                        self._position_key.symbol,
                        self._position_key.position_side.value,
                    )
                ]
            return _Rows(rows)
        statement_text = str(statement).lower()
        for model, rows in self._rows_by_model.items():
            if model.__tablename__ in statement_text:
                result = list(rows)
                if self._position_key is not None:
                    result = [
                        row
                        for row in result
                        if row
                        == (
                            self._position_key.symbol,
                            self._position_key.position_side.value,
                        )
                    ]
                return _Rows(result)
        return _Rows([])


def _head(key: PositionKey, *, revision: int = 1, schema_version: int = 4):
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="account-events", stream_epoch="epoch-1"
    )
    book = ExecutionBook()
    book._stream_scopes[key.canonical_id] = scope
    payload = build_execution_head_payload(
        book, key, active_reservations=lambda _key: ()
    )
    payload["schema_version"] = schema_version
    return ExecutionBookHeadRow(
        environment=key.environment,
        account_label=key.account_label,
        symbol=key.symbol,
        position_side=key.position_side.value,
        stream_id=scope.stream_id,
        stream_epoch=scope.stream_epoch,
        revision=revision,
        projection_version=f"projection-{revision}",
        state_payload=payload,
        updated_at=NOW,
    )


def _uow(session):
    return AsyncPostgresExecutionUnitOfWork(
        Mock(return_value=_SessionContext(session)),
        command_repository=AsyncMock(),
        reservation_repository=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_current_recovery_reads_only_the_head_facts():
    key = PositionKey("live", "primary", "BTCUSDT", FuturesPositionSide.BOTH)
    head = _head(key, revision=4)
    session = _FakeSession(heads=[head])
    unit_of_work = _uow(session)

    states = await unit_of_work.load_positions(
        environment=key.environment,
        account_label=key.account_label,
        as_of=NOW,
    )

    assert len(states) == 1
    assert states[0].scope.stream_id == head.stream_id
    assert states[0].head.revision == 4
    assert states[0].cut.revision == 4
    assert states[0].cut.facts == PositionRecoveryCodec.decode_facts(
        head.state_payload["position_state"]["verification"]
    )


@pytest.mark.asyncio
async def test_watermarks_load_once_for_all_position_heads():
    btc = PositionKey("live", "primary", "BTCUSDT", FuturesPositionSide.BOTH)
    eth = PositionKey("live", "primary", "ETHUSDT", FuturesPositionSide.BOTH)
    rows = [
        SimpleNamespace(
            symbol=key.symbol,
            position_side=key.position_side.value,
            order_id=f"order-{key.symbol}",
            cumulative_quantity=Decimal("1"),
            cumulative_quote=Decimal("100"),
            updated_at=NOW,
        )
        for key in (btc, eth)
    ]
    session = _FakeSession(
        heads=[_head(btc), _head(eth)],
        rows_by_model={ExecutionOrderWatermarkRow: rows},
    )

    states = await _uow(session).load_positions(
        environment="live", account_label="primary", as_of=NOW
    )

    assert {state.scope.symbol: len(state.watermarks) for state in states} == {
        "BTCUSDT": 1,
        "ETHUSDT": 1,
    }
    watermark_queries = [
        call.args[0]
        for call in session.scalars.await_args_list
        if call.args[0].column_descriptions[0].get("entity")
        is ExecutionOrderWatermarkRow
    ]
    assert len(watermark_queries) == 1


@pytest.mark.asyncio
async def test_new_account_without_heads_or_obligations_restores_empty():
    session = _FakeSession()
    unit_of_work = _uow(session)

    states = await unit_of_work.load_positions(
        environment="live", account_label="new-account", as_of=NOW
    )

    assert states == ()
    assert not any(
        call.args[0].column_descriptions[0].get("entity")
        is ExecutionOrderWatermarkRow
        for call in session.scalars.await_args_list
    )


@pytest.mark.asyncio
async def test_legacy_head_requires_explicit_format_migration():
    key = PositionKey("live", "primary", "BTCUSDT", FuturesPositionSide.BOTH)
    session = _FakeSession(heads=[_head(key, schema_version=1)])
    unit_of_work = _uow(session)

    with pytest.raises(
        DecisionCommitConflict, match="unsupported execution head schema"
    ):
        await unit_of_work.load_positions(
            environment=key.environment,
            account_label=key.account_label,
            as_of=NOW,
        )


@pytest.mark.asyncio
async def test_corrupt_head_facts_block_recovery():
    key = PositionKey("live", "primary", "BTCUSDT", FuturesPositionSide.BOTH)
    head = _head(key)
    head.state_payload["position_state"] = {"schema_version": 999}
    unit_of_work = _uow(_FakeSession(heads=[head]))

    with pytest.raises(DecisionCommitConflict, match="invalid execution head"):
        await unit_of_work.load_positions(
            environment=key.environment,
            account_label=key.account_label,
            as_of=NOW,
        )


@pytest.mark.asyncio
async def test_historical_trade_identity_without_head_does_not_block_restore():
    key = PositionKey("live", "primary", "BTCUSDT", FuturesPositionSide.BOTH)
    unit_of_work = _uow(_FakeSession())

    states = await unit_of_work.load_positions(
        environment=key.environment,
        account_label=key.account_label,
        as_of=NOW,
    )

    assert states == ()


@pytest.mark.asyncio
async def test_active_reservation_without_its_position_head_blocks_restore():
    btc = PositionKey("live", "primary", "BTCUSDT", FuturesPositionSide.BOTH)
    missing_eth = PositionKey("live", "primary", "ETHUSDT", FuturesPositionSide.BOTH)
    head = _head(btc)
    reservation = PositionReservationRow(
        reservation_id="res-eth-1",
        environment="live",
        account_label="primary",
        strategy_name="trend_v1",
        symbol="ETHUSDT",
        position_side="BOTH",
        batch_id="batch-1",
        command_id="cmd-1",
        reserved_quantity=Decimal("5"),
        consumed_quantity=Decimal("0"),
        released_quantity=Decimal("0"),
        status="ACTIVE",
        created_at=NOW,
        updated_at=NOW,
    )
    unit_of_work = _uow(_FakeSession(heads=[head], reservations=[reservation]))

    with pytest.raises(
        DecisionCommitConflict,
        match=f"position {missing_eth.symbol}:BOTH has active reservation res-eth-1",
    ):
        await unit_of_work.load_positions(
            environment=btc.environment,
            account_label=btc.account_label,
            as_of=NOW,
        )


@pytest.mark.asyncio
async def test_position_read_does_not_block_on_another_positions_identity():
    btc = PositionKey("live", "primary", "BTCUSDT", FuturesPositionSide.BOTH)
    head = _head(btc)
    session = _FakeSession(
        heads=[head],
        position_key=btc,
        rows_by_model={ExecutionTradeIdentityRow: []},
    )
    unit_of_work = _uow(session)

    state = await unit_of_work.load_position(btc, as_of=NOW)

    assert state is not None
    assert state.scope.symbol == btc.symbol
