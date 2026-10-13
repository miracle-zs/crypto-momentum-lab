from datetime import UTC, datetime
from decimal import Decimal
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


class _FakeSession:
    """Session double covering the queries the recovery loader may issue."""

    def __init__(self, *, heads=(), execute_rows=(), get_result=None):
        self._heads = tuple(heads)
        self._execute_rows = tuple(execute_rows)
        self.get_bind = Mock(
            return_value=SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
        )
        self.get = AsyncMock(return_value=get_result)
        self.scalars = AsyncMock(side_effect=self._scalars)
        self.scalar = AsyncMock(return_value=None)
        self.execute = AsyncMock(side_effect=self._execute)
        self.statements: list[object] = []

    async def _scalars(self, statement):
        self.statements.append(statement)
        text = str(statement)
        if "execution_book_heads" in text:
            return _Rows(list(self._heads))
        return _Rows([])

    async def _execute(self, _statement):
        return _Rows(list(self._execute_rows))


@pytest.mark.asyncio
async def test_bulk_recovery_enumerates_current_heads_without_scanning_journal():

    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        PositionLedgerProjection,
    )
    from crypto_momentum_lab.domain.execution.recovery_codec import (
        PositionRecoveryCodec,
    )

    key = PositionKey("live", "primary", "BTCUSDT", FuturesPositionSide.BOTH)
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="account-events", stream_epoch="epoch-1"
    )
    projection = PositionLedgerProjection(
        position_key=key,
        total_active_quantity=Decimal("0"),
        unallocated_quantity=Decimal("0"),
        reconciliation_gap=Decimal("0"),
        high_watermark_trade_at=None,
        active_batches=(),
        archived_episodes=(),
        active_episode=None,
        event_cut=NOW,
        stream_scope=scope,
    )
    encoded_proj = PositionRecoveryCodec.encode_projection(projection)
    head = ExecutionBookHeadRow(
        environment=key.environment,
        account_label=key.account_label,
        symbol=key.symbol,
        position_side=key.position_side.value,
        stream_id=scope.stream_id,
        stream_epoch=scope.stream_epoch,
        revision=1,
        projection_version="projection-1",
        state_payload={
            "projection": encoded_proj,
            "facts_hash": "a" * 64,
            "journal_revision": 1,
            "active_trade_ids": [],
        },
        updated_at=NOW,
    )
    statements = []

    async def scalars(statement):
        statements.append(statement)
        return _Rows([head] if "execution_book_heads" in str(statement) else [])

    session = _FakeSession(heads=[head], get_result=head)
    fake_cut = SimpleNamespace(facts=SimpleNamespace(fills=()))
    journal_store = SimpleNamespace(
        list_scopes_in_session=AsyncMock(return_value=(scope,)),
        load_recovery_in_session=AsyncMock(return_value=fake_cut),
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
    assert states[0].trade_ids == ()
    assert states[0].evidence_ids == ()
    assert states[0].latest_evidence_sequence is None
    # The head-coverage audit must consult the durable journal scopes to prove
    # that no journal-only position lost its head, even when heads exist.
    journal_store.list_scopes_in_session.assert_awaited()
    assert "execution_book_heads" in str(session.statements[0])
    session.scalar.assert_awaited_once()
    evidence_query = str(session.scalar.call_args[0][0])
    assert "max" in evidence_query.lower()
    assert "execution_evidence_receipts" in evidence_query.lower()
    for statement in session.statements:
        assert "execution_evidence_receipts" not in str(statement).lower()


@pytest.mark.asyncio
async def test_bulk_recovery_blocks_headless_account_with_existing_journal():
    from crypto_momentum_lab.domain.execution.ports import DecisionCommitConflict

    key = PositionKey("live", "legacy", "BTCUSDT", FuturesPositionSide.BOTH)
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="account-events", stream_epoch="epoch-1"
    )

    async def scalars(_statement):
        return _Rows([])

    session = _FakeSession(heads=[], get_result=None)
    journal_store = SimpleNamespace(
        list_scopes_in_session=AsyncMock(return_value=(scope,)),
    )
    unit_of_work = AsyncPostgresExecutionUnitOfWork(
        Mock(return_value=_SessionContext(session)),
        journal_store=journal_store,
        command_repository=AsyncMock(),
        reservation_repository=AsyncMock(),
    )

    with pytest.raises(
        DecisionCommitConflict, match="existing journal streams but no durable head"
    ):
        await unit_of_work.load_positions(
            environment=key.environment,
            account_label=key.account_label,
            as_of=NOW,
        )


@pytest.mark.asyncio
async def test_bulk_recovery_returns_empty_for_brand_new_account():
    key = PositionKey("live", "brand_new", "BTCUSDT", FuturesPositionSide.BOTH)

    async def scalars(_statement):
        return _Rows([])

    session = _FakeSession(heads=[], get_result=None)
    journal_store = SimpleNamespace(
        list_scopes_in_session=AsyncMock(return_value=()),
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
    assert states == ()


@pytest.mark.asyncio
async def test_corrupted_head_missing_projection_blocks_recovery():
    from crypto_momentum_lab.domain.execution.ports import DecisionCommitConflict

    key = PositionKey("live", "corrupted", "BTCUSDT", FuturesPositionSide.BOTH)
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="account-events", stream_epoch="epoch-1"
    )
    corrupted_head = ExecutionBookHeadRow(
        environment=key.environment,
        account_label=key.account_label,
        symbol=key.symbol,
        position_side=key.position_side.value,
        stream_id=scope.stream_id,
        stream_epoch=scope.stream_epoch,
        revision=1,
        projection_version="projection-1",
        state_payload={},  # missing projection!
        updated_at=NOW,
    )

    async def scalars(statement):
        return _Rows([corrupted_head] if "execution_book_heads" in str(statement) else [])

    session = _FakeSession(heads=[corrupted_head], get_result=corrupted_head)
    journal_store = SimpleNamespace(
        list_scopes_in_session=AsyncMock(return_value=(scope,)),
    )
    unit_of_work = AsyncPostgresExecutionUnitOfWork(
        Mock(return_value=_SessionContext(session)),
        journal_store=journal_store,
        command_repository=AsyncMock(),
        reservation_repository=AsyncMock(),
    )

    with pytest.raises(
        DecisionCommitConflict, match="requires a valid durable head with projection"
    ):
        await unit_of_work.load_positions(
            environment=key.environment,
            account_label=key.account_label,
            as_of=NOW,
        )


@pytest.mark.asyncio
async def test_head_direct_recovery_does_not_load_journal_cut():

    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        PositionLedgerProjection,
    )
    from crypto_momentum_lab.domain.execution.recovery_codec import (
        PositionRecoveryCodec,
    )

    key = PositionKey("live", "primary", "BTCUSDT", FuturesPositionSide.BOTH)
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="account-events", stream_epoch="epoch-1"
    )
    projection = PositionLedgerProjection(
        position_key=key,
        total_active_quantity=Decimal("0"),
        unallocated_quantity=Decimal("0"),
        reconciliation_gap=Decimal("0"),
        high_watermark_trade_at=None,
        active_batches=(),
        archived_episodes=(),
        active_episode=None,
        event_cut=NOW,
        stream_scope=scope,
    )
    encoded_proj = PositionRecoveryCodec.encode_projection(projection)
    head = ExecutionBookHeadRow(
        environment=key.environment,
        account_label=key.account_label,
        symbol=key.symbol,
        position_side=key.position_side.value,
        stream_id=scope.stream_id,
        stream_epoch=scope.stream_epoch,
        revision=2,
        projection_version="projection-2",
        state_payload={
            "projection": encoded_proj,
            "facts_hash": "a" * 64,
            "journal_revision": 5,
            "active_trade_ids": ["trade-1", "trade-2"],
        },
        updated_at=NOW,
    )

    from crypto_momentum_lab.persistence.postgres.models import AccountFillEventRow

    fill_rows = [
        AccountFillEventRow(
            environment=key.environment,
            account_label=key.account_label,
            symbol=key.symbol,
            trade_id=trade_id,
            order_id=f"order-{trade_id}",
            side="BUY",
            price=Decimal("50000"),
            quantity=Decimal("0.5"),
            realized_pnl=Decimal("0"),
            fee=Decimal("0"),
            fee_asset="USDT",
            trade_at=NOW,
            raw_payload={"positionSide": key.position_side.value},
        )
        for trade_id in ("trade-1", "trade-2")
    ]

    async def scalars(statement):
        text = str(statement)
        if "execution_book_heads" in text:
            return _Rows([head])
        if "account_fill_events" in text:
            # The completeness audit is a DISTINCT symbol projection; the load
            # path selects whole rows.
            if text.upper().startswith("SELECT DISTINCT"):
                return _Rows([key.symbol])
            return _Rows(fill_rows)
        return _Rows([])

    session = _FakeSession(heads=[head], get_result=head)
    session.scalars = AsyncMock(side_effect=scalars)
    journal_store = SimpleNamespace(
        list_scopes_in_session=AsyncMock(),
        load_recovery_in_session=AsyncMock(),
        load_checkpoint_by_id_in_session=AsyncMock(return_value=None),
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
    assert states[0].head.revision == 2
    assert states[0].trade_ids == ("trade-1", "trade-2")
    assert states[0].cut.revision == 5
    journal_store.load_recovery_in_session.assert_not_awaited()
    # Coverage is audited against journal scopes; the journal *cut* itself is
    # still never loaded on the direct-head path.
    journal_store.list_scopes_in_session.assert_awaited()


@pytest.mark.asyncio
async def test_tampered_quantity_in_head_fails_closed():
    from crypto_momentum_lab.domain.execution.ports import DecisionCommitConflict
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        PositionLedgerProjection,
    )
    from crypto_momentum_lab.domain.execution.recovery_codec import (
        PositionRecoveryCodec,
    )

    key = PositionKey("live", "primary", "BTCUSDT", FuturesPositionSide.BOTH)
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="account-events", stream_epoch="epoch-1"
    )
    projection = PositionLedgerProjection(
        position_key=key,
        total_active_quantity=Decimal("1.5"),
        unallocated_quantity=Decimal("1.5"),
        reconciliation_gap=Decimal("0"),
        high_watermark_trade_at=None,
        active_batches=(),
        archived_episodes=(),
        active_episode=None,
        event_cut=NOW,
        stream_scope=scope,
    )
    encoded_proj = PositionRecoveryCodec.encode_projection(projection)
    correct_digest = PositionRecoveryCodec.compute_projection_digest(projection)

    # Tamper with the quantity in encoded projection without updating projection_digest
    tampered_proj = dict(encoded_proj)
    tampered_proj["total_active_quantity"] = "999.0"

    head = ExecutionBookHeadRow(
        environment=key.environment,
        account_label=key.account_label,
        symbol=key.symbol,
        position_side=key.position_side.value,
        stream_id=scope.stream_id,
        stream_epoch=scope.stream_epoch,
        revision=1,
        projection_version="projection-1",
        state_payload={
            "projection": tampered_proj,
            "projection_digest": correct_digest,
            "facts_hash": "a" * 64,
            "journal_revision": 1,
            "active_trade_ids": [],
        },
        updated_at=NOW,
    )

    async def scalars(statement):
        return _Rows([head] if "execution_book_heads" in str(statement) else [])

    session = _FakeSession(heads=[head], get_result=head)
    unit_of_work = AsyncPostgresExecutionUnitOfWork(
        Mock(return_value=_SessionContext(session)),
        journal_store=SimpleNamespace(list_scopes_in_session=AsyncMock()),
        command_repository=AsyncMock(),
        reservation_repository=AsyncMock(),
    )

    with pytest.raises(DecisionCommitConflict, match="projection digest mismatch"):
        await unit_of_work.load_positions(
            environment=key.environment,
            account_label=key.account_label,
            as_of=NOW,
        )


@pytest.mark.asyncio
async def test_corrupted_payload_format_fails_closed():
    from crypto_momentum_lab.domain.execution.ports import DecisionCommitConflict

    key = PositionKey("live", "primary", "BTCUSDT", FuturesPositionSide.BOTH)
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
        state_payload={
            "projection": "NOT_A_VALID_PROJECTION_DICT",
            "facts_hash": "a" * 64,
            "journal_revision": 1,
        },
        updated_at=NOW,
    )

    async def scalars(statement):
        return _Rows([head] if "execution_book_heads" in str(statement) else [])

    session = _FakeSession(heads=[head], get_result=head)
    unit_of_work = AsyncPostgresExecutionUnitOfWork(
        Mock(return_value=_SessionContext(session)),
        journal_store=SimpleNamespace(list_scopes_in_session=AsyncMock()),
        command_repository=AsyncMock(),
        reservation_repository=AsyncMock(),
    )

    with pytest.raises(DecisionCommitConflict, match="corrupted execution head payload"):
        await unit_of_work.load_positions(
            environment=key.environment,
            account_label=key.account_label,
            as_of=NOW,
        )


@pytest.mark.asyncio
async def test_multi_symbol_missing_head_with_active_responsibility_fails_closed():
    from crypto_momentum_lab.domain.execution.ports import DecisionCommitConflict
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        PositionLedgerProjection,
    )
    from crypto_momentum_lab.domain.execution.recovery_codec import (
        PositionRecoveryCodec,
    )
    from crypto_momentum_lab.persistence.postgres.models import PositionReservationRow

    # Account has BTCUSDT and ETHUSDT.
    # BTCUSDT has a valid durable head.
    # ETHUSDT has an active reservation, but its head was deleted / missing.
    key_btc = PositionKey("live", "primary", "BTCUSDT", FuturesPositionSide.BOTH)
    scope_btc = AccountFactStreamScope.for_position_key(
        key_btc, stream_id="btc-events", stream_epoch="epoch-1"
    )
    proj_btc = PositionLedgerProjection(
        position_key=key_btc,
        total_active_quantity=Decimal("1.0"),
        unallocated_quantity=Decimal("1.0"),
        reconciliation_gap=Decimal("0"),
        high_watermark_trade_at=None,
        active_batches=(),
        archived_episodes=(),
        active_episode=None,
        event_cut=NOW,
        stream_scope=scope_btc,
    )
    head_btc = ExecutionBookHeadRow(
        environment=key_btc.environment,
        account_label=key_btc.account_label,
        symbol=key_btc.symbol,
        position_side=key_btc.position_side.value,
        stream_id=scope_btc.stream_id,
        stream_epoch=scope_btc.stream_epoch,
        revision=1,
        projection_version="proj-btc",
        state_payload={
            "projection": PositionRecoveryCodec.encode_projection(proj_btc),
            "projection_digest": PositionRecoveryCodec.compute_projection_digest(proj_btc),
            "facts_hash": "a" * 64,
            "journal_revision": 1,
            "active_trade_ids": [],
        },
        updated_at=NOW,
    )

    # Active reservation for ETHUSDT
    active_res_eth = PositionReservationRow(
        reservation_id="res-eth-1",
        environment="live",
        account_label="primary",
        strategy_name="trend_v1",
        symbol="ETHUSDT",
        position_side="BOTH",
        batch_id="b-1",
        command_id="cmd-1",
        reserved_quantity=Decimal("5.0"),
        consumed_quantity=Decimal("0"),
        released_quantity=Decimal("0"),
        status="ACTIVE",
        created_at=NOW,
        updated_at=NOW,
    )

    async def scalars(statement):
        s_str = str(statement).lower()
        if "execution_book_heads" in s_str:
            return _Rows([head_btc])
        if "position_reservations" in s_str:
            return _Rows([active_res_eth])
        return _Rows([])

    session = _FakeSession(heads=[head_btc], get_result=head_btc)
    session.scalars = AsyncMock(side_effect=scalars)
    unit_of_work = AsyncPostgresExecutionUnitOfWork(
        Mock(return_value=_SessionContext(session)),
        journal_store=SimpleNamespace(list_scopes_in_session=AsyncMock()),
        command_repository=AsyncMock(),
        reservation_repository=AsyncMock(),
    )

    with pytest.raises(
        DecisionCommitConflict, match="position ETHUSDT:BOTH has active reservation res-eth-1 but no durable head"
    ):
        await unit_of_work.load_positions(
            environment="live",
            account_label="primary",
            as_of=NOW,
        )

