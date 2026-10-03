from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.command_models import (
    DispatchState,
    ExecutionScope,
)
from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
from crypto_momentum_lab.domain.execution.execution_book import (
    Accepted,
    AlreadyAccepted,
    Blocked,
    CommandConflict,
    ExecutionBook,
    ExecutionRequest,
    StaleView,
)
from crypto_momentum_lab.domain.execution.legacy_command_repository import (
    LegacyCommandRepositoryAdapter,
)
from crypto_momentum_lab.domain.execution.legacy_reservation_repository import (
    assemble_legacy_execution_book,
)
from crypto_momentum_lab.domain.execution.observation_models import Applied, Duplicate
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderState,
    FuturesPositionSide,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    FactCoverageInterval,
    FactCoverageStatus,
    PositionHealthStatus,
)
from crypto_momentum_lab.domain.execution.trade_command import TradeCommandType


def _dt(hour: int, minute: int, second: int = 0) -> datetime:
    return datetime(2026, 9, 25, hour, minute, second, tzinfo=UTC)


def _scope() -> ExecutionScope:
    return ExecutionScope(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.LONG,
    )


@pytest.mark.asyncio
async def test_old_stream_snapshot_waits_without_copy_or_transaction() -> None:
    from crypto_momentum_lab.domain.execution.observation_models import (
        EvidencePendingReason,
        WaitingForEvidence,
    )
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        AccountFactStreamScope,
    )

    class NoTransaction:
        def transaction(self, key):
            raise AssertionError("old-stream snapshot opened a transaction")

    book = ExecutionBook(execution_unit_of_work=NoTransaction())
    book._persistence_failed = False
    key = _scope().to_position_key()
    book._stream_scopes[key.canonical_id] = AccountFactStreamScope.for_position_key(
        key, stream_id="account_event_hub", stream_epoch="old-epoch"
    )

    def reject_copy(*, key):
        raise AssertionError("old-stream snapshot cloned the execution book")

    book._staged_copy = reject_copy
    result = await book.observe(
        ExecutionEvidence(
            evidence_id="new-epoch-snapshot",
            scope=_scope(),
            observed_at=_dt(10, 0),
            stream_id="account_event_hub",
            stream_epoch="new-epoch",
            sequence=1,
        )
    )
    assert isinstance(result, WaitingForEvidence)
    assert result.reason is EvidencePendingReason.STREAM_RECOVERY_PROOF_REQUIRED


@pytest.mark.asyncio
async def test_flat_position_stream_adoption_avoids_copy_and_transaction() -> None:
    from crypto_momentum_lab.domain.account.models import AccountPositionSnapshot
    from crypto_momentum_lab.domain.execution.observation_models import (
        Applied,
    )
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        AccountFactStreamScope,
    )

    class NoTransaction:
        def transaction(self, key):
            raise AssertionError("flat snapshot stream adoption opened a transaction")

    book = ExecutionBook(execution_unit_of_work=NoTransaction())
    book._persistence_failed = False
    key = _scope().to_position_key()
    book._stream_scopes[key.canonical_id] = AccountFactStreamScope.for_position_key(
        key, stream_id="account_event_hub", stream_epoch="old-epoch"
    )

    def reject_copy(*, key):
        raise AssertionError("flat snapshot stream adoption cloned the execution book")

    book._staged_copy = reject_copy
    flat_snap = AccountPositionSnapshot(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side="LONG",
        position_amt=Decimal("0"),
        entry_price=Decimal("0"),
        mark_price=Decimal("65000"),
        unrealized_pnl=Decimal("0"),
        notional=Decimal("0"),
        leverage=None,
        margin_type=None,
        observed_at=_dt(10, 0),
        raw_payload={},
    )
    result = await book.observe(
        ExecutionEvidence(
            evidence_id="new-epoch-flat-snapshot",
            scope=_scope(),
            observed_at=_dt(10, 0),
            stream_id="account_event_hub",
            stream_epoch="new-epoch",
            sequence=1,
            snapshot=flat_snap,
        )
    )
    assert isinstance(result, Applied)
    assert book._stream_scopes[key.canonical_id].stream_epoch == "new-epoch"

    # Reading with the new stream epoch must succeed cleanly without stream mismatch
    view = await book.read(
        _scope(),
        stream_id="account_event_hub",
        stream_epoch="new-epoch",
    )
    assert view.total_quantity == Decimal("0")
    assert view.stream_scope.stream_epoch == "new-epoch"


@pytest.mark.asyncio
async def test_legacy_stream_scope_smoothly_adopts_active_epoch_when_exchange_is_flat() -> (
    None
):
    from crypto_momentum_lab.domain.account.models import AccountPositionSnapshot
    from crypto_momentum_lab.domain.execution.observation_models import (
        Applied,
    )
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        AccountFactStreamScope,
    )

    class NoTransaction:
        def transaction(self, key):
            raise AssertionError("legacy snapshot stream adoption opened a transaction")

    book = ExecutionBook(execution_unit_of_work=NoTransaction())
    book._persistence_failed = False
    key = _scope().to_position_key()
    book._stream_scopes[key.canonical_id] = AccountFactStreamScope.for_position_key(
        key, stream_id="legacy-postgres-account", stream_epoch="unversioned"
    )

    flat_snap = AccountPositionSnapshot(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side="LONG",
        position_amt=Decimal("0"),
        entry_price=Decimal("0"),
        mark_price=Decimal("65000"),
        unrealized_pnl=Decimal("0"),
        notional=Decimal("0"),
        leverage=None,
        margin_type=None,
        observed_at=_dt(10, 0),
        raw_payload={},
    )
    result = await book.observe(
        ExecutionEvidence(
            evidence_id="new-epoch-flat-from-legacy",
            scope=_scope(),
            observed_at=_dt(10, 0),
            stream_id="account_event_hub",
            stream_epoch="active-epoch",
            sequence=1,
            snapshot=flat_snap,
        )
    )
    assert isinstance(result, Applied)
    assert book._stream_scopes[key.canonical_id].stream_id == "account_event_hub"
    assert book._stream_scopes[key.canonical_id].stream_epoch == "active-epoch"

    # Reading with active stream must succeed
    view = await book.read(
        _scope(),
        stream_id="account_event_hub",
        stream_epoch="active-epoch",
    )
    assert view.total_quantity == Decimal("0")
    assert view.stream_scope.stream_epoch == "active-epoch"


@pytest.mark.asyncio
async def test_flat_position_act_when_head_is_none() -> None:
    from contextlib import asynccontextmanager

    from crypto_momentum_lab.domain.execution.execution_book import Accepted
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        AccountFactStreamScope,
    )

    class FakeTx:
        def __init__(self):
            self.persisted_head = None
            self.saved_reservations = None

        async def load_head(self, key):
            return None

        async def save_reservations(self, reservations, **kwargs):
            self.saved_reservations = reservations

        async def persist_head(self, **kwargs):
            self.persisted_head = kwargs
            return 1

        async def upsert_outbox(self, **kwargs):
            pass

    class FakeUow:
        def __init__(self):
            self.tx = FakeTx()

        @asynccontextmanager
        async def transaction(self, key):
            yield self.tx

    uow = FakeUow()
    book = ExecutionBook(execution_unit_of_work=uow)
    book._persistence_failed = False
    scope = ExecutionScope(
        environment="paper",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.LONG,
    )
    key = scope.to_position_key()
    book._stream_scopes[key.canonical_id] = AccountFactStreamScope.for_position_key(
        key, stream_id="account_event_hub", stream_epoch="current-epoch"
    )

    # Set up ready coverage so it can trade
    timestamp = _dt(10, 0)
    journal = book._ensure_journal(key)
    journal.set_coverage(
        FactCoverageInterval(
            start_at=timestamp,
            end_at=timestamp,
            status=FactCoverageStatus.CONFIRMED,
            stream_scope=book._stream_scopes[key.canonical_id],
            evidence_observed_at=timestamp,
        )
    )
    view = await book.read(scope)

    req = ExecutionRequest(
        request_id="entry-1",
        scope=scope,
        strategy_name="trend_v1",
        strategy_version="1.0.0",
        run_id="run-1",
        decision_ref="dec-1",
        expected_view_token=view.projection_version,
        action=TradeCommandType.ENTRY,
        requested_quantity=Decimal("1.0"),
    )
    result = await book.act(req)
    assert isinstance(result, Accepted)
    assert uow.tx.persisted_head is not None
    assert uow.tx.persisted_head["expected_revision"] == 0
    assert uow.tx.persisted_head["is_flat_adoption"] is False
    assert book._head_revisions[key.canonical_id] == 1


@pytest.mark.asyncio
async def test_flat_position_act_live_without_coverage_succeeds() -> None:
    from contextlib import asynccontextmanager

    from crypto_momentum_lab.domain.execution.execution_book import Accepted
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        AccountFactStreamScope,
    )

    class FakeTx:
        def __init__(self):
            self.persisted_head = None
            self.saved_reservations = None

        async def load_head(self, key):
            return None

        async def save_reservations(self, reservations, **kwargs):
            self.saved_reservations = reservations

        async def persist_head(self, **kwargs):
            self.persisted_head = kwargs
            return 1

        async def upsert_outbox(self, **kwargs):
            pass

    class FakeUow:
        def __init__(self):
            self.tx = FakeTx()

        @asynccontextmanager
        async def transaction(self, key):
            yield self.tx

    uow = FakeUow()
    book = ExecutionBook(execution_unit_of_work=uow)
    book._persistence_failed = False
    scope = ExecutionScope(
        environment="live",
        account_label="primary",
        symbol="ALGOUSDT",
        position_side=FuturesPositionSide.LONG,
    )
    key = scope.to_position_key()
    stream_scope = AccountFactStreamScope.for_position_key(
        key, stream_id="account_event_hub", stream_epoch="live-epoch-20260928"
    )
    book._stream_scopes[key.canonical_id] = stream_scope
    book._ensure_journal(key)

    # In live trading, a clean flat position has stream scope but NO coverage.
    view = await book.read(
        scope, stream_id="account_event_hub", stream_epoch="live-epoch-20260928"
    )
    assert view.is_ready_for_trade is True

    req = ExecutionRequest(
        request_id="entry-algo-1",
        scope=scope,
        strategy_name="trend_v1",
        strategy_version="1.0.0",
        run_id="run-1",
        decision_ref="dec-algo-1",
        expected_view_token=view.projection_version,
        action=TradeCommandType.ENTRY,
        requested_quantity=Decimal("100.0"),
    )
    result = await book.act(req)
    assert isinstance(result, Accepted)
    assert uow.tx.persisted_head is not None
    assert uow.tx.persisted_head["expected_revision"] == 0
    assert book._head_revisions[key.canonical_id] == 1


@pytest.mark.asyncio
async def test_open_position_act_live_without_coverage_succeeds() -> None:
    from contextlib import asynccontextmanager

    from crypto_momentum_lab.domain.account import AccountFillEvent
    from crypto_momentum_lab.domain.execution.execution_book import Accepted
    from crypto_momentum_lab.domain.execution.ports import ExecutionHeadSnapshot
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        AccountFactStreamScope,
    )

    class FakeTx:
        def __init__(self, projection_version: str):
            self.projection_version = projection_version
            self.persisted_head = None
            self.saved_reservations = None

        async def load_head(self, key):
            return ExecutionHeadSnapshot(
                revision=1,
                stream_id="account_event_hub",
                stream_epoch="live-epoch-20260928",
                projection_version=self.projection_version,
                state_payload={"active_reservation_ids": []},
            )

        async def save_reservations(self, reservations, **kwargs):
            self.saved_reservations = reservations

        async def persist_head(self, **kwargs):
            self.persisted_head = kwargs
            return 2

        async def upsert_outbox(self, **kwargs):
            pass

    class FakeUow:
        def __init__(self):
            self.tx = None

        @asynccontextmanager
        async def transaction(self, key):
            yield self.tx

    uow = FakeUow()
    book = ExecutionBook(execution_unit_of_work=uow)
    book._persistence_failed = False
    scope = ExecutionScope(
        environment="live",
        account_label="primary",
        symbol="ALGOUSDT",
        position_side=FuturesPositionSide.LONG,
    )
    key = scope.to_position_key()
    stream_scope = AccountFactStreamScope.for_position_key(
        key, stream_id="account_event_hub", stream_epoch="live-epoch-20260928"
    )
    book._stream_scopes[key.canonical_id] = stream_scope
    book._head_revisions[key.canonical_id] = 1
    journal = book._ensure_journal(key)
    journal.append_fill(
        AccountFillEvent(
            environment="live",
            account_label="primary",
            symbol="ALGOUSDT",
            trade_id="fill-1",
            order_id="order-1",
            side="BUY",
            price=Decimal("1.0"),
            quantity=Decimal("100.0"),
            realized_pnl=Decimal("0"),
            fee=Decimal("0.01"),
            fee_asset="USDT",
            trade_at=datetime.now(UTC),
            raw_payload={"row": {"ps": "LONG"}},
        )
    )

    view = await book.read(
        scope, stream_id="account_event_hub", stream_epoch="live-epoch-20260928"
    )
    assert view.is_ready_for_trade is True
    assert view.total_quantity == Decimal("100.0")
    assert len(view.batches) == 1
    uow.tx = FakeTx(projection_version=view.projection_version)

    req = ExecutionRequest(
        request_id="exit-algo-1",
        scope=scope,
        strategy_name="trend_v1",
        strategy_version="1.0.0",
        run_id="run-1",
        decision_ref="dec-algo-1",
        expected_view_token=view.projection_version,
        action=TradeCommandType.EXIT,
        requested_quantity=Decimal("100.0"),
    )
    result = await book.act(req)
    assert isinstance(result, Accepted)


@pytest.mark.asyncio
async def test_flat_position_act_can_adopt_older_flat_head() -> None:
    from contextlib import asynccontextmanager

    from crypto_momentum_lab.domain.execution.execution_book import Accepted
    from crypto_momentum_lab.domain.execution.ports import ExecutionHeadSnapshot
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        AccountFactStreamScope,
    )

    class FakeTx:
        def __init__(self):
            self.persisted_head = None
            self.saved_reservations = None

        async def load_head(self, key):
            return ExecutionHeadSnapshot(
                revision=1,
                stream_id="account_event_hub",
                stream_epoch="older-epoch",
                projection_version="old_pv",
                state_payload={"active_reservation_ids": []},
            )

        async def save_reservations(self, reservations, **kwargs):
            self.saved_reservations = reservations

        async def persist_head(self, **kwargs):
            self.persisted_head = kwargs
            return 2

        async def upsert_outbox(self, **kwargs):
            pass

    class FakeUow:
        def __init__(self):
            self.tx = FakeTx()

        @asynccontextmanager
        async def transaction(self, key):
            yield self.tx

    uow = FakeUow()
    book = ExecutionBook(execution_unit_of_work=uow)
    book._persistence_failed = False
    scope = ExecutionScope(
        environment="paper",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.LONG,
    )
    key = scope.to_position_key()
    book._stream_scopes[key.canonical_id] = AccountFactStreamScope.for_position_key(
        key, stream_id="account_event_hub", stream_epoch="current-epoch"
    )
    book._head_revisions[key.canonical_id] = 1

    # Set up ready coverage so it can trade
    timestamp = _dt(10, 0)
    journal = book._ensure_journal(key)
    journal.set_coverage(
        FactCoverageInterval(
            start_at=timestamp,
            end_at=timestamp,
            status=FactCoverageStatus.CONFIRMED,
            stream_scope=book._stream_scopes[key.canonical_id],
            evidence_observed_at=timestamp,
        )
    )
    view = await book.read(scope)

    req = ExecutionRequest(
        request_id="entry-2",
        scope=scope,
        strategy_name="trend_v1",
        strategy_version="1.0.0",
        run_id="run-1",
        decision_ref="dec-1",
        expected_view_token=view.projection_version,
        action=TradeCommandType.ENTRY,
        requested_quantity=Decimal("1.0"),
    )
    result = await book.act(req)
    assert isinstance(result, Accepted)
    assert uow.tx.persisted_head is not None
    assert uow.tx.persisted_head["expected_revision"] == 1
    assert uow.tx.persisted_head["is_flat_adoption"] is True
    assert book._head_revisions[key.canonical_id] == 2


@pytest.mark.asyncio
async def test_non_flat_position_act_with_older_head_is_blocked() -> None:
    from contextlib import asynccontextmanager

    from crypto_momentum_lab.domain.execution.execution_book import Blocked
    from crypto_momentum_lab.domain.execution.ports import ExecutionHeadSnapshot
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        AccountFactStreamScope,
    )

    class FakeTx:
        async def load_head(self, key):
            return ExecutionHeadSnapshot(
                revision=1,
                stream_id="account_event_hub",
                stream_epoch="older-epoch",
                projection_version="old_pv",
                # Active reservations present => NOT flat!
                state_payload={"active_reservation_ids": ["res-1"]},
            )

    class FakeUow:
        @asynccontextmanager
        async def transaction(self, key):
            yield FakeTx()

    uow = FakeUow()
    book = ExecutionBook(execution_unit_of_work=uow)
    book._persistence_failed = False
    scope = _scope()
    key = scope.to_position_key()
    book._stream_scopes[key.canonical_id] = AccountFactStreamScope.for_position_key(
        key, stream_id="account_event_hub", stream_epoch="current-epoch"
    )
    book._head_revisions[key.canonical_id] = 1

    view = await book.read(scope)
    req = ExecutionRequest(
        request_id="entry-3",
        scope=scope,
        strategy_name="trend_v1",
        strategy_version="1.0.0",
        run_id="run-1",
        decision_ref="dec-1",
        expected_view_token=view.projection_version,
        action=TradeCommandType.ENTRY,
        requested_quantity=Decimal("1.0"),
    )
    result = await book.act(req)
    assert isinstance(result, Blocked)
    assert "stream changed without a validated recovery checkpoint" in result.reason


@pytest.mark.asyncio
async def test_execution_book_read_deterministic_view() -> None:
    book = ExecutionBook()
    scope = _scope()

    view1 = await book.read(scope)
    view2 = await book.read(scope)

    # Calling read multiple times without state changes returns exact same token
    assert view1.projection_version == view2.projection_version
    assert view1.health_status == PositionHealthStatus.READY
    assert view1.is_ready_for_trade is False  # unconfirmed coverage


@pytest.mark.asyncio
async def test_execution_book_act_stale_view_rejected() -> None:
    book = ExecutionBook()
    scope = _scope()

    req = ExecutionRequest(
        request_id="req-1",
        scope=scope,
        strategy_name="trend_v1",
        strategy_version="1.0.0",
        run_id="run-1",
        decision_ref="dec-1",
        expected_view_token="pv_BTCUSDT_wrong_token",
        action=TradeCommandType.ENTRY,
        requested_quantity=Decimal("1.0"),
    )

    result = await book.act(req)
    assert isinstance(result, StaleView)
    assert result.expected_token == "pv_BTCUSDT_wrong_token"


@pytest.mark.asyncio
async def test_execution_book_act_blocked_when_not_ready() -> None:
    book = ExecutionBook()
    scope = _scope()
    view = await book.read(scope)

    req = ExecutionRequest(
        request_id="req-1",
        scope=scope,
        strategy_name="trend_v1",
        strategy_version="1.0.0",
        run_id="run-1",
        decision_ref="dec-1",
        expected_view_token=view.projection_version,
        action=TradeCommandType.ENTRY,
        requested_quantity=Decimal("1.0"),
    )

    # Not ready because coverage is unconfirmed
    result = await book.act(req)
    assert isinstance(result, Blocked)
    assert "not ready for trade" in result.reason


@pytest.mark.asyncio
async def test_execution_book_act_and_idempotency_workflow() -> None:
    book = ExecutionBook()
    scope = _scope()
    t0 = _dt(10, 0)

    # Ingest confirmed coverage, fill, and snapshot via observe
    f1 = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id="t1",
        order_id="ord_1",
        side="BUY",
        price=Decimal("50000"),
        quantity=Decimal("1.0"),
        realized_pnl=Decimal("0"),
        fee=Decimal("1"),
        fee_asset="USDT",
        trade_at=t0,
        raw_payload={"positionSide": "LONG", "is_system": True},
    )
    snap = AccountPositionSnapshot(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side="LONG",
        position_amt=Decimal("1.0"),
        entry_price=Decimal("50000"),
        mark_price=Decimal("50000"),
        unrealized_pnl=Decimal("0"),
        notional=Decimal("50000"),
        leverage=5,
        margin_type="cross",
        observed_at=t0,
        raw_payload={},
    )

    key = scope.to_position_key()
    journal = book._ensure_journal(key)
    journal.set_coverage(
        FactCoverageInterval(
            start_at=t0,
            end_at=t0,
            status=FactCoverageStatus.CONFIRMED,
        )
    )

    ev_fill = ExecutionEvidence(
        evidence_id="ev_f1",
        scope=scope,
        observed_at=t0,
        fill=f1,
        snapshot=snap,
    )
    applied = await book.observe(ev_fill)
    assert isinstance(applied, Applied)

    # Now read authoritative view
    view = await book.read(scope)
    assert view.health_status == PositionHealthStatus.READY
    assert view.is_ready_for_trade is True

    # Submit exit request
    req = ExecutionRequest(
        request_id="req-exit-1",
        scope=scope,
        strategy_name="trend_v1",
        strategy_version="1.0.0",
        run_id="run-1",
        decision_ref="dec-exit-1",
        expected_view_token=view.projection_version,
        action=TradeCommandType.EXIT,
        requested_quantity=Decimal("1.0"),
    )

    act_result = await book.act(req)
    assert isinstance(act_result, Accepted)
    assert act_result.receipt.request_id == "req-exit-1"
    assert len(act_result.receipt.reservations) == 1
    assert act_result.receipt.reservations[0].reserved_quantity == Decimal("1.0")

    # Idempotent re-submission returns AlreadyAccepted with same receipt
    act_repeat = await book.act(req)
    assert isinstance(act_repeat, AlreadyAccepted)
    assert act_repeat.receipt.request_id == "req-exit-1"

    # Conflicting submission with different requested quantity returns CommandConflict
    req_conflict = ExecutionRequest(
        request_id="req-exit-1",
        scope=scope,
        strategy_name="trend_v1",
        strategy_version="1.0.0",
        run_id="run-1",
        decision_ref="dec-exit-1",
        expected_view_token=view.projection_version,
        action=TradeCommandType.EXIT,
        requested_quantity=Decimal("0.5"),
    )
    conflict_result = await book.act(req_conflict)
    assert isinstance(conflict_result, CommandConflict)


@pytest.mark.asyncio
async def test_execution_book_observe_idempotency_and_settlement() -> None:
    book = ExecutionBook()
    scope = _scope()
    t0 = _dt(10, 0)

    f1 = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id="t1",
        order_id="ord_1",
        side="BUY",
        price=Decimal("50000"),
        quantity=Decimal("1.0"),
        realized_pnl=Decimal("0"),
        fee=Decimal("1"),
        fee_asset="USDT",
        trade_at=t0,
        raw_payload={"positionSide": "LONG", "is_system": True},
    )
    ev = ExecutionEvidence(
        evidence_id="ev_1",
        scope=scope,
        observed_at=t0,
        fill=f1,
    )

    res1 = await book.observe(ev)
    assert isinstance(res1, Applied)

    # Re-observing same evidence_id returns Duplicate
    res2 = await book.observe(ev)
    assert isinstance(res2, Duplicate)
    assert res2.evidence_id == "ev_1"


@pytest.mark.asyncio
async def test_execution_book_fails_closed_when_acceptance_persistence_fails() -> None:
    class FailingCommandRepository:
        async def upsert_execution_command(self, **kwargs: object) -> None:
            raise RuntimeError("database unavailable")

    book = ExecutionBook(
        command_repository=LegacyCommandRepositoryAdapter(FailingCommandRepository())
    )
    scope = _scope()
    t0 = _dt(10, 0)
    flat = AccountPositionSnapshot(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side="LONG",
        position_amt=Decimal("0"),
        entry_price=Decimal("0"),
        mark_price=Decimal("50000"),
        unrealized_pnl=Decimal("0"),
        notional=Decimal("0"),
        leverage=5,
        margin_type="cross",
        observed_at=t0,
        raw_payload={},
    )
    await book.observe(
        ExecutionEvidence(
            evidence_id="flat-for-fail-closed",
            scope=scope,
            observed_at=t0,
            snapshot=flat,
            coverage=FactCoverageInterval(
                start_at=t0,
                end_at=t0,
                status=FactCoverageStatus.CONFIRMED,
            ),
        )
    )
    view = await book.read(scope)
    req = ExecutionRequest(
        request_id="req-persist-failure",
        scope=scope,
        strategy_name="trend_v1",
        strategy_version="1.0.0",
        run_id="run-1",
        decision_ref="dec-1",
        expected_view_token=view.projection_version,
        action=TradeCommandType.ENTRY,
        requested_quantity=Decimal("1.0"),
    )

    result = await book.act(req)

    assert isinstance(result, Blocked)
    assert "database unavailable" in " ".join(result.diagnostics)
    assert book.get_outbox(req.request_id) is None
    assert book.list_outbox() == ()


@pytest.mark.asyncio
async def test_execution_book_persists_reservation_link_with_first_outbox_write() -> (
    None
):
    class RecordingCommandRepository:
        def __init__(self) -> None:
            self.writes: list[dict[str, object]] = []

        async def upsert_execution_command(self, **kwargs: object) -> None:
            self.writes.append(kwargs)

    command_repo = RecordingCommandRepository()
    book = ExecutionBook(
        command_repository=LegacyCommandRepositoryAdapter(command_repo)
    )
    scope = _scope()
    t0 = _dt(10, 0)
    fill = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id="outbox-entry-fill",
        order_id="entry-order",
        side="BUY",
        price=Decimal("50000"),
        quantity=Decimal("2.0"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=t0,
        raw_payload={"positionSide": "LONG"},
    )
    snapshot = AccountPositionSnapshot(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side="LONG",
        position_amt=Decimal("2.0"),
        entry_price=Decimal("50000"),
        mark_price=Decimal("50000"),
        unrealized_pnl=Decimal("0"),
        notional=Decimal("100000"),
        leverage=5,
        margin_type="cross",
        observed_at=t0,
        raw_payload={},
    )
    journal = book._ensure_journal(scope.to_position_key())
    journal.set_coverage(
        FactCoverageInterval(
            start_at=t0, end_at=t0, status=FactCoverageStatus.CONFIRMED
        )
    )
    await book.observe(
        ExecutionEvidence(
            evidence_id="seed-outbox-position",
            scope=scope,
            observed_at=t0,
            fill=fill,
            snapshot=snapshot,
        )
    )
    view = await book.read(scope)
    req = ExecutionRequest(
        request_id="req-with-reservation-link",
        scope=scope,
        strategy_name="trend_v1",
        strategy_version="1.0.0",
        run_id="run-1",
        decision_ref="dec-exit",
        expected_view_token=view.projection_version,
        action=TradeCommandType.EXIT,
        requested_quantity=Decimal("1.0"),
    )

    result = await book.act(req)

    assert isinstance(result, Accepted)
    assert len(command_repo.writes) == 1
    details = command_repo.writes[0]["details"]
    assert isinstance(details, dict)
    assert details["reservations"] == [
        reservation.reservation_id for reservation in result.receipt.reservations
    ]


@pytest.mark.asyncio
async def test_execution_book_restore_rejects_incomplete_active_command() -> None:
    class LegacyCommandRepository:
        async def load_active_execution_commands(self, **kwargs: object):
            return (
                {
                    "command_id": "legacy-command",
                    "status": "prepared",
                    "details": {"scope": {"account_label": "primary"}},
                },
            )

        async def load_seen_event_ids(self) -> tuple[str, ...]:
            return ()

        async def load_seen_fill_trade_ids(self) -> tuple[str, ...]:
            return ()

        async def load_execution_order_watermarks(self, **kwargs: object):
            return ()

    book = ExecutionBook(
        command_repository=LegacyCommandRepositoryAdapter(LegacyCommandRepository())
    )

    with pytest.raises(RuntimeError, match="restore active execution commands"):
        await book.restore(account_label="primary")

    assert book._persistence_failed is True


@pytest.mark.asyncio
async def test_durable_restore_requires_explicit_command_repository() -> None:
    book = ExecutionBook(execution_unit_of_work=object())

    with pytest.raises(RuntimeError, match="requires a command repository"):
        await book.restore(account_label="primary")

    assert book._persistence_failed is True


@pytest.mark.asyncio
async def test_linked_settlement_ignores_late_duplicate() -> None:
    from crypto_momentum_lab.domain.execution.trade_command import (
        PositionReservation,
        TradeCommand,
    )
    from crypto_momentum_lab.domain.strategy import EntryType, StrategySide

    book = ExecutionBook()
    scope = _scope()
    key = scope.to_position_key()

    def register(command_id: str, reservation_id: str, quantity: str) -> None:
        command = TradeCommand(
            command_id=command_id,
            position_key=key,
            command_type=TradeCommandType.EXIT,
            side=StrategySide.LONG,
            order_type=EntryType.MARKET,
            requested_quantity=Decimal(quantity),
            reduce_only=True,
        )
        reservation = PositionReservation(
            reservation_id=reservation_id,
            command_id=command_id,
            position_key=key,
            batch_id=f"batch-{command_id}",
            reserved_quantity=Decimal(quantity),
            created_at=_dt(10, 0),
        )
        book.coordinator.register_reservation(reservation)
        book.register_prepared_command(command, scope, [reservation_id])

    register("order-a", "reservation-a", "4")
    register("order-b", "reservation-b", "5")

    def evidence(
        evidence_id: str,
        order_id: str,
        quantity: str,
        state: ExchangeOrderState,
    ) -> ExecutionEvidence:
        cumulative = Decimal(quantity)
        return ExecutionEvidence(
            evidence_id=evidence_id,
            scope=scope,
            observed_at=_dt(10, 1),
            fill=AccountFillEvent(
                environment="live",
                account_label="primary",
                symbol="BTCUSDT",
                trade_id=f"{order_id}-cum-{quantity}",
                order_id=order_id,
                side="SELL",
                price=Decimal("100"),
                quantity=cumulative,
                realized_pnl=Decimal("0"),
                fee=Decimal("0"),
                fee_asset="USDT",
                trade_at=_dt(10, 1),
                raw_payload={
                    "is_cumulative": True,
                    "cum_qty": quantity,
                    "cum_quote": str(cumulative * Decimal("100")),
                    "reduce_only": True,
                },
            ),
            order_event=ExchangeOrderEvent(
                event_id=evidence_id,
                client_order_id=order_id,
                state=state,
                occurred_at=_dt(10, 1),
                exchange_order_id=f"exchange-{order_id}",
                details={"is_reduce_only": True},
            ),
        )

    await book.observe(
        evidence(
            "fill-a-2",
            "order-a",
            "2",
            ExchangeOrderState.PARTIALLY_FILLED,
        )
    )
    await book.observe(
        evidence(
            "fill-b-3",
            "order-b",
            "3",
            ExchangeOrderState.PARTIALLY_FILLED,
        )
    )
    reservations = {r.reservation_id: r for r in book.get_active_reservations(key)}
    assert reservations["reservation-a"].consumed_quantity == Decimal("2")
    assert reservations["reservation-b"].consumed_quantity == Decimal("3")

    await book.observe(
        ExecutionEvidence(
            evidence_id="cancel-b",
            scope=scope,
            observed_at=_dt(10, 2),
            order_event=ExchangeOrderEvent(
                event_id="cancel-b",
                client_order_id="order-b",
                state=ExchangeOrderState.CANCELED,
                occurred_at=_dt(10, 2),
                exchange_order_id="exchange-order-b",
                details={},
            ),
        )
    )
    duplicate_late_report = await book.observe(
        evidence(
            "late-b-same-watermark",
            "order-b",
            "3",
            ExchangeOrderState.PARTIALLY_FILLED,
        )
    )
    assert isinstance(duplicate_late_report, Applied)
    assert book.get_outbox("order-b").state is DispatchState.TERMINAL
    active = {r.reservation_id: r for r in book.get_active_reservations(key)}
    assert active["reservation-a"].active_quantity == Decimal("2")
    assert "reservation-b" not in active

    late_additional_fill = await book.observe(
        evidence(
            "late-b-new-fill",
            "order-b",
            "4",
            ExchangeOrderState.PARTIALLY_FILLED,
        )
    )
    assert isinstance(late_additional_fill, Applied)
    assert late_additional_fill.recovery_required is True
    assert book.get_active_reservations(key)[0].reservation_id == "reservation-a"


@pytest.mark.asyncio
async def test_duplicate_trade_id_does_not_consume_twice() -> None:
    from crypto_momentum_lab.domain.execution.trade_command import (
        PositionReservation,
        TradeCommand,
    )
    from crypto_momentum_lab.domain.strategy import EntryType, StrategySide

    book = ExecutionBook()
    scope = _scope()
    key = scope.to_position_key()
    reservation = PositionReservation(
        reservation_id="reservation-duplicate-trade",
        command_id="order-duplicate-trade",
        position_key=key,
        batch_id="batch-duplicate-trade",
        reserved_quantity=Decimal("5"),
    )
    book.coordinator.register_reservation(reservation)
    book.register_prepared_command(
        TradeCommand(
            command_id="order-duplicate-trade",
            position_key=key,
            command_type=TradeCommandType.EXIT,
            side=StrategySide.LONG,
            order_type=EntryType.MARKET,
            requested_quantity=Decimal("5"),
            reduce_only=True,
        ),
        scope,
        [reservation.reservation_id],
    )

    def event(evidence_id: str) -> ExecutionEvidence:
        return ExecutionEvidence(
            evidence_id=evidence_id,
            scope=scope,
            observed_at=_dt(10, 0),
            fill=AccountFillEvent(
                environment="live",
                account_label="primary",
                symbol="BTCUSDT",
                trade_id="exchange-trade-1",
                order_id="order-duplicate-trade",
                side="SELL",
                price=Decimal("100"),
                quantity=Decimal("2"),
                realized_pnl=Decimal("0"),
                fee=Decimal("0"),
                fee_asset="USDT",
                trade_at=_dt(10, 0),
                raw_payload={"reduce_only": True},
            ),
        )

    first = await book.observe(event("trade-event-1"))
    duplicate = await book.observe(event("trade-event-2"))

    assert isinstance(first, Applied)
    assert first.consumed_quantity == Decimal("2")
    assert isinstance(duplicate, Applied)
    assert duplicate.consumed_quantity == Decimal("0")
    assert book.get_active_reservations(key)[0].consumed_quantity == Decimal("2")


@pytest.mark.asyncio
async def test_restore_cumulative_quantity_and_quote_watermarks() -> None:
    from copy import deepcopy

    from crypto_momentum_lab.domain.execution.execution_coordinator import (
        InMemoryPositionReservationRepository,
    )
    from crypto_momentum_lab.domain.execution.trade_command import (
        PositionReservation,
        TradeCommand,
    )
    from crypto_momentum_lab.domain.strategy import EntryType, StrategySide

    class PersistedCommandRepository:
        def __init__(self) -> None:
            self.rows: dict[str, dict[str, object]] = {}

        async def upsert_execution_command(self, **values: object) -> None:
            command_id = str(values["command_id"])
            self.rows[command_id] = dict(values)

        async def load_active_execution_commands(
            self, account_label: str | None = None
        ) -> tuple[dict[str, object], ...]:
            active = []
            for row in self.rows.values():
                details = row["details"]
                assert isinstance(details, dict)
                scope_data = details["scope"]
                assert isinstance(scope_data, dict)
                if row["status"] in ("terminal", "rejected"):
                    continue
                if (
                    account_label is not None
                    and scope_data["account_label"] != account_label
                ):
                    continue
                active.append(row)
            return tuple(active)

        async def load_execution_order_watermarks(
            self, account_label: str | None = None
        ) -> tuple[dict[str, object], ...]:
            watermarks = []
            for row in self.rows.values():
                details = row["details"]
                assert isinstance(details, dict)
                scope_data = details["scope"]
                assert isinstance(scope_data, dict)
                if (
                    account_label is not None
                    and scope_data["account_label"] != account_label
                ):
                    continue
                watermarks.append(
                    {
                        "scope": scope_data,
                        "client_order_id": row["client_order_id"],
                        "cumulative_filled_quantity": details[
                            "cumulative_filled_quantity"
                        ],
                        "cumulative_filled_quote": details["cumulative_filled_quote"],
                        "status": row["status"],
                    }
                )
            return tuple(watermarks)

        async def load_seen_event_ids(self) -> tuple[str, ...]:
            return ()

        async def load_seen_fill_trade_ids(self) -> tuple[str, ...]:
            return ()

    scope = _scope()
    key = scope.to_position_key()
    command_repo = PersistedCommandRepository()
    reservation_repo = InMemoryPositionReservationRepository()
    order_command = TradeCommand(
        command_id="order-restart-cumulative",
        position_key=key,
        command_type=TradeCommandType.EXIT,
        side=StrategySide.LONG,
        order_type=EntryType.MARKET,
        requested_quantity=Decimal("10"),
        reduce_only=True,
        created_at=_dt(10, 0),
    )
    reservation = PositionReservation(
        reservation_id="reservation-restart-cumulative",
        command_id=order_command.command_id,
        position_key=key,
        batch_id="batch-restart",
        reserved_quantity=Decimal("10"),
        created_at=_dt(10, 0),
    )
    reservation_repo.save_reservation(reservation)

    first_book = assemble_legacy_execution_book(
        command_repository=LegacyCommandRepositoryAdapter(command_repo),
        reservation_repository=reservation_repo,
    )
    first_book.coordinator.register_reservation(reservation)
    first_entry = first_book.register_prepared_command(
        order_command, scope, [reservation.reservation_id]
    )
    await first_book._persist_outbox_state(first_entry)
    await first_book.mark_dispatching(order_command.command_id)
    await first_book.mark_acknowledged(order_command.command_id, "exchange-order-1")

    terminal_command = TradeCommand(
        command_id="order-restart-terminal",
        position_key=key,
        command_type=TradeCommandType.ENTRY,
        side=StrategySide.LONG,
        order_type=EntryType.MARKET,
        requested_quantity=Decimal("1"),
        reduce_only=False,
        created_at=_dt(9, 0),
    )
    terminal_entry = first_book.register_prepared_command(terminal_command, scope)
    terminal_key = first_book._order_watermark_key(key, terminal_command.command_id)
    first_book._order_cumulative_fills[terminal_key] = Decimal("2")
    first_book._order_cumulative_quotes[terminal_key] = Decimal("190")
    await first_book._persist_outbox_state(terminal_entry)
    await first_book.mark_terminal(terminal_command.command_id)

    def cumulative_evidence(
        evidence_id: str,
        quantity: str,
        quote: str,
        price: str,
    ) -> ExecutionEvidence:
        cumulative_quantity = Decimal(quantity)
        return ExecutionEvidence(
            evidence_id=evidence_id,
            scope=scope,
            observed_at=_dt(10, 1),
            fill=AccountFillEvent(
                environment="live",
                account_label="primary",
                symbol="BTCUSDT",
                trade_id=f"cum-{quantity}",
                order_id=order_command.command_id,
                side="SELL",
                price=Decimal(price),
                quantity=cumulative_quantity,
                realized_pnl=Decimal("0"),
                fee=Decimal("0"),
                fee_asset="USDT",
                trade_at=_dt(10, 1),
                raw_payload={
                    "is_cumulative": True,
                    "cum_qty": quantity,
                    "cum_quote": quote,
                    "reduce_only": True,
                },
            ),
        )

    first_result = await first_book.observe(
        cumulative_evidence("cumulative-3", "3", "300", "100")
    )
    assert isinstance(first_result, Applied)
    assert first_result.consumed_quantity == Decimal("3")

    restored_book = assemble_legacy_execution_book(
        command_repository=LegacyCommandRepositoryAdapter(command_repo),
        reservation_repository=reservation_repo,
    )
    await restored_book.restore(account_label="primary")
    assert restored_book._order_cumulative_fills[
        restored_book._order_watermark_key(key, order_command.command_id)
    ] == Decimal("3")
    assert restored_book._order_cumulative_quotes[
        restored_book._order_watermark_key(key, order_command.command_id)
    ] == Decimal("300")
    assert restored_book._order_cumulative_fills[
        restored_book._order_watermark_key(key, terminal_command.command_id)
    ] == Decimal("2")

    second_result = await restored_book.observe(
        cumulative_evidence("cumulative-5-after-restart", "5", "700", "140")
    )
    assert isinstance(second_result, Applied)
    assert second_result.consumed_quantity == Decimal("2")
    fills = restored_book._ensure_journal(key).read_cut().fills
    incremental_fill = next(fill for fill in fills if fill.trade_id == "cum-5")
    assert incremental_fill.quantity == Decimal("2")
    assert incremental_fill.price == Decimal("200")
    restored_reservation = restored_book.get_active_reservations(key)[0]
    assert restored_reservation.consumed_quantity == Decimal("5")
    assert restored_reservation.active_quantity == Decimal("5")

    invalid_command_repo = deepcopy(command_repo)
    active_details = invalid_command_repo.rows[order_command.command_id]["details"]
    assert isinstance(active_details, dict)
    active_details["cumulative_filled_quote"] = "0"
    invalid_book = ExecutionBook(
        command_repository=LegacyCommandRepositoryAdapter(invalid_command_repo)
    )
    with pytest.raises(RuntimeError, match="cumulative fill watermarks"):
        await invalid_book.restore(account_label="primary")


@pytest.mark.parametrize(
    "resolution_state",
    (
        ExchangeOrderState.CANCELED,
        ExchangeOrderState.FILLED,
        ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
    ),
)
@pytest.mark.asyncio
async def test_restored_dispatch_latch_requires_durable_resolution(
    resolution_state: ExchangeOrderState,
) -> None:
    from crypto_momentum_lab.domain.execution.execution_coordinator import (
        InMemoryPositionReservationRepository,
    )
    from crypto_momentum_lab.domain.execution.trade_command import (
        PositionReservation,
        TradeCommand,
    )
    from crypto_momentum_lab.domain.strategy import EntryType, StrategySide

    class CommandRepository:
        def __init__(self) -> None:
            self.rows: dict[str, dict[str, object]] = {}

        async def upsert_execution_command(self, **values: object) -> None:
            self.rows[str(values["command_id"])] = dict(values)

        async def load_active_execution_commands(
            self, account_label: str | None = None
        ) -> tuple[dict[str, object], ...]:
            active = []
            for row in self.rows.values():
                if row["status"] in ("terminal", "rejected"):
                    continue
                details = row["details"]
                assert isinstance(details, dict)
                scope_data = details["scope"]
                assert isinstance(scope_data, dict)
                if (
                    account_label is None
                    or scope_data["account_label"] == account_label
                ):
                    active.append(row)
            return tuple(active)

        async def load_execution_order_watermarks(
            self, account_label: str | None = None
        ) -> tuple[dict[str, object], ...]:
            watermarks = []
            for row in self.rows.values():
                details = row["details"]
                assert isinstance(details, dict)
                scope_data = details["scope"]
                assert isinstance(scope_data, dict)
                if (
                    account_label is None
                    or scope_data["account_label"] == account_label
                ):
                    watermarks.append(
                        {
                            "scope": scope_data,
                            "client_order_id": row["client_order_id"],
                            "cumulative_filled_quantity": details[
                                "cumulative_filled_quantity"
                            ],
                            "cumulative_filled_quote": details[
                                "cumulative_filled_quote"
                            ],
                        }
                    )
            return tuple(watermarks)

        async def load_seen_event_ids(self) -> tuple[str, ...]:
            return ()

        async def load_seen_fill_trade_ids(self) -> tuple[str, ...]:
            return ()

    scope = _scope()
    key = scope.to_position_key()
    command_id = f"restored-{resolution_state.value}"
    command_repo = CommandRepository()
    reservation_repo = InMemoryPositionReservationRepository()
    command = TradeCommand(
        command_id=command_id,
        position_key=key,
        command_type=TradeCommandType.EXIT,
        side=StrategySide.LONG,
        order_type=EntryType.MARKET,
        requested_quantity=Decimal("1"),
        reduce_only=True,
        created_at=_dt(10, 0),
    )
    reservation = PositionReservation(
        reservation_id=f"reservation-{command_id}",
        command_id=command_id,
        position_key=key,
        batch_id="batch-restored",
        reserved_quantity=Decimal("1"),
        created_at=_dt(10, 0),
    )
    reservation_repo.save_reservation(reservation)
    first_book = assemble_legacy_execution_book(
        command_repository=LegacyCommandRepositoryAdapter(command_repo),
        reservation_repository=reservation_repo,
    )
    first_book.coordinator.register_reservation(reservation)
    prepared = first_book.register_prepared_command(
        command, scope, [reservation.reservation_id]
    )
    await first_book._persist_outbox_state(prepared)
    await first_book.mark_dispatching(command_id)

    book = assemble_legacy_execution_book(
        command_repository=LegacyCommandRepositoryAdapter(command_repo),
        reservation_repository=reservation_repo,
    )
    await book.restore(account_label="primary")
    restored = book.get_outbox(command_id)
    assert restored is not None
    assert restored.state is DispatchState.UNKNOWN
    assert command_id in book._dispatch_reconciliation_required_commands

    fill = None
    if resolution_state is ExchangeOrderState.FILLED:
        fill = AccountFillEvent(
            environment="live",
            account_label="primary",
            symbol="BTCUSDT",
            trade_id=f"fill-{command_id}",
            order_id=command_id,
            side="SELL",
            price=Decimal("100"),
            quantity=Decimal("1"),
            realized_pnl=Decimal("0"),
            fee=Decimal("0"),
            fee_asset="USDT",
            trade_at=_dt(10, 1),
            raw_payload={
                "positionSide": "LONG",
                "is_cumulative": True,
                "cum_qty": "1",
                "cum_quote": "100",
                "reduce_only": True,
            },
        )
    observed = await book.observe(
        ExecutionEvidence(
            evidence_id=f"resolution-{resolution_state.value}",
            scope=scope,
            observed_at=_dt(10, 1),
            fill=fill,
            order_event=ExchangeOrderEvent(
                event_id=f"event-{resolution_state.value}",
                client_order_id=command_id,
                state=resolution_state,
                occurred_at=_dt(10, 1),
                exchange_order_id="exchange-1",
                details={"account_label": "primary", "symbol": "BTCUSDT"},
            ),
        )
    )
    assert isinstance(observed, Applied)

    if resolution_state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION:
        assert command_id in book._dispatch_reconciliation_required_commands
        blocked = await book.act(
            ExecutionRequest(
                request_id=f"new-entry-{command_id}",
                scope=scope,
                strategy_name="trend_v1",
                strategy_version="1.0.0",
                run_id="run-after-restore",
                decision_ref="decision-after-restore",
                expected_view_token="*",
                action=TradeCommandType.ENTRY,
                requested_quantity=Decimal("1"),
            )
        )
        assert isinstance(blocked, Blocked)
        assert "reconciliation" in blocked.reason
        assert command_id in blocked.diagnostics
        return

    assert command_id not in book._dispatch_reconciliation_required_commands
    assert command_id not in book._recovery_required_commands

    timestamp = _dt(10, 2)
    journal = book._ensure_journal(key)
    journal.set_coverage(
        FactCoverageInterval(
            start_at=timestamp,
            end_at=timestamp,
            status=FactCoverageStatus.CONFIRMED,
        )
    )
    journal.record_snapshot(
        AccountPositionSnapshot(
            environment="live",
            account_label="primary",
            symbol="BTCUSDT",
            position_side="LONG",
            position_amt=Decimal("0"),
            entry_price=Decimal("0"),
            mark_price=Decimal("100"),
            unrealized_pnl=Decimal("0"),
            notional=Decimal("0"),
            leverage=5,
            margin_type="cross",
            observed_at=timestamp,
            raw_payload={},
        )
    )
    view = await book.read(scope)
    accepted = await book.act(
        ExecutionRequest(
            request_id=f"new-entry-{command_id}",
            scope=scope,
            strategy_name="trend_v1",
            strategy_version="1.0.0",
            run_id="run-after-restore",
            decision_ref="decision-after-restore",
            expected_view_token=view.projection_version,
            action=TradeCommandType.ENTRY,
            requested_quantity=Decimal("1"),
        )
    )
    assert isinstance(accepted, Accepted)


@pytest.mark.asyncio
async def test_execution_book_outbox_lifecycle_and_transitions() -> None:
    """Outbox state transitions:
    PREPARED -> DISPATCHING -> ACKNOWLEDGED / UNKNOWN / REJECTED.
    """
    book = ExecutionBook()
    scope = _scope()
    t0 = _dt(10, 0)
    key = scope.to_position_key()

    # Ingest position so view is trade-ready
    f_entry = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id="t_entry",
        order_id="ord_entry",
        side="BUY",
        price=Decimal("50000"),
        quantity=Decimal("2.0"),
        realized_pnl=Decimal("0"),
        fee=Decimal("1"),
        fee_asset="USDT",
        trade_at=t0,
        raw_payload={"positionSide": "LONG"},
    )
    journal = book._ensure_journal(key)
    journal.set_coverage(
        FactCoverageInterval(
            start_at=t0,
            end_at=t0,
            status=FactCoverageStatus.CONFIRMED,
        )
    )
    await book.observe(
        ExecutionEvidence(
            evidence_id="ev_init", scope=scope, observed_at=t0, fill=f_entry
        )
    )

    view = await book.read(scope)
    req = ExecutionRequest(
        request_id="req-outbox-1",
        scope=scope,
        strategy_name="trend_v1",
        strategy_version="1.0.0",
        run_id="run-1",
        decision_ref="dec-1",
        expected_view_token=view.projection_version,
        action=TradeCommandType.EXIT,
        requested_quantity=Decimal("2.0"),
    )

    act_res = await book.act(req)
    assert isinstance(act_res, Accepted)
    cmd_id = req.request_id

    # 1. Outbox entry is PREPARED upon act acceptance
    outbox = book.get_outbox(cmd_id)
    assert outbox is not None
    assert outbox.state == DispatchState.PREPARED
    assert outbox.attempt_count == 0

    # 2. mark_dispatching transitions to DISPATCHING
    dispatching = await book.mark_dispatching(cmd_id)
    assert dispatching.state == DispatchState.DISPATCHING
    assert dispatching.attempt_count == 1

    # 3. mark_unknown preserves active reservations!
    unknown = await book.mark_unknown(cmd_id, reason="Gateway timeout 504")
    assert unknown.state == DispatchState.UNKNOWN
    assert unknown.last_error == "Gateway timeout 504"
    active_res = book._coordinator.get_active_reservations(key)
    assert len(active_res) == 1
    assert active_res[0].active_quantity == Decimal("2.0")

    # 4. mark_acknowledged transitions to ACKNOWLEDGED
    acked = await book.mark_acknowledged(cmd_id, external_order_id="binance_ord_999")
    assert acked.state == DispatchState.ACKNOWLEDGED
    assert acked.external_order_id == "binance_ord_999"

    # 5. mark_terminal releases active reservations
    terminal = await book.mark_terminal(cmd_id, reason="Fully settled")
    assert terminal.state == DispatchState.TERMINAL
    active_after_term = book._coordinator.get_active_reservations(key)
    assert len(active_after_term) == 0


@pytest.mark.asyncio
async def test_execution_book_cumulative_fills_and_reservation_settlement() -> None:
    """Cumulative fill reports 3 -> 3 -> 5 only consume 5 total, never 11."""
    book = ExecutionBook()
    scope = _scope()
    t0 = _dt(10, 0)
    key = scope.to_position_key()

    # Open 5 BTC position
    f_open = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id="t_open",
        order_id="ord_open",
        side="BUY",
        price=Decimal("60000"),
        quantity=Decimal("5.0"),
        realized_pnl=Decimal("0"),
        fee=Decimal("1"),
        fee_asset="USDT",
        trade_at=t0,
        raw_payload={"positionSide": "LONG"},
    )
    journal = book._ensure_journal(key)
    journal.set_coverage(
        FactCoverageInterval(
            start_at=t0,
            end_at=t0,
            status=FactCoverageStatus.CONFIRMED,
        )
    )
    await book.observe(
        ExecutionEvidence(
            evidence_id="ev_open", scope=scope, observed_at=t0, fill=f_open
        )
    )

    view = await book.read(scope)
    req = ExecutionRequest(
        request_id="req-exit-cum",
        scope=scope,
        strategy_name="trend_v1",
        strategy_version="1.0.0",
        run_id="run-1",
        decision_ref="dec-cum",
        expected_view_token=view.projection_version,
        action=TradeCommandType.EXIT,
        requested_quantity=Decimal("5.0"),
    )

    act_res = await book.act(req)
    assert isinstance(act_res, Accepted)
    res_id = act_res.receipt.reservations[0].reservation_id
    r0 = book._coordinator.get_reservation(res_id)
    assert r0 is not None
    assert r0.reserved_quantity == Decimal("5.0")
    assert r0.consumed_quantity == Decimal("0")
    assert r0.active_quantity == Decimal("5.0")

    # Report 1: cumulative filled = 3.0
    f_cum1 = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id="t_fill_1",
        order_id="req-exit-cum",
        side="SELL",
        price=Decimal("61000"),
        quantity=Decimal("3.0"),
        realized_pnl=Decimal("100"),
        fee=Decimal("1"),
        fee_asset="USDT",
        trade_at=t0,
        raw_payload={"positionSide": "LONG", "is_cumulative": True, "cum_qty": "3.0"},
    )
    app1 = await book.observe(
        ExecutionEvidence(evidence_id="ev_c1", scope=scope, observed_at=t0, fill=f_cum1)
    )
    assert isinstance(app1, Applied)
    assert app1.consumed_quantity == Decimal("3.0")

    r1 = book._coordinator.get_reservation(res_id)
    assert r1 is not None
    assert r1.consumed_quantity == Decimal("3.0")
    assert r1.active_quantity == Decimal("2.0")

    # Report 2: duplicate cumulative filled = 3.0 (must consume 0 delta)
    f_cum2 = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id="t_fill_2",
        order_id="req-exit-cum",
        side="SELL",
        price=Decimal("61000"),
        quantity=Decimal("3.0"),
        realized_pnl=Decimal("100"),
        fee=Decimal("1"),
        fee_asset="USDT",
        trade_at=t0,
        raw_payload={"positionSide": "LONG", "is_cumulative": True, "cum_qty": "3.0"},
    )
    app2 = await book.observe(
        ExecutionEvidence(evidence_id="ev_c2", scope=scope, observed_at=t0, fill=f_cum2)
    )
    assert isinstance(app2, Applied)
    assert app2.consumed_quantity == Decimal("0")

    r2 = book._coordinator.get_reservation(res_id)
    assert r2 is not None
    assert r2.consumed_quantity == Decimal("3.0")
    assert r2.active_quantity == Decimal("2.0")

    # Report 3: cumulative filled = 5.0 (delta = 2.0, must consume remaining 2.0)
    f_cum3 = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id="t_fill_3",
        order_id="req-exit-cum",
        side="SELL",
        price=Decimal("61000"),
        quantity=Decimal("5.0"),
        realized_pnl=Decimal("150"),
        fee=Decimal("1"),
        fee_asset="USDT",
        trade_at=t0,
        raw_payload={"positionSide": "LONG", "is_cumulative": True, "cum_qty": "5.0"},
    )
    app3 = await book.observe(
        ExecutionEvidence(evidence_id="ev_c3", scope=scope, observed_at=t0, fill=f_cum3)
    )
    assert isinstance(app3, Applied)
    assert app3.consumed_quantity == Decimal("2.0")

    r3 = book._coordinator.get_reservation(res_id)
    assert r3 is not None
    # Crucial invariant: total consumed is 5.0, not 11.0!
    assert r3.consumed_quantity == Decimal("5.0")
    assert r3.active_quantity == Decimal("0")
    assert r3.reserved_quantity == (
        r3.active_quantity + r3.consumed_quantity + r3.released_quantity
    )


@pytest.mark.asyncio
async def test_execution_book_partial_fill_and_order_cancel_event() -> None:
    """When order partially filled then canceled, remaining reservation is released."""
    book = ExecutionBook()
    scope = _scope()
    t0 = _dt(10, 0)
    key = scope.to_position_key()

    # Open 2.0 BTC position
    f_open = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id="t_open",
        order_id="ord_open",
        side="BUY",
        price=Decimal("60000"),
        quantity=Decimal("2.0"),
        realized_pnl=Decimal("0"),
        fee=Decimal("1"),
        fee_asset="USDT",
        trade_at=t0,
        raw_payload={"positionSide": "LONG"},
    )
    journal = book._ensure_journal(key)
    journal.set_coverage(
        FactCoverageInterval(
            start_at=t0,
            end_at=t0,
            status=FactCoverageStatus.CONFIRMED,
        )
    )
    await book.observe(
        ExecutionEvidence(
            evidence_id="ev_open", scope=scope, observed_at=t0, fill=f_open
        )
    )

    view = await book.read(scope)
    req = ExecutionRequest(
        request_id="cmd-exit-partial",
        scope=scope,
        strategy_name="trend_v1",
        strategy_version="1.0.0",
        run_id="run-1",
        decision_ref="dec-part",
        expected_view_token=view.projection_version,
        action=TradeCommandType.EXIT,
        requested_quantity=Decimal("2.0"),
    )

    act_res = await book.act(req)
    assert isinstance(act_res, Accepted)
    res_id = act_res.receipt.reservations[0].reservation_id

    # 1. Partial fill of 0.8 BTC
    f_part = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id="t_part",
        order_id="cmd-exit-partial",
        side="SELL",
        price=Decimal("61000"),
        quantity=Decimal("0.8"),
        realized_pnl=Decimal("50"),
        fee=Decimal("1"),
        fee_asset="USDT",
        trade_at=t0,
        raw_payload={"positionSide": "LONG"},
    )
    await book.observe(
        ExecutionEvidence(
            evidence_id="ev_part", scope=scope, observed_at=t0, fill=f_part
        )
    )

    r_part = book._coordinator.get_reservation(res_id)
    assert r_part is not None
    assert r_part.consumed_quantity == Decimal("0.8")
    assert r_part.active_quantity == Decimal("1.2")

    # 2. Exchange order cancel event arrives
    ev_cancel = ExchangeOrderEvent(
        event_id="ev_cancel_1",
        client_order_id="cmd-exit-partial",
        exchange_order_id="binance_ord_cancel",
        state=ExchangeOrderState.CANCELED,
        occurred_at=t0,
        details={},
    )
    app_cancel = await book.observe(
        ExecutionEvidence(
            evidence_id="ev_cancel_evidence",
            scope=scope,
            observed_at=t0,
            order_event=ev_cancel,
        )
    )
    assert isinstance(app_cancel, Applied)
    assert app_cancel.released_quantity == Decimal("1.2")

    # 3. Invariant: reserved (2.0) == active (0) + consumed (0.8) + released (1.2)
    r_final = book._coordinator.get_reservation(res_id)
    assert r_final is not None
    assert r_final.active_quantity == Decimal("0")
    assert r_final.consumed_quantity == Decimal("0.8")
    assert r_final.released_quantity == Decimal("1.2")
    assert r_final.reserved_quantity == (
        r_final.active_quantity + r_final.consumed_quantity + r_final.released_quantity
    )

    # 4. Outbox is marked TERMINAL
    outbox = book.get_outbox("cmd-exit-partial")
    assert outbox is not None
    assert outbox.state == DispatchState.TERMINAL


@pytest.mark.asyncio
async def test_staged_copy_preserves_unrelated_positions_and_copies_target():
    book = ExecutionBook()
    scope_btc = ExecutionScope(environment="live", account_label="p1", symbol="BTCUSDT")
    scope_eth = ExecutionScope(environment="live", account_label="p1", symbol="ETHUSDT")
    key_btc = scope_btc.to_position_key()
    key_eth = scope_eth.to_position_key()

    book._ensure_book(key_btc)
    book._ensure_book(key_eth)

    candidate = book._staged_copy(key=key_btc)

    assert (
        candidate._journals[key_btc.canonical_id]
        is not book._journals[key_btc.canonical_id]
    )
    assert (
        candidate._books[key_btc.canonical_id] is not book._books[key_btc.canonical_id]
    )
    assert (
        candidate._journals[key_eth.canonical_id]
        is book._journals[key_eth.canonical_id]
    )
    assert candidate._books[key_eth.canonical_id] is book._books[key_eth.canonical_id]


@pytest.mark.asyncio
async def test_staged_copy_shares_frozen_facts_without_leaking_candidate_writes():
    """Container copies isolate a candidate while recorded facts stay shared."""
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        ExitOrderSubmissionFact,
    )

    book = ExecutionBook()
    key = _scope().to_position_key()
    journal = book._ensure_journal(key)
    published_snapshot = AccountPositionSnapshot(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side="LONG",
        position_amt=Decimal("1"),
        entry_price=Decimal("100"),
        mark_price=Decimal("101"),
        unrealized_pnl=Decimal("1"),
        notional=Decimal("101"),
        leverage=1,
        margin_type="isolated",
        observed_at=_dt(10, 0),
        raw_payload={"positionAmt": "1"},
    )
    journal.record_snapshot(published_snapshot)
    journal.record_boundary(
        ExitOrderSubmissionFact(
            order_id="exit-1",
            submitted_at=_dt(10, 1),
            symbol="BTCUSDT",
            position_side=FuturesPositionSide.LONG,
        )
    )
    published_facts = journal.read_cut()
    published_hash = published_facts.compute_facts_hash()
    published_revision = journal.revision
    published_view = book._ensure_book(key).get_view()

    candidate = book._staged_copy(key=key)
    candidate_journal = candidate._journals[key.canonical_id]
    assert candidate_journal is not journal
    assert candidate_journal._snapshots is not journal._snapshots
    assert candidate_journal._boundaries is not journal._boundaries
    assert candidate_journal._fills_by_id is not journal._fills_by_id
    # Facts themselves are shared on purpose: frozen and never mutated in place.
    assert candidate_journal._snapshots[0] is published_snapshot
    assert candidate._books[key.canonical_id]._journal is candidate_journal
    assert candidate._books[key.canonical_id] is not book._books[key.canonical_id]

    candidate_journal.record_snapshot(
        AccountPositionSnapshot(
            environment="live",
            account_label="primary",
            symbol="BTCUSDT",
            position_side="LONG",
            position_amt=Decimal("2"),
            entry_price=Decimal("100"),
            mark_price=Decimal("102"),
            unrealized_pnl=Decimal("2"),
            notional=Decimal("204"),
            leverage=1,
            margin_type="isolated",
            observed_at=_dt(10, 2),
            raw_payload={"positionAmt": "2"},
        )
    )
    candidate_journal.record_integrity_issue("candidate-only issue")

    assert len(candidate_journal.read_cut().snapshots) == 2
    assert candidate_journal.read_cut() != published_facts
    assert journal.read_cut() == published_facts
    assert journal.read_cut().compute_facts_hash() == published_hash
    assert journal.revision == published_revision
    assert book._ensure_book(key).get_view() == published_view


def test_position_book_get_view_caches_projection_until_revision_changes():
    from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
    from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
    from crypto_momentum_lab.domain.execution.position_book import PositionBook
    from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey

    key = PositionKey("live", "acc", "BTCUSDT", FuturesPositionSide.LONG)
    journal = AccountJournal(key)
    book = PositionBook(journal)

    view1 = book.get_view()
    assert len(book._view_cache) == 1

    view2 = book.get_view()
    assert view2.projection_version == view1.projection_version
    assert len(book._view_cache) == 1

    now = datetime(2026, 9, 28, tzinfo=UTC)
    snap = AccountPositionSnapshot(
        environment="live",
        account_label="acc",
        symbol="BTCUSDT",
        position_side="LONG",
        position_amt=Decimal("1.5"),
        entry_price=Decimal("50000"),
        mark_price=Decimal("50100"),
        unrealized_pnl=Decimal("150"),
        notional=Decimal("75000"),
        leverage=5,
        margin_type="CROSSED",
        observed_at=now,
        raw_payload={},
    )
    journal.record_snapshot(snap)
    assert journal.revision == 1

    view3 = book.get_view()
    assert view3.input_revision == 1
    assert view3.projection_version != view1.projection_version


def test_account_facts_compute_facts_hash_caching():
    from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        AccountFacts,
        PositionKey,
    )

    key = PositionKey("live", "acc", "BTCUSDT", FuturesPositionSide.LONG)
    facts = AccountFacts(position_key=key)
    h1 = facts.compute_facts_hash()
    assert getattr(facts, "_cached_facts_hash", None) == h1
    h2 = facts.compute_facts_hash()
    assert h1 == h2


def test_position_book_get_view_caches_advancing_future_cuts():
    from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
    from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
    from crypto_momentum_lab.domain.execution.position_book import PositionBook
    from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey

    key = PositionKey("live", "acc", "BTCUSDT", FuturesPositionSide.LONG)
    journal = AccountJournal(key)
    book = PositionBook(journal)

    cut1 = datetime(2026, 9, 28, 12, 0, 0, tzinfo=UTC)
    cut2 = datetime(2026, 9, 28, 12, 0, 15, tzinfo=UTC)
    cut3 = datetime(2026, 9, 28, 12, 0, 30, tzinfo=UTC)

    view1 = book.get_view(cut1)
    assert len(book._view_cache) == 1

    view2 = book.get_view(cut2)
    assert len(book._view_cache) == 1
    assert view2.projection_version == view1.projection_version

    view3 = book.get_view(cut3)
    assert len(book._view_cache) == 1
    assert view3.projection_version == view1.projection_version


def test_account_journal_appends_fill_with_nested_position_side():
    from crypto_momentum_lab.domain.account.models import AccountFillEvent
    from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
    from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
    from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey

    key = PositionKey("live", "primary", "牛来USDT", FuturesPositionSide.LONG)
    journal = AccountJournal(key)
    fill = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="牛来USDT",
        trade_id="trade-nested-1",
        order_id="order-1",
        side="SELL",
        price=Decimal("0.12"),
        quantity=Decimal("100"),
        realized_pnl=Decimal("1"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=datetime(2026, 9, 28, 12, 0, 0, tzinfo=UTC),
        raw_payload={"row": {"ps": "LONG"}},
    )
    rev = journal.append_fill(fill)
    assert rev == 1
    assert len(journal.read_cut().fills) == 1


@pytest.mark.asyncio
async def test_restore_durable_positions_migrates_projection_digest_when_no_reservations() -> (
    None
):
    from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
    from crypto_momentum_lab.domain.execution.ports import (
        DurableExecutionPositionState,
        ExecutionHeadSnapshot,
    )
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        AccountFacts,
        AccountFactStreamScope,
        PositionKey,
    )
    from crypto_momentum_lab.domain.execution.recovery_models import DurableJournalCut

    key = PositionKey("live", "primary", "BTCUSDT", FuturesPositionSide.LONG)
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="s1", stream_epoch="epoch-1"
    )
    facts = AccountFacts(
        position_key=key,
        stream_scope=scope,
        fills=(),
        prefix_facts_complete=True,
    )
    cut = DurableJournalCut(
        scope=scope,
        as_of=datetime.now(UTC),
        facts=facts,
        checkpoint=None,
        revision=1,
    )
    old_payload = {
        "schema_version": 1,
        "position_key": {
            "environment": key.environment,
            "account_label": key.account_label,
            "symbol": key.symbol,
            "position_side": key.position_side.value,
        },
        "stream_scope": {
            "stream_id": scope.stream_id,
            "stream_epoch": scope.stream_epoch,
        },
        "facts_hash": facts.compute_facts_hash(),
        "projection_digest": "old-stale-digest",
        "view_digest": "old-stale-view-digest",
        "journal_revision": 1,
        "active_reservation_ids": [],
    }
    head = ExecutionHeadSnapshot(
        stream_id=scope.stream_id,
        stream_epoch=scope.stream_epoch,
        revision=1,
        projection_version="pv_test",
        state_payload=old_payload,
    )
    state = DurableExecutionPositionState(
        scope=scope,
        cut=cut,
        head=head,
        trade_ids=(),
        evidence_ids=(),
        watermarks=(),
    )

    class StubUow:
        async def load_positions(self, **kwargs):
            return (state,)

    unit_of_work = StubUow()
    book = ExecutionBook(execution_unit_of_work=unit_of_work)
    # Restoration must succeed and migrate the digest instead of crashing
    await book._restore_durable_positions(
        unit_of_work=unit_of_work,
        account_label="primary",
        environment="live",
        as_of=datetime.now(UTC),
    )
    assert key.canonical_id in book._books
    assert book._head_projection_digests[key.canonical_id] != "old-stale-digest"


@pytest.mark.parametrize("has_position", [False, True])
async def test_repaired_position_reload_uses_the_real_uow_contract(
    has_position: bool,
) -> None:
    from unittest.mock import create_autospec

    from crypto_momentum_lab.domain.execution.ports import DurableExecutionPositionState
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        AccountFacts,
        AccountFactStreamScope,
        FuturesPositionSide,
        PositionKey,
    )
    from crypto_momentum_lab.domain.execution.recovery_models import DurableJournalCut
    from crypto_momentum_lab.persistence.postgres.execution_unit_of_work import (
        AsyncPostgresExecutionUnitOfWork,
    )

    uow = create_autospec(
        AsyncPostgresExecutionUnitOfWork, instance=True, spec_set=True
    )
    book = ExecutionBook(execution_unit_of_work=uow)
    key = PositionKey("live", "incident-account", "TESTUSDT", FuturesPositionSide.LONG)
    now = datetime(2026, 9, 29, 10, tzinfo=UTC)

    scope = AccountFactStreamScope.for_position_key(
        key,
        stream_id="account_event_hub",
        stream_epoch="current-epoch",
    )
    cut = DurableJournalCut(
        scope=scope,
        facts=AccountFacts(position_key=key, stream_scope=scope),
        revision=0,
        as_of=now,
    )
    state = DurableExecutionPositionState(
        scope=scope,
        cut=cut,
        head=None,
        trade_ids=(),
        evidence_ids=(),
        watermarks=(),
    )
    uow.load_position.return_value = state if has_position else None
    result = await book.reload_position(key, as_of=now)
    if has_position:
        assert result is not None
        assert result.stream_scope == scope
        assert result.total_quantity == Decimal("0")
    else:
        assert result is None
    uow.load_position.assert_awaited_once_with(key, as_of=now)
    uow.load_positions.assert_not_awaited()


def test_reconnect_selects_latest_registered_epoch_for_each_account() -> None:
    book = ExecutionBook()
    book.register_active_stream(
        environment="live",
        account_label="other-account",
        stream_id="account_event_hub",
        stream_epoch="other-epoch",
    )
    for index in range(16):
        epoch = f"epoch-{index}"
        book.register_active_stream(
            environment="live",
            account_label="incident-account",
            stream_id="account_event_hub",
            stream_epoch=epoch,
        )
        assert book.get_active_stream("live", "incident-account") == (
            "account_event_hub",
            epoch,
        ), "a reconnect must not route new observations back to an obsolete epoch"
    assert book.get_active_stream("live", "other-account") == (
        "account_event_hub",
        "other-epoch",
    )


@pytest.mark.asyncio
async def test_restore_durable_positions_migrates_facts_hash_when_no_reservations() -> (
    None
):
    from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
    from crypto_momentum_lab.domain.execution.ports import (
        DurableExecutionPositionState,
        ExecutionHeadSnapshot,
    )
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        AccountFacts,
        AccountFactStreamScope,
        PositionKey,
    )
    from crypto_momentum_lab.domain.execution.recovery_models import DurableJournalCut

    key = PositionKey("live", "primary", "ZESTUSDT", FuturesPositionSide.LONG)
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="account_event_hub", stream_epoch="epoch-1"
    )
    facts = AccountFacts(
        position_key=key,
        stream_scope=scope,
        fills=(),
        prefix_facts_complete=True,
    )
    cut = DurableJournalCut(
        scope=scope,
        as_of=datetime.now(UTC),
        facts=facts,
        checkpoint=None,
        revision=1,
    )
    stale_facts_payload = {
        "schema_version": 1,
        "position_key": {
            "environment": key.environment,
            "account_label": key.account_label,
            "symbol": key.symbol,
            "position_side": key.position_side.value,
        },
        "stream_scope": {
            "stream_id": scope.stream_id,
            "stream_epoch": scope.stream_epoch,
        },
        "facts_hash": "fabricated-or-stale-hash",
        "projection_digest": "361b284fd6b1a250a670107f715f5522acd1c6e59c3e07f30bfaa76e436b648e",
        "view_digest": "dffb40e034709dccfa27eda3ead495ca8254381e08a474f559643ffb6a99c2d4",
        "journal_revision": 1,
        "active_reservation_ids": [],
    }
    head = ExecutionHeadSnapshot(
        stream_id=scope.stream_id,
        stream_epoch=scope.stream_epoch,
        revision=1,
        projection_version="pv_test",
        state_payload=stale_facts_payload,
    )
    state = DurableExecutionPositionState(
        scope=scope,
        cut=cut,
        head=head,
        trade_ids=(),
        evidence_ids=(),
        watermarks=(),
    )

    class StubUow:
        async def load_positions(self, **kwargs):
            return (state,)

    unit_of_work = StubUow()
    book = ExecutionBook(execution_unit_of_work=unit_of_work)
    # Must succeed without raising RuntimeError("durable position facts do not match the execution head")
    await book._restore_durable_positions(
        unit_of_work=unit_of_work,
        account_label="primary",
        environment="live",
        as_of=datetime.now(UTC),
    )
    assert key.canonical_id in book._books


async def test_restore_durable_positions_heals_mismatch_even_with_active_reservations() -> (
    None
):
    from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
    from crypto_momentum_lab.domain.execution.ports import (
        DurableExecutionPositionState,
        ExecutionHeadSnapshot,
    )
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        AccountFacts,
        AccountFactStreamScope,
        PositionKey,
    )
    from crypto_momentum_lab.domain.execution.recovery_models import DurableJournalCut

    key = PositionKey(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.LONG,
    )
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="test_stream", stream_epoch="test_epoch"
    )
    facts = AccountFacts(
        position_key=key,
        stream_scope=scope,
        fills=(),
        prefix_facts_complete=True,
    )
    cut = DurableJournalCut(
        scope=scope,
        as_of=datetime.now(UTC),
        facts=facts,
        checkpoint=None,
        revision=1,
    )
    payload_with_active_res = {
        "schema_version": 1,
        "position_key": {
            "environment": key.environment,
            "account_label": key.account_label,
            "symbol": key.symbol,
            "position_side": key.position_side.value,
        },
        "stream_scope": {
            "stream_id": scope.stream_id,
            "stream_epoch": scope.stream_epoch,
        },
        "facts_hash": "diverged-hash",
        "projection_digest": "diverged-proj-digest",
        "view_digest": "diverged-view-digest",
        "recovery_checkpoint": "diverged-checkpoint",
        "journal_revision": 1,
        "active_reservation_ids": ["res-1"],
    }
    head = ExecutionHeadSnapshot(
        stream_id=scope.stream_id,
        stream_epoch=scope.stream_epoch,
        revision=1,
        projection_version="pv_test",
        state_payload=payload_with_active_res,
    )
    state = DurableExecutionPositionState(
        scope=scope,
        cut=cut,
        head=head,
        trade_ids=(),
        evidence_ids=(),
        watermarks=(),
    )

    class StubUow:
        async def load_positions(self, **kwargs):
            return (state,)

    unit_of_work = StubUow()
    book = ExecutionBook(execution_unit_of_work=unit_of_work)
    # The current durable snapshot repairs the in-memory head while reservations remain active.
    await book._restore_durable_positions(
        unit_of_work=unit_of_work,
        account_label="primary",
        environment="live",
        as_of=datetime.now(UTC),
    )
    assert key.canonical_id in book._books


@pytest.mark.parametrize("operation", ["read", "list"])
@pytest.mark.parametrize(
    ("stream_id", "stream_epoch", "reason"),
    [
        ("accounts", None, "must be supplied together"),
        (None, "epoch", "must be supplied together"),
        (" ", "epoch", "must not be empty"),
        ("accounts", " ", "must not be empty"),
    ],
)
async def test_position_reads_reject_incomplete_account_stream(
    operation, stream_id, stream_epoch, reason
) -> None:
    book = ExecutionBook()
    with pytest.raises(ValueError, match=reason):
        if operation == "read":
            await book.read(
                ExecutionScope(
                    environment="live",
                    account_label="primary",
                    symbol="BTCUSDT",
                    position_side=FuturesPositionSide.BOTH,
                ),
                stream_id=stream_id,
                stream_epoch=stream_epoch,
            )
        else:
            await book.list_position_views(
                environment="live",
                account_label="primary",
                stream_id=stream_id,
                stream_epoch=stream_epoch,
            )
    assert (
        await book.list_position_views(environment="live", account_label="primary") == ()
    )


@pytest.mark.parametrize("query", ["book", "coordinator"])
def test_active_reservation_query_preserves_order_and_scope(query) -> None:
    from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
    from crypto_momentum_lab.domain.execution.trade_command import PositionReservation

    book = ExecutionBook()
    btc = PositionKey("live", "primary", "BTCUSDT", FuturesPositionSide.LONG)
    eth = PositionKey("live", "primary", "ETHUSDT", FuturesPositionSide.LONG)
    for identity, key, created_at, released in (
        ("z", btc, _dt(10, 1), "0"),
        ("b", eth, _dt(10, 0), "0"),
        ("a", btc, _dt(10, 0), "0"),
        ("released", btc, _dt(9, 0), "1"),
    ):
        book.coordinator.register_reservation(
            PositionReservation(
                reservation_id=identity,
                command_id=f"command-{identity}",
                position_key=key,
                batch_id=f"batch-{identity}",
                reserved_quantity=Decimal("1"),
                released_quantity=Decimal(released),
                created_at=created_at,
            )
        )
    owner = book if query == "book" else book.coordinator
    assert [r.reservation_id for r in owner.get_active_reservations()] == [
        "a", "b", "z"
    ]
    assert [r.reservation_id for r in owner.get_active_reservations(btc)] == ["z", "a"]
    assert [r.reservation_id for r in owner.get_active_reservations(eth)] == ["b"]
    missing = PositionKey("live", "other", "BTCUSDT", FuturesPositionSide.LONG)
    assert owner.get_active_reservations(missing) == ()


@pytest.mark.parametrize(
    ("policy_version", "schema_version"), [("policy-custom", "v1"), ("v1", "schema-custom")]
)
def test_historical_view_preserves_configuration_and_current_state(
    policy_version, schema_version
) -> None:
    from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
    from crypto_momentum_lab.domain.execution.position_book import PositionBook
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        AccountFacts,
        AccountFactStreamScope,
        PositionKey,
    )
    from crypto_momentum_lab.domain.execution.recovery_models import DurableJournalCut

    key = PositionKey("live", "primary", "BTCUSDT", FuturesPositionSide.LONG)
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="accounts", stream_epoch="epoch-1"
    )
    cut = DurableJournalCut(
        scope=scope,
        as_of=_dt(10, 0),
        facts=AccountFacts(position_key=key, stream_scope=scope),
        revision=1,
    )
    book = PositionBook(
        AccountJournal.from_durable_cut(cut),
        policy_version=policy_version,
        schema_version=schema_version,
    )
    book.use_durable_projection_version("live-token", event_cut=cut.as_of)
    before = book.get_view(now=cut.as_of)
    historical = book.get_historical_view(cut, event_cut=cut.as_of, now=cut.as_of)
    expected = PositionBook(
        AccountJournal.from_durable_cut(cut),
        policy_version=policy_version,
        schema_version=schema_version,
    ).get_view(cut=cut.as_of, now=cut.as_of)
    default = PositionBook(AccountJournal.from_durable_cut(cut)).get_view(
        cut=cut.as_of, now=cut.as_of
    )
    assert historical == expected
    assert historical.policy_version == policy_version
    assert historical.schema_version == schema_version
    assert (historical.policy_version, historical.schema_version) != (
        default.policy_version, default.schema_version
    )
    assert historical.projection_version != "live-token"
    assert book.get_view(now=cut.as_of) == before


async def test_nonempty_historical_read_preserves_latest_position() -> None:
    from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
    from crypto_momentum_lab.domain.execution.position_book import PositionBook
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        AccountFacts,
        AccountFactStreamScope,
    )
    from crypto_momentum_lab.domain.execution.recovery_models import DurableJournalCut

    scope = _scope()
    key = scope.to_position_key()
    stream = AccountFactStreamScope.for_position_key(
        key, stream_id="accounts", stream_epoch="epoch-1"
    )
    fills = tuple(
        AccountFillEvent(
            environment="live",
            account_label="primary",
            symbol="BTCUSDT",
            trade_id=f"trade-{minute}",
            order_id=f"order-{minute}",
            side="BUY",
            price=Decimal("100"),
            quantity=Decimal("1"),
            realized_pnl=Decimal("0"),
            fee=Decimal("0"),
            fee_asset="USDT",
            trade_at=_dt(10, minute),
            raw_payload={"positionSide": "LONG"},
        )
        for minute in (0, 1)
    )
    historical_cut = DurableJournalCut(
        scope=stream,
        as_of=_dt(10, 0),
        revision=1,
        facts=AccountFacts(
            position_key=key,
            stream_scope=stream,
            fills=fills[:1],
            prefix_facts_complete=True,
        ),
    )
    latest_cut = DurableJournalCut(
        scope=stream,
        as_of=_dt(10, 1),
        revision=2,
        facts=AccountFacts(
            position_key=key,
            stream_scope=stream,
            fills=fills,
            prefix_facts_complete=True,
        ),
    )

    class Uow:
        def __init__(self):
            self.reads = []

        async def load_journal_cut(self, *, scope, as_of):
            self.reads.append((scope, as_of))
            return historical_cut

        def transaction(self, key):
            raise AssertionError("historical read must not start a write transaction")

    uow = Uow()
    book = ExecutionBook(execution_unit_of_work=uow)
    book._persistence_failed = False
    journal = AccountJournal.from_durable_cut(latest_cut)
    book._journals[key.canonical_id] = journal
    book._books[key.canonical_id] = PositionBook(journal)
    book._stream_scopes[key.canonical_id] = stream
    before = await book.read(scope, now=_dt(10, 1))
    historical = await book.read(scope, event_cut=_dt(10, 0), now=_dt(10, 1))
    after = await book.read(scope, now=_dt(10, 1))

    assert before.total_quantity == Decimal("2")
    assert historical.total_quantity == Decimal("1")
    assert historical.event_cut == _dt(10, 0)
    assert after == before
    assert uow.reads == [(stream, _dt(10, 0))]


@pytest.mark.asyncio
@pytest.mark.parametrize("same_position", [True, False])
@pytest.mark.parametrize("dispatch_state", list(DispatchState))
async def test_outbox_scope_controls_flat_stream_fast_path(same_position, dispatch_state) -> None:
    from crypto_momentum_lab.domain.account.models import AccountPositionSnapshot
    from crypto_momentum_lab.domain.execution.observation_models import (
        Applied,
    )
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        AccountFactStreamScope,
    )

    class NoTransaction:
        def transaction(self, key):
            raise AssertionError("flat snapshot stream adoption opened a transaction")

    book = ExecutionBook(execution_unit_of_work=NoTransaction())
    book._persistence_failed = False
    key = _scope().to_position_key()
    book._stream_scopes[key.canonical_id] = AccountFactStreamScope.for_position_key(
        key, stream_id="account_event_hub", stream_epoch="old-epoch"
    )

    from crypto_momentum_lab.domain.execution.command_models import OutboxEntry
    from crypto_momentum_lab.domain.execution.trade_command import TradeCommand
    from crypto_momentum_lab.domain.strategy import EntryType, StrategySide

    command_scope = _scope() if same_position else ExecutionScope(
        environment="live", account_label="other-account", symbol="BTCUSDT",
        position_side=FuturesPositionSide.LONG,
    )
    command = TradeCommand(
        command_id="pending-command", position_key=command_scope.to_position_key(),
        command_type=TradeCommandType.ENTRY, side=StrategySide.LONG,
        order_type=EntryType.MARKET, requested_quantity=Decimal("1"),
    )
    book._outbox_by_command_id[command.command_id] = OutboxEntry(
        command_id=command.command_id, request_id="request", scope=command_scope,
        command=command, state=dispatch_state,
    )

    def reject_copy(*, key):
        raise AssertionError("flat snapshot stream adoption cloned the execution book")

    book._staged_copy = reject_copy
    flat_snap = AccountPositionSnapshot(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side="LONG",
        position_amt=Decimal("0"),
        entry_price=Decimal("0"),
        mark_price=Decimal("65000"),
        unrealized_pnl=Decimal("0"),
        notional=Decimal("0"),
        leverage=None,
        margin_type=None,
        observed_at=_dt(10, 0),
        raw_payload={},
    )
    result = await book.observe(
        ExecutionEvidence(
            evidence_id="new-epoch-flat-snapshot",
            scope=_scope(),
            observed_at=_dt(10, 0),
            stream_id="account_event_hub",
            stream_epoch="new-epoch",
            sequence=1,
            snapshot=flat_snap,
        )
    )
    if same_position:
        from crypto_momentum_lab.domain.execution.observation_models import (
            WaitingForEvidence,
        )

        assert isinstance(result, WaitingForEvidence)
        assert book._stream_scopes[key.canonical_id].stream_epoch == "old-epoch"
        return
    assert isinstance(result, Applied)
    assert book._stream_scopes[key.canonical_id].stream_epoch == "new-epoch"

    # Reading with the new stream epoch must succeed cleanly without stream mismatch
    view = await book.read(
        _scope(),
        stream_id="account_event_hub",
        stream_epoch="new-epoch",
    )
    assert view.total_quantity == Decimal("0")
    assert view.stream_scope.stream_epoch == "new-epoch"


@pytest.mark.asyncio
@pytest.mark.parametrize("command_location", ["same", "other-account", "other-side"])
@pytest.mark.parametrize("dispatch_state", list(DispatchState))
async def test_outbox_scope_controls_cross_stream_read(command_location, dispatch_state) -> None:
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        AccountFactStreamScope,
    )

    class NoTransaction:
        def transaction(self, key):
            raise AssertionError("flat snapshot stream adoption opened a transaction")

    book = ExecutionBook(execution_unit_of_work=NoTransaction())
    book._persistence_failed = False
    key = _scope().to_position_key()
    book._stream_scopes[key.canonical_id] = AccountFactStreamScope.for_position_key(
        key, stream_id="account_event_hub", stream_epoch="old-epoch"
    )

    from crypto_momentum_lab.domain.execution.command_models import OutboxEntry
    from crypto_momentum_lab.domain.execution.trade_command import TradeCommand
    from crypto_momentum_lab.domain.strategy import EntryType, StrategySide

    command_scope = _scope() if command_location == "same" else ExecutionScope(
        environment="live", account_label=("other-account" if command_location == "other-account" else "primary"), symbol="BTCUSDT",
        position_side=(FuturesPositionSide.LONG if command_location == "other-account" else FuturesPositionSide.SHORT),
    )
    command = TradeCommand(
        command_id="pending-command", position_key=command_scope.to_position_key(),
        command_type=TradeCommandType.ENTRY, side=StrategySide.LONG,
        order_type=EntryType.MARKET, requested_quantity=Decimal("1"),
    )
    book._outbox_by_command_id[command.command_id] = OutboxEntry(
        command_id=command.command_id, request_id="request", scope=command_scope,
        command=command, state=dispatch_state,
    )

    book.register_active_stream(
        environment="live", account_label="primary",
        stream_id="account_event_hub", stream_epoch="new-epoch",
    )
    if command_location == "same":
        with pytest.raises(ValueError, match="does not match the restored position"):
            await book.read(
                _scope(), stream_id="account_event_hub", stream_epoch="new-epoch"
            )
        assert book._stream_scopes[key.canonical_id].stream_epoch == "old-epoch"
        assert key.canonical_id not in book._journals
    else:
        view = await book.read(
            _scope(), stream_id="account_event_hub", stream_epoch="new-epoch"
        )
        assert view.total_quantity == Decimal("0")
        assert view.stream_scope.stream_epoch == "new-epoch"
    assert book._outbox_by_command_id[command.command_id].command is command


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field", ["expected_projection_version", "external_order_id", "last_error"]
)
async def test_unparseable_active_command_blocks_restore_before_identity_reads(field):
    from unittest.mock import AsyncMock

    repository = AsyncMock()
    details = {
        "scope": {"environment": "live", "account_label": "primary",
                  "symbol": "BTCUSDT", "position_side": "LONG"},
        "side": "long", "order_type": "market", "quantity": "1",
        "reduce_only": False, "reservations": [], "request_id": "request",
        "attempt_count": 0, field: 123,
    }
    repository.load_active_execution_commands.return_value = [{
        "command_id": "unparseable-command", "client_order_id": "unparseable-command",
        "command": "entry", "status": "prepared", "requested_at": _dt(10, 0),
        "details": details,
    }]
    from copy import deepcopy

    first_valid = deepcopy(repository.load_active_execution_commands.return_value[0])
    first_valid["command_id"] = "first-valid-command"
    first_valid["client_order_id"] = "first-valid-command"
    first_valid["details"][field] = None
    first_valid["details"]["reservations"] = ["reservation-before-failure"]
    first_valid["status"] = "unknown"
    repository.load_active_execution_commands.return_value.insert(0, first_valid)
    book = ExecutionBook(command_repository=repository)
    with pytest.raises(RuntimeError, match="restore active execution commands") as error:
        await book.restore(account_label="primary")
    assert field in str(error.value.__cause__)
    assert "unparseable-command" in str(error.value.__cause__)
    assert book._persistence_failed is True
    assert not book._outbox_by_command_id
    assert not book._command_reservations
    assert not book._dispatch_reconciliation_required_commands
    repository.load_seen_event_ids.assert_not_awaited()
    repository.load_seen_fill_trade_ids.assert_not_awaited()
    repository.load_execution_order_watermarks.assert_not_awaited()

    request = ExecutionRequest(
        request_id="blocked-during-restore", scope=_scope(),
        strategy_name="trend", strategy_version="1", run_id="run",
        decision_ref="decision", expected_view_token="unused",
        action=TradeCommandType.ENTRY, requested_quantity=Decimal("1"),
    )
    result = await book.act(request)
    assert isinstance(result, Blocked)
    assert "restore is required" in result.reason
    repository.upsert_execution_command.assert_not_awaited()

    # Correct the durable record and retry on the same Book instance.
    details[field] = None
    repository.load_seen_event_ids.return_value = ()
    repository.load_seen_fill_trade_ids.return_value = ()
    repository.load_execution_order_watermarks.return_value = ()
    await book.restore(account_label="primary")
    assert book._persistence_failed is False
    entry = book.get_outbox("unparseable-command")
    assert entry is not None
    assert entry.state is DispatchState.PREPARED
    assert entry.scope == _scope()
    assert entry.command.requested_quantity == Decimal("1")
    first = book.get_outbox("first-valid-command")
    assert first is not None and first.state is DispatchState.UNKNOWN
    assert book._command_reservations[first.command_id] == ["reservation-before-failure"]
    assert book._dispatch_reconciliation_required_commands == {first.command_id}
    repository.load_seen_event_ids.assert_awaited_once()
    repository.load_seen_fill_trade_ids.assert_awaited_once()
    repository.load_execution_order_watermarks.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("conflicting", [False, True])
@pytest.mark.parametrize("status", ["prepared", "dispatching"])
async def test_restore_deduplicates_equal_command_rows_and_rejects_conflicts(
    conflicting, status
):
    from copy import deepcopy
    from unittest.mock import AsyncMock

    repository = AsyncMock()
    row = {
        "command_id": "duplicate", "client_order_id": "duplicate",
        "command": "entry", "status": status, "requested_at": _dt(10, 0),
        "details": {
            "scope": {"environment": "live", "account_label": "primary",
                      "symbol": "BTCUSDT", "position_side": "LONG"},
            "side": "long", "order_type": "market", "quantity": "1",
            "reduce_only": False, "reservations": [], "request_id": "request",
            "attempt_count": 0,
        },
    }
    duplicate = deepcopy(row)
    if conflicting:
        duplicate["details"]["quantity"] = "2"
    repository.load_active_execution_commands.return_value = [row, duplicate]
    repository.load_seen_event_ids.return_value = ()
    repository.load_seen_fill_trade_ids.return_value = ()
    repository.load_execution_order_watermarks.return_value = ()
    book = ExecutionBook(command_repository=repository)
    if conflicting:
        with pytest.raises(RuntimeError, match="restore active execution commands") as error:
            await book.restore(account_label="primary")
        assert "conflicting rows" in str(error.value.__cause__)
        assert book._persistence_failed is True
        assert not book._outbox_by_command_id
        repository.upsert_execution_command.assert_not_awaited()
        repository.load_seen_event_ids.assert_not_awaited()
    else:
        await book.restore(account_label="primary")
        assert book._persistence_failed is False
        assert len(book._outbox_by_command_id) == 1
        entry = book.get_outbox("duplicate")
        assert entry.command.requested_quantity == Decimal("1")
        if status == "dispatching":
            assert entry.state is DispatchState.UNKNOWN
            repository.upsert_execution_command.assert_awaited_once()
        else:
            assert entry.state is DispatchState.PREPARED
            repository.upsert_execution_command.assert_not_awaited()
