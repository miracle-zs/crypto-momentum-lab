from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution import (
    Accepted,
    AlreadyAccepted,
    Applied,
    Blocked,
    CommandConflict,
    Duplicate,
    ExecutionBook,
    ExecutionEvidence,
    ExecutionRequest,
    ExecutionScope,
    FactCoverageInterval,
    FactCoverageStatus,
    FuturesPositionSide,
    PositionHealthStatus,
    StaleView,
    TradeCommandType,
)


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
