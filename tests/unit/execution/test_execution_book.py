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
    DispatchState,
    Duplicate,
    ExchangeOrderEvent,
    ExchangeOrderState,
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
    dispatching = book.mark_dispatching(cmd_id)
    assert dispatching.state == DispatchState.DISPATCHING
    assert dispatching.attempt_count == 1

    # 3. mark_unknown preserves active reservations!
    unknown = book.mark_unknown(cmd_id, reason="Gateway timeout 504")
    assert unknown.state == DispatchState.UNKNOWN
    assert unknown.last_error == "Gateway timeout 504"
    active_res = book._coordinator.get_active_reservations(key)
    assert len(active_res) == 1
    assert active_res[0].active_quantity == Decimal("2.0")

    # 4. mark_acknowledged transitions to ACKNOWLEDGED
    acked = book.mark_acknowledged(cmd_id, external_order_id="binance_ord_999")
    assert acked.state == DispatchState.ACKNOWLEDGED
    assert acked.external_order_id == "binance_ord_999"

    # 5. mark_terminal releases active reservations
    terminal = book.mark_terminal(cmd_id, reason="Fully settled")
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
        ExecutionEvidence(
            evidence_id="ev_c1", scope=scope, observed_at=t0, fill=f_cum1
        )
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
        ExecutionEvidence(
            evidence_id="ev_c2", scope=scope, observed_at=t0, fill=f_cum2
        )
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
        ExecutionEvidence(
            evidence_id="ev_c3", scope=scope, observed_at=t0, fill=f_cum3
        )
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

