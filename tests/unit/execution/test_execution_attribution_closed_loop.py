"""Unit tests for Phase P2 Attribution and Execution Closed Loop per architecture RFC 2026-09-25.

Validates:
1. Targeted lot attribution (directed exit consumes target batch, not FIFO first batch);
2. Transactional batch reservations preventing concurrent over-exit (anti-double-dipping);
3. CAS projection version pinning and conflict detection;
4. Fill reconciliation and unconsumed reservation releases;
5. Execution readiness guard on degraded views.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.account_journal import (
    AccountJournal,
)
from crypto_momentum_lab.domain.execution.execution_coordinator import (
    ExecutionCoordinator,
    ExecutionReadinessError,
    ReservationConflictError,
    VersionConflictError,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_book import PositionBook
from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFacts,
    ExitOrderSubmissionFact,
    PositionHealthStatus,
    PositionKey,
    PositionLedgerBatch,
    PositionView,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    ExitAllocation,
    ExitAllocationPlan,
    ExitAllocator,
    ExitPolicyMode,
    PositionReservation,
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide


def _dt(hour: int, minute: int) -> datetime:
    return datetime(2026, 9, 25, hour, minute, 0, tzinfo=UTC)


def _setup_two_batch_book() -> tuple[PositionBook, PositionKey]:
    key = PositionKey("live", "primary", "BTCUSDT", FuturesPositionSide.LONG)
    journal = AccountJournal(key)
    t1 = _dt(10, 0)
    t_exit = _dt(10, 15)
    t2 = _dt(10, 30)

    # First lot: BUY 1.0 @ 50,000
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
        trade_at=t1,
        raw_payload={"positionSide": "LONG", "is_system": True},
    )
    # Exit order submitted on first lot at 10:15 (defining batch boundary)
    boundary = ExitOrderSubmissionFact(
        order_id="exit_ord_1",
        submitted_at=t_exit,
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.LONG,
    )
    # Second lot: BUY 2.0 @ 51,000
    f2 = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id="t2",
        order_id="ord_2",
        side="BUY",
        price=Decimal("51000"),
        quantity=Decimal("2.0"),
        realized_pnl=Decimal("0"),
        fee=Decimal("2"),
        fee_asset="USDT",
        trade_at=t2,
        raw_payload={"positionSide": "LONG", "is_system": True},
    )
    snap = AccountPositionSnapshot(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side="LONG",
        position_amt=Decimal("3.0"),
        entry_price=Decimal("50666.67"),
        mark_price=Decimal("51000"),
        unrealized_pnl=Decimal("0"),
        notional=Decimal("153000"),
        leverage=5,
        margin_type="cross",
        observed_at=t2,
        raw_payload={},
    )
    journal.append_fill(f1)
    journal.record_boundary(boundary)
    journal.append_fill(f2)
    journal.record_snapshot(snap)

    book = PositionBook(journal)
    return book, key


def test_targeted_exit_allocates_exact_batch_not_fifo_first() -> None:
    book, key = _setup_two_batch_book()
    view = book.get_view()

    assert len(view.batches) == 2
    batch_1, batch_2 = view.batches

    # Explicitly target batch 2 (quantity 2.0)
    cmd = ExitAllocator.create_exit_command(
        view,
        target_batch_ids=(batch_2.batch_id,),
        requested_quantity=Decimal("1.5"),
        policy=ExitPolicyMode.TARGET_BATCHES_ONLY,
    )

    assert cmd is not None
    assert cmd.command_type == TradeCommandType.EXIT
    assert cmd.requested_quantity == Decimal("1.5")
    assert cmd.allocation_plan is not None
    assert len(cmd.allocation_plan.allocations) == 1
    # Strictly allocated to batch_2, batch_1 untouched!
    alloc = cmd.allocation_plan.allocations[0]
    assert alloc.batch_id == batch_2.batch_id
    assert alloc.allocated_quantity == Decimal("1.5")
    assert cmd.expected_projection_version == view.projection_version


def test_transactional_reservation_prevents_concurrent_double_dipping() -> None:
    book, key = _setup_two_batch_book()
    view = book.get_view()
    coordinator = ExecutionCoordinator()

    batch_1, batch_2 = view.batches
    assert batch_1.quantity == Decimal("1.0")

    # Command A: reserve 0.8 of batch_1
    cmd_a = ExitAllocator.create_exit_command(
        view,
        command_id="cmd_a",
        target_batch_ids=(batch_1.batch_id,),
        requested_quantity=Decimal("0.8"),
    )
    assert cmd_a is not None
    res_a = coordinator.reserve_exit(cmd_a, view)
    assert len(res_a) == 1
    assert res_a[0].reserved_quantity == Decimal("0.8")
    assert res_a[0].active_quantity == Decimal("0.8")

    # Available remaining on batch_1 is now 1.0 - 0.8 = 0.2
    assert coordinator.get_available_batch_quantity(view, batch_1.batch_id) == Decimal("0.2")

    # Command B: concurrent attempt to reserve 0.5 of batch_1 must fail!
    cmd_b = ExitAllocator.create_exit_command(
        view,
        command_id="cmd_b",
        target_batch_ids=(batch_1.batch_id,),
        requested_quantity=Decimal("0.5"),
    )
    assert cmd_b is not None
    with pytest.raises(ReservationConflictError) as exc_info:
        coordinator.reserve_exit(cmd_b, view)
    assert "insufficient available quantity" in str(exc_info.value)


def test_cas_version_fencing_rejects_stale_command() -> None:
    book, key = _setup_two_batch_book()
    view = book.get_view()
    coordinator = ExecutionCoordinator()

    cmd = ExitAllocator.create_exit_command(
        view,
        target_batch_ids=(view.batches[0].batch_id,),
        requested_quantity=Decimal("0.5"),
    )
    assert cmd is not None

    # Construct a new view with newer projection version
    newer_view = book.get_view()
    assert newer_view.projection_version != view.projection_version

    # Attempting to reserve with stale expected_projection_version fails
    with pytest.raises(VersionConflictError) as exc_info:
        coordinator.reserve_exit(cmd, newer_view)
    assert "CAS version mismatch" in str(exc_info.value)


def test_reservation_fill_reconciliation_and_release() -> None:
    book, key = _setup_two_batch_book()
    view = book.get_view()
    coordinator = ExecutionCoordinator()

    batch_1 = view.batches[0]
    cmd = ExitAllocator.create_exit_command(
        view,
        command_id="cmd_order_123",
        target_batch_ids=(batch_1.batch_id,),
        requested_quantity=Decimal("1.0"),
    )
    assert cmd is not None
    res = coordinator.reserve_exit(cmd, view)[0]

    # 1. Partial fill 0.6 arrives
    res = coordinator.reconcile_fill(res.reservation_id, Decimal("0.6"))
    assert res.consumed_quantity == Decimal("0.6")
    assert res.active_quantity == Decimal("0.4")

    # 2. Order cancelled, release remaining 0.4
    res = coordinator.release_reservation(res.reservation_id)
    assert res.released_quantity == Decimal("0.4")
    assert res.active_quantity == Decimal("0.0")

    # 3. Available quantity restored to 1.0 (no active reservations remaining)
    assert coordinator.get_available_batch_quantity(view, batch_1.batch_id) == Decimal("1.0")


def test_execution_readiness_error_on_degraded_view() -> None:
    book, key = _setup_two_batch_book()
    degraded_view = PositionView(
        key=key,
        projection_version="pv_test",
        input_revision=1,
        event_cut=datetime.now(UTC),
        policy_version="v1",
        schema_version="v1",
        coverage=None,
        active_episode=None,
        batches=(),
        unallocated_quantity=Decimal("0"),
        health_status=PositionHealthStatus.INCOMPLETE,
    )
    coordinator = ExecutionCoordinator()
    cmd = TradeCommand(
        command_id="cmd_fail",
        position_key=key,
        command_type=TradeCommandType.EXIT,
        side=StrategySide.SHORT,
        order_type=EntryType.MARKET,
        requested_quantity=Decimal("1.0"),
    )

    with pytest.raises(ExecutionReadinessError) as exc_info:
        coordinator.reserve_exit(cmd, degraded_view)
    assert "not ready for trade" in str(exc_info.value)
