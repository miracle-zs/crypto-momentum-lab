from crypto_momentum_lab.domain.strategy import StrategySide

"""Unit tests for Phase P2 Attribution and Execution Closed Loop per architecture RFC 2026-09-25.

Validates:
1. Targeted lot attribution (directed exit consumes target batch, not FIFO first batch);
2. Transactional batch reservations preventing concurrent over-exit (anti-double-dipping);
3. CAS projection version pinning and conflict detection;
4. Fill reconciliation and unconsumed reservation releases;
5. Known batch exits remain available on degraded views.
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.account_journal import (
    AccountJournal,
)
from crypto_momentum_lab.domain.execution.reservation_registry import (
    ReservationRegistry,
    ReservationConflictError,
    VersionConflictError,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_book import PositionBook
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    ExitOrderSubmissionFact,
    FactCoverageInterval,
    FactCoverageStatus,
    PositionHealthStatus,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    ExitPolicyMode,
    TradeCommand,
    TradeCommandType,
    plan_exit_allocations,
)
from crypto_momentum_lab.domain.strategy import EntryType


def _dt(hour: int, minute: int) -> datetime:
    return datetime(2026, 9, 25, hour, minute, 0, tzinfo=UTC)


def _exit_command(
    view,
    *,
    command_id: str,
    target_batch_ids: tuple[str, ...] | None = None,
    requested_quantity: Decimal | None = None,
    policy: ExitPolicyMode = ExitPolicyMode.TARGET_BATCHES_ONLY,
) -> TradeCommand:
    allocation = plan_exit_allocations(
        view,
        target_batch_ids=target_batch_ids,
        requested_quantity=requested_quantity,
        policy=policy,
    )
    assert view.active_episode is not None
    return TradeCommand(
        command_id=command_id,
        position_key=view.key,
        command_type=TradeCommandType.EXIT,
        side=view.active_episode.side,
        order_type=EntryType.MARKET,
        requested_quantity=allocation.total_allocated_quantity,
        reduce_only=True,
        allocation_plan=allocation,
        expected_projection_version=view.projection_version,
        created_at=_dt(10, 30),
    )


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
    journal.set_coverage(
        FactCoverageInterval(
            start_at=t1,
            end_at=t2,
            status=FactCoverageStatus.CONFIRMED,
        )
    )

    book = PositionBook(journal)
    return book, key


def test_targeted_exit_allocates_exact_batch_not_fifo_first() -> None:
    book, key = _setup_two_batch_book()
    view = book.get_view()

    assert len(view.batches) == 2
    batch_1, batch_2 = view.batches

    # Explicitly target batch 2 (quantity 2.0)
    cmd = _exit_command(
        view,
        command_id="targeted-exit",
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
    coordinator = ReservationRegistry()

    batch_1, batch_2 = view.batches
    assert batch_1.quantity == Decimal("1.0")

    # Command A: reserve 0.8 of batch_1
    cmd_a = _exit_command(
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
    assert coordinator.get_available_batch_quantity(view, batch_1.batch_id) == Decimal(
        "0.2"
    )

    # Command B: concurrent attempt to reserve 0.5 of batch_1 must fail!
    cmd_b = _exit_command(
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
    coordinator = ReservationRegistry()

    cmd = _exit_command(
        view,
        command_id="stale-projection-exit",
        target_batch_ids=(view.batches[0].batch_id,),
        requested_quantity=Decimal("0.5"),
    )
    assert cmd is not None

    # Construct a new view with newer projection version by appending a new fact
    t_new = _dt(10, 45)
    f3 = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id="t3",
        order_id="ord_3",
        side="BUY",
        price=Decimal("52000"),
        quantity=Decimal("0.1"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0.1"),
        fee_asset="USDT",
        trade_at=t_new,
        raw_payload={"positionSide": "LONG", "is_system": True},
    )
    book._journal.append_fill(f3)
    snap_new = AccountPositionSnapshot(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side="LONG",
        position_amt=Decimal("3.1"),
        entry_price=Decimal("50709.68"),
        mark_price=Decimal("52000"),
        unrealized_pnl=Decimal("0"),
        notional=Decimal("161200"),
        leverage=5,
        margin_type="cross",
        observed_at=t_new,
        raw_payload={},
    )
    book._journal.record_snapshot(snap_new)
    book._journal.set_coverage(
        FactCoverageInterval(
            start_at=_dt(10, 0),
            end_at=t_new,
            status=FactCoverageStatus.CONFIRMED,
        )
    )
    newer_view = book.get_view(now=t_new)
    assert newer_view.projection_version != view.projection_version

    # Attempting to reserve with stale expected_projection_version fails
    with pytest.raises(VersionConflictError) as exc_info:
        coordinator.reserve_exit(cmd, newer_view)
    assert "CAS version mismatch" in str(exc_info.value)


def test_reservation_fill_reconciliation_and_release() -> None:
    book, key = _setup_two_batch_book()
    view = book.get_view()
    coordinator = ReservationRegistry()

    batch_1 = view.batches[0]
    cmd = _exit_command(
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
    assert coordinator.get_available_batch_quantity(view, batch_1.batch_id) == Decimal(
        "1.0"
    )


def test_known_batch_exit_can_reserve_with_degraded_view() -> None:
    from dataclasses import replace

    book, key = _setup_two_batch_book()
    view = replace(book.get_view(), health_status=PositionHealthStatus.INCOMPLETE)
    batch = view.batches[0]
    allocation = plan_exit_allocations(
        view,
        target_batch_ids=(batch.batch_id,),
        requested_quantity=Decimal("1"),
        policy=ExitPolicyMode.TARGET_BATCHES_ONLY,
        reason="timeout",
    )
    command = TradeCommand(
        command_id="exit-degraded",
        position_key=key,
        command_type=TradeCommandType.EXIT,
        side=StrategySide.LONG,
        order_type=EntryType.MARKET,
        requested_quantity=Decimal("1"),
        reduce_only=True,
        allocation_plan=allocation,
        expected_projection_version=view.projection_version,
    )
    reservations = ReservationRegistry().reserve_exit(command, view)
    assert len(reservations) == 1
    assert reservations[0].batch_id == batch.batch_id
    assert reservations[0].reserved_quantity == Decimal("1")


def test_multi_batch_reservation_failure_publishes_no_partial_state():
    from crypto_momentum_lab.domain.execution.reservation_registry import (
        InMemoryPositionReservationRepository,
    )

    class FailingRepository(InMemoryPositionReservationRepository):
        def save_reservations(self, reservations, **kwargs):
            raise OSError("reservation commit failed")

    position_book, key = _setup_two_batch_book()
    view = position_book.get_view()
    repository = FailingRepository()
    coordinator = ReservationRegistry(repository=repository)
    command = _exit_command(
        view,
        command_id="two-batch-exit",
        policy=ExitPolicyMode.FULL_POSITION_CLOSE,
    )
    assert command is not None
    assert len(command.allocation_plan.allocations) == 2
    with pytest.raises(OSError, match="reservation commit failed"):
        coordinator.reserve_exit(command, view)
    assert coordinator.get_active_reservations(key) == ()
    assert repository.load_active_reservations(key) == ()


async def test_execution_book_preserves_planned_multi_batch_split(monkeypatch) -> None:
    from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
    from crypto_momentum_lab.domain.execution.execution_book import (
        Accepted,
        ExecutionBook,
        ExecutionRequest,
    )

    position_book, key = _setup_two_batch_book()
    view = position_book.get_view()
    first, second = view.batches
    book = ExecutionBook()
    monkeypatch.setattr(book, "_ensure_book", lambda _key: position_book)
    request = ExecutionRequest(
        request_id="planned-split",
        scope=ExecutionScope(
            key.environment, key.account_label, key.symbol, key.position_side
        ),
        strategy_name="strategy",
        run_id="run-1",
        decision_ref="decision-1",
        expected_view_token=view.projection_version,
        action=TradeCommandType.EXIT,
        requested_quantity=Decimal("1"),
        target_batch_ids=(first.batch_id, second.batch_id),
        batch_quantities={
            first.batch_id: Decimal("0.2"),
            second.batch_id: Decimal("0.8"),
        },
        exit_policy_mode=ExitPolicyMode.TARGET_BATCHES_ONLY,
        created_at=_dt(10, 30),
        side=StrategySide.LONG,
    )
    result = await book.act(request)
    assert isinstance(result, Accepted)
    assert {
        r.batch_id: r.reserved_quantity for r in result.receipt.reservations
    } == request.batch_quantities
    assert {
        a.batch_id: a.allocated_quantity
        for a in result.receipt.command.allocation_plan.allocations
    } == request.batch_quantities
