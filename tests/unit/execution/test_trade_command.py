"""Unit tests for TradeCommand and exit allocation planning."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    PositionEpisode,
    PositionKey,
    PositionLedgerBatch,
    PositionView,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    ExitAllocation,
    ExitAllocationPlan,
    ExitPolicyMode,
    PositionReservation,
    TradeCommand,
    TradeCommandType,
    plan_exit_allocations,
)
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide

NOW = datetime(2026, 9, 20, 10, 0, 0, tzinfo=UTC)
POS_KEY_LONG = PositionKey(
    environment="production",
    account_label="binance-prod",
    symbol="BTCUSDT",
    position_side=FuturesPositionSide.LONG,
)
POS_KEY_SHORT = PositionKey(
    environment="production",
    account_label="binance-prod",
    symbol="BTCUSDT",
    position_side=FuturesPositionSide.SHORT,
)


def _make_view(
    batches: tuple[PositionLedgerBatch, ...],
    key: PositionKey = POS_KEY_LONG,
) -> PositionView:
    side = (
        StrategySide.SHORT
        if key.position_side == FuturesPositionSide.SHORT
        else StrategySide.LONG
    )
    episode = (
        PositionEpisode(
            position_key=key,
            episode_id="ep-1",
            side=side,
            opened_at=NOW,
            batches=batches,
        )
        if batches
        else None
    )
    return PositionView(
        key=key,
        projection_version="pv_test",
        input_revision=1,
        event_cut=NOW,
        policy_version="1",
        schema_version="1",
        coverage=None,
        active_episode=episode,
        batches=tuple(b for b in batches if b.quantity > 0),
        unallocated_quantity=Decimal("0"),
        reconciliation_gap=Decimal("0"),
    )


def test_trade_command_and_allocation_plan_invariants() -> None:
    """TradeCommand enforces exact quantity conservation with its ExitAllocationPlan."""
    plan = ExitAllocationPlan(
        position_key=POS_KEY_LONG,
        allocations=(
            ExitAllocation(
                batch_id="batch-1",
                allocated_quantity=Decimal("0.5"),
                entry_price=Decimal("50000"),
            ),
        ),
        total_allocated_quantity=Decimal("0.5"),
        policy=ExitPolicyMode.TARGET_BATCHES_ONLY,
    )

    cmd = TradeCommand(
        command_id="cmd-1",
        position_key=POS_KEY_LONG,
        command_type=TradeCommandType.EXIT,
        side=StrategySide.LONG,
        order_type=EntryType.MARKET,
        requested_quantity=Decimal("0.5"),
        reduce_only=True,
        allocation_plan=plan,
    )
    assert cmd.requested_quantity == Decimal("0.5")

    # Mismatched requested_quantity raises ValueError
    with pytest.raises(ValueError, match="requested_quantity 0.6 must strictly match"):
        TradeCommand(
            command_id="cmd-2",
            position_key=POS_KEY_LONG,
            command_type=TradeCommandType.EXIT,
            side=StrategySide.LONG,
            order_type=EntryType.MARKET,
            requested_quantity=Decimal("0.6"),
            reduce_only=True,
            allocation_plan=plan,
        )

    # Mismatched position_key raises ValueError
    with pytest.raises(ValueError, match="allocation_plan position_key does not match"):
        TradeCommand(
            command_id="cmd-3",
            position_key=POS_KEY_SHORT,
            command_type=TradeCommandType.EXIT,
            side=StrategySide.SHORT,
            order_type=EntryType.MARKET,
            requested_quantity=Decimal("0.5"),
            reduce_only=True,
            allocation_plan=plan,
        )


@pytest.mark.parametrize(
    ("policy", "requested_quantity"),
    (
        (ExitPolicyMode.FULL_POSITION_CLOSE, None),
        (ExitPolicyMode.FULL_POSITION_CLOSE, Decimal("0.0001")),
        (ExitPolicyMode.TARGET_BATCHES_ONLY, None),
    ),
)
def test_exit_allocator_allocates_all_available_batches(
    policy, requested_quantity
) -> None:
    """Full close or unspecified quantity consumes the available batch capacity."""
    b1 = PositionLedgerBatch(
        batch_id="b1",
        episode_id="ep-1",
        quantity=Decimal("0.0004"),
        original_quantity=Decimal("0.0004"),
        entry_price=Decimal("60000"),
        opened_at=NOW,
    )
    b2 = PositionLedgerBatch(
        batch_id="b2",
        episode_id="ep-1",
        quantity=Decimal("0.0003"),
        original_quantity=Decimal("0.0003"),
        entry_price=Decimal("61000"),
        opened_at=NOW,
    )
    proj = _make_view((b1, b2))

    plan = plan_exit_allocations(
        proj, policy=policy, requested_quantity=requested_quantity
    )
    assert plan.total_allocated_quantity == Decimal("0.0007")
    assert len(plan.allocations) == 2
    assert plan.allocations[0].batch_id == "b1"
    assert plan.allocations[0].allocated_quantity == Decimal("0.0004")
    assert plan.allocations[1].batch_id == "b2"
    assert plan.allocations[1].allocated_quantity == Decimal("0.0003")
    assert plan.unallocated_remainder == Decimal("0")


def test_exit_allocator_target_batches_only_fifo() -> None:
    """TARGET_BATCHES_ONLY allocates FIFO across target batches up to requested quantity."""
    b1 = PositionLedgerBatch(
        batch_id="b1",
        episode_id="ep-1",
        quantity=Decimal("0.0004"),
        original_quantity=Decimal("0.0004"),
        entry_price=Decimal("60000"),
        opened_at=NOW,
    )
    b2 = PositionLedgerBatch(
        batch_id="b2",
        episode_id="ep-1",
        quantity=Decimal("0.0003"),
        original_quantity=Decimal("0.0003"),
        entry_price=Decimal("61000"),
        opened_at=NOW,
    )
    proj = _make_view((b1, b2))

    # Partial exit targeting b1 only
    plan1 = plan_exit_allocations(
        proj,
        target_batch_ids=("b1",),
        requested_quantity=Decimal("0.0002"),
        policy=ExitPolicyMode.TARGET_BATCHES_ONLY,
    )
    assert plan1.total_allocated_quantity == Decimal("0.0002")
    assert len(plan1.allocations) == 1
    assert plan1.allocations[0].batch_id == "b1"
    assert plan1.allocations[0].allocated_quantity == Decimal("0.0002")

    # Partial exit targeting b1 with requested > b1 quantity
    plan2 = plan_exit_allocations(
        proj,
        target_batch_ids=("b1",),
        requested_quantity=Decimal("0.0005"),
        policy=ExitPolicyMode.TARGET_BATCHES_ONLY,
    )
    # b1 only has 0.0004, so it allocates 0.0004 and records 0.0001 unallocated remainder
    assert plan2.total_allocated_quantity == Decimal("0.0004")
    assert plan2.unallocated_remainder == Decimal("0.0001")


def test_exit_allocator_respects_active_reservations() -> None:
    """Exit allocation must deduct active reservations before planning lot allocation."""
    b1 = PositionLedgerBatch(
        batch_id="b1",
        episode_id="ep-1",
        quantity=Decimal("1.0"),
        original_quantity=Decimal("1.0"),
        entry_price=Decimal("60000"),
        opened_at=NOW,
    )
    b2 = PositionLedgerBatch(
        batch_id="b2",
        episode_id="ep-1",
        quantity=Decimal("2.0"),
        original_quantity=Decimal("2.0"),
        entry_price=Decimal("61000"),
        opened_at=NOW,
    )
    episode = PositionEpisode(
        position_key=POS_KEY_LONG,
        episode_id="ep-1",
        side=StrategySide.LONG,
        opened_at=NOW,
        batches=(b1, b2),
    )
    proj = PositionView(
        key=POS_KEY_LONG,
        projection_version="pv_test",
        input_revision=1,
        event_cut=NOW,
        policy_version="1",
        schema_version="1",
        coverage=None,
        active_episode=episode,
        batches=(b1, b2),
        unallocated_quantity=Decimal("0"),
        reconciliation_gap=Decimal("0"),
    )

    # Reservation on b1 for 0.8 units (leaving 0.2 units available on b1)
    res_b1 = PositionReservation(
        reservation_id="res-1",
        command_id="cmd-prior-exit",
        position_key=POS_KEY_LONG,
        batch_id="b1",
        reserved_quantity=Decimal("0.8"),
    )

    # Request 1.0 unit. Should allocate 0.2 from b1 and 0.8 from b2!
    plan = plan_exit_allocations(
        proj,
        requested_quantity=Decimal("1.0"),
        active_reservations=(res_b1,),
    )
    assert plan.total_allocated_quantity == Decimal("1.0")
    assert len(plan.allocations) == 2
    assert plan.allocations[0].batch_id == "b1"
    assert plan.allocations[0].allocated_quantity == Decimal("0.2")
    assert plan.allocations[1].batch_id == "b2"
    assert plan.allocations[1].allocated_quantity == Decimal("0.8")
