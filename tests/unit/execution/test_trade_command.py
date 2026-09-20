"""Unit tests for TradeCommand and ExitAllocator domain services."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    PositionEpisode,
    PositionKey,
    PositionLedgerBatch,
    PositionLedgerProjection,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    ExitAllocation,
    ExitAllocationPlan,
    ExitAllocator,
    ExitPolicyMode,
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide

NOW = datetime(2026, 9, 20, 10, 0, 0, tzinfo=timezone.utc)
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


def _make_projection(
    batches: tuple[PositionLedgerBatch, ...],
    key: PositionKey = POS_KEY_LONG,
) -> PositionLedgerProjection:
    total_qty = sum((b.quantity for b in batches), start=Decimal("0"))
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
    return PositionLedgerProjection(
        position_key=key,
        active_episode=episode,
        active_batches=tuple(b for b in batches if b.quantity > 0),
        total_active_quantity=total_qty,
        unallocated_quantity=Decimal("0"),
        reconciliation_gap=Decimal("0"),
        high_watermark_trade_at=NOW,
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
    with pytest.raises(
        ValueError, match="requested_quantity 0.6 must strictly match"
    ):
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
    with pytest.raises(
        ValueError, match="allocation_plan position_key does not match"
    ):
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


def test_exit_allocator_full_position_close() -> None:
    """FULL_POSITION_CLOSE allocates 100% of all open batches."""
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
    proj = _make_projection((b1, b2))

    plan = ExitAllocator.plan_exit(proj, policy=ExitPolicyMode.FULL_POSITION_CLOSE)
    assert plan.total_allocated_quantity == Decimal("0.0007")
    assert len(plan.allocations) == 2
    assert plan.allocations[0].batch_id == "b1"
    assert plan.allocations[0].allocated_quantity == Decimal("0.0004")
    assert plan.allocations[1].batch_id == "b2"
    assert plan.allocations[1].allocated_quantity == Decimal("0.0003")
    assert plan.absorbed_dust == Decimal("0")
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
    proj = _make_projection((b1, b2))

    # Partial exit targeting b1 only
    plan1 = ExitAllocator.plan_exit(
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
    plan2 = ExitAllocator.plan_exit(
        proj,
        target_batch_ids=("b1",),
        requested_quantity=Decimal("0.0005"),
        policy=ExitPolicyMode.TARGET_BATCHES_ONLY,
    )
    # b1 only has 0.0004, so it allocates 0.0004 and records 0.0001 unallocated remainder
    assert plan2.total_allocated_quantity == Decimal("0.0004")
    assert plan2.unallocated_remainder == Decimal("0.0001")


def test_exit_allocator_absorb_dust_single_batch() -> None:
    """Decision layer explicitly absorbs dust when single batch leaves remainder below min_notional."""
    # Single batch of 0.0007 BTC.
    # At price $10,000, notional is $7.00.
    # Strategy requests to close 0.0004 BTC ($4.00).
    # Remainder is 0.0003 BTC ($3.00 < min_notional $5.00).
    b1 = PositionLedgerBatch(
        batch_id="b1",
        episode_id="ep-1",
        quantity=Decimal("0.0007"),
        original_quantity=Decimal("0.0007"),
        entry_price=Decimal("10000"),
        opened_at=NOW,
    )
    proj = _make_projection((b1,))

    plan = ExitAllocator.plan_exit(
        proj,
        requested_quantity=Decimal("0.0004"),
        policy=ExitPolicyMode.ABSORB_DUST_SINGLE_BATCH,
        reference_price=Decimal("10000"),
        min_notional=Decimal("5"),
        reason="test_dust_close",
    )
    # Decision layer deliberately plans the full 0.0007 close and records absorbed_dust
    assert plan.total_allocated_quantity == Decimal("0.0007")
    assert plan.absorbed_dust == Decimal("0.0003")
    assert len(plan.allocations) == 1
    assert plan.allocations[0].allocated_quantity == Decimal("0.0007")


def test_exit_allocator_never_absorbs_dust_when_multiple_batches_exist() -> None:
    """Multi-batch positions must NEVER absorb dust across lots, preserving lot boundaries."""
    b1 = PositionLedgerBatch(
        batch_id="b1",
        episode_id="ep-1",
        quantity=Decimal("0.0004"),
        original_quantity=Decimal("0.0004"),
        entry_price=Decimal("10000"),
        opened_at=NOW,
    )
    b2 = PositionLedgerBatch(
        batch_id="b2",
        episode_id="ep-1",
        quantity=Decimal("0.0003"),
        original_quantity=Decimal("0.0003"),
        entry_price=Decimal("10000"),
        opened_at=NOW,
    )
    proj = _make_projection((b1, b2))

    plan = ExitAllocator.plan_exit(
        proj,
        target_batch_ids=("b1",),
        requested_quantity=Decimal("0.0004"),
        policy=ExitPolicyMode.ABSORB_DUST_SINGLE_BATCH,
        reference_price=Decimal("10000"),
        min_notional=Decimal("5"),
    )
    # Must NOT absorb b2!
    assert plan.total_allocated_quantity == Decimal("0.0004")
    assert plan.absorbed_dust == Decimal("0")
    assert len(plan.allocations) == 1
    assert plan.allocations[0].batch_id == "b1"


def test_exit_allocator_create_exit_command() -> None:
    """ExitAllocator.create_exit_command creates a ready-to-execute TradeCommand."""
    b1 = PositionLedgerBatch(
        batch_id="b1",
        episode_id="ep-1",
        quantity=Decimal("1.5"),
        original_quantity=Decimal("1.5"),
        entry_price=Decimal("3000"),
        opened_at=NOW,
    )
    proj = _make_projection((b1,), key=POS_KEY_SHORT)

    cmd = ExitAllocator.create_exit_command(
        proj,
        requested_quantity=Decimal("1.5"),
        policy=ExitPolicyMode.TARGET_BATCHES_ONLY,
        order_type=EntryType.LIMIT,
        limit_price=Decimal("2950"),
        reason="take_profit",
        fencing_token="fence-123",
        idempotency_key="idemp-xyz",
    )
    assert cmd is not None
    assert cmd.position_key == POS_KEY_SHORT
    assert cmd.side == StrategySide.SHORT
    assert cmd.reduce_only is True
    assert cmd.requested_quantity == Decimal("1.5")
    assert cmd.limit_price == Decimal("2950")
    assert cmd.fencing_token == "fence-123"
    assert cmd.idempotency_key == "idemp-xyz"
    assert cmd.allocation_plan is not None
    assert cmd.allocation_plan.total_allocated_quantity == Decimal("1.5")


def test_exit_allocator_create_exit_command_both_mode_short() -> None:
    """In one-way BOTH mode, exiting a SHORT position generates StrategySide.SHORT command and BUY execution."""
    pos_key_both = PositionKey(
        environment="production",
        account_label="binance-prod",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )
    b1 = PositionLedgerBatch(
        batch_id="b1",
        episode_id="ep-short-1",
        quantity=Decimal("1.5"),
        original_quantity=Decimal("1.5"),
        entry_price=Decimal("3000"),
        opened_at=NOW,
    )
    episode = PositionEpisode(
        episode_id="ep-short-1",
        position_key=pos_key_both,
        side=StrategySide.SHORT,
        opened_at=NOW,
        batches=(b1,),
    )
    proj = PositionLedgerProjection(
        position_key=pos_key_both,
        active_episode=episode,
        active_batches=(b1,),
        total_active_quantity=Decimal("1.5"),
        unallocated_quantity=Decimal("0"),
        reconciliation_gap=Decimal("0"),
        high_watermark_trade_at=NOW,
    )

    cmd = ExitAllocator.create_exit_command(
        proj,
        requested_quantity=Decimal("1.5"),
        policy=ExitPolicyMode.TARGET_BATCHES_ONLY,
        order_type=EntryType.MARKET,
        reason="stop_loss",
    )
    assert cmd is not None
    assert cmd.position_key.position_side == FuturesPositionSide.BOTH
    assert cmd.side == StrategySide.SHORT
    assert cmd.reduce_only is True

    from crypto_momentum_lab.execution_account.orders.trade_command_executor import (
        TradeCommandExecutor,
    )
    from crypto_momentum_lab.execution_account.orders.quantization import SymbolTradingRules
    rules = SymbolTradingRules(
        symbol="BTCUSDT",
        tick_size=Decimal("0.10"),
        step_size=Decimal("0.1"),
        min_quantity=Decimal("0.1"),
        max_quantity=Decimal("100.0"),
        min_notional=Decimal("5.0"),
    )
    exec_plan = TradeCommandExecutor.plan_execution(
        cmd,
        rules,
        run_id="run-1",
        reference_price=Decimal("3000"),
        hedge_mode=False,
    )
    assert exec_plan.plan is not None
    assert exec_plan.plan.side == "BUY"
    assert exec_plan.plan.position_side == FuturesPositionSide.BOTH

