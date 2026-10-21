"""Unit tests for TradeCommand and exit allocation planning."""

from __future__ import annotations

from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    PositionKey,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    ExitAllocation,
    ExitAllocationPlan,
    ExitPolicyMode,
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide

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
