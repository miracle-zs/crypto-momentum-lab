"""Unit tests for plan_order_execution."""

from __future__ import annotations

from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.execution.order_rules import SymbolTradingRules
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
from crypto_momentum_lab.domain.execution.trade_command import (
    ExitAllocation,
    ExitAllocationPlan,
    ExitPolicyMode,
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.execution.trade_command_planner import (
    plan_order_execution,
)
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide

RULES = SymbolTradingRules(
    symbol="BTCUSDT",
    tick_size=Decimal("0.10"),
    step_size=Decimal("0.001"),
    min_quantity=Decimal("0.001"),
    max_quantity=Decimal("100.0"),
    min_notional=Decimal("5.0"),
)

POS_KEY = PositionKey(
    environment="production",
    account_label="binance-prod",
    symbol="BTCUSDT",
    position_side=FuturesPositionSide.LONG,
)


def test_trade_command_executor_downward_quantization() -> None:
    """Planning quantizes downward to step_size and records the remainder."""
    cmd = TradeCommand(
        command_id="cmd-1",
        position_key=POS_KEY,
        command_type=TradeCommandType.EXIT,
        side=StrategySide.LONG,
        order_type=EntryType.MARKET,
        requested_quantity=Decimal("0.0058"),
        reduce_only=True,
    )

    result = plan_order_execution(
        cmd,
        RULES,
        run_id="run-1",
        reference_price=Decimal("50000"),
    )

    assert result.rejection is None
    assert result.plan is not None
    assert result.plan.quantity == Decimal("0.005")
    assert result.plan.quantity <= cmd.requested_quantity
    assert result.unexecuted_quantization_remainder == Decimal("0.0008")
    assert result.plan.side == "SELL"  # reduce_only exit of LONG


def test_trade_command_executor_min_quantity_rejection() -> None:
    """Quantized quantity below min_quantity results in explicit rejection."""
    cmd = TradeCommand(
        command_id="cmd-2",
        position_key=POS_KEY,
        command_type=TradeCommandType.ENTRY,
        side=StrategySide.LONG,
        order_type=EntryType.MARKET,
        requested_quantity=Decimal("0.0005"),
    )

    result = plan_order_execution(
        cmd,
        RULES,
        run_id="run-1",
        reference_price=Decimal("50000"),
    )

    assert result.plan is None
    assert result.rejection is not None
    assert result.rejection.reason == "min_quantity_breached"
    assert result.unexecuted_quantization_remainder == Decimal("0.0005")


def test_trade_command_executor_max_quantity_rejection() -> None:
    """Quantized quantity above max_quantity results in explicit rejection."""
    cmd = TradeCommand(
        command_id="cmd-3",
        position_key=POS_KEY,
        command_type=TradeCommandType.ENTRY,
        side=StrategySide.LONG,
        order_type=EntryType.MARKET,
        requested_quantity=Decimal("150.0"),
    )

    result = plan_order_execution(
        cmd,
        RULES,
        run_id="run-1",
        reference_price=Decimal("50000"),
    )

    assert result.plan is None
    assert result.rejection is not None
    assert result.rejection.reason == "max_quantity_breached"


def test_trade_command_executor_min_notional_handling() -> None:
    """Reduce-only exits permit notional below min_notional, entries are rejected."""
    # 0.001 BTC * $3000 = $3.00 (< $5.00 min_notional)
    entry_cmd = TradeCommand(
        command_id="cmd-entry",
        position_key=POS_KEY,
        command_type=TradeCommandType.ENTRY,
        side=StrategySide.LONG,
        order_type=EntryType.MARKET,
        requested_quantity=Decimal("0.001"),
        reduce_only=False,
    )
    entry_result = plan_order_execution(
        entry_cmd,
        RULES,
        run_id="run-1",
        reference_price=Decimal("3000"),
    )
    assert entry_result.plan is None
    assert entry_result.rejection is not None
    assert entry_result.rejection.reason == "min_notional_breached"

    exit_cmd = TradeCommand(
        command_id="cmd-exit",
        position_key=POS_KEY,
        command_type=TradeCommandType.EXIT,
        side=StrategySide.LONG,
        order_type=EntryType.MARKET,
        requested_quantity=Decimal("0.001"),
        reduce_only=True,
    )
    exit_result = plan_order_execution(
        exit_cmd,
        RULES,
        run_id="run-1",
        reference_price=Decimal("3000"),
    )
    assert exit_result.plan is not None
    assert exit_result.rejection is None
    assert exit_result.plan.quantity == Decimal("0.001")


def test_trade_command_executor_limit_price_and_hedge_mode() -> None:
    """Limit price is quantized to tick_size; hedge_mode preserves position_side."""
    cmd = TradeCommand(
        command_id="cmd-limit",
        position_key=POS_KEY,
        command_type=TradeCommandType.ENTRY,
        side=StrategySide.LONG,
        order_type=EntryType.LIMIT,
        requested_quantity=Decimal("0.01"),
        limit_price=Decimal("50123.456"),
        idempotency_key="custom-order-id-1",
    )

    # In One-Way mode:
    result_one_way = plan_order_execution(
        cmd,
        RULES,
        run_id="run-1",
        reference_price=Decimal("50000"),
        hedge_mode=False,
    )
    assert result_one_way.plan is not None
    assert result_one_way.plan.position_side == FuturesPositionSide.BOTH
    assert result_one_way.plan.price == Decimal(
        "50123.40"
    )  # tick_size 0.10 ROUND_DOWN for BUY
    assert result_one_way.plan.client_order_id == "custom-order-id-1"

    # In Hedge mode:
    result_hedge = plan_order_execution(
        cmd,
        RULES,
        run_id="run-1",
        reference_price=Decimal("50000"),
        hedge_mode=True,
    )
    assert result_hedge.plan is not None
    assert result_hedge.plan.position_side == FuturesPositionSide.LONG


def test_trade_command_executor_limit_sell_price_rounding_up() -> None:
    """Sell limit order price is quantized using ROUND_UP to tick_size."""
    cmd = TradeCommand(
        command_id="cmd-limit-sell",
        position_key=POS_KEY,
        command_type=TradeCommandType.EXIT,
        side=StrategySide.LONG,
        order_type=EntryType.LIMIT,
        requested_quantity=Decimal("0.01"),
        limit_price=Decimal("50123.41"),
        reduce_only=True,
    )

    result = plan_order_execution(
        cmd,
        RULES,
        run_id="run-1",
        reference_price=Decimal("50000"),
    )
    assert result.plan is not None
    assert result.plan.side == "SELL"
    # tick_size 0.10: 50123.41 ROUND_UP -> 50123.50
    assert result.plan.price == Decimal("50123.50")
    assert result.plan.reference_price == Decimal("50000")


def test_limit_order_min_notional_uses_the_quantized_limit_price() -> None:
    command = TradeCommand(
        command_id="limit-min-notional",
        position_key=POS_KEY,
        command_type=TradeCommandType.ENTRY,
        side=StrategySide.LONG,
        order_type=EntryType.LIMIT,
        requested_quantity=Decimal("1"),
        limit_price=Decimal("4.91"),
    )

    result = plan_order_execution(
        command,
        RULES,
        run_id="run-1",
        reference_price=Decimal("5.10"),
    )

    assert result.plan is None
    assert result.rejection is not None
    assert result.rejection.reason == "min_notional_breached"
    assert result.rejection.details["actual_notional"] == "4.90"


def test_plan_order_execution_preserves_reference_price_for_market_order() -> None:
    cmd = TradeCommand(
        command_id="cmd-market-ref",
        position_key=PositionKey(
            environment="live",
            account_label="primary",
            symbol="BTCUSDT",
            position_side=FuturesPositionSide.BOTH,
        ),
        command_type=TradeCommandType.ENTRY,
        side=StrategySide.LONG,
        order_type=EntryType.MARKET,
        requested_quantity=Decimal("0.05"),
        limit_price=None,
    )
    result = plan_order_execution(
        cmd,
        RULES,
        run_id="run-1",
        reference_price=Decimal("65432.10"),
    )
    assert result.plan is not None
    assert result.plan.price is None
    assert result.plan.reference_price == Decimal("65432.10")


@pytest.mark.parametrize(
    "quantities", [("0.0058",), ("0.003", "0.0028"), ("0.005", "0.0008")]
)
def test_exit_quantization_trims_allocations_without_losing_entry_prices(quantities):
    allocations = tuple(
        ExitAllocation(
            batch_id=f"batch-{index}",
            allocated_quantity=Decimal(quantity),
            entry_price=Decimal(50000 + index),
        )
        for index, quantity in enumerate(quantities)
    )
    total = sum((a.allocated_quantity for a in allocations), Decimal("0"))
    command = TradeCommand(
        command_id="exit-quantized",
        position_key=POS_KEY,
        command_type=TradeCommandType.EXIT,
        side=StrategySide.LONG,
        order_type=EntryType.MARKET,
        requested_quantity=total,
        reduce_only=True,
        allocation_plan=ExitAllocationPlan(
            position_key=POS_KEY,
            allocations=allocations,
            total_allocated_quantity=total,
            policy=ExitPolicyMode.TARGET_BATCHES_ONLY,
        ),
    )
    result = plan_order_execution(
        command, RULES, run_id="run-1", reference_price=Decimal("50000")
    )
    assert result.plan is not None
    assert result.plan.quantity == Decimal("0.005")
    assert (
        sum((a.allocated_quantity for a in result.plan.allocations), Decimal("0"))
        == result.plan.quantity
    )
    original = {a.batch_id: a for a in allocations}
    for allocation in result.plan.allocations:
        assert allocation.entry_price == original[allocation.batch_id].entry_price
        assert (
            allocation.allocated_quantity
            <= original[allocation.batch_id].allocated_quantity
        )
    assert result.unexecuted_quantization_remainder == Decimal("0.0008")


def test_one_way_short_exit_plans_buy_order() -> None:
    command = TradeCommand(
        command_id="one-way-short-exit",
        position_key=PositionKey(
            environment="live",
            account_label="primary",
            symbol="BTCUSDT",
            position_side=FuturesPositionSide.BOTH,
        ),
        command_type=TradeCommandType.EXIT,
        side=StrategySide.SHORT,
        order_type=EntryType.MARKET,
        requested_quantity=Decimal("0.01"),
        reduce_only=True,
    )
    result = plan_order_execution(
        command,
        RULES,
        run_id="run-1",
        reference_price=Decimal("50000"),
        hedge_mode=False,
    )
    assert result.plan is not None
    assert result.plan.side == "BUY"
    assert result.plan.position_side is FuturesPositionSide.BOTH
    assert result.plan.reduce_only
