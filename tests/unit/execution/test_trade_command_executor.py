"""Unit tests for TradeCommandExecutor."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
from crypto_momentum_lab.domain.execution.trade_command import (
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide
from crypto_momentum_lab.execution_account.orders.quantization import (
    SymbolTradingRules,
)
from crypto_momentum_lab.execution_account.orders.trade_command_executor import (
    TradeCommandExecutor,
)

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
    """TradeCommandExecutor quantizes strictly downward to step_size, recording remainder."""
    cmd = TradeCommand(
        command_id="cmd-1",
        position_key=POS_KEY,
        command_type=TradeCommandType.EXIT,
        side=StrategySide.LONG,
        order_type=EntryType.MARKET,
        requested_quantity=Decimal("0.0058"),
        reduce_only=True,
    )

    result = TradeCommandExecutor.plan_execution(
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

    result = TradeCommandExecutor.plan_execution(
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

    result = TradeCommandExecutor.plan_execution(
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
    entry_result = TradeCommandExecutor.plan_execution(
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
    exit_result = TradeCommandExecutor.plan_execution(
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
    result_one_way = TradeCommandExecutor.plan_execution(
        cmd,
        RULES,
        run_id="run-1",
        reference_price=Decimal("50000"),
        hedge_mode=False,
    )
    assert result_one_way.plan is not None
    assert result_one_way.plan.position_side == FuturesPositionSide.BOTH
    assert result_one_way.plan.price == Decimal("50123.40")  # tick_size 0.10 ROUND_DOWN for BUY
    assert result_one_way.plan.client_order_id == "custom-order-id-1"

    # In Hedge mode:
    result_hedge = TradeCommandExecutor.plan_execution(
        cmd,
        RULES,
        run_id="run-1",
        reference_price=Decimal("50000"),
        hedge_mode=True,
    )
    assert result_hedge.plan is not None
    assert result_hedge.plan.position_side == FuturesPositionSide.LONG
