from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account.models import (
    AccountOpenOrderSnapshot,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.order_state import (
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.execution_account.binance.exit_recovery_rules import (
    exit_position_quantity,
    open_order_matches_exit,
)
from crypto_momentum_lab.execution_account.orders.recovery import (
    ExitRecoveryInspectionUnknownError,
)


def plan(side="SELL", position_side="BOTH"):
    return OrderExecutionPlan(
        intent_id="intent",
        run_id="run",
        client_order_id="exit",
        symbol="BTCUSDT",
        side=side,
        order_type="MARKET",
        quantity=Decimal("1"),
        price=None,
        reduce_only=True,
        created_at=datetime(2026, 10, 1, tzinfo=UTC),
        position_side=FuturesPositionSide(position_side),
    )


def position(side, amount, symbol="BTCUSDT"):
    return AccountPositionSnapshot(
        environment="live",
        account_label="primary",
        symbol=symbol,
        position_side=side,
        position_amt=Decimal(amount),
        entry_price=Decimal("10"),
        mark_price=Decimal("10"),
        unrealized_pnl=Decimal("0"),
        notional=Decimal("10"),
        leverage=1,
        margin_type="cross",
        observed_at=datetime(2026, 10, 1, tzinfo=UTC),
        raw_payload={},
    )


def order(side="SELL", position_side=None, reduce_only=True, symbol="BTCUSDT"):
    return AccountOpenOrderSnapshot(
        environment="live",
        account_label="primary",
        symbol=symbol,
        order_id="1",
        client_order_id="exit",
        side=side,
        order_type="MARKET",
        status="NEW",
        price=Decimal("0"),
        original_quantity=Decimal("1"),
        executed_quantity=Decimal("0"),
        reduce_only=reduce_only,
        observed_at=datetime(2026, 10, 1, tzinfo=UTC),
        raw_payload={} if position_side is None else {"positionSide": position_side},
    )


@pytest.mark.parametrize(
    "amount,position_side,plan_side,plan_position_side,expected",
    [
        ("2", "BOTH", "SELL", "BOTH", "2"),
        ("-2", "BOTH", "BUY", "BOTH", "2"),
        ("-2", "BOTH", "SELL", "BOTH", "0"),
        ("2", "BOTH", "BUY", "BOTH", "0"),
        ("0", "BOTH", "SELL", "BOTH", "0"),
        ("2", "long", "SELL", "LONG", "2"),
        ("-2", "SHORT", "BUY", "SHORT", "2"),
        ("2", "LONG", "SELL", "SHORT", "0"),
    ],
)
def test_exit_quantity_scopes_position_direction(
    amount, position_side, plan_side, plan_position_side, expected
):
    assert exit_position_quantity(
        position(position_side, amount), plan(plan_side, plan_position_side)
    ) == Decimal(expected)


def test_other_symbol_is_ignored_before_position_side_validation():
    assert exit_position_quantity(position("INVALID", "1", "ETHUSDT"), plan()) == 0
    assert (
        open_order_matches_exit(
            order(position_side="INVALID", symbol="ETHUSDT"), plan()
        )
        is False
    )


@pytest.mark.parametrize(
    "raw_side,plan_side,reduce_only,expected",
    [
        (None, "BOTH", True, True),
        (None, "BOTH", False, False),
        ("BOTH", "BOTH", False, False),
        ("BOTH", "BOTH", True, True),
        ("long", "LONG", False, True),
        ("SHORT", "SHORT", False, True),
        ("SHORT", "LONG", True, False),
    ],
)
def test_order_direction_and_one_way_reduce_only(
    raw_side, plan_side, reduce_only, expected
):
    assert (
        open_order_matches_exit(
            order(position_side=raw_side, reduce_only=reduce_only),
            plan(position_side=plan_side),
        )
        is expected
    )


def test_order_side_is_case_insensitive_and_mismatch_precedes_payload_validation():
    assert open_order_matches_exit(order(side="sell"), plan()) is True
    assert (
        open_order_matches_exit(order(side="BUY", position_side="INVALID"), plan())
        is False
    )


@pytest.mark.parametrize(
    "source", ["position", "missing_order_side", "invalid_order_side"]
)
def test_invalid_or_missing_position_side_remains_unknown_inspection(source):
    with pytest.raises(ExitRecoveryInspectionUnknownError) as caught:
        if source == "position":
            exit_position_quantity(position("INVALID", "1"), plan())
        elif source == "missing_order_side":
            open_order_matches_exit(order(), plan(position_side="LONG"))
        else:
            open_order_matches_exit(order(position_side="INVALID"), plan())
    if source != "missing_order_side":
        assert isinstance(caught.value.__cause__, ValueError)
    else:
        assert "omitted positionSide" in str(caught.value)
        assert caught.value.__cause__ is None
