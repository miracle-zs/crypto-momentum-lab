from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.execution import OrderExecutionPlan
from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    StrategySide,
)
from crypto_momentum_lab.execution_account.orders.quantization import (
    QuantizationRejection,
    SymbolTradingRules,
    quantize_order_plan,
)


def test_quantize_market_quantity_to_step_size() -> None:
    result = quantize_order_plan(
        _intent(Decimal("100")),
        _rules(),
        reference_price=Decimal("30000"),
        resize_tolerance=Decimal("0.20"),
    )

    assert isinstance(result, OrderExecutionPlan)
    assert result.quantity == Decimal("0.003")
    assert result.price is None


def test_rejects_below_min_notional() -> None:
    result = quantize_order_plan(
        _intent(Decimal("4")),
        replace(
            _rules(),
            step_size=Decimal("0.0001"),
            min_quantity=Decimal("0.0001"),
        ),
        reference_price=Decimal("30000"),
        resize_tolerance=Decimal("0.50"),
    )

    assert isinstance(result, QuantizationRejection)
    assert result.reason == "below_min_notional"


def test_rejects_resize_beyond_tolerance() -> None:
    result = quantize_order_plan(
        _intent(Decimal("100")),
        _rules(),
        reference_price=Decimal("30000"),
        resize_tolerance=Decimal("0.05"),
    )

    assert isinstance(result, QuantizationRejection)
    assert result.reason == "resize_beyond_tolerance"


def _rules() -> SymbolTradingRules:
    return SymbolTradingRules(
        symbol="BTCUSDT",
        tick_size=Decimal("0.10"),
        step_size=Decimal("0.001"),
        min_quantity=Decimal("0.001"),
        max_quantity=Decimal("100"),
        min_notional=Decimal("5"),
    )


def _intent(desired_notional: Decimal) -> OrderIntentCandidate:
    now = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)
    return OrderIntentCandidate(
        candidate_id="candidate-1",
        signal_id="signal-1",
        run_id="run-1",
        strategy_name="compression_breakout",
        strategy_version="v1",
        config_hash="a" * 64,
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        entry_type=EntryType.MARKET,
        limit_price=None,
        desired_notional=desired_notional,
        reduce_only=False,
        expires_at=now + timedelta(seconds=30),
        created_at=now,
        reason="test",
        features={},
    )
