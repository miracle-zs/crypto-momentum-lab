from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal

from crypto_momentum_lab.domain.execution import OrderExecutionPlan
from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    StrategySide,
)
from crypto_momentum_lab.execution_account.orders.ids import (
    deterministic_client_order_id,
)


@dataclass(frozen=True, slots=True)
class SymbolTradingRules:
    symbol: str
    tick_size: Decimal
    step_size: Decimal
    min_quantity: Decimal
    max_quantity: Decimal
    min_notional: Decimal

    def __post_init__(self) -> None:
        if not self.symbol.strip():
            raise ValueError("symbol must not be empty")
        for field_name in (
            "tick_size",
            "step_size",
            "min_quantity",
            "max_quantity",
            "min_notional",
        ):
            if getattr(self, field_name) <= 0:
                raise ValueError(f"{field_name} must be positive")
        if self.max_quantity < self.min_quantity:
            raise ValueError("max_quantity must not be below min_quantity")


@dataclass(frozen=True, slots=True)
class QuantizationRejection:
    reason: str
    details: dict[str, str]


def quantize_order_plan(
    intent: OrderIntentCandidate,
    rules: SymbolTradingRules,
    *,
    reference_price: Decimal,
    resize_tolerance: Decimal,
) -> OrderExecutionPlan | QuantizationRejection:
    if intent.symbol != rules.symbol:
        raise ValueError("intent symbol must match trading rules")
    if reference_price <= 0:
        raise ValueError("reference_price must be positive")
    if resize_tolerance < 0 or resize_tolerance >= 1:
        raise ValueError("resize_tolerance must be in [0, 1)")
    if intent.desired_notional is None:
        return QuantizationRejection("missing_desired_notional", {})

    price = _quantized_price(intent, rules, reference_price)
    sizing_price = reference_price if price is None else price
    raw_quantity = intent.desired_notional / sizing_price
    quantity = _round_down(raw_quantity, rules.step_size)
    actual_notional = quantity * sizing_price

    if quantity < rules.min_quantity:
        return _rejection(
            "below_min_quantity",
            quantity=quantity,
            limit=rules.min_quantity,
        )
    if quantity > rules.max_quantity:
        return _rejection(
            "above_max_quantity",
            quantity=quantity,
            limit=rules.max_quantity,
        )
    if actual_notional < rules.min_notional:
        return _rejection(
            "below_min_notional",
            actual_notional=actual_notional,
            limit=rules.min_notional,
        )
    resize_fraction = (intent.desired_notional - actual_notional).copy_abs() / (
        intent.desired_notional
    )
    if resize_fraction > resize_tolerance:
        return _rejection(
            "resize_beyond_tolerance",
            desired_notional=intent.desired_notional,
            actual_notional=actual_notional,
            resize_fraction=resize_fraction,
            tolerance=resize_tolerance,
        )

    return OrderExecutionPlan(
        intent_id=intent.candidate_id,
        run_id=intent.run_id,
        client_order_id=deterministic_client_order_id(
            intent.run_id,
            intent.candidate_id,
        ),
        symbol=intent.symbol,
        side=_exchange_side(intent),
        order_type=intent.entry_type.value.upper(),
        quantity=quantity,
        price=price,
        reduce_only=intent.reduce_only,
        created_at=intent.created_at,
    )


def _quantized_price(
    intent: OrderIntentCandidate,
    rules: SymbolTradingRules,
    reference_price: Decimal,
) -> Decimal | None:
    if intent.entry_type is EntryType.MARKET:
        return None
    source_price = intent.limit_price or reference_price
    return _round_down(source_price, rules.tick_size)


def _exchange_side(intent: OrderIntentCandidate) -> str:
    opening_buy = intent.side is StrategySide.LONG
    should_buy = not opening_buy if intent.reduce_only else opening_buy
    return "BUY" if should_buy else "SELL"


def _round_down(value: Decimal, increment: Decimal) -> Decimal:
    units = (value / increment).to_integral_value(rounding=ROUND_DOWN)
    return units * increment


def _rejection(reason: str, **details: Decimal) -> QuantizationRejection:
    return QuantizationRejection(
        reason=reason,
        details={key: str(value) for key, value in details.items()},
    )
