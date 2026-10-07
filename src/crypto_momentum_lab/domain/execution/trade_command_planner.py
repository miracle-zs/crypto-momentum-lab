"""Translate domain trade commands into quantized exchange execution plans."""

from dataclasses import dataclass, replace
from decimal import ROUND_DOWN, ROUND_UP, Decimal

from crypto_momentum_lab.domain.execution.order_rules import SymbolTradingRules
from crypto_momentum_lab.domain.execution.order_state import (
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    ExitAllocation,
    TradeCommand,
)
from crypto_momentum_lab.domain.trading import OrderType, TradeSide


@dataclass(frozen=True, slots=True)
class QuantizationRejection:
    reason: str
    details: dict[str, str]


@dataclass(frozen=True, slots=True)
class TradeExecutionPlanResult:
    """Outcome of converting a trade command into an exchange execution plan."""

    command: TradeCommand
    plan: OrderExecutionPlan | None
    rejection: QuantizationRejection | None = None
    unexecuted_quantization_remainder: Decimal = Decimal("0")


def plan_order_execution(
    command: TradeCommand,
    rules: SymbolTradingRules,
    *,
    run_id: str,
    reference_price: Decimal,
    hedge_mode: bool = True,
) -> TradeExecutionPlanResult:
    """Build a plan without inflating quantity; retain quantization remainder."""
    if command.position_key.symbol != rules.symbol:
        raise ValueError("command symbol must match trading rules")
    if reference_price <= 0:
        raise ValueError("reference price must be positive")

    units = (command.requested_quantity / rules.step_size).to_integral_value(
        rounding=ROUND_DOWN
    )
    quantized_quantity = units * rules.step_size
    remainder = command.requested_quantity - quantized_quantity
    if quantized_quantity < rules.min_quantity:
        return TradeExecutionPlanResult(
            command=command,
            plan=None,
            rejection=QuantizationRejection(
                reason="min_quantity_breached",
                details={
                    "requested_quantity": str(command.requested_quantity),
                    "quantized_quantity": str(quantized_quantity),
                    "min_quantity": str(rules.min_quantity),
                },
            ),
            unexecuted_quantization_remainder=command.requested_quantity,
        )
    if quantized_quantity > rules.max_quantity:
        return TradeExecutionPlanResult(
            command=command,
            plan=None,
            rejection=QuantizationRejection(
                reason="max_quantity_breached",
                details={
                    "requested_quantity": str(command.requested_quantity),
                    "quantized_quantity": str(quantized_quantity),
                    "max_quantity": str(rules.max_quantity),
                },
            ),
            unexecuted_quantization_remainder=command.requested_quantity,
        )
    opening_buy = command.side is TradeSide.LONG
    should_buy = not opening_buy if command.reduce_only else opening_buy
    exchange_side = "BUY" if should_buy else "SELL"
    if command.order_type is OrderType.MARKET:
        price = None
    else:
        source_price = command.limit_price or reference_price
        price_rounding = ROUND_DOWN if exchange_side == "BUY" else ROUND_UP
        price = (source_price / rules.tick_size).to_integral_value(
            rounding=price_rounding
        ) * rules.tick_size
    order_price = price if price is not None else reference_price
    order_notional = quantized_quantity * order_price
    if not command.reduce_only and order_notional < rules.min_notional:
        return TradeExecutionPlanResult(
            command=command,
            plan=None,
            rejection=QuantizationRejection(
                reason="min_notional_breached",
                details={
                    "actual_notional": str(order_notional),
                    "min_notional": str(rules.min_notional),
                },
            ),
            unexecuted_quantization_remainder=command.requested_quantity,
        )
    position_side = (
        command.position_key.position_side if hedge_mode else FuturesPositionSide.BOTH
    )
    allocations: tuple[ExitAllocation, ...] = ()
    if command.allocation_plan is not None:
        adjusted: list[ExitAllocation] = []
        remaining = quantized_quantity
        for allocation in command.allocation_plan.allocations:
            if remaining <= 0:
                break
            quantity = min(allocation.allocated_quantity, remaining)
            adjusted.append(replace(allocation, allocated_quantity=quantity))
            remaining -= quantity
        allocations = tuple(adjusted)
    batch_id = (
        allocations[0].batch_id
        if len(allocations) == 1
        else (f"batch_multi_{len(allocations)}" if allocations else None)
    )
    batch_quantities = (
        command.allocation_plan.batch_quantities
        if command.allocation_plan and command.allocation_plan.batch_quantities
        else None
    )
    return TradeExecutionPlanResult(
        command=command,
        plan=OrderExecutionPlan(
            intent_id=command.command_id,
            run_id=run_id,
            client_order_id=command.client_order_id(run_id),
            symbol=command.position_key.symbol,
            side=exchange_side,
            order_type=command.order_type.value.upper(),
            time_in_force="GTC" if command.order_type is OrderType.LIMIT else None,
            quantity=quantized_quantity,
            price=price,
            reduce_only=command.reduce_only,
            created_at=command.created_at,
            position_side=position_side,
            quantized=True,
            batch_id=batch_id,
            allocations=allocations,
            projection_version=command.expected_projection_version,
            batch_quantities=batch_quantities,
            reference_price=reference_price,
        ),
        rejection=None,
        unexecuted_quantization_remainder=remainder,
    )
