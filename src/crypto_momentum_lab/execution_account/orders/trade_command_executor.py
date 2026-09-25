"""Trade command executor translating domain TradeCommand into exchange execution plans."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_DOWN, ROUND_UP, Decimal

from crypto_momentum_lab.domain.execution import (
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    TradeCommand,
)
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide
from crypto_momentum_lab.execution_account.orders.ids import (
    deterministic_client_order_id,
)
from crypto_momentum_lab.execution_account.orders.quantization import (
    QuantizationRejection,
    SymbolTradingRules,
)


@dataclass(frozen=True, slots=True)
class TradeExecutionPlanResult:
    """Outcome of converting a TradeCommand into an exchange execution plan."""

    command: TradeCommand
    plan: OrderExecutionPlan | None
    rejection: QuantizationRejection | None = None
    unexecuted_quantization_remainder: Decimal = Decimal("0")


class TradeCommandExecutor:
    """Authoritative execution plan generator for TradeCommand.

    Guarantees:
    - Never inflates requested_quantity;
    - Downward step_size quantization only;
    - Records unexecuted quantization remainder explicitly.
    """

    @classmethod
    def plan_execution(
        cls,
        command: TradeCommand,
        rules: SymbolTradingRules,
        *,
        run_id: str,
        reference_price: Decimal,
        hedge_mode: bool = False,
    ) -> TradeExecutionPlanResult:
        if command.position_key.symbol != rules.symbol:
            raise ValueError("command symbol must match trading rules")
        if reference_price <= 0:
            raise ValueError("reference price must be positive")

        # Step size downward quantization
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

        actual_notional = quantized_quantity * reference_price
        if not command.reduce_only and actual_notional < rules.min_notional:
            return TradeExecutionPlanResult(
                command=command,
                plan=None,
                rejection=QuantizationRejection(
                    reason="min_notional_breached",
                    details={
                        "actual_notional": str(actual_notional),
                        "min_notional": str(rules.min_notional),
                    },
                ),
                unexecuted_quantization_remainder=command.requested_quantity,
            )

        # Exchange side calculation
        opening_buy = command.side is StrategySide.LONG
        should_buy = not opening_buy if command.reduce_only else opening_buy
        exchange_side = "BUY" if should_buy else "SELL"

        # Price calculation for limit orders
        if command.order_type is EntryType.MARKET:
            price = None
        else:
            source_price = command.limit_price or reference_price
            price_rounding = ROUND_DOWN if exchange_side == "BUY" else ROUND_UP
            price_units = (source_price / rules.tick_size).to_integral_value(
                rounding=price_rounding
            )
            price = price_units * rules.tick_size

        position_side = (
            command.position_key.position_side
            if hedge_mode
            else FuturesPositionSide.BOTH
        )

        client_order_id = command.idempotency_key or deterministic_client_order_id(
            run_id,
            command.command_id,
        )

        allocations = (
            tuple(command.allocation_plan.allocations)
            if command.allocation_plan and command.allocation_plan.allocations
            else ()
        )
        batch_id = (
            allocations[0].batch_id
            if len(allocations) == 1
            else (f"batch_multi_{len(allocations)}" if allocations else None)
        )

        plan = OrderExecutionPlan(
            intent_id=command.command_id,
            run_id=run_id,
            client_order_id=client_order_id,
            symbol=command.position_key.symbol,
            side=exchange_side,
            order_type=command.order_type.value.upper(),
            quantity=quantized_quantity,
            price=price,
            reduce_only=command.reduce_only,
            created_at=command.created_at,
            position_side=position_side,
            quantized=True,
            batch_id=batch_id,
            allocations=allocations,
        )

        return TradeExecutionPlanResult(
            command=command,
            plan=plan,
            rejection=None,
            unexecuted_quantization_remainder=remainder,
        )


__all__ = [
    "TradeCommandExecutor",
    "TradeExecutionPlanResult",
]
