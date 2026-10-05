"""Domain result of a submitted, reconciled, or cancelled order."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    OrderExecutionPlan,
)


@dataclass(frozen=True, slots=True)
class OrderExecutionResult:
    """The exchange-facing outcome consumed outside the execution adapter."""

    client_order_id: str
    state: ExchangeOrderState
    exchange_order_id: str | None
    executed_quantity: Decimal = Decimal("0")
    average_price: Decimal = Decimal("0")
    plan: OrderExecutionPlan | None = None
    prepared_at: datetime | None = None
