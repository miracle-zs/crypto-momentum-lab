"""Immutable recovered order values shared by storage and execution callers."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    OrderExecutionPlan,
)


@dataclass(frozen=True, slots=True)
class PersistedExchangeOrder:
    plan: OrderExecutionPlan
    state: ExchangeOrderState
    exchange_order_id: str | None
    updated_at: datetime
    executed_quantity: Decimal = Decimal("0")


@dataclass(frozen=True, slots=True)
class OrderObservation:
    symbol: str
    position_side: str
    side: str
    reduce_only: bool
    order_type: str
    quantity: Decimal
    executed_quantity: Decimal
    state: str
    client_order_id: str | None
    exchange_order_id: str | None
    created_at: datetime
    updated_at: datetime
    price: Decimal | None


@dataclass(frozen=True, slots=True)
class OrderIdentityEvent:
    client_order_id: str | None
    exchange_order_id: str | None
    state: str
    occurred_at: datetime
    details: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class PositionObservation:
    symbol: str
    position_side: str
    position_amt: Decimal
    entry_price: Decimal
    observed_at: datetime
