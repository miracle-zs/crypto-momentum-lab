"""Immutable recovered order values shared by storage and execution callers."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from crypto_momentum_lab.domain.account.models import AccountFillEvent
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    OrderExecutionPlan,
)


@dataclass(frozen=True, slots=True)
class PersistedOrderReceipt:
    """A priced terminal exchange fact recovered from durable observations."""

    client_order_id: str
    state: ExchangeOrderState
    exchange_order_id: str | None
    executed_quantity: Decimal
    average_price: Decimal
    account_fills: tuple[AccountFillEvent, ...] = ()

    def __post_init__(self) -> None:
        if not self.state.terminal:
            raise ValueError("persisted terminal receipt must be terminal")
        if not self.executed_quantity.is_finite() or self.executed_quantity < 0:
            raise ValueError(
                "persisted executed quantity must be finite and nonnegative"
            )
        if not self.average_price.is_finite() or self.average_price < 0:
            raise ValueError("persisted average price must be finite and nonnegative")
        if self.executed_quantity > 0 and self.average_price <= 0:
            raise ValueError("persisted positive execution requires a priced fact")
        if self.account_fills:
            if len({fill.trade_id for fill in self.account_fills}) != len(
                self.account_fills
            ):
                raise ValueError(
                    "persisted settlement trades must have distinct identities"
                )
            if any(
                fill.order_id not in {self.client_order_id, self.exchange_order_id}
                for fill in self.account_fills
            ):
                raise ValueError("persisted settlement trade order identity mismatch")
            if (
                sum((fill.quantity for fill in self.account_fills), Decimal(0))
                != self.executed_quantity
            ):
                raise ValueError(
                    "persisted settlement trades do not match executed quantity"
                )


@dataclass(frozen=True, slots=True)
class PersistedExchangeOrder:
    plan: OrderExecutionPlan
    state: ExchangeOrderState
    exchange_order_id: str | None
    updated_at: datetime
    executed_quantity: Decimal = Decimal("0")
    terminal_receipt: PersistedOrderReceipt | None = None


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
class PositionObservation:
    symbol: str
    position_side: str
    position_amt: Decimal
    entry_price: Decimal
    observed_at: datetime
