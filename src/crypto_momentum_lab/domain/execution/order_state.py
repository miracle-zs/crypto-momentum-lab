from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from crypto_momentum_lab.domain.market.models import JsonValue


class ExchangeOrderState(StrEnum):
    INTENT_APPROVED = "intent_approved"
    CLAIMED = "claimed"
    PLANNED = "planned"
    SUBMITTING = "submitting"
    CANCELING = "canceling"
    SUBMITTED = "submitted"
    ACKNOWLEDGED = "acknowledged"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    SUPPRESSED = "suppressed"
    UNKNOWN_PENDING_RECONCILIATION = "unknown_pending_reconciliation"

    @property
    def terminal(self) -> bool:
        return self in {
            self.FILLED,
            self.CANCELED,
            self.REJECTED,
            self.EXPIRED,
            self.SUPPRESSED,
        }


class FuturesPositionSide(StrEnum):
    BOTH = "BOTH"
    LONG = "LONG"
    SHORT = "SHORT"


@dataclass(frozen=True, slots=True)
class OrderExecutionPlan:
    intent_id: str
    run_id: str
    client_order_id: str
    symbol: str
    side: str
    order_type: str
    quantity: Decimal
    price: Decimal | None
    reduce_only: bool
    created_at: datetime
    position_side: FuturesPositionSide = FuturesPositionSide.BOTH
    quantized: bool = False

    def __post_init__(self) -> None:
        for value, field_name in (
            (self.intent_id, "intent_id"),
            (self.run_id, "run_id"),
            (self.client_order_id, "client_order_id"),
            (self.symbol, "symbol"),
            (self.side, "side"),
            (self.order_type, "order_type"),
        ):
            _require_non_empty(value, field_name)
        if self.quantity <= 0:
            raise ValueError("quantity must be positive")
        if self.price is not None and self.price <= 0:
            raise ValueError("price must be positive when present")
        if not isinstance(self.position_side, FuturesPositionSide):
            object.__setattr__(
                self,
                "position_side",
                FuturesPositionSide(self.position_side),
            )
        _require_aware(self.created_at, "created_at")


@dataclass(frozen=True, slots=True)
class ExchangeOrderEvent:
    event_id: str
    client_order_id: str
    state: ExchangeOrderState
    occurred_at: datetime
    exchange_order_id: str | None
    details: dict[str, JsonValue]

    def __post_init__(self) -> None:
        _require_non_empty(self.event_id, "event_id")
        _require_non_empty(self.client_order_id, "client_order_id")
        if not isinstance(self.state, ExchangeOrderState):
            raise ValueError("state must be an ExchangeOrderState")
        _require_aware(self.occurred_at, "occurred_at")


@dataclass(frozen=True, slots=True)
class ExchangeOrderFill:
    fill_id: str
    client_order_id: str
    exchange_trade_id: str
    price: Decimal
    quantity: Decimal
    fee: Decimal
    fee_asset: str
    filled_at: datetime
    details: dict[str, JsonValue]

    def __post_init__(self) -> None:
        for value, field_name in (
            (self.fill_id, "fill_id"),
            (self.client_order_id, "client_order_id"),
            (self.exchange_trade_id, "exchange_trade_id"),
            (self.fee_asset, "fee_asset"),
        ):
            _require_non_empty(value, field_name)
        if self.price <= 0:
            raise ValueError("price must be positive")
        if self.quantity <= 0:
            raise ValueError("quantity must be positive")
        if self.fee < 0:
            raise ValueError("fee must be non-negative")
        _require_aware(self.filled_at, "filled_at")


@dataclass(frozen=True, slots=True)
class ExchangeOrderSnapshot:
    client_order_id: str
    exchange_order_id: str
    state: ExchangeOrderState
    observed_at: datetime
    executed_quantity: Decimal
    average_price: Decimal
    fills: tuple[ExchangeOrderFill, ...] = ()

    def __post_init__(self) -> None:
        _require_non_empty(self.client_order_id, "client_order_id")
        _require_non_empty(self.exchange_order_id, "exchange_order_id")
        if not isinstance(self.state, ExchangeOrderState):
            raise ValueError("state must be an ExchangeOrderState")
        if self.executed_quantity < 0:
            raise ValueError("executed_quantity must be non-negative")
        if self.average_price < 0:
            raise ValueError("average_price must be non-negative")
        _require_aware(self.observed_at, "observed_at")


def _require_non_empty(value: str, field_name: str) -> None:
    if not value.strip():
        raise ValueError(f"{field_name} must not be empty")


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
