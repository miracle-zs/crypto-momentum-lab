import hashlib
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

BINANCE_CLIENT_ORDER_ID_MAX_LENGTH = 36
_CLIENT_ORDER_ID_PREFIX = "cml_"
_DIGEST_LENGTH = BINANCE_CLIENT_ORDER_ID_MAX_LENGTH - len(_CLIENT_ORDER_ID_PREFIX)


def deterministic_client_order_id(run_id: str, intent_id: str) -> str:
    if not run_id.strip():
        raise ValueError("run_id must not be empty")
    if not intent_id.strip():
        raise ValueError("intent_id must not be empty")
    digest = hashlib.sha256(f"{run_id}\0{intent_id}".encode()).hexdigest()
    return f"{_CLIENT_ORDER_ID_PREFIX}{digest[:_DIGEST_LENGTH]}"


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
    ABSENT_RECONCILED = "absent_reconciled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    SUPPRESSED = "suppressed"
    UNKNOWN_PENDING_RECONCILIATION = "unknown_pending_reconciliation"

    @property
    def terminal(self) -> bool:
        return self in {
            self.FILLED,
            self.CANCELED,
            self.ABSENT_RECONCILED,
            self.REJECTED,
            self.EXPIRED,
            self.SUPPRESSED,
        }


class FuturesPositionSide(StrEnum):
    BOTH = "BOTH"
    LONG = "LONG"
    SHORT = "SHORT"


@dataclass(frozen=True, slots=True)
class ExitAllocation:
    """Explicit allocation of an exit order to a specific lot/batch."""

    batch_id: str
    allocated_quantity: Decimal
    entry_price: Decimal = Decimal("0")

    def __post_init__(self) -> None:
        if not self.batch_id.strip():
            raise ValueError("batch_id must not be empty")
        if self.allocated_quantity <= 0:
            raise ValueError("allocated_quantity must be positive")
        if self.entry_price < 0:
            raise ValueError("entry_price must be non-negative")


@dataclass(frozen=True, slots=True)
class OrderExecutionPlan:
    client_order_id: str
    symbol: str
    side: str
    quantity: Decimal
    reduce_only: bool
    position_side: FuturesPositionSide = FuturesPositionSide.BOTH

    def __post_init__(self) -> None:
        for value, field_name in (
            (self.client_order_id, "client_order_id"),
            (self.symbol, "symbol"),
            (self.side, "side"),
        ):
            _require_non_empty(value, field_name)
        if self.quantity <= 0:
            raise ValueError("quantity must be positive")


@dataclass(frozen=True, slots=True)
class ExchangeOrderSnapshot:
    client_order_id: str
    exchange_order_id: str
    state: ExchangeOrderState
    observed_at: datetime
    executed_quantity: Decimal
    average_price: Decimal

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
