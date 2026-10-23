from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from crypto_momentum_lab.domain.market.models import JsonValue


@dataclass(frozen=True, slots=True)
class AccountPositionSnapshot:
    environment: str
    account_label: str
    symbol: str
    position_side: str
    position_amt: Decimal
    entry_price: Decimal
    mark_price: Decimal
    unrealized_pnl: Decimal
    notional: Decimal
    leverage: int | None
    margin_type: str | None
    observed_at: datetime
    raw_payload: dict[str, JsonValue]

    def __post_init__(self) -> None:
        _require_common(self.environment, self.account_label)
        _require_non_empty(self.symbol, "symbol")
        _require_non_empty(self.position_side, "position_side")
        _require_aware(self.observed_at, "observed_at")
        if self.leverage is not None and self.leverage < 0:
            raise ValueError("leverage must be non-negative")


@dataclass(frozen=True, slots=True)
class AccountFillEvent:
    environment: str
    account_label: str
    symbol: str
    trade_id: str
    order_id: str
    side: str
    price: Decimal
    quantity: Decimal
    realized_pnl: Decimal
    fee: Decimal
    fee_asset: str
    trade_at: datetime
    raw_payload: dict[str, JsonValue]

    def __post_init__(self) -> None:
        _require_common(self.environment, self.account_label)
        _require_non_empty(self.symbol, "symbol")
        _require_non_empty(self.trade_id, "trade_id")
        _require_non_empty(self.order_id, "order_id")
        _require_non_empty(self.side, "side")
        _require_non_empty(self.fee_asset, "fee_asset")
        _require_non_negative(self.price, "price")
        _require_non_negative(self.quantity, "quantity")
        _require_non_negative(self.fee, "fee")
        _require_aware(self.trade_at, "trade_at")


def _require_common(environment: str, account_label: str) -> None:
    _require_non_empty(environment, "environment")
    _require_non_empty(account_label, "account_label")


def _require_non_empty(value: str, field_name: str) -> None:
    if not value.strip():
        raise ValueError(f"{field_name} must not be empty")


def _require_non_negative(value: Decimal, field_name: str) -> None:
    if value < 0:
        raise ValueError(f"{field_name} must be non-negative")


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
