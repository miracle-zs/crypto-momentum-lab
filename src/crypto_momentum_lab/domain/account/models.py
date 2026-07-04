from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from crypto_momentum_lab.domain.market.models import JsonValue


class ExecutionAccountStatus(StrEnum):
    STARTING = "starting"
    SYNCING = "syncing"
    READY_READONLY = "ready_readonly"
    DEGRADED = "degraded"
    HALTED_READONLY = "halted_readonly"
    STOPPED = "stopped"


@dataclass(frozen=True, slots=True)
class AccountBalanceSnapshot:
    environment: str
    account_label: str
    asset: str
    wallet_balance: Decimal
    available_balance: Decimal
    unrealized_pnl: Decimal
    observed_at: datetime
    raw_payload: dict[str, JsonValue]

    def __post_init__(self) -> None:
        _require_common(self.environment, self.account_label)
        _require_non_empty(self.asset, "asset")
        _require_non_negative(self.wallet_balance, "wallet_balance")
        _require_non_negative(self.available_balance, "available_balance")
        _require_aware(self.observed_at, "observed_at")


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
class AccountOpenOrderSnapshot:
    environment: str
    account_label: str
    symbol: str
    order_id: str
    client_order_id: str
    side: str
    order_type: str
    status: str
    price: Decimal
    original_quantity: Decimal
    executed_quantity: Decimal
    reduce_only: bool
    observed_at: datetime
    raw_payload: dict[str, JsonValue]

    def __post_init__(self) -> None:
        _require_common(self.environment, self.account_label)
        _require_non_empty(self.symbol, "symbol")
        _require_non_empty(self.order_id, "order_id")
        _require_non_empty(self.client_order_id, "client_order_id")
        _require_non_empty(self.side, "side")
        _require_non_empty(self.order_type, "order_type")
        _require_non_empty(self.status, "status")
        _require_non_negative(self.price, "price")
        _require_non_negative(self.original_quantity, "original_quantity")
        _require_non_negative(self.executed_quantity, "executed_quantity")
        _require_aware(self.observed_at, "observed_at")


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


@dataclass(frozen=True, slots=True)
class AccountFundingEvent:
    environment: str
    account_label: str
    symbol: str
    income_id: str
    amount: Decimal
    asset: str
    funding_at: datetime
    raw_payload: dict[str, JsonValue]

    def __post_init__(self) -> None:
        _require_common(self.environment, self.account_label)
        _require_non_empty(self.symbol, "symbol")
        _require_non_empty(self.income_id, "income_id")
        _require_non_empty(self.asset, "asset")
        _require_aware(self.funding_at, "funding_at")


@dataclass(frozen=True, slots=True)
class AccountConfigSnapshot:
    environment: str
    account_label: str
    multi_assets_mode: bool
    can_trade: bool
    fee_tier: int | None
    observed_at: datetime
    raw_payload: dict[str, JsonValue]

    def __post_init__(self) -> None:
        _require_common(self.environment, self.account_label)
        if self.fee_tier is not None and self.fee_tier < 0:
            raise ValueError("fee_tier must be non-negative")
        _require_aware(self.observed_at, "observed_at")


@dataclass(frozen=True, slots=True)
class AccountReconciliationRun:
    reconciliation_id: str
    environment: str
    account_label: str
    status: str
    observed_at: datetime
    balance_count: int
    position_count: int
    open_order_count: int
    fill_count: int
    mismatch_count: int
    details: dict[str, JsonValue]

    def __post_init__(self) -> None:
        _require_non_empty(self.reconciliation_id, "reconciliation_id")
        _require_common(self.environment, self.account_label)
        _require_non_empty(self.status, "status")
        _require_aware(self.observed_at, "observed_at")
        for field_name in (
            "balance_count",
            "position_count",
            "open_order_count",
            "fill_count",
            "mismatch_count",
        ):
            value = getattr(self, field_name)
            if value < 0:
                raise ValueError(f"{field_name} must be non-negative")


@dataclass(frozen=True, slots=True)
class ExecutionAccountProcessState:
    environment: str
    account_label: str
    state: ExecutionAccountStatus
    occurred_at: datetime
    reason: str | None

    def __post_init__(self) -> None:
        _require_common(self.environment, self.account_label)
        if not isinstance(self.state, ExecutionAccountStatus):
            raise ValueError("state must be an ExecutionAccountStatus")
        _require_aware(self.occurred_at, "occurred_at")


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
