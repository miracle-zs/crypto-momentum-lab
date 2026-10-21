from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from crypto_momentum_lab.domain.market.models import JsonValue


class ExecutionAccountStatus(StrEnum):
    STARTING = "starting"
    RUNNING = "running"
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


def extract_fill_position_side(raw_payload: Mapping[str, Any] | None) -> str | None:
    """Extract normalized position side (LONG, SHORT, BOTH) from Binance payloads."""
    if not raw_payload or not isinstance(raw_payload, Mapping):
        return None
    value = raw_payload.get("positionSide", raw_payload.get("position_side"))
    if value is None and "row" in raw_payload:
        row = raw_payload["row"]
        if isinstance(row, Mapping):
            value = row.get("ps", row.get("positionSide", row.get("position_side")))
    if value is None and "event" in raw_payload:
        event_dict = raw_payload["event"]
        if isinstance(event_dict, Mapping):
            o = event_dict.get("o")
            target = o if isinstance(o, Mapping) else event_dict
            value = target.get(
                "ps", target.get("positionSide", target.get("position_side"))
            )
    if value is None:
        value = raw_payload.get("ps")
    return str(value).strip().upper() if value is not None else None


def compact_fill_metadata(
    raw_payload: Mapping[str, Any] | None,
) -> dict[str, JsonValue]:
    """Keep only fill metadata used to assign and attribute the position fact."""
    if not isinstance(raw_payload, Mapping):
        return {}

    sources: list[Mapping[str, Any]] = [raw_payload]
    row = raw_payload.get("row")
    if isinstance(row, Mapping):
        sources.append(row)
    event = raw_payload.get("event")
    if isinstance(event, Mapping):
        sources.append(event)
        order = event.get("o")
        if isinstance(order, Mapping):
            sources.append(order)

    def value_for(*keys: str) -> Any:
        for source in sources:
            for key in keys:
                value = source.get(key)
                if value is not None:
                    return value
        return None

    def bool_for(*keys: str) -> bool | None:
        value = value_for(*keys)
        if isinstance(value, bool):
            return value
        if isinstance(value, int) and value in (0, 1):
            return bool(value)
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "1"}:
                return True
            if normalized in {"false", "0"}:
                return False
        return None

    result: dict[str, JsonValue] = {}
    position_side = extract_fill_position_side(raw_payload)
    if position_side is not None:
        result["positionSide"] = position_side

    client_order_id = value_for("client_order_id", "clientOrderId", "c")
    if isinstance(client_order_id, str) and client_order_id.strip():
        result["client_order_id"] = client_order_id.strip()

    for target, aliases in (
        ("is_system", ("is_system",)),
        ("reduce_only", ("reduce_only", "reduceOnly", "R")),
        ("synthetic_from_order", ("synthetic_from_order",)),
    ):
        if bool_for(*aliases) is True:
            result[target] = True

    is_cumulative = bool_for("is_cumulative") is True or any(
        "cum_qty" in source for source in sources
    )
    if is_cumulative:
        result["is_cumulative"] = True
    return result


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

    @property
    def raw_position_side(self) -> str | None:
        return extract_fill_position_side(self.raw_payload)


@dataclass(frozen=True, slots=True)
class AccountFillReconciliationCursor:
    """Durable position in Binance per-symbol trade reconciliation."""

    environment: str
    account_label: str
    symbol: str
    from_id: int | None
    start_time_ms: int | None
    last_checked_at: datetime

    def __post_init__(self) -> None:
        _require_common(self.environment, self.account_label)
        _require_non_empty(self.symbol, "symbol")
        _require_aware(self.last_checked_at, "last_checked_at")
        if self.from_id is not None and self.from_id < 0:
            raise ValueError("from_id must be non-negative")
        if self.start_time_ms is not None and self.start_time_ms < 0:
            raise ValueError("start_time_ms must be non-negative")
        if (self.from_id is None) == (self.start_time_ms is None):
            raise ValueError("exactly one of from_id and start_time_ms must be present")


@dataclass(frozen=True, slots=True)
class AccountConfigSnapshot:
    environment: str
    account_label: str
    multi_assets_mode: bool
    hedge_mode: bool
    fee_tier: int | None
    observed_at: datetime
    raw_payload: dict[str, JsonValue]

    def __post_init__(self) -> None:
        _require_common(self.environment, self.account_label)
        if self.fee_tier is not None and self.fee_tier < 0:
            raise ValueError("fee_tier must be non-negative")
        _require_aware(self.observed_at, "observed_at")


@dataclass(frozen=True, slots=True)
class AccountPositionStateSnapshot:
    """The latest authoritative set of non-zero account position legs."""

    environment: str
    account_label: str
    reconciliation_id: str
    observed_at: datetime
    position_count: int
    position_keys: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        _require_common(self.environment, self.account_label)
        _require_non_empty(self.reconciliation_id, "reconciliation_id")
        _require_aware(self.observed_at, "observed_at")
        if self.position_count < 0:
            raise ValueError("position_count must be non-negative")
        normalized = tuple(
            (symbol.strip().upper(), side.strip().upper())
            for symbol, side in self.position_keys
        )
        if any(not symbol or not side for symbol, side in normalized):
            raise ValueError("position keys must contain non-empty symbol and side")
        if len(set(normalized)) != len(normalized):
            raise ValueError("position keys must be unique")
        if len(normalized) != self.position_count:
            raise ValueError("position snapshot count must match its keys")
        object.__setattr__(self, "position_keys", normalized)

    @property
    def symbols(self) -> frozenset[str]:
        return frozenset(symbol for symbol, _side in self.position_keys)


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
