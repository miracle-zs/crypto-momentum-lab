"""Pure Binance REST value and bounded userTrades parsing."""

from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

from crypto_momentum_lab.domain.account.models import (
    AccountBalanceSnapshot,
    AccountFillEvent,
    AccountOpenOrderSnapshot,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.order_state import ExchangeOrderSnapshot
from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.execution_account.binance.order_status import (
    exchange_order_state,
)


def decimal_value(value: object) -> Decimal:
    return Decimal(str(value))


def account_fill_from_trade_item(
    item: Mapping[str, object],
    *,
    environment: str,
    account_label: str,
    expected_symbol: str,
) -> AccountFillEvent:
    symbol = rest_require_string(item, "symbol").strip().upper()
    if symbol != expected_symbol:
        raise ValueError("Binance userTrades response contained another symbol")
    return AccountFillEvent(
        environment=environment,
        account_label=account_label,
        symbol=symbol,
        trade_id=str(rest_require_int(item, "id")),
        order_id=str(rest_require_int(item, "orderId")),
        side=rest_require_string(item, "side"),
        price=Decimal(rest_require_string(item, "price")),
        quantity=Decimal(rest_require_string(item, "qty")),
        realized_pnl=Decimal(rest_require_string(item, "realizedPnl")),
        fee=Decimal(rest_require_string(item, "commission")),
        fee_asset=rest_require_string(item, "commissionAsset"),
        trade_at=datetime.fromtimestamp(
            rest_require_int(item, "time") / 1000,
            tz=UTC,
        ),
        raw_payload=json_mapping(item),
    )


def account_open_order_from_item(
    item: Mapping[str, object],
    *,
    environment: str,
    account_label: str,
    observed_at: datetime,
    expected_symbol: str | None = None,
) -> AccountOpenOrderSnapshot:
    symbol = rest_require_string(item, "symbol").strip().upper()
    if expected_symbol is not None and symbol != expected_symbol:
        raise ValueError("Binance openOrders response contained another symbol")
    return AccountOpenOrderSnapshot(
        environment=environment,
        account_label=account_label,
        symbol=symbol,
        order_id=str(rest_require_int(item, "orderId")),
        client_order_id=rest_require_string(item, "clientOrderId"),
        side=rest_require_string(item, "side"),
        order_type=rest_require_string(item, "type"),
        status=rest_require_string(item, "status"),
        price=Decimal(rest_require_string(item, "price")),
        original_quantity=Decimal(rest_require_string(item, "origQty")),
        executed_quantity=Decimal(rest_require_string(item, "executedQty")),
        reduce_only=rest_require_bool(item, "reduceOnly"),
        observed_at=observed_at,
        raw_payload=json_mapping(item),
    )


def rest_require_string(data: Mapping[str, object], field_name: str) -> str:
    value = data.get(field_name)
    if not isinstance(value, str):
        raise ValueError(f"Binance response field {field_name} must be a string")
    return value


def rest_require_int(data: Mapping[str, object], field_name: str) -> int:
    value = data.get(field_name)
    if type(value) is not int:
        raise ValueError(f"Binance response field {field_name} must be an integer")
    return value


def rest_require_bool(data: Mapping[str, object], field_name: str) -> bool:
    value = data.get(field_name)
    if type(value) is not bool:
        raise ValueError(f"Binance response field {field_name} must be a boolean")
    return value


def json_mapping(value: Mapping[str, object]) -> dict[str, JsonValue]:
    return {str(key): _json_value(item) for key, item in value.items()}


def _json_value(value: object) -> JsonValue:
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    return str(value)


def rest_optional_int(value: object) -> int | None:
    if value is None:
        return None
    return int(str(value))


def rest_optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def rest_require_mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError("expected JSON object")
    return cast(dict[str, object], value)


def rest_require_sequence_of_mappings(value: object) -> tuple[dict[str, object], ...]:
    if not isinstance(value, list):
        raise ValueError("expected JSON array")
    rows: list[dict[str, object]] = []
    for item in value:
        rows.append(rest_require_mapping(item))
    return tuple(rows)


def order_snapshot_from_response(
    data: dict[str, object],
    *,
    observed_at: datetime,
    entry_leverage: int | None = None,
) -> ExchangeOrderSnapshot:
    executed_quantity = decimal_value(rest_require_string(data, "executedQty"))
    average_price = decimal_value(rest_require_string(data, "avgPrice"))
    if executed_quantity > Decimal("0") and average_price == Decimal("0"):
        cum_quote = decimal_value(rest_require_string(data, "cumQuote"))
        if cum_quote < Decimal("0"):
            raise ValueError("Binance order response cumQuote must be non-negative")
        if cum_quote > Decimal("0"):
            average_price = cum_quote / executed_quantity
    return ExchangeOrderSnapshot(
        client_order_id=rest_require_string(data, "clientOrderId"),
        exchange_order_id=str(rest_require_int(data, "orderId")),
        state=exchange_order_state(rest_require_string(data, "status")),
        observed_at=observed_at,
        executed_quantity=executed_quantity,
        average_price=average_price,
        entry_leverage=entry_leverage,
    )


def balances_from_response(
    payload: object,
    *,
    environment: str,
    account_label: str,
    observed_at: datetime,
) -> tuple[AccountBalanceSnapshot, ...]:
    return tuple(
        AccountBalanceSnapshot(
            environment=environment,
            account_label=account_label,
            asset=rest_require_string(item, "asset"),
            wallet_balance=decimal_value(rest_require_string(item, "balance")),
            available_balance=decimal_value(
                rest_require_string(item, "availableBalance")
            ),
            unrealized_pnl=decimal_value(rest_require_string(item, "crossUnPnl")),
            observed_at=observed_at,
            raw_payload=json_mapping(item),
        )
        for item in rest_require_sequence_of_mappings(payload)
    )


def positions_from_response(
    payload: object,
    *,
    environment: str,
    account_label: str,
    observed_at: datetime,
) -> tuple[AccountPositionSnapshot, ...]:
    return tuple(
        AccountPositionSnapshot(
            environment=environment,
            account_label=account_label,
            symbol=rest_require_string(item, "symbol"),
            position_side=rest_require_string(item, "positionSide"),
            position_amt=decimal_value(rest_require_string(item, "positionAmt")),
            entry_price=decimal_value(rest_require_string(item, "entryPrice")),
            mark_price=decimal_value(rest_require_string(item, "markPrice")),
            unrealized_pnl=decimal_value(rest_require_string(item, "unRealizedProfit")),
            notional=decimal_value(rest_require_string(item, "notional")),
            leverage=rest_optional_int(item.get("leverage")),
            margin_type=rest_optional_str(item.get("marginType")),
            observed_at=observed_at,
            raw_payload=json_mapping(item),
        )
        for item in rest_require_sequence_of_mappings(payload)
    )
