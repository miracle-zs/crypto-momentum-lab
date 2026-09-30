"""Pure Binance REST value and bounded userTrades parsing."""

from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

from crypto_momentum_lab.domain.account.models import (
    AccountBalanceSnapshot,
    AccountFillEvent,
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
    fallback_symbol: str,
) -> AccountFillEvent:
    symbol = str(item.get("symbol", fallback_symbol)).strip().upper()
    if symbol != fallback_symbol:
        raise ValueError("Binance userTrades response contained another symbol")
    return AccountFillEvent(
        environment=environment,
        account_label=account_label,
        symbol=symbol,
        trade_id=str(item.get("id", "")),
        order_id=str(item.get("orderId", "")),
        side=str(item.get("side", "")),
        price=decimal_value(item.get("price", "0")),
        quantity=decimal_value(item.get("qty", "0")),
        realized_pnl=decimal_value(item.get("realizedPnl", "0")),
        fee=decimal_value(item.get("commission", "0")),
        fee_asset=str(item.get("commissionAsset", "")),
        trade_at=datetime.fromtimestamp(
            int(str(item.get("time", 0))) / 1000,
            tz=UTC,
        ),
        raw_payload=json_mapping(item),
    )


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
    executed_quantity = decimal_value(data.get("executedQty", "0"))
    average_price = decimal_value(data.get("avgPrice", "0"))
    if executed_quantity > Decimal("0") and average_price <= Decimal("0"):
        cum_quote = decimal_value(data.get("cumQuote", "0"))
        if cum_quote > Decimal("0"):
            average_price = cum_quote / executed_quantity
    return ExchangeOrderSnapshot(
        client_order_id=str(data.get("clientOrderId", "")),
        exchange_order_id=str(data.get("orderId", "")),
        state=exchange_order_state(str(data.get("status", ""))),
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
            asset=str(item.get("asset", "")),
            wallet_balance=decimal_value(item.get("balance", "0")),
            available_balance=decimal_value(item.get("availableBalance", "0")),
            unrealized_pnl=decimal_value(item.get("crossUnPnl", "0")),
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
            symbol=str(item.get("symbol", "")),
            position_side=str(item.get("positionSide", "BOTH")),
            position_amt=decimal_value(item.get("positionAmt", "0")),
            entry_price=decimal_value(item.get("entryPrice", "0")),
            mark_price=decimal_value(item.get("markPrice", "0")),
            unrealized_pnl=decimal_value(item.get("unRealizedProfit", "0")),
            notional=decimal_value(item.get("notional", "0")),
            leverage=rest_optional_int(item.get("leverage")),
            margin_type=rest_optional_str(item.get("marginType")),
            observed_at=observed_at,
            raw_payload=json_mapping(item),
        )
        for item in rest_require_sequence_of_mappings(payload)
    )
