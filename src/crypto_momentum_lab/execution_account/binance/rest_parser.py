"""Pure Binance REST value and bounded userTrades parsing."""

from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

from crypto_momentum_lab.domain.account.models import AccountFillEvent
from crypto_momentum_lab.domain.market.models import JsonValue


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
