"""Strict Binance user-data parsing independent of WebSocket transport."""

import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderSnapshot,
    ExchangeOrderState,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.execution_account.binance.order_status import (
    exchange_order_state,
)
from crypto_momentum_lab.execution_account.binance.user_data_models import (
    BinanceUserDataEvent,
)


class BinancePayloadError(ValueError):
    """A Binance user-data payload that cannot be safely interpreted."""


def parse_user_data_event(
    message: str | bytes | Mapping[str, object],
    *,
    received_at: datetime | None = None,
) -> BinanceUserDataEvent:
    """Parse a direct or combined Binance account-event message.

    User data streams normally send the event object directly. Accepting the
    combined-stream wrapper here is harmless and makes the parser resilient to
    an accidentally configured ``/stream`` endpoint.
    """
    payload = _decode_mapping(message)
    if "data" in payload and "stream" in payload:
        payload = _require_mapping(payload["data"])
    normalized = _json_mapping(payload)
    event_type = normalized.get("e")
    if not isinstance(event_type, str) or not event_type.strip():
        raise BinancePayloadError("user data event is missing string field e")
    if event_type == "ACCOUNT_UPDATE" and not isinstance(
        normalized.get("a"),
        dict,
    ):
        raise BinancePayloadError("ACCOUNT_UPDATE is missing object field a")
    if event_type == "ORDER_TRADE_UPDATE" and not isinstance(
        normalized.get("o"),
        dict,
    ):
        raise BinancePayloadError("ORDER_TRADE_UPDATE is missing object field o")
    event_timestamp = normalized.get("E")
    if isinstance(event_timestamp, bool) or not isinstance(
        event_timestamp,
        int | float | str,
    ):
        raise BinancePayloadError("user data event is missing numeric field E")
    event_at = _timestamp_from_millis(event_timestamp, "E")
    exchange_timestamp = normalized.get("T")
    if exchange_timestamp is None and event_type == "ORDER_TRADE_UPDATE":
        order = normalized.get("o")
        if isinstance(order, dict):
            exchange_timestamp = order.get("T")
    exchange_event_at = (
        event_at
        if exchange_timestamp is None
        else _timestamp_from_millis(exchange_timestamp, "T")
    )
    exchange_update_id = _optional_update_id(normalized.get("u"), "u")
    exchange_previous_update_id = _optional_update_id(
        normalized.get("pu"),
        "pu",
    )
    if event_type == "ORDER_TRADE_UPDATE":
        order = normalized.get("o")
        if isinstance(order, dict):
            if exchange_update_id is None:
                exchange_update_id = _optional_update_id(order.get("u"), "o.u")
            if exchange_previous_update_id is None:
                exchange_previous_update_id = _optional_update_id(
                    order.get("pu"),
                    "o.pu",
                )
    resolved_received_at = received_at or datetime.now(tz=UTC)
    if resolved_received_at.tzinfo is None or resolved_received_at.utcoffset() is None:
        raise ValueError("received_at must be timezone-aware")
    event_id = hashlib.sha256(
        json.dumps(
            normalized,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    ).hexdigest()
    return BinanceUserDataEvent(
        event_type=event_type,
        event_at=event_at,
        received_at=resolved_received_at,
        payload=normalized,
        event_id=event_id,
        exchange_event_at=exchange_event_at,
        exchange_update_id=exchange_update_id,
        exchange_previous_update_id=exchange_previous_update_id,
    )


def _decode_mapping(message: str | bytes | Mapping[str, object]) -> dict[str, object]:
    if isinstance(message, bytes):
        try:
            decoded: object = json.loads(message.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise BinancePayloadError("user data message is not valid JSON") from error
    elif isinstance(message, str):
        try:
            decoded = json.loads(message)
        except json.JSONDecodeError as error:
            raise BinancePayloadError("user data message is not valid JSON") from error
    else:
        decoded = message
    return _require_mapping(decoded)


def _require_mapping(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise BinancePayloadError("expected JSON object")
    return {str(key): item for key, item in value.items()}


def _json_mapping(value: Mapping[str, object]) -> dict[str, JsonValue]:
    return {str(key): _json_value(item) for key, item in value.items()}


def _json_value(value: object) -> JsonValue:
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_value(item) for item in value]
    raise BinancePayloadError(f"unsupported JSON value type: {type(value).__name__}")


def _timestamp_from_millis(value: object, field_name: str) -> datetime:
    if isinstance(value, bool) or not isinstance(value, int | float | str):
        raise BinancePayloadError(
            f"user data event is missing numeric field {field_name}"
        )
    try:
        return datetime.fromtimestamp(float(str(value)) / 1000, tz=UTC)
    except (TypeError, ValueError, OverflowError, OSError) as error:
        raise BinancePayloadError(
            f"user data event has invalid timestamp {field_name}"
        ) from error


def _optional_update_id(value: object, field_name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int | float | str):
        raise BinancePayloadError(f"user data event field {field_name} is not numeric")
    try:
        parsed = int(str(value))
    except (TypeError, ValueError, OverflowError) as error:
        raise BinancePayloadError(
            f"user data event field {field_name} is not an integer"
        ) from error
    if parsed < 0:
        raise BinancePayloadError(
            f"user data event field {field_name} must be non-negative"
        )
    return parsed


def order_snapshot_from_update(
    order: Mapping[str, object],
    plan: OrderExecutionPlan,
) -> ExchangeOrderSnapshot | None:
    """Validate a complete WS fact; incomplete facts require a recovery read."""
    required = {"c", "s", "S", "o", "q", "p", "R", "ps", "i", "X", "z", "ap", "T"}
    if not required.issubset(order):
        return None
    try:
        quantity = Decimal(str(order["q"]))
        price = Decimal(str(order["p"]))
        executed = Decimal(str(order["z"]))
        average = Decimal(str(order["ap"]))
        state = exchange_order_state(str(order["X"]))
        if not all(value.is_finite() for value in (quantity, price, executed, average)):
            return None
        if (
            quantity <= 0
            or price < 0
            or executed < 0
            or executed > quantity
            or average < 0
            or (executed > 0 and average <= 0)
        ):
            return None
        if not isinstance(order["R"], bool):
            return None
        if (
            isinstance(order["i"], bool)
            or not str(order["i"]).isdigit()
            or int(str(order["i"])) <= 0
        ):
            return None
        if (
            isinstance(order["T"], bool)
            or not str(order["T"]).isdigit()
            or int(str(order["T"])) <= 0
        ):
            return None
        observed_at = datetime.fromtimestamp(int(str(order["T"])) / 1000, tz=UTC)
    except (ValueError, InvalidOperation, OverflowError, OSError):
        return None
    if (
        str(order["c"]) != plan.client_order_id
        or str(order["s"]) != plan.symbol
        or str(order["S"]) != plan.side
        or str(order["o"]) != plan.order_type
        or str(order["ps"]) != plan.position_side.value
        or order["R"] != plan.reduce_only
        or quantity != plan.quantity
        or (plan.price is not None and price != plan.price)
    ):
        raise ValueError("WS order update conflicts with its durable identity")
    if (
        state is ExchangeOrderState.FILLED
        and executed != quantity
        or state is ExchangeOrderState.ACKNOWLEDGED
        and executed != 0
        or state is ExchangeOrderState.PARTIALLY_FILLED
        and not 0 < executed < quantity
    ):
        return None
    return ExchangeOrderSnapshot(
        client_order_id=plan.client_order_id,
        exchange_order_id=str(order["i"]),
        state=state,
        observed_at=observed_at,
        executed_quantity=executed,
        average_price=average,
    )
