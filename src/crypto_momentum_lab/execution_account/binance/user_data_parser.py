"""Strict Binance user-data parsing independent of WebSocket transport."""

import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, datetime

from crypto_momentum_lab.domain.market.models import JsonValue
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
