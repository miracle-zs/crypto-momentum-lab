import json
from datetime import UTC, datetime
from typing import cast
from uuid import UUID

from crypto_momentum_lab.domain.market.models import (
    CaptureRoute,
    CaptureStream,
    JsonValue,
    RawEnvelope,
)


class BinancePayloadError(ValueError):
    pass


def route_for(stream: CaptureStream) -> CaptureRoute:
    return (
        CaptureRoute.PUBLIC
        if stream is CaptureStream.BOOK_TICKER
        else CaptureRoute.MARKET
    )


def parse_binance_message(
    *,
    route: CaptureRoute,
    message: str | bytes,
    environment: str,
    connection_session_id: UUID,
    local_sequence: int,
    subscription_generation: int,
    received_at: datetime,
    received_monotonic_ns: int,
) -> RawEnvelope:
    decoded = json.loads(message)
    if not isinstance(decoded, dict):
        raise BinancePayloadError("message must be an object")
    stream_name = decoded.get("stream")
    payload = decoded.get("data")
    if not isinstance(stream_name, str) or not isinstance(payload, dict):
        raise BinancePayloadError("combined stream envelope is invalid")

    stream = _stream_from_name(stream_name)
    if route_for(stream) is not route:
        raise BinancePayloadError("stream arrived on unexpected route")

    symbol = _symbol(payload)
    event_ms = _event_time_ms(payload)
    exchange_sequence = _exchange_sequence(stream, payload)
    raw_payload = cast(dict[str, JsonValue], payload)
    return RawEnvelope(
        schema_version=1,
        exchange="binance-usdm",
        environment=environment,
        route=route,
        stream=stream,
        symbol=symbol,
        exchange_event_at=None if event_ms is None else _utc_from_ms(event_ms),
        received_at=received_at,
        received_monotonic_ns=received_monotonic_ns,
        connection_session_id=connection_session_id,
        local_sequence=local_sequence,
        exchange_sequence=exchange_sequence,
        subscription_generation=subscription_generation,
        raw_payload=raw_payload,
    )


def _stream_from_name(stream_name: str) -> CaptureStream:
    _, separator, suffix = stream_name.partition("@")
    if not separator:
        raise BinancePayloadError("stream name is invalid")
    try:
        return CaptureStream(suffix)
    except ValueError as exc:
        raise BinancePayloadError(f"unsupported stream: {suffix}") from exc


def _symbol(payload: dict[object, object]) -> str:
    direct = payload.get("s")
    if isinstance(direct, str):
        return direct
    order = payload.get("o")
    if isinstance(order, dict):
        nested = order.get("s")
        if isinstance(nested, str):
            return nested
    raise BinancePayloadError("payload symbol is missing")


def _event_time_ms(payload: dict[object, object]) -> int | None:
    value = payload.get("E")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    order = payload.get("o")
    if isinstance(order, dict):
        nested = order.get("T")
        if isinstance(nested, int):
            return nested
    return None


def _exchange_sequence(
    stream: CaptureStream,
    payload: dict[object, object],
) -> str | None:
    if stream is CaptureStream.AGG_TRADE:
        return _required_sequence(payload, "a")
    if stream is CaptureStream.BOOK_TICKER:
        return _required_sequence(payload, "u")
    if stream is CaptureStream.KLINE_1M:
        kline = payload.get("k")
        if not isinstance(kline, dict):
            raise BinancePayloadError("kline payload is missing")
        return _required_sequence(kline, "t")
    return None


def _required_sequence(payload: dict[object, object], key: str) -> str:
    value = payload.get(key)
    if isinstance(value, int | str):
        return str(value)
    raise BinancePayloadError(f"payload sequence {key} is missing")


def _utc_from_ms(milliseconds: int) -> datetime:
    return datetime.fromtimestamp(milliseconds / 1000, tz=UTC)
