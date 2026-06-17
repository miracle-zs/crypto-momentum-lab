from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import TypedDict
from uuid import UUID

from crypto_momentum_lab.domain.market.models import (
    AggressorSide,
    CaptureStream,
    JsonValue,
    NormalizedAggTrade,
    NormalizedBookTicker,
    NormalizedKline1m,
    NormalizedLiquidation,
    NormalizedMarketEvent,
    NormalizedMarkPrice,
    OrderSide,
    RawEnvelope,
)


class BinanceNormalizationError(ValueError):
    pass


class _SourceKwargs(TypedDict):
    schema_version: int
    exchange: str
    environment: str
    symbol: str
    event_at: datetime
    received_at: datetime
    source_connection_session_id: UUID
    source_local_sequence: int
    source_stream: CaptureStream


def normalize_binance_envelope(envelope: RawEnvelope) -> NormalizedMarketEvent:
    payload = envelope.raw_payload
    if not isinstance(payload, dict):
        raise BinanceNormalizationError("raw_payload must be an object")

    if envelope.stream is CaptureStream.AGG_TRADE:
        return _normalize_agg_trade(envelope, payload)
    if envelope.stream is CaptureStream.BOOK_TICKER:
        return _normalize_book_ticker(envelope, payload)
    if envelope.stream is CaptureStream.MARK_PRICE:
        return _normalize_mark_price(envelope, payload)
    if envelope.stream is CaptureStream.KLINE_1M:
        return _normalize_kline_1m(envelope, payload)
    if envelope.stream is CaptureStream.FORCE_ORDER:
        return _normalize_force_order(envelope, payload)
    raise BinanceNormalizationError(f"unsupported stream: {envelope.stream}")


def _normalize_agg_trade(
    envelope: RawEnvelope,
    payload: Mapping[str, JsonValue],
) -> NormalizedAggTrade:
    price = _required_decimal(payload, "p")
    quantity = _required_decimal(payload, "q")
    buyer_is_maker = _required_bool(payload, "m")
    return NormalizedAggTrade(
        **_source_kwargs(envelope),
        trade_id=_required_stringish(payload, "a"),
        price=price,
        quantity=quantity,
        notional=price * quantity,
        aggressor_side=(
            AggressorSide.SELL if buyer_is_maker else AggressorSide.BUY
        ),
    )


def _normalize_book_ticker(
    envelope: RawEnvelope,
    payload: Mapping[str, JsonValue],
) -> NormalizedBookTicker:
    return NormalizedBookTicker(
        **_source_kwargs(envelope),
        update_id=_required_stringish(payload, "u"),
        bid_price=_required_decimal(payload, "b"),
        bid_quantity=_required_decimal(payload, "B"),
        ask_price=_required_decimal(payload, "a"),
        ask_quantity=_required_decimal(payload, "A"),
    )


def _normalize_mark_price(
    envelope: RawEnvelope,
    payload: Mapping[str, JsonValue],
) -> NormalizedMarkPrice:
    return NormalizedMarkPrice(
        **_source_kwargs(envelope),
        mark_price=_required_decimal(payload, "p"),
        index_price=_optional_decimal(payload, "i"),
        estimated_settle_price=_optional_decimal(payload, "P"),
        funding_rate=_optional_decimal(payload, "r"),
        next_funding_at=_optional_ms(payload, "T"),
    )


def _normalize_kline_1m(
    envelope: RawEnvelope,
    payload: Mapping[str, JsonValue],
) -> NormalizedKline1m:
    kline = payload.get("k")
    if not isinstance(kline, dict):
        raise BinanceNormalizationError("k must be an object")
    kline_payload = kline
    return NormalizedKline1m(
        **_source_kwargs(envelope),
        open_time=_required_ms(kline_payload, "t"),
        close_time=_required_ms(kline_payload, "T"),
        open_price=_required_decimal(kline_payload, "o"),
        high_price=_required_decimal(kline_payload, "h"),
        low_price=_required_decimal(kline_payload, "l"),
        close_price=_required_decimal(kline_payload, "c"),
        volume=_required_decimal(kline_payload, "v"),
        quote_volume=_required_decimal(kline_payload, "q"),
        trade_count=_required_int(kline_payload, "n"),
        closed=_required_bool(kline_payload, "x"),
    )


def _normalize_force_order(
    envelope: RawEnvelope,
    payload: Mapping[str, JsonValue],
) -> NormalizedLiquidation:
    order = payload.get("o")
    if not isinstance(order, dict):
        raise BinanceNormalizationError("o must be an object")
    order_payload = order
    price = _required_decimal(order_payload, "p")
    average_price = _required_decimal(order_payload, "ap")
    quantity = _required_decimal(order_payload, "q")
    notional_price = average_price if average_price > 0 else price
    return NormalizedLiquidation(
        **_source_kwargs(envelope),
        order_side=_order_side(_required_str(order_payload, "S")),
        price=price,
        average_price=average_price,
        quantity=quantity,
        notional=notional_price * quantity,
        trade_time=_optional_ms(order_payload, "T"),
    )


def _source_kwargs(envelope: RawEnvelope) -> _SourceKwargs:
    if envelope.symbol is None:
        raise BinanceNormalizationError("symbol is required")
    return {
        "schema_version": 1,
        "exchange": envelope.exchange,
        "environment": envelope.environment,
        "symbol": envelope.symbol,
        "event_at": envelope.exchange_event_at or envelope.received_at,
        "received_at": envelope.received_at,
        "source_connection_session_id": envelope.connection_session_id,
        "source_local_sequence": envelope.local_sequence,
        "source_stream": envelope.stream,
    }


def _required_str(payload: Mapping[str, JsonValue], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str):
        raise BinanceNormalizationError(f"{key} must be a string")
    return value


def _required_stringish(payload: Mapping[str, JsonValue], key: str) -> str:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int | str):
        raise BinanceNormalizationError(f"{key} must be an integer or string")
    return str(value)


def _required_int(payload: Mapping[str, JsonValue], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise BinanceNormalizationError(f"{key} must be an integer")
    return value


def _required_bool(payload: Mapping[str, JsonValue], key: str) -> bool:
    value = payload.get(key)
    if not isinstance(value, bool):
        raise BinanceNormalizationError(f"{key} must be a boolean")
    return value


def _required_decimal(payload: Mapping[str, JsonValue], key: str) -> Decimal:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int | float | str):
        raise BinanceNormalizationError(f"{key} must be numeric")
    try:
        return Decimal(str(value))
    except InvalidOperation as exc:
        raise BinanceNormalizationError(f"{key} must be numeric") from exc


def _optional_decimal(
    payload: Mapping[str, JsonValue],
    key: str,
) -> Decimal | None:
    value = payload.get(key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int | float | str):
        raise BinanceNormalizationError(f"{key} must be numeric or null")
    try:
        return Decimal(str(value))
    except InvalidOperation as exc:
        raise BinanceNormalizationError(f"{key} must be numeric or null") from exc


def _required_ms(payload: Mapping[str, JsonValue], key: str) -> datetime:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise BinanceNormalizationError(f"{key} must be a millisecond timestamp")
    return datetime.fromtimestamp(value / 1000, tz=UTC)


def _optional_ms(payload: Mapping[str, JsonValue], key: str) -> datetime | None:
    value = payload.get(key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise BinanceNormalizationError(
            f"{key} must be a millisecond timestamp or null"
        )
    return datetime.fromtimestamp(value / 1000, tz=UTC)


def _order_side(value: str) -> OrderSide:
    try:
        return OrderSide(value.lower())
    except ValueError as exc:
        raise BinanceNormalizationError(f"unsupported order side: {value}") from exc
