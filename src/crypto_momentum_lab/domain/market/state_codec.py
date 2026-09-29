"""JSON-compatible serialization for immutable market-state domain values."""

from collections.abc import Mapping
from dataclasses import fields
from datetime import datetime
from decimal import Decimal

from crypto_momentum_lab.domain.market.models import MarketState15s


class MarketStateCodecError(ValueError):
    """A market-state payload does not satisfy the domain data contract."""


def market_state_to_payload(state: MarketState15s) -> dict[str, object]:
    """Encode one market state using JSON primitives only."""
    return {
        item.name: _encode_value(getattr(state, item.name))
        for item in fields(MarketState15s)
    }


def market_state_from_payload(
    payload: Mapping[str, object],
) -> MarketState15s:
    """Decode a previously serialized market state."""
    try:
        return MarketState15s(
            schema_version=_require_int(payload, "schema_version"),
            exchange=_require_string(payload, "exchange"),
            environment=_require_string(payload, "environment"),
            symbol=_require_string(payload, "symbol"),
            bucket_start=_require_datetime(payload, "bucket_start"),
            bucket_end=_require_datetime(payload, "bucket_end"),
            open_price=_optional_decimal(payload, "open_price"),
            high_price=_optional_decimal(payload, "high_price"),
            low_price=_optional_decimal(payload, "low_price"),
            close_price=_optional_decimal(payload, "close_price"),
            trade_count=_require_int(payload, "trade_count"),
            trade_notional=_require_decimal(payload, "trade_notional"),
            aggressive_buy_notional=_require_decimal(
                payload, "aggressive_buy_notional"
            ),
            aggressive_sell_notional=_require_decimal(
                payload, "aggressive_sell_notional"
            ),
            last_bid_price=_optional_decimal(payload, "last_bid_price"),
            last_ask_price=_optional_decimal(payload, "last_ask_price"),
            spread=_optional_decimal(payload, "spread"),
            midpoint=_optional_decimal(payload, "midpoint"),
            liquidation_count=_require_int(payload, "liquidation_count"),
            liquidation_notional=_require_decimal(
                payload, "liquidation_notional"
            ),
            mark_price=_optional_decimal(payload, "mark_price"),
            closed_kline_count=_require_int(payload, "closed_kline_count"),
            source_event_count=_require_int(payload, "source_event_count"),
            first_received_at=_optional_datetime(payload, "first_received_at"),
            last_received_at=_optional_datetime(payload, "last_received_at"),
            closed_kline_1m_open_time=_optional_datetime(
                payload, "closed_kline_1m_open_time"
            ),
            closed_kline_1m_close_time=_optional_datetime(
                payload, "closed_kline_1m_close_time"
            ),
            closed_kline_1m_open_price=_optional_decimal(
                payload, "closed_kline_1m_open_price"
            ),
            closed_kline_1m_close_price=_optional_decimal(
                payload, "closed_kline_1m_close_price"
            ),
            data_complete=_optional_bool_default(
                payload,
                "data_complete",
                default=True,
            ),
            missing_agg_trade_count=(
                _optional_int(payload, "missing_agg_trade_count") or 0
            ),
            is_backfill=_optional_bool_default(
                payload,
                "is_backfill",
                default=False,
            ),
        )
    except ValueError as error:
        if isinstance(error, MarketStateCodecError):
            raise
        raise MarketStateCodecError(str(error)) from error


def _encode_value(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def _require_string(payload: Mapping[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value.strip():
        raise MarketStateCodecError(f"{name} must be a non-empty string")
    return value


def _require_int(payload: Mapping[str, object], name: str) -> int:
    value = payload.get(name)
    if not isinstance(value, int) or isinstance(value, bool):
        raise MarketStateCodecError(f"{name} must be an integer")
    return value


def _optional_int(payload: Mapping[str, object], name: str) -> int | None:
    value = payload.get(name)
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise MarketStateCodecError(f"{name} must be an integer")
    return value


def _optional_bool_default(
    payload: Mapping[str, object],
    name: str,
    *,
    default: bool,
) -> bool:
    if name not in payload:
        return default
    value = payload[name]
    if not isinstance(value, bool):
        raise MarketStateCodecError(f"{name} must be a boolean")
    return value


def _require_datetime(payload: Mapping[str, object], name: str) -> datetime:
    value = payload.get(name)
    if not isinstance(value, str):
        raise MarketStateCodecError(f"{name} must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as cause:
        raise MarketStateCodecError(f"{name} is not a valid timestamp") from cause
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise MarketStateCodecError(f"{name} must include a timezone")
    return parsed


def _optional_datetime(
    payload: Mapping[str, object],
    name: str,
) -> datetime | None:
    if payload.get(name) is None:
        return None
    return _require_datetime(payload, name)


def _require_decimal(payload: Mapping[str, object], name: str) -> Decimal:
    value = payload.get(name)
    if not isinstance(value, str | int | float) or isinstance(value, bool):
        raise MarketStateCodecError(f"{name} must be numeric")
    try:
        return Decimal(str(value))
    except Exception as cause:
        raise MarketStateCodecError(f"{name} is not numeric") from cause


def _optional_decimal(
    payload: Mapping[str, object],
    name: str,
) -> Decimal | None:
    if payload.get(name) is None:
        return None
    return _require_decimal(payload, name)
