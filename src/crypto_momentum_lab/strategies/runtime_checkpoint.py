from collections import deque
from datetime import datetime
from decimal import Decimal

from crypto_momentum_lab.domain.market.models import JsonValue, MarketState15s


def market_state_payload(state: MarketState15s) -> dict[str, JsonValue]:
    return {
        "schema_version": state.schema_version,
        "exchange": state.exchange,
        "environment": state.environment,
        "symbol": state.symbol,
        "bucket_start": state.bucket_start.isoformat(),
        "bucket_end": state.bucket_end.isoformat(),
        "open_price": _decimal_payload(state.open_price),
        "high_price": _decimal_payload(state.high_price),
        "low_price": _decimal_payload(state.low_price),
        "close_price": _decimal_payload(state.close_price),
        "trade_count": state.trade_count,
        "trade_notional": str(state.trade_notional),
        "aggressive_buy_notional": str(state.aggressive_buy_notional),
        "aggressive_sell_notional": str(state.aggressive_sell_notional),
        "last_bid_price": _decimal_payload(state.last_bid_price),
        "last_ask_price": _decimal_payload(state.last_ask_price),
        "spread": _decimal_payload(state.spread),
        "midpoint": _decimal_payload(state.midpoint),
        "liquidation_count": state.liquidation_count,
        "liquidation_notional": str(state.liquidation_notional),
        "mark_price": _decimal_payload(state.mark_price),
        "closed_kline_count": state.closed_kline_count,
        "closed_kline_1m_open_time": _datetime_payload(state.closed_kline_1m_open_time),
        "closed_kline_1m_close_time": _datetime_payload(
            state.closed_kline_1m_close_time
        ),
        "closed_kline_1m_open_price": _decimal_payload(
            state.closed_kline_1m_open_price
        ),
        "closed_kline_1m_close_price": _decimal_payload(
            state.closed_kline_1m_close_price
        ),
        "source_event_count": state.source_event_count,
        "first_received_at": _datetime_payload(state.first_received_at),
        "last_received_at": _datetime_payload(state.last_received_at),
        "data_complete": state.data_complete,
        "missing_agg_trade_count": state.missing_agg_trade_count,
    }


def restore_market_state_buffers(
    payload: dict[str, JsonValue],
    *,
    maxlen: int,
) -> dict[str, deque[MarketState15s]]:
    if maxlen <= 0:
        raise ValueError("maxlen must be positive")

    restored: dict[str, deque[MarketState15s]] = {}
    for symbol, raw_states in payload.items():
        if not isinstance(raw_states, list):
            continue
        states: deque[MarketState15s] = deque(maxlen=maxlen)
        for raw_state in raw_states:
            if not isinstance(raw_state, dict):
                continue
            state = market_state_from_payload(raw_state)
            if state.symbol != symbol:
                continue
            states.append(state)
        if states:
            restored[symbol] = states
    return restored


def market_state_from_payload(
    payload: dict[str, JsonValue],
) -> MarketState15s:
    return MarketState15s(
        schema_version=int(str(payload["schema_version"])),
        exchange=str(payload["exchange"]),
        environment=str(payload["environment"]),
        symbol=str(payload["symbol"]),
        bucket_start=datetime.fromisoformat(str(payload["bucket_start"])),
        bucket_end=datetime.fromisoformat(str(payload["bucket_end"])),
        open_price=_payload_decimal(payload["open_price"]),
        high_price=_payload_decimal(payload["high_price"]),
        low_price=_payload_decimal(payload["low_price"]),
        close_price=_payload_decimal(payload["close_price"]),
        trade_count=int(str(payload["trade_count"])),
        trade_notional=_required_decimal(payload["trade_notional"]),
        aggressive_buy_notional=_required_decimal(payload["aggressive_buy_notional"]),
        aggressive_sell_notional=_required_decimal(payload["aggressive_sell_notional"]),
        last_bid_price=_payload_decimal(payload["last_bid_price"]),
        last_ask_price=_payload_decimal(payload["last_ask_price"]),
        spread=_payload_decimal(payload["spread"]),
        midpoint=_payload_decimal(payload["midpoint"]),
        liquidation_count=int(str(payload["liquidation_count"])),
        liquidation_notional=_required_decimal(payload["liquidation_notional"]),
        mark_price=_payload_decimal(payload["mark_price"]),
        closed_kline_count=int(str(payload["closed_kline_count"])),
        closed_kline_1m_open_time=_payload_datetime(
            payload["closed_kline_1m_open_time"]
        ),
        closed_kline_1m_close_time=_payload_datetime(
            payload["closed_kline_1m_close_time"]
        ),
        closed_kline_1m_open_price=_payload_decimal(
            payload["closed_kline_1m_open_price"]
        ),
        closed_kline_1m_close_price=_payload_decimal(
            payload["closed_kline_1m_close_price"]
        ),
        source_event_count=int(str(payload["source_event_count"])),
        first_received_at=_payload_datetime(payload["first_received_at"]),
        last_received_at=_payload_datetime(payload["last_received_at"]),
        data_complete=_payload_bool(payload, "data_complete"),
        missing_agg_trade_count=int(str(payload["missing_agg_trade_count"])),
    )


def _decimal_payload(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _datetime_payload(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def _payload_decimal(value: JsonValue) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


def _required_decimal(value: JsonValue) -> Decimal:
    parsed = _payload_decimal(value)
    if parsed is None:
        return Decimal("0")
    return parsed


def _payload_datetime(value: JsonValue) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(str(value))


def _payload_bool(payload: dict[str, JsonValue], key: str) -> bool:
    value = payload[key]
    if not isinstance(value, bool):
        raise ValueError(f"{key} must be a boolean")
    return value
