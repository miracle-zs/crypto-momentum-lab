from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.strategies.runtime_checkpoint import (
    market_state_from_payload,
    market_state_payload,
    restore_market_state_buffers,
)


def test_market_state_checkpoint_round_trips_quality_and_kline_fields() -> None:
    state = _state(
        closed_kline=True,
        data_complete=False,
        missing_agg_trade_count=7,
    )

    payload = market_state_payload(state)
    restored = market_state_from_payload(payload)
    buffered = restore_market_state_buffers(
        {state.symbol: [payload]},
        maxlen=2,
    )[state.symbol][0]

    assert restored == state
    assert buffered == state


def test_market_state_checkpoint_keeps_legacy_defaults() -> None:
    payload = market_state_payload(_state())
    for key in (
        "closed_kline_1m_open_time",
        "closed_kline_1m_close_time",
        "closed_kline_1m_open_price",
        "closed_kline_1m_close_price",
        "data_complete",
        "missing_agg_trade_count",
    ):
        del payload[key]

    restored = market_state_from_payload(payload)

    assert restored.closed_kline_1m_open_time is None
    assert restored.closed_kline_1m_close_time is None
    assert restored.closed_kline_1m_open_price is None
    assert restored.closed_kline_1m_close_price is None
    assert restored.data_complete is True
    assert restored.missing_agg_trade_count == 0


def _state(
    *,
    closed_kline: bool = False,
    data_complete: bool = True,
    missing_agg_trade_count: int = 0,
) -> MarketState15s:
    bucket_start = datetime(2026, 9, 10, 0, 0, tzinfo=UTC)
    return MarketState15s(
        schema_version=1,
        exchange="binance",
        environment="test",
        symbol="BTCUSDT",
        bucket_start=bucket_start,
        bucket_end=bucket_start + timedelta(seconds=15),
        open_price=Decimal("100"),
        high_price=Decimal("101"),
        low_price=Decimal("99"),
        close_price=Decimal("100.5"),
        trade_count=12,
        trade_notional=Decimal("1200"),
        aggressive_buy_notional=Decimal("700"),
        aggressive_sell_notional=Decimal("500"),
        last_bid_price=Decimal("100.4"),
        last_ask_price=Decimal("100.6"),
        spread=Decimal("0.2"),
        midpoint=Decimal("100.5"),
        liquidation_count=2,
        liquidation_notional=Decimal("300"),
        mark_price=Decimal("100.55"),
        closed_kline_count=1 if closed_kline else 0,
        source_event_count=20,
        first_received_at=bucket_start,
        last_received_at=bucket_start + timedelta(seconds=14),
        closed_kline_1m_open_time=(
            bucket_start - timedelta(seconds=45) if closed_kline else None
        ),
        closed_kline_1m_close_time=(
            bucket_start - timedelta(seconds=15) if closed_kline else None
        ),
        closed_kline_1m_open_price=Decimal("98.5") if closed_kline else None,
        closed_kline_1m_close_price=Decimal("100.25") if closed_kline else None,
        data_complete=data_complete,
        missing_agg_trade_count=missing_agg_trade_count,
    )
