from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID

from crypto_momentum_lab.domain.market.models import (
    AggressorSide,
    CaptureStream,
    MarketState15s,
    NormalizedAggTrade,
)
from crypto_momentum_lab.persistence.parquet.datasets import (
    market_event_row,
    market_state_15s_row,
    partition_for_market_event,
    partition_for_market_state,
)


def test_market_event_row_flattens_aggregate_trade() -> None:
    event = NormalizedAggTrade(
        schema_version=1,
        exchange="binance-usdm",
        environment="research",
        symbol="BTCUSDT",
        event_at=datetime(2026, 6, 15, 2, 0, tzinfo=UTC),
        received_at=datetime(2026, 6, 15, 2, 0, 1, tzinfo=UTC),
        source_connection_session_id=UUID(int=1),
        source_local_sequence=7,
        source_stream=CaptureStream.AGG_TRADE,
        trade_id="42",
        price=Decimal("100.25"),
        quantity=Decimal("0.5"),
        notional=Decimal("50.125"),
        aggressor_side=AggressorSide.BUY,
    )

    row = market_event_row(event)

    assert row["event_type"] == "agg_trade"
    assert row["source_stream"] == "aggTrade"
    assert row["source_connection_session_id"] == str(UUID(int=1))
    assert row["price"] == "100.25"
    assert row["quantity"] == "0.5"
    assert row["notional"] == "50.125"
    assert row["aggressor_side"] == "buy"
    assert partition_for_market_event(event) == Path(
        "market_events/date=2026-06-15/stream=aggTrade/symbol=BTCUSDT"
    )


def test_market_state_15s_row_preserves_decimal_strings() -> None:
    state = MarketState15s(
        schema_version=1,
        exchange="binance-usdm",
        environment="research",
        symbol="BTCUSDT",
        bucket_start=datetime(2026, 6, 15, 2, 0, tzinfo=UTC),
        bucket_end=datetime(2026, 6, 15, 2, 0, 15, tzinfo=UTC),
        open_price=Decimal("100"),
        high_price=Decimal("101"),
        low_price=Decimal("99"),
        close_price=Decimal("100.5"),
        trade_count=2,
        trade_notional=Decimal("200.5"),
        aggressive_buy_notional=Decimal("120.25"),
        aggressive_sell_notional=Decimal("80.25"),
        last_bid_price=Decimal("100.4"),
        last_ask_price=Decimal("100.6"),
        spread=Decimal("0.2"),
        midpoint=Decimal("100.5"),
        liquidation_count=1,
        liquidation_notional=Decimal("50.5"),
        mark_price=Decimal("100.55"),
        closed_kline_count=1,
        source_event_count=5,
        first_received_at=datetime(2026, 6, 15, 2, 0, 1, tzinfo=UTC),
        last_received_at=datetime(2026, 6, 15, 2, 0, 14, tzinfo=UTC),
    )

    row = market_state_15s_row(state)

    assert row["bucket_start"] == datetime(2026, 6, 15, 2, 0, tzinfo=UTC)
    assert row["open_price"] == "100"
    assert row["trade_notional"] == "200.5"
    assert row["spread"] == "0.2"
    assert partition_for_market_state(state) == Path(
        "market_states_15s/date=2026-06-15/symbol=BTCUSDT"
    )
