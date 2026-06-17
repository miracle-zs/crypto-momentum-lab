from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from crypto_momentum_lab.domain.market.models import (
    AggressorSide,
    CaptureStream,
    NormalizedAggTrade,
    NormalizedBookTicker,
    NormalizedKline1m,
    NormalizedLiquidation,
    NormalizedMarkPrice,
    OrderSide,
)
from crypto_momentum_lab.market_data.aggregation.state_15s import (
    aggregate_market_states_15s,
    bucket_start_15s,
)


def test_bucket_start_15s_aligns_utc_boundaries() -> None:
    assert bucket_start_15s(datetime(2026, 6, 15, 2, 0, 0, tzinfo=UTC)) == (
        datetime(2026, 6, 15, 2, 0, 0, tzinfo=UTC)
    )
    assert bucket_start_15s(
        datetime(2026, 6, 15, 2, 0, 14, 999000, tzinfo=UTC)
    ) == datetime(2026, 6, 15, 2, 0, 0, tzinfo=UTC)
    assert bucket_start_15s(datetime(2026, 6, 15, 2, 0, 15, tzinfo=UTC)) == (
        datetime(2026, 6, 15, 2, 0, 15, tzinfo=UTC)
    )
    assert bucket_start_15s(datetime(2026, 6, 15, 2, 0, 30, tzinfo=UTC)) == (
        datetime(2026, 6, 15, 2, 0, 30, tzinfo=UTC)
    )


def test_aggregate_trades_calculates_ohlc_and_aggressive_notional() -> None:
    states = aggregate_market_states_15s(
        (
            _trade(
                sequence=1,
                price=Decimal("100"),
                quantity=Decimal("1"),
                side=AggressorSide.BUY,
            ),
            _trade(
                sequence=2,
                price=Decimal("101"),
                quantity=Decimal("2"),
                side=AggressorSide.SELL,
            ),
        )
    )

    assert len(states) == 1
    state = states[0]
    assert state.bucket_start == datetime(2026, 6, 15, 2, 0, tzinfo=UTC)
    assert state.bucket_end == datetime(2026, 6, 15, 2, 0, 15, tzinfo=UTC)
    assert state.open_price == Decimal("100")
    assert state.high_price == Decimal("101")
    assert state.low_price == Decimal("100")
    assert state.close_price == Decimal("101")
    assert state.trade_count == 2
    assert state.trade_notional == Decimal("302")
    assert state.aggressive_buy_notional == Decimal("100")
    assert state.aggressive_sell_notional == Decimal("202")
    assert state.source_event_count == 2


def test_book_ticker_sets_last_spread_and_midpoint() -> None:
    states = aggregate_market_states_15s(
        (
            NormalizedBookTicker(
                **_source(CaptureStream.BOOK_TICKER),
                update_id="7",
                bid_price=Decimal("100"),
                bid_quantity=Decimal("1.5"),
                ask_price=Decimal("101"),
                ask_quantity=Decimal("2.5"),
            ),
        )
    )

    state = states[0]
    assert state.last_bid_price == Decimal("100")
    assert state.last_ask_price == Decimal("101")
    assert state.spread == Decimal("1")
    assert state.midpoint == Decimal("100.5")


def test_liquidation_mark_price_and_closed_kline_update_bucket() -> None:
    states = aggregate_market_states_15s(
        (
            NormalizedLiquidation(
                **_source(CaptureStream.FORCE_ORDER, sequence=1),
                order_side=OrderSide.SELL,
                price=Decimal("100"),
                average_price=Decimal("100.5"),
                quantity=Decimal("0.4"),
                notional=Decimal("40.20"),
                trade_time=datetime(2026, 6, 15, 1, 59, 59, tzinfo=UTC),
            ),
            NormalizedMarkPrice(
                **_source(CaptureStream.MARK_PRICE, sequence=2),
                mark_price=Decimal("100.1"),
                index_price=None,
                estimated_settle_price=None,
                funding_rate=None,
                next_funding_at=None,
            ),
            NormalizedKline1m(
                **_source(CaptureStream.KLINE_1M, sequence=3),
                open_time=datetime(2026, 6, 15, 2, 0, tzinfo=UTC),
                close_time=datetime(2026, 6, 15, 2, 0, 59, 999000, tzinfo=UTC),
                open_price=Decimal("99"),
                high_price=Decimal("101"),
                low_price=Decimal("98"),
                close_price=Decimal("100.5"),
                volume=Decimal("10"),
                quote_volume=Decimal("1000"),
                trade_count=20,
                closed=True,
            ),
        )
    )

    state = states[0]
    assert state.liquidation_count == 1
    assert state.liquidation_notional == Decimal("40.20")
    assert state.mark_price == Decimal("100.1")
    assert state.closed_kline_count == 1
    assert state.source_event_count == 3


def test_different_symbols_produce_separate_states() -> None:
    states = aggregate_market_states_15s(
        (
            _trade(sequence=1, symbol="BTCUSDT"),
            _trade(sequence=2, symbol="ETHUSDT"),
        )
    )

    assert [(state.symbol, state.trade_count) for state in states] == [
        ("BTCUSDT", 1),
        ("ETHUSDT", 1),
    ]


def _trade(
    *,
    sequence: int,
    price: Decimal = Decimal("100"),
    quantity: Decimal = Decimal("1"),
    side: AggressorSide = AggressorSide.BUY,
    symbol: str = "BTCUSDT",
) -> NormalizedAggTrade:
    return NormalizedAggTrade(
        **_source(CaptureStream.AGG_TRADE, sequence=sequence, symbol=symbol),
        trade_id=str(sequence),
        price=price,
        quantity=quantity,
        notional=price * quantity,
        aggressor_side=side,
    )


def _source(
    stream: CaptureStream,
    *,
    sequence: int = 1,
    symbol: str = "BTCUSDT",
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "exchange": "binance-usdm",
        "environment": "research",
        "symbol": symbol,
        "event_at": datetime(2026, 6, 15, 2, 0, sequence, tzinfo=UTC),
        "received_at": datetime(2026, 6, 15, 2, 0, sequence, 1000, tzinfo=UTC),
        "source_connection_session_id": UUID(int=1),
        "source_local_sequence": sequence,
        "source_stream": stream,
    }
