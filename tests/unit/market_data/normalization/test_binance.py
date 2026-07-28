from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest

from crypto_momentum_lab.domain.market.models import (
    AggressorSide,
    CaptureRoute,
    CaptureStream,
    NormalizedAggTrade,
    NormalizedBookTicker,
    NormalizedKline1m,
    NormalizedLiquidation,
    NormalizedMarkPrice,
    OrderSide,
    RawEnvelope,
)
from crypto_momentum_lab.market_data.normalization.binance import (
    BinanceNormalizationError,
    normalize_binance_envelope,
)


def test_normalizes_aggregate_trade_and_aggressor_side() -> None:
    buy = normalize_binance_envelope(
        _envelope(
            CaptureStream.AGG_TRADE,
            {
                "e": "aggTrade",
                "E": 1781488800000,
                "s": "BTCUSDT",
                "a": 42,
                "p": "100.25",
                "q": "0.5",
                "m": False,
            },
        )
    )
    sell = normalize_binance_envelope(
        _envelope(
            CaptureStream.AGG_TRADE,
            {
                "e": "aggTrade",
                "E": 1781488800000,
                "s": "BTCUSDT",
                "a": 43,
                "p": "101",
                "q": "0.2",
                "m": True,
            },
        )
    )

    assert isinstance(buy, NormalizedAggTrade)
    assert buy.trade_id == "42"
    assert buy.price == Decimal("100.25")
    assert buy.quantity == Decimal("0.5")
    assert buy.notional == Decimal("50.125")
    assert buy.aggressor_side is AggressorSide.BUY
    assert isinstance(sell, NormalizedAggTrade)
    assert sell.aggressor_side is AggressorSide.SELL


def test_normalizes_book_ticker() -> None:
    event = normalize_binance_envelope(
        _envelope(
            CaptureStream.BOOK_TICKER,
            {
                "e": "bookTicker",
                "E": 1781488800000,
                "s": "BTCUSDT",
                "u": 7,
                "b": "100",
                "B": "1.5",
                "a": "100.5",
                "A": "2.5",
            },
        )
    )

    assert isinstance(event, NormalizedBookTicker)
    assert event.update_id == "7"
    assert event.bid_price == Decimal("100")
    assert event.bid_quantity == Decimal("1.5")
    assert event.ask_price == Decimal("100.5")
    assert event.ask_quantity == Decimal("2.5")


def test_normalizes_mark_price() -> None:
    event = normalize_binance_envelope(
        _envelope(
            CaptureStream.MARK_PRICE,
            {
                "e": "markPriceUpdate",
                "E": 1781488800000,
                "s": "BTCUSDT",
                "p": "100.1",
                "i": "100.0",
                "P": "100.2",
                "r": "0.0001",
                "T": 1781510400000,
            },
        )
    )

    assert isinstance(event, NormalizedMarkPrice)
    assert event.mark_price == Decimal("100.1")
    assert event.index_price == Decimal("100.0")
    assert event.estimated_settle_price == Decimal("100.2")
    assert event.funding_rate == Decimal("0.0001")
    assert event.next_funding_at == datetime.fromtimestamp(
        1781510400000 / 1000,
        tz=UTC,
    )


def test_normalizes_kline_1m() -> None:
    event = normalize_binance_envelope(
        _envelope(
            CaptureStream.KLINE_1M,
            {
                "e": "kline",
                "E": 1781488800000,
                "s": "BTCUSDT",
                "k": {
                    "t": 1781488800000,
                    "T": 1781488859999,
                    "o": "100",
                    "h": "101",
                    "l": "99",
                    "c": "100.5",
                    "v": "12",
                    "q": "1200",
                    "n": 33,
                    "x": True,
                },
            },
        )
    )

    assert isinstance(event, NormalizedKline1m)
    assert event.open_price == Decimal("100")
    assert event.high_price == Decimal("101")
    assert event.low_price == Decimal("99")
    assert event.close_price == Decimal("100.5")
    assert event.volume == Decimal("12")
    assert event.quote_volume == Decimal("1200")
    assert event.trade_count == 33
    assert event.closed is True


def test_normalizes_liquidation_snapshot() -> None:
    event = normalize_binance_envelope(
        _envelope(
            CaptureStream.FORCE_ORDER,
            {
                "e": "forceOrder",
                "E": 1781488800000,
                "o": {
                    "s": "BTCUSDT",
                    "S": "SELL",
                    "p": "100",
                    "ap": "100.5",
                    "q": "0.4",
                    "T": 1781488799000,
                },
            },
        )
    )

    assert isinstance(event, NormalizedLiquidation)
    assert event.order_side is OrderSide.SELL
    assert event.price == Decimal("100")
    assert event.average_price == Decimal("100.5")
    assert event.quantity == Decimal("0.4")
    assert event.notional == Decimal("40.20")
    assert event.trade_time == datetime.fromtimestamp(
        1781488799000 / 1000,
        tz=UTC,
    )


def test_uses_accumulated_filled_quantity_for_liquidation_notional() -> None:
    event = normalize_binance_envelope(
        _envelope(
            CaptureStream.FORCE_ORDER,
            {
                "e": "forceOrder",
                "E": 1781488800000,
                "o": {
                    "s": "BTCUSDT",
                    "S": "SELL",
                    "p": "100",
                    "ap": "100.5",
                    "q": "0.4",
                    "z": "0.25",
                    "T": 1781488799000,
                },
            },
        )
    )

    assert isinstance(event, NormalizedLiquidation)
    assert event.quantity == Decimal("0.25")
    assert event.notional == Decimal("25.125")


def test_rejects_malformed_aggregate_trade() -> None:
    with pytest.raises(BinanceNormalizationError, match="p"):
        normalize_binance_envelope(
            _envelope(
                CaptureStream.AGG_TRADE,
                {"e": "aggTrade", "s": "BTCUSDT", "a": 42, "q": "0.5"},
            )
        )


def _envelope(
    stream: CaptureStream,
    payload: dict[str, object],
) -> RawEnvelope:
    return RawEnvelope(
        schema_version=1,
        exchange="binance-usdm",
        environment="research",
        route=(
            CaptureRoute.PUBLIC
            if stream is CaptureStream.BOOK_TICKER
            else CaptureRoute.MARKET
        ),
        stream=stream,
        symbol="BTCUSDT",
        exchange_event_at=datetime(2026, 6, 15, 2, 0, tzinfo=UTC),
        received_at=datetime(2026, 6, 15, 2, 0, 1, tzinfo=UTC),
        received_monotonic_ns=100,
        connection_session_id=UUID(int=1),
        local_sequence=7,
        exchange_sequence="42",
        subscription_generation=3,
        raw_payload=payload,
    )
