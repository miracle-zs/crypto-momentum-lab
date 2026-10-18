from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import pytest

from crypto_momentum_lab.next_gen.market_feed import MarketDataFeed
from crypto_momentum_lab.next_gen.strategy_engine import Bar15s


@pytest.mark.asyncio
async def test_market_feed_binance_stream_dispatch() -> None:
    received_bars: list[Bar15s] = []

    feed = MarketDataFeed(
        symbols=["BTCUSDT"],
        on_bar=received_bars.append,
    )

    sample_binance_msg = {
        "stream": "btcusdt@kline_1m",
        "data": {
            "e": "kline",
            "E": 1729252800000,
            "s": "BTCUSDT",
            "k": {
                "t": 1729252740000,
                "T": 1729252799999,
                "s": "BTCUSDT",
                "i": "1m",
                "o": "68000.10",
                "c": "68100.50",
                "h": "68150.00",
                "l": "67980.00",
                "v": "12.345",
            },
        },
    }

    await feed._dispatch_message(sample_binance_msg)

    assert len(received_bars) == 1
    bar = received_bars[0]
    assert bar.symbol == "BTCUSDT"
    assert bar.open_price == Decimal("68000.10")
    assert bar.close_price == Decimal("68100.50")
    assert bar.high_price == Decimal("68150.00")
    assert bar.low_price == Decimal("67980.00")
    assert bar.volume == Decimal("12.345")


@pytest.mark.asyncio
async def test_market_feed_hub_dispatch() -> None:
    received_bars: list[Bar15s] = []

    feed = MarketDataFeed(
        symbols=["ETHUSDT"],
        on_bar=received_bars.append,
    )

    sample_hub_msg = {
        "states": [
            {
                "symbol": "ETHUSDT",
                "timestamp": 1729252800000,
                "open": "2600.00",
                "high": "2620.00",
                "low": "2595.00",
                "close": "2615.50",
                "volume": "100.5",
            }
        ]
    }

    await feed._dispatch_message(sample_hub_msg)

    assert len(received_bars) == 1
    bar = received_bars[0]
    assert bar.symbol == "ETHUSDT"
    assert bar.close_price == Decimal("2615.50")


@pytest.mark.asyncio
async def test_market_feed_run_with_mock_connector() -> None:
    received_bars: list[Bar15s] = []
    stop_event = asyncio.Event()

    class MockWSStream:
        def __init__(self) -> None:
            self.messages = [
                json.dumps(
                    {
                        "data": {
                            "k": {
                                "s": "BTCUSDT",
                                "T": 1729252800000,
                                "o": "65000",
                                "h": "65100",
                                "l": "64900",
                                "c": "65050",
                                "v": "5",
                            }
                        }
                    }
                )
            ]

        def __aiter__(self) -> MockWSStream:
            return self

        async def __anext__(self) -> str:
            if self.messages:
                return self.messages.pop(0)
            stop_event.set()
            raise asyncio.CancelledError()

        async def close(self) -> None:
            pass

    async def mock_connector(url: str) -> MockWSStream:
        return MockWSStream()

    feed = MarketDataFeed(
        symbols=["BTCUSDT"],
        on_bar=received_bars.append,
        ws_connector=mock_connector,
        initial_backoff=0.01,
    )

    await feed.run(stop_event=stop_event)

    assert len(received_bars) == 1
    assert received_bars[0].close_price == Decimal("65050")
    await feed.close()
    assert feed.is_connected is False
