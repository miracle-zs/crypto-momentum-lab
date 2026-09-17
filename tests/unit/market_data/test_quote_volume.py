from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.market_data.binance.rest import Binance24hTicker
from crypto_momentum_lab.market_data.quote_volume import (
    Binance24hQuoteVolumePublisher,
    QuoteVolume24hSnapshot,
)


class FakeTickerClient:
    def __init__(self) -> None:
        self.tickers = {
            "BTCUSDT": Binance24hTicker(
                symbol="BTCUSDT",
                quote_volume=Decimal("100"),
                open_time=datetime(2026, 8, 27, 0, 0, tzinfo=UTC),
                close_time=datetime(2026, 8, 28, 0, 0, tzinfo=UTC),
            ),
            "SOLUSDT": Binance24hTicker(
                symbol="SOLUSDT",
                quote_volume=Decimal("50"),
                open_time=datetime(2026, 8, 27, 0, 0, tzinfo=UTC),
                close_time=datetime(2026, 8, 28, 0, 0, tzinfo=UTC),
            ),
            "BTCUSDC": Binance24hTicker(
                symbol="BTCUSDC",
                quote_volume=Decimal("999"),
                open_time=datetime(2026, 8, 27, 0, 0, tzinfo=UTC),
                close_time=datetime(2026, 8, 28, 0, 0, tzinfo=UTC),
            ),
        }

    async def fetch_24h_tickers(self) -> dict[str, Binance24hTicker]:
        return self.tickers


@pytest.mark.asyncio
async def test_quote_volume_publisher_filters_by_universe() -> None:
    client = FakeTickerClient()
    published: list[tuple[QuoteVolume24hSnapshot, ...]] = []

    async def sink(snapshots: tuple[QuoteVolume24hSnapshot, ...]) -> None:
        published.append(snapshots)

    # Filter: only BTCUSDT is in universe
    publisher = Binance24hQuoteVolumePublisher(
        client,
        publish=sink,
        symbols_filter=lambda: {"btcusdt"},
    )

    refreshed_count = await publisher.refresh_once()
    assert refreshed_count == 1
    assert len(published) == 1
    symbols = [s.symbol for s in published[0]]
    assert symbols == ["BTCUSDT"]


@pytest.mark.asyncio
async def test_quote_volume_publisher_publishes_all_when_filter_none() -> None:
    client = FakeTickerClient()
    published: list[tuple[QuoteVolume24hSnapshot, ...]] = []

    async def sink(snapshots: tuple[QuoteVolume24hSnapshot, ...]) -> None:
        published.append(snapshots)

    # Filter returns None (e.g. before first universe refresh)
    publisher = Binance24hQuoteVolumePublisher(
        client,
        publish=sink,
        symbols_filter=lambda: None,
    )

    refreshed_count = await publisher.refresh_once()
    assert refreshed_count == 2
    symbols = sorted(s.symbol for s in published[0])
    assert symbols == ["BTCUSDT", "SOLUSDT"]


@pytest.mark.asyncio
async def test_quote_volume_publisher_handles_filter_exception_gracefully() -> None:
    client = FakeTickerClient()
    published: list[tuple[QuoteVolume24hSnapshot, ...]] = []

    async def sink(snapshots: tuple[QuoteVolume24hSnapshot, ...]) -> None:
        published.append(snapshots)

    def failing_filter():
        raise RuntimeError("universe state error")

    publisher = Binance24hQuoteVolumePublisher(
        client,
        publish=sink,
        symbols_filter=failing_filter,
    )

    refreshed_count = await publisher.refresh_once()
    # Falls back gracefully to all USDT symbols
    assert refreshed_count == 2
    symbols = sorted(s.symbol for s in published[0])
    assert symbols == ["BTCUSDT", "SOLUSDT"]
