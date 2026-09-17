from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.live_rollout.volume import (
    Binance24hQuoteVolumeCache,
)
from crypto_momentum_lab.market_data.binance.rest import Binance24hTicker


class FakeTickerClient:
    def __init__(self) -> None:
        self.tickers = {
            "BTCUSDT": Binance24hTicker(
                symbol="BTCUSDT",
                quote_volume=Decimal("100"),
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
        self.fail = False

    async def fetch_24h_tickers(self) -> dict[str, Binance24hTicker]:
        if self.fail:
            raise TimeoutError("ticker endpoint unavailable")
        return self.tickers


@pytest.mark.asyncio
async def test_cache_returns_latest_causal_usdt_snapshot() -> None:
    client = FakeTickerClient()
    now = datetime(2026, 8, 28, 0, 0, tzinfo=UTC)
    cache = Binance24hQuoteVolumeCache(client, clock=lambda: now)

    await cache.refresh_once()
    client.tickers["BTCUSDT"] = Binance24hTicker(
        symbol="BTCUSDT",
        quote_volume=Decimal("200"),
        open_time=datetime(2026, 8, 27, 0, 0, tzinfo=UTC),
        close_time=datetime(2026, 8, 28, 0, 1, tzinfo=UTC),
    )
    later = now + timedelta(minutes=1)
    cache._clock = lambda: later
    await cache.refresh_once()

    assert cache.snapshot("BTCUSDT", as_of=now) is not None
    assert cache.snapshot("BTCUSDT", as_of=now).quote_volume == Decimal("100")
    assert (
        cache.snapshot("BTCUSDT", as_of=later).quote_volume
        == Decimal("200")
    )
    assert cache.snapshot("BTCUSDC", as_of=later) is None


@pytest.mark.asyncio
async def test_failed_refresh_keeps_previous_snapshot() -> None:
    client = FakeTickerClient()
    now = datetime(2026, 8, 28, 0, 0, tzinfo=UTC)
    cache = Binance24hQuoteVolumeCache(client, clock=lambda: now)
    await cache.refresh_once()
    client.fail = True

    with pytest.raises(TimeoutError):
        await cache.refresh_once()

    snapshot = cache.snapshot("BTCUSDT", as_of=now)
    assert snapshot is not None
    assert snapshot.quote_volume == Decimal("100")


@pytest.mark.asyncio
async def test_cache_metrics_tracking() -> None:
    client = FakeTickerClient()
    t0 = datetime(2026, 8, 28, 0, 0, tzinfo=UTC)
    cache = Binance24hQuoteVolumeCache(client, clock=lambda: t0)

    # Initial empty metrics
    assert cache.cached_symbol_count == 0
    assert cache.total_snapshot_count == 0
    assert cache.lookup_hit_count == 0
    assert cache.lookup_miss_count == 0
    assert cache.oldest_snapshot_age_seconds(now=t0) is None

    # After first refresh (FakeTickerClient has BTCUSDT and BTCUSDC, only USDT observed)
    await cache.refresh_once()
    assert cache.cached_symbol_count == 1
    assert cache.total_snapshot_count == 1

    # Check oldest snapshot age 60 seconds later
    t1 = t0 + timedelta(seconds=60)
    assert cache.oldest_snapshot_age_seconds(now=t1) == 60.0

    # Test hit
    hit_res = cache.snapshot("BTCUSDT", as_of=t0)
    assert hit_res is not None
    assert cache.lookup_hit_count == 1
    assert cache.lookup_miss_count == 0

    # Test miss by unknown symbol
    miss_res = cache.snapshot("ETHUSDT", as_of=t0)
    assert miss_res is None
    assert cache.lookup_hit_count == 1
    assert cache.lookup_miss_count == 1

    # Test miss by as_of before earliest snapshot
    before_t0 = t0 - timedelta(seconds=10)
    miss_time_res = cache.snapshot("BTCUSDT", as_of=before_t0)
    assert miss_time_res is None
    assert cache.lookup_hit_count == 1
    assert cache.lookup_miss_count == 2

    # Verify metrics_snapshot dictionary
    stats = cache.metrics_snapshot(now=t1)
    assert stats == {
        "cached_symbol_count": 1,
        "total_snapshot_count": 1,
        "oldest_snapshot_age_seconds": 60.0,
        "lookup_hit_count": 1,
        "lookup_miss_count": 2,
    }


