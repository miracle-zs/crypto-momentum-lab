from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from crypto_momentum_lab.live_rollout.market_cache import (
    LatestMarketQuoteCache,
    LatestMarketStateCache,
)

NOW = datetime(2026, 9, 11, 6, 0, tzinfo=UTC)


def _state(symbol: str, offset_seconds: int) -> object:
    return SimpleNamespace(
        symbol=symbol,
        bucket_start=NOW + timedelta(seconds=offset_seconds),
    )


def _quote(symbol: str, offset_seconds: int) -> object:
    return SimpleNamespace(
        symbol=symbol,
        received_at=NOW + timedelta(seconds=offset_seconds),
    )


def test_latest_market_state_cache_keeps_newest_value_per_symbol() -> None:
    cache = LatestMarketStateCache()
    old_btc = _state("BTCUSDT", 0)
    new_btc = _state("BTCUSDT", 15)
    eth = _state("ETHUSDT", 30)

    cache.observe(old_btc)  # type: ignore[arg-type]
    cache.observe(new_btc)  # type: ignore[arg-type]
    cache.observe(old_btc)  # type: ignore[arg-type]
    cache.observe(eth)  # type: ignore[arg-type]

    assert cache.for_symbols(("BTCUSDT",)) == (new_btc,)
    assert cache.for_symbols(()) == (new_btc, eth)
    assert cache.for_symbols(("SOLUSDT",)) == ()


def test_latest_market_quote_cache_orders_and_filters_latest_values() -> None:
    cache = LatestMarketQuoteCache()
    old_btc = _quote("BTCUSDT", 0)
    new_btc = _quote("BTCUSDT", 15)
    eth = _quote("ETHUSDT", 30)

    cache.observe(old_btc)  # type: ignore[arg-type]
    cache.observe(new_btc)  # type: ignore[arg-type]
    cache.observe(old_btc)  # type: ignore[arg-type]
    cache.observe(eth)  # type: ignore[arg-type]

    assert cache.for_symbols(("ETHUSDT", "BTCUSDT")) == (new_btc, eth)
    assert cache.for_symbols(()) == (new_btc, eth)
    assert cache.for_symbols(("SOLUSDT",)) == ()
