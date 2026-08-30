import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest

from crypto_momentum_lab.domain.universe.models import (
    MarketCandidate,
    RankEntry,
    RankingResult,
    RankingSide,
    UniverseSnapshot,
)
from crypto_momentum_lab.live_rollout.entry_cache import (
    EntryFilterCacheConfig,
    LiveEntryFilterCache,
    LiveEntrySymbolCache,
    LiveEntryUniverseData,
    universe_context_for,
)
from crypto_momentum_lab.strategy_runner.candle_source import (
    ClosedCandleEmaSnapshot,
)


@pytest.mark.asyncio
async def test_entry_filter_cache_warms_in_background_and_reads_without_io() -> None:
    now = datetime(2026, 8, 22, 1, 2, 3, tzinfo=UTC)
    calls: list[tuple[str, datetime]] = []
    ready = asyncio.Event()

    class Provider:
        def load(
            self,
            *,
            symbol: str,
            observed_at: datetime,
        ) -> ClosedCandleEmaSnapshot:
            calls.append((symbol, observed_at))
            return ClosedCandleEmaSnapshot(
                ema5=Decimal("100"),
                ema10=Decimal("99"),
            )

    async def load_symbols(_: datetime) -> frozenset[str]:
        return frozenset({"BTCUSDT", "ETHUSDT"})

    def on_ready(value: bool) -> None:
        assert value is True
        ready.set()

    cache = LiveEntryFilterCache(
        ema_provider=Provider(),  # type: ignore[arg-type]
        symbol_loader=load_symbols,
        clock=lambda: now,
        on_ready=on_ready,
    )
    task = asyncio.create_task(cache.run())
    try:
        await asyncio.wait_for(ready.wait(), timeout=1)
        assert cache.ready is True
        assert cache.symbols_for(now) == frozenset({"BTCUSDT", "ETHUSDT"})
        assert cache.snapshot_for(symbol="btcusdt", observed_at=now) == (
            ClosedCandleEmaSnapshot(
                ema5=Decimal("100"),
                ema10=Decimal("99"),
            )
        )
        assert cache.snapshot_for(
            symbol="BTCUSDT",
            observed_at=now + timedelta(minutes=15),
        ) is None
        assert sorted(symbol for symbol, _ in calls) == ["BTCUSDT", "ETHUSDT"]
    finally:
        await cache.stop()
        await task


def test_entry_filter_cache_rejects_invalid_prefetch_config() -> None:
    with pytest.raises(ValueError, match="prefetch_concurrency"):
        EntryFilterCacheConfig(prefetch_concurrency=0)


@pytest.mark.asyncio
async def test_entry_symbol_cache_refreshes_pool_without_blocking_reads() -> None:
    now = datetime(2026, 8, 22, 1, 2, 3, tzinfo=UTC)
    ready = asyncio.Event()
    calls: list[datetime] = []

    async def load_symbols(observed_at: datetime) -> frozenset[str]:
        calls.append(observed_at)
        return frozenset({"BTCUSDT"})

    def on_ready(value: bool) -> None:
        assert value is True
        ready.set()

    cache = LiveEntrySymbolCache(
        symbol_loader=load_symbols,
        clock=lambda: now,
        on_ready=on_ready,
    )
    task = asyncio.create_task(cache.run())
    try:
        await asyncio.wait_for(ready.wait(), timeout=1)
        assert cache.ready is True
        assert cache.symbols_for(now) == frozenset({"BTCUSDT"})
        assert cache.symbols_for(now + timedelta(seconds=15)) == frozenset(
            {"BTCUSDT"}
        )
        assert calls == [now]
    finally:
        await cache.stop()
        await task


@pytest.mark.asyncio
async def test_entry_symbol_cache_keeps_universe_context_in_memory() -> None:
    now = datetime(2026, 8, 22, 1, 2, 3, tzinfo=UTC)
    snapshot = UniverseSnapshot(
        snapshot_id=uuid4(),
        observed_at=now,
        utc_day=now.date(),
        config_hash="a" * 64,
        activated=True,
        ranking=RankingResult(
            candidates=(
                MarketCandidate(
                    "BTCUSDT",
                    Decimal("100"),
                    Decimal("112.3"),
                    now,
                ),
            ),
            gainers=(
                RankEntry(
                    "BTCUSDT",
                    Decimal("0.123"),
                    7,
                    RankingSide.GAINER,
                ),
            ),
            losers=(),
            target_symbols=frozenset({"BTCUSDT"}),
            exclusions={},
        ),
        memberships=(),
    )
    data = LiveEntryUniverseData(
        symbols=frozenset({"BTCUSDT"}),
        snapshot=snapshot,
    )
    ready = asyncio.Event()

    async def load_universe(_: datetime) -> LiveEntryUniverseData:
        return data

    def on_ready(value: bool) -> None:
        assert value is True
        ready.set()

    cache = LiveEntrySymbolCache(
        universe_loader=load_universe,
        clock=lambda: now,
        on_ready=on_ready,
    )
    task = asyncio.create_task(cache.run())
    try:
        await asyncio.wait_for(ready.wait(), timeout=1)
        assert cache.symbols_for(now) == frozenset({"BTCUSDT"})
        assert cache.universe_data_for(now) == data
        context = universe_context_for(
            cache.universe_data_for(now),
            symbol="BTCUSDT",
            entry_pool_name="positive_gainer_top100",
            entry_pool_top_count=100,
        )
        assert context is not None
        assert context["gainer_rank"] == 7
        assert context["utc_day_return"] == Decimal("0.123")
        assert context["utc_day_return_pct"] == Decimal("12.300")
        assert context["in_entry_pool"] is True
    finally:
        await cache.stop()
        await task
