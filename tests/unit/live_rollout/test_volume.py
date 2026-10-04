import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.live_rollout.volume import WebSocketQuoteVolumeProvider
from crypto_momentum_lab.market_data.quote_volume import QuoteVolume24hSnapshot


class SnapshotSource:
    def __init__(self, snapshots):
        self.snapshots = snapshots
        self.delivered = asyncio.Event()
        self.release = asyncio.Event()
        self.stopped = False

    async def __aiter__(self):
        for snapshot in self.snapshots:
            yield snapshot
        self.delivered.set()
        await self.release.wait()

    def stop(self):
        self.stopped = True
        self.release.set()


@pytest.mark.asyncio
async def test_hub_volume_preserves_causal_history_and_filters_quote_asset():
    now = datetime(2026, 10, 4, tzinfo=UTC)
    later = now + timedelta(minutes=1)
    source = SnapshotSource(
        [
            QuoteVolume24hSnapshot("btcusdt", Decimal("100"), now, now),
            QuoteVolume24hSnapshot("BTCUSDT", Decimal("200"), later, later),
            QuoteVolume24hSnapshot("BTCUSDC", Decimal("999"), later, later),
        ]
    )
    provider = WebSocketQuoteVolumeProvider(source)
    await provider.start()
    try:
        await asyncio.wait_for(source.delivered.wait(), 1)
        assert provider.snapshot("BTCUSDT", as_of=now).quote_volume == 100
        assert provider.snapshot("BTCUSDT", as_of=later).quote_volume == 200
        assert provider.snapshot("BTCUSDC", as_of=later) is None
        assert provider.snapshot("BTCUSDT", as_of=now - timedelta(seconds=1)) is None
    finally:
        await provider.stop()
    assert source.stopped


@pytest.mark.asyncio
async def test_hub_volume_history_is_bounded_without_losing_latest_value():
    now = datetime(2026, 10, 4, tzinfo=UTC)
    source = SnapshotSource(
        [
            QuoteVolume24hSnapshot(
                "BTCUSDT", Decimal(index), now, now + timedelta(seconds=index)
            )
            for index in range(3)
        ]
    )
    provider = WebSocketQuoteVolumeProvider(source, history_size=2)
    await provider.start()
    try:
        await asyncio.wait_for(source.delivered.wait(), 1)
        assert provider.snapshot("BTCUSDT", as_of=now) is None
        assert (
            provider.snapshot("BTCUSDT", as_of=now + timedelta(seconds=2)).quote_volume
            == 2
        )
        assert (
            provider.metrics_snapshot(now=now + timedelta(seconds=2))[
                "total_snapshot_count"
            ]
            == 2
        )
    finally:
        await provider.stop()
