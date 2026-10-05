from dataclasses import replace
from datetime import UTC, datetime, timedelta

from crypto_momentum_lab.live_rollout.market_timing import LiveMarketTimingTracker
from crypto_momentum_lab.market_data.hub import MarketStateBatch
from tests.fixtures.live_market import _state


def test_timing_tracker_correlates_hub_batch_and_evicts_oldest_state() -> None:
    start = datetime(2026, 10, 5, tzinfo=UTC)
    first = replace(
        _state(),
        symbol="BTCUSDT",
        bucket_start=start,
        bucket_end=start + timedelta(seconds=15),
    )
    second = replace(
        _state(),
        symbol="ETHUSDT",
        bucket_start=start + timedelta(seconds=15),
        bucket_end=start + timedelta(seconds=30),
    )
    tracker = LiveMarketTimingTracker(max_entries=1)

    tracker.observe_batch(
        MarketStateBatch(
            sequence=1,
            stream_id="stream-1",
            environment="live",
            published_at=start + timedelta(seconds=15, milliseconds=400),
            states=(first,),
        ),
        received_at=start + timedelta(seconds=15, milliseconds=430),
    )
    timing = tracker.timing_for(first)
    assert timing is not None
    assert timing.published_at == start + timedelta(seconds=15, milliseconds=400)
    assert timing.socket_received_at == start + timedelta(seconds=15, milliseconds=430)

    tracker.observe_batch(
        MarketStateBatch(
            sequence=2,
            stream_id="stream-1",
            environment="live",
            published_at=start + timedelta(seconds=30, milliseconds=400),
            states=(second,),
        ),
        received_at=start + timedelta(seconds=30, milliseconds=430),
    )
    assert tracker.timing_for(first) is None
    assert tracker.timing_for(second) is not None
