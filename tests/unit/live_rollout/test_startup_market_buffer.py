import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from crypto_momentum_lab.live_rollout.startup_market_buffer import (
    StartupMarketStateBuffer,
)


@pytest.mark.asyncio
async def test_stream_skips_history_duplicates_and_keeps_future_states() -> None:
    start = datetime(2026, 9, 12, 0, 0, tzinfo=UTC)
    buffer = StartupMarketStateBuffer(max_states=8)
    await buffer.append(  # type: ignore[arg-type]
        SimpleNamespace(symbol="BTCUSDT", bucket_start=start)
    )
    await buffer.append(
        SimpleNamespace(
            symbol="BTCUSDT",
            bucket_start=start + timedelta(seconds=15),
        )
    )  # type: ignore[arg-type]
    await buffer.append(
        SimpleNamespace(
            symbol="ETHUSDT",
            bucket_start=start - timedelta(seconds=15),
        )
    )  # type: ignore[arg-type]
    buffer.close()

    observed = [
        state
        async for state in buffer.stream(
            skip_through={
                "BTCUSDT": start,
                "ETHUSDT": start - timedelta(seconds=15),
            }
        )
    ]

    assert [state.symbol for state in observed] == ["BTCUSDT"]
    assert observed[0].bucket_start == start + timedelta(seconds=15)


@pytest.mark.asyncio
async def test_append_backpressures_instead_of_dropping_history() -> None:
    buffer = StartupMarketStateBuffer(max_states=1)
    first = SimpleNamespace(
        symbol="BTCUSDT",
        bucket_start=datetime(2026, 9, 12, tzinfo=UTC),
    )
    second = SimpleNamespace(
        symbol="BTCUSDT",
        bucket_start=datetime(2026, 9, 12, 0, 0, 15, tzinfo=UTC),
    )

    await buffer.append(first)  # type: ignore[arg-type]
    pending = asyncio.create_task(buffer.append(second))  # type: ignore[arg-type]
    await asyncio.sleep(0)
    assert pending.done() is False

    assert buffer.buffered_state_count == 1
    stream = buffer.stream()
    assert await anext(stream) is first
    await pending
    buffer.close()
    assert await anext(stream) is second
    with pytest.raises(StopAsyncIteration):
        await anext(stream)
