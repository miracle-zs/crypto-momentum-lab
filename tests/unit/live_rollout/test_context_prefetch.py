import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from crypto_momentum_lab.live_rollout.context_prefetch import LiveContextPrefetcher
from tests.fixtures.live_market import _state


@pytest.mark.asyncio
async def test_cancellation_drains_full_queue_and_awaits_context_tasks() -> None:
    started_three = asyncio.Event()
    started = 0
    cancelled = 0

    async def states():
        while True:
            yield _state()

    async def context_provider(_state):
        nonlocal started, cancelled
        started += 1
        if started == 3:
            started_three.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled += 1
            raise

    prefetcher = LiveContextPrefetcher(
        context_provider=context_provider,
        context_generation=lambda: 0,
        clock=lambda: datetime.now(UTC),
    )

    async def consume() -> None:
        async for _ in prefetcher.stream(states()):
            pass

    consumer = asyncio.create_task(consume())
    await asyncio.wait_for(started_three.wait(), timeout=1)
    consumer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(consumer, timeout=1)
    assert started >= 3
    assert cancelled == started


@pytest.mark.asyncio
async def test_backfill_does_not_load_or_repair_live_account_context() -> None:
    calls = []
    live_context = object()

    async def context_provider(state):
        calls.append(state)
        return live_context

    history = replace(_state(), is_backfill=True)
    current = replace(
        _state(),
        bucket_start=history.bucket_start + timedelta(seconds=15),
        bucket_end=history.bucket_end + timedelta(seconds=15),
    )

    async def states():
        yield history
        yield current

    prefetcher = LiveContextPrefetcher(
        context_provider=context_provider,
        context_generation=lambda: 4,
        clock=lambda: datetime.now(UTC),
    )
    results = [item async for item in prefetcher.stream(states())]

    assert calls == [current], "history must not query or repair live positions"
    assert [item.state for item in results] == [history, current]
    assert results[0].context is None
    assert results[0].error is None
    assert results[1].context is live_context
