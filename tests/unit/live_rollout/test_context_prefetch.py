import asyncio
from datetime import UTC, datetime

import pytest

from crypto_momentum_lab.live_rollout.context_prefetch import LiveContextPrefetcher
from tests.unit.shadow_operation.test_service import _state


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
