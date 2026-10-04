import asyncio

import pytest

from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
from crypto_momentum_lab.live_rollout.position_lifecycle import (
    PositionLifecycleLocks,
)


async def _run(locks, key, operation):
    async with locks.hold(key):
        return await operation()


def _key(symbol: str, side: FuturesPositionSide) -> PositionKey:
    return PositionKey("live", "account-1", symbol, side)


@pytest.mark.asyncio
async def test_actor_serializes_one_position_and_runs_other_positions_concurrently():
    actors = PositionLifecycleLocks()
    started = asyncio.Event()
    release = asyncio.Event()
    order: list[str] = []

    async def first() -> str:
        order.append("first-start")
        started.set()
        await release.wait()
        order.append("first-end")
        return "first-result"

    async def second() -> str:
        order.append("second")
        return "second-result"

    first_call = asyncio.create_task(
        _run(actors, _key("BTCUSDT", FuturesPositionSide.LONG), first)
    )
    await started.wait()
    second_call = asyncio.create_task(
        _run(actors, _key("BTCUSDT", FuturesPositionSide.LONG), second)
    )
    other_call = asyncio.create_task(
        _run(actors,
            _key("ETHUSDT", FuturesPositionSide.LONG),
            lambda: asyncio.sleep(0, result="other-result"),
        )
    )

    assert await other_call == "other-result"
    assert order == ["first-start"]
    release.set()
    assert await first_call == "first-result"
    assert await second_call == "second-result"
    assert order == ["first-start", "first-end", "second"]
    await actors.close()


@pytest.mark.asyncio
async def test_actor_continues_after_one_lifecycle_operation_fails():
    actors = PositionLifecycleLocks()
    key = _key("BTCUSDT", FuturesPositionSide.SHORT)

    async def fail() -> None:
        raise RuntimeError("failed operation")

    with pytest.raises(RuntimeError, match="failed operation"):
        await _run(actors, key, fail)

    assert await _run(actors, key, lambda: asyncio.sleep(0, result=7)) == 7
    await actors.close()


@pytest.mark.asyncio
async def test_cancelled_waiter_never_runs_and_drain_waits_for_admitted_work():
    locks = PositionLifecycleLocks()
    key = _key("BTCUSDT", FuturesPositionSide.LONG)
    started = asyncio.Event()
    release = asyncio.Event()
    executed = []

    async def first():
        started.set()
        await release.wait()

    async def cancelled_operation():
        executed.append("unexpected")

    active = asyncio.create_task(_run(locks, key, first))
    await started.wait()
    waiter = asyncio.create_task(_run(locks, key, cancelled_operation))
    await asyncio.sleep(0)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    draining = asyncio.create_task(locks.drain())
    await asyncio.sleep(0)
    assert not draining.done()
    release.set()
    await active
    await draining
    assert executed == []
    assert await _run(locks, key, lambda: asyncio.sleep(0, result=7)) == 7
    await locks.close()
