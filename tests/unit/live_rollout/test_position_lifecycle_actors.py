import asyncio

import pytest

from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
from crypto_momentum_lab.live_rollout.position_lifecycle import (
    PositionLifecycleActors,
)


def _key(symbol: str, side: FuturesPositionSide) -> PositionKey:
    return PositionKey("live", "account-1", symbol, side)


@pytest.mark.asyncio
async def test_actor_serializes_one_position_and_runs_other_positions_concurrently():
    actors = PositionLifecycleActors()
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
        actors.run(_key("BTCUSDT", FuturesPositionSide.LONG), first)
    )
    await started.wait()
    second_call = asyncio.create_task(
        actors.run(_key("BTCUSDT", FuturesPositionSide.LONG), second)
    )
    other_call = asyncio.create_task(
        actors.run(
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
    actors = PositionLifecycleActors()
    key = _key("BTCUSDT", FuturesPositionSide.SHORT)

    async def fail() -> None:
        raise RuntimeError("failed operation")

    with pytest.raises(RuntimeError, match="failed operation"):
        await actors.run(key, fail)

    assert await actors.run(key, lambda: asyncio.sleep(0, result=7)) == 7
    await actors.close()
