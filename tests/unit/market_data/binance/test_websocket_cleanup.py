import asyncio

import pytest

from crypto_momentum_lab.market_data.binance.websocket import (
    _cancel_and_drain_tasks,
)


@pytest.mark.asyncio
async def test_child_task_cleanup_consumes_done_exception() -> None:
    loop = asyncio.get_running_loop()
    unhandled: list[dict[str, object]] = []
    previous_handler = loop.get_exception_handler()

    def capture_unhandled(
        _loop: asyncio.AbstractEventLoop,
        context: dict[str, object],
    ) -> None:
        unhandled.append(context)

    loop.set_exception_handler(capture_unhandled)
    try:
        async def fail() -> None:
            raise RuntimeError("normal-close regression")

        task = asyncio.create_task(fail())
        await asyncio.sleep(0)
        await _cancel_and_drain_tasks((task,))
        await asyncio.sleep(0)
    finally:
        loop.set_exception_handler(previous_handler)

    assert unhandled == []
