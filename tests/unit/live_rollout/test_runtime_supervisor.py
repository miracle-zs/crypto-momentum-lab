import asyncio

import pytest

from crypto_momentum_lab.live_rollout.market_loop import LiveDaemonResult
from crypto_momentum_lab.live_rollout.runtime_supervisor import (
    LiveRuntimeSupervisor,
    LiveRuntimeTasks,
)


async def _wait_forever() -> None:
    await asyncio.Event().wait()


async def _wait_for_market_result() -> LiveDaemonResult:
    await asyncio.Event().wait()
    raise AssertionError("market task should be cancelled")


async def _raise_account_failure() -> None:
    raise RuntimeError("account stream failed")


async def _close_nothing() -> None:
    return None


def _runtime_tasks(
    *,
    account: asyncio.Task[None],
) -> LiveRuntimeTasks:
    return LiveRuntimeTasks(
        market=asyncio.create_task(_wait_for_market_result()),
        account=account,
        lease=asyncio.create_task(_wait_forever()),
        reconcile=asyncio.create_task(_wait_forever()),
    )


async def test_supervisor_fails_when_account_channel_stops() -> None:
    tasks = _runtime_tasks(account=asyncio.create_task(_raise_account_failure()))
    supervisor = LiveRuntimeSupervisor(
        tasks=tasks,
        block_entry_submissions=lambda: None,
        stop_sources=lambda: None,
        close_risk_control=_close_nothing,
        stop_entry_caches=_close_nothing,
    )

    await asyncio.sleep(0)
    with pytest.raises(RuntimeError, match="account stream failed"):
        await supervisor.run()

    await supervisor.stop()


async def test_supervisor_shutdown_preserves_safety_order_and_is_idempotent() -> None:
    events: list[str] = []
    tasks = _runtime_tasks(account=asyncio.create_task(_wait_forever()))

    async def close_risk_control() -> None:
        events.append("close-risk-control")

    async def stop_entry_caches() -> None:
        events.append("stop-entry-caches")

    supervisor = LiveRuntimeSupervisor(
        tasks=tasks,
        block_entry_submissions=lambda: events.append("block-entries"),
        stop_sources=lambda: events.append("stop-sources"),
        close_risk_control=close_risk_control,
        stop_entry_caches=stop_entry_caches,
    )

    await supervisor.stop()
    await supervisor.stop()

    assert events == [
        "block-entries",
        "stop-sources",
        "close-risk-control",
        "stop-entry-caches",
    ]
    assert all(task.done() for task in tasks.all_tasks())
