"""One unfinished repair must not restart already completed repair families."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from crypto_momentum_lab.domain.execution.order_state import ExchangeOrderState
from crypto_momentum_lab.live_rollout.order_reconciliation import (
    LiveOrderReconciliation,
)
from tests.unit.live_rollout.test_order_reconciliation import _ws_order_case

REQUESTS = {
    "positions": "request_position_recovery",
    "orders": "request_order_recovery",
    "exits": "request_exit_recovery",
}


@pytest.mark.asyncio
@pytest.mark.parametrize("unfinished", ["positions", "orders", "exits"])
async def test_retry_runs_only_the_unfinished_repair_family(unfinished: str) -> None:
    order, _ = _ws_order_case()
    order = replace(order, state=ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION)
    completed = asyncio.Event()
    counts = {"positions": 0, "orders": 0, "exits": 0}

    async def repair(family: str) -> bool:
        counts[family] += 1
        if family == unfinished and counts[family] == 2:
            completed.set()
        return family == unfinished and counts[family] == 1

    async def orders(_run: str) -> tuple:
        pending = await repair("orders")
        return (order,) if pending else ()

    worker = LiveOrderReconciliation(
        SimpleNamespace(load_unresolved_orders=orders),
        SimpleNamespace(reconcile_order=AsyncMock()),
        order.plan.run_id,
        interval_seconds=0.01,
        repair_positions=lambda: repair("positions"),
        recover_exits=lambda: repair("exits"),
    )
    task = asyncio.create_task(worker.run_requested())
    try:
        worker.request_recovery()
        await asyncio.wait_for(completed.wait(), 1)
        for _ in range(20):
            await asyncio.sleep(0)
        await asyncio.sleep(0.03)
        assert counts == {family: 2 if family == unfinished else 1 for family in counts}
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("family", REQUESTS)
async def test_explicit_request_runs_only_its_repair_family(family: str) -> None:
    positions = AsyncMock(return_value=False)
    orders = AsyncMock(return_value=())
    exits = AsyncMock(return_value=False)
    worker = LiveOrderReconciliation(
        SimpleNamespace(load_unresolved_orders=orders),
        SimpleNamespace(),
        "run",
        repair_positions=positions,
        recover_exits=exits,
    )
    task = asyncio.create_task(worker.run_requested())
    try:
        for _ in range(20):
            getattr(worker, REQUESTS[family])()
        for _ in range(20):
            await asyncio.sleep(0)
        counts = {
            "positions": positions.await_count,
            "orders": orders.await_count,
            "exits": exits.await_count,
        }
        assert counts == {name: int(name == family) for name in counts}
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_inflight_requests_keep_only_the_requested_families() -> None:
    started, release, completed = asyncio.Event(), asyncio.Event(), asyncio.Event()
    exits = AsyncMock(return_value=False)
    orders = AsyncMock(return_value=())
    calls = 0

    async def positions() -> bool:
        nonlocal calls
        calls += 1
        if calls == 1:
            started.set()
            await release.wait()
        else:
            completed.set()
        return False

    worker = LiveOrderReconciliation(
        SimpleNamespace(load_unresolved_orders=orders),
        SimpleNamespace(),
        "run",
        repair_positions=positions,
        recover_exits=exits,
    )
    task = asyncio.create_task(worker.run_requested())
    try:
        worker.request_position_recovery()
        await asyncio.wait_for(started.wait(), 1)
        for _ in range(20):
            worker.request_position_recovery()
            worker.request_exit_recovery()
        release.set()
        await asyncio.wait_for(completed.wait(), 1)
        for _ in range(20):
            await asyncio.sleep(0)
        assert calls == 2
        exits.assert_awaited_once()
        orders.assert_not_awaited()
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("failed", REQUESTS)
async def test_failure_retries_only_failed_family_and_preserves_other_work(
    failed: str,
) -> None:
    completed = asyncio.Event()
    counts = {"positions": 0, "orders": 0, "exits": 0}

    async def repair(family: str) -> bool:
        counts[family] += 1
        if family == failed:
            if counts[family] == 1:
                raise ConnectionError("database unavailable")
            completed.set()
        return False

    async def orders(_run: str) -> tuple:
        await repair("orders")
        return ()

    worker = LiveOrderReconciliation(
        SimpleNamespace(load_unresolved_orders=orders),
        SimpleNamespace(),
        "run",
        interval_seconds=0.01,
        repair_positions=lambda: repair("positions"),
        recover_exits=lambda: repair("exits"),
    )
    task = asyncio.create_task(worker.run_requested())
    try:
        worker.request_recovery()
        await asyncio.wait_for(completed.wait(), 1)
        for _ in range(20):
            await asyncio.sleep(0)
        assert counts == {name: 2 if name == failed else 1 for name in counts}
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_frequent_exit_requests_do_not_postpone_order_retry_deadline() -> None:
    order, _ = _ws_order_case()
    order = replace(order, state=ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION)
    completed = asyncio.Event()
    calls = 0

    async def orders(_run: str) -> tuple:
        nonlocal calls
        calls += 1
        if calls == 1:
            return (order,)
        completed.set()
        return ()

    exits = AsyncMock(return_value=False)
    positions = AsyncMock(return_value=False)
    worker = LiveOrderReconciliation(
        SimpleNamespace(load_unresolved_orders=orders),
        SimpleNamespace(reconcile_order=AsyncMock()),
        "run",
        interval_seconds=0.02,
        repair_positions=positions,
        recover_exits=exits,
    )

    async def keep_requesting_exits() -> None:
        while True:
            worker.request_exit_recovery()
            await asyncio.sleep(0.001)

    task = asyncio.create_task(worker.run_requested())
    producer = asyncio.create_task(keep_requesting_exits())
    try:
        worker.request_order_recovery()
        await asyncio.wait_for(completed.wait(), 0.5)
        assert calls == 2
        assert exits.await_count > 1
        positions.assert_not_awaited()
    finally:
        producer.cancel()
        task.cancel()
        await asyncio.gather(task, producer, return_exceptions=True)


@pytest.mark.asyncio
async def test_incomplete_ws_order_event_does_not_wake_exit_or_position_scan() -> None:
    order, event = _ws_order_case()
    orders = AsyncMock(return_value=())
    positions, exits = AsyncMock(return_value=False), AsyncMock(return_value=False)
    worker = LiveOrderReconciliation(
        SimpleNamespace(
            load_order=AsyncMock(return_value=order), load_unresolved_orders=orders
        ),
        SimpleNamespace(mark_reconciliation_pending=AsyncMock()),
        order.plan.run_id,
        repair_positions=positions,
        recover_exits=exits,
    )
    task = asyncio.create_task(worker.run_requested())
    try:
        await worker.reconcile_account_event(replace(event, order_update=None))
        for _ in range(20):
            await asyncio.sleep(0)
        orders.assert_awaited_once_with(order.plan.run_id)
        positions.assert_not_awaited()
        exits.assert_not_awaited()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_delegated_unknown_exit_is_retried_only_by_exit_owner() -> None:
    order, _ = _ws_order_case()
    order = replace(
        order,
        state=ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
        plan=replace(order.plan, reduce_only=True),
    )
    orders = AsyncMock(return_value=(order,))
    completed = asyncio.Event()
    calls = 0

    async def exits() -> bool:
        nonlocal calls
        calls += 1
        if calls == 2:
            completed.set()
        return calls == 1

    machine = SimpleNamespace(reconcile_order=AsyncMock())
    worker = LiveOrderReconciliation(
        SimpleNamespace(load_unresolved_orders=orders),
        machine,
        order.plan.run_id,
        interval_seconds=0.01,
        recover_exits=exits,
        request_unknown_exit=lambda _order: True,
    )
    task = asyncio.create_task(worker.run_requested())
    try:
        worker.request_order_recovery()
        await asyncio.wait_for(completed.wait(), 1)
        for _ in range(20):
            await asyncio.sleep(0)
        orders.assert_awaited_once()
        machine.reconcile_order.assert_not_awaited()
        assert calls == 2
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
