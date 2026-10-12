"""SYSTEM.md section 8/6: an exhausted fast-query budget becomes low-frequency work.

"快查询预算耗尽，同证据连续账户事件｜不绕过退避、不热循环；转低频；相关新事实及
时处理，其他key公平运行" and Q9 "快速查证预算耗尽后告警、保持隔离与责任，转低频
查询".

The existing unit suite already covers per-pass lookup rotation, family fairness and
"new facts resume pending work". What was uncovered, and is asserted here, is the
exhaustion outcome itself: after a pass spends its whole lookup budget the recovery
worker keeps the responsibility, publishes a positive next-attempt delay, and the
running worker waits for that deadline instead of immediately spending again.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from crypto_momentum_lab.domain.execution.order_state import ExchangeOrderState
from crypto_momentum_lab.live_rollout.order_reconciliation import (
    LiveOrderReconciliation,
)

pytestmark = pytest.mark.contract


def _unresolved(count: int) -> tuple[SimpleNamespace, ...]:
    return tuple(
        SimpleNamespace(
            plan=SimpleNamespace(client_order_id=f"order-{index}", reduce_only=False),
            state=ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
        )
        for index in range(count)
    )


def _worker(orders, *, budget: int, interval: float) -> LiveOrderReconciliation:
    return LiveOrderReconciliation(
        order_repository=SimpleNamespace(
            load_unresolved_orders=AsyncMock(return_value=orders)
        ),
        state_machine=SimpleNamespace(reconcile_order=AsyncMock()),
        run_id="run-1",
        max_order_lookups_per_pass=budget,
        interval_seconds=interval,
    )


@pytest.mark.asyncio
async def test_an_exhausted_pass_keeps_the_responsibility() -> None:
    """One pass spends exactly its budget and reports the work as unfinished."""
    worker = _worker(_unresolved(4), budget=2, interval=30.0)

    pending = await worker.reconcile_all()

    assert pending is True, (
        "unfinished lookups must keep the recovery responsibility instead of "
        "being treated as done"
    )
    assert worker.state_machine.reconcile_order.await_count == 2, (
        "a single pass must spend exactly its lookup budget, got "
        f"{worker.state_machine.reconcile_order.await_count}"
    )


@pytest.mark.asyncio
async def test_the_worker_waits_for_the_low_frequency_deadline() -> None:
    """The running worker must not immediately re-spend an exhausted budget."""
    worker = _worker(_unresolved(4), budget=2, interval=3600.0)
    worker.request_recovery()
    task = asyncio.create_task(worker.run_requested())
    try:
        # Wait (bounded) until the first pass has published its deadline.
        for _ in range(50):
            await asyncio.sleep(0.01)
            entry = {row["family"]: row for row in worker.recovery_status()}["orders"]
            if entry["retry_in_seconds"] is not None:
                break
        else:
            pytest.fail("the worker never published a low-frequency deadline")

        spent = worker.state_machine.reconcile_order.await_count
        assert spent == 2

        # Within the one-hour deadline the worker must stay parked, even though
        # the repository keeps reporting the same unresolved orders.
        await asyncio.sleep(0.2)
        assert worker.state_machine.reconcile_order.await_count == spent, (
            "the worker re-spent the budget before its low-frequency deadline: "
            f"{worker.state_machine.reconcile_order.await_count} lookups"
        )
        assert not task.done()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_a_difference_free_pass_does_not_recheck_confirmed_orders() -> None:
    """A resting order with no difference must not be re-queried every pass.

    ``reconcile_all`` is the normal low-frequency verification pass. A confirmed
    resting order that the durable ledger already agrees with is a no-difference
    observation, so the pass must not spend a lookup on it; an explicit
    full-repair request may still include it.
    """
    resting = SimpleNamespace(
        plan=SimpleNamespace(client_order_id="resting-1", reduce_only=False),
        state=ExchangeOrderState.ACKNOWLEDGED,
        executed_quantity=Decimal("0"),
    )
    worker = LiveOrderReconciliation(
        order_repository=SimpleNamespace(
            load_unresolved_orders=AsyncMock(return_value=(resting,))
        ),
        state_machine=SimpleNamespace(reconcile_order=AsyncMock()),
        run_id="run-1",
    )

    pending = await worker.reconcile_all()

    assert worker.state_machine.reconcile_order.await_count == 0, (
        "a difference-free resting order triggered a full re-query"
    )
    assert pending is False, "a difference-free pass has nothing left to repair"

    await worker.reconcile_all(include_confirmed=True)
    assert worker.state_machine.reconcile_order.await_count == 1, (
        "an explicit full repair must still include confirmed resting orders"
    )
