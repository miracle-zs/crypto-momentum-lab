"""SYSTEM.md section 8/6: an exhausted fast-query budget becomes low-frequency work.

"快查询预算耗尽，同证据连续账户事件｜不绕过退避、不热循环；转低频；相关新事实及
时处理，其他key公平运行" and Q9 "快速查证预算耗尽后告警、保持隔离与责任，转低频
查询".

Scope and limits of what is proven here:

* The worker's own ``time_source`` is injected, so the deadline is exact and no
  real sleeping or polling is used.
* Covered: a pass spends only its budget and the responsibility is retained.
* Covered: crossing the recorded deadline runs exactly one more bounded pass and
  publishes the next deadline again.
* Covered: ordinary account-fact notifications with the same evidence must not
  start another spending pass inside the low-frequency window (currently failing:
  the worker re-spends the budget).
* Covered: a difference-free verification pass does not spend a lookup on an
  already-confirmed resting order.
* NOT covered here: proof that a real UNKNOWN order command keeps its reservation
  and isolation through the coordinator (see test_execution_facts_acceptance.py),
  and any real-exchange evidence about not re-sending (see
  test_process_crash_recovery.py and the compensation-chain limits in SYSTEM.md 8.1).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderState,
    OrderExecutionPlan,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionStateMachine,
)
from crypto_momentum_lab.live_rollout.order_event_runtime import LiveOrderEventRuntime
from crypto_momentum_lab.live_rollout.order_reconciliation import (
    LiveOrderReconciliation,
)

pytestmark = pytest.mark.contract


class _Clock:
    """The worker's injectable time source."""

    def __init__(self, start: float = 1_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _unresolved(count: int) -> tuple[SimpleNamespace, ...]:
    return tuple(
        SimpleNamespace(
            plan=SimpleNamespace(client_order_id=f"order-{index}", reduce_only=False),
            state=ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
        )
        for index in range(count)
    )


def _worker(
    orders,
    *,
    budget: int,
    interval: float,
    clock: _Clock,
    reconciled: asyncio.Event | None = None,
) -> LiveOrderReconciliation:
    async def reconcile_order(plan):
        if reconciled is not None:
            reconciled.set()

    state_machine = SimpleNamespace(
        reconcile_order=AsyncMock(side_effect=reconcile_order)
    )
    return LiveOrderReconciliation(
        order_repository=SimpleNamespace(
            load_unresolved_orders=AsyncMock(return_value=orders)
        ),
        state_machine=state_machine,
        run_id="run-1",
        max_order_lookups_per_pass=budget,
        interval_seconds=interval,
        time_source=clock,
    )


@pytest.mark.asyncio
async def test_an_exhausted_pass_keeps_the_responsibility() -> None:
    """One pass spends exactly its budget and reports the work as unfinished."""
    worker = _worker(_unresolved(4), budget=2, interval=30.0, clock=_Clock())

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
async def test_ordinary_account_events_do_not_re_spend_an_exhausted_budget() -> None:
    """Same-evidence account events must not bypass the low-frequency retry.

    The worker's clock is frozen, so the low-frequency deadline is far in the
    future and no correct implementation may start another lookup pass.
    """
    clock = _Clock()
    reconciled = asyncio.Event()
    worker = _worker(
        _unresolved(4),
        budget=2,
        interval=3600.0,
        clock=clock,
        reconciled=reconciled,
    )
    worker.request_recovery()
    task = asyncio.create_task(worker.run_requested())
    try:
        await asyncio.wait_for(reconciled.wait(), timeout=1.0)
        for _ in range(5):
            await asyncio.sleep(0)
        spent = worker.state_machine.reconcile_order.await_count
        assert spent == 2

        for _ in range(5):
            worker.notify_account_facts_changed()
            await asyncio.sleep(0)

        assert worker.state_machine.reconcile_order.await_count == spent, (
            "ordinary account events bypassed the low-frequency retry and spent "
            f"the lookup budget again ({spent} -> "
            f"{worker.state_machine.reconcile_order.await_count})"
        )
        assert not task.done()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_account_events_during_recovery_do_not_re_spend_budget() -> None:
    """Same-evidence account events arriving during lookup must not bypass backoff.

    When an ordinary account-fact notification arrives while a pass is actively
    executing its lookups, the worker must not immediately schedule another pass
    if the finished pass exhausted its budget and backed off to a future deadline.
    """
    clock = _Clock()
    barrier = asyncio.Event()
    first_lookup_started = asyncio.Event()
    calls = 0

    async def reconcile_order(_plan) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            first_lookup_started.set()
            await barrier.wait()

    worker = LiveOrderReconciliation(
        order_repository=SimpleNamespace(
            load_unresolved_orders=AsyncMock(return_value=_unresolved(4))
        ),
        state_machine=SimpleNamespace(
            reconcile_order=AsyncMock(side_effect=reconcile_order)
        ),
        run_id="run-1",
        max_order_lookups_per_pass=2,
        interval_seconds=3600.0,
        time_source=clock,
    )
    worker.request_recovery()
    task = asyncio.create_task(worker.run_requested())
    try:
        # Wait until the first lookup is actively suspended inside the pass.
        await asyncio.wait_for(first_lookup_started.wait(), timeout=1.0)
        assert calls == 1

        # Send same-evidence account notifications while lookup is executing.
        for _ in range(5):
            worker.notify_account_facts_changed()
            await asyncio.sleep(0)

        # Release the first lookup so the pass can complete its 2-order budget.
        barrier.set()
        for _ in range(10):
            await asyncio.sleep(0)

        assert calls == 2, (
            "ordinary account events arriving during lookup bypassed the backoff "
            f"and re-spent the budget ({calls} > 2)"
        )
        assert not task.done()

        # The worker must be parked on its low-frequency deadline.
        status = {row["family"]: row for row in worker.recovery_status()}["orders"]
        assert status["retry_in_seconds"] is not None
        assert status["retry_in_seconds"] == pytest.approx(3600.0, abs=1.0)
    finally:
        barrier.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_material_order_evidence_during_recovery_executes_followup_pass() -> None:
    """Material order evidence during recovery executes a follow-up pass."""
    clock = _Clock()
    barrier = asyncio.Event()
    first_lookup_started = asyncio.Event()
    second_pass_started = asyncio.Event()
    calls = 0

    async def reconcile_order(_plan) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            first_lookup_started.set()
            await barrier.wait()
        elif calls >= 3:
            second_pass_started.set()

    worker = LiveOrderReconciliation(
        order_repository=SimpleNamespace(
            load_unresolved_orders=AsyncMock(return_value=_unresolved(4))
        ),
        state_machine=SimpleNamespace(
            reconcile_order=AsyncMock(side_effect=reconcile_order)
        ),
        run_id="run-1",
        max_order_lookups_per_pass=2,
        interval_seconds=3600.0,
        time_source=clock,
    )
    worker.request_recovery()
    task = asyncio.create_task(worker.run_requested())
    try:
        await asyncio.wait_for(first_lookup_started.wait(), timeout=1.0)
        assert calls == 1

        # Real new order evidence arrives during the pass
        worker.request_order_recovery(
            orphan_plans=(
                SimpleNamespace(
                    client_order_id="new-orphan-1",
                    reduce_only=False,
                ),
            )
        )

        barrier.set()
        # Follow-up pass must run to process the new evidence
        await asyncio.wait_for(second_pass_started.wait(), timeout=1.0)
        assert calls >= 3
    finally:
        barrier.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_crossing_the_deadline_runs_one_more_bounded_pass() -> None:
    """Before the deadline nothing runs; crossing it runs one more bounded pass.

    The worker's clock is injected. It waits on its own short retry interval and
    the clock is advanced past the recorded deadline synchronously. Progress is
    observed through events, so no polling or fixed sleep decides the outcome.
    Limitation: the injected clock is a step function, so the countdown shrinking
    with real time is not observable through it; the case asserts the republished
    deadline is a full interval ahead instead.
    """
    clock = _Clock()
    interval = 2.0
    spend_budget = asyncio.Event()
    calls = 0

    async def reconcile_order(_plan) -> None:
        nonlocal calls
        calls += 1
        if calls >= 2:
            spend_budget.set()

    worker = LiveOrderReconciliation(
        order_repository=SimpleNamespace(
            load_unresolved_orders=AsyncMock(return_value=_unresolved(4))
        ),
        state_machine=SimpleNamespace(
            reconcile_order=AsyncMock(side_effect=reconcile_order)
        ),
        run_id="run-1",
        max_order_lookups_per_pass=2,
        interval_seconds=interval,
        time_source=clock,
    )
    task = asyncio.create_task(worker.run_requested())
    try:
        worker.request_recovery()
        await asyncio.wait_for(spend_budget.wait(), timeout=1.0)
        assert calls == 2

        entry = {row["family"]: row for row in worker.recovery_status()}["orders"]
        assert entry["retry_in_seconds"] is not None
        deadline_in = entry["retry_in_seconds"]

        # Cross the recorded deadline in the worker's own clock; the worker then
        # wakes on its own retry interval and runs exactly one more pass.
        spend_budget.clear()
        clock.advance(deadline_in + 0.5)
        await asyncio.wait_for(spend_budget.wait(), timeout=5.0)
        assert calls == 4, (
            "crossing the deadline must run exactly one more bounded pass, got "
            f"{calls - 2} lookups"
        )
        status = {row["family"]: row for row in worker.recovery_status()}["orders"]
        next_in = status["retry_in_seconds"]
        assert next_in is not None, (
            "the responsibility and its next deadline must stay visible"
        )
        assert next_in == pytest.approx(interval, abs=0.5), (
            "the next deadline must be republished a full interval ahead, not "
            f"left at the expired one: {next_in}"
        )
        assert task.done() is False

        # The worker is parked on its low-frequency deadline: no pending wake-up
        # request remains, the deadline is still in the worker's future, and it
        # is scheduled to fire later. This is the barrier -- no real sleep is
        # used to let the worker "settle".
        assert not worker._requested.is_set(), (
            "a wake-up request is still pending, so the worker has not parked"
        )
        assert worker._retry_at["orders"] > clock.now
        assert worker._requested_tasks == set()

        # Draining the loop cannot add lookups while the deadline is in the
        # future, so a hot loop or a stale deadline would show up as extra calls.
        for _ in range(50):
            await asyncio.sleep(0)
        assert calls == 4, (
            "the worker hot-looped instead of waiting for the new deadline: "
            f"{calls - 4} extra lookups"
        )
        status_after = {row["family"]: row for row in worker.recovery_status()}[
            "orders"
        ]
        assert status_after["retry_in_seconds"] is not None, (
            "the next deadline must remain published while the worker waits"
        )
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


@pytest.mark.asyncio
async def test_order_event_runtime_unknown_result_does_not_self_trigger_retry() -> None:
    """The UNKNOWN outcome of an in-flight query must not self-trigger a retry pass.

    Real callback chain:
    GET returns not found -> State machine publishes UNKNOWN -> LiveOrderEventRuntime
    requests recovery -> order reconciliation must recognize this as the query's own
    unresolved result rather than a new evidence version.
    Duplicate evidence must not bypass the backoff deadline, while material new
    evidence must execute a follow-up pass.
    """
    clock = _Clock()
    now_dt = datetime(2026, 4, 1, 12, 0, tzinfo=UTC)
    plan = OrderExecutionPlan(
        client_order_id="order-unknown-1",
        symbol="BTCUSDT",
        side="BUY",
        position_side="LONG",
        order_type="MARKET",
        quantity=Decimal("1.0"),
        price=None,
        reduce_only=False,
        run_id="run-1",
        intent_id="intent-1",
        created_at=now_dt,
    )
    unresolved_order = SimpleNamespace(
        plan=plan,
        state=ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
        exchange_order_id=None,
        updated_at=now_dt,
        executed_quantity=Decimal("0"),
        terminal_receipt=None,
    )

    class _FakeOrderRepository:
        def __init__(self) -> None:
            self.events: list[ExchangeOrderEvent] = []

        async def record_order_observation(self, event, fills=()) -> bool:
            self.events.append(event)
            return True

        async def load_unresolved_orders(self, run_id: str):
            return (unresolved_order,)

        async def load_order(self, client_order_id: str):
            return unresolved_order

    repo = _FakeOrderRepository()
    exchange = SimpleNamespace()
    exchange.set_exchange_boundary_callbacks = lambda **kw: None
    query_calls = 0
    second_pass_executed = asyncio.Event()

    async def query_order(*args, **kwargs):
        nonlocal query_calls
        query_calls += 1
        if query_calls >= 2:
            second_pass_executed.set()
        await asyncio.sleep(0.001)
        return None

    exchange.query_order_by_client_id = query_order

    worker: LiveOrderReconciliation | None = None
    event_runtime = LiveOrderEventRuntime(
        telemetry=AsyncMock(),
        request_recovery=lambda plan=None: (
            worker.request_order_recovery(plan=plan) if worker else None
        ),
    )

    machine = OrderExecutionStateMachine(
        exchange=exchange,
        event_repository=repo,
        live_submit_enabled=True,
        clock=lambda: now_dt,
        reconciliation_retry_delays=(),
        on_event=event_runtime.handle,
    )
    machine.cancel_order = AsyncMock(
        return_value=SimpleNamespace(state=ExchangeOrderState.CANCELED)
    )

    worker = LiveOrderReconciliation(
        order_repository=repo,
        state_machine=machine,
        run_id="run-1",
        max_order_lookups_per_pass=2,
        interval_seconds=3600.0,
        time_source=clock,
    )

    worker.request_recovery()
    task = asyncio.create_task(worker.run_requested())
    try:
        await asyncio.sleep(0.02)
        assert query_calls == 1, (
            "UNKNOWN query result self-triggered an immediate re-query pass: "
            f"{query_calls} > 1"
        )
        status = {row["family"]: row for row in worker.recovery_status()}["orders"]
        assert status["retry_in_seconds"] == pytest.approx(3600.0, abs=1.0)

        # 1. Duplicate evidence and same-evidence notifications must not re-spend budget
        for _ in range(5):
            worker.notify_account_facts_changed()
            worker.request_order_recovery()
            await asyncio.sleep(0.002)

        assert query_calls == 1, (
            "duplicate evidence notifications bypassed backoff and re-spent budget: "
            f"{query_calls} > 1"
        )

        # 2. Material new evidence must trigger a follow-up recovery pass
        worker.request_order_recovery(
            orphan_plans=(
                SimpleNamespace(
                    client_order_id="new-orphan-1",
                    reduce_only=False,
                ),
            )
        )
        await asyncio.wait_for(second_pass_executed.wait(), timeout=1.0)
        assert query_calls == 2, (
            "material new evidence did not execute a follow-up recovery pass"
        )
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
