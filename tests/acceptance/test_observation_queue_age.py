"""SYSTEM.md section 6: queue oldest-wait age is computed from the current head.

"队列必须有界，最老等待时间按当前队首计算" — the reported age is the wait of the
item sitting at the head *now*. It must not be the age of a result that has
already been taken off the queue, and it must not keep growing while a consumed
result stays in flight.

The coordinator's monotonic clock is injected so the elapsed runtime is exact;
the observer's own scheduling still uses the real event loop.
"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from crypto_momentum_lab.domain.execution.order_result import OrderExecutionResult
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionCoordinator,
)

pytestmark = pytest.mark.contract

NOW = datetime(2026, 10, 12, 18, 0, tzinfo=UTC)
BASE_CLOCK = 5_000_000.0


class _StepClock:
    """A monotonic clock the test advances explicitly.

    ``stop_at`` freezes the clock at an instant so a backlog can be measured
    without also simulating the consumption that would clear it.
    """

    def __init__(self, start: float = BASE_CLOCK) -> None:
        self.now = start
        self.stop_at: float | None = None

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds
        if self.stop_at is not None and self.now >= self.stop_at:
            self.now = self.stop_at

    def freeze_at(self, seconds: float) -> None:
        self.stop_at = seconds

    def unfreeze(self) -> None:
        self.stop_at = None


def _plan(order_id: str) -> OrderExecutionPlan:
    return OrderExecutionPlan(
        intent_id=f"intent-{order_id}",
        run_id="run-1",
        client_order_id=order_id,
        symbol="BTCUSDT",
        side="BUY",
        order_type="MARKET",
        quantity=Decimal("1"),
        price=None,
        reduce_only=False,
        position_side=FuturesPositionSide.BOTH,
        created_at=NOW,
        quantized=True,
    )


def _result(order_id: str) -> OrderExecutionResult:
    return OrderExecutionResult(
        client_order_id=order_id,
        state=ExchangeOrderState.ACKNOWLEDGED,
        exchange_order_id="ex-1",
        executed_quantity=Decimal("0"),
        plan=_plan(order_id),
    )


def _coordinator(capacity: int = 8) -> OrderExecutionCoordinator:
    book = MagicMock()
    book.has_execution_unit_of_work = True
    book.require_command_recovery = MagicMock()
    return OrderExecutionCoordinator(
        backend=AsyncMock(),
        environment="live",
        account_label="primary",
        execution_book=book,
        order_observation_capacity=capacity,
    )


@contextmanager
def _observer_gated(monkeypatch: pytest.MonkeyPatch):
    """Start the real observer task but keep it parked until the gate opens.

    This only delays the observer's first step so the queue state under
    measurement is exact; the task itself is the real one throughout.
    """
    gate = asyncio.Event()
    real_create_task = asyncio.create_task

    def gated_create_task(coro, **kwargs):
        async def wait_for_gate() -> None:
            await gate.wait()
            await coro

        return real_create_task(wait_for_gate(), **kwargs)

    monkeypatch.setattr(asyncio, "create_task", gated_create_task)
    try:
        yield gate
    finally:
        monkeypatch.setattr(asyncio, "create_task", real_create_task)


async def _join_observer(coordinator: OrderExecutionCoordinator) -> None:
    task = coordinator._order_observer_task
    if task is not None and not task.done():
        await asyncio.wait_for(asyncio.shield(task), timeout=1.0)
    for _ in range(4):
        await asyncio.sleep(0)
        task = coordinator._order_observer_task
        if task is None or task.done():
            return


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> _StepClock:
    step = _StepClock()
    monkeypatch.setattr(
        "crypto_momentum_lab.execution_account.orders.coordinator.time.monotonic",
        step,
    )
    return step


@pytest.mark.asyncio
async def test_real_backlog_is_reported_and_a_consumed_head_stops_counting(
    clock: _StepClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A genuine six-hour backlog reports six hours; its successor does not.

    order-1..4 sit pending in the queue for six hours. That backlog is real and
    must be reported. Once the observer takes order-1 off the queue, only the
    successors are pending; their wait starts from their own arrival, so the
    consumed head's six hours must no longer be charged to them.
    """
    coordinator = _coordinator(capacity=8)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def staged_apply(_plan: object, _result: object) -> None:
        entered.set()
        await release.wait()

    with _observer_gated(monkeypatch) as gate:
        coordinator._apply_returned_order_result = staged_apply
        coordinator._defer_order_projection(_plan("order-1"), _result("order-1"))
        for index in (2, 3, 4):
            coordinator._defer_order_projection(
                _plan(f"order-{index}"), _result(f"order-{index}")
            )
        clock.advance(6 * 60 * 60.0)
        backlog = coordinator.order_observation_metrics
        assert backlog.depth == 4
        assert backlog.dequeued_count == 0
        assert backlog.oldest_pending_age_seconds == pytest.approx(21600.0), (
            "pending items that have waited six hours must report six hours: "
            f"{backlog.oldest_pending_age_seconds}"
        )
        gate.set()

    # The real observer takes order-1. order-2..4 have been pending
    # since t0, so their own wait is the full six hours, not more.
    await asyncio.wait_for(entered.wait(), timeout=1.0)
    assert coordinator.order_observation_metrics.depth == 3
    clock.advance(2.0)
    successor = coordinator.order_observation_metrics
    assert successor.dequeued_count == 1
    assert successor.depth == 3
    assert successor.oldest_pending_age_seconds is not None
    assert successor.oldest_pending_age_seconds == pytest.approx(21602.0, abs=5.0), (
        "the queue's reported wait must follow the results that are actually "
        f"pending, got {successor.oldest_pending_age_seconds}"
    )

    release.set()
    coordinator._apply_returned_order_result = AsyncMock()
    await _join_observer(coordinator)


@pytest.mark.asyncio
async def test_age_does_not_grow_for_results_already_consumed(
    clock: _StepClock,
) -> None:
    """A never-drained queue must not keep ageing results that were consumed.

    Every result in this queue is committed within four seconds of arriving, so
    no pending item can have waited more than that. The metric must follow that
    fact instead of growing with the runtime of the results already in flight.
    """
    coordinator = _coordinator(capacity=8)
    release = asyncio.Event()

    async def staged_apply(_plan: object, _result: object) -> None:
        await release.wait()

    coordinator._apply_returned_order_result = staged_apply
    coordinator._defer_order_projection(_plan("order-1"), _result("order-1"))
    for index in (2, 3, 4):
        coordinator._defer_order_projection(
            _plan(f"order-{index}"), _result(f"order-{index}")
        )
    # Only order-1 is taken; the successors keep the queue non-empty.
    for _ in range(3):
        await asyncio.sleep(0)
    assert coordinator.order_observation_metrics.dequeued_count == 1

    clock.advance(1800.0)
    metrics = coordinator.order_observation_metrics
    assert metrics.depth == 3
    assert metrics.oldest_pending_age_seconds is not None
    assert metrics.oldest_pending_age_seconds <= 60.0, (
        "the wait of results that were consumed is still charged to the pending "
        f"queue: {metrics.oldest_pending_age_seconds}"
    )

    release.set()
    coordinator._apply_returned_order_result = AsyncMock()
    await _join_observer(coordinator)


@pytest.mark.asyncio
async def test_pending_head_wait_is_anchored_to_its_own_arrival(
    clock: _StepClock,
) -> None:
    """A consumed result in flight must not age the results waiting behind it."""
    coordinator = _coordinator(capacity=8)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def staged_apply(_plan: object, _result: object) -> None:
        entered.set()
        await release.wait()

    coordinator._apply_returned_order_result = staged_apply
    coordinator._defer_order_projection(_plan("order-1"), _result("order-1"))
    await asyncio.wait_for(entered.wait(), timeout=1.0)
    assert coordinator.order_observation_metrics.depth == 0

    # order-1 is off the queue and its commit is blocked. An hour of runtime
    # passes before the successors arrive, so from their arrival the queue is
    # non-empty and their reported wait must start from that arrival.
    clock.advance(3600.0)
    for index in (2, 3):
        coordinator._defer_order_projection(
            _plan(f"order-{index}"), _result(f"order-{index}")
        )
    clock.advance(2.0)
    metrics = coordinator.order_observation_metrics
    assert metrics.depth == 2
    assert metrics.dequeued_count == 1
    assert metrics.oldest_pending_age_seconds is not None
    assert metrics.oldest_pending_age_seconds <= 100.0, (
        "the waiting results' age is anchored to a consumed result instead of "
        f"their own arrival: {metrics.oldest_pending_age_seconds}"
    )

    release.set()
    coordinator._apply_returned_order_result = AsyncMock()
    await _join_observer(coordinator)


@pytest.mark.asyncio
async def test_draining_the_queue_clears_the_oldest_age() -> None:
    """An empty queue has no oldest item, so the metric is absent."""
    coordinator = _coordinator(capacity=8)
    apply_result = AsyncMock()
    coordinator._apply_returned_order_result = apply_result
    coordinator._defer_order_projection(_plan("order-1"), _result("order-1"))
    await _join_observer(coordinator)

    metrics = coordinator.order_observation_metrics
    assert metrics.dequeued_count == 1
    assert metrics.depth == 0
    assert metrics.oldest_pending_age_seconds is None
    apply_result.assert_awaited_once()
