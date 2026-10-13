"""SYSTEM.md section 6: observation-queue health is bounded and never hangs.

"队列必须有界，最老等待时间按当前队首计算" — this file covers the bounded/closed
half and the oldest-wait half through the real coordinator.  The coordinator now
accepts an injectable ``clock`` so the test can control monotonic time without
touching the global ``time.monotonic`` or asyncio's timeout machinery.
"""

from __future__ import annotations

import asyncio
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


class _StepClock:
    """An injectable monotonic clock the test advances explicitly."""

    def __init__(self, start: float = 1000.0) -> None:
        self._now = start

    def __call__(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds


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


def _coordinator(
    capacity: int = 8, clock: _StepClock | None = None
) -> OrderExecutionCoordinator:
    book = MagicMock()
    book.has_execution_unit_of_work = True
    book.require_command_recovery = MagicMock()
    return OrderExecutionCoordinator(
        backend=AsyncMock(),
        environment="live",
        account_label="primary",
        execution_book=book,
        order_observation_capacity=capacity,
        clock=clock if clock is not None else _StepClock(),
    )


async def _join_observer(coordinator: OrderExecutionCoordinator) -> None:
    task = coordinator._order_observer_task
    if task is not None and not task.done():
        await asyncio.wait_for(asyncio.shield(task), timeout=1.0)
    for _ in range(4):
        await asyncio.sleep(0)
        task = coordinator._order_observer_task
        if task is None or task.done():
            return


@pytest.mark.asyncio
async def test_overflow_is_counted_and_routed_to_recovery() -> None:
    """A full queue must never drop a fact silently."""
    coordinator = _coordinator(capacity=1)
    try:
        coordinator._defer_order_projection(_plan("order-1"), _result("order-1"))
        coordinator._defer_order_projection(_plan("order-2"), _result("order-2"))

        metrics = coordinator.order_observation_metrics
        assert metrics.capacity == 1
        assert metrics.depth == 1
        assert metrics.overflow_count == 1
        coordinator._execution_book.require_command_recovery.assert_called_once_with(
            "order-2"
        )
    finally:
        coordinator._apply_returned_order_result = AsyncMock()
        await coordinator.aclose()


@pytest.mark.asyncio
async def test_queue_close_is_bounded_and_keeps_every_pending_fact() -> None:
    """Closing must not hang, and no pending fact may be lost."""
    coordinator = _coordinator(capacity=8)
    coordinator._apply_returned_order_result = AsyncMock()
    coordinator._defer_order_projection(_plan("order-1"), _result("order-1"))
    coordinator._defer_order_projection(_plan("order-2"), _result("order-2"))

    try:
        await asyncio.wait_for(coordinator.aclose(), timeout=2.0)

        # Both pending facts must be either committed or routed to recovery:
        # neither may be dropped just because the queue was closing.
        routed = {
            call.args[0]
            for call in (
                coordinator._execution_book.require_command_recovery.call_args_list
            )
        }
        applied = {
            call.args[1].client_order_id
            for call in coordinator._apply_returned_order_result.await_args_list
        }
        assert routed | applied == {"order-1", "order-2"}, (
            f"closing lost a pending fact: routed={routed} applied={applied}"
        )
        assert coordinator.order_observation_metrics.depth == 0
    finally:
        coordinator._apply_returned_order_result = AsyncMock()
        await coordinator.aclose()


@pytest.mark.asyncio
async def test_draining_the_queue_clears_the_oldest_age() -> None:
    """An empty queue has no oldest item, so the metric is absent."""
    clock = _StepClock()
    coordinator = _coordinator(capacity=8, clock=clock)
    apply_result = AsyncMock()
    coordinator._apply_returned_order_result = apply_result
    try:
        coordinator._defer_order_projection(_plan("order-1"), _result("order-1"))
        await _join_observer(coordinator)

        metrics = coordinator.order_observation_metrics
        assert metrics.dequeued_count == 1
        assert metrics.depth == 0
        assert metrics.oldest_pending_age_seconds is None
        apply_result.assert_awaited_once()
    finally:
        await coordinator.aclose()


@pytest.mark.asyncio
async def test_oldest_age_tracks_the_current_queue_head() -> None:
    """Oldest age must update to the new head after the first item is dequeued.

    Uses an injectable component clock so the assertion is bounded and
    deterministic without touching asyncio's global clock.  Event barriers
    control the consumer's progress so we can inspect between items.
    """
    clock = _StepClock(start=100.0)
    coordinator = _coordinator(capacity=8, clock=clock)
    processed: list[str] = []
    entered_first = asyncio.Event()
    entered_second = asyncio.Event()
    release_first = asyncio.Event()
    release_second = asyncio.Event()

    async def gated_apply(plan, result):
        if result.client_order_id == "order-1":
            entered_first.set()
            await asyncio.wait_for(release_first.wait(), timeout=2.0)
        else:
            entered_second.set()
            await asyncio.wait_for(release_second.wait(), timeout=2.0)
        processed.append(result.client_order_id)

    coordinator._apply_returned_order_result = gated_apply
    try:
        # 1. Enqueue synchronously — worker not yet running.
        coordinator._defer_order_projection(_plan("order-1"), _result("order-1"))
        clock.advance(5.0)
        coordinator._defer_order_projection(_plan("order-2"), _result("order-2"))

        metrics = coordinator.order_observation_metrics
        assert metrics.depth == 2
        assert metrics.oldest_pending_age_seconds == pytest.approx(5.0)

        # 2. Advance clock to 108. Consumer picks up order-1, enters
        #    gated_apply, sets entered_first, blocks on release_first.
        clock.advance(3.0)
        await asyncio.wait_for(entered_first.wait(), timeout=2.0)

        # 3. order-1 is processing, order-2 still in queue.
        metrics = coordinator.order_observation_metrics
        assert metrics.depth == 1, (
            "first item dequeued, second still queued"
        )
        # order-2 enqueued at clock=105; current clock=108; age=3s.
        # Old bug: would report 8s (108-100) from stale first-item timestamp.
        assert metrics.oldest_pending_age_seconds == pytest.approx(3.0)

        # 4. Release first; wait for second to enter gated_apply.
        release_first.set()
        await asyncio.wait_for(entered_second.wait(), timeout=2.0)

        metrics = coordinator.order_observation_metrics
        assert metrics.depth == 0
        assert metrics.oldest_pending_age_seconds is None

        # Release second, join consumer, verify both facts completed.
        release_second.set()
        task = coordinator._order_observer_task
        if task is not None:
            await asyncio.wait_for(asyncio.shield(task), timeout=2.0)
        assert processed == ["order-1", "order-2"]
    finally:
        release_first.set()
        release_second.set()
        coordinator._apply_returned_order_result = AsyncMock()
        await coordinator.aclose()
