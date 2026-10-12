"""SYSTEM.md section 6: queue oldest-wait age is computed from the current head.

"队列必须有界，最老等待时间按当前队首计算" — the reported age is the wait of the
item sitting at the head *now*, not the enqueue time of the first item that
happened to arrive after the queue was last empty.  A long-running, continuously
consumed queue must therefore not report a growing, fictional backpressure.

Each case holds the real observer task inside its first projection commit, so the
observation queue stays genuinely non-empty while the metric is read.  The
coordinator's monotonic clock is patched to model an elapsing runtime; the
observer's own scheduling still uses the real event loop.
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
BASE_CLOCK = 5_000_000.0


class _StepClock:
    """A monotonic clock the test advances explicitly."""

    def __init__(self, start: float = BASE_CLOCK) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


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
async def test_a_long_running_queue_reports_the_current_heads_wait(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A never-drained queue must not age every new head from the first arrival.

    Three results queue up behind a blocked commit. After six simulated hours
    of runtime their head is still one of the three that were waiting, so the
    reported oldest wait must stay in the same order of magnitude as the time
    they actually waited.
    """
    clock = _StepClock()
    monkeypatch.setattr(
        "crypto_momentum_lab.execution_account.orders.coordinator.time.monotonic",
        clock,
    )
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

    # Results keep arriving while the same result stays in flight, so the queue
    # never drains and its head is one of these three.
    for index in (2, 3, 4):
        coordinator._defer_order_projection(
            _plan(f"order-{index}"), _result(f"order-{index}")
        )

    clock.advance(60.0 * 60.0 * 6.0)
    metrics = coordinator.order_observation_metrics
    assert metrics.depth == 3
    assert metrics.dequeued_count == 1
    assert metrics.oldest_pending_age_seconds is not None
    assert metrics.oldest_pending_age_seconds < 600.0, (
        "the whole runtime was charged to a head that only just arrived: "
        f"{metrics.oldest_pending_age_seconds:.1f}s"
    )

    release.set()
    coordinator._apply_returned_order_result = AsyncMock()
    await _join_observer(coordinator)


@pytest.mark.asyncio
async def test_pending_head_is_anchored_to_its_own_arrival(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The wait is measured from the pending head's arrival, not from t0."""
    clock = _StepClock()
    monkeypatch.setattr(
        "crypto_momentum_lab.execution_account.orders.coordinator.time.monotonic",
        clock,
    )
    coordinator = _coordinator(capacity=8)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def staged_apply(_plan: object, _result: object) -> None:
        entered.set()
        await release.wait()

    coordinator._apply_returned_order_result = staged_apply
    coordinator._defer_order_projection(_plan("order-1"), _result("order-1"))
    await asyncio.wait_for(entered.wait(), timeout=1.0)

    # One hour of runtime, then the pending head arrives and waits 2s.
    clock.advance(3600.0)
    for index in (2, 3):
        coordinator._defer_order_projection(
            _plan(f"order-{index}"), _result(f"order-{index}")
        )
    clock.advance(2.0)
    metrics = coordinator.order_observation_metrics
    assert metrics.depth == 2
    assert metrics.oldest_pending_age_seconds is not None
    assert metrics.oldest_pending_age_seconds <= 3.0, (
        "the head's wait is anchored to t0 instead of its own arrival: "
        f"{metrics.oldest_pending_age_seconds:.1f}s"
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
