"""SYSTEM.md section 6: observation-queue health is bounded and never hangs.

"队列必须有界，最老等待时间按当前队首计算" — this file covers the bounded/closed
half of that row through the real coordinator. The oldest-wait half is reported as
not executable: no public seam currently isolates the coordinator's own clock, and
substituting the shared ``time.monotonic`` would also drive asyncio's timeouts, so
a frozen-clock age assertion cannot be made bounded and honest. See SYSTEM.md 8.1
for the missing seam.
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
async def test_overflow_is_counted_and_routed_to_recovery() -> None:
    """A full queue must never drop a fact silently."""
    coordinator = _coordinator(capacity=1)
    coordinator._defer_order_projection(_plan("order-1"), _result("order-1"))
    coordinator._defer_order_projection(_plan("order-2"), _result("order-2"))

    metrics = coordinator.order_observation_metrics
    assert metrics.capacity == 1
    assert metrics.depth == 1
    assert metrics.overflow_count == 1
    coordinator._execution_book.require_command_recovery.assert_called_once_with(
        "order-2"
    )

    coordinator._apply_returned_order_result = AsyncMock()
    await _join_observer(coordinator)


@pytest.mark.asyncio
async def test_queue_close_is_bounded_and_keeps_every_pending_fact() -> None:
    """Closing must not hang, and no pending fact may be lost."""
    coordinator = _coordinator(capacity=8)
    coordinator._apply_returned_order_result = AsyncMock()
    coordinator._defer_order_projection(_plan("order-1"), _result("order-1"))
    coordinator._defer_order_projection(_plan("order-2"), _result("order-2"))

    await asyncio.wait_for(coordinator.aclose(), timeout=2.0)

    coordinator._execution_book.require_command_recovery.assert_called()
    routed = {
        call.args[0]
        for call in coordinator._execution_book.require_command_recovery.call_args_list
    }
    assert routed <= {"order-1", "order-2"}
    assert coordinator.order_observation_metrics.depth == 0


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
