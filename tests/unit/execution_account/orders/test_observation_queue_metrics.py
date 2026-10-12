"""Observation-queue backpressure must be measurable and recoverable."""

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


def _plan(order_id: str = "order-1") -> OrderExecutionPlan:
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
        created_at=__import__("datetime").datetime(
            2026, 10, 12, tzinfo=__import__("datetime").UTC
        ),
        quantized=True,
    )


def _result(order_id: str = "order-1") -> OrderExecutionResult:
    return OrderExecutionResult(
        client_order_id=order_id,
        state=ExchangeOrderState.ACKNOWLEDGED,
        exchange_order_id="ex-1",
        executed_quantity=Decimal("0"),
        plan=_plan(order_id),
    )


def _coordinator(capacity: int = 2) -> OrderExecutionCoordinator:
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


@pytest.mark.asyncio
async def test_observation_queue_depth_and_oldest_age_are_reported() -> None:
    coordinator = _coordinator(capacity=4)

    assert coordinator.order_observation_metrics.depth == 0
    assert coordinator.order_observation_metrics.oldest_pending_age_seconds is None

    coordinator._defer_order_projection(_plan("order-1"), _result("order-1"))
    coordinator._defer_order_projection(_plan("order-2"), _result("order-2"))

    metrics = coordinator.order_observation_metrics
    assert metrics.capacity == 4
    assert metrics.depth == 2
    assert metrics.high_watermark == 2
    assert metrics.enqueued_count == 2
    assert metrics.dequeued_count == 0
    assert metrics.overflow_count == 0
    assert metrics.oldest_pending_age_seconds is not None


@pytest.mark.asyncio
async def test_observation_overflow_is_counted_and_routed_to_recovery() -> None:
    coordinator = _coordinator(capacity=1)
    coordinator._defer_order_projection(_plan("order-1"), _result("order-1"))
    # The queue is full: the second result must not be dropped silently.
    coordinator._defer_order_projection(_plan("order-2"), _result("order-2"))

    metrics = coordinator.order_observation_metrics
    assert metrics.depth == 1
    assert metrics.overflow_count == 1
    assert metrics.enqueued_count == 1
    coordinator._execution_book.require_command_recovery.assert_called_once_with(
        "order-2"
    )


@pytest.mark.asyncio
async def test_draining_the_queue_clears_the_oldest_age() -> None:
    coordinator = _coordinator(capacity=4)
    apply_result = AsyncMock()
    coordinator._apply_returned_order_result = apply_result
    coordinator._defer_order_projection(_plan("order-1"), _result("order-1"))

    await coordinator._consume_order_observations()

    metrics = coordinator.order_observation_metrics
    assert metrics.depth == 0
    assert metrics.dequeued_count == 1
    assert metrics.oldest_pending_age_seconds is None
    apply_result.assert_awaited_once()
