import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.execution import (
    ExchangeOrderEvent,
    ExchangeOrderState,
    OrderExecutionPlan,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionResult,
)
from crypto_momentum_lab.live_rollout.entry_orders import LiveLimitOrderLifecycle

NOW = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)


async def test_limit_entry_is_cancelled_when_its_gtd_window_expires() -> None:
    cancelled = asyncio.Event()
    cancelled_plans: list[OrderExecutionPlan] = []

    async def cancel_order(plan: OrderExecutionPlan) -> OrderExecutionResult:
        cancelled_plans.append(plan)
        cancelled.set()
        return OrderExecutionResult(
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.CANCELED,
            exchange_order_id="exchange-1",
        )

    lifecycle = LiveLimitOrderLifecycle(
        cancel_order=cancel_order,
        clock=lambda: NOW,
    )
    plan = _plan(expires_at=NOW + timedelta(milliseconds=10))

    await lifecycle.track(plan, _acknowledged(plan))
    await asyncio.wait_for(cancelled.wait(), timeout=1)
    await lifecycle.stop()

    assert cancelled_plans == [plan]


async def test_stopping_worker_does_not_cancel_live_entry_on_exchange() -> None:
    cancelled = False

    async def cancel_order(_plan: OrderExecutionPlan) -> OrderExecutionResult:
        nonlocal cancelled
        cancelled = True
        return OrderExecutionResult(
            client_order_id=_plan.client_order_id,
            state=ExchangeOrderState.CANCELED,
            exchange_order_id="exchange-1",
        )

    lifecycle = LiveLimitOrderLifecycle(
        cancel_order=cancel_order,
        clock=lambda: NOW,
    )
    plan = _plan(expires_at=NOW + timedelta(hours=1))

    await lifecycle.track(plan, _acknowledged(plan))
    await lifecycle.stop()

    assert cancelled is False


async def test_terminal_order_event_removes_expiry_timer() -> None:
    cancelled = False

    async def cancel_order(_plan: OrderExecutionPlan) -> OrderExecutionResult:
        nonlocal cancelled
        cancelled = True
        return OrderExecutionResult(
            client_order_id=_plan.client_order_id,
            state=ExchangeOrderState.CANCELED,
            exchange_order_id="exchange-1",
        )

    lifecycle = LiveLimitOrderLifecycle(
        cancel_order=cancel_order,
        clock=lambda: NOW,
    )
    plan = _plan(expires_at=NOW + timedelta(milliseconds=20))

    await lifecycle.track(plan, _acknowledged(plan))
    lifecycle.observe(
        plan,
        ExchangeOrderEvent(
            event_id="event-1",
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.FILLED,
            occurred_at=NOW,
            exchange_order_id="exchange-1",
            details={},
        ),
    )
    await asyncio.sleep(0.05)
    await lifecycle.stop()

    assert cancelled is False


def _plan(*, expires_at: datetime) -> OrderExecutionPlan:
    return OrderExecutionPlan(
        intent_id="intent-1",
        run_id="run-1",
        client_order_id="client-1",
        symbol="BTCUSDT",
        side="BUY",
        order_type="LIMIT",
        quantity=Decimal("0.001"),
        price=Decimal("30000"),
        reduce_only=False,
        created_at=NOW,
        quantized=True,
        time_in_force="GTD",
        expires_at=expires_at,
    )


def _acknowledged(plan: OrderExecutionPlan) -> OrderExecutionResult:
    return OrderExecutionResult(
        client_order_id=plan.client_order_id,
        state=ExchangeOrderState.ACKNOWLEDGED,
        exchange_order_id="exchange-1",
    )
