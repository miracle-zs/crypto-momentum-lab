"""Lifecycle helpers for live limit-entry orders.

The exchange owns the authoritative GTD expiry.  This small companion keeps a
local timer as a prompt best-effort cancellation path and restores timers for
known resting orders after a worker restart.  It deliberately does not cancel
entries when an exit is submitted: a later fill is an independent add-on and
the normal account-event/context path will make it the latest position anchor.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable
from datetime import UTC, datetime

import structlog

from crypto_momentum_lab.domain.execution import (
    ExchangeOrderEvent,
    OrderExecutionPlan,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionResult,
)
from crypto_momentum_lab.persistence.postgres.order_repository import (
    PersistedExchangeOrder,
)

log = structlog.get_logger()

CancelOrder = Callable[[OrderExecutionPlan], Awaitable[OrderExecutionResult]]
Clock = Callable[[], datetime]


class LiveLimitOrderLifecycle:
    """Restore and expire live GTD entry orders without blocking the daemon."""

    def __init__(
        self,
        *,
        cancel_order: CancelOrder,
        clock: Clock | None = None,
    ) -> None:
        self._cancel_order = cancel_order
        self._clock = clock or (lambda: datetime.now(tz=UTC))
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._stopped = False

    async def restore(self, orders: Iterable[PersistedExchangeOrder]) -> None:
        """Start expiry timers for unresolved GTD entry orders."""

        for order in orders:
            await self.track(order.plan, _result_from_persisted(order))

    async def track(
        self,
        plan: OrderExecutionPlan,
        result: OrderExecutionResult,
    ) -> None:
        """Track one newly submitted/restored order if it can still rest."""

        if (
            self._stopped
            or plan.reduce_only
            or plan.order_type.upper() != "LIMIT"
            or plan.expires_at is None
            or result.state.terminal
            or result.executed_quantity >= plan.quantity
        ):
            return
        previous = self._tasks.pop(plan.client_order_id, None)
        if previous is not None:
            previous.cancel()
        task = asyncio.create_task(
            self._expire(plan),
            name=f"live-entry-expiry:{plan.symbol}:{plan.client_order_id}",
        )
        self._tasks[plan.client_order_id] = task

        def on_done(completed: asyncio.Task[None]) -> None:
            self._task_done(plan.client_order_id, completed)

        task.add_done_callback(on_done)

    def observe(self, plan: OrderExecutionPlan, event: ExchangeOrderEvent) -> None:
        """Stop a timer as soon as an account/order event confirms completion."""

        if not plan.reduce_only and event.state.terminal:
            task = self._tasks.pop(plan.client_order_id, None)
            if task is not None and task is not asyncio.current_task():
                task.cancel()

    async def stop(self) -> None:
        """Stop local timers without sending exchange cancellations."""

        self._stopped = True
        tasks = tuple(self._tasks.values())
        self._tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _expire(self, plan: OrderExecutionPlan) -> None:
        assert plan.expires_at is not None
        delay = (plan.expires_at - self._clock()).total_seconds()
        if delay > 0:
            await asyncio.sleep(delay)
        try:
            result = await self._cancel_order(plan)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            log.warning(
                "live_entry_limit_expiry_cancel_failed",
                symbol=plan.symbol,
                client_order_id=plan.client_order_id,
                error_type=type(error).__name__,
            )
            return
        if not result.state.terminal:
            log.warning(
                "live_entry_limit_expiry_not_confirmed",
                symbol=plan.symbol,
                client_order_id=plan.client_order_id,
                state=result.state.value,
            )
        else:
            log.info(
                "live_entry_limit_expired",
                symbol=plan.symbol,
                client_order_id=plan.client_order_id,
                state=result.state.value,
                executed_quantity=str(result.executed_quantity),
            )

    def _task_done(
        self,
        client_order_id: str,
        task: asyncio.Task[None],
    ) -> None:
        if self._tasks.get(client_order_id) is task:
            self._tasks.pop(client_order_id, None)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            log.warning(
                "live_entry_limit_expiry_task_failed",
                client_order_id=client_order_id,
                error_type=type(error).__name__,
            )


def _result_from_persisted(order: PersistedExchangeOrder) -> OrderExecutionResult:
    return OrderExecutionResult(
        client_order_id=order.plan.client_order_id,
        state=order.state,
        exchange_order_id=order.exchange_order_id,
        executed_quantity=order.executed_quantity,
    )


__all__ = ["LiveLimitOrderLifecycle"]
