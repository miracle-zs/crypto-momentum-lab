"""Priority-aware coordination for live order commands.

The coordinator is the live execution seam.  It keeps commands for one
account/symbol/position side serial, while allowing unrelated symbols to make
progress independently.  Reconciliation is deliberately lowest priority so
an unknown REST read cannot hold an order command for another symbol hostage.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Protocol, cast

from crypto_momentum_lab.domain.execution import (
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionResult,
    PreparedOrderSubmission,
)


class OrderExecutionPort(Protocol):
    async def execute_approved_intent(
        self,
        plan: OrderExecutionPlan,
        *,
        prepared_submission: PreparedOrderSubmission | None = None,
    ) -> OrderExecutionResult: ...

    async def reconcile_order(
        self,
        plan: OrderExecutionPlan,
    ) -> OrderExecutionResult: ...

    async def cancel_order(
        self,
        plan: OrderExecutionPlan,
    ) -> OrderExecutionResult: ...


OrderExecutionBackend = OrderExecutionPort


@dataclass(frozen=True, slots=True)
class OrderExecutionKey:
    account_label: str
    symbol: str
    position_side: FuturesPositionSide


class _KeyCommandScheduler:
    """One priority queue for one position serialization key."""

    def __init__(self, key: OrderExecutionKey) -> None:
        self._key = key
        self._queue: asyncio.PriorityQueue[tuple[int, int, Any, Any]] = (
            asyncio.PriorityQueue()
        )
        self._sequence = 0
        self._closed = False
        self._worker = asyncio.create_task(
            self._run(),
            name=(
                "live-order-key-"
                f"{key.symbol.lower()}-{key.position_side.value.lower()}"
            ),
        )

    async def submit(
        self,
        *,
        priority: int,
        operation: Callable[[], Awaitable[Any]],
    ) -> Any:
        if self._closed:
            raise RuntimeError("order command scheduler is closed")
        future = asyncio.get_running_loop().create_future()
        sequence = self._sequence
        self._sequence += 1
        await self._queue.put((priority, sequence, operation, future))
        return await future

    async def close(self) -> None:
        if self._closed:
            await self._worker
            return
        self._closed = True
        await self._queue.put((2**31 - 1, self._sequence, None, None))
        await self._worker

    async def _run(self) -> None:
        while True:
            _priority, _sequence, operation, future = await self._queue.get()
            try:
                if operation is None:
                    return
                try:
                    result = await operation()
                except BaseException as error:
                    if future is not None and not future.done():
                        future.set_exception(error)
                else:
                    if future is not None and not future.done():
                        future.set_result(result)
            finally:
                self._queue.task_done()


class OrderExecutionCoordinator:
    """Coordinate live submit, cancel, and reconcile commands.

    Interface invariants:

    * commands for the same account/symbol/position side execute one at a
      time;
    * reduce-only submit and cancel outrank entry submit, and all commands
      outrank reconciliation;
    * commands for different symbols do not wait on one another;
    * the wrapped state machine remains responsible for durable state
      transitions, idempotency, and exchange outcome recovery.
    """

    _EXIT_PRIORITY = 0
    _ENTRY_PRIORITY = 10
    _RECONCILE_PRIORITY = 20

    def __init__(
        self,
        *,
        backend: OrderExecutionPort,
        account_label: str,
    ) -> None:
        if not account_label.strip():
            raise ValueError("account_label must not be empty")
        self._backend = backend
        self._account_label = account_label.strip()
        self._schedulers: dict[OrderExecutionKey, _KeyCommandScheduler] = {}

    async def submit(
        self,
        plan: OrderExecutionPlan,
        *,
        prepared_submission: PreparedOrderSubmission | None = None,
    ) -> OrderExecutionResult:
        priority = (
            self._EXIT_PRIORITY if plan.reduce_only else self._ENTRY_PRIORITY
        )

        async def operation() -> OrderExecutionResult:
            if prepared_submission is None:
                return await self._backend.execute_approved_intent(plan)
            return await self._backend.execute_approved_intent(
                plan,
                prepared_submission=prepared_submission,
            )

        return cast(
            OrderExecutionResult,
            await self._schedule(plan, priority=priority, operation=operation),
        )

    async def execute_approved_intent(
        self,
        plan: OrderExecutionPlan,
        *,
        prepared_submission: PreparedOrderSubmission | None = None,
    ) -> OrderExecutionResult:
        """Compatibility name used by existing live and shadow call sites."""

        return await self.submit(plan, prepared_submission=prepared_submission)

    async def cancel_order(self, plan: OrderExecutionPlan) -> OrderExecutionResult:
        priority = self._EXIT_PRIORITY if plan.reduce_only else self._ENTRY_PRIORITY
        return cast(
            OrderExecutionResult,
            await self._schedule(
                plan,
                priority=priority,
                operation=lambda: self._backend.cancel_order(plan),
            ),
        )

    async def reconcile_order(
        self,
        plan: OrderExecutionPlan,
    ) -> OrderExecutionResult:
        return cast(
            OrderExecutionResult,
            await self._schedule(
                plan,
                priority=self._RECONCILE_PRIORITY,
                operation=lambda: self._backend.reconcile_order(plan),
            ),
        )

    async def aclose(self) -> None:
        schedulers = tuple(self._schedulers.values())
        self._schedulers.clear()
        if schedulers:
            await asyncio.gather(*(scheduler.close() for scheduler in schedulers))

    async def _schedule(
        self,
        plan: OrderExecutionPlan,
        *,
        priority: int,
        operation: Callable[[], Awaitable[Any]],
    ) -> Any:
        key = OrderExecutionKey(
            account_label=self._account_label,
            symbol=plan.symbol.strip().upper(),
            position_side=plan.position_side,
        )
        scheduler = self._schedulers.get(key)
        if scheduler is None:
            scheduler = _KeyCommandScheduler(key)
            self._schedulers[key] = scheduler
        return await scheduler.submit(priority=priority, operation=operation)


__all__ = [
    "OrderExecutionCoordinator",
    "OrderExecutionKey",
    "OrderExecutionBackend",
    "OrderExecutionPort",
]
