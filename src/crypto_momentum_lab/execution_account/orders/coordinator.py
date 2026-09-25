"""Priority-aware coordination for live order commands.

The coordinator is the live execution seam.  It keeps commands for one
account/symbol/position side serial, while allowing unrelated symbols to make
progress independently.  Reconciliation is deliberately lowest priority so
an unknown REST read cannot hold an order command for another symbol hostage.
"""

from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Protocol, cast

import structlog

from crypto_momentum_lab.domain.execution import (
    ExchangeOrderSnapshot,
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.execution_coordinator import (
    ExecutionCoordinator,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
from crypto_momentum_lab.domain.execution.trade_command import PositionReservation
from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionResult,
    OrderPreSubmissionError,
    PreparedOrderSubmission,
)

log = structlog.get_logger()


async def _maybe_await(val: Any) -> Any:
    if inspect.isawaitable(val):
        return await val
    return val


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

    async def apply_observed_snapshot(
        self,
        plan: OrderExecutionPlan,
        snapshot: ExchangeOrderSnapshot,
    ) -> OrderExecutionResult: ...

    async def mark_absent_reconciled(
        self,
        plan: OrderExecutionPlan,
        *,
        details: dict[str, JsonValue],
    ) -> OrderExecutionResult: ...


OrderExecutionBackend = OrderExecutionPort


@dataclass(frozen=True, slots=True)
class OrderExecutionKey:
    account_label: str
    symbol: str
    position_side: FuturesPositionSide


class _KeyCommandScheduler:
    """One bounded priority queue for one position serialization key."""

    def __init__(
        self,
        key: OrderExecutionKey,
        *,
        max_queue_depth: int = 64,
        exit_headroom: int = 16,
        max_queue_wait_seconds: float = 30.0,
        idle_timeout_seconds: float = 120.0,
        on_idle: Callable[[OrderExecutionKey], None] | None = None,
    ) -> None:
        self._key = key
        self._max_queue_depth = max_queue_depth
        self._exit_headroom = exit_headroom
        self._max_queue_wait_seconds = max_queue_wait_seconds
        self._idle_timeout_seconds = idle_timeout_seconds
        self._on_idle = on_idle
        self._queue: asyncio.PriorityQueue[tuple[int, int, Any, Any, float]] = (
            asyncio.PriorityQueue(maxsize=max_queue_depth)
        )
        self._state_lock = asyncio.Lock()
        self._sequence = 0
        self._closed = False
        self._worker = asyncio.create_task(
            self._run(),
            name=(
                "live-order-key-"
                f"{key.symbol.lower()}-{key.position_side.value.lower()}"
            ),
        )

    @property
    def is_closed(self) -> bool:
        return self._closed

    @property
    def qsize(self) -> int:
        return self._queue.qsize()

    async def submit(
        self,
        *,
        priority: int,
        operation: Callable[[], Awaitable[Any]],
    ) -> Any:
        future = asyncio.get_running_loop().create_future()
        started = asyncio.Event()
        async with self._state_lock:
            if self._closed:
                raise RuntimeError("order command scheduler is closed")

            is_exit = (priority <= OrderExecutionCoordinator._EXIT_PRIORITY)
            current_depth = self._queue.qsize()

            if is_exit:
                if current_depth >= self._max_queue_depth:
                    raise OrderPreSubmissionError(
                        f"order scheduler queue full ({current_depth}/{self._max_queue_depth}) "
                        f"for key {self._key.symbol}:{self._key.position_side.value}"
                    )
            else:
                entry_limit = max(1, self._max_queue_depth - self._exit_headroom)
                if current_depth >= entry_limit:
                    raise OrderPreSubmissionError(
                        f"order scheduler entry capacity exceeded ({current_depth}/{entry_limit}, "
                        f"headroom={self._exit_headroom}) for key {self._key.symbol}:{self._key.position_side.value}"
                    )

            sequence = self._sequence
            self._sequence += 1
            enqueued_at = time.monotonic()
            try:
                self._queue.put_nowait((priority, sequence, operation, future, enqueued_at, started))
            except asyncio.QueueFull as err:
                raise OrderPreSubmissionError(
                    f"order scheduler queue full for key {self._key.symbol}:{self._key.position_side.value}"
                ) from err
        if not is_exit and self._max_queue_wait_seconds > 0:
            waiter = asyncio.create_task(started.wait())
            try:
                done, _ = await asyncio.wait(
                    [waiter, future],
                    timeout=self._max_queue_wait_seconds,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if not done:
                    future.cancel()
                    raise OrderPreSubmissionError(
                        f"order command waited {self._max_queue_wait_seconds:.2f}s in queue exceeding limit {self._max_queue_wait_seconds:.2f}s"
                    )
            finally:
                if not waiter.done():
                    waiter.cancel()
        return await future

    async def close(self) -> None:
        async with self._state_lock:
            if not self._closed:
                self._closed = True
                while not self._queue.empty():
                    try:
                        item = self._queue.get_nowait()
                        self._queue.task_done()
                    except asyncio.QueueEmpty:
                        break
                    _priority, _sequence, _operation, future, _enqueued_at, *rest = item
                    if future is not None and not future.done():
                        future.set_exception(
                            RuntimeError("order command scheduler is closed")
                        )
                try:
                    self._queue.put_nowait((2**31 - 1, self._sequence, None, None, 0.0, None))
                except asyncio.QueueFull:
                    pass
        if not self._worker.done():
            try:
                await self._worker
            except asyncio.CancelledError:
                pass

    async def _run(self) -> None:
        while True:
            try:
                if self._idle_timeout_seconds > 0:
                    try:
                        item = await asyncio.wait_for(
                            self._queue.get(),
                            timeout=self._idle_timeout_seconds,
                        )
                    except TimeoutError:
                        async with self._state_lock:
                            if self._queue.empty() and not self._closed:
                                self._closed = True
                                if self._on_idle is not None:
                                    self._on_idle(self._key)
                                return
                        continue
                else:
                    item = await self._queue.get()
            except asyncio.CancelledError:
                return

            _priority, _sequence, operation, future, enqueued_at, started = item
            try:
                if operation is None:
                    return
                if future is None or future.cancelled():
                    continue

                waited = time.monotonic() - enqueued_at
                is_exit = (_priority <= OrderExecutionCoordinator._EXIT_PRIORITY)
                if (
                    not is_exit
                    and self._max_queue_wait_seconds > 0
                    and waited > self._max_queue_wait_seconds
                ):
                    if not future.done():
                        future.set_exception(
                            OrderPreSubmissionError(
                                f"order command waited {waited:.2f}s in queue exceeding limit {self._max_queue_wait_seconds:.2f}s"
                            )
                        )
                    continue

                if started is not None:
                    started.set()
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
        max_queue_depth: int = 64,
        exit_headroom: int = 16,
        max_queue_wait_seconds: float = 30.0,
        idle_timeout_seconds: float = 120.0,
        domain_coordinator: ExecutionCoordinator | None = None,
        reservation_repository: Any | None = None,
    ) -> None:
        if not account_label.strip():
            raise ValueError("account_label must not be empty")
        if max_queue_depth <= 0:
            raise ValueError("max_queue_depth must be positive")
        if exit_headroom < 0 or exit_headroom >= max_queue_depth:
            raise ValueError("exit_headroom must be non-negative and less than max_queue_depth")
        self._backend = backend
        self._account_label = account_label.strip()
        self._max_queue_depth = max_queue_depth
        self._exit_headroom = exit_headroom
        self._max_queue_wait_seconds = max_queue_wait_seconds
        self._idle_timeout_seconds = idle_timeout_seconds
        self._domain_coordinator = domain_coordinator
        self._reservation_repository = reservation_repository
        self._schedulers: dict[OrderExecutionKey, _KeyCommandScheduler] = {}
        self._scheduler_lock = asyncio.Lock()
        self._closed = False
        self._entry_submissions_blocked = False
        self._active_entry_submissions = 0
        self._entry_submissions_idle = asyncio.Event()
        self._entry_submissions_idle.set()

    @property
    def domain_coordinator(self) -> ExecutionCoordinator | None:
        return self._domain_coordinator

    @property
    def reservation_repository(self) -> Any | None:
        return self._reservation_repository

    def block_entry_submissions(self) -> None:
        """Reject queued/future entries and drain the one already in flight."""
        self._entry_submissions_blocked = True
        if self._active_entry_submissions == 0:
            self._entry_submissions_idle.set()

    def unblock_entry_submissions(self) -> None:
        """Reopen entries after the caller has completed its safety gate."""
        if self._closed:
            return
        self._entry_submissions_blocked = False

    async def wait_for_entry_submissions_idle(self) -> None:
        """Wait until no entry operation can still reach the exchange."""
        await self._entry_submissions_idle.wait()

    async def _ensure_reservation(self, plan: OrderExecutionPlan) -> None:
        if not plan.reduce_only or self._reservation_repository is None:
            return
        try:
            key = PositionKey(
                environment="live",
                account_label=self._account_label,
                symbol=plan.symbol,
                position_side=plan.position_side,
            )
            active_res = await _maybe_await(
                self._reservation_repository.load_active_reservations(key)
            )
            existing = next(
                (r for r in active_res if r.command_id == plan.client_order_id),
                None,
            )
            if existing is None:
                reservation = PositionReservation(
                    reservation_id=f"res_{plan.client_order_id}",
                    command_id=plan.client_order_id,
                    position_key=key,
                    batch_id=f"batch_{plan.symbol}_{plan.position_side.value}",
                    reserved_quantity=Decimal(str(plan.quantity)),
                )
                await _maybe_await(
                    self._reservation_repository.save_reservation(reservation)
                )
        except Exception as res_err:
            log.error(
                "order_reservation_creation_failed_refusing_submission",
                client_order_id=plan.client_order_id,
                error=str(res_err),
            )
            raise OrderPreSubmissionError(
                f"Failed to create position reservation for "
                f"{plan.client_order_id}: {res_err}"
            ) from res_err

    async def _consume_reservation_if_filled(
        self,
        plan: OrderExecutionPlan,
        res: OrderExecutionResult,
    ) -> None:
        if (
            not plan.reduce_only
            or self._reservation_repository is None
            or res.executed_quantity <= 0
        ):
            return
        try:
            key = PositionKey(
                environment="live",
                account_label=self._account_label,
                symbol=plan.symbol,
                position_side=plan.position_side,
            )
            active_res = await _maybe_await(
                self._reservation_repository.load_active_reservations(key)
            )
            for r in active_res:
                if r.command_id == plan.client_order_id:
                    updated = r.consume(Decimal(str(res.executed_quantity)))
                    await _maybe_await(
                        self._reservation_repository.update_reservation(updated)
                    )
                    break
        except Exception as consume_err:
            log.warning(
                "order_reservation_consume_failed",
                client_order_id=plan.client_order_id,
                error=str(consume_err),
            )

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
            async def submit() -> OrderExecutionResult:
                await self._ensure_reservation(plan)
                res = (
                    await self._backend.execute_approved_intent(
                        plan,
                        prepared_submission=prepared_submission,
                    )
                    if prepared_submission is not None
                    else await self._backend.execute_approved_intent(plan)
                )
                await self._consume_reservation_if_filled(plan, res)
                return res

            return cast(
                OrderExecutionResult,
                await self._run_entry_submission(plan, submit),
            )

        return cast(
            OrderExecutionResult,
            await self._schedule(plan, priority=priority, operation=operation),
        )

    async def prepare_and_execute(
        self,
        plan: OrderExecutionPlan,
        *,
        prepare_submission: Callable[
            [], Awaitable[PreparedOrderSubmission | None]
        ],
    ) -> OrderExecutionResult | None:
        """Prepare and submit one plan inside the same per-key scheduler.

        A durable ``SUBMITTING`` row must not become visible to reconciliation
        while the corresponding exchange POST is still waiting to enter the
        coordinator.  The callback is deliberately executed by the scheduler
        worker, immediately followed by the backend submit, so reconcile and
        cancel operations for this key cannot interleave the two steps.
        """

        priority = (
            self._EXIT_PRIORITY if plan.reduce_only else self._ENTRY_PRIORITY
        )

        async def operation() -> OrderExecutionResult | None:
            async def prepare_and_submit() -> OrderExecutionResult | None:
                await self._ensure_reservation(plan)
                prepared = await prepare_submission()
                if prepared is None:
                    return None
                res = await self._backend.execute_approved_intent(
                    plan,
                    prepared_submission=prepared,
                )
                await self._consume_reservation_if_filled(plan, res)
                return res

            return cast(
                OrderExecutionResult | None,
                await self._run_entry_submission(plan, prepare_and_submit),
            )

        return cast(
            OrderExecutionResult | None,
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
        result = cast(
            OrderExecutionResult,
            await self._schedule(
                plan,
                priority=self._EXIT_PRIORITY,
                operation=lambda: self._backend.cancel_order(plan),
            ),
        )
        if (
            plan.reduce_only
            and self._reservation_repository is not None
            and result.state
            in (ExchangeOrderState.CANCELED, ExchangeOrderState.ABSENT_RECONCILED)
        ):
            try:
                key = PositionKey(
                    environment="live",
                    account_label=self._account_label,
                    symbol=plan.symbol,
                    position_side=plan.position_side,
                )
                active_res = await _maybe_await(
                    self._reservation_repository.load_active_reservations(key)
                )
                for r in active_res:
                    if r.command_id == plan.client_order_id:
                        updated = r.release(r.active_quantity)
                        await _maybe_await(
                            self._reservation_repository.update_reservation(
                                updated, release_reason="order_cancelled"
                            )
                        )
                        break
            except Exception as cancel_err:
                log.warning(
                    "order_reservation_release_on_cancel_failed",
                    client_order_id=plan.client_order_id,
                    error=str(cancel_err),
                )

        return result

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

    async def apply_observed_snapshot(
        self,
        plan: OrderExecutionPlan,
        snapshot: ExchangeOrderSnapshot,
    ) -> OrderExecutionResult:
        return cast(
            OrderExecutionResult,
            await self._schedule(
                plan,
                priority=(
                    self._EXIT_PRIORITY
                    if plan.reduce_only
                    else self._RECONCILE_PRIORITY
                ),
                operation=lambda: self._backend.apply_observed_snapshot(
                    plan,
                    snapshot,
                ),
            ),
        )

    async def mark_absent_reconciled(
        self,
        plan: OrderExecutionPlan,
        *,
        details: dict[str, JsonValue],
    ) -> OrderExecutionResult:
        return cast(
            OrderExecutionResult,
            await self._schedule(
                plan,
                priority=(
                    self._EXIT_PRIORITY
                    if plan.reduce_only
                    else self._RECONCILE_PRIORITY
                ),
                operation=lambda: self._backend.mark_absent_reconciled(
                    plan,
                    details=details,
                ),
            ),
        )

    async def aclose(self) -> None:
        self.block_entry_submissions()
        async with self._scheduler_lock:
            if self._closed:
                return
            self._closed = True
            schedulers = tuple(self._schedulers.values())
            self._schedulers.clear()
        if schedulers:
            await asyncio.gather(*(scheduler.close() for scheduler in schedulers))

    def _remove_idle_scheduler(self, key: OrderExecutionKey) -> None:
        self._schedulers.pop(key, None)

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
        async with self._scheduler_lock:
            if self._closed:
                raise RuntimeError("order execution coordinator is closed")
            scheduler = self._schedulers.get(key)
            if scheduler is None or scheduler.is_closed:
                scheduler = _KeyCommandScheduler(
                    key,
                    max_queue_depth=self._max_queue_depth,
                    exit_headroom=self._exit_headroom,
                    max_queue_wait_seconds=self._max_queue_wait_seconds,
                    idle_timeout_seconds=self._idle_timeout_seconds,
                    on_idle=self._remove_idle_scheduler,
                )
                self._schedulers[key] = scheduler
        return await scheduler.submit(priority=priority, operation=operation)

    async def _run_entry_submission(
        self,
        plan: OrderExecutionPlan,
        operation: Callable[[], Awaitable[Any]],
    ) -> Any:
        if plan.reduce_only:
            return await operation()
        if self._entry_submissions_blocked:
            raise OrderPreSubmissionError("entry submissions are blocked")
        self._active_entry_submissions += 1
        self._entry_submissions_idle.clear()
        try:
            return await operation()
        finally:
            self._active_entry_submissions -= 1
            if self._active_entry_submissions == 0:
                self._entry_submissions_idle.set()


__all__ = [
    "OrderExecutionCoordinator",
    "OrderExecutionKey",
    "OrderExecutionBackend",
    "OrderExecutionPort",
]
