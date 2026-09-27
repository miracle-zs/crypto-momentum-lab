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
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Protocol, cast

import structlog

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.domain.execution import (
    ExchangeOrderEvent,
    ExchangeOrderSnapshot,
    ExchangeOrderState,
    ExecutionEvidence,
    ExecutionScope,
    FuturesPositionSide,
    OrderExecutionPlan,
    TradeCommandType,
)
from crypto_momentum_lab.domain.execution.execution_book import (
    Blocked,
    CommandConflict,
    ExecutionBook,
    ExecutionRequest,
    StaleView,
)
from crypto_momentum_lab.domain.execution.execution_coordinator import (
    ExecutionCoordinator,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
from crypto_momentum_lab.domain.execution.trade_command import (
    ExitPolicyMode,
    PositionReservation,
)
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
        self._queue: asyncio.PriorityQueue[tuple[int, int, Any, Any, float, Any]] = (
            asyncio.PriorityQueue(maxsize=max_queue_depth)
        )
        self._state_lock = asyncio.Lock()
        self._sequence = 0
        self._closed = False
        self._worker = asyncio.create_task(
            self._run(),
            name=(
                f"live-order-key-{key.symbol.lower()}-{key.position_side.value.lower()}"
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

            is_exit = priority <= OrderExecutionCoordinator._EXIT_PRIORITY
            current_depth = self._queue.qsize()

            if is_exit:
                if current_depth >= self._max_queue_depth:
                    raise OrderPreSubmissionError(
                        f"order scheduler queue full "
                        f"({current_depth}/{self._max_queue_depth}) "
                        f"for key {self._key.symbol}:"
                        f"{self._key.position_side.value}"
                    )
            else:
                entry_limit = max(1, self._max_queue_depth - self._exit_headroom)
                if current_depth >= entry_limit:
                    raise OrderPreSubmissionError(
                        f"order scheduler entry capacity exceeded "
                        f"({current_depth}/{entry_limit}, "
                        f"headroom={self._exit_headroom}) for key "
                        f"{self._key.symbol}:"
                        f"{self._key.position_side.value}"
                    )

            sequence = self._sequence
            self._sequence += 1
            enqueued_at = time.monotonic()
            try:
                self._queue.put_nowait(
                    (priority, sequence, operation, future, enqueued_at, started)
                )
            except asyncio.QueueFull as err:
                raise OrderPreSubmissionError(
                    f"order scheduler queue full for key "
                    f"{self._key.symbol}:{self._key.position_side.value}"
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
                        f"order command waited "
                        f"{self._max_queue_wait_seconds:.2f}s in queue "
                        f"exceeding limit "
                        f"{self._max_queue_wait_seconds:.2f}s"
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
                    (
                        _priority,
                        _sequence,
                        _operation,
                        future,
                        _enqueued_at,
                        _started,
                    ) = item
                    if future is not None and not future.done():
                        future.set_exception(
                            RuntimeError("order command scheduler is closed")
                        )
                try:
                    self._queue.put_nowait(
                        (2**31 - 1, self._sequence, None, None, 0.0, None)
                    )
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
                is_exit = _priority <= OrderExecutionCoordinator._EXIT_PRIORITY
                if (
                    not is_exit
                    and self._max_queue_wait_seconds > 0
                    and waited > self._max_queue_wait_seconds
                ):
                    if not future.done():
                        future.set_exception(
                            OrderPreSubmissionError(
                                f"order command waited {waited:.2f}s in "
                                f"queue exceeding limit "
                                f"{self._max_queue_wait_seconds:.2f}s"
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
        execution_book: ExecutionBook | None = None,
        initial_reservations: Iterable[PositionReservation] | None = None,
    ) -> None:
        if not account_label.strip():
            raise ValueError("account_label must not be empty")
        if max_queue_depth <= 0:
            raise ValueError("max_queue_depth must be positive")
        if exit_headroom < 0 or exit_headroom >= max_queue_depth:
            raise ValueError(
                "exit_headroom must be non-negative and less than max_queue_depth"
            )
        self._backend = backend
        self._account_label = account_label.strip()
        self._max_queue_depth = max_queue_depth
        self._exit_headroom = exit_headroom
        self._max_queue_wait_seconds = max_queue_wait_seconds
        self._idle_timeout_seconds = idle_timeout_seconds
        self._domain_coordinator = domain_coordinator
        self._reservation_repository = reservation_repository
        self._execution_book = execution_book or ExecutionBook(
            coordinator=self._domain_coordinator,
            reservation_repository=self._reservation_repository,
        )
        if self._domain_coordinator is None and self._execution_book is not None:
            self._domain_coordinator = self._execution_book.coordinator
        self._settled_cumulative_quantities: dict[str, Decimal] = {}
        if initial_reservations:
            for r in initial_reservations:
                if self._domain_coordinator is not None:
                    self._domain_coordinator.register_reservation(r)
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
    def execution_book(self) -> ExecutionBook:
        return self._execution_book

    @property
    def reservation_repository(self) -> Any | None:
        return self._reservation_repository

    @property
    def is_execution_book_enabled(self) -> bool:
        return True

    def get_active_reservations(
        self, key: PositionKey | None = None
    ) -> tuple[PositionReservation, ...]:
        """Returns currently tracked active reservations."""
        if self._execution_book is not None:
            return self._execution_book.get_active_reservations(key)
        return ()

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
        if self._reservation_repository is None or self._execution_book is None:
            return

        if not plan.reduce_only:
            try:
                scope = ExecutionScope(
                    environment="live",
                    account_label=self._account_label,
                    symbol=plan.symbol,
                    position_side=plan.position_side,
                )
                current_view = await self._execution_book.read(scope)
                proj_ver = getattr(plan, "projection_version", None)
                token = (
                    proj_ver
                    if (proj_ver and proj_ver == current_view.projection_version)
                    else "*"
                )
                req = ExecutionRequest(
                    request_id=plan.client_order_id,
                    scope=scope,
                    strategy_name=getattr(plan, "strategy_name", "live_strategy"),
                    strategy_version=getattr(plan, "strategy_version", "v1"),
                    run_id=getattr(plan, "run_id", self._account_label),
                    decision_ref=getattr(plan, "decision_ref", plan.client_order_id),
                    expected_view_token=token,
                    expected_projection_version=proj_ver,
                    action=TradeCommandType.ENTRY,
                    requested_quantity=Decimal(str(plan.quantity)),
                    order_type=(
                        str(plan.order_type.value)
                        if hasattr(plan.order_type, "value")
                        else str(plan.order_type)
                    ),
                    limit_price=(
                        Decimal(str(plan.price)) if plan.price is not None else None
                    ),
                    reduce_only=False,
                    target_batch_ids=(),
                    batch_quantities=None,
                    created_at=plan.created_at,
                )
                act_res = await self._execution_book.act(req)
            except Exception as err:
                log.error(
                    "order_entry_acceptance_failed_refusing_submission",
                    client_order_id=plan.client_order_id,
                    error=str(err),
                )
                raise OrderPreSubmissionError(
                    f"Failed to create position entry for "
                    f"{plan.client_order_id}: {err}"
                ) from err

            if isinstance(act_res, Blocked):
                raise OrderPreSubmissionError(
                    f"Failed to create position entry for "
                    f"{plan.client_order_id}: {act_res.reason}"
                )
            if isinstance(act_res, StaleView):
                raise OrderPreSubmissionError(
                    f"Failed to create position entry (stale view) for "
                    f"{plan.client_order_id}: {act_res.reason}"
                )
            if isinstance(act_res, CommandConflict):
                raise OrderPreSubmissionError(
                    f"Failed to create position entry (command conflict) for "
                    f"{plan.client_order_id}: {act_res.reason}"
                )
            return

        batch_str = getattr(plan, "batch_id", None)
        if batch_str and (
            str(batch_str).startswith(f"batch_{plan.symbol}_")
            or str(batch_str) in ("batch_default", "batch_synthetic")
        ):
            raise OrderPreSubmissionError(
                f"Account {self._account_label}: "
                f"synthetic batch {batch_str} is prohibited"
            )

        try:
            scope = ExecutionScope(
                environment="live",
                account_label=self._account_label,
                symbol=plan.symbol,
                position_side=plan.position_side,
            )
            current_view = await self._execution_book.read(scope)
            proj_ver = getattr(plan, "projection_version", None)
            token = (
                proj_ver
                if (proj_ver and proj_ver == current_view.projection_version)
                else "*"
            )

            allocations = getattr(plan, "allocations", ())
            batch_quantities = getattr(plan, "batch_quantities", None)
            if allocations:
                target_batch_ids = tuple(a.batch_id for a in allocations)
                if batch_quantities is None:
                    batch_quantities = {
                        a.batch_id: a.allocated_quantity for a in allocations
                    }
            elif getattr(plan, "batch_id", None):
                target_batch_ids = (str(plan.batch_id),)
                if batch_quantities is None:
                    batch_quantities = {str(plan.batch_id): Decimal(str(plan.quantity))}
            else:
                raise OrderPreSubmissionError(
                    f"Exit order {plan.client_order_id} has no allocated batches "
                    f"or batch_id; cannot invent synthetic batch"
                )

            req = ExecutionRequest(
                request_id=plan.client_order_id,
                scope=scope,
                strategy_name=getattr(plan, "strategy_name", "live_strategy"),
                strategy_version=getattr(plan, "strategy_version", "v1"),
                run_id=getattr(plan, "run_id", self._account_label),
                decision_ref=getattr(plan, "decision_ref", plan.client_order_id),
                expected_view_token=token,
                expected_projection_version=proj_ver,
                action=TradeCommandType.EXIT,
                requested_quantity=Decimal(str(plan.quantity)),
                order_type=(
                    str(plan.order_type.value)
                    if hasattr(plan.order_type, "value")
                    else str(plan.order_type)
                ),
                limit_price=(
                    Decimal(str(plan.price)) if plan.price is not None else None
                ),
                reduce_only=True,
                target_batch_ids=target_batch_ids,
                batch_quantities=batch_quantities,
                exit_policy_mode=getattr(
                    plan,
                    "exit_policy_mode",
                    ExitPolicyMode.TARGET_BATCHES_ONLY,
                ),
                created_at=plan.created_at,
            )
            act_res = await self._execution_book.act(req)
        except Exception as err:
            log.error(
                "order_reservation_creation_failed_refusing_submission",
                client_order_id=plan.client_order_id,
                error=str(err),
            )
            raise OrderPreSubmissionError(
                f"Failed to create position reservation for "
                f"{plan.client_order_id}: {err}"
            ) from err

        if isinstance(act_res, Blocked):
            raise OrderPreSubmissionError(
                f"Failed to create position reservation for "
                f"{plan.client_order_id}: {act_res.reason}"
            )
        if isinstance(act_res, StaleView):
            raise OrderPreSubmissionError(
                f"Failed to create position reservation (stale view) for "
                f"{plan.client_order_id}: {act_res.reason}"
            )
        if isinstance(act_res, CommandConflict):
            raise OrderPreSubmissionError(
                f"Failed to create position reservation (command conflict) for "
                f"{plan.client_order_id}: {act_res.reason}"
            )

    async def _release_reservation_if_present(
        self,
        plan: OrderExecutionPlan,
        reason: str = "preparation_or_execution_failed",
    ) -> None:
        if not plan.reduce_only:
            return
        try:
            key = PositionKey(
                environment="live",
                account_label=self._account_label,
                symbol=plan.symbol,
                position_side=plan.position_side,
            )
            active_res = list(self.get_active_reservations(key))
            if self._reservation_repository is not None:
                try:
                    repo_res = await _maybe_await(
                        self._reservation_repository.load_active_reservations(key)
                    )
                    existing_ids = {r.reservation_id for r in active_res}
                    for r in repo_res:
                        if r.reservation_id not in existing_ids:
                            active_res.append(r)
                            if self._domain_coordinator is not None:
                                try:
                                    self._domain_coordinator.register_reservation(r)
                                except Exception:
                                    pass
                except Exception:
                    pass
            for r in active_res:
                if r.command_id == plan.client_order_id and r.active_quantity > Decimal(
                    "0"
                ):
                    if self._domain_coordinator is not None:
                        released = self._domain_coordinator.release_reservation(
                            r.reservation_id, r.active_quantity
                        )
                    else:
                        released = r.release(r.active_quantity)
                    if self._reservation_repository is not None:
                        await _maybe_await(
                            self._reservation_repository.update_reservation(
                                released, release_reason=reason
                            )
                        )
        except Exception as rel_err:
            log.warning(
                "order_reservation_rollback_failed",
                client_order_id=plan.client_order_id,
                error=str(rel_err),
            )

    async def _consume_reservation_if_filled(
        self,
        plan: OrderExecutionPlan,
        res: OrderExecutionResult,
    ) -> None:
        if not plan.reduce_only or res is None:
            return
        is_terminal = res.state in {
            ExchangeOrderState.FILLED,
            ExchangeOrderState.CANCELED,
            ExchangeOrderState.EXPIRED,
            ExchangeOrderState.REJECTED,
            ExchangeOrderState.ABSENT_RECONCILED,
        }
        if res.executed_quantity <= 0 and not is_terminal:
            return
        try:
            key = PositionKey(
                environment="live",
                account_label=self._account_label,
                symbol=plan.symbol,
                position_side=plan.position_side,
            )
            active_res = list(self.get_active_reservations(key))
            if self._reservation_repository is not None:
                try:
                    repo_res = await _maybe_await(
                        self._reservation_repository.load_active_reservations(key)
                    )
                    existing_ids = {r.reservation_id for r in active_res}
                    for r in repo_res:
                        if r.reservation_id not in existing_ids:
                            active_res.append(r)
                            if self._domain_coordinator is not None:
                                try:
                                    self._domain_coordinator.register_reservation(r)
                                except Exception:
                                    pass
                except Exception:
                    pass
            order_key = plan.client_order_id or str(res.exchange_order_id or "")
            cum_executed = Decimal(str(res.executed_quantity))
            prev_settled = self._settled_cumulative_quantities.get(
                order_key, Decimal("0")
            )

            delta_to_consume = max(Decimal("0"), cum_executed - prev_settled)
            if delta_to_consume > Decimal("0"):
                remaining = delta_to_consume
                for r in active_res:
                    if (
                        r.command_id == plan.client_order_id
                        and r.active_quantity > Decimal("0")
                        and remaining > Decimal("0")
                    ):
                        qty = min(remaining, r.active_quantity)
                        if self._domain_coordinator is not None:
                            updated = self._domain_coordinator.reconcile_fill(
                                r.reservation_id, qty
                            )
                        else:
                            updated = r.consume(qty)
                        if self._reservation_repository is not None:
                            await _maybe_await(
                                self._reservation_repository.update_reservation(updated)
                            )
                        remaining -= qty
                settled_now = delta_to_consume - remaining
                self._settled_cumulative_quantities[order_key] = (
                    prev_settled + settled_now
                )

            if is_terminal:
                for r in self.get_active_reservations(key):
                    if (
                        r.command_id == plan.client_order_id
                        and r.active_quantity > Decimal("0")
                    ):
                        if self._domain_coordinator is not None:
                            released = self._domain_coordinator.release_reservation(
                                r.reservation_id, r.active_quantity
                            )
                        else:
                            released = r.release(r.active_quantity)
                        if self._reservation_repository is not None:
                            await _maybe_await(
                                self._reservation_repository.update_reservation(
                                    released,
                                    release_reason=f"order_finished_residual_release_{res.state.value}",
                                )
                            )
                self._settled_cumulative_quantities.pop(order_key, None)
        except Exception as consume_err:
            log.warning(
                "order_reservation_consume_failed",
                client_order_id=plan.client_order_id,
                error=str(consume_err),
            )

    async def _observe_order_result_in_execution_book(
        self,
        plan: OrderExecutionPlan,
        res: OrderExecutionResult | None,
    ) -> None:
        if not self.is_execution_book_enabled or res is None:
            return
        try:
            scope = ExecutionScope(
                environment="live",
                account_label=self._account_label,
                symbol=plan.symbol,
                position_side=plan.position_side,
            )
            now_dt = datetime.now(UTC)
            order_ev = ExchangeOrderEvent(
                event_id=f"ev_{res.client_order_id}_{res.state.value}",
                client_order_id=res.client_order_id,
                state=res.state,
                occurred_at=now_dt,
                exchange_order_id=res.exchange_order_id,
                details={
                    "account_label": self._account_label,
                    "symbol": plan.symbol,
                    "executed_quantity": str(res.executed_quantity),
                    "cumulative_quote_quantity": "0",
                    "average_price": (
                        str(res.average_price)
                        if res.average_price is not None
                        else None
                    ),
                    "limit_price": str(plan.price) if plan.price is not None else None,
                    "is_reduce_only": plan.reduce_only,
                    "position_side": (
                        plan.position_side.value
                        if hasattr(plan.position_side, "value")
                        else str(plan.position_side)
                    ),
                },
            )
            order_key = plan.client_order_id or str(res.exchange_order_id or "")
            cum_executed = Decimal(str(res.executed_quantity))
            prev_settled = self._settled_cumulative_quantities.get(
                order_key, Decimal("0")
            )
            delta_qty = max(Decimal("0"), cum_executed - prev_settled)
            fill_ev = None
            if delta_qty > Decimal("0"):
                fill_ev = AccountFillEvent(
                    environment="live",
                    account_label=self._account_label,
                    symbol=plan.symbol,
                    trade_id=f"fill_{res.client_order_id}_{cum_executed}",
                    order_id=res.client_order_id,
                    side=plan.side,
                    price=Decimal(str(res.average_price or (plan.price or "0"))),
                    quantity=delta_qty,
                    realized_pnl=Decimal("0"),
                    fee=Decimal("0"),
                    fee_asset="USDT",
                    trade_at=now_dt,
                    raw_payload={"is_cumulative": True, "cum_qty": str(cum_executed)},
                )
            await self._execution_book.observe(
                ExecutionEvidence(
                    evidence_id=order_ev.event_id,
                    scope=scope,
                    observed_at=now_dt,
                    order_event=order_ev,
                    fill=fill_ev,
                )
            )
        except Exception as obs_err:
            log.warning("execution_book_observe_order_event_failed", error=str(obs_err))

    async def submit(
        self,
        plan: OrderExecutionPlan,
        *,
        prepared_submission: PreparedOrderSubmission | None = None,
    ) -> OrderExecutionResult:
        priority = self._EXIT_PRIORITY if plan.reduce_only else self._ENTRY_PRIORITY

        async def operation() -> OrderExecutionResult:
            async def submit() -> OrderExecutionResult:
                await self._ensure_reservation(plan)
                if self.is_execution_book_enabled:
                    try:
                        self._execution_book.mark_dispatching(plan.client_order_id)
                    except Exception:
                        pass
                try:
                    res = (
                        await self._backend.execute_approved_intent(
                            plan,
                            prepared_submission=prepared_submission,
                        )
                        if prepared_submission is not None
                        else await self._backend.execute_approved_intent(plan)
                    )
                    await self._consume_reservation_if_filled(plan, res)
                    await self._observe_order_result_in_execution_book(plan, res)
                    return res
                except Exception as sub_err:
                    if self.is_execution_book_enabled:
                        try:
                            self._execution_book.mark_rejected(
                                plan.client_order_id, reason=str(sub_err)
                            )
                        except Exception:
                            pass
                    await self._release_reservation_if_present(plan)
                    raise

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
        prepare_submission: Callable[[], Awaitable[PreparedOrderSubmission | None]],
    ) -> OrderExecutionResult | None:
        """Prepare and submit one plan inside the same per-key scheduler.

        A durable ``SUBMITTING`` row must not become visible to reconciliation
        while the corresponding exchange POST is still waiting to enter the
        coordinator.  The callback is deliberately executed by the scheduler
        worker, immediately followed by the backend submit, so reconcile and
        cancel operations for this key cannot interleave the two steps.
        """

        priority = self._EXIT_PRIORITY if plan.reduce_only else self._ENTRY_PRIORITY

        async def operation() -> OrderExecutionResult | None:
            async def prepare_and_submit() -> OrderExecutionResult | None:
                await self._ensure_reservation(plan)
                if self.is_execution_book_enabled:
                    try:
                        self._execution_book.mark_dispatching(plan.client_order_id)
                    except Exception:
                        pass
                try:
                    prepared = await prepare_submission()
                    if prepared is None:
                        if self.is_execution_book_enabled:
                            try:
                                self._execution_book.mark_rejected(
                                    plan.client_order_id,
                                    reason="prepare_submission_returned_none",
                                )
                            except Exception:
                                pass
                        await self._release_reservation_if_present(
                            plan, reason="prepare_submission_returned_none"
                        )
                        return None
                    res = await self._backend.execute_approved_intent(
                        plan,
                        prepared_submission=prepared,
                    )
                    await self._consume_reservation_if_filled(plan, res)
                    await self._observe_order_result_in_execution_book(plan, res)
                    return res
                except Exception as sub_err:
                    if self.is_execution_book_enabled:
                        try:
                            self._execution_book.mark_rejected(
                                plan.client_order_id, reason=str(sub_err)
                            )
                        except Exception:
                            pass
                    await self._release_reservation_if_present(plan)
                    raise

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
        if plan.reduce_only and result.state in (
            ExchangeOrderState.CANCELED,
            ExchangeOrderState.ABSENT_RECONCILED,
        ):
            await self._release_reservation_if_present(plan, reason="order_cancelled")

        await self._observe_order_result_in_execution_book(plan, result)
        return result

    async def reconcile_order(
        self,
        plan: OrderExecutionPlan,
    ) -> OrderExecutionResult:
        async def operation() -> OrderExecutionResult:
            res = await self._backend.reconcile_order(plan)
            await self._consume_reservation_if_filled(plan, res)
            await self._observe_order_result_in_execution_book(plan, res)
            return res

        return cast(
            OrderExecutionResult,
            await self._schedule(
                plan,
                priority=self._RECONCILE_PRIORITY,
                operation=operation,
            ),
        )

    async def apply_observed_snapshot(
        self,
        plan: OrderExecutionPlan,
        snapshot: ExchangeOrderSnapshot,
    ) -> OrderExecutionResult:
        async def operation() -> OrderExecutionResult:
            res = await self._backend.apply_observed_snapshot(plan, snapshot)
            await self._consume_reservation_if_filled(plan, res)
            await self._observe_order_result_in_execution_book(plan, res)
            return res

        return cast(
            OrderExecutionResult,
            await self._schedule(
                plan,
                priority=(
                    self._EXIT_PRIORITY
                    if plan.reduce_only
                    else self._RECONCILE_PRIORITY
                ),
                operation=operation,
            ),
        )

    async def mark_absent_reconciled(
        self,
        plan: OrderExecutionPlan,
        *,
        details: dict[str, JsonValue],
    ) -> OrderExecutionResult:
        async def operation() -> OrderExecutionResult:
            res = await self._backend.mark_absent_reconciled(plan, details=details)
            await self._consume_reservation_if_filled(plan, res)
            await self._observe_order_result_in_execution_book(plan, res)
            return res

        return cast(
            OrderExecutionResult,
            await self._schedule(
                plan,
                priority=self._RECONCILE_PRIORITY,
                operation=operation,
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
