"""Priority-aware coordination for live order commands.

The coordinator is the live execution seam.  It keeps commands for one
account/symbol/position side serial, while allowing unrelated symbols to make
progress independently.  Reconciliation is deliberately lowest priority so
an unknown REST read cannot hold an order command for another symbol hostage.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import time
from collections import Counter
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Protocol, cast

import structlog

from crypto_momentum_lab.domain.account import (
    AccountConfigSnapshot,
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution import (
    ExchangeOrderEvent,
    ExchangeOrderSnapshot,
    ExecutionEvidence,
    ExecutionScope,
    FuturesPositionSide,
    OrderExecutionPlan,
    TradeCommandType,
)
from crypto_momentum_lab.domain.execution.execution_book import (
    Blocked,
    CommandConflict,
    EvidenceConflict,
    ExecutionBook,
    ExecutionCumulativeOrderReport,
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
    ExchangeOrderRejectedError,
    OrderExecutionResult,
    OrderPreSubmissionError,
    PreparedOrderSubmission,
)
from crypto_momentum_lab.execution_account.sync import AccountSnapshot

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
        environment: str,
        max_queue_depth: int = 64,
        exit_headroom: int = 16,
        max_queue_wait_seconds: float = 30.0,
        idle_timeout_seconds: float = 120.0,
        domain_coordinator: ExecutionCoordinator | None = None,
        reservation_repository: Any | None = None,
        execution_book: ExecutionBook | None = None,
        initial_reservations: Iterable[PositionReservation] | None = None,
    ) -> None:
        if not environment.strip():
            raise ValueError("environment must not be empty")
        if not account_label.strip():
            raise ValueError("account_label must not be empty")
        if max_queue_depth <= 0:
            raise ValueError("max_queue_depth must be positive")
        if exit_headroom < 0 or exit_headroom >= max_queue_depth:
            raise ValueError(
                "exit_headroom must be non-negative and less than max_queue_depth"
            )
        self._backend = backend
        self._environment = environment.strip()
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
        self._confirmed_flat_streams: dict[PositionKey, tuple[str, str]] = {}

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

    async def observe_account_snapshot(
        self,
        snapshot: AccountPositionSnapshot | AccountSnapshot | None,
        symbols: tuple[str, ...] | frozenset[str] = (),
        *,
        fills: tuple[AccountFillEvent, ...] = (),
        stream_id: str | None = None,
        stream_epoch: str | None = None,
        sequence: int | None = None,
        evidence_id: str | None = None,
        hedge_mode: bool | None = None,
    ) -> None:
        """Feed authoritative exchange account snapshot into ExecutionBook."""
        if self._execution_book is None:
            return
        if self._execution_book.has_execution_unit_of_work and (
            not stream_id or not stream_epoch or sequence is None or sequence <= 0
        ):
            raise ValueError(
                "durable account evidence requires source stream, epoch, and sequence"
            )
        if (
            stream_id
            and stream_epoch
            and self._execution_book is not None
            and hasattr(self._execution_book, "register_active_stream")
        ):
            self._execution_book.register_active_stream(
                environment=self._environment,
                account_label=self._account_label,
                stream_id=stream_id,
                stream_epoch=stream_epoch,
            )
        if snapshot is None:
            positions = ()
        elif isinstance(snapshot, AccountPositionSnapshot):
            positions = (snapshot,)
        elif isinstance(snapshot, AccountSnapshot):
            if not isinstance(snapshot.config, AccountConfigSnapshot):
                raise TypeError("AccountSnapshot.config must be AccountConfigSnapshot")
            if (
                snapshot.config.environment != self._environment
                or snapshot.config.account_label != self._account_label
            ):
                raise ValueError(
                    "AccountSnapshot config scope does not match coordinator"
                )
            positions = snapshot.positions
            if hedge_mode is not None and hedge_mode != snapshot.config.hedge_mode:
                raise ValueError("hedge_mode does not match AccountSnapshot config")
            hedge_mode = snapshot.config.hedge_mode
        else:
            raise TypeError(
                "snapshot must be AccountPositionSnapshot or AccountSnapshot"
            )

        # `symbols` describes event context, not proof that an omitted position
        # is flat. Only explicit exchange position rows are ingested here.
        positions_by_key: dict[PositionKey, AccountPositionSnapshot] = {}
        for pos in positions:
            if not isinstance(pos, AccountPositionSnapshot):
                raise TypeError(
                    "AccountSnapshot.positions must contain AccountPositionSnapshot"
                )
            if pos.environment != self._environment:
                raise ValueError(
                    "AccountPositionSnapshot environment must match coordinator "
                    f"({self._environment})"
                )
            if pos.account_label != self._account_label:
                raise ValueError(
                    "AccountPositionSnapshot account_label does not match coordinator"
                )
            side_str = pos.position_side.upper()
            scope = ExecutionScope(
                environment=self._environment,
                account_label=self._account_label,
                symbol=pos.symbol,
                position_side=FuturesPositionSide(side_str),
            )
            positions_by_key[scope.to_position_key()] = pos

        fills_by_key: dict[PositionKey, list[AccountFillEvent]] = {}
        for fill in fills:
            if (
                fill.environment != self._environment
                or fill.account_label != self._account_label
            ):
                raise ValueError("account fill scope does not match coordinator")
            raw_position_side = (
                fill.raw_payload.get("positionSide", fill.raw_payload.get("position_side"))
                if isinstance(fill.raw_payload, dict)
                else None
            )
            if raw_position_side is None and isinstance(fill.raw_payload, dict):
                row = fill.raw_payload.get("row")
                if isinstance(row, dict):
                    raw_position_side = row.get("ps", row.get("positionSide", row.get("position_side")))
            if raw_position_side is None:
                if hedge_mode is not False:
                    raise ValueError(
                        "account fill is missing positionSide and one-way mode "
                        "was not proven"
                    )
                position_side = FuturesPositionSide.BOTH
            else:
                try:
                    position_side = FuturesPositionSide(
                        str(raw_position_side).strip().upper()
                    )
                except ValueError as error:
                    raise ValueError(
                        f"account fill has invalid positionSide {raw_position_side!r}"
                    ) from error
            key = PositionKey(
                environment=fill.environment,
                account_label=fill.account_label,
                symbol=fill.symbol,
                position_side=position_side,
            )
            fills_by_key.setdefault(key, []).append(fill)

        conflict_reasons: Counter[str] = Counter()
        for key in sorted(
            positions_by_key.keys() | fills_by_key.keys(),
            key=lambda item: item.canonical_id,
        ):
            pos = positions_by_key.get(key)
            scoped_fills = tuple(fills_by_key.get(key, ()))
            repeated_flat = bool(
                pos is not None
                and pos.position_amt == 0
                and not scoped_fills
                and stream_id is not None
                and stream_epoch is not None
                and self._confirmed_flat_streams.get(key)
                == (stream_id, stream_epoch)
            )
            if repeated_flat:
                continue
            self._confirmed_flat_streams.pop(key, None)
            observed_at = (
                pos.observed_at
                if pos is not None
                else max(fill.trade_at for fill in scoped_fills)
                if scoped_fills
                else datetime.now(UTC)
            )
            identity = "\x1f".join(
                (
                    evidence_id or "snapshot",
                    key.canonical_id,
                    ",".join(fill.trade_id for fill in scoped_fills),
                    "" if pos is None else pos.observed_at.isoformat(),
                    "" if pos is None else str(pos.position_amt),
                )
            )
            scoped_evidence_id = (
                "account_"
                + hashlib.sha256(identity.encode("utf-8")).hexdigest()
            )
            execution_scope = ExecutionScope(
                environment=key.environment,
                account_label=key.account_label,
                symbol=key.symbol,
                position_side=key.position_side,
            )
            result = await self._execution_book.observe(
                ExecutionEvidence(
                    evidence_id=scoped_evidence_id,
                    scope=execution_scope,
                    observed_at=observed_at,
                    fills=scoped_fills,
                    snapshot=pos,
                    stream_id=stream_id,
                    stream_epoch=stream_epoch,
                    sequence=sequence,
                )
            )
            if isinstance(result, EvidenceConflict):
                conflict_reasons[result.reason] += 1
            if (
                not isinstance(result, EvidenceConflict)
                and pos is not None
                and pos.position_amt == 0
                and not scoped_fills
                and stream_id is not None
                and stream_epoch is not None
            ):
                self._confirmed_flat_streams[key] = (stream_id, stream_epoch)
        if conflict_reasons:
            log.warning(
                "account_snapshot_execution_book_conflicts",
                account_label=self._account_label,
                stream_id=stream_id,
                stream_epoch=stream_epoch,
                sequence=sequence,
                position_count=len(positions_by_key),
                fill_count=len(fills),
                reasons=dict(conflict_reasons),
            )

    async def _ensure_reservation(self, plan: OrderExecutionPlan) -> None:
        if self._reservation_repository is None or self._execution_book is None:
            return

        if not plan.reduce_only:
            try:
                scope = ExecutionScope(
                    environment=self._environment,
                    account_label=self._account_label,
                    symbol=plan.symbol,
                    position_side=plan.position_side,
                )
                current_view = await self._execution_book.read(scope)
                proj_ver = getattr(plan, "projection_version", None)
                if not isinstance(proj_ver, str) or not proj_ver.strip():
                    raise OrderPreSubmissionError(
                        f"entry {plan.client_order_id} has no Book projection token"
                    )
                if proj_ver != current_view.projection_version:
                    raise OrderPreSubmissionError(
                        f"entry {plan.client_order_id} was built from stale position "
                        f"projection {proj_ver}; current projection is "
                        f"{current_view.projection_version}"
                    )
                strategy_name = getattr(plan, "strategy_name", None)
                if not strategy_name or not str(strategy_name).strip():
                    strategy_name = (
                        getattr(self, "_strategy_name", None) or "orderflow_impulse"
                    )
                strategy_version = getattr(plan, "strategy_version", None)
                if not strategy_version or not str(strategy_version).strip():
                    strategy_version = getattr(self, "_strategy_version", None) or "v0"
                req = ExecutionRequest(
                    request_id=plan.client_order_id,
                    scope=scope,
                    strategy_name=str(strategy_name).strip(),
                    strategy_version=str(strategy_version).strip(),
                    run_id=getattr(plan, "run_id", self._account_label),
                    decision_ref=getattr(plan, "decision_ref", plan.client_order_id),
                    expected_view_token=proj_ver,
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
                    f"Failed to create position entry for {plan.client_order_id}: {err}"
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
                environment=self._environment,
                account_label=self._account_label,
                symbol=plan.symbol,
                position_side=plan.position_side,
            )
            current_view = await self._execution_book.read(scope)
            proj_ver = getattr(plan, "projection_version", None)
            if not isinstance(proj_ver, str) or not proj_ver.strip():
                raise OrderPreSubmissionError(
                    f"exit {plan.client_order_id} has no Book projection token"
                )
            if proj_ver != current_view.projection_version:
                raise OrderPreSubmissionError(
                    f"exit {plan.client_order_id} was allocated from stale position "
                    f"projection {proj_ver}; current projection is "
                    f"{current_view.projection_version}"
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

            strategy_name = getattr(plan, "strategy_name", None)
            if not strategy_name or not str(strategy_name).strip():
                strategy_name = (
                    getattr(self, "_strategy_name", None) or "orderflow_impulse"
                )
            strategy_version = getattr(plan, "strategy_version", None)
            if not strategy_version or not str(strategy_version).strip():
                strategy_version = getattr(self, "_strategy_version", None) or "v0"

            req = ExecutionRequest(
                request_id=plan.client_order_id,
                scope=scope,
                strategy_name=str(strategy_name).strip(),
                strategy_version=str(strategy_version).strip(),
                run_id=getattr(plan, "run_id", self._account_label),
                decision_ref=getattr(plan, "decision_ref", plan.client_order_id),
                expected_view_token=proj_ver,
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

    async def _consume_reservation_if_filled(
        self,
        plan: OrderExecutionPlan,
        res: OrderExecutionResult,
    ) -> None:
        """Compatibility wrapper; ExecutionBook.observe owns fill settlement."""
        await self._observe_order_result_in_execution_book(plan, res)

    async def _observe_order_result_in_execution_book(
        self,
        plan: OrderExecutionPlan,
        res: OrderExecutionResult | None,
    ) -> None:
        if not self.is_execution_book_enabled or res is None:
            return
        scope = ExecutionScope(
            environment=self._environment,
            account_label=self._account_label,
            symbol=plan.symbol,
            position_side=plan.position_side,
        )
        now_dt = datetime.now(UTC)
        cumulative_quantity = Decimal(str(res.executed_quantity))
        if cumulative_quantity < Decimal("0"):
            raise ValueError("exchange cumulative executed quantity cannot be negative")
        average_price = (
            Decimal(str(res.average_price)) if res.average_price is not None else None
        )
        if cumulative_quantity > Decimal("0") and (
            average_price is None or average_price <= Decimal("0")
        ):
            raise RuntimeError(
                "positive cumulative fill has no positive cumulative average price; "
                "execution facts require recovery"
            )
        cumulative_quote = (
            cumulative_quantity * average_price
            if average_price is not None
            else Decimal("0")
        )
        position_side = (
            plan.position_side.value
            if hasattr(plan.position_side, "value")
            else str(plan.position_side)
        )
        identity = "\x1f".join(
            (
                self._account_label,
                plan.symbol,
                position_side,
                res.client_order_id,
                str(res.exchange_order_id or ""),
                res.state.value,
                str(cumulative_quantity),
                str(cumulative_quote),
            )
        )
        identity_hash = hashlib.sha256(identity.encode("utf-8")).hexdigest()
        order_ev = ExchangeOrderEvent(
            event_id=f"order_{identity_hash}",
            client_order_id=res.client_order_id,
            state=res.state,
            occurred_at=now_dt,
            exchange_order_id=res.exchange_order_id,
            details={
                "account_label": self._account_label,
                "symbol": plan.symbol,
                "executed_quantity": str(cumulative_quantity),
                "cumulative_quote_quantity": str(cumulative_quote),
                "average_price": str(average_price)
                if average_price is not None
                else None,
                "limit_price": str(plan.price) if plan.price is not None else None,
                "is_reduce_only": plan.reduce_only,
                "position_side": position_side,
            },
        )
        try:
            stream_id: str | None = None
            stream_epoch: str | None = None
            if self._execution_book.has_execution_unit_of_work:
                current_view = await self._execution_book.read(scope)
                stream_scope = current_view.stream_scope
                if stream_scope is None:
                    raise RuntimeError(
                        "cumulative order report has no restored Book stream identity"
                    )
                stream_id = stream_scope.stream_id
                stream_epoch = stream_scope.stream_epoch
            result = await self._execution_book.observe(
                ExecutionEvidence(
                    evidence_id=order_ev.event_id,
                    scope=scope,
                    observed_at=now_dt,
                    order_event=order_ev,
                    stream_id=stream_id,
                    stream_epoch=stream_epoch,
                    cumulative_order=ExecutionCumulativeOrderReport(
                        order_id=res.client_order_id,
                        cumulative_quantity=cumulative_quantity,
                        cumulative_quote=cumulative_quote,
                        observed_at=now_dt,
                    ),
                )
            )
            if isinstance(result, EvidenceConflict):
                raise RuntimeError(
                    "cumulative order evidence was rejected: " + result.reason
                )
        except Exception as observe_err:
            if self._execution_book.get_outbox(res.client_order_id) is not None:
                try:
                    await self._execution_book.mark_unknown(
                        res.client_order_id,
                        reason=(
                            "exchange result could not be persisted: "
                            f"{observe_err}"
                        ),
                    )
                except Exception as transition_err:
                    raise RuntimeError(
                        "exchange returned a result, fact persistence failed, and "
                        "the UNKNOWN outbox transition also failed: "
                        f"{transition_err}"
                    ) from transition_err
            raise
        if getattr(result, "recovery_required", False):
            raise RuntimeError(
                "ExecutionBook applied order facts but reservation settlement "
                f"requires recovery: {result.diagnostics}"
            )

    async def _record_submission_failure(
        self,
        plan: OrderExecutionPlan,
        error: Exception,
        *,
        before_exchange_post: bool,
    ) -> None:
        if not self.is_execution_book_enabled:
            return
        if before_exchange_post or isinstance(
            error, (OrderPreSubmissionError, ExchangeOrderRejectedError)
        ):
            await self._execution_book.mark_rejected(
                plan.client_order_id,
                reason=str(error),
            )
            return
        await self._execution_book.mark_unknown(
            plan.client_order_id,
            reason=str(error) or "submission outcome unknown",
        )

    async def _mark_dispatching_if_accepted(self, plan: OrderExecutionPlan) -> None:
        if not self.is_execution_book_enabled:
            return
        entry = self._execution_book.get_outbox(plan.client_order_id)
        if entry is None:
            if (
                self._execution_book.has_command_repository
                or self._reservation_repository
            ):
                raise OrderPreSubmissionError(
                    f"execution command {plan.client_order_id} has no accepted outbox"
                )
            # In-memory coordinators are used by isolated scheduler tests and
            # shadow adapters. Durable live wiring must supply both repositories.
            return
        await self._execution_book.mark_dispatching(plan.client_order_id)

    async def _observe_returned_order_result(
        self,
        plan: OrderExecutionPlan,
        result: OrderExecutionResult,
    ) -> None:
        await self._observe_order_result_in_execution_book(plan, result)

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
                await self._mark_dispatching_if_accepted(plan)
                try:
                    res = (
                        await self._backend.execute_approved_intent(
                            plan,
                            prepared_submission=prepared_submission,
                        )
                        if prepared_submission is not None
                        else await self._backend.execute_approved_intent(plan)
                    )
                except Exception as sub_err:
                    await self._record_submission_failure(
                        plan,
                        sub_err,
                        before_exchange_post=isinstance(
                            sub_err,
                            (OrderPreSubmissionError, ExchangeOrderRejectedError),
                        ),
                    )
                    raise
                await self._observe_returned_order_result(plan, res)
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
                await self._mark_dispatching_if_accepted(plan)
                try:
                    prepared = await prepare_submission()
                except Exception as prepare_err:
                    await self._record_submission_failure(
                        plan,
                        prepare_err,
                        before_exchange_post=True,
                    )
                    raise
                if prepared is None:
                    await self._execution_book.mark_rejected(
                        plan.client_order_id,
                        reason="prepare_submission_returned_none",
                    )
                    return None
                try:
                    res = await self._backend.execute_approved_intent(
                        plan,
                        prepared_submission=prepared,
                    )
                except Exception as sub_err:
                    await self._record_submission_failure(
                        plan,
                        sub_err,
                        before_exchange_post=isinstance(
                            sub_err,
                            (OrderPreSubmissionError, ExchangeOrderRejectedError),
                        ),
                    )
                    raise
                await self._observe_returned_order_result(plan, res)
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
        await self._observe_returned_order_result(plan, result)
        return result

    async def reconcile_order(
        self,
        plan: OrderExecutionPlan,
    ) -> OrderExecutionResult:
        async def operation() -> OrderExecutionResult:
            res = await self._backend.reconcile_order(plan)
            await self._observe_returned_order_result(plan, res)
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
            await self._observe_returned_order_result(plan, res)
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
            await self._observe_returned_order_result(plan, res)
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
        if self._execution_book is not None and hasattr(self._execution_book, "drain"):
            await self._execution_book.drain()

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
