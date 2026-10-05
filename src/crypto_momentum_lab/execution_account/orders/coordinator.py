"""Priority-aware coordination for live order commands.

The coordinator is the live execution seam.  It keeps commands for one
account/symbol/position side serial, while allowing unrelated symbols to make
progress independently.  Reconciliation is deliberately lowest priority so
an unknown REST read cannot hold an order command for another symbol hostage.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections import Counter
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from functools import partial
from typing import TYPE_CHECKING, Any, cast

import structlog

from crypto_momentum_lab.domain.account.models import (
    AccountConfigSnapshot,
    AccountFillEvent,
    AccountFillLoadScan,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.command_models import (
    DispatchState,
    ExecutionScope,
)
from crypto_momentum_lab.domain.execution.evidence_models import (
    ExecutionCumulativeOrderReport,
    ExecutionEvidence,
)
from crypto_momentum_lab.domain.execution.exchange_contract import (
    ExchangeOrderRejectedError,
    LiveSubmissionDisabledError,
)
from crypto_momentum_lab.domain.execution.execution_action_models import (
    Accepted,
    Blocked,
    CommandConflict,
    ExecutionActResult,
    ExecutionRecoveryPending,
    ExecutionRequest,
    PositionNotReady,
    StaleView,
)
from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.observation_models import (
    Applied,
    Duplicate,
    EvidenceConflict,
    EvidencePendingReason,
    WaitingForEvidence,
)
from crypto_momentum_lab.domain.execution.order_read_models import PersistedOrderReceipt
from crypto_momentum_lab.domain.execution.order_execution_port import (
    OrderExecutionPort as _OrderExecutionPort,
)
from crypto_momentum_lab.domain.execution.order_result import OrderExecutionResult
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderSnapshot,
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderAlreadyPreparedError,
    OrderPreSubmissionError,
    OrderProjectionConflictError,
    OrderRecoveryPendingError,
    OrderSubmissionPreparation,
    OrderSubmissionRepository,
    PreparedOrderSubmission,
)
from crypto_momentum_lab.domain.execution.ports import (
    ExecutionTransactionPort,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.recovery_models import (
    StreamCheckpointAdoption,
)
from crypto_momentum_lab.domain.execution.reservation_registry import (
    ExecutionReadinessError,
    ReservationRegistry,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    ExitPolicyMode,
    PositionReservation,
    TradeCommandType,
)
from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.domain.trading import TradeSide
from crypto_momentum_lab.execution_account.orders.fill_scan_evidence import (
    coverage_from_scan,
)

if TYPE_CHECKING:
    from crypto_momentum_lab.domain.account.snapshot_models import (
        AccountSnapshot,
    )

log = structlog.get_logger()


OrderExecutionBackend = _OrderExecutionPort


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
        self._queue: asyncio.PriorityQueue[
            tuple[int, int, Any, Any, float, Any, str | None]
        ] = asyncio.PriorityQueue(maxsize=max_queue_depth)
        self._queued_commands: dict[str, tuple[asyncio.Future[Any], asyncio.Event]] = {}
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

    def cancel_queued(self, client_order_id: str) -> bool:
        entry = self._queued_commands.get(client_order_id)
        if entry is None:
            return False
        future, started = entry
        if started.is_set():
            return False
        if not future.done():
            future.cancel()
        self._queued_commands.pop(client_order_id, None)
        return True

    async def wait_for_order(self, client_order_id: str) -> None:
        """Wait only for this order's queued/in-flight command, never other orders."""
        entry = self._queued_commands.get(client_order_id)
        if entry is None:
            return
        future, _ = entry
        try:
            await asyncio.shield(future)
        except asyncio.CancelledError:
            if not future.cancelled():
                raise
        except Exception:
            # A failed POST is precisely what the lookup must resolve.
            pass

    async def submit(
        self,
        *,
        priority: int,
        operation: Callable[[], Awaitable[Any]],
        client_order_id: str | None = None,
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
            if client_order_id is not None:
                self._queued_commands[client_order_id] = (future, started)
            try:
                self._queue.put_nowait(
                    (
                        priority,
                        sequence,
                        operation,
                        future,
                        enqueued_at,
                        started,
                        client_order_id,
                    )
                )
            except asyncio.QueueFull as err:
                if client_order_id is not None:
                    self._queued_commands.pop(client_order_id, None)
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
            except asyncio.CancelledError:
                if not started.is_set():
                    future.cancel()
                    if client_order_id is not None:
                        self._queued_commands.pop(client_order_id, None)
                raise
            finally:
                if not waiter.done():
                    waiter.cancel()
        if future.cancelled():
            return OrderExecutionResult(
                client_order_id=client_order_id or "",
                state=ExchangeOrderState.CANCELED,
                exchange_order_id=None,
            )
        try:
            return await asyncio.shield(future)
        except asyncio.CancelledError:
            if not started.is_set():
                future.cancel()
            else:
                # Cancellation of the waiter cannot erase an in-flight POST.
                future.add_done_callback(
                    lambda done: done.exception() if not done.cancelled() else None
                )
            raise

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
                        *rest,
                    ) = item
                    client_order_id = rest[0] if rest else None
                    if client_order_id is not None:
                        self._queued_commands.pop(client_order_id, None)
                    if future is not None and not future.done():
                        future.set_exception(
                            RuntimeError("order command scheduler is closed")
                        )
                try:
                    self._queue.put_nowait(
                        (2**31 - 1, self._sequence, None, None, 0.0, None, None)
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

            (
                _priority,
                _sequence,
                operation,
                future,
                enqueued_at,
                started,
                client_order_id,
            ) = item
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
                if client_order_id is not None:
                    entry = self._queued_commands.get(client_order_id)
                    if entry is not None and entry[0] is future:
                        self._queued_commands.pop(client_order_id, None)
                self._queue.task_done()


class OrderExecutionCoordinator:
    """Coordinate live submit, cancel, and reconcile commands.

    Interface invariants:

    * commands for the same account/symbol/position side execute one at a
      time;
    * reduce-only submit and cancel outrank entry submit; read-only
      reconciliation never owns the symbol command queue;
    * commands for different symbols do not wait on one another;
    * the wrapped state machine remains responsible for durable state
      transitions, idempotency, and exchange outcome recovery.
    """

    _EXIT_PRIORITY = 0
    _ENTRY_PRIORITY = 10

    def __init__(
        self,
        *,
        backend: _OrderExecutionPort,
        account_label: str,
        environment: str,
        max_queue_depth: int = 64,
        exit_headroom: int = 16,
        max_queue_wait_seconds: float = 30.0,
        idle_timeout_seconds: float = 120.0,
        execution_book: ExecutionBook,
        submission_repository: OrderSubmissionRepository | None = None,
        submission_clock: Callable[[], datetime] = lambda: datetime.now(UTC),
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
        self._submission_repository = submission_repository
        self._submission_configuration_locked = False
        self._submission_clock = submission_clock
        self._backend = backend
        self._environment = environment.strip()
        self._account_label = account_label.strip()
        self._max_queue_depth = max_queue_depth
        self._exit_headroom = exit_headroom
        self._max_queue_wait_seconds = max_queue_wait_seconds
        self._idle_timeout_seconds = idle_timeout_seconds
        self._execution_book = execution_book
        self._schedulers: dict[OrderExecutionKey, _KeyCommandScheduler] = {}
        self._scheduler_lock = asyncio.Lock()
        self._closed = False
        self._order_observations: asyncio.Queue[
            tuple[OrderExecutionPlan, OrderExecutionResult]
        ] = asyncio.Queue()
        self._order_observer_task: asyncio.Task[None] | None = None
        self._entry_submissions_blocked = False
        self._active_entry_submissions = 0
        self._entry_submissions_idle = asyncio.Event()
        self._entry_submissions_idle.set()
        self._confirmed_flat_streams: dict[PositionKey, tuple[str, str]] = {}
        self._waiting_for_evidence: dict[
            PositionKey, tuple[str | None, str | None, EvidencePendingReason]
        ] = {}
        self._active_stream: tuple[str, str] | None = None

    @property
    def reservation_registry(self) -> ReservationRegistry:
        return self._execution_book.coordinator

    @property
    def execution_book(self) -> ExecutionBook:
        return self._execution_book

    def get_active_reservations(
        self, key: PositionKey | None = None
    ) -> tuple[PositionReservation, ...]:
        """Returns currently tracked active reservations."""
        return self._execution_book.get_active_reservations(key)

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
        *,
        fills: tuple[AccountFillEvent, ...] = (),
        fill_load_scans: tuple[AccountFillLoadScan, ...] = (),
        stream_id: str | None = None,
        stream_epoch: str | None = None,
        sequence: int | None = None,
        evidence_id: str | None = None,
        hedge_mode: bool | None = None,
    ) -> None:
        """Feed authoritative exchange account snapshot into ExecutionBook."""
        if self._execution_book.has_execution_unit_of_work and (
            not stream_id or not stream_epoch or sequence is None or sequence <= 0
        ):
            raise ValueError(
                "durable account evidence requires source stream, epoch, and sequence"
            )
        if stream_id and stream_epoch:
            self._execution_book.register_active_stream(
                environment=self._environment,
                account_label=self._account_label,
                stream_id=stream_id,
                stream_epoch=stream_epoch,
            )
        if stream_id and stream_epoch:
            self._active_stream = (stream_id, stream_epoch)
        positions: tuple[AccountPositionSnapshot, ...]
        if snapshot is None:
            positions = ()
        elif isinstance(snapshot, AccountPositionSnapshot):
            positions = (snapshot,)
        else:
            from crypto_momentum_lab.domain.account.snapshot_models import (
                AccountSnapshot,
            )

            if isinstance(snapshot, AccountSnapshot):
                if not isinstance(snapshot.config, AccountConfigSnapshot):
                    raise TypeError(
                        "AccountSnapshot.config must be AccountConfigSnapshot"
                    )
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
        for position in positions:
            if not isinstance(position, AccountPositionSnapshot):
                raise TypeError(
                    "AccountSnapshot.positions must contain AccountPositionSnapshot"
                )
            if position.environment != self._environment:
                raise ValueError(
                    "AccountPositionSnapshot environment must match coordinator "
                    f"({self._environment})"
                )
            if position.account_label != self._account_label:
                raise ValueError(
                    "AccountPositionSnapshot account_label does not match coordinator"
                )
            side_str = position.position_side.upper()
            scope = ExecutionScope(
                environment=self._environment,
                account_label=self._account_label,
                symbol=position.symbol,
                position_side=FuturesPositionSide(side_str),
            )
            positions_by_key[scope.to_position_key()] = position

        fills_by_key: dict[PositionKey, list[AccountFillEvent]] = {}
        for fill in fills:
            if (
                fill.environment != self._environment
                or fill.account_label != self._account_label
            ):
                raise ValueError("account fill scope does not match coordinator")
            raw_position_side = fill.raw_position_side
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

        scans_by_key: dict[PositionKey, AccountFillLoadScan] = {}
        for source_scan in fill_load_scans:
            key = PositionKey(
                source_scan.environment,
                source_scan.account_label,
                source_scan.symbol,
                FuturesPositionSide(source_scan.position_side),
            )
            if key not in positions_by_key:
                raise ValueError(
                    "fill scan requires a matching explicit account snapshot"
                )
            if key in scans_by_key:
                raise ValueError("account event contains duplicate position scans")
            if not stream_id or not stream_epoch:
                raise ValueError("fill scan requires a source stream")
            scans_by_key[key] = source_scan
        conflict_reasons: Counter[str] = Counter()
        waiting_reasons: Counter[str] = Counter()
        waiting_positions: list[str] = []
        ready_positions: list[str] = []

        def note_waiting(key: PositionKey, reason: EvidencePendingReason) -> None:
            state = (stream_id, stream_epoch, reason)
            if self._waiting_for_evidence.get(key) != state:
                self._waiting_for_evidence[key] = state
                waiting_reasons[reason.value] += 1
                waiting_positions.append(key.canonical_id)

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
                and key not in scans_by_key
                and stream_id is not None
                and stream_epoch is not None
                and self._confirmed_flat_streams.get(key) == (stream_id, stream_epoch)
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
                "account_" + hashlib.sha256(identity.encode("utf-8")).hexdigest()
            )
            execution_scope = ExecutionScope(
                environment=key.environment,
                account_label=key.account_label,
                symbol=key.symbol,
                position_side=key.position_side,
            )
            proof = None
            adoption = None
            scan = scans_by_key.get(key)
            if scan is not None:
                assert (
                    pos is not None
                    and stream_id is not None
                    and stream_epoch is not None
                )
                target_scope = AccountFactStreamScope.for_position_key(
                    key, stream_id=stream_id, stream_epoch=stream_epoch
                )
                proof = coverage_from_scan(scan, snapshot=pos, scope=target_scope)
                assert proof.load_provenance is not None
                if (
                    scan.source_anchor_kind == "recovery_checkpoint"
                    and proof.load_provenance.is_complete
                ):
                    assert (
                        scan.source_stream_id is not None
                        and scan.source_stream_epoch is not None
                    )
                    parent_scope = AccountFactStreamScope.for_position_key(
                        key,
                        stream_id=scan.source_stream_id,
                        stream_epoch=scan.source_stream_epoch,
                    )
                    if parent_scope != target_scope:
                        parent = await self._execution_book.load_recovery_checkpoint(
                            parent_scope
                        )
                        if parent is None:
                            note_waiting(
                                key, EvidencePendingReason.PARENT_CHECKPOINT_UNAVAILABLE
                            )
                            continue
                        if parent.checkpoint_id != scan.source_anchor_id:
                            self._waiting_for_evidence.pop(key, None)
                            conflict_reasons[
                                "fill scan parent checkpoint identity mismatch"
                            ] += 1
                            continue
                        adoption = StreamCheckpointAdoption(
                            parent_checkpoint=parent,
                            target_scope=target_scope,
                            fill_load_provenance=proof.load_provenance,
                            target_event_cut=scan.observed_at,
                        )
            result = await self._execution_book.observe(
                ExecutionEvidence(
                    evidence_id=scoped_evidence_id,
                    scope=execution_scope,
                    observed_at=observed_at,
                    fills=scoped_fills,
                    snapshot=pos,
                    coverage_evidence=proof,
                    fill_load_provenance=None
                    if proof is None
                    else proof.load_provenance,
                    stream_checkpoint_adoption=adoption,
                    source_anchor_snapshot=None
                    if scan is None
                    else scan.source_anchor_snapshot,
                    stream_id=stream_id,
                    stream_epoch=stream_epoch,
                    sequence=sequence,
                )
            )
            if isinstance(result, WaitingForEvidence):
                note_waiting(key, result.reason)
            elif isinstance(result, EvidenceConflict):
                self._waiting_for_evidence.pop(key, None)
                conflict_reasons[result.reason] += 1
            elif isinstance(result, (Applied, Duplicate)):
                if self._waiting_for_evidence.pop(key, None) is not None:
                    ready_positions.append(key.canonical_id)
            if (
                isinstance(result, (Applied, Duplicate))
                and pos is not None
                and pos.position_amt == 0
                and not scoped_fills
                and stream_id is not None
                and stream_epoch is not None
            ):
                self._confirmed_flat_streams[key] = (stream_id, stream_epoch)
        if waiting_positions:
            log.info(
                "account_snapshot_execution_book_waiting_for_evidence",
                account_label=self._account_label,
                stream_id=stream_id,
                stream_epoch=stream_epoch,
                sequence=sequence,
                positions=tuple(waiting_positions),
                reasons=dict(waiting_reasons),
            )
        if ready_positions:
            log.info(
                "account_snapshot_execution_book_evidence_ready",
                account_label=self._account_label,
                stream_id=stream_id,
                stream_epoch=stream_epoch,
                sequence=sequence,
                positions=tuple(ready_positions),
            )
        if conflict_reasons:
            log.error(
                "account_snapshot_execution_book_conflicts",
                account_label=self._account_label,
                stream_id=stream_id,
                stream_epoch=stream_epoch,
                sequence=sequence,
                position_count=len(positions_by_key),
                fill_count=len(fills),
                reasons=dict(conflict_reasons),
            )

    def _execution_request(
        self, plan: OrderExecutionPlan, *, strategy_name: str
    ) -> ExecutionRequest:
        if (
            plan.reduce_only
            and plan.batch_id
            and (
                str(plan.batch_id).startswith(f"batch_{plan.symbol}_")
                or str(plan.batch_id) in ("batch_default", "batch_synthetic")
            )
        ):
            raise OrderPreSubmissionError(
                f"Account {self._account_label}: synthetic batch "
                f"{plan.batch_id} is prohibited"
            )
        projection_version = plan.projection_version
        if not projection_version or not projection_version.strip():
            kind = "exit" if plan.reduce_only else "entry"
            raise OrderPreSubmissionError(
                f"{kind} {plan.client_order_id} has no Book projection token"
            )
        scope = ExecutionScope(
            environment=self._environment,
            account_label=self._account_label,
            symbol=plan.symbol,
            position_side=plan.position_side,
        )
        opening_buy = (plan.side == "BUY") != plan.reduce_only
        side = TradeSide.LONG if opening_buy else TradeSide.SHORT
        if not plan.reduce_only:
            return ExecutionRequest(
                request_id=plan.client_order_id,
                scope=scope,
                strategy_name=strategy_name,
                run_id=plan.run_id,
                decision_ref=plan.client_order_id,
                expected_view_token=projection_version,
                action=TradeCommandType.ENTRY,
                requested_quantity=plan.quantity,
                side=side,
                order_type=plan.order_type,
                limit_price=plan.price,
                created_at=plan.created_at,
            )
        allocations = plan.allocations
        if allocations:
            target_batch_ids = tuple(item.batch_id for item in allocations)
            batch_quantities = {
                item.batch_id: item.allocated_quantity for item in allocations
            }
        elif plan.batch_id:
            target_batch_ids = (str(plan.batch_id),)
            batch_quantities = {str(plan.batch_id): plan.quantity}
        else:
            raise OrderPreSubmissionError(
                f"Exit order {plan.client_order_id} has no allocated batches or batch_id"
            )
        return ExecutionRequest(
            request_id=plan.client_order_id,
            scope=scope,
            strategy_name=strategy_name,
            run_id=plan.run_id,
            decision_ref=plan.client_order_id,
            expected_view_token=projection_version,
            action=TradeCommandType.EXIT,
            requested_quantity=plan.quantity,
            side=side,
            order_type=plan.order_type,
            limit_price=plan.price,
            reduce_only=True,
            target_batch_ids=target_batch_ids,
            batch_quantities=batch_quantities,
            exit_policy_mode=ExitPolicyMode.TARGET_BATCHES_ONLY,
            created_at=plan.created_at,
        )

    @staticmethod
    def _require_accepted_execution_result(
        plan: OrderExecutionPlan, result: ExecutionActResult
    ) -> None:
        if isinstance(result, Blocked):
            cause = (
                ExecutionReadinessError(result.reason)
                if isinstance(result, (PositionNotReady, ExecutionRecoveryPending))
                else None
            )
            error_type = (
                OrderRecoveryPendingError
                if isinstance(result, ExecutionRecoveryPending) and not plan.reduce_only
                else (
                    OrderProjectionConflictError
                    if "ReservationConflictError" in result.diagnostics
                    else OrderPreSubmissionError
                )
            )
            raise error_type(
                "Failed to create position reservation for "
                f"{plan.client_order_id}: {result.reason}"
            ) from cause
        if isinstance(result, StaleView):
            error_type = (
                OrderProjectionConflictError
                if plan.reduce_only
                else OrderPreSubmissionError
            )
            raise error_type(
                "Failed to create position reservation (stale view) for "
                f"{plan.client_order_id}: {result.reason}"
            )
        if isinstance(result, CommandConflict):
            raise OrderPreSubmissionError(
                "Failed to create position reservation (command conflict) for "
                f"{plan.client_order_id}: {result.reason}"
            )

    async def _ensure_reservation(self, plan: OrderExecutionPlan) -> None:
        if not self._execution_book.has_reservation_repository:
            return
        try:
            if plan.strategy_name is None:
                raise OrderPreSubmissionError("new order plan requires strategy_name")
            req = self._execution_request(plan, strategy_name=plan.strategy_name)
            act_res = await self._execution_book.act(req)
        except OrderProjectionConflictError:
            raise
        except Exception as err:
            log.error(
                "order_reservation_creation_failed_refusing_submission",
                client_order_id=plan.client_order_id,
                error=str(err),
            )
            prefix = (
                "Failed to create position reservation for "
                if plan.reduce_only
                else "Failed to create position entry for "
            )
            raise OrderPreSubmissionError(
                f"{prefix}{plan.client_order_id}: {err}"
            ) from err
        self._require_accepted_execution_result(plan, act_res)

    async def _atomic_prepare_submission(
        self,
        plan: OrderExecutionPlan,
        preparation: OrderSubmissionPreparation,
    ) -> PreparedOrderSubmission | None:
        if self._submission_repository is None:
            raise OrderPreSubmissionError("submission repository is not configured")
        if not self._execution_book.has_execution_unit_of_work:
            raise OrderPreSubmissionError(
                "order submission requires an execution transaction"
            )
        repository = self._submission_repository
        assert repository is not None

        async def in_tx(tx: ExecutionTransactionPort | None) -> PreparedOrderSubmission:
            if tx is None or tx.session is None:
                raise OrderPreSubmissionError(
                    "order submission requires an execution transaction"
                )
            prepared = await repository.prepare_submission_in_session(
                tx.session,
                plan=plan,
                intent=preparation.intent,
                evaluation=preparation.evaluation,
                prepared_at=self._submission_clock(),
                environment=preparation.environment,
                account_label=preparation.account_label,
                strategy_name=preparation.strategy_name,
                max_open_positions=preparation.max_open_positions,
                max_daily_loss=preparation.max_daily_loss,
                max_gross_exposure=preparation.max_gross_exposure,
                current_daily_pnl=preparation.current_daily_pnl,
                current_gross_exposure=preparation.current_gross_exposure,
                open_position_symbols=preparation.open_position_symbols,
                exposure_notional=preparation.exposure_notional,
                baseline_observed_at=preparation.baseline_observed_at,
            )
            if prepared is None:
                raise OrderAlreadyPreparedError("submission already prepared")
            return prepared

        try:
            req = self._execution_request(
                plan, strategy_name=preparation.intent.strategy_name
            )
            act_res = await self._execution_book.act(req, prepare_submission=in_tx)
            self._require_accepted_execution_result(plan, act_res)
        except OrderProjectionConflictError:
            raise
        except OrderRecoveryPendingError as recovery_rejection:
            log.info(
                "order_submission_recovery_admission_rejected",
                client_order_id=plan.client_order_id,
                reason=str(recovery_rejection),
            )
            return None
        except OrderAlreadyPreparedError:
            return None
        except OrderPreSubmissionError as err:
            if plan.reduce_only and "Failed to create position reservation" not in str(
                err
            ):
                raise OrderPreSubmissionError(
                    f"Failed to create position reservation for {plan.client_order_id}: {err}"
                ) from err
            raise
        except Exception as err:
            log.error(
                "order_reservation_creation_failed_refusing_submission",
                client_order_id=plan.client_order_id,
                error=str(err),
            )
            prefix = (
                "Failed to create position reservation for "
                if plan.reduce_only
                else "Failed to create position entry for "
            )
            raise OrderPreSubmissionError(
                f"{prefix}{plan.client_order_id}: {err}"
            ) from err

        if isinstance(act_res, Accepted):
            return act_res.prepared_submission
        return None

    async def _observe_order_result_in_execution_book(
        self,
        plan: OrderExecutionPlan,
        res: OrderExecutionResult | None,
        *,
        settlement_fills: tuple[AccountFillEvent, ...] = (),
    ) -> None:
        if res is None:
            return
        scope = ExecutionScope(
            environment=self._environment,
            account_label=self._account_label,
            symbol=plan.symbol,
            position_side=plan.position_side,
        )
        now_dt = datetime.now(UTC)
        cumulative_quantity = res.executed_quantity
        if cumulative_quantity < Decimal("0"):
            raise ValueError("exchange cumulative executed quantity cannot be negative")
        average_price = res.average_price
        if cumulative_quantity > Decimal("0") and average_price <= Decimal("0"):
            if res.state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION:
                if self._execution_book.get_outbox(res.client_order_id) is not None:
                    await self._execution_book.mark_unknown(
                        res.client_order_id,
                        reason="cumulative_fill_price_pending",
                    )
                return
            raise RuntimeError(
                "positive cumulative fill has no positive cumulative average price; "
                "execution facts require recovery"
            )
        cumulative_quote = cumulative_quantity * average_price
        position_side = plan.position_side.value
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
        if settlement_fills:
            from crypto_momentum_lab.domain.execution.evidence_digest import (
                trade_payload_digest,
            )

            identity += "\x1f" + "\x1f".join(
                trade_payload_digest(fill)
                for fill in sorted(settlement_fills, key=lambda item: item.trade_id)
            )
        identity_hash = hashlib.sha256(identity.encode("utf-8")).hexdigest()
        order_event = ExchangeOrderEvent(
            event_id=f"order_{identity_hash}",
            client_order_id=res.client_order_id,
            state=res.state,
            occurred_at=now_dt,
            exchange_order_id=res.exchange_order_id,
            details={
                "account_label": self._account_label,
                "symbol": plan.symbol,
                **({"side": plan.side} if settlement_fills else {}),
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
                if stream_scope is not None:
                    stream_id = stream_scope.stream_id
                    stream_epoch = stream_scope.stream_epoch
                else:
                    active = self._execution_book.get_active_stream(
                        self._environment, self._account_label
                    )
                    if active is not None:
                        stream_id, stream_epoch = active
                    elif self._active_stream is not None:
                        stream_id, stream_epoch = self._active_stream
                    else:
                        raise RuntimeError(
                            "cumulative order report has no restored Book stream "
                            "identity and no active stream scope is registered"
                        )
            result = await self._execution_book.observe(
                ExecutionEvidence(
                    evidence_id=order_event.event_id,
                    scope=scope,
                    observed_at=now_dt,
                    order_event=order_event,
                    stream_id=stream_id,
                    stream_epoch=stream_epoch,
                    cumulative_order=ExecutionCumulativeOrderReport(
                        order_id=res.client_order_id,
                        cumulative_quantity=cumulative_quantity,
                        cumulative_quote=cumulative_quote,
                        observed_at=now_dt,
                    ),
                    settlement_fills=settlement_fills,
                )
            )
            if isinstance(result, WaitingForEvidence):
                if res.state.terminal:
                    self._execution_book.require_command_recovery(res.client_order_id)
                    log.warning(
                        "order_terminal_evidence_waiting_for_recovery",
                        client_order_id=res.client_order_id,
                        state=res.state.value,
                        reason=result.reason.value,
                    )
                    return
                raise ExecutionReadinessError(
                    "cumulative order evidence is waiting for recovery: "
                    + result.reason.value
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
        if isinstance(result, Applied) and result.recovery_required:
            self._execution_book.require_command_recovery(res.client_order_id)
            log.warning(
                "order_facts_applied_reservation_recovery_required",
                client_order_id=res.client_order_id,
                diagnostics=result.diagnostics,
            )

    async def _record_submission_failure(
        self,
        plan: OrderExecutionPlan,
        error: BaseException,
        *,
        before_exchange_post: bool,
    ) -> None:
        if self._execution_book.get_outbox(plan.client_order_id) is None:
            return
        if before_exchange_post or isinstance(
            error,
            (
                OrderAlreadyPreparedError,
                OrderPreSubmissionError,
                ExchangeOrderRejectedError,
                LiveSubmissionDisabledError,
            ),
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
        entry = self._execution_book.get_outbox(plan.client_order_id)
        if entry is None:
            if (
                self._execution_book.has_command_repository
                or self._execution_book.has_reservation_repository
            ):
                raise OrderPreSubmissionError(
                    f"execution command {plan.client_order_id} has no accepted outbox"
                )
            # In-memory coordinators are used by isolated scheduler tests and
            # adapters. Durable live wiring must supply both repositories.
            return
        if entry.state == DispatchState.PREPARED:
            await self._execution_book.mark_dispatching(plan.client_order_id)
        elif entry.state in (DispatchState.REJECTED, DispatchState.TERMINAL):
            raise OrderPreSubmissionError(
                f"execution command {plan.client_order_id} is in non-dispatchable state {entry.state.value}"
            )

    async def _observe_returned_order_result(
        self,
        plan: OrderExecutionPlan,
        result: OrderExecutionResult,
    ) -> None:
        if (
            not plan.reduce_only
            and result.state is ExchangeOrderState.ACKNOWLEDGED
            and result.executed_quantity == Decimal("0")
        ):
            self._defer_order_projection(plan, result)
            return
        await self._apply_returned_order_result(plan, result)

    def _defer_order_projection(
        self, plan: OrderExecutionPlan, result: OrderExecutionResult
    ) -> None:
        self._order_observations.put_nowait((plan, result))
        if self._order_observer_task is None:
            self._order_observer_task = asyncio.create_task(
                self._consume_order_observations()
            )

    async def _consume_order_observations(self) -> None:
        try:
            while not self._order_observations.empty():
                plan, result = self._order_observations.get_nowait()
                try:
                    await self._apply_returned_order_result(plan, result)
                finally:
                    self._order_observations.task_done()
        finally:
            self._order_observer_task = None

    async def _apply_returned_order_result(
        self,
        plan: OrderExecutionPlan,
        result: OrderExecutionResult,
    ) -> None:
        try:
            await self._observe_order_result_in_execution_book(plan, result)
        except Exception as error:
            # The exchange result is authoritative. A secondary projection
            # failure must not turn a successful POST into a failed order.
            self._execution_book.require_command_recovery(result.client_order_id)
            log.error(
                "order_projection_recovery_required",
                account_label=self._account_label,
                client_order_id=result.client_order_id,
                exchange_state=result.state.value,
                error=str(error),
            )

    async def observe_recovered_receipt(
        self,
        plan: OrderExecutionPlan,
        receipt: PersistedOrderReceipt,
    ) -> None:
        """Replay a durable exchange terminal fact through normal Book settlement."""
        if self._closed:
            raise RuntimeError("Order execution coordinator is closed")
        if receipt.client_order_id != plan.client_order_id:
            raise ValueError("recovered receipt client order id mismatch")
        await self._observe_order_result_in_execution_book(
            plan,
            OrderExecutionResult(
                receipt.client_order_id,
                receipt.state,
                receipt.exchange_order_id,
                executed_quantity=receipt.executed_quantity,
                average_price=receipt.average_price,
                plan=plan,
            ),
            settlement_fills=receipt.account_fills,
        )

    async def submit(
        self,
        plan: OrderExecutionPlan,
        *,
        prepared_submission: PreparedOrderSubmission,
    ) -> OrderExecutionResult:
        self._submission_configuration_locked = True
        priority = self._EXIT_PRIORITY if plan.reduce_only else self._ENTRY_PRIORITY

        async def operation() -> OrderExecutionResult:
            async with self._entry_submission(plan):
                await self._ensure_reservation(plan)
                await self._mark_dispatching_if_accepted(plan)
                try:
                    res = await self._backend.submit(
                        plan,
                        prepared_submission=prepared_submission,
                    )
                except BaseException as sub_err:
                    await self._record_submission_failure(
                        plan,
                        sub_err,
                        before_exchange_post=isinstance(
                            sub_err,
                            (
                                OrderAlreadyPreparedError,
                                OrderPreSubmissionError,
                                ExchangeOrderRejectedError,
                                LiveSubmissionDisabledError,
                                ValueError,
                            ),
                        ),
                    )
                    raise
                await self._observe_returned_order_result(plan, res)
                return res

        return cast(
            OrderExecutionResult,
            await self._schedule(plan, priority=priority, operation=operation),
        )

    def configure_submission(
        self,
        repository: OrderSubmissionRepository,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if self._submission_configuration_locked:
            raise RuntimeError("cannot configure submission after execution starts")
        self._submission_repository = repository
        self._submission_clock = clock

    async def prepare_and_execute(
        self,
        plan: OrderExecutionPlan,
        *,
        preparation: OrderSubmissionPreparation,
    ) -> OrderExecutionResult | None:
        """Prepare and submit one plan inside the same per-key scheduler.

        Repository preparation runs after dequeue.
        A durable ``SUBMITTING`` row must not become visible to reconciliation
        while the corresponding exchange POST is still waiting to enter the
        coordinator. Preparation is immediately followed by backend submit, so reconcile and
        cancel operations for this key cannot interleave the two steps.
        """

        self._submission_configuration_locked = True
        priority = self._EXIT_PRIORITY if plan.reduce_only else self._ENTRY_PRIORITY

        return cast(
            OrderExecutionResult | None,
            await self._schedule(
                plan,
                priority=priority,
                operation=partial(self._execute_prepared_submission, plan, preparation),
            ),
        )

    async def _execute_prepared_submission(
        self,
        plan: OrderExecutionPlan,
        preparation: OrderSubmissionPreparation,
    ) -> OrderExecutionResult | None:
        async with self._entry_submission(plan):
            try:
                prepared = await self._atomic_prepare_submission(plan, preparation)
            except Exception as prepare_err:
                await self._record_submission_failure(
                    plan,
                    prepare_err,
                    before_exchange_post=True,
                )
                raise
            if prepared is None:
                return None
            try:
                res = await self._backend.submit(
                    plan,
                    prepared_submission=prepared,
                )
            except BaseException as sub_err:
                await self._record_submission_failure(
                    plan,
                    sub_err,
                    before_exchange_post=isinstance(
                        sub_err,
                        (
                            OrderAlreadyPreparedError,
                            OrderPreSubmissionError,
                            ExchangeOrderRejectedError,
                            LiveSubmissionDisabledError,
                            ValueError,
                        ),
                    ),
                )
                raise
            if (
                plan.reduce_only
                and self._execution_book.has_execution_unit_of_work
                and res.state is ExchangeOrderState.ACKNOWLEDGED
                and res.executed_quantity == Decimal("0")
            ):
                self._defer_order_projection(plan, res)
            else:
                await self._observe_returned_order_result(plan, res)
            return replace(res, prepared_at=prepared.submitting_event.occurred_at)

    async def cancel_order(self, plan: OrderExecutionPlan) -> OrderExecutionResult:
        key = OrderExecutionKey(
            account_label=self._account_label,
            symbol=plan.symbol.strip().upper(),
            position_side=plan.position_side,
        )
        async with self._scheduler_lock:
            scheduler = self._schedulers.get(key)
        if scheduler is not None and scheduler.cancel_queued(plan.client_order_id):
            if self._execution_book.get_outbox(plan.client_order_id) is not None:
                await self._execution_book.mark_rejected(
                    plan.client_order_id,
                    reason="cancelled_before_submission",
                )
            return OrderExecutionResult(
                client_order_id=plan.client_order_id,
                state=ExchangeOrderState.CANCELED,
                exchange_order_id=None,
                plan=plan,
            )
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
        if self._closed:
            raise RuntimeError("Order execution coordinator is closed")
        key = OrderExecutionKey(
            account_label=self._account_label,
            symbol=plan.symbol.strip().upper(),
            position_side=plan.position_side,
        )
        async with self._scheduler_lock:
            scheduler = self._schedulers.get(key)
        if scheduler is not None:
            await scheduler.wait_for_order(plan.client_order_id)
        # Read-only remote inspection does not occupy the symbol command queue.
        # Order observations still serialize their short durable commits.
        result = await self._backend.reconcile_order(plan)
        await self._observe_returned_order_result(plan, result)
        return result

    async def apply_observed_snapshot(
        self,
        plan: OrderExecutionPlan,
        snapshot: ExchangeOrderSnapshot,
    ) -> OrderExecutionResult:
        # An observation is an exchange fact, not a command. Do not put it
        # behind a REST command that is waiting on network I/O or backoff.
        if self._closed:
            raise RuntimeError("Order execution coordinator is closed")
        result = await self._backend.apply_observed_snapshot(plan, snapshot)
        await self._observe_returned_order_result(plan, result)
        return result

    async def mark_reconciliation_pending(
        self,
        plan: OrderExecutionPlan,
    ) -> OrderExecutionResult:
        if self._closed:
            raise RuntimeError("Order execution coordinator is closed")
        result = await self._backend.mark_reconciliation_pending(
            plan,
        )
        await self._observe_returned_order_result(plan, result)
        return result

    async def mark_absent_reconciled(
        self,
        plan: OrderExecutionPlan,
        *,
        details: dict[str, JsonValue],
    ) -> OrderExecutionResult:
        if self._closed:
            raise RuntimeError("Order execution coordinator is closed")
        # Proven absence is an observation, not a queued exchange command.
        result = await self._backend.mark_absent_reconciled(plan, details=details)
        await self._observe_returned_order_result(plan, result)
        return result

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
        await self._order_observations.join()
        if self._order_observer_task is not None:
            await self._order_observer_task

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
        return await scheduler.submit(
            priority=priority,
            operation=operation,
            client_order_id=plan.client_order_id,
        )

    @asynccontextmanager
    async def _entry_submission(self, plan: OrderExecutionPlan) -> AsyncIterator[None]:
        if plan.reduce_only:
            yield
            return
        if self._entry_submissions_blocked:
            raise OrderPreSubmissionError("entry submissions are blocked")
        self._active_entry_submissions += 1
        self._entry_submissions_idle.clear()
        try:
            yield
        finally:
            self._active_entry_submissions -= 1
            if self._active_entry_submissions == 0:
                self._entry_submissions_idle.set()


__all__ = [
    "OrderExecutionCoordinator",
    "OrderExecutionKey",
    "OrderExecutionBackend",
]
