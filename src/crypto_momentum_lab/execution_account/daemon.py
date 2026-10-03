import asyncio
import inspect
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from time import perf_counter
from typing import Protocol, TypeVar, cast
from uuid import uuid4

import structlog

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    ExecutionAccountStatus,
)
from crypto_momentum_lab.domain.account.snapshot_models import (
    AccountSnapshot,
)
from crypto_momentum_lab.execution_account.binance.user_data import (
    UserDataEventSink,
)
from crypto_momentum_lab.execution_account.binance.user_data_models import (
    BinanceUserDataEvent,
)
from crypto_momentum_lab.execution_account.expectations import (
    AccountPositionExpectationRegistry,
)
from crypto_momentum_lab.execution_account.sync_models import (
    ExecutionAccountSyncResult,
)
from crypto_momentum_lab.execution_account.user_data_models import (
    AccountUserDataUpdate,
)
from crypto_momentum_lab.execution_account.user_data_sync import (
    AccountUserDataState,
)

log = structlog.get_logger(__name__)

_QueueItem = TypeVar("_QueueItem")


def _accepts_state_kwarg(func: object) -> bool:
    try:
        sig = inspect.signature(func)  # type: ignore[arg-type]
        if "state" in sig.parameters:
            return True
        for p in sig.parameters.values():
            if p.kind == inspect.Parameter.VAR_KEYWORD:
                return True
        return False
    except (ValueError, TypeError):
        return True


def _accepts_reason_kwarg(func: object) -> bool:
    try:
        sig = inspect.signature(func)  # type: ignore[arg-type]
        if "reason" in sig.parameters:
            return True
        for p in sig.parameters.values():
            if p.kind == inspect.Parameter.VAR_KEYWORD:
                return True
        return False
    except (ValueError, TypeError):
        return True


class AccountSyncCycle(Protocol):
    async def sync_once(
        self,
        *,
        observed_at: datetime,
        publish_transient_states: bool,
        include_fills: bool,
    ) -> ExecutionAccountSyncResult: ...


@dataclass(frozen=True, slots=True)
class ContinuousAccountSyncConfig:
    interval_seconds: float = 5.0
    fill_interval_seconds: float = 60.0
    failure_backoff_initial_seconds: float = 10.0
    failure_backoff_max_seconds: float = 300.0

    def __post_init__(self) -> None:
        if self.interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        if self.fill_interval_seconds < self.interval_seconds:
            raise ValueError("fill_interval_seconds must not be below interval_seconds")
        if self.failure_backoff_initial_seconds <= 0:
            raise ValueError("failure_backoff_initial_seconds must be positive")
        if self.failure_backoff_max_seconds < self.failure_backoff_initial_seconds:
            raise ValueError(
                "failure_backoff_max_seconds must not be below "
                "failure_backoff_initial_seconds"
            )


@dataclass(frozen=True, slots=True)
class ContinuousAccountSyncResult:
    cycle_count: int
    failure_count: int
    last_sync: ExecutionAccountSyncResult | None


class ContinuousAccountSyncDaemon:
    def __init__(
        self,
        *,
        service: AccountSyncCycle,
        config: ContinuousAccountSyncConfig,
        clock: Callable[[], datetime] = lambda: datetime.now(tz=UTC),
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        on_error: Callable[[Exception], None] | None = None,
    ) -> None:
        self._service = service
        self._config = config
        self._clock = clock
        self._sleep = sleep
        self._on_error = on_error

    async def run(
        self,
        *,
        max_cycles: int | None = None,
    ) -> ContinuousAccountSyncResult:
        if max_cycles is not None and max_cycles <= 0:
            raise ValueError("max_cycles must be positive when present")
        cycle_count = 0
        failure_count = 0
        consecutive_failures = 0
        last_sync: ExecutionAccountSyncResult | None = None
        last_fill_sync_at: datetime | None = None
        publish_transient_states = True
        while max_cycles is None or cycle_count < max_cycles:
            observed_at = self._now()
            include_fills = (
                last_fill_sync_at is None
                or observed_at
                >= last_fill_sync_at
                + timedelta(seconds=self._config.fill_interval_seconds)
            )
            try:
                last_sync = await self._service.sync_once(
                    observed_at=observed_at,
                    publish_transient_states=publish_transient_states,
                    include_fills=include_fills,
                )
                if include_fills:
                    last_fill_sync_at = observed_at
                consecutive_failures = 0
                retry_after_seconds = None
            except Exception as error:
                failure_count += 1
                consecutive_failures += 1
                retry_after_seconds = _retry_after_seconds(error)
                if self._on_error is not None:
                    self._on_error(error)
            publish_transient_states = False
            cycle_count += 1
            if max_cycles is None or cycle_count < max_cycles:
                await self._sleep_for_interval(
                    consecutive_failures=consecutive_failures,
                    retry_after_seconds=retry_after_seconds,
                )
        return ContinuousAccountSyncResult(
            cycle_count=cycle_count,
            failure_count=failure_count,
            last_sync=last_sync,
        )

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("clock must return timezone-aware datetime")
        return value

    async def _sleep_for_interval(
        self,
        *,
        consecutive_failures: int,
        retry_after_seconds: float | None,
    ) -> None:
        if consecutive_failures == 0:
            delay = self._config.interval_seconds
        else:
            exponent = min(consecutive_failures - 1, 30)
            delay = min(
                self._config.failure_backoff_initial_seconds * (2**exponent),
                self._config.failure_backoff_max_seconds,
            )
            if retry_after_seconds is not None:
                delay = min(
                    max(delay, retry_after_seconds),
                    self._config.failure_backoff_max_seconds,
                )
        await self._sleep(delay)


class UserDataAccountSyncCycle(AccountSyncCycle, Protocol):
    async def publish_user_data_heartbeat(
        self,
        *,
        observed_at: datetime,
        state: ExecutionAccountStatus | None = None,
    ) -> None: ...

    async def persist_user_data_event(
        self,
        *,
        snapshot: AccountSnapshot,
        event: BinanceUserDataEvent,
        fills: tuple[AccountFillEvent, ...] = (),
    ) -> ExecutionAccountSyncResult: ...


UserDataAccountPersistedCallback = Callable[
    [BinanceUserDataEvent, ExecutionAccountSyncResult],
    None,
]
UserDataAccountAppliedCallback = Callable[
    [BinanceUserDataEvent, ExecutionAccountSyncResult],
    None,
]
UserDataAccountSnapshotCallback = Callable[
    [ExecutionAccountSyncResult],
    None,
]
UserDataAccountReconciledFillCallback = Callable[
    [AccountFillEvent, ExecutionAccountSyncResult],
    None,
]


class UserDataAccountEventStream(Protocol):
    def set_handler(self, on_event: UserDataEventSink) -> None:
        pass

    async def run(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    @property
    def metrics(self) -> object:
        pass

    async def request_reconnect(self, reason: str) -> None:
        pass


@dataclass(frozen=True, slots=True)
class UserDataAccountSyncConfig:
    heartbeat_interval_seconds: float = 30.0
    failure_backoff_initial_seconds: float = 10.0
    failure_backoff_max_seconds: float = 300.0
    event_queue_size: int = 256
    persistence_queue_size: int = 256
    deferred_event_buffer_size: int = 512
    fill_reconciliation_interval_seconds: float = 300.0

    def __post_init__(self) -> None:
        if self.heartbeat_interval_seconds <= 0:
            raise ValueError("heartbeat_interval_seconds must be positive")
        if self.failure_backoff_initial_seconds <= 0:
            raise ValueError("failure_backoff_initial_seconds must be positive")
        if self.failure_backoff_max_seconds < self.failure_backoff_initial_seconds:
            raise ValueError(
                "failure_backoff_max_seconds must not be below "
                "failure_backoff_initial_seconds"
            )
        if self.event_queue_size <= 0:
            raise ValueError("event_queue_size must be positive")
        if self.persistence_queue_size <= 0:
            raise ValueError("persistence_queue_size must be positive")
        if self.deferred_event_buffer_size <= 0:
            raise ValueError("deferred_event_buffer_size must be positive")
        if self.fill_reconciliation_interval_seconds <= 0:
            raise ValueError("fill_reconciliation_interval_seconds must be positive")


@dataclass(frozen=True, slots=True)
class _ReceivedUserDataEvent:
    event: BinanceUserDataEvent
    stream_token: int | None


@dataclass(frozen=True, slots=True)
class _PendingUserDataPersistence:
    snapshot: AccountSnapshot
    event: BinanceUserDataEvent
    fills: tuple[AccountFillEvent, ...]


class UserDataAccountSyncDaemon:
    """Use Binance account events as the fast path and REST as the authority."""

    def __init__(
        self,
        *,
        service: UserDataAccountSyncCycle,
        stream: UserDataAccountEventStream,
        config: UserDataAccountSyncConfig,
        clock: Callable[[], datetime] = lambda: datetime.now(tz=UTC),
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        on_error: Callable[[Exception], None] | None = None,
        on_event_applied: UserDataAccountAppliedCallback | None = None,
        on_snapshot: UserDataAccountSnapshotCallback | None = None,
        on_persisted: UserDataAccountPersistedCallback | None = None,
        on_heartbeat: Callable[[], None] | None = None,
        expected_position_registry: AccountPositionExpectationRegistry | None = None,
        on_reconciled_fill: UserDataAccountReconciledFillCallback | None = None,
    ) -> None:
        self._service = service
        self._stream = stream
        self._config = config
        self._clock = clock
        self._sleep = sleep
        self._on_error = on_error
        self._on_persisted = on_persisted
        self._on_heartbeat = on_heartbeat
        self._on_event_applied = on_event_applied
        self._on_snapshot = on_snapshot
        self._expected_position_registry = expected_position_registry
        self._on_reconciled_fill = on_reconciled_fill
        self._state: AccountUserDataState | None = None
        self._last_sync_result: ExecutionAccountSyncResult | None = None
        self._accept_events = False
        self._state_lock = asyncio.Lock()
        self._rest_sync_lock = asyncio.Lock()
        self._event_queue: asyncio.Queue[_ReceivedUserDataEvent] | None = None
        self._deferred_events: deque[BinanceUserDataEvent] = deque()
        self._reconciliation_active = False
        self._receiver_session_id = uuid4().hex
        self._persistence_queue: (
            asyncio.Queue[_PendingUserDataPersistence | None] | None
        ) = None
        self._event_worker_task: asyncio.Task[None] | None = None
        self._persistence_worker_task: asyncio.Task[None] | None = None
        self._pipeline_recovery_event = asyncio.Event()
        self._pipeline_recovery_reason: str | None = None
        self._pipeline_recovery_origin_event: BinanceUserDataEvent | None = None
        self._pipeline_recovery_generation = 0
        self._observed_stream_queue_overflow_count = 0
        self._reconciliation_persistence_tasks: set[asyncio.Task[None]] = set()

    async def run(self, *, stop_requested: asyncio.Event | None = None) -> None:
        startup_started_at = perf_counter()
        startup_ready_logged = False
        startup_stream_logged = False
        log.info("execution_account_startup_started")
        stream_task: asyncio.Task[None] | None = None
        heartbeat_task: asyncio.Task[None] | None = None
        recovery_task: asyncio.Task[None] | None = None
        fill_scan_timer: asyncio.Task[None] | None = None
        stop_task = (
            asyncio.create_task(
                _wait_for_stop(stop_requested),
                name="execution-account-stop-waiter",
            )
            if stop_requested is not None
            else None
        )
        consecutive_failures = 0
        try:
            heartbeat_task = asyncio.create_task(
                self._run_heartbeat_loop(),
                name="execution-account-heartbeat-loop",
            )
            while True:
                if stop_requested is not None and stop_requested.is_set():
                    return
                if self._state is None:
                    try:
                        # Restore account and fills once before accepting live events.
                        result = await self._reconcile(include_fills=True)
                        if not _is_ready_result(result):
                            consecutive_failures += 1
                            await self._sleep_for_failure(consecutive_failures, None)
                            continue
                        consecutive_failures = 0
                        if not startup_ready_logged:
                            log.info(
                                "execution_account_startup_phase",
                                phase="initial_reconciliation_ready",
                                elapsed_ms=round(
                                    (perf_counter() - startup_started_at) * 1000,
                                    3,
                                ),
                            )
                            startup_ready_logged = True
                        self._start_pipeline()
                        self._stream.set_handler(self._on_event)
                        stream_task = asyncio.create_task(
                            self._stream.run(),
                            name="binance-user-data-stream",
                        )
                        if not startup_stream_logged:
                            log.info(
                                "execution_account_startup_phase",
                                phase="user_data_stream_started",
                                elapsed_ms=round(
                                    (perf_counter() - startup_started_at) * 1000,
                                    3,
                                ),
                            )
                            startup_stream_logged = True
                    except Exception as error:
                        consecutive_failures += 1
                        self._report_error(error)
                        await self._sleep_for_failure(
                            consecutive_failures,
                            _retry_after_seconds(error),
                        )
                        continue

                self._start_pipeline()
                if stream_task is None or stream_task.done():
                    if stream_task is not None:
                        self._observe_stream_failure(stream_task)
                    stream_task = asyncio.create_task(
                        self._stream.run(),
                        name="binance-user-data-stream",
                    )

                if recovery_task is None or recovery_task.done():
                    recovery_task = asyncio.create_task(
                        self._wait_for_pipeline_recovery(),
                        name="binance-user-data-pipeline-recovery-waiter",
                    )
                if fill_scan_timer is None:
                    fill_scan_timer = asyncio.create_task(
                        asyncio.sleep(
                            self._config.fill_reconciliation_interval_seconds
                        ),
                        name="account-authoritative-fill-scan-timer",
                    )
                wait_tasks: set[asyncio.Future[None]] = {
                    heartbeat_task,
                    stream_task,
                    recovery_task,
                    fill_scan_timer,
                }
                if self._event_worker_task is not None:
                    wait_tasks.add(self._event_worker_task)
                if self._persistence_worker_task is not None:
                    wait_tasks.add(self._persistence_worker_task)
                if stop_task is not None:
                    wait_tasks.add(stop_task)
                done, _ = await asyncio.wait(
                    wait_tasks,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if stop_task is not None and stop_task in done:
                    return
                if recovery_task in done:
                    recovery_task = None
                    if fill_scan_timer is not None:
                        await _cancel_task(fill_scan_timer)
                        fill_scan_timer = None
                    try:
                        result = await self._recover_pipeline()
                        if _is_usable_result(result):
                            consecutive_failures = 0
                        else:
                            consecutive_failures += 1
                            await self._sleep_for_failure(
                                consecutive_failures,
                                None,
                            )
                    except Exception as error:
                        consecutive_failures += 1
                        if self._event_queue is not None:
                            self._request_pipeline_recovery(
                                f"reconciliation_failed:{type(error).__name__}"
                            )
                        self._report_error(error)
                        await self._sleep_for_failure(
                            consecutive_failures,
                            _retry_after_seconds(error),
                        )
                    continue
                if fill_scan_timer in done:
                    fill_scan_timer = None
                    try:
                        # A healthy WS does not prove every consumer committed
                        # every fill. Replay verified anchored REST scans through
                        # the normal serialized reconciliation and snapshot path.
                        await self._reconcile(include_fills=True)
                    except Exception as error:
                        log.warning(
                            "periodic_fill_reconciliation_failed",
                            error_type=type(error).__name__,
                        )
                        self._report_error(error)
                        # _reconcile retains closed admission and requests the
                        # existing recovery worker on failure; never forge a cut.
                    continue
                if self._event_worker_task in done:
                    self._observe_worker_failure(
                        self._event_worker_task,
                        worker_name="event",
                    )
                    self._event_worker_task = None
                    self._request_pipeline_recovery("event_worker_stopped")
                    continue
                if self._persistence_worker_task in done:
                    self._observe_worker_failure(
                        self._persistence_worker_task,
                        worker_name="persistence",
                    )
                    self._persistence_worker_task = None
                    self._request_pipeline_recovery("persistence_worker_stopped")
                    continue
                if stream_task in done:
                    self._observe_stream_failure(stream_task)
                    continue
                if heartbeat_task in done:
                    self._observe_worker_failure(
                        heartbeat_task,
                        worker_name="heartbeat",
                    )
                    heartbeat_task = asyncio.create_task(
                        self._run_heartbeat_loop(),
                        name="execution-account-heartbeat-loop",
                    )
        finally:
            self._accept_events = False
            await self._stream.stop()
            if stop_task is not None:
                await _cancel_task(stop_task)
            if heartbeat_task is not None:
                await _cancel_task(heartbeat_task)
            if recovery_task is not None:
                await _cancel_task(recovery_task)
            if fill_scan_timer is not None:
                await _cancel_task(fill_scan_timer)
            await self._stop_pipeline()
            if stream_task is not None and not stream_task.done():
                stream_task.cancel()
                try:
                    await stream_task
                except asyncio.CancelledError:
                    pass

    async def _on_event(self, event: BinanceUserDataEvent) -> None:
        receipt = _ReceivedUserDataEvent(
            event,
            getattr(self._stream, "continuity_token", None),
        )
        queue = self._event_queue
        if queue is None:
            await self._record_received_event(receipt)
            await self._process_event(event)
            return
        try:
            queue.put_nowait(receipt)
        except asyncio.QueueFull:
            self._request_pipeline_recovery("event_queue_overflow", origin_event=event)

    async def _record_received_event(self, receipt: _ReceivedUserDataEvent) -> None:
        record = getattr(self._service, "record_user_data_event", None)
        if callable(record):
            try:
                await record(
                    event=receipt.event,
                    receiver_session_id=self._receiver_session_id,
                    stream_token=receipt.stream_token,
                )
            except Exception as error:
                self._report_error(error)
                self._request_pipeline_recovery(
                    "user_data_journal_failed", origin_event=receipt.event
                )
                raise

    async def _process_event(
        self,
        event: BinanceUserDataEvent,
        *,
        replay: bool = False,
    ) -> None:
        needs_reconciliation = False
        try:
            async with self._state_lock:
                if not replay and (
                    self._reconciliation_active
                    or self._pipeline_recovery_event.is_set()
                ):
                    self._defer_event(event)
                    return
                if self._state is None:
                    return
                if not self._accept_events and not replay:
                    return
                update = self._state.apply(event)
                if update.needs_reconciliation:
                    self._accept_events = False
                    needs_reconciliation = self._event_queue is None
                    if not needs_reconciliation:
                        self._request_pipeline_recovery(
                            update.reason or "user_data_event_requires_reconciliation",
                            origin_event=event,
                        )
                elif update.changed:
                    applied_result = _event_applied_result(
                        update,
                        readiness_status=_event_readiness_status(
                            self._last_sync_result
                        ),
                        fills_catching_up=(
                            self._last_sync_result is not None
                            and self._last_sync_result.fills_catching_up
                        ),
                    )
                    persistence_queue = self._persistence_queue
                    if persistence_queue is not None:
                        if persistence_queue.full():
                            self._accept_events = False
                            self._request_pipeline_recovery(
                                "persistence_queue_overflow",
                                origin_event=event,
                            )
                            return
                        self._notify_event_applied(event, applied_result)
                        try:
                            persistence_queue.put_nowait(
                                _PendingUserDataPersistence(
                                    snapshot=update.snapshot,
                                    event=event,
                                    fills=update.fills,
                                )
                            )
                        except asyncio.QueueFull:
                            self._accept_events = False
                            self._request_pipeline_recovery(
                                "persistence_queue_overflow",
                                origin_event=event,
                            )
                    else:
                        self._notify_event_applied(event, applied_result)
                        result = await self._service.persist_user_data_event(
                            snapshot=update.snapshot,
                            event=event,
                            fills=update.fills,
                        )
                        self._notify_persisted(event, result)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._report_error(error)
            if self._event_queue is None:
                needs_reconciliation = True
            else:
                self._request_pipeline_recovery(
                    f"event_processing_failed:{type(error).__name__}",
                    origin_event=event,
                )
        if needs_reconciliation:
            try:
                result = await self._reconcile(include_fills=True)
                if self._on_persisted is not None and _is_ready_result(result):
                    self._notify_persisted(event, result)
            except Exception as error:
                self._report_error(error)

    async def _event_worker(
        self,
        queue: asyncio.Queue[_ReceivedUserDataEvent],
    ) -> None:
        while True:
            receipt = await queue.get()
            event = receipt.event
            try:
                await self._record_received_event(receipt)
                if (
                    self._reconciliation_active
                    or self._pipeline_recovery_event.is_set()
                ):
                    self._defer_event(event)
                else:
                    await self._process_event(event, replay=True)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self._report_error(error)
                self._request_pipeline_recovery(
                    f"event_worker_failed:{type(error).__name__}",
                    origin_event=event,
                )
            finally:
                queue.task_done()

    async def _persistence_worker(
        self,
        queue: asyncio.Queue[_PendingUserDataPersistence | None],
    ) -> None:
        while True:
            pending = await queue.get()
            try:
                if pending is None:
                    return
                if self._pipeline_recovery_event.is_set():
                    continue
                result = await self._service.persist_user_data_event(
                    snapshot=pending.snapshot,
                    event=pending.event,
                    fills=pending.fills,
                )
                self._notify_persisted(pending.event, result)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self._report_error(error)
                self._request_pipeline_recovery(
                    f"event_persistence_failed:{type(error).__name__}",
                    origin_event=(None if pending is None else pending.event),
                )
            finally:
                queue.task_done()

    def _start_pipeline(self) -> None:
        if self._event_queue is None:
            self._event_queue = asyncio.Queue(maxsize=self._config.event_queue_size)
        if self._persistence_queue is None:
            self._persistence_queue = asyncio.Queue(
                maxsize=self._config.persistence_queue_size
            )
        if self._event_worker_task is None:
            self._event_worker_task = asyncio.create_task(
                self._event_worker(self._event_queue),
                name="binance-user-data-event-worker",
            )
        if self._persistence_worker_task is None:
            self._persistence_worker_task = asyncio.create_task(
                self._persistence_worker(self._persistence_queue),
                name="binance-user-data-persistence-worker",
            )

    async def _stop_pipeline(self) -> None:
        event_queue = self._event_queue
        persistence_queue = self._persistence_queue
        if event_queue is not None:
            await self._wait_for_queue_drain(event_queue, "event")
        if persistence_queue is not None:
            await self._wait_for_queue_drain(persistence_queue, "persistence")
        tasks = (
            self._event_worker_task,
            self._persistence_worker_task,
        )
        for task in tasks:
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in tasks if task is not None),
            return_exceptions=True,
        )
        reconciliation_tasks = tuple(self._reconciliation_persistence_tasks)
        self._reconciliation_persistence_tasks.clear()
        if reconciliation_tasks:
            done, pending = await asyncio.wait(
                reconciliation_tasks,
                timeout=30.0,
            )
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            if done:
                await asyncio.gather(*done, return_exceptions=True)
        self._event_worker_task = None
        self._persistence_worker_task = None
        self._event_queue = None
        self._persistence_queue = None

    async def _recover_pipeline(self) -> ExecutionAccountSyncResult:
        self._accept_events = False
        recovery_generation = self._pipeline_recovery_generation
        reason = self._pipeline_recovery_reason or "unspecified"
        request_reconnect = getattr(self._stream, "request_reconnect", None)
        if callable(request_reconnect):
            try:
                reconnect_result = request_reconnect(
                    f"account_event_pipeline_recovery:{reason}"
                )
                if inspect.isawaitable(reconnect_result):
                    await reconnect_result
            except Exception as error:
                self._report_error(error)
        if self._event_queue is not None and not await self._wait_for_queue_drain(
            self._event_queue,
            "event recovery",
        ):
            raise TimeoutError("account event queue did not drain for recovery")
        if self._persistence_queue is not None and not await self._wait_for_queue_drain(
            self._persistence_queue,
            "persistence recovery",
        ):
            raise TimeoutError("account persistence queue did not drain for recovery")
        result = await self._reconcile(
            include_fills=True,
            wait_for_pipeline=False,
        )
        if _is_usable_result(result):
            origin_event = self._pipeline_recovery_origin_event
            async with self._state_lock:
                recovery_still_current = (
                    recovery_generation == self._pipeline_recovery_generation
                )
                if recovery_still_current:
                    self._pipeline_recovery_reason = None
                    self._pipeline_recovery_origin_event = None
                    self._pipeline_recovery_event.clear()
            if recovery_still_current:
                await self._replay_deferred_events()
                async with self._state_lock:
                    if not self._pipeline_recovery_event.is_set():
                        self._reconciliation_active = False
                        self._accept_events = True
            if origin_event is not None and self._on_persisted is not None:
                self._notify_persisted(origin_event, result)
        return result

    async def _wait_for_queue_drain(
        self,
        queue: asyncio.Queue[_QueueItem],
        queue_name: str,
    ) -> bool:
        try:
            await asyncio.wait_for(queue.join(), timeout=30.0)
        except TimeoutError:
            log.error(
                "binance_user_data_queue_drain_timed_out",
                queue=queue_name,
                queue_size=queue.qsize(),
            )
            return False
        return True

    async def _wait_for_pipeline_recovery(self) -> None:
        await self._pipeline_recovery_event.wait()

    def _defer_event(self, event: BinanceUserDataEvent) -> None:
        """Keep events received during a snapshot/recovery window for replay."""
        if len(self._deferred_events) >= self._config.deferred_event_buffer_size:
            self._request_pipeline_recovery(
                "deferred_event_buffer_overflow",
                origin_event=event,
            )
            return
        self._deferred_events.append(event)

    async def _replay_deferred_events(self) -> None:
        if not self._deferred_events:
            return
        deferred = sorted(
            self._deferred_events,
            key=lambda item: (
                item.exchange_event_at or item.event_at,
                item.event_at,
                item.received_at,
                item.event_id,
            ),
        )
        self._deferred_events.clear()
        for event in deferred:
            if self._pipeline_recovery_event.is_set():
                self._defer_event(event)
                continue
            await self._process_event(event, replay=True)

    def _notify_event_applied(
        self,
        event: BinanceUserDataEvent,
        result: ExecutionAccountSyncResult,
    ) -> None:
        if self._on_event_applied is None:
            return
        try:
            self._on_event_applied(event, result)
        except Exception as error:
            self._report_error(error)

    def _notify_persisted(
        self,
        event: BinanceUserDataEvent,
        result: ExecutionAccountSyncResult,
    ) -> None:
        if self._on_persisted is None:
            return
        try:
            self._on_persisted(event, result)
        except Exception as error:
            self._report_error(error)

    def _notify_snapshot(self, result: ExecutionAccountSyncResult) -> None:
        if self._on_snapshot is None:
            return
        try:
            self._on_snapshot(result)
        except Exception as error:
            self._report_error(error)

    def _notify_reconciled_fills(
        self,
        result: ExecutionAccountSyncResult,
    ) -> None:
        if self._on_reconciled_fill is None:
            return
        for fill in result.new_fills:
            try:
                self._on_reconciled_fill(fill, result)
            except Exception as error:
                self._report_error(error)

    def _schedule_reconciliation_persistence(
        self,
        result: ExecutionAccountSyncResult,
    ) -> None:
        persist = getattr(
            self._service,
            "persist_reconciliation_result",
            None,
        )
        if not callable(persist):
            return
        task = asyncio.create_task(
            self._persist_reconciliation_result(persist, result),
            name="account-reconciliation-persistence",
        )
        self._reconciliation_persistence_tasks.add(task)
        task.add_done_callback(self._reconciliation_persistence_tasks.discard)

    async def _persist_reconciliation_result(
        self,
        persist: Callable[[ExecutionAccountSyncResult], Awaitable[None]],
        result: ExecutionAccountSyncResult,
    ) -> None:
        try:
            await persist(result)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._report_error(error)

    def _request_pipeline_recovery(
        self,
        reason: str,
        *,
        origin_event: BinanceUserDataEvent | None = None,
    ) -> None:
        self._accept_events = False
        self._pipeline_recovery_generation += 1
        if self._pipeline_recovery_reason is None:
            self._pipeline_recovery_reason = reason
        if origin_event is not None and self._pipeline_recovery_origin_event is None:
            self._pipeline_recovery_origin_event = origin_event
        self._pipeline_recovery_event.set()
        log_fn = (
            log.info
            if reason == "account_config_update"
            else log.error
        )
        log_fn(
            "binance_user_data_pipeline_recovery_requested",
            reason=reason,
            queue_size=(
                None if self._event_queue is None else self._event_queue.qsize()
            ),
            persistence_queue_size=(
                None
                if self._persistence_queue is None
                else self._persistence_queue.qsize()
            ),
        )

    async def _publish_heartbeat(self) -> None:
        self._check_stream_queue_health()
        if self._state is None or not (
            self._accept_events
            or self._reconciliation_active
            or self._pipeline_recovery_event.is_set()
        ):
            return
        last_sync_result = self._last_sync_result
        is_syncing = (
            self._reconciliation_active or self._pipeline_recovery_event.is_set()
        ) or (
            last_sync_result is not None
            and (
                last_sync_result.fills_catching_up
                or last_sync_result.status == ExecutionAccountStatus.SYNCING
            )
        )
        target_state = ExecutionAccountStatus.RUNNING
        reason = (
            "reconciling"
            if (self._reconciliation_active or self._pipeline_recovery_event.is_set())
            else ("fills_catching_up" if is_syncing else None)
        )
        now = self._now()
        publish_heartbeat = self._service.publish_user_data_heartbeat
        if _accepts_state_kwarg(publish_heartbeat):
            if _accepts_reason_kwarg(publish_heartbeat):
                await publish_heartbeat(
                    observed_at=now,
                    state=target_state,
                    reason=reason,
                )
            else:
                await publish_heartbeat(
                    observed_at=now,
                    state=target_state,
                )
        else:
            await publish_heartbeat(observed_at=now)
        self._notify_heartbeat()
        stream_token = getattr(self._stream, "continuity_token", 1)
        is_stream_open = stream_token is not None
        if not is_syncing and self._accept_events and is_stream_open:
            snapshot_fn = getattr(self._state, "snapshot", None)
            if callable(snapshot_fn):
                snapshot = snapshot_fn(now)
            elif isinstance(self._state, AccountSnapshot):
                snapshot = replace(
                    self._state,
                    config=replace(self._state.config, observed_at=now),
                )
            else:
                snapshot = None
            if snapshot is not None:
                heartbeat_result = ExecutionAccountSyncResult(
                    status=target_state,
                    reconciliation_id=f"heartbeat:{now.isoformat()}",
                    mismatch_count=0,
                    snapshot=snapshot,
                )
                self._notify_snapshot(heartbeat_result)

    async def _run_heartbeat_loop(self) -> None:
        while True:
            await self._sleep(self._config.heartbeat_interval_seconds)
            try:
                await self._publish_heartbeat()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self._report_error(error)

    async def _reconcile(
        self,
        *,
        include_fills: bool,
        wait_for_pipeline: bool = True,
    ) -> ExecutionAccountSyncResult:
        try:
            return await self._reconcile_impl(
                include_fills=include_fills,
                wait_for_pipeline=wait_for_pipeline,
            )
        except Exception:
            if not self._pipeline_recovery_event.is_set():
                self._reconciliation_active = False
                self._accept_events = False
                if self._event_queue is not None:
                    self._request_pipeline_recovery("reconciliation_failed")
            raise

    async def _reconcile_impl(
        self,
        *,
        include_fills: bool,
        wait_for_pipeline: bool = True,
    ) -> ExecutionAccountSyncResult:
        self._reconciliation_active = True
        if wait_for_pipeline and self._event_queue is not None:
            async with self._state_lock:
                self._accept_events = False
            if not await self._wait_for_queue_drain(
                self._event_queue,
                "event reconciliation",
            ):
                self._request_pipeline_recovery("event_queue_drain_timeout")
                raise TimeoutError(
                    "account event queue did not drain before reconciliation"
                )
            if (
                self._persistence_queue is not None
                and not await self._wait_for_queue_drain(
                    self._persistence_queue,
                    "persistence reconciliation",
                )
            ):
                self._request_pipeline_recovery("persistence_queue_drain_timeout")
                raise TimeoutError(
                    "account persistence queue did not drain before reconciliation"
                )
        async with self._state_lock:
            async with self._rest_sync_lock:
                realtime_sync = getattr(
                    self._service,
                    "sync_once_for_realtime",
                    None,
                )
                realtime_sync_callable = (
                    cast(
                        Callable[..., Awaitable[ExecutionAccountSyncResult]],
                        realtime_sync,
                    )
                    if callable(realtime_sync)
                    else None
                )
                use_realtime_sync = (
                    self._state is not None and realtime_sync_callable is not None
                )
                if realtime_sync_callable is not None and use_realtime_sync:
                    result = await realtime_sync_callable(
                        observed_at=self._now(),
                        publish_transient_states=False,
                        include_fills=include_fills,
                    )
                else:
                    result = await self._service.sync_once(
                        observed_at=self._now(),
                        publish_transient_states=False,
                        include_fills=include_fills,
                    )
            if _is_usable_result(result) and result.snapshot is not None:
                self._last_sync_result = result
                snapshot = result.snapshot
                if self._state is None:
                    self._state = AccountUserDataState(
                        snapshot,
                        expected_position_registry=(self._expected_position_registry),
                    )
                else:
                    self._state.replace_snapshot(snapshot)
                self._accept_events = False
            else:
                self._last_sync_result = result
                self._reconciliation_active = False
                self._accept_events = False
                if self._event_queue is not None:
                    self._request_pipeline_recovery("reconciliation_not_ready")
        if _is_usable_result(result):
            self._notify_heartbeat()
            # Publish immediately after the in-memory state has been replaced.
            # Reconciliation inspection is telemetry/recovery bookkeeping and
            # must not delay the account-state handoff to live consumers.
            self._notify_snapshot(result)
            self._notify_reconciled_fills(result)
            if use_realtime_sync:
                self._schedule_reconciliation_persistence(result)
            if not self._pipeline_recovery_event.is_set():
                await self._replay_deferred_events()
                async with self._state_lock:
                    if not self._pipeline_recovery_event.is_set():
                        self._reconciliation_active = False
                        self._accept_events = True
        self._check_stream_queue_health()
        return result

    def _notify_heartbeat(self) -> None:
        if self._on_heartbeat is None:
            return
        try:
            self._on_heartbeat()
        except Exception as error:
            self._report_error(error)

    def _check_stream_queue_health(self) -> None:
        metrics = getattr(self._stream, "metrics", None)
        overflow_count = _metric_int(metrics, "event_queue_overflow_count")
        if overflow_count is None:
            return
        if overflow_count <= self._observed_stream_queue_overflow_count:
            return
        self._observed_stream_queue_overflow_count = overflow_count
        self._request_pipeline_recovery("stream_event_queue_overflow")

    async def _sleep_for_failure(
        self,
        consecutive_failures: int,
        retry_after_seconds: float | None,
    ) -> None:
        exponent = min(max(consecutive_failures - 1, 0), 30)
        delay = min(
            self._config.failure_backoff_initial_seconds * (2**exponent),
            self._config.failure_backoff_max_seconds,
        )
        if retry_after_seconds is not None:
            delay = min(
                max(delay, retry_after_seconds),
                self._config.failure_backoff_max_seconds,
            )
        await self._sleep(delay)

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("clock must return timezone-aware datetime")
        return value

    def _report_error(self, error: Exception) -> None:
        if self._on_error is not None:
            self._on_error(error)

    def _observe_worker_failure(
        self,
        task: asyncio.Task[None] | None,
        *,
        worker_name: str,
    ) -> None:
        if task is None:
            return
        try:
            error = task.exception()
        except asyncio.CancelledError:
            error = RuntimeError(f"{worker_name} worker was cancelled")
        if error is None:
            error = RuntimeError(f"{worker_name} worker stopped")
        elif not isinstance(error, Exception):
            error = RuntimeError(
                f"{worker_name} worker failed with {type(error).__name__}"
            )
        self._report_error(error)

    def _observe_stream_failure(self, task: asyncio.Task[None]) -> None:
        try:
            error = task.exception()
        except asyncio.CancelledError:
            return
        if error is not None:
            if isinstance(error, Exception):
                self._report_error(error)


def _retry_after_seconds(error: Exception) -> float | None:
    value = getattr(error, "retry_after_seconds", None)
    if isinstance(value, int | float) and not isinstance(value, bool):
        if value >= 0:
            return float(value)
    return None


def _metric_int(metrics: object, name: str) -> int | None:
    value = getattr(metrics, name, None)
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    return None


def _is_ready_result(result: ExecutionAccountSyncResult) -> bool:
    return (
        result.status in (ExecutionAccountStatus.RUNNING, ExecutionAccountStatus.READY_READONLY)
        and result.snapshot is not None
    )


def _is_usable_result(result: ExecutionAccountSyncResult) -> bool:
    return (
        result.status in (
            ExecutionAccountStatus.RUNNING,
            ExecutionAccountStatus.READY_READONLY,
            ExecutionAccountStatus.SYNCING,
        )
        and result.snapshot is not None
    )


def _ready_snapshot(result: ExecutionAccountSyncResult) -> AccountSnapshot:
    if not _is_ready_result(result) or result.snapshot is None:
        raise ValueError("execution account result does not contain a ready snapshot")
    return result.snapshot


async def _cancel_task(task: asyncio.Future[None]) -> None:
    if task.done():
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def _wait_for_stop(stop_requested: asyncio.Event) -> None:
    await stop_requested.wait()


def _event_applied_result(
    update: AccountUserDataUpdate,
    *,
    readiness_status: ExecutionAccountStatus,
    fills_catching_up: bool,
) -> ExecutionAccountSyncResult:
    event = update.event
    fills = update.fills
    return ExecutionAccountSyncResult(
        status=readiness_status,
        reconciliation_id=f"account-event:{event.event_id}",
        mismatch_count=0,
        snapshot=update.snapshot,
        delta=update.delta,
        fill_count=len(fills),
        fills=fills,
        new_fills=fills,
        new_fill_keys=frozenset((fill.symbol, fill.trade_id) for fill in fills),
        fill_count_by_symbol=_fill_counts_by_symbol(fills),
        fills_catching_up=fills_catching_up,
    )


def _event_readiness_status(
    last_sync_result: ExecutionAccountSyncResult | None,
) -> ExecutionAccountStatus:
    """User-data deltas inherit readiness from the last REST reconciliation."""
    if last_sync_result is not None and last_sync_result.status in (
        ExecutionAccountStatus.HALTED_READONLY,
        ExecutionAccountStatus.STOPPED,
    ):
        return last_sync_result.status
    return ExecutionAccountStatus.RUNNING


def _fill_counts_by_symbol(
    fills: tuple[AccountFillEvent, ...],
) -> tuple[tuple[str, int], ...]:
    counts: dict[str, int] = {}
    for fill in fills:
        counts[fill.symbol] = counts.get(fill.symbol, 0) + 1
    return tuple(sorted(counts.items()))
