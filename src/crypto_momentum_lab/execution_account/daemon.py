import asyncio
import inspect
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol, TypeVar

import structlog

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    ExecutionAccountStatus,
)
from crypto_momentum_lab.execution_account.binance.user_data import (
    BinanceUserDataEvent,
    UserDataEventSink,
)
from crypto_momentum_lab.execution_account.sync import (
    AccountSnapshot,
    ExecutionAccountSyncResult,
    FillKey,
)
from crypto_momentum_lab.execution_account.user_data_sync import (
    AccountUserDataState,
    AccountUserDataUpdate,
)

log = structlog.get_logger(__name__)

_QueueItem = TypeVar("_QueueItem")


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
            raise ValueError(
                "fill_interval_seconds must not be below interval_seconds"
            )
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
    async def snapshot_once(self, *, observed_at: datetime) -> None: ...

    async def publish_user_data_heartbeat(
        self,
        *,
        observed_at: datetime,
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
    rest_reconciliation_interval_seconds: float = 300.0
    snapshot_interval_seconds: float = 15.0
    heartbeat_interval_seconds: float = 30.0
    failure_backoff_initial_seconds: float = 10.0
    failure_backoff_max_seconds: float = 300.0
    event_queue_size: int = 256
    persistence_queue_size: int = 256

    def __post_init__(self) -> None:
        if self.rest_reconciliation_interval_seconds <= 0:
            raise ValueError("rest_reconciliation_interval_seconds must be positive")
        if self.snapshot_interval_seconds <= 0:
            raise ValueError("snapshot_interval_seconds must be positive")
        if self.heartbeat_interval_seconds <= 0:
            raise ValueError("heartbeat_interval_seconds must be positive")
        if self.heartbeat_interval_seconds > self.rest_reconciliation_interval_seconds:
            raise ValueError(
                "heartbeat_interval_seconds must not exceed "
                "rest_reconciliation_interval_seconds"
            )
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
    ) -> None:
        self._service = service
        self._stream = stream
        self._config = config
        self._clock = clock
        self._sleep = sleep
        self._on_error = on_error
        self._on_persisted = on_persisted
        self._on_event_applied = on_event_applied
        self._on_snapshot = on_snapshot
        self._state: AccountUserDataState | None = None
        self._accept_events = False
        self._state_lock = asyncio.Lock()
        self._rest_sync_lock = asyncio.Lock()
        self._pending_missing_fill_keys: dict[FillKey, datetime] = {}
        self._event_queue: asyncio.Queue[BinanceUserDataEvent] | None = None
        self._persistence_queue: asyncio.Queue[
            _PendingUserDataPersistence | None
        ] | None = None
        self._event_worker_task: asyncio.Task[None] | None = None
        self._persistence_worker_task: asyncio.Task[None] | None = None
        self._pipeline_recovery_event = asyncio.Event()
        self._pipeline_recovery_reason: str | None = None
        self._pipeline_recovery_origin_event: BinanceUserDataEvent | None = None
        self._observed_stream_queue_overflow_count = 0

    async def run(self) -> None:
        stream_task: asyncio.Task[None] | None = None
        heartbeat_task: asyncio.Future[None] | None = None
        reconciliation_task: asyncio.Future[None] | None = None
        snapshot_task: asyncio.Future[None] | None = None
        recovery_task: asyncio.Task[None] | None = None
        consecutive_failures = 0
        try:
            while True:
                if self._state is None:
                    try:
                        result = await self._reconcile(include_fills=True)
                        if not _is_ready_result(result):
                            consecutive_failures += 1
                            await self._sleep_for_failure(consecutive_failures, None)
                            continue
                        consecutive_failures = 0
                        self._start_pipeline()
                        self._stream.set_handler(self._on_event)
                        stream_task = asyncio.create_task(
                            self._stream.run(),
                            name="binance-user-data-stream",
                        )
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

                if heartbeat_task is None or heartbeat_task.done():
                    heartbeat_task = asyncio.ensure_future(
                        self._sleep(self._config.heartbeat_interval_seconds)
                    )
                if reconciliation_task is None or reconciliation_task.done():
                    reconciliation_task = asyncio.ensure_future(
                        self._sleep(
                            self._config.rest_reconciliation_interval_seconds
                        )
                    )
                if snapshot_task is None or snapshot_task.done():
                    snapshot_task = asyncio.ensure_future(
                        self._sleep(self._config.snapshot_interval_seconds)
                    )
                if recovery_task is None or recovery_task.done():
                    recovery_task = asyncio.create_task(
                        self._wait_for_pipeline_recovery(),
                        name="binance-user-data-pipeline-recovery-waiter",
                    )
                wait_tasks: set[asyncio.Future[None]] = {
                    heartbeat_task,
                    reconciliation_task,
                    snapshot_task,
                    stream_task,
                    recovery_task,
                }
                if self._event_worker_task is not None:
                    wait_tasks.add(self._event_worker_task)
                if self._persistence_worker_task is not None:
                    wait_tasks.add(self._persistence_worker_task)
                done, _ = await asyncio.wait(
                    wait_tasks,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if recovery_task in done:
                    recovery_task = None
                    if heartbeat_task is not None:
                        await _cancel_task(heartbeat_task)
                    if reconciliation_task is not None:
                        await _cancel_task(reconciliation_task)
                    if snapshot_task is not None:
                        await _cancel_task(snapshot_task)
                    heartbeat_task = None
                    reconciliation_task = None
                    snapshot_task = None
                    try:
                        result = await self._recover_pipeline()
                        if _is_ready_result(result):
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
                    if heartbeat_task is not None:
                        await _cancel_task(heartbeat_task)
                    if reconciliation_task is not None:
                        await _cancel_task(reconciliation_task)
                    if snapshot_task is not None:
                        await _cancel_task(snapshot_task)
                    heartbeat_task = None
                    reconciliation_task = None
                    snapshot_task = None
                    self._observe_stream_failure(stream_task)
                    continue
                if heartbeat_task in done:
                    heartbeat_task = None
                    try:
                        await self._publish_heartbeat()
                    except Exception as error:
                        self._report_error(error)
                if reconciliation_task in done:
                    reconciliation_task = None
                    try:
                        result = await self._reconcile(include_fills=True)
                        if _is_ready_result(result):
                            consecutive_failures = 0
                        else:
                            self._accept_events = False
                            consecutive_failures += 1
                    except Exception as error:
                        consecutive_failures += 1
                        self._report_error(error)
                        await self._sleep_for_failure(
                            consecutive_failures,
                            _retry_after_seconds(error),
                        )
                if snapshot_task in done:
                    snapshot_task = None
                    try:
                        await self._snapshot()
                    except Exception as error:
                        if self._event_queue is not None:
                            self._request_pipeline_recovery(
                                f"snapshot_failed:{type(error).__name__}"
                            )
                        self._report_error(error)
        finally:
            self._accept_events = False
            await self._stream.stop()
            if heartbeat_task is not None:
                await _cancel_task(heartbeat_task)
            if reconciliation_task is not None:
                await _cancel_task(reconciliation_task)
            if snapshot_task is not None:
                await _cancel_task(snapshot_task)
            if recovery_task is not None:
                await _cancel_task(recovery_task)
            await self._stop_pipeline()
            if stream_task is not None and not stream_task.done():
                stream_task.cancel()
                try:
                    await stream_task
                except asyncio.CancelledError:
                    pass

    async def _on_event(self, event: BinanceUserDataEvent) -> None:
        event_queue = self._event_queue
        if event_queue is None:
            await self._process_event(event)
            return
        if not self._accept_events or self._state is None:
            return
        try:
            event_queue.put_nowait(event)
        except asyncio.QueueFull:
            self._request_pipeline_recovery(
                "event_queue_overflow",
                origin_event=event,
            )

    async def _process_event(self, event: BinanceUserDataEvent) -> None:
        needs_reconciliation = False
        try:
            async with self._state_lock:
                if not self._accept_events or self._state is None:
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
                    applied_result = _event_applied_result(update)
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
        queue: asyncio.Queue[BinanceUserDataEvent],
    ) -> None:
        while True:
            event = await queue.get()
            try:
                await self._process_event(event)
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
                async with self._rest_sync_lock:
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
            self._event_queue = asyncio.Queue(
                maxsize=self._config.event_queue_size
            )
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
        self._event_worker_task = None
        self._persistence_worker_task = None
        self._event_queue = None
        self._persistence_queue = None

    async def _recover_pipeline(self) -> ExecutionAccountSyncResult:
        self._accept_events = False
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
        if (
            self._persistence_queue is not None
            and not await self._wait_for_queue_drain(
                self._persistence_queue,
                "persistence recovery",
            )
        ):
            raise TimeoutError(
                "account persistence queue did not drain for recovery"
            )
        result = await self._reconcile(
            include_fills=True,
            wait_for_pipeline=False,
        )
        if _is_ready_result(result):
            origin_event = self._pipeline_recovery_origin_event
            async with self._state_lock:
                self._pipeline_recovery_reason = None
                self._pipeline_recovery_origin_event = None
                self._pipeline_recovery_event.clear()
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

    def _request_pipeline_recovery(
        self,
        reason: str,
        *,
        origin_event: BinanceUserDataEvent | None = None,
    ) -> None:
        self._accept_events = False
        if self._pipeline_recovery_reason is None:
            self._pipeline_recovery_reason = reason
        if (
            origin_event is not None
            and self._pipeline_recovery_origin_event is None
        ):
            self._pipeline_recovery_origin_event = origin_event
        self._pipeline_recovery_event.set()
        log.error(
            "binance_user_data_pipeline_recovery_requested",
            reason=reason,
            queue_size=(
                None
                if self._event_queue is None
                else self._event_queue.qsize()
            ),
            persistence_queue_size=(
                None
                if self._persistence_queue is None
                else self._persistence_queue.qsize()
            ),
        )

    async def _publish_heartbeat(self) -> None:
        self._check_stream_queue_health()
        async with self._state_lock:
            if self._accept_events and self._state is not None:
                async with self._rest_sync_lock:
                    await self._service.publish_user_data_heartbeat(
                        observed_at=self._now(),
                    )

    async def _reconcile(
        self,
        *,
        include_fills: bool,
        wait_for_pipeline: bool = True,
    ) -> ExecutionAccountSyncResult:
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
            if self._persistence_queue is not None and not await (
                self._wait_for_queue_drain(
                    self._persistence_queue,
                    "persistence reconciliation",
                )
            ):
                self._request_pipeline_recovery(
                    "persistence_queue_drain_timeout"
                )
                raise TimeoutError(
                    "account persistence queue did not drain before reconciliation"
                )
        async with self._state_lock:
            async with self._rest_sync_lock:
                result = await self._service.sync_once(
                    observed_at=self._now(),
                    publish_transient_states=False,
                    include_fills=include_fills,
                )
            if _is_ready_result(result):
                snapshot = _ready_snapshot(result)
                if self._state is None:
                    self._state = AccountUserDataState(snapshot)
                else:
                    self._state.replace_snapshot(snapshot)
                self._accept_events = not self._pipeline_recovery_event.is_set()
            else:
                self._accept_events = False
        if _is_ready_result(result):
            # Publish immediately after the in-memory state has been replaced.
            # Reconciliation inspection is telemetry/recovery bookkeeping and
            # must not delay the account-state handoff to live consumers.
            self._notify_snapshot(result)
        await self._inspect_reconciliation(result)
        return result

    async def _snapshot(self) -> None:
        pipeline_active = self._event_queue is not None
        if pipeline_active:
            async with self._state_lock:
                self._accept_events = False
            if self._event_queue is not None:
                if not await self._wait_for_queue_drain(
                    self._event_queue,
                    "event snapshot",
                ):
                    self._request_pipeline_recovery("event_queue_drain_timeout")
                    return
            if self._persistence_queue is not None:
                if not await self._wait_for_queue_drain(
                    self._persistence_queue,
                    "persistence snapshot",
                ):
                    self._request_pipeline_recovery(
                        "persistence_queue_drain_timeout"
                    )
                    return
        async with self._state_lock:
            async with self._rest_sync_lock:
                await self._service.snapshot_once(observed_at=self._now())
            if pipeline_active:
                self._accept_events = (
                    self._state is not None
                    and not self._pipeline_recovery_event.is_set()
                )

    async def _inspect_reconciliation(
        self,
        result: ExecutionAccountSyncResult,
    ) -> None:
        if not _is_ready_result(result):
            return
        self._check_stream_queue_health()
        metrics = getattr(self._stream, "metrics", None)
        stream_event_count = _metric_int(metrics, "parsed_event_count")
        stream_fill_event_count = _metric_int(metrics, "fill_event_count")
        if stream_event_count is None or stream_fill_event_count is None:
            return

        stream_fill_keys = _metric_fill_keys(metrics)
        if stream_fill_keys is not None:
            pending_before = dict(self._pending_missing_fill_keys)
            candidates = set(pending_before)
            candidates.update(result.new_fill_keys)
            now = self._now()
            pending_after: dict[FillKey, datetime] = {}
            still_missing: set[FillKey] = set()
            for fill_key in candidates:
                if fill_key in stream_fill_keys:
                    continue
                if fill_key in pending_before:
                    still_missing.add(fill_key)
                else:
                    pending_after[fill_key] = now

            reconnect_requested = False
            if still_missing:
                request_reconnect = getattr(
                    self._stream,
                    "request_reconnect",
                    None,
                )
                if callable(request_reconnect):
                    try:
                        reconnect_result = request_reconnect(
                            "rest_reconciliation_found_unmatched_fill_keys"
                        )
                        if inspect.isawaitable(reconnect_result):
                            await reconnect_result
                        reconnect_requested = True
                    except Exception as error:
                        self._report_error(error)
                        pending_after.update(
                            {
                                fill_key: pending_before[fill_key]
                                for fill_key in still_missing
                            }
                        )
                log.warning(
                    "binance_user_data_stream_missing_fill_events",
                    rest_new_fill_count=len(result.new_fill_keys),
                    unmatched_fill_count=len(still_missing),
                    unmatched_fill_keys=sorted(still_missing)[:10],
                    pending_fill_count=len(pending_after),
                    parsed_event_count=stream_event_count,
                    reconnect_requested=reconnect_requested,
                )
            self._pending_missing_fill_keys = pending_after

        last_event_received_at = getattr(
            metrics,
            "last_event_received_at",
            None,
        )
        log.info(
            "binance_user_data_stream_health",
            parsed_event_count=stream_event_count,
            fill_event_count=stream_fill_event_count,
            fill_event_key_count=(
                None if stream_fill_keys is None else len(stream_fill_keys)
            ),
            rest_fill_count=result.fill_count,
            rest_new_fill_count=len(result.new_fill_keys),
            rest_fill_counts_by_symbol=dict(result.fill_count_by_symbol),
            pending_fill_count=len(self._pending_missing_fill_keys),
            event_queue_size=(
                None if self._event_queue is None else self._event_queue.qsize()
            ),
            persistence_queue_size=(
                None
                if self._persistence_queue is None
                else self._persistence_queue.qsize()
            ),
            last_event_received_at=(
                None
                if not isinstance(last_event_received_at, datetime)
                else last_event_received_at.isoformat()
            ),
        )

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


def _metric_fill_keys(metrics: object) -> set[FillKey] | None:
    value = getattr(metrics, "fill_event_keys", None)
    if value is None or isinstance(value, str | bytes):
        return None
    if not isinstance(value, Iterable):
        return None
    keys: set[FillKey] = set()
    for item in value:
        if not isinstance(item, tuple | list) or len(item) != 2:
            return None
        symbol, trade_id = item
        if not isinstance(symbol, str) or not isinstance(trade_id, str):
            return None
        normalized_symbol = symbol.strip().upper()
        normalized_trade_id = trade_id.strip()
        if not normalized_symbol or not normalized_trade_id:
            return None
        keys.add((normalized_symbol, normalized_trade_id))
    return keys


def _is_ready_result(result: ExecutionAccountSyncResult) -> bool:
    return (
        result.status.value == "ready_readonly"
        and result.snapshot is not None
    )


def _ready_snapshot(result: ExecutionAccountSyncResult) -> AccountSnapshot:
    if not _is_ready_result(result):
        raise ValueError("execution account result does not contain a ready snapshot")
    assert result.snapshot is not None
    return result.snapshot


async def _cancel_task(task: asyncio.Future[None]) -> None:
    if task.done():
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def _event_applied_result(
    update: AccountUserDataUpdate,
) -> ExecutionAccountSyncResult:
    event = update.event
    fills = update.fills
    return ExecutionAccountSyncResult(
        status=ExecutionAccountStatus.READY_READONLY,
        reconciliation_id=f"account-event:{event.event_id}",
        mismatch_count=0,
        snapshot=update.snapshot,
        delta=update.delta,
        fill_count=len(fills),
        new_fill_keys=frozenset((fill.symbol, fill.trade_id) for fill in fills),
        fill_count_by_symbol=_fill_counts_by_symbol(fills),
    )


def _fill_counts_by_symbol(
    fills: tuple[AccountFillEvent, ...],
) -> tuple[tuple[str, int], ...]:
    counts: dict[str, int] = {}
    for fill in fills:
        counts[fill.symbol] = counts.get(fill.symbol, 0) + 1
    return tuple(sorted(counts.items()))
