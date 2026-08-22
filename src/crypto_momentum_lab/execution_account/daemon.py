import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.execution_account.binance.user_data import (
    BinanceUserDataEvent,
    UserDataEventSink,
)
from crypto_momentum_lab.execution_account.sync import (
    AccountSnapshot,
    ExecutionAccountSyncResult,
)
from crypto_momentum_lab.execution_account.user_data_sync import (
    AccountUserDataState,
)


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


class UserDataAccountEventStream(Protocol):
    def set_handler(self, on_event: UserDataEventSink) -> None:
        pass

    async def run(self) -> None:
        pass

    async def stop(self) -> None:
        pass


@dataclass(frozen=True, slots=True)
class UserDataAccountSyncConfig:
    rest_reconciliation_interval_seconds: float = 300.0
    heartbeat_interval_seconds: float = 30.0
    failure_backoff_initial_seconds: float = 10.0
    failure_backoff_max_seconds: float = 300.0

    def __post_init__(self) -> None:
        if self.rest_reconciliation_interval_seconds <= 0:
            raise ValueError("rest_reconciliation_interval_seconds must be positive")
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
        on_persisted: UserDataAccountPersistedCallback | None = None,
    ) -> None:
        self._service = service
        self._stream = stream
        self._config = config
        self._clock = clock
        self._sleep = sleep
        self._on_error = on_error
        self._on_persisted = on_persisted
        self._state: AccountUserDataState | None = None
        self._accept_events = False
        self._state_lock = asyncio.Lock()

    async def run(self) -> None:
        stream_task: asyncio.Task[None] | None = None
        heartbeat_task: asyncio.Future[None] | None = None
        reconciliation_task: asyncio.Future[None] | None = None
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
                        self._stream.set_handler(self._on_event)
                        stream_task = asyncio.create_task(self._stream.run())
                    except Exception as error:
                        consecutive_failures += 1
                        self._report_error(error)
                        await self._sleep_for_failure(
                            consecutive_failures,
                            _retry_after_seconds(error),
                        )
                        continue

                if stream_task is None or stream_task.done():
                    if stream_task is not None:
                        self._observe_stream_failure(stream_task)
                    stream_task = asyncio.create_task(self._stream.run())

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
                done, _ = await asyncio.wait(
                    {heartbeat_task, reconciliation_task, stream_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if stream_task in done:
                    if heartbeat_task is not None:
                        await _cancel_task(heartbeat_task)
                    if reconciliation_task is not None:
                        await _cancel_task(reconciliation_task)
                    heartbeat_task = None
                    reconciliation_task = None
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
        finally:
            await self._stream.stop()
            if heartbeat_task is not None:
                await _cancel_task(heartbeat_task)
            if reconciliation_task is not None:
                await _cancel_task(reconciliation_task)
            if stream_task is not None and not stream_task.done():
                stream_task.cancel()
                try:
                    await stream_task
                except asyncio.CancelledError:
                    pass

    async def _on_event(self, event: BinanceUserDataEvent) -> None:
        needs_reconciliation = False
        try:
            async with self._state_lock:
                if not self._accept_events or self._state is None:
                    return
                update = self._state.apply(event)
                if update.needs_reconciliation:
                    needs_reconciliation = True
                elif update.changed:
                    result = await self._service.persist_user_data_event(
                        snapshot=update.snapshot,
                        event=event,
                        fills=update.fills,
                    )
                    if self._on_persisted is not None:
                        self._on_persisted(event, result)
        except Exception as error:
            self._report_error(error)
            needs_reconciliation = True
        if needs_reconciliation:
            try:
                result = await self._reconcile(include_fills=True)
                if self._on_persisted is not None and _is_ready_result(result):
                    self._on_persisted(event, result)
            except Exception as error:
                self._report_error(error)

    async def _publish_heartbeat(self) -> None:
        async with self._state_lock:
            if self._accept_events and self._state is not None:
                await self._service.publish_user_data_heartbeat(
                    observed_at=self._now(),
                )

    async def _reconcile(
        self,
        *,
        include_fills: bool,
    ) -> ExecutionAccountSyncResult:
        async with self._state_lock:
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
                self._accept_events = True
            else:
                self._accept_events = False
            return result

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
