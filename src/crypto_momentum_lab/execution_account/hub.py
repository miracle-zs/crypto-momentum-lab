"""Low-latency account-state fan-out for live execution.

The execution-account process owns the Binance user-data WebSocket and the
complete in-memory account state.  A new subscriber receives a full account
snapshot; ordinary notifications then carry versioned account-local deltas.
The Hub retains the latest effective snapshot for gap recovery. PostgreSQL
remains the durable recovery adapter, while the Hub removes account-position
and account-balance lookup from the normal live order path.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import time
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field, replace
from datetime import datetime
from decimal import Decimal, InvalidOperation
from uuid import uuid4

import structlog
from websockets.asyncio.client import ClientConnection, connect
from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.exceptions import ConnectionClosed

from crypto_momentum_lab.domain.account import (
    AccountBalanceSnapshot,
    AccountConfigSnapshot,
    AccountOpenOrderSnapshot,
    AccountPositionSnapshot,
    ExecutionAccountStatus,
)
from crypto_momentum_lab.execution_account.expectations import (
    AccountPositionExpectation,
)
from crypto_momentum_lab.execution_account.sync import (
    AccountSnapshot,
    AccountSnapshotDelta,
    apply_account_snapshot_delta,
)

log = structlog.get_logger()

_SCHEMA_VERSION = 1
_SUBSCRIBE_MESSAGE = "subscribe_account_events"
_READY_MESSAGE = "account_event_hub_ready"
_EVENT_MESSAGE = "account_event"
_REGISTER_EXPECTED_POSITION_MESSAGE = "register_expected_position"
_EXPECTED_POSITION_READY_MESSAGE = "expected_position_registered"
_CLIENT_RECEIVE_QUEUE_SIZE = 16
_MAX_MESSAGE_SIZE = 1024 * 1024
_FILL_KEY_CACHE_SIZE = 8192
_SNAPSHOT_KIND_NOTIFICATION = "notification"
_SNAPSHOT_KIND_FULL = "full"
_SNAPSHOT_KIND_DELTA = "delta"
_SNAPSHOT_KINDS = frozenset(
    {
        _SNAPSHOT_KIND_NOTIFICATION,
        _SNAPSHOT_KIND_FULL,
        _SNAPSHOT_KIND_DELTA,
    }
)

AccountPositionExpectationCallback = Callable[
    [AccountPositionExpectation], None | Awaitable[None]
]


class AccountEventHubError(RuntimeError):
    """Base error for account-event hub protocol and transport failures."""


class AccountEventHubProtocolError(AccountEventHubError):
    """Raised when an account-event hub message is malformed."""


class AccountEventHubSequenceGap(AccountEventHubError):
    """Raised when the consumer cannot apply a contiguous account stream."""


@dataclass(frozen=True, slots=True)
class AccountEvent:
    environment: str
    account_label: str
    event_type: str
    event_id: str
    event_at: datetime
    received_at: datetime
    symbols: tuple[str, ...] = ()
    symbol: str | None = None
    client_order_id: str | None = None
    order_status: str | None = None
    reason: str | None = None
    has_fill: bool = False
    trade_id: str | None = None
    # Transport metadata.  Zero means the event has not yet been assigned a
    # Hub sequence; equality intentionally remains about the account event
    # itself rather than which transport envelope carried it.
    sequence: int = field(default=0, compare=False)
    stream_epoch: str | None = field(default=None, compare=False)
    account_state: ExecutionAccountStatus | None = None
    account_snapshot: AccountSnapshot | None = None
    snapshot_kind: str = _SNAPSHOT_KIND_NOTIFICATION
    account_delta: AccountSnapshotDelta | None = None

    def __post_init__(self) -> None:
        for text_value, field_name in (
            (self.environment, "environment"),
            (self.account_label, "account_label"),
            (self.event_type, "event_type"),
            (self.event_id, "event_id"),
        ):
            if not text_value.strip():
                raise ValueError(f"{field_name} must not be empty")
        for timestamp_value, field_name in (
            (self.event_at, "event_at"),
            (self.received_at, "received_at"),
        ):
            if (
                timestamp_value.tzinfo is None
                or timestamp_value.utcoffset() is None
            ):
                raise ValueError(f"{field_name} must be timezone-aware")
        normalized_symbols = tuple(
            sorted({item.strip().upper() for item in self.symbols})
        )
        if any(not item for item in normalized_symbols):
            raise ValueError("symbols must not contain empty values")
        object.__setattr__(self, "symbols", normalized_symbols)
        for optional_value, field_name in (
            (self.symbol, "symbol"),
            (self.client_order_id, "client_order_id"),
            (self.order_status, "order_status"),
            (self.reason, "reason"),
            (self.trade_id, "trade_id"),
        ):
            if optional_value is not None and not optional_value.strip():
                raise ValueError(f"{field_name} must not be blank when present")
        if not isinstance(self.has_fill, bool):
            raise TypeError("has_fill must be a bool")
        if (
            not isinstance(self.sequence, int)
            or isinstance(self.sequence, bool)
            or self.sequence < 0
        ):
            raise ValueError("sequence must be a non-negative integer")
        if self.stream_epoch is not None and not self.stream_epoch.strip():
            raise ValueError("stream_epoch must not be blank when present")
        if self.snapshot_kind not in _SNAPSHOT_KINDS:
            raise ValueError("snapshot_kind is not supported")
        if (
            self.snapshot_kind == _SNAPSHOT_KIND_NOTIFICATION
            and self.account_snapshot is not None
        ):
            object.__setattr__(self, "snapshot_kind", _SNAPSHOT_KIND_FULL)
        elif (
            self.snapshot_kind == _SNAPSHOT_KIND_NOTIFICATION
            and self.account_delta is not None
        ):
            object.__setattr__(self, "snapshot_kind", _SNAPSHOT_KIND_DELTA)
        if self.account_state is not None and not isinstance(
            self.account_state,
            ExecutionAccountStatus,
        ):
            raise TypeError("account_state must be an ExecutionAccountStatus")
        if self.account_snapshot is not None:
            snapshot_scope = (
                self.account_snapshot.config.environment,
                self.account_snapshot.config.account_label,
            )
            event_scope = (self.environment, self.account_label)
            if snapshot_scope != event_scope:
                raise ValueError(
                    "account snapshot scope does not match account event"
                )
        if self.account_delta is not None:
            delta_scope = _delta_scope(self.account_delta)
            event_scope = (self.environment, self.account_label)
            if delta_scope is not None and delta_scope != event_scope:
                raise ValueError("account delta scope does not match account event")


@dataclass(frozen=True, slots=True)
class _AccountEventQueueOverflow:
    latest_sequence: int


_AccountEventQueueItem = AccountEvent | _AccountEventQueueOverflow | Exception


@dataclass(frozen=True, slots=True)
class AccountEventHubConfig:
    host: str = "0.0.0.0"
    port: int = 8767
    subscriber_queue_size: int = 16
    handshake_timeout_seconds: float = 10.0
    unavailable_timeout_seconds: float = 120.0
    reconnect_delays: tuple[float, ...] = (0.0, 1.0, 5.0, 15.0)
    replay_event_count: int = 4096

    def __post_init__(self) -> None:
        if not self.host.strip():
            raise ValueError("host must not be empty")
        if not 0 <= self.port <= 65535:
            raise ValueError("port must be between 0 and 65535")
        if self.subscriber_queue_size <= 0:
            raise ValueError("subscriber_queue_size must be positive")
        if self.handshake_timeout_seconds <= 0:
            raise ValueError("handshake_timeout_seconds must be positive")
        if self.unavailable_timeout_seconds <= 0:
            raise ValueError("unavailable_timeout_seconds must be positive")
        if not self.reconnect_delays or any(
            delay < 0 for delay in self.reconnect_delays
        ):
            raise ValueError("reconnect_delays must contain non-negative values")
        if self.replay_event_count <= 0:
            raise ValueError("replay_event_count must be positive")


@dataclass(slots=True)
class _Subscriber:
    connection: ServerConnection
    environment: str
    account_label: str
    queue: asyncio.Queue[str]
    writer_start: asyncio.Event
    writer_task: asyncio.Task[None]


@dataclass(frozen=True, slots=True)
class _ReplayEntry:
    sequence: int
    message: str
    snapshot_kind: str


class AccountEventHub:
    """Bounded latest-event fan-out owned by execution-account."""

    def __init__(
        self,
        config: AccountEventHubConfig | None = None,
        *,
        on_position_expectation: AccountPositionExpectationCallback | None = None,
    ) -> None:
        self._config = config or AccountEventHubConfig()
        self._on_position_expectation = on_position_expectation
        self._server: Server | None = None
        self._bound_host: str | None = None
        self._bound_port: int | None = None
        self._subscribers: dict[int, _Subscriber] = {}
        self._subscriber_lock = asyncio.Lock()
        self._sequences: dict[tuple[str, str], int] = {}
        self._latest_messages: dict[tuple[str, str], str] = {}
        self._latest_events: dict[tuple[str, str], AccountEvent] = {}
        self._latest_snapshots: dict[tuple[str, str], AccountSnapshot] = {}
        self._seen_fill_keys: set[tuple[str, str]] = set()
        self._seen_fill_key_order: deque[tuple[str, str]] = deque(
            maxlen=_FILL_KEY_CACHE_SIZE
        )
        self._replay_buffers: dict[
            tuple[str, str], deque[_ReplayEntry]
        ] = {}
        self._stream_epoch = str(uuid4())
        self._started_once = False

    @property
    def url(self) -> str:
        if self._bound_host is None or self._bound_port is None:
            raise RuntimeError("account-event hub is not started")
        host = "127.0.0.1" if self._bound_host in {"0.0.0.0", ""} else self._bound_host
        return f"ws://{host}:{self._bound_port}"

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)

    async def start(self) -> None:
        if self._server is not None:
            return
        if self._started_once:
            self._stream_epoch = str(uuid4())
            self._sequences.clear()
            self._latest_messages.clear()
            self._latest_events.clear()
            self._latest_snapshots.clear()
            self._replay_buffers.clear()
            self._seen_fill_keys.clear()
            self._seen_fill_key_order.clear()
        self._server = await serve(
            self._handle_connection,
            self._config.host,
            self._config.port,
            max_size=_MAX_MESSAGE_SIZE,
            max_queue=16,
        )
        socket = next(iter(self._server.sockets), None)
        if socket is None:
            await self._server.wait_closed()
            self._server = None
            raise RuntimeError("account-event hub did not bind a socket")
        self._bound_host = self._config.host
        self._bound_port = int(socket.getsockname()[1])
        self._started_once = True
        log.info(
            "account_event_hub_started",
            host=self._bound_host,
            port=self._bound_port,
        )

    async def stop(self) -> None:
        server = self._server
        self._server = None
        if server is not None:
            server.close()
            await server.wait_closed()
        async with self._subscriber_lock:
            subscribers = tuple(self._subscribers.values())
            self._subscribers.clear()
        for subscriber in subscribers:
            subscriber.writer_start.set()
            await subscriber.connection.close()
        if subscribers:
            for subscriber in subscribers:
                if not subscriber.writer_task.done():
                    subscriber.writer_task.cancel()
            await asyncio.gather(
                *(subscriber.writer_task for subscriber in subscribers),
                return_exceptions=True,
            )
        self._bound_host = None
        self._bound_port = None

    def publish(self, event: AccountEvent) -> None:
        """Publish without waiting on a consumer or database operation."""
        if event.has_fill and event.symbol is not None and event.trade_id is not None:
            fill_key = (event.symbol, event.trade_id)
            if (
                event.event_type == "ACCOUNT_FILL_RECONCILED"
                and fill_key in self._seen_fill_keys
            ):
                return
            duplicate_fill = fill_key in self._seen_fill_keys
            self._remember_fill_key(fill_key)
            if duplicate_fill:
                # Keep the order/status notification for consumers, but do not
                # count a REST-replayed fill twice in live latency telemetry.
                event = replace(event, has_fill=False)
        scope = (event.environment, event.account_label)
        current_snapshot = self._next_snapshot(scope, event)
        sequence = self._sequences.get(scope, 0) + 1
        self._sequences[scope] = sequence
        wire_event = replace(
            event,
            sequence=sequence,
            stream_epoch=self._stream_epoch,
        )
        message = encode_account_event(wire_event, sequence=sequence)
        self._latest_messages[scope] = message
        self._latest_events[scope] = wire_event
        if current_snapshot is not None:
            self._latest_snapshots[scope] = current_snapshot
        replay_buffer = self._replay_buffers.setdefault(
            scope,
            deque(maxlen=self._config.replay_event_count),
        )
        if wire_event.snapshot_kind == _SNAPSHOT_KIND_FULL:
            replay_buffer.clear()
        replay_buffer.append(
            _ReplayEntry(
                sequence=sequence,
                message=message,
                snapshot_kind=wire_event.snapshot_kind,
            )
        )
        for subscriber in tuple(self._subscribers.values()):
            if (
                subscriber.environment == event.environment
                and subscriber.account_label == event.account_label
            ):
                self._enqueue_latest(subscriber, message)

    def _remember_fill_key(self, key: tuple[str, str]) -> None:
        if key in self._seen_fill_keys:
            return
        if len(self._seen_fill_key_order) == self._seen_fill_key_order.maxlen:
            oldest = self._seen_fill_key_order.popleft()
            self._seen_fill_keys.discard(oldest)
        self._seen_fill_key_order.append(key)
        self._seen_fill_keys.add(key)

    def _next_snapshot(
        self,
        scope: tuple[str, str],
        event: AccountEvent,
    ) -> AccountSnapshot | None:
        if event.snapshot_kind == _SNAPSHOT_KIND_FULL:
            if event.account_snapshot is None:
                raise ValueError("full account event requires account_snapshot")
            return event.account_snapshot
        if event.snapshot_kind == _SNAPSHOT_KIND_DELTA:
            if event.account_delta is None:
                raise ValueError("delta account event requires account_delta")
            previous = self._latest_snapshots.get(scope)
            if previous is None:
                # A producer may already hold the effective state while the
                # Hub is starting.  Use it to seed the Hub, but keep the wire
                # envelope as a delta for subscribers that already have a
                # bootstrap.
                if event.account_snapshot is None:
                    raise ValueError(
                        "first delta account event requires a Hub snapshot"
                    )
                return event.account_snapshot
            return apply_account_snapshot_delta(previous, event.account_delta)
        return event.account_snapshot

    def _bootstrap_message(self, scope: tuple[str, str]) -> str | None:
        snapshot = self._latest_snapshots.get(scope)
        latest_event = self._latest_events.get(scope)
        latest_sequence = self._sequences.get(scope, 0)
        if snapshot is None or latest_event is None or latest_sequence <= 0:
            return self._latest_messages.get(scope)
        observed_at = snapshot.config.observed_at
        bootstrap = replace(
            latest_event,
            event_type="ACCOUNT_SNAPSHOT",
            event_id=f"hub-bootstrap:{uuid4()}",
            event_at=observed_at,
            received_at=observed_at,
            symbols=tuple(
                sorted(
                    {
                        *(position.symbol for position in snapshot.positions),
                        *(order.symbol for order in snapshot.open_orders),
                    }
                )
            ),
            sequence=latest_sequence,
            stream_epoch=self._stream_epoch,
            account_snapshot=snapshot,
            snapshot_kind=_SNAPSHOT_KIND_FULL,
            account_delta=None,
        )
        return encode_account_event(bootstrap, sequence=latest_sequence)

    def _subscription_messages(
        self,
        scope: tuple[str, str],
        *,
        requested_epoch: str | None,
        last_sequence: int | None,
        require_full_snapshot: bool,
    ) -> tuple[list[str], bool, bool]:
        latest_sequence = self._sequences.get(scope, 0)
        stream_reset = (
            requested_epoch is not None
            and requested_epoch != self._stream_epoch
        )
        if require_full_snapshot or last_sequence is None or stream_reset:
            bootstrap = self._bootstrap_message(scope)
            return (
                ([] if bootstrap is None else [bootstrap]),
                bootstrap is not None and _message_has_full_snapshot(bootstrap),
                stream_reset,
            )
        if last_sequence == latest_sequence:
            return [], False, False
        if last_sequence < 0 or last_sequence > latest_sequence:
            bootstrap = self._bootstrap_message(scope)
            return (
                ([] if bootstrap is None else [bootstrap]),
                bootstrap is not None and _message_has_full_snapshot(bootstrap),
                False,
            )
        replay_buffer = self._replay_buffers.get(scope)
        if not replay_buffer:
            bootstrap = self._bootstrap_message(scope)
            return (
                ([] if bootstrap is None else [bootstrap]),
                bootstrap is not None and _message_has_full_snapshot(bootstrap),
                False,
            )
        oldest_sequence = replay_buffer[0].sequence
        if last_sequence < oldest_sequence - 1:
            bootstrap = self._bootstrap_message(scope)
            return (
                ([] if bootstrap is None else [bootstrap]),
                bootstrap is not None and _message_has_full_snapshot(bootstrap),
                False,
            )
        replay = [
            entry.message for entry in replay_buffer if entry.sequence > last_sequence
        ]
        if not replay or _first_sequence(replay) != last_sequence + 1:
            bootstrap = self._bootstrap_message(scope)
            return (
                ([] if bootstrap is None else [bootstrap]),
                bootstrap is not None and _message_has_full_snapshot(bootstrap),
                False,
            )
        return replay, False, False

    def _enqueue_latest(self, subscriber: _Subscriber, message: str) -> None:
        if subscriber.queue.full():
            while True:
                try:
                    subscriber.queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
        try:
            subscriber.queue.put_nowait(message)
        except asyncio.QueueFull:
            pass

    async def _handle_connection(self, connection: ServerConnection) -> None:
        subscriber: _Subscriber | None = None
        try:
            raw_message = await asyncio.wait_for(
                connection.recv(),
                timeout=self._config.handshake_timeout_seconds,
            )
            request = _decode_object(raw_message)
            if request.get("type") == _REGISTER_EXPECTED_POSITION_MESSAGE:
                await self._handle_position_expectation(connection, request)
                return
            if request.get("type") != _SUBSCRIBE_MESSAGE:
                raise AccountEventHubProtocolError("invalid subscription message")
            environment = _require_string(request, "environment")
            account_label = _require_string(request, "account_label")
            consumer_id = _require_string(request, "consumer_id")
            requested_epoch = _optional_string(request, "stream_epoch")
            last_sequence = _optional_nullable_non_negative_int(
                request,
                "last_sequence",
            )
            require_full_snapshot = _optional_bool(
                request,
                "require_full_snapshot",
            )
            scope = (environment, account_label)
            queue: asyncio.Queue[str] = asyncio.Queue(
                maxsize=self._config.subscriber_queue_size
            )
            writer_start = asyncio.Event()
            subscriber = _Subscriber(
                connection=connection,
                environment=environment,
                account_label=account_label,
                queue=queue,
                writer_start=writer_start,
                writer_task=asyncio.create_task(
                    self._write_messages(connection, queue, writer_start)
                ),
            )
            async with self._subscriber_lock:
                self._subscribers[id(connection)] = subscriber
                messages, full_snapshot, stream_reset = self._subscription_messages(
                    scope,
                    requested_epoch=requested_epoch,
                    last_sequence=last_sequence,
                    require_full_snapshot=require_full_snapshot,
                )
                if len(messages) > self._config.subscriber_queue_size:
                    bootstrap = self._bootstrap_message(scope)
                    messages = [] if bootstrap is None else [bootstrap]
                    full_snapshot = (
                        bootstrap is not None
                        and _message_has_full_snapshot(bootstrap)
                    )
                for message in messages:
                    self._enqueue_latest(subscriber, message)
                latest_sequence = self._sequences.get(scope, 0)
                replay_buffer = self._replay_buffers.get(scope)
                oldest_sequence = (
                    replay_buffer[0].sequence if replay_buffer else None
                )
            await connection.send(
                json.dumps(
                    {
                        "type": _READY_MESSAGE,
                        "schema_version": _SCHEMA_VERSION,
                        "environment": environment,
                        "account_label": account_label,
                        "stream_epoch": self._stream_epoch,
                        "stream_reset": stream_reset,
                        "replay_available": True,
                        "full_snapshot": full_snapshot,
                        "oldest_sequence": oldest_sequence,
                        "latest_sequence": latest_sequence,
                    },
                    separators=(",", ":"),
                )
            )
            writer_start.set()
            log.info(
                "account_event_hub_subscriber_connected",
                consumer_id=consumer_id,
                environment=environment,
                account_label=account_label,
            )
            await connection.wait_closed()
        except (ConnectionClosed, TimeoutError):
            return
        except AccountEventHubProtocolError as error:
            await connection.close(code=1008, reason=str(error))
        except Exception as error:
            log.exception("account_event_hub_connection_failed", error=str(error))
        finally:
            if subscriber is not None:
                async with self._subscriber_lock:
                    self._subscribers.pop(id(connection), None)
                if not subscriber.writer_task.done():
                    subscriber.writer_task.cancel()
                await asyncio.gather(
                    subscriber.writer_task,
                    return_exceptions=True,
                )
            log.info(
                "account_event_hub_subscriber_disconnected"
                if subscriber is not None
                else "account_event_hub_control_disconnected"
            )

    async def _handle_position_expectation(
        self,
        connection: ServerConnection,
        request: dict[str, object],
    ) -> None:
        callback = self._on_position_expectation
        if callback is None:
            raise AccountEventHubProtocolError(
                "account position expectation registration is not enabled"
            )
        expectation = decode_account_position_expectation(request)
        callback_result = callback(expectation)
        if inspect.isawaitable(callback_result):
            await callback_result
        await connection.send(
            json.dumps(
                {
                    "type": _EXPECTED_POSITION_READY_MESSAGE,
                    "schema_version": _SCHEMA_VERSION,
                    "environment": expectation.environment,
                    "account_label": expectation.account_label,
                    "client_order_id": expectation.client_order_id,
                },
                separators=(",", ":"),
            )
        )

    async def _write_messages(
        self,
        connection: ServerConnection,
        queue: asyncio.Queue[str],
        writer_start: asyncio.Event,
    ) -> None:
        try:
            await writer_start.wait()
            while True:
                await connection.send(await queue.get())
        except (ConnectionClosed, asyncio.CancelledError):
            raise


class WebSocketAccountEventSource:
    """Async iterator adapter for the live account-event channel."""

    def __init__(
        self,
        *,
        url: str,
        environment: str,
        account_label: str,
        consumer_id: str,
        config: AccountEventHubConfig | None = None,
        on_recovery: Callable[[str], None] | None = None,
    ) -> None:
        for value, field_name in (
            (url, "url"),
            (environment, "environment"),
            (account_label, "account_label"),
            (consumer_id, "consumer_id"),
        ):
            if not value.strip():
                raise ValueError(f"{field_name} must not be empty")
        self._url = url
        self._environment = environment
        self._account_label = account_label
        self._consumer_id = consumer_id
        self._config = config or AccountEventHubConfig()
        self._on_recovery = on_recovery
        self._stopping = False
        self._stream_epoch: str | None = None
        self._last_sequence: int | None = None
        self._account_snapshot: AccountSnapshot | None = None
        self._require_full_snapshot = True

    def stop(self) -> None:
        self._stopping = True

    def __aiter__(self) -> AsyncIterator[AccountEvent]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[AccountEvent]:
        unavailable_since = time.monotonic()
        reconnect_attempt = 0
        while not self._stopping:
            try:
                async with connect(
                    self._url,
                    open_timeout=self._config.handshake_timeout_seconds,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=_MAX_MESSAGE_SIZE,
                    max_queue=16,
                    proxy=None,
                ) as connection:
                    require_full_snapshot = self._require_full_snapshot
                    subscription = {
                        "type": _SUBSCRIBE_MESSAGE,
                        "schema_version": _SCHEMA_VERSION,
                        "environment": self._environment,
                        "account_label": self._account_label,
                        "consumer_id": self._consumer_id,
                        "require_full_snapshot": require_full_snapshot,
                    }
                    if self._stream_epoch is not None:
                        subscription["stream_epoch"] = self._stream_epoch
                    if not require_full_snapshot and self._last_sequence is not None:
                        subscription["last_sequence"] = self._last_sequence
                    await connection.send(
                        json.dumps(subscription, separators=(",", ":"))
                    )
                    ready = _decode_object(await connection.recv())
                    if ready.get("type") != _READY_MESSAGE:
                        raise AccountEventHubProtocolError(
                            "account-event hub did not acknowledge subscription"
                        )
                    if (
                        ready.get("environment") != self._environment
                        or ready.get("account_label") != self._account_label
                    ):
                        raise AccountEventHubProtocolError(
                            "account-event hub subscription mismatch"
                        )
                    ready_epoch = _optional_string(ready, "stream_epoch")
                    if ready_epoch is not None:
                        if (
                            self._stream_epoch is not None
                            and self._stream_epoch != ready_epoch
                        ):
                            self._prepare_full_snapshot_recovery(
                                "account_event_stream_epoch_changed"
                            )
                        self._stream_epoch = ready_epoch
                    if ready.get("stream_reset") is True:
                        self._prepare_full_snapshot_recovery(
                            "account_event_stream_reset"
                        )
                    if ready.get("full_snapshot") is True:
                        if self._account_snapshot is None:
                            self._last_sequence = None
                        else:
                            self._prepare_full_snapshot_recovery(
                                "account_event_full_snapshot_recovery"
                            )
                    unavailable_since = time.monotonic()
                    reconnect_attempt = 0
                    receive_queue: asyncio.Queue[_AccountEventQueueItem] = (
                        asyncio.Queue(maxsize=_CLIENT_RECEIVE_QUEUE_SIZE)
                    )
                    reader_task = asyncio.create_task(
                        self._read_account_events(
                            connection,
                            receive_queue,
                        ),
                        name=f"account-event-reader:{self._consumer_id}",
                    )
                    try:
                        while not self._stopping:
                            item = await receive_queue.get()
                            if isinstance(item, Exception):
                                raise item
                            if isinstance(item, _AccountEventQueueOverflow):
                                self._prepare_full_snapshot_recovery(
                                    "account_event_queue_overflow"
                                )
                                raise AccountEventHubSequenceGap(
                                    "account-event hub client queue overflow"
                                )
                            materialized = self._materialize_event(item)
                            if materialized is not None:
                                yield materialized
                    finally:
                        if not reader_task.done():
                            reader_task.cancel()
                        await asyncio.gather(
                            reader_task,
                            return_exceptions=True,
                        )
            except (
                ConnectionClosed,
                OSError,
                TimeoutError,
                AccountEventHubError,
            ) as error:
                if time.monotonic() - unavailable_since >= (
                    self._config.unavailable_timeout_seconds
                ):
                    raise AccountEventHubError(
                        "account-event hub unavailable beyond timeout"
                    ) from error
                delay = self._config.reconnect_delays[
                    min(reconnect_attempt, len(self._config.reconnect_delays) - 1)
                ]
                reconnect_attempt += 1
                if delay > 0:
                    await asyncio.sleep(delay)
        return

    def _prepare_full_snapshot_recovery(self, reason: str) -> None:
        self._account_snapshot = None
        self._last_sequence = None
        self._require_full_snapshot = True
        if self._on_recovery is not None:
            try:
                self._on_recovery(reason)
            except Exception:
                log.exception(
                    "account_event_hub_recovery_callback_failed",
                    reason=reason,
                    consumer_id=self._consumer_id,
                )

    def _materialize_event(self, event: AccountEvent) -> AccountEvent | None:
        if event.stream_epoch is not None:
            if (
                self._stream_epoch is not None
                and event.stream_epoch != self._stream_epoch
            ):
                self._prepare_full_snapshot_recovery(
                    "account_event_stream_epoch_changed"
                )
                raise AccountEventHubSequenceGap(
                    "account-event hub stream epoch changed mid-connection"
                )
            self._stream_epoch = event.stream_epoch
        if event.sequence > 0:
            if self._last_sequence is not None:
                if event.sequence <= self._last_sequence:
                    return None
                if (
                    event.sequence != self._last_sequence + 1
                    and event.snapshot_kind != _SNAPSHOT_KIND_FULL
                ):
                    self._prepare_full_snapshot_recovery(
                        "account_event_sequence_gap"
                    )
                    raise AccountEventHubSequenceGap(
                        "account-event hub sequence is not contiguous"
                    )
                if event.sequence != self._last_sequence + 1:
                    self._account_snapshot = None
                    self._last_sequence = None
            if event.snapshot_kind == _SNAPSHOT_KIND_FULL:
                if event.account_snapshot is None:
                    raise AccountEventHubProtocolError(
                        "full account event is missing account_snapshot"
                    )
                self._account_snapshot = event.account_snapshot
                self._require_full_snapshot = False
            elif event.snapshot_kind == _SNAPSHOT_KIND_DELTA:
                if self._account_snapshot is None or event.account_delta is None:
                    self._prepare_full_snapshot_recovery(
                        "account_event_delta_without_snapshot"
                    )
                    raise AccountEventHubSequenceGap(
                        "account-event delta has no local full snapshot"
                    )
                self._account_snapshot = apply_account_snapshot_delta(
                    self._account_snapshot,
                    event.account_delta,
                )
            self._last_sequence = event.sequence
        if self._account_snapshot is None:
            return event
        return replace(event, account_snapshot=self._account_snapshot)

    async def _read_account_events(
        self,
        connection: ClientConnection,
        receive_queue: asyncio.Queue[_AccountEventQueueItem],
    ) -> None:
        try:
            while True:
                event = decode_account_event(
                    await connection.recv(),
                    expected_environment=self._environment,
                    expected_account_label=self._account_label,
                )
                self._enqueue_account_event(receive_queue, event)
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._enqueue_account_event_reader_error(receive_queue, error)

    def _enqueue_account_event(
        self,
        receive_queue: asyncio.Queue[_AccountEventQueueItem],
        event: AccountEvent,
    ) -> None:
        if receive_queue.full():
            while True:
                try:
                    receive_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
            log.warning(
                "account_event_hub_client_queue_overflow",
                consumer_id=self._consumer_id,
                environment=self._environment,
                account_label=self._account_label,
                event_id=event.event_id,
            )
            receive_queue.put_nowait(
                _AccountEventQueueOverflow(latest_sequence=event.sequence)
            )
            return
        receive_queue.put_nowait(event)

    @staticmethod
    def _enqueue_account_event_reader_error(
        receive_queue: asyncio.Queue[_AccountEventQueueItem],
        error: Exception,
    ) -> None:
        while receive_queue.full():
            receive_queue.get_nowait()
        receive_queue.put_nowait(error)


class WebSocketAccountPositionExpectationPublisher:
    """Register an entry expectation over the Hub's low-volume control path."""

    def __init__(
        self,
        *,
        url: str,
        environment: str,
        account_label: str,
        config: AccountEventHubConfig | None = None,
    ) -> None:
        for value, field_name in (
            (url, "url"),
            (environment, "environment"),
            (account_label, "account_label"),
        ):
            if not value.strip():
                raise ValueError(f"{field_name} must not be empty")
        self._url = url
        self._environment = environment
        self._account_label = account_label
        self._config = config or AccountEventHubConfig()

    async def register(self, expectation: AccountPositionExpectation) -> None:
        if (
            expectation.environment != self._environment
            or expectation.account_label != self._account_label
        ):
            raise ValueError(
                "account position expectation scope does not match publisher"
            )
        async with connect(
            self._url,
            open_timeout=self._config.handshake_timeout_seconds,
            ping_interval=20,
            ping_timeout=20,
            max_size=_MAX_MESSAGE_SIZE,
            max_queue=16,
            proxy=None,
        ) as connection:
            await connection.send(encode_account_position_expectation(expectation))
            response = _decode_object(await connection.recv())
            if response.get("type") != _EXPECTED_POSITION_READY_MESSAGE:
                raise AccountEventHubProtocolError(
                    "account-event hub did not acknowledge position expectation"
                )
            if response.get("schema_version") != _SCHEMA_VERSION:
                raise AccountEventHubProtocolError(
                    "unsupported account-event hub expectation acknowledgement"
                )
            if (
                _require_string(response, "environment") != self._environment
                or _require_string(response, "account_label")
                != self._account_label
                or _require_string(response, "client_order_id")
                != expectation.client_order_id
            ):
                raise AccountEventHubProtocolError(
                    "account-event hub expectation acknowledgement mismatch"
                )


def encode_account_position_expectation(
    expectation: AccountPositionExpectation,
) -> str:
    return json.dumps(
        {
            "type": _REGISTER_EXPECTED_POSITION_MESSAGE,
            "schema_version": _SCHEMA_VERSION,
            "environment": expectation.environment,
            "account_label": expectation.account_label,
            "symbol": expectation.symbol,
            "position_side": expectation.position_side,
            "client_order_id": expectation.client_order_id,
            "side": expectation.side,
            "quantity": str(expectation.quantity),
            "created_at": expectation.created_at.isoformat(),
            "expires_at": expectation.expires_at.isoformat(),
        },
        separators=(",", ":"),
    )


def decode_account_position_expectation(
    message: str | bytes | object,
) -> AccountPositionExpectation:
    payload = _decode_object(message)
    if payload.get("type") != _REGISTER_EXPECTED_POSITION_MESSAGE:
        raise AccountEventHubProtocolError(
            "invalid account position expectation message type"
        )
    if payload.get("schema_version") != _SCHEMA_VERSION:
        raise AccountEventHubProtocolError(
            "unsupported account position expectation schema"
        )
    try:
        return AccountPositionExpectation(
            environment=_require_string(payload, "environment"),
            account_label=_require_string(payload, "account_label"),
            symbol=_require_string(payload, "symbol"),
            position_side=_require_string(payload, "position_side"),
            client_order_id=_require_string(payload, "client_order_id"),
            side=_require_string(payload, "side"),
            quantity=_required_decimal(payload, "quantity"),
            created_at=_parse_datetime(payload, "created_at"),
            expires_at=_parse_datetime(payload, "expires_at"),
        )
    except (TypeError, ValueError) as error:
        raise AccountEventHubProtocolError(
            "invalid account position expectation payload"
        ) from error


def encode_account_event(event: AccountEvent, *, sequence: int) -> str:
    if sequence <= 0:
        raise ValueError("sequence must be positive")
    return json.dumps(
        {
            "type": _EVENT_MESSAGE,
            "schema_version": _SCHEMA_VERSION,
            "sequence": sequence,
            "stream_epoch": event.stream_epoch,
            "environment": event.environment,
            "account_label": event.account_label,
            "event_type": event.event_type,
            "event_id": event.event_id,
            "event_at": event.event_at.isoformat(),
            "received_at": event.received_at.isoformat(),
            "symbols": list(event.symbols),
            "symbol": event.symbol,
            "client_order_id": event.client_order_id,
            "order_status": event.order_status,
            "reason": event.reason,
            "has_fill": event.has_fill,
            "trade_id": event.trade_id,
            "account_state": (
                None
                if event.account_state is None
                else event.account_state.value
            ),
            "snapshot_kind": event.snapshot_kind,
            "account_snapshot": (
                None
                if event.snapshot_kind != _SNAPSHOT_KIND_FULL
                or event.account_snapshot is None
                else _encode_account_snapshot(event.account_snapshot)
            ),
            "account_delta": (
                None
                if event.snapshot_kind != _SNAPSHOT_KIND_DELTA
                or event.account_delta is None
                else _encode_account_snapshot_delta(event.account_delta)
            ),
        },
        separators=(",", ":"),
    )


def decode_account_event(
    message: str | bytes | object,
    *,
    expected_environment: str,
    expected_account_label: str,
) -> AccountEvent:
    payload = _decode_object(message)
    if payload.get("type") != _EVENT_MESSAGE:
        raise AccountEventHubProtocolError("invalid account-event message type")
    if payload.get("schema_version") != _SCHEMA_VERSION:
        raise AccountEventHubProtocolError("unsupported account-event schema")
    environment = _require_string(payload, "environment")
    account_label = _require_string(payload, "account_label")
    if environment != expected_environment or account_label != expected_account_label:
        raise AccountEventHubProtocolError("account-event scope mismatch")
    symbols = payload.get("symbols", [])
    if not isinstance(symbols, list) or any(
        not isinstance(item, str) for item in symbols
    ):
        raise AccountEventHubProtocolError("symbols must be a string array")
    sequence = _optional_non_negative_int(payload, "sequence")
    stream_epoch = _optional_string(payload, "stream_epoch")
    account_state = _optional_account_state(payload)
    snapshot_kind = _optional_snapshot_kind(payload)
    snapshot_payload = payload.get("account_snapshot")
    account_snapshot = (
        None
        if snapshot_payload is None
        else _decode_account_snapshot(
            snapshot_payload,
            expected_environment=environment,
            expected_account_label=account_label,
        )
    )
    delta_payload = payload.get("account_delta")
    account_delta = (
        None
        if delta_payload is None
        else _decode_account_snapshot_delta(
            delta_payload,
            expected_environment=environment,
            expected_account_label=account_label,
        )
    )
    return AccountEvent(
        environment=environment,
        account_label=account_label,
        event_type=_require_string(payload, "event_type"),
        event_id=_require_string(payload, "event_id"),
        event_at=_parse_datetime(payload, "event_at"),
        received_at=_parse_datetime(payload, "received_at"),
        symbols=tuple(symbols),
        symbol=_optional_string(payload, "symbol"),
        client_order_id=_optional_string(payload, "client_order_id"),
        order_status=_optional_string(payload, "order_status"),
        reason=_optional_string(payload, "reason"),
        has_fill=_optional_bool(payload, "has_fill"),
        trade_id=_optional_string(payload, "trade_id"),
        sequence=sequence,
        stream_epoch=stream_epoch,
        account_state=account_state,
        account_snapshot=account_snapshot,
        snapshot_kind=snapshot_kind,
        account_delta=account_delta,
    )


def _encode_account_snapshot(snapshot: AccountSnapshot) -> dict[str, object]:
    """Encode current account state without forwarding exchange raw payloads."""
    return {
        "config": {
            "environment": snapshot.config.environment,
            "account_label": snapshot.config.account_label,
            "multi_assets_mode": snapshot.config.multi_assets_mode,
            "hedge_mode": snapshot.config.hedge_mode,
            "can_trade": snapshot.config.can_trade,
            "fee_tier": snapshot.config.fee_tier,
            "observed_at": snapshot.config.observed_at.isoformat(),
        },
        "balances": [
            {
                "environment": balance.environment,
                "account_label": balance.account_label,
                "asset": balance.asset,
                "wallet_balance": str(balance.wallet_balance),
                "available_balance": str(balance.available_balance),
                "unrealized_pnl": str(balance.unrealized_pnl),
                "observed_at": balance.observed_at.isoformat(),
            }
            for balance in snapshot.balances
        ],
        "positions": [
            {
                "environment": position.environment,
                "account_label": position.account_label,
                "symbol": position.symbol,
                "position_side": position.position_side,
                "position_amt": str(position.position_amt),
                "entry_price": str(position.entry_price),
                "mark_price": str(position.mark_price),
                "unrealized_pnl": str(position.unrealized_pnl),
                "notional": str(position.notional),
                "leverage": position.leverage,
                "margin_type": position.margin_type,
                "observed_at": position.observed_at.isoformat(),
            }
            for position in snapshot.positions
        ],
        "open_orders": [
            {
                "environment": order.environment,
                "account_label": order.account_label,
                "symbol": order.symbol,
                "order_id": order.order_id,
                "client_order_id": order.client_order_id,
                "side": order.side,
                "order_type": order.order_type,
                "status": order.status,
                "price": str(order.price),
                "original_quantity": str(order.original_quantity),
                "executed_quantity": str(order.executed_quantity),
                "reduce_only": order.reduce_only,
                "observed_at": order.observed_at.isoformat(),
            }
            for order in snapshot.open_orders
        ],
    }


def _encode_account_snapshot_delta(delta: AccountSnapshotDelta) -> dict[str, object]:
    """Encode only material account changes; raw exchange payloads stay local."""
    return {
        "observed_at": delta.observed_at.isoformat(),
        "config": (
            None
            if delta.config is None
            else {
                "environment": delta.config.environment,
                "account_label": delta.config.account_label,
                "multi_assets_mode": delta.config.multi_assets_mode,
                "hedge_mode": delta.config.hedge_mode,
                "can_trade": delta.config.can_trade,
                "fee_tier": delta.config.fee_tier,
                "observed_at": delta.config.observed_at.isoformat(),
            }
        ),
        "balances": [_encode_balance_snapshot(item) for item in delta.balances],
        "removed_balance_assets": list(delta.removed_balance_assets),
        "positions": [_encode_position_snapshot(item) for item in delta.positions],
        "removed_positions": [list(key) for key in delta.removed_positions],
        "open_orders": [
            _encode_open_order_snapshot(item) for item in delta.open_orders
        ],
        "removed_open_orders": [
            list(key) for key in delta.removed_open_orders
        ],
    }


def _encode_balance_snapshot(balance: AccountBalanceSnapshot) -> dict[str, object]:
    return {
        "environment": balance.environment,
        "account_label": balance.account_label,
        "asset": balance.asset,
        "wallet_balance": str(balance.wallet_balance),
        "available_balance": str(balance.available_balance),
        "unrealized_pnl": str(balance.unrealized_pnl),
        "observed_at": balance.observed_at.isoformat(),
    }


def _encode_position_snapshot(position: AccountPositionSnapshot) -> dict[str, object]:
    return {
        "environment": position.environment,
        "account_label": position.account_label,
        "symbol": position.symbol,
        "position_side": position.position_side,
        "position_amt": str(position.position_amt),
        "entry_price": str(position.entry_price),
        "mark_price": str(position.mark_price),
        "unrealized_pnl": str(position.unrealized_pnl),
        "notional": str(position.notional),
        "leverage": position.leverage,
        "margin_type": position.margin_type,
        "observed_at": position.observed_at.isoformat(),
    }


def _encode_open_order_snapshot(order: AccountOpenOrderSnapshot) -> dict[str, object]:
    return {
        "environment": order.environment,
        "account_label": order.account_label,
        "symbol": order.symbol,
        "order_id": order.order_id,
        "client_order_id": order.client_order_id,
        "side": order.side,
        "order_type": order.order_type,
        "status": order.status,
        "price": str(order.price),
        "original_quantity": str(order.original_quantity),
        "executed_quantity": str(order.executed_quantity),
        "reduce_only": order.reduce_only,
        "observed_at": order.observed_at.isoformat(),
    }


def _decode_account_snapshot(
    value: object,
    *,
    expected_environment: str,
    expected_account_label: str,
) -> AccountSnapshot:
    payload = _require_mapping(value, "account_snapshot")
    config_payload = _mapping_field(payload, "config")
    _require_snapshot_scope(
        config_payload,
        expected_environment=expected_environment,
        expected_account_label=expected_account_label,
    )
    config = AccountConfigSnapshot(
        environment=expected_environment,
        account_label=expected_account_label,
        multi_assets_mode=_required_bool(config_payload, "multi_assets_mode"),
        hedge_mode=_required_bool(config_payload, "hedge_mode"),
        can_trade=_required_bool(config_payload, "can_trade"),
        fee_tier=_optional_int_value(config_payload, "fee_tier"),
        observed_at=_parse_datetime(config_payload, "observed_at"),
        raw_payload={},
    )
    balances = tuple(
        _decode_balance_snapshot(
            item,
            expected_environment=expected_environment,
            expected_account_label=expected_account_label,
        )
        for item in _required_list(payload, "balances")
    )
    positions = tuple(
        _decode_position_snapshot(
            item,
            expected_environment=expected_environment,
            expected_account_label=expected_account_label,
        )
        for item in _required_list(payload, "positions")
    )
    open_orders = tuple(
        _decode_open_order_snapshot(
            item,
            expected_environment=expected_environment,
            expected_account_label=expected_account_label,
        )
        for item in _required_list(payload, "open_orders")
    )
    return AccountSnapshot(
        config=config,
        balances=balances,
        positions=positions,
        open_orders=open_orders,
    )


def _decode_account_snapshot_delta(
    value: object,
    *,
    expected_environment: str,
    expected_account_label: str,
) -> AccountSnapshotDelta:
    payload = _require_mapping(value, "account_delta")
    config_value = payload.get("config")
    config = (
        None
        if config_value is None
        else _decode_account_config_snapshot(
            config_value,
            expected_environment=expected_environment,
            expected_account_label=expected_account_label,
            field_name="account_delta.config",
        )
    )
    return AccountSnapshotDelta(
        observed_at=_parse_datetime(payload, "observed_at"),
        config=config,
        balances=tuple(
            _decode_balance_snapshot(
                item,
                expected_environment=expected_environment,
                expected_account_label=expected_account_label,
            )
            for item in _required_list(payload, "balances")
        ),
        removed_balance_assets=tuple(
            _required_string_list(payload, "removed_balance_assets")
        ),
        positions=tuple(
            _decode_position_snapshot(
                item,
                expected_environment=expected_environment,
                expected_account_label=expected_account_label,
            )
            for item in _required_list(payload, "positions")
        ),
        removed_positions=tuple(
            _required_key_list(payload, "removed_positions")
        ),
        open_orders=tuple(
            _decode_open_order_snapshot(
                item,
                expected_environment=expected_environment,
                expected_account_label=expected_account_label,
            )
            for item in _required_list(payload, "open_orders")
        ),
        removed_open_orders=tuple(
            _required_key_list(payload, "removed_open_orders")
        ),
    )


def _decode_account_config_snapshot(
    value: object,
    *,
    expected_environment: str,
    expected_account_label: str,
    field_name: str,
) -> AccountConfigSnapshot:
    payload = _require_mapping(value, field_name)
    _require_snapshot_scope(
        payload,
        expected_environment=expected_environment,
        expected_account_label=expected_account_label,
    )
    return AccountConfigSnapshot(
        environment=expected_environment,
        account_label=expected_account_label,
        multi_assets_mode=_required_bool(payload, "multi_assets_mode"),
        hedge_mode=_required_bool(payload, "hedge_mode"),
        can_trade=_required_bool(payload, "can_trade"),
        fee_tier=_optional_int_value(payload, "fee_tier"),
        observed_at=_parse_datetime(payload, "observed_at"),
        raw_payload={},
    )


def _decode_balance_snapshot(
    value: object,
    *,
    expected_environment: str,
    expected_account_label: str,
) -> AccountBalanceSnapshot:
    payload = _require_mapping(value, "account_snapshot.balances[]")
    _require_snapshot_scope(
        payload,
        expected_environment=expected_environment,
        expected_account_label=expected_account_label,
    )
    return AccountBalanceSnapshot(
        environment=expected_environment,
        account_label=expected_account_label,
        asset=_require_string(payload, "asset"),
        wallet_balance=_required_decimal(payload, "wallet_balance"),
        available_balance=_required_decimal(payload, "available_balance"),
        unrealized_pnl=_required_decimal(payload, "unrealized_pnl"),
        observed_at=_parse_datetime(payload, "observed_at"),
        raw_payload={},
    )


def _decode_position_snapshot(
    value: object,
    *,
    expected_environment: str,
    expected_account_label: str,
) -> AccountPositionSnapshot:
    payload = _require_mapping(value, "account_snapshot.positions[]")
    _require_snapshot_scope(
        payload,
        expected_environment=expected_environment,
        expected_account_label=expected_account_label,
    )
    return AccountPositionSnapshot(
        environment=expected_environment,
        account_label=expected_account_label,
        symbol=_require_string(payload, "symbol"),
        position_side=_require_string(payload, "position_side"),
        position_amt=_required_decimal(payload, "position_amt"),
        entry_price=_required_decimal(payload, "entry_price"),
        mark_price=_required_decimal(payload, "mark_price"),
        unrealized_pnl=_required_decimal(payload, "unrealized_pnl"),
        notional=_required_decimal(payload, "notional"),
        leverage=_optional_int_value(payload, "leverage"),
        margin_type=_optional_string(payload, "margin_type"),
        observed_at=_parse_datetime(payload, "observed_at"),
        raw_payload={},
    )


def _decode_open_order_snapshot(
    value: object,
    *,
    expected_environment: str,
    expected_account_label: str,
) -> AccountOpenOrderSnapshot:
    payload = _require_mapping(value, "account_snapshot.open_orders[]")
    _require_snapshot_scope(
        payload,
        expected_environment=expected_environment,
        expected_account_label=expected_account_label,
    )
    return AccountOpenOrderSnapshot(
        environment=expected_environment,
        account_label=expected_account_label,
        symbol=_require_string(payload, "symbol"),
        order_id=_require_string(payload, "order_id"),
        client_order_id=_require_string(payload, "client_order_id"),
        side=_require_string(payload, "side"),
        order_type=_require_string(payload, "order_type"),
        status=_require_string(payload, "status"),
        price=_required_decimal(payload, "price"),
        original_quantity=_required_decimal(payload, "original_quantity"),
        executed_quantity=_required_decimal(payload, "executed_quantity"),
        reduce_only=_required_bool(payload, "reduce_only"),
        observed_at=_parse_datetime(payload, "observed_at"),
        raw_payload={},
    )


def _require_mapping(value: object, field_name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise AccountEventHubProtocolError(f"{field_name} must be an object")
    return {str(key): item for key, item in value.items()}


def _mapping_field(
    payload: dict[str, object],
    field_name: str,
) -> dict[str, object]:
    return _require_mapping(payload.get(field_name), field_name)


def _required_list(
    payload: dict[str, object],
    field_name: str,
) -> list[object]:
    value = payload.get(field_name)
    if not isinstance(value, list):
        raise AccountEventHubProtocolError(f"{field_name} must be an array")
    return value


def _required_string_list(
    payload: dict[str, object],
    field_name: str,
) -> list[str]:
    values = _required_list(payload, field_name)
    if any(not isinstance(value, str) or not value.strip() for value in values):
        raise AccountEventHubProtocolError(
            f"{field_name} must contain non-empty strings"
        )
    return values  # type: ignore[return-value]


def _required_key_list(
    payload: dict[str, object],
    field_name: str,
) -> list[tuple[str, str]]:
    values = _required_list(payload, field_name)
    keys: list[tuple[str, str]] = []
    for value in values:
        if (
            not isinstance(value, list)
            or len(value) != 2
            or any(not isinstance(item, str) or not item.strip() for item in value)
        ):
            raise AccountEventHubProtocolError(
                f"{field_name} must contain two-string keys"
            )
        keys.append((value[0], value[1]))
    return keys


def _require_snapshot_scope(
    payload: dict[str, object],
    *,
    expected_environment: str,
    expected_account_label: str,
) -> None:
    if (
        _require_string(payload, "environment") != expected_environment
        or _require_string(payload, "account_label") != expected_account_label
    ):
        raise AccountEventHubProtocolError(
            "account snapshot nested scope mismatch"
        )


def _required_bool(payload: dict[str, object], field_name: str) -> bool:
    value = payload.get(field_name)
    if not isinstance(value, bool):
        raise AccountEventHubProtocolError(f"{field_name} must be a boolean")
    return value


def _optional_int_value(
    payload: dict[str, object],
    field_name: str,
) -> int | None:
    value = payload.get(field_name)
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise AccountEventHubProtocolError(
            f"{field_name} must be a non-negative integer or null"
        )
    return value


def _optional_non_negative_int(
    payload: dict[str, object],
    field_name: str,
) -> int:
    value = payload.get(field_name, 0)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise AccountEventHubProtocolError(
            f"{field_name} must be a non-negative integer"
        )
    return value


def _optional_nullable_non_negative_int(
    payload: dict[str, object],
    field_name: str,
) -> int | None:
    value = payload.get(field_name)
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise AccountEventHubProtocolError(
            f"{field_name} must be a non-negative integer or null"
        )
    return value


def _optional_snapshot_kind(payload: dict[str, object]) -> str:
    value = payload.get("snapshot_kind")
    if value is None:
        return (
            _SNAPSHOT_KIND_FULL
            if payload.get("account_snapshot") is not None
            else _SNAPSHOT_KIND_NOTIFICATION
        )
    if not isinstance(value, str) or value not in _SNAPSHOT_KINDS:
        raise AccountEventHubProtocolError(
            "snapshot_kind is not a supported account snapshot kind"
        )
    return value


def _first_sequence(messages: list[str]) -> int:
    payload = _decode_object(messages[0])
    return _optional_non_negative_int(payload, "sequence")


def _message_has_full_snapshot(message: str) -> bool:
    payload = _decode_object(message)
    return (
        payload.get("snapshot_kind") == _SNAPSHOT_KIND_FULL
        or payload.get("account_snapshot") is not None
    )


def _delta_scope(delta: AccountSnapshotDelta) -> tuple[str, str] | None:
    if delta.config is not None:
        return delta.config.environment, delta.config.account_label
    if delta.balances:
        first_balance = delta.balances[0]
        return first_balance.environment, first_balance.account_label
    if delta.positions:
        first_position = delta.positions[0]
        return first_position.environment, first_position.account_label
    if delta.open_orders:
        first_order = delta.open_orders[0]
        return first_order.environment, first_order.account_label
    return None


def _optional_account_state(
    payload: dict[str, object],
) -> ExecutionAccountStatus | None:
    value = payload.get("account_state")
    if value is None:
        return None
    if not isinstance(value, str):
        raise AccountEventHubProtocolError(
            "account_state must be a string or null"
        )
    try:
        return ExecutionAccountStatus(value)
    except ValueError as error:
        raise AccountEventHubProtocolError(
            "account_state is not a supported execution account status"
        ) from error


def _required_decimal(
    payload: dict[str, object],
    field_name: str,
) -> Decimal:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise AccountEventHubProtocolError(f"{field_name} must be a decimal string")
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError) as error:
        raise AccountEventHubProtocolError(
            f"{field_name} is not a valid decimal"
        ) from error
    if not parsed.is_finite():
        raise AccountEventHubProtocolError(
            f"{field_name} must be a finite decimal"
        )
    return parsed


def _decode_object(message: object) -> dict[str, object]:
    if isinstance(message, bytes):
        message = message.decode("utf-8")
    if isinstance(message, str):
        try:
            value = json.loads(message)
        except json.JSONDecodeError as error:
            raise AccountEventHubProtocolError("message is not valid JSON") from error
    else:
        value = message
    if not isinstance(value, dict):
        raise AccountEventHubProtocolError("message must be a JSON object")
    return {str(key): item for key, item in value.items()}


def _require_string(payload: dict[str, object], field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise AccountEventHubProtocolError(f"{field_name} must be a non-empty string")
    return value


def _optional_string(payload: dict[str, object], field_name: str) -> str | None:
    value = payload.get(field_name)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise AccountEventHubProtocolError(f"{field_name} must be a non-empty string")
    return value


def _optional_bool(payload: dict[str, object], field_name: str) -> bool:
    value = payload.get(field_name, False)
    if not isinstance(value, bool):
        raise AccountEventHubProtocolError(f"{field_name} must be a boolean")
    return value


def _parse_datetime(payload: dict[str, object], field_name: str) -> datetime:
    value = _require_string(payload, field_name)
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise AccountEventHubProtocolError(
            f"{field_name} is not ISO datetime"
        ) from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise AccountEventHubProtocolError(f"{field_name} must be timezone-aware")
    return parsed


__all__ = [
    "AccountEvent",
    "AccountEventHub",
    "AccountEventHubConfig",
    "AccountEventHubError",
    "AccountEventHubProtocolError",
    "AccountEventHubSequenceGap",
    "WebSocketAccountEventSource",
    "WebSocketAccountPositionExpectationPublisher",
    "decode_account_position_expectation",
    "decode_account_event",
    "encode_account_position_expectation",
    "encode_account_event",
]
