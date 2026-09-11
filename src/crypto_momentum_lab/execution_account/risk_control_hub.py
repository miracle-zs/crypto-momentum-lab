"""Low-volume, fail-closed risk-control notifications.

The execution-account process owns this transport next to the account-state
Hub.  PostgreSQL remains the durable authority: an operator writes the
transition or halt first and publishes a notification only as a latency
optimization.  Consumers must therefore re-read durable control state after a
reconnect or sequence recovery before reopening entries.
"""

from __future__ import annotations

import asyncio
import hmac
import json
import time
from collections import deque
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field, replace
from datetime import datetime
from enum import StrEnum
from typing import cast
from uuid import uuid4

import structlog
from websockets.asyncio.client import ClientConnection, connect
from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.exceptions import ConnectionClosed

log = structlog.get_logger()

_SCHEMA_VERSION = 1
_SUBSCRIBE_MESSAGE = "subscribe_risk_control"
_PUBLISH_MESSAGE = "publish_risk_control"
_READY_MESSAGE = "risk_control_hub_ready"
_PUBLISHED_MESSAGE = "risk_control_published"
_EVENT_MESSAGE = "risk_control_event"
_GAP_MESSAGE = "risk_control_gap"
_CLIENT_RECEIVE_QUEUE_SIZE = 16
_MAX_MESSAGE_SIZE = 256 * 1024


class RiskControlAction(StrEnum):
    """Durable control transitions that may be pushed to a live worker."""

    DISABLE_ENTRIES = "disable_entries"
    DRAIN = "drain"
    HALT = "halt"
    REFRESH = "refresh"


@dataclass(frozen=True, slots=True)
class RiskControlEvent:
    """A durable control transition plus transport continuity metadata."""

    environment: str
    account_label: str
    strategy_name: str | None
    session_id: str | None
    action: RiskControlAction
    event_id: str
    command_id: str
    reason: str
    issued_at: datetime
    details: dict[str, object] = field(default_factory=dict)
    sequence: int = field(default=0, compare=False)
    stream_epoch: str | None = field(default=None, compare=False)

    def __post_init__(self) -> None:
        for value, field_name in (
            (self.environment, "environment"),
            (self.account_label, "account_label"),
            (self.event_id, "event_id"),
            (self.command_id, "command_id"),
            (self.reason, "reason"),
        ):
            if not value.strip():
                raise ValueError(f"{field_name} must not be empty")
        for optional_value, field_name in (
            (self.strategy_name, "strategy_name"),
            (self.session_id, "session_id"),
        ):
            if optional_value is not None and not optional_value.strip():
                raise ValueError(f"{field_name} must not be blank when present")
        if not isinstance(self.action, RiskControlAction):
            raise TypeError("action must be a RiskControlAction")
        if self.issued_at.tzinfo is None or self.issued_at.utcoffset() is None:
            raise ValueError("issued_at must be timezone-aware")
        if (
            not isinstance(self.sequence, int)
            or isinstance(self.sequence, bool)
            or self.sequence < 0
        ):
            raise ValueError("sequence must be a non-negative integer")
        if self.stream_epoch is not None and not self.stream_epoch.strip():
            raise ValueError("stream_epoch must not be blank when present")
        if not isinstance(self.details, dict):
            raise TypeError("details must be a dict")


class RiskControlHubError(RuntimeError):
    """Base error for risk-control transport and protocol failures."""


class RiskControlHubProtocolError(RiskControlHubError):
    """Raised when a risk-control message is malformed."""


class RiskControlHubSequenceGap(RiskControlHubError):
    """Raised when a consumer cannot apply a contiguous control stream."""


@dataclass(frozen=True, slots=True)
class RiskControlHubConfig:
    host: str = "0.0.0.0"
    port: int = 8769
    subscriber_queue_size: int = 16
    replay_event_count: int = 512
    handshake_timeout_seconds: float = 10.0
    unavailable_timeout_seconds: float = 120.0
    reconnect_delays: tuple[float, ...] = (0.0, 1.0, 5.0, 15.0)
    publish_token: str | None = None

    def __post_init__(self) -> None:
        if not self.host.strip():
            raise ValueError("host must not be empty")
        if not 0 <= self.port <= 65535:
            raise ValueError("port must be between 0 and 65535")
        if self.subscriber_queue_size <= 0:
            raise ValueError("subscriber_queue_size must be positive")
        if self.replay_event_count <= 0:
            raise ValueError("replay_event_count must be positive")
        if self.handshake_timeout_seconds <= 0:
            raise ValueError("handshake_timeout_seconds must be positive")
        if self.unavailable_timeout_seconds <= 0:
            raise ValueError("unavailable_timeout_seconds must be positive")
        if not self.reconnect_delays or any(
            delay < 0 for delay in self.reconnect_delays
        ):
            raise ValueError("reconnect_delays must contain non-negative values")
        if self.publish_token is not None and not self.publish_token.strip():
            raise ValueError("publish_token must not be blank when present")


@dataclass(frozen=True, slots=True)
class RiskControlHubMetrics:
    published_event_count: int
    subscriber_queue_overflow_count: int
    replay_request_count: int


@dataclass(frozen=True, slots=True)
class RiskControlHubClientMetrics:
    recovery_count: int
    queue_overflow_count: int
    last_recovery_reason: str | None


@dataclass(slots=True)
class _Subscriber:
    connection: ServerConnection
    environment: str
    account_label: str
    strategy_name: str | None
    session_id: str | None
    queue: asyncio.Queue[str]
    writer_start: asyncio.Event
    writer_task: asyncio.Task[None]


@dataclass(frozen=True, slots=True)
class _QueueOverflow:
    latest_sequence: int


_QueueItem = RiskControlEvent | _QueueOverflow | Exception


class RiskControlHub:
    """Bounded control-event fan-out owned by execution-account."""

    def __init__(self, config: RiskControlHubConfig | None = None) -> None:
        self._config = config or RiskControlHubConfig()
        self._server: Server | None = None
        self._bound_host: str | None = None
        self._bound_port: int | None = None
        self._subscribers: dict[int, _Subscriber] = {}
        self._subscriber_lock = asyncio.Lock()
        self._stream_epoch = str(uuid4())
        self._sequence = 0
        self._replay_buffer: deque[tuple[int, str]] = deque(
            maxlen=self._config.replay_event_count
        )
        self._published_event_count = 0
        self._subscriber_queue_overflow_count = 0
        self._replay_request_count = 0
        self._started_once = False

    @property
    def url(self) -> str:
        if self._bound_host is None or self._bound_port is None:
            raise RuntimeError("risk-control hub is not started")
        host = "127.0.0.1" if self._bound_host in {"0.0.0.0", ""} else self._bound_host
        return f"ws://{host}:{self._bound_port}"

    @property
    def metrics(self) -> RiskControlHubMetrics:
        return RiskControlHubMetrics(
            published_event_count=self._published_event_count,
            subscriber_queue_overflow_count=self._subscriber_queue_overflow_count,
            replay_request_count=self._replay_request_count,
        )

    async def start(self) -> None:
        if self._server is not None:
            return
        if self._started_once:
            self._stream_epoch = str(uuid4())
            self._sequence = 0
            self._replay_buffer.clear()
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
            raise RuntimeError("risk-control hub did not bind a socket")
        self._bound_host = self._config.host
        self._bound_port = int(socket.getsockname()[1])
        self._started_once = True
        log.info(
            "risk_control_hub_started",
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
        for subscriber in subscribers:
            if not subscriber.writer_task.done():
                subscriber.writer_task.cancel()
        if subscribers:
            await asyncio.gather(
                *(subscriber.writer_task for subscriber in subscribers),
                return_exceptions=True,
            )
        self._bound_host = None
        self._bound_port = None

    def publish(self, event: RiskControlEvent) -> RiskControlEvent:
        """Publish without waiting on a subscriber or database operation."""
        self._sequence += 1
        wire_event = replace(
            event,
            sequence=self._sequence,
            stream_epoch=self._stream_epoch,
        )
        message = encode_risk_control_event(wire_event)
        self._replay_buffer.append((self._sequence, message))
        self._published_event_count += 1
        for subscriber in tuple(self._subscribers.values()):
            if _event_matches_subscriber(wire_event, subscriber):
                self._enqueue(subscriber, message)
        return wire_event

    def _enqueue(self, subscriber: _Subscriber, message: str) -> None:
        if subscriber.queue.full():
            self._subscriber_queue_overflow_count += 1
            while True:
                try:
                    subscriber.queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
            gap = json.dumps(
                {
                    "type": _GAP_MESSAGE,
                    "schema_version": _SCHEMA_VERSION,
                    "latest_sequence": self._sequence,
                },
                separators=(",", ":"),
            )
            subscriber.queue.put_nowait(gap)
            return
        try:
            subscriber.queue.put_nowait(message)
        except asyncio.QueueFull:
            self._subscriber_queue_overflow_count += 1

    async def _handle_connection(self, connection: ServerConnection) -> None:
        subscriber: _Subscriber | None = None
        try:
            raw_message = await asyncio.wait_for(
                connection.recv(),
                timeout=self._config.handshake_timeout_seconds,
            )
            request = _decode_object(raw_message)
            message_type = request.get("type")
            if message_type == _PUBLISH_MESSAGE:
                self._authorize_publish(request)
                event = decode_risk_control_event(request)
                published = self.publish(event)
                await connection.send(
                    json.dumps(
                        {
                            "type": _PUBLISHED_MESSAGE,
                            "schema_version": _SCHEMA_VERSION,
                            "event_id": published.event_id,
                            "command_id": published.command_id,
                            "sequence": published.sequence,
                            "stream_epoch": published.stream_epoch,
                        },
                        separators=(",", ":"),
                    )
                )
                return
            if message_type != _SUBSCRIBE_MESSAGE:
                raise RiskControlHubProtocolError("invalid risk-control message")
            environment = _require_string(request, "environment")
            account_label = _require_string(request, "account_label")
            consumer_id = _require_string(request, "consumer_id")
            strategy_name = _optional_string(request, "strategy_name")
            session_id = _optional_string(request, "session_id")
            requested_epoch = _optional_string(request, "stream_epoch")
            last_sequence = _optional_nullable_non_negative_int(
                request,
                "last_sequence",
            )
            replay_messages, stream_reset, replay_available = (
                self._subscription_messages(
                    requested_epoch=requested_epoch,
                    last_sequence=last_sequence,
                )
            )
            if not replay_available:
                await connection.send(
                    _encode_ready(
                        environment=environment,
                        account_label=account_label,
                        stream_epoch=self._stream_epoch,
                        stream_reset=stream_reset,
                        replay_available=False,
                    )
                )
                await connection.close(
                    code=1013,
                    reason="risk-control replay unavailable",
                )
                return
            queue: asyncio.Queue[str] = asyncio.Queue(
                maxsize=max(
                    self._config.subscriber_queue_size,
                    len(replay_messages) + 1,
                )
            )
            writer_start = asyncio.Event()
            subscriber = _Subscriber(
                connection=connection,
                environment=environment,
                account_label=account_label,
                strategy_name=strategy_name,
                session_id=session_id,
                queue=queue,
                writer_start=writer_start,
                writer_task=asyncio.create_task(
                    self._write_messages(connection, queue, writer_start)
                ),
            )
            for message in replay_messages:
                queue.put_nowait(message)
            async with self._subscriber_lock:
                self._subscribers[id(connection)] = subscriber
            await connection.send(
                _encode_ready(
                    environment=environment,
                    account_label=account_label,
                    stream_epoch=self._stream_epoch,
                    stream_reset=stream_reset,
                    replay_available=True,
                )
            )
            writer_start.set()
            log.info(
                "risk_control_hub_subscriber_connected",
                consumer_id=consumer_id,
                environment=environment,
                account_label=account_label,
            )
            await connection.wait_closed()
        except (ConnectionClosed, TimeoutError):
            return
        except RiskControlHubProtocolError as error:
            await connection.close(code=1008, reason=str(error))
        except Exception as error:
            log.exception("risk_control_hub_connection_failed", error=str(error))
        finally:
            if subscriber is not None:
                async with self._subscriber_lock:
                    self._subscribers.pop(id(connection), None)
                subscriber.writer_start.set()
                if not subscriber.writer_task.done():
                    subscriber.writer_task.cancel()
                await asyncio.gather(
                    subscriber.writer_task,
                    return_exceptions=True,
                )

    def _authorize_publish(self, request: dict[str, object]) -> None:
        expected = self._config.publish_token
        if expected is None:
            return
        supplied = request.get("auth_token")
        if not isinstance(supplied, str) or not hmac.compare_digest(
            supplied,
            expected,
        ):
            raise RiskControlHubProtocolError(
                "risk-control publish authorization failed"
            )

    def _subscription_messages(
        self,
        *,
        requested_epoch: str | None,
        last_sequence: int | None,
    ) -> tuple[tuple[str, ...], bool, bool]:
        stream_reset = (
            requested_epoch is not None and requested_epoch != self._stream_epoch
        )
        if stream_reset or last_sequence is None or last_sequence == self._sequence:
            return (), stream_reset, True
        if last_sequence > self._sequence or not self._replay_buffer:
            return (), stream_reset, False
        oldest_sequence = self._replay_buffer[0][0]
        if last_sequence < oldest_sequence - 1:
            self._replay_request_count += 1
            return (), stream_reset, False
        replay = tuple(
            message
            for sequence, message in self._replay_buffer
            if sequence > last_sequence
        )
        if not replay or _first_sequence(replay) != last_sequence + 1:
            self._replay_request_count += 1
            return (), stream_reset, False
        return replay, stream_reset, True

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


class WebSocketRiskControlSource:
    """Async iterator for matching risk-control events."""

    def __init__(
        self,
        *,
        url: str,
        environment: str,
        account_label: str,
        consumer_id: str,
        strategy_name: str | None = None,
        session_id: str | None = None,
        config: RiskControlHubConfig | None = None,
        on_connection_change: Callable[[bool, str | None], None] | None = None,
    ) -> None:
        for value, field_name in (
            (url, "url"),
            (environment, "environment"),
            (account_label, "account_label"),
            (consumer_id, "consumer_id"),
        ):
            if not value.strip():
                raise ValueError(f"{field_name} must not be empty")
        for optional_value, field_name in (
            (strategy_name, "strategy_name"),
            (session_id, "session_id"),
        ):
            if optional_value is not None and not optional_value.strip():
                raise ValueError(f"{field_name} must not be blank when present")
        self._url = url
        self._environment = environment
        self._account_label = account_label
        self._consumer_id = consumer_id
        self._strategy_name = strategy_name
        self._session_id = session_id
        self._config = config or RiskControlHubConfig()
        self._on_connection_change = on_connection_change
        self._stopping = False
        self._connection_available: bool | None = None
        self._stream_epoch: str | None = None
        self._last_sequence: int | None = None
        self._recovery_count = 0
        self._queue_overflow_count = 0
        self._last_recovery_reason: str | None = None

    @property
    def metrics(self) -> RiskControlHubClientMetrics:
        return RiskControlHubClientMetrics(
            recovery_count=self._recovery_count,
            queue_overflow_count=self._queue_overflow_count,
            last_recovery_reason=self._last_recovery_reason,
        )

    def stop(self) -> None:
        self._stopping = True

    def __aiter__(self) -> AsyncIterator[RiskControlEvent]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[RiskControlEvent]:
        self._notify_connection_change(False, "connecting")
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
                    await connection.send(
                        json.dumps(
                            {
                                "type": _SUBSCRIBE_MESSAGE,
                                "schema_version": _SCHEMA_VERSION,
                                "environment": self._environment,
                                "account_label": self._account_label,
                                "consumer_id": self._consumer_id,
                                "strategy_name": self._strategy_name,
                                "session_id": self._session_id,
                                "stream_epoch": self._stream_epoch,
                                "last_sequence": self._last_sequence,
                            },
                            separators=(",", ":"),
                        )
                    )
                    ready = _decode_object(await connection.recv())
                    if ready.get("type") != _READY_MESSAGE:
                        raise RiskControlHubProtocolError(
                            "risk-control hub did not acknowledge subscription"
                        )
                    if (
                        _require_string(ready, "environment") != self._environment
                        or _require_string(ready, "account_label")
                        != self._account_label
                    ):
                        raise RiskControlHubProtocolError(
                            "risk-control hub subscription mismatch"
                        )
                    if ready.get("replay_available") is not True:
                        self._prepare_recovery("risk_control_replay_unavailable")
                        raise RiskControlHubSequenceGap(
                            "risk-control replay is unavailable"
                        )
                    ready_epoch = _optional_string(ready, "stream_epoch")
                    if (
                        self._stream_epoch is not None
                        and ready_epoch is not None
                        and ready_epoch != self._stream_epoch
                    ):
                        self._prepare_recovery("risk_control_stream_epoch_changed")
                    if ready_epoch is not None:
                        self._stream_epoch = ready_epoch
                    if ready.get("stream_reset") is True:
                        self._prepare_recovery("risk_control_stream_reset")
                    self._notify_connection_change(True, None)
                    unavailable_since = time.monotonic()
                    reconnect_attempt = 0
                    receive_queue: asyncio.Queue[_QueueItem] = asyncio.Queue(
                        maxsize=_CLIENT_RECEIVE_QUEUE_SIZE
                    )
                    reader_task = asyncio.create_task(
                        self._read_events(connection, receive_queue),
                        name=f"risk-control-reader:{self._consumer_id}",
                    )
                    try:
                        while not self._stopping:
                            item = await receive_queue.get()
                            if isinstance(item, Exception):
                                raise item
                            if isinstance(item, _QueueOverflow):
                                self._queue_overflow_count += 1
                                self._prepare_recovery("risk_control_queue_overflow")
                                raise RiskControlHubSequenceGap(
                                    "risk-control client queue overflow"
                                )
                            event = self._materialize(item)
                            if event is not None and _event_matches_filter(
                                event,
                                strategy_name=self._strategy_name,
                                session_id=self._session_id,
                            ):
                                yield event
                    finally:
                        if not reader_task.done():
                            reader_task.cancel()
                        await asyncio.gather(
                            reader_task,
                            return_exceptions=True,
                        )
            except asyncio.CancelledError:
                raise
            except (
                ConnectionClosed,
                OSError,
                TimeoutError,
                RiskControlHubError,
            ) as error:
                self._notify_connection_change(False, type(error).__name__)
                if time.monotonic() - unavailable_since >= (
                    self._config.unavailable_timeout_seconds
                ):
                    raise RiskControlHubError(
                        "risk-control hub unavailable beyond timeout"
                    ) from error
                delay = self._config.reconnect_delays[
                    min(reconnect_attempt, len(self._config.reconnect_delays) - 1)
                ]
                reconnect_attempt += 1
                if delay > 0:
                    await asyncio.sleep(delay)
        return

    async def _read_events(
        self,
        connection: ClientConnection,
        receive_queue: asyncio.Queue[_QueueItem],
    ) -> None:
        try:
            while True:
                payload = _decode_object(await connection.recv())
                if payload.get("type") == _GAP_MESSAGE:
                    latest_sequence = _require_non_negative_int(
                        payload,
                        "latest_sequence",
                    )
                    _put_queue_item(
                        receive_queue,
                        _QueueOverflow(latest_sequence=latest_sequence),
                    )
                    continue
                event = decode_risk_control_event(
                    payload,
                    expected_environment=self._environment,
                    expected_account_label=self._account_label,
                )
                _put_queue_item(receive_queue, event)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            _put_queue_item(receive_queue, error)

    def _materialize(self, event: RiskControlEvent) -> RiskControlEvent | None:
        if event.stream_epoch is not None:
            if (
                self._stream_epoch is not None
                and event.stream_epoch != self._stream_epoch
            ):
                self._prepare_recovery("risk_control_stream_epoch_changed")
                raise RiskControlHubSequenceGap(
                    "risk-control stream epoch changed mid-connection"
                )
            self._stream_epoch = event.stream_epoch
        if event.sequence <= 0:
            raise RiskControlHubProtocolError("risk-control sequence must be positive")
        if self._last_sequence is not None:
            if event.sequence <= self._last_sequence:
                return None
            if event.sequence != self._last_sequence + 1:
                self._prepare_recovery("risk_control_sequence_gap")
                raise RiskControlHubSequenceGap(
                    "risk-control sequence is not contiguous"
                )
        self._last_sequence = event.sequence
        return event

    def _prepare_recovery(self, reason: str) -> None:
        self._last_sequence = None
        self._recovery_count += 1
        self._last_recovery_reason = reason
        self._notify_connection_change(False, reason)

    def _notify_connection_change(self, available: bool, reason: str | None) -> None:
        if (
            self._connection_available == available
            and (available or self._last_recovery_reason == reason)
        ):
            return
        self._connection_available = available
        if self._on_connection_change is None:
            return
        try:
            self._on_connection_change(available, reason)
        except Exception:
            log.exception(
                "risk_control_hub_connection_callback_failed",
                available=available,
                reason=reason,
                consumer_id=self._consumer_id,
            )


class WebSocketRiskControlPublisher:
    """One-shot publisher used after a durable operator transition commits."""

    def __init__(
        self,
        *,
        url: str,
        token: str | None = None,
        config: RiskControlHubConfig | None = None,
    ) -> None:
        if not url.strip():
            raise ValueError("url must not be empty")
        if token is not None and not token.strip():
            raise ValueError("token must not be blank when present")
        self._url = url
        self._token = token
        self._config = config or RiskControlHubConfig()

    async def publish(self, event: RiskControlEvent) -> RiskControlEvent:
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
                await connection.send(
                    encode_risk_control_event(
                        event,
                        message_type=_PUBLISH_MESSAGE,
                        auth_token=self._token,
                    )
                )
                response = _decode_object(await connection.recv())
        except ConnectionClosed as error:
            raise RiskControlHubProtocolError(
                "risk-control hub rejected publish"
            ) from error
        if response.get("type") != _PUBLISHED_MESSAGE:
            raise RiskControlHubProtocolError(
                "risk-control hub did not acknowledge publish"
            )
        if _require_string(response, "event_id") != event.event_id:
            raise RiskControlHubProtocolError(
                "risk-control publish acknowledgement event mismatch"
            )
        return replace(
            event,
            sequence=_require_positive_int(response, "sequence"),
            stream_epoch=_require_string(response, "stream_epoch"),
        )


def encode_risk_control_event(
    event: RiskControlEvent,
    *,
    message_type: str = _EVENT_MESSAGE,
    auth_token: str | None = None,
) -> str:
    payload: dict[str, object] = {
        "type": message_type,
        "schema_version": _SCHEMA_VERSION,
        "environment": event.environment,
        "account_label": event.account_label,
        "strategy_name": event.strategy_name,
        "session_id": event.session_id,
        "action": event.action.value,
        "event_id": event.event_id,
        "command_id": event.command_id,
        "reason": event.reason,
        "issued_at": event.issued_at.isoformat(),
        "details": event.details,
        "sequence": event.sequence,
        "stream_epoch": event.stream_epoch,
    }
    if auth_token is not None:
        payload["auth_token"] = auth_token
    return json.dumps(payload, separators=(",", ":"))


def decode_risk_control_event(
    message: str | bytes | object,
    *,
    expected_environment: str | None = None,
    expected_account_label: str | None = None,
) -> RiskControlEvent:
    payload = _decode_object(message)
    if payload.get("type") not in {_EVENT_MESSAGE, _PUBLISH_MESSAGE}:
        raise RiskControlHubProtocolError("invalid risk-control event type")
    if payload.get("schema_version") != _SCHEMA_VERSION:
        raise RiskControlHubProtocolError("unsupported risk-control schema")
    environment = _require_string(payload, "environment")
    account_label = _require_string(payload, "account_label")
    if (
        expected_environment is not None
        and environment != expected_environment
    ) or (
        expected_account_label is not None
        and account_label != expected_account_label
    ):
        raise RiskControlHubProtocolError("risk-control event scope mismatch")
    try:
        action = RiskControlAction(_require_string(payload, "action"))
        details = payload.get("details", {})
        if not isinstance(details, dict):
            raise TypeError("details must be an object")
        return RiskControlEvent(
            environment=environment,
            account_label=account_label,
            strategy_name=_optional_string(payload, "strategy_name"),
            session_id=_optional_string(payload, "session_id"),
            action=action,
            event_id=_require_string(payload, "event_id"),
            command_id=_require_string(payload, "command_id"),
            reason=_require_string(payload, "reason"),
            issued_at=_parse_datetime(payload, "issued_at"),
            details=cast(dict[str, object], details),
            sequence=_optional_non_negative_int(payload, "sequence"),
            stream_epoch=_optional_string(payload, "stream_epoch"),
        )
    except (TypeError, ValueError) as error:
        raise RiskControlHubProtocolError(
            "invalid risk-control event payload"
        ) from error


def _event_matches_subscriber(
    event: RiskControlEvent,
    subscriber: _Subscriber,
) -> bool:
    return (
        event.environment == subscriber.environment
        and event.account_label == subscriber.account_label
    )


def _event_matches_filter(
    event: RiskControlEvent,
    *,
    strategy_name: str | None,
    session_id: str | None,
) -> bool:
    return (
        (event.strategy_name is None or event.strategy_name == strategy_name)
        and (event.session_id is None or event.session_id == session_id)
    )


def _encode_ready(
    *,
    environment: str,
    account_label: str,
    stream_epoch: str,
    stream_reset: bool,
    replay_available: bool,
) -> str:
    return json.dumps(
        {
            "type": _READY_MESSAGE,
            "schema_version": _SCHEMA_VERSION,
            "environment": environment,
            "account_label": account_label,
            "stream_epoch": stream_epoch,
            "stream_reset": stream_reset,
            "replay_available": replay_available,
        },
        separators=(",", ":"),
    )


def _decode_object(message: str | bytes | object) -> dict[str, object]:
    if isinstance(message, dict):
        return cast(dict[str, object], message)
    if isinstance(message, bytes):
        try:
            message = message.decode("utf-8")
        except UnicodeDecodeError as error:
            raise RiskControlHubProtocolError("message is not valid UTF-8") from error
    if not isinstance(message, str):
        raise RiskControlHubProtocolError("message must be text")
    try:
        payload = json.loads(message)
    except json.JSONDecodeError as error:
        raise RiskControlHubProtocolError("message is not valid JSON") from error
    if not isinstance(payload, dict):
        raise RiskControlHubProtocolError("message must be a JSON object")
    return cast(dict[str, object], payload)


def _require_string(payload: dict[str, object], field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise RiskControlHubProtocolError(f"{field_name} must be a non-empty string")
    return value


def _optional_string(
    payload: dict[str, object],
    field_name: str,
) -> str | None:
    value = payload.get(field_name)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise RiskControlHubProtocolError(f"{field_name} must be a non-empty string")
    return value


def _optional_non_negative_int(
    payload: dict[str, object],
    field_name: str,
) -> int:
    value = payload.get(field_name, 0)
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 0
    ):
        raise RiskControlHubProtocolError(
            f"{field_name} must be a non-negative integer"
        )
    return value


def _optional_nullable_non_negative_int(
    payload: dict[str, object],
    field_name: str,
) -> int | None:
    if field_name not in payload or payload[field_name] is None:
        return None
    return _optional_non_negative_int(payload, field_name)


def _require_non_negative_int(payload: dict[str, object], field_name: str) -> int:
    return _optional_non_negative_int(payload, field_name)


def _require_positive_int(payload: dict[str, object], field_name: str) -> int:
    value = _require_non_negative_int(payload, field_name)
    if value <= 0:
        raise RiskControlHubProtocolError(f"{field_name} must be positive")
    return value


def _parse_datetime(payload: dict[str, object], field_name: str) -> datetime:
    value = _require_string(payload, field_name)
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise RiskControlHubProtocolError(
            f"{field_name} must be an ISO-8601 datetime"
        ) from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RiskControlHubProtocolError(f"{field_name} must be timezone-aware")
    return parsed


def _first_sequence(messages: tuple[str, ...]) -> int:
    if not messages:
        raise ValueError("messages must not be empty")
    return _require_positive_int(_decode_object(messages[0]), "sequence")


def _put_queue_item(
    queue: asyncio.Queue[_QueueItem],
    item: _QueueItem,
) -> None:
    if queue.full():
        while True:
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        queue.put_nowait(
            _QueueOverflow(
                latest_sequence=(
                    item.sequence
                    if isinstance(item, RiskControlEvent)
                    else 0
                )
            )
        )
        return
    queue.put_nowait(item)
