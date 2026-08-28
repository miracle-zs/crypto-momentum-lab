"""Low-latency account-event fan-out for live execution.

The execution-account process owns the Binance user-data WebSocket.  Once an
event has been persisted, this module publishes a small, versioned event
notification to live consumers.  The notification is deliberately not the
durable account snapshot: PostgreSQL remains the authoritative recovery
adapter, while the hub removes the account-event latency from the normal live
order path.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime

import structlog
from websockets.asyncio.client import connect
from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.exceptions import ConnectionClosed

log = structlog.get_logger()

_SCHEMA_VERSION = 1
_SUBSCRIBE_MESSAGE = "subscribe_account_events"
_READY_MESSAGE = "account_event_hub_ready"
_EVENT_MESSAGE = "account_event"


class AccountEventHubError(RuntimeError):
    """Base error for account-event hub protocol and transport failures."""


class AccountEventHubProtocolError(AccountEventHubError):
    """Raised when an account-event hub message is malformed."""


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


@dataclass(frozen=True, slots=True)
class AccountEventHubConfig:
    host: str = "0.0.0.0"
    port: int = 8767
    subscriber_queue_size: int = 16
    handshake_timeout_seconds: float = 10.0
    unavailable_timeout_seconds: float = 120.0
    reconnect_delays: tuple[float, ...] = (0.0, 1.0, 5.0, 15.0)

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


@dataclass(slots=True)
class _Subscriber:
    connection: ServerConnection
    environment: str
    account_label: str
    queue: asyncio.Queue[str]
    writer_task: asyncio.Task[None]


class AccountEventHub:
    """Bounded latest-event fan-out owned by execution-account."""

    def __init__(self, config: AccountEventHubConfig | None = None) -> None:
        self._config = config or AccountEventHubConfig()
        self._server: Server | None = None
        self._bound_host: str | None = None
        self._bound_port: int | None = None
        self._subscribers: dict[int, _Subscriber] = {}
        self._subscriber_lock = asyncio.Lock()
        self._sequence = 0

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
        self._server = await serve(
            self._handle_connection,
            self._config.host,
            self._config.port,
            max_size=256 * 1024,
            max_queue=16,
        )
        socket = next(iter(self._server.sockets), None)
        if socket is None:
            await self._server.wait_closed()
            self._server = None
            raise RuntimeError("account-event hub did not bind a socket")
        self._bound_host = self._config.host
        self._bound_port = int(socket.getsockname()[1])
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
            await subscriber.connection.close()
        if subscribers:
            await asyncio.gather(
                *(subscriber.writer_task for subscriber in subscribers),
                return_exceptions=True,
            )
        self._bound_host = None
        self._bound_port = None

    def publish(self, event: AccountEvent) -> None:
        """Publish without waiting on a consumer or database operation."""
        self._sequence += 1
        message = encode_account_event(event, sequence=self._sequence)
        for subscriber in tuple(self._subscribers.values()):
            if (
                subscriber.environment == event.environment
                and subscriber.account_label == event.account_label
            ):
                self._enqueue_latest(subscriber, message)

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
            if request.get("type") != _SUBSCRIBE_MESSAGE:
                raise AccountEventHubProtocolError("invalid subscription message")
            environment = _require_string(request, "environment")
            account_label = _require_string(request, "account_label")
            consumer_id = _require_string(request, "consumer_id")
            await connection.send(
                json.dumps(
                    {
                        "type": _READY_MESSAGE,
                        "schema_version": _SCHEMA_VERSION,
                        "environment": environment,
                        "account_label": account_label,
                    },
                    separators=(",", ":"),
                )
            )
            queue: asyncio.Queue[str] = asyncio.Queue(
                maxsize=self._config.subscriber_queue_size
            )
            subscriber = _Subscriber(
                connection=connection,
                environment=environment,
                account_label=account_label,
                queue=queue,
                writer_task=asyncio.create_task(
                    self._write_messages(connection, queue)
                ),
            )
            async with self._subscriber_lock:
                self._subscribers[id(connection)] = subscriber
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
            log.info("account_event_hub_subscriber_disconnected")

    async def _write_messages(
        self,
        connection: ServerConnection,
        queue: asyncio.Queue[str],
    ) -> None:
        try:
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
        self._stopping = False

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
                    max_size=256 * 1024,
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
                            },
                            separators=(",", ":"),
                        )
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
                    unavailable_since = time.monotonic()
                    reconnect_attempt = 0
                    while not self._stopping:
                        yield decode_account_event(
                            await connection.recv(),
                            expected_environment=self._environment,
                            expected_account_label=self._account_label,
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


def encode_account_event(event: AccountEvent, *, sequence: int) -> str:
    if sequence <= 0:
        raise ValueError("sequence must be positive")
    return json.dumps(
        {
            "type": _EVENT_MESSAGE,
            "schema_version": _SCHEMA_VERSION,
            "sequence": sequence,
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
    )


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
    "WebSocketAccountEventSource",
    "decode_account_event",
    "encode_account_event",
]
