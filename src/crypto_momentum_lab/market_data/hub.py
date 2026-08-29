"""Low-latency fan-out for closed market-state buckets.

The market-data process owns the Binance connections and publishes closed
15-second states through this module. Consumers receive new batches and can
resume from a bounded replay window; PostgreSQL remains the durable audit and
recovery adapter, but is not in the live decision path.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, fields
from datetime import datetime
from decimal import Decimal
from typing import cast
from uuid import uuid4

import structlog
from websockets.asyncio.client import ClientConnection, connect
from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.exceptions import ConnectionClosed

from crypto_momentum_lab.domain.market.models import MarketState15s

log = structlog.get_logger()

_HUB_SCHEMA_VERSION = 1
_SUBSCRIBE_MESSAGE = "subscribe_market_states"
_READY_MESSAGE = "market_state_hub_ready"
_BATCH_MESSAGE = "market_state_batch"
_CLIENT_RECEIVE_QUEUE_SIZE = 2


class MarketStateHubError(RuntimeError):
    """Base error for the market-state hub protocol and transport."""


class MarketStateHubProtocolError(MarketStateHubError):
    """Raised when a hub message is malformed or has an incompatible schema."""


class MarketStateHubReplayUnavailable(MarketStateHubError):
    """Raised when the requested sequence is older than the replay window."""


class MarketStateHubSequenceGap(MarketStateHubError):
    """Raised when a consumer receives a non-contiguous batch sequence."""


@dataclass(frozen=True, slots=True)
class MarketStateBatch:
    sequence: int
    published_at: datetime
    environment: str
    states: tuple[MarketState15s, ...]
    stream_id: str | None = None


@dataclass(frozen=True, slots=True)
class _MarketStateQueueOverflow:
    latest_sequence: int


_MarketStateQueueItem = MarketStateBatch | _MarketStateQueueOverflow | Exception


@dataclass(frozen=True, slots=True)
class MarketStateHubConfig:
    host: str = "0.0.0.0"
    port: int = 8766
    subscriber_queue_size: int = 8
    replay_batch_count: int = 64
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
        if self.replay_batch_count <= 0:
            raise ValueError("replay_batch_count must be positive")
        if self.handshake_timeout_seconds <= 0:
            raise ValueError("handshake_timeout_seconds must be positive")
        if self.unavailable_timeout_seconds <= 0:
            raise ValueError("unavailable_timeout_seconds must be positive")
        if not self.reconnect_delays or any(
            delay < 0 for delay in self.reconnect_delays
        ):
            raise ValueError("reconnect_delays must contain non-negative values")


@dataclass(frozen=True, slots=True)
class MarketStateHubMetrics:
    connected_subscriber_count: int
    published_batch_count: int
    dropped_batch_count: int
    latest_bucket_start: datetime | None
    latest_published_at: datetime | None


@dataclass(slots=True)
class _Subscriber:
    connection: ServerConnection
    environment: str
    queue: asyncio.Queue[str]
    writer_task: asyncio.Task[None]


class MarketStateHub:
    """Deep module that hides the WebSocket fan-out implementation.

    Interface invariants:

    * ``publish`` is non-blocking with respect to a slow subscriber. A full
      live subscriber queue is replaced by the newest batch, while replay
      clients receive a contiguous sequence or an explicit failure.
    * subscribers receive only states for the requested environment.
    * a subscriber without a cursor starts at the next published batch. A
      subscriber with a matching stream and sequence cursor receives every
      buffered batch after that cursor.
    * a cursor outside the replay window is rejected explicitly. The client
      must remain fail-closed until a durable recovery path restores state.
    * a hub transport failure never needs to stop the market-data publisher;
      PostgreSQL persistence remains the independent durable adapter.
    """

    def __init__(self, config: MarketStateHubConfig | None = None) -> None:
        self._config = config or MarketStateHubConfig()
        self._server: Server | None = None
        self._bound_host: str | None = None
        self._bound_port: int | None = None
        self._subscribers: dict[int, _Subscriber] = {}
        self._subscriber_lock = asyncio.Lock()
        self._stream_id = str(uuid4())
        self._sequence_by_environment: dict[str, int] = {}
        self._replay_buffers: dict[str, deque[tuple[int, str]]] = {}
        self._published_batch_count = 0
        self._dropped_batch_count = 0
        self._latest_bucket_start: datetime | None = None
        self._latest_published_at: datetime | None = None

    @property
    def url(self) -> str:
        if self._bound_host is None or self._bound_port is None:
            raise RuntimeError("market-state hub is not started")
        host = "127.0.0.1" if self._bound_host in {"0.0.0.0", ""} else self._bound_host
        return f"ws://{host}:{self._bound_port}"

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)

    @property
    def metrics(self) -> MarketStateHubMetrics:
        return MarketStateHubMetrics(
            connected_subscriber_count=len(self._subscribers),
            published_batch_count=self._published_batch_count,
            dropped_batch_count=self._dropped_batch_count,
            latest_bucket_start=self._latest_bucket_start,
            latest_published_at=self._latest_published_at,
        )

    async def start(self) -> None:
        if self._server is not None:
            return
        self._server = await serve(
            self._handle_connection,
            self._config.host,
            self._config.port,
            max_size=4 * 1024 * 1024,
            max_queue=16,
        )
        socket = next(iter(self._server.sockets), None)
        if socket is None:
            await self._server.wait_closed()
            self._server = None
            raise RuntimeError("market-state hub did not bind a socket")
        self._bound_host = self._config.host
        self._bound_port = int(socket.getsockname()[1])
        log.info(
            "market_state_hub_started",
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

    async def publish(self, states: tuple[MarketState15s, ...]) -> None:
        """Publish a closed-state batch without waiting on consumers."""
        if not states:
            return
        published_at = datetime.now(tz=states[0].bucket_start.tzinfo)
        for environment in sorted({state.environment for state in states}):
            environment_states = tuple(
                state for state in states if state.environment == environment
            )
            sequence = self._sequence_by_environment.get(environment, 0) + 1
            self._sequence_by_environment[environment] = sequence
            message = encode_market_state_batch(
                environment_states,
                sequence=sequence,
                published_at=published_at,
                stream_id=self._stream_id,
            )
            replay_buffer = self._replay_buffers.setdefault(
                environment,
                deque(maxlen=self._config.replay_batch_count),
            )
            replay_buffer.append((sequence, message))
            self._published_batch_count += 1
            self._latest_published_at = published_at
            self._latest_bucket_start = max(
                state.bucket_start for state in environment_states
            )
            async with self._subscriber_lock:
                subscribers = tuple(
                    item
                    for item in self._subscribers.values()
                    if item.environment == environment
                )
            for subscriber in subscribers:
                self._enqueue_latest(subscriber, message)

    def _enqueue_latest(self, subscriber: _Subscriber, message: str) -> None:
        if subscriber.queue.full():
            self._dropped_batch_count += 1
            while True:
                try:
                    subscriber.queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
        try:
            subscriber.queue.put_nowait(message)
        except asyncio.QueueFull:
            self._dropped_batch_count += 1

    async def _handle_connection(self, connection: ServerConnection) -> None:
        subscriber: _Subscriber | None = None
        try:
            raw_message = await asyncio.wait_for(
                connection.recv(),
                timeout=self._config.handshake_timeout_seconds,
            )
            request = _decode_object(raw_message)
            if request.get("type") != _SUBSCRIBE_MESSAGE:
                raise MarketStateHubProtocolError("invalid subscription message")
            environment = _require_string(request, "environment")
            consumer_id = _require_string(request, "consumer_id")
            last_sequence = _optional_int(request, "last_sequence")
            requested_stream_id = _optional_string(request, "stream_id")
            if last_sequence is not None and last_sequence < 0:
                raise MarketStateHubProtocolError(
                    "last_sequence must not be negative"
                )
            stream_reset = (
                requested_stream_id is not None
                and requested_stream_id != self._stream_id
            )
            async with self._subscriber_lock:
                (
                    replay_available,
                    oldest_sequence,
                    latest_sequence,
                    replay_messages,
                ) = self._replay_snapshot(
                    environment,
                    None if stream_reset else last_sequence,
                )
                await connection.send(
                    json.dumps(
                        {
                            "type": _READY_MESSAGE,
                            "schema_version": _HUB_SCHEMA_VERSION,
                            "environment": environment,
                            "stream_id": self._stream_id,
                            "stream_reset": stream_reset,
                            "replay_available": replay_available,
                            "oldest_sequence": oldest_sequence,
                            "latest_sequence": latest_sequence,
                        },
                        separators=(",", ":"),
                    )
                )
                if not replay_available:
                    await connection.close(
                        code=1013,
                        reason="market-state replay is unavailable",
                    )
                    return
                queue: asyncio.Queue[str] = asyncio.Queue(
                    maxsize=max(
                        self._config.subscriber_queue_size,
                        len(replay_messages) + 1,
                    )
                )
                for replay_message in replay_messages:
                    queue.put_nowait(replay_message)
                subscriber = _Subscriber(
                    connection=connection,
                    environment=environment,
                    queue=queue,
                    writer_task=asyncio.create_task(
                        self._write_messages(connection, queue)
                    ),
                )
                self._subscribers[id(connection)] = subscriber
            log.info(
                "market_state_hub_subscriber_connected",
                consumer_id=consumer_id,
                environment=environment,
            )
            await connection.wait_closed()
        except (ConnectionClosed, TimeoutError):
            return
        except MarketStateHubProtocolError as error:
            await connection.close(code=1008, reason=str(error))
        except Exception as error:
            log.exception("market_state_hub_connection_failed", error=str(error))
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
            log.info("market_state_hub_subscriber_disconnected")

    def _replay_snapshot(
        self,
        environment: str,
        last_sequence: int | None,
    ) -> tuple[bool, int | None, int | None, tuple[str, ...]]:
        replay_buffer = self._replay_buffers.get(environment)
        buffered = () if replay_buffer is None else tuple(replay_buffer)
        latest_sequence = self._sequence_by_environment.get(environment, 0)
        oldest_sequence = buffered[0][0] if buffered else None
        if last_sequence is None or last_sequence == latest_sequence:
            return True, oldest_sequence, latest_sequence, ()
        if last_sequence > latest_sequence or not buffered:
            return False, oldest_sequence, latest_sequence, ()
        if oldest_sequence is None or last_sequence < oldest_sequence - 1:
            return False, oldest_sequence, latest_sequence, ()
        replay_entries = tuple(
            (sequence, message)
            for sequence, message in buffered
            if sequence > last_sequence
        )
        if not replay_entries:
            return False, oldest_sequence, latest_sequence, ()
        if replay_entries[0][0] != last_sequence + 1:
            return False, oldest_sequence, latest_sequence, ()
        if replay_entries[-1][0] != latest_sequence:
            return False, oldest_sequence, latest_sequence, ()
        return (
            True,
            oldest_sequence,
            latest_sequence,
            tuple(message for _sequence, message in replay_entries),
        )

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


class WebSocketMarketStateSource:
    """Async iterator adapter for the live strategy market-state stream."""

    def __init__(
        self,
        *,
        url: str,
        environment: str,
        consumer_id: str,
        config: MarketStateHubConfig | None = None,
        on_connection_change: Callable[[bool, str | None], None] | None = None,
    ) -> None:
        if not url.strip():
            raise ValueError("url must not be empty")
        if not environment.strip():
            raise ValueError("environment must not be empty")
        if not consumer_id.strip():
            raise ValueError("consumer_id must not be empty")
        self._url = url
        self._environment = environment
        self._consumer_id = consumer_id
        self._config = config or MarketStateHubConfig()
        self._on_connection_change = on_connection_change
        self._connection_available: bool | None = None
        self._connection_reason: str | None = None
        self._stream_id: str | None = None
        self._last_sequence: int | None = None
        self._rewarm_required = False
        self._stopping = False

    def stop(self) -> None:
        self._stopping = True

    def __aiter__(self) -> AsyncIterator[MarketState15s]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[MarketState15s]:
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
                    max_size=4 * 1024 * 1024,
                    max_queue=16,
                    proxy=None,
                ) as connection:
                    await connection.send(
                        _encode_subscription(
                            environment=self._environment,
                            consumer_id=self._consumer_id,
                            stream_id=self._stream_id,
                            last_sequence=self._last_sequence,
                        )
                    )
                    ready = _decode_object(await connection.recv())
                    if ready.get("type") != _READY_MESSAGE:
                        raise MarketStateHubProtocolError(
                            "market-state hub did not acknowledge subscription"
                        )
                    if ready.get("environment") != self._environment:
                        raise MarketStateHubProtocolError(
                            "market-state hub environment mismatch"
                        )
                    ready_stream_id = _optional_string(ready, "stream_id")
                    if (
                        ready_stream_id is not None
                        and ready_stream_id != self._stream_id
                    ):
                        self._stream_id = ready_stream_id
                        self._last_sequence = None
                    if ready.get("replay_available") is False:
                        raise MarketStateHubReplayUnavailable(
                            "market-state replay is unavailable: "
                            f"requested={self._last_sequence}, "
                            f"oldest={ready.get('oldest_sequence')}, "
                            f"latest={ready.get('latest_sequence')}"
                        )
                    ready_latest_sequence = _optional_int(
                        ready,
                        "latest_sequence",
                    )
                    if (
                        not self._rewarm_required
                        and self._last_sequence is not None
                        and ready_latest_sequence is not None
                        and self._last_sequence >= ready_latest_sequence
                    ):
                        self._notify_connection_change(True, None)
                    else:
                        self._notify_connection_change(
                            False,
                            (
                                "market_state_rewarming"
                                if self._rewarm_required
                                else "market_state_replaying"
                            ),
                        )
                    unavailable_since = time.monotonic()
                    reconnect_attempt = 0
                    receive_queue: asyncio.Queue[_MarketStateQueueItem] = (
                        asyncio.Queue(maxsize=_CLIENT_RECEIVE_QUEUE_SIZE)
                    )
                    reader_task = asyncio.create_task(
                        self._read_market_state_batches(
                            connection,
                            receive_queue,
                        ),
                        name=f"market-state-reader:{self._consumer_id}",
                    )
                    try:
                        while not self._stopping:
                            item = await receive_queue.get()
                            if isinstance(item, _MarketStateQueueOverflow):
                                self._last_sequence = item.latest_sequence
                                self._rewarm_required = True
                                self._notify_connection_change(
                                    False,
                                    "market_state_consumer_lagged",
                                )
                                raise MarketStateHubSequenceGap(
                                    "market-state consumer queue overflowed; "
                                    f"skipped through sequence={item.latest_sequence}"
                                )
                            if isinstance(item, Exception):
                                raise item
                            batch = item
                            if (
                                self._stream_id is not None
                                and batch.stream_id is not None
                                and batch.stream_id != self._stream_id
                            ):
                                raise MarketStateHubProtocolError(
                                    "market-state stream mismatch"
                                )
                            if self._last_sequence is not None:
                                if batch.sequence <= self._last_sequence:
                                    log.warning(
                                        "market_state_batch_duplicate_ignored",
                                        sequence=batch.sequence,
                                        last_sequence=self._last_sequence,
                                        environment=self._environment,
                                        consumer_id=self._consumer_id,
                                    )
                                    continue
                                expected_sequence = self._last_sequence + 1
                                if batch.sequence != expected_sequence:
                                    raise MarketStateHubSequenceGap(
                                        "market-state sequence gap: "
                                        f"expected={expected_sequence}, "
                                        f"received={batch.sequence}"
                                    )
                            for state in batch.states:
                                yield state
                            self._last_sequence = batch.sequence
                            if self._rewarm_required:
                                self._rewarm_required = False
                                self._notify_connection_change(True, None)
                            elif (
                                ready_latest_sequence is None
                                or self._last_sequence >= ready_latest_sequence
                            ):
                                self._notify_connection_change(True, None)
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
                MarketStateHubError,
            ) as error:
                self._notify_connection_change(
                    False,
                    f"{type(error).__name__}: {error}",
                )
                if (
                    time.monotonic() - unavailable_since
                    >= self._config.unavailable_timeout_seconds
                ):
                    raise MarketStateHubError(
                        "market-state hub unavailable for "
                        f"{self._config.unavailable_timeout_seconds:.1f} seconds"
                    ) from error
                delay = self._config.reconnect_delays[
                    min(reconnect_attempt, len(self._config.reconnect_delays) - 1)
                ]
                reconnect_attempt += 1
                if delay > 0:
                    await asyncio.sleep(delay)

    async def _read_market_state_batches(
        self,
        connection: ClientConnection,
        receive_queue: asyncio.Queue[_MarketStateQueueItem],
    ) -> None:
        try:
            while True:
                batch = decode_market_state_batch_envelope(
                    await connection.recv(),
                    expected_environment=self._environment,
                )
                if (
                    self._stream_id is not None
                    and batch.stream_id is not None
                    and batch.stream_id != self._stream_id
                ):
                    raise MarketStateHubProtocolError(
                        "market-state stream mismatch"
                    )
                self._enqueue_market_state_batch(receive_queue, batch)
                # Yield to the strategy consumer after each buffered message.
                # Without this fairness point a burst can be drained and
                # coalesced before the consumer receives its first batch.
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._enqueue_market_state_reader_error(receive_queue, error)

    def _enqueue_market_state_batch(
        self,
        receive_queue: asyncio.Queue[_MarketStateQueueItem],
        batch: MarketStateBatch,
    ) -> None:
        queued_items: list[_MarketStateQueueItem] = []
        while True:
            try:
                queued_items.append(receive_queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        if any(
            isinstance(item, _MarketStateQueueOverflow)
            for item in queued_items
        ):
            receive_queue.put_nowait(
                _MarketStateQueueOverflow(latest_sequence=batch.sequence)
            )
            log.warning(
                "market_state_hub_client_queue_overflow",
                consumer_id=self._consumer_id,
                environment=self._environment,
                latest_sequence=batch.sequence,
            )
            return
        for item in queued_items:
            receive_queue.put_nowait(item)
        try:
            receive_queue.put_nowait(batch)
            return
        except asyncio.QueueFull:
            pass
        while True:
            try:
                receive_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        receive_queue.put_nowait(
            _MarketStateQueueOverflow(latest_sequence=batch.sequence)
        )
        log.warning(
            "market_state_hub_client_queue_overflow",
            consumer_id=self._consumer_id,
            environment=self._environment,
            latest_sequence=batch.sequence,
        )

    @staticmethod
    def _enqueue_market_state_reader_error(
        receive_queue: asyncio.Queue[_MarketStateQueueItem],
        error: Exception,
    ) -> None:
        queued_items: list[_MarketStateQueueItem] = []
        while True:
            try:
                queued_items.append(receive_queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        overflow = next(
            (
                item
                for item in queued_items
                if isinstance(item, _MarketStateQueueOverflow)
            ),
            None,
        )
        receive_queue.put_nowait(error if overflow is None else overflow)

    def _notify_connection_change(
        self,
        available: bool,
        reason: str | None,
    ) -> None:
        if (
            self._connection_available == available
            and self._connection_reason == reason
        ):
            return
        self._connection_available = available
        self._connection_reason = reason
        log.warning(
            "market_state_hub_connection_state_changed",
            available=available,
            environment=self._environment,
            consumer_id=self._consumer_id,
            reason=reason,
        )
        if self._on_connection_change is None:
            return
        try:
            self._on_connection_change(available, reason)
        except Exception:
            log.exception(
                "market_state_hub_connection_callback_failed",
                available=available,
                environment=self._environment,
                consumer_id=self._consumer_id,
            )


def encode_market_state_batch(
    states: tuple[MarketState15s, ...],
    *,
    sequence: int,
    published_at: datetime,
    stream_id: str | None = None,
) -> str:
    if not states:
        raise ValueError("states must not be empty")
    if sequence <= 0:
        raise ValueError("sequence must be positive")
    if published_at.tzinfo is None or published_at.utcoffset() is None:
        raise ValueError("published_at must be timezone-aware")
    environments = {state.environment for state in states}
    if len(environments) != 1:
        raise ValueError("states must belong to one environment")
    payload: dict[str, object] = {
        "type": _BATCH_MESSAGE,
        "schema_version": _HUB_SCHEMA_VERSION,
        "sequence": sequence,
        "published_at": published_at.isoformat(),
        "environment": next(iter(environments)),
        "states": [market_state_to_payload(state) for state in states],
    }
    if stream_id is not None:
        payload["stream_id"] = stream_id
    return json.dumps(payload, separators=(",", ":"))


def decode_market_state_batch_envelope(
    raw_message: str | bytes,
    *,
    expected_environment: str | None = None,
) -> MarketStateBatch:
    payload = _decode_object(raw_message)
    if payload.get("type") != _BATCH_MESSAGE:
        raise MarketStateHubProtocolError("unexpected market-state message type")
    if payload.get("schema_version") != _HUB_SCHEMA_VERSION:
        raise MarketStateHubProtocolError("unsupported market-state schema")
    sequence = _require_int(payload, "sequence")
    if sequence <= 0:
        raise MarketStateHubProtocolError("sequence must be positive")
    published_at = _require_datetime(payload, "published_at")
    stream_id = _optional_string(payload, "stream_id")
    environment = _require_string(payload, "environment")
    if expected_environment is not None and environment != expected_environment:
        raise MarketStateHubProtocolError("market-state environment mismatch")
    raw_states = payload.get("states")
    if not isinstance(raw_states, list) or not raw_states:
        raise MarketStateHubProtocolError("market-state batch is empty")
    states = tuple(
        market_state_from_payload(cast(dict[str, object], item))
        for item in raw_states
        if isinstance(item, dict)
    )
    if len(states) != len(raw_states):
        raise MarketStateHubProtocolError("market-state payload contains invalid state")
    if any(state.environment != environment for state in states):
        raise MarketStateHubProtocolError("market-state state environment mismatch")
    return MarketStateBatch(
        sequence=sequence,
        published_at=published_at,
        environment=environment,
        states=states,
        stream_id=stream_id,
    )


def decode_market_state_batch(
    raw_message: str | bytes,
    *,
    expected_environment: str | None = None,
) -> tuple[MarketState15s, ...]:
    return decode_market_state_batch_envelope(
        raw_message,
        expected_environment=expected_environment,
    ).states


def market_state_to_payload(state: MarketState15s) -> dict[str, object]:
    return {
        item.name: _encode_value(getattr(state, item.name))
        for item in fields(MarketState15s)
    }


def market_state_from_payload(payload: dict[str, object]) -> MarketState15s:
    return MarketState15s(
        schema_version=_require_int(payload, "schema_version"),
        exchange=_require_string(payload, "exchange"),
        environment=_require_string(payload, "environment"),
        symbol=_require_string(payload, "symbol"),
        bucket_start=_require_datetime(payload, "bucket_start"),
        bucket_end=_require_datetime(payload, "bucket_end"),
        open_price=_optional_decimal(payload, "open_price"),
        high_price=_optional_decimal(payload, "high_price"),
        low_price=_optional_decimal(payload, "low_price"),
        close_price=_optional_decimal(payload, "close_price"),
        trade_count=_require_int(payload, "trade_count"),
        trade_notional=_require_decimal(payload, "trade_notional"),
        aggressive_buy_notional=_require_decimal(
            payload, "aggressive_buy_notional"
        ),
        aggressive_sell_notional=_require_decimal(
            payload, "aggressive_sell_notional"
        ),
        last_bid_price=_optional_decimal(payload, "last_bid_price"),
        last_ask_price=_optional_decimal(payload, "last_ask_price"),
        spread=_optional_decimal(payload, "spread"),
        midpoint=_optional_decimal(payload, "midpoint"),
        liquidation_count=_require_int(payload, "liquidation_count"),
        liquidation_notional=_require_decimal(payload, "liquidation_notional"),
        mark_price=_optional_decimal(payload, "mark_price"),
        closed_kline_count=_require_int(payload, "closed_kline_count"),
        source_event_count=_require_int(payload, "source_event_count"),
        first_received_at=_optional_datetime(payload, "first_received_at"),
        last_received_at=_optional_datetime(payload, "last_received_at"),
        closed_kline_1m_open_time=_optional_datetime(
            payload, "closed_kline_1m_open_time"
        ),
        closed_kline_1m_close_time=_optional_datetime(
            payload, "closed_kline_1m_close_time"
        ),
        closed_kline_1m_open_price=_optional_decimal(
            payload, "closed_kline_1m_open_price"
        ),
        closed_kline_1m_close_price=_optional_decimal(
            payload, "closed_kline_1m_close_price"
        ),
        data_complete=_optional_bool_default(
            payload,
            "data_complete",
            default=True,
        ),
        missing_agg_trade_count=(
            _optional_int(payload, "missing_agg_trade_count") or 0
        ),
    )


def _encode_value(value: object) -> object:
    if isinstance(value, Decimal | datetime):
        return value.isoformat() if isinstance(value, datetime) else str(value)
    return value


def _decode_object(raw_message: str | bytes | object) -> dict[str, object]:
    if isinstance(raw_message, bytes):
        raw_message = raw_message.decode("utf-8")
    if not isinstance(raw_message, str):
        raise MarketStateHubProtocolError("hub message must be text")
    try:
        payload = json.loads(raw_message)
    except json.JSONDecodeError as error:
        raise MarketStateHubProtocolError("hub message is not valid JSON") from error
    if not isinstance(payload, dict):
        raise MarketStateHubProtocolError("hub message must be an object")
    return cast(dict[str, object], payload)


def _encode_subscription(
    *,
    environment: str,
    consumer_id: str,
    stream_id: str | None,
    last_sequence: int | None,
) -> str:
    payload: dict[str, object] = {
        "type": _SUBSCRIBE_MESSAGE,
        "schema_version": _HUB_SCHEMA_VERSION,
        "environment": environment,
        "consumer_id": consumer_id,
    }
    if last_sequence is not None:
        payload["last_sequence"] = last_sequence
    if stream_id is not None:
        payload["stream_id"] = stream_id
    return json.dumps(payload, separators=(",", ":"))


def _require_string(payload: dict[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value.strip():
        raise MarketStateHubProtocolError(f"{name} must be a non-empty string")
    return value


def _optional_string(payload: dict[str, object], name: str) -> str | None:
    value = payload.get(name)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise MarketStateHubProtocolError(
            f"{name} must be a non-empty string when present"
        )
    return value


def _require_int(payload: dict[str, object], name: str) -> int:
    value = payload.get(name)
    if not isinstance(value, int) or isinstance(value, bool):
        raise MarketStateHubProtocolError(f"{name} must be an integer")
    return value


def _optional_int(payload: dict[str, object], name: str) -> int | None:
    value = payload.get(name)
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise MarketStateHubProtocolError(f"{name} must be an integer")
    return value


def _optional_bool_default(
    payload: dict[str, object],
    name: str,
    *,
    default: bool,
) -> bool:
    if name not in payload:
        return default
    value = payload[name]
    if not isinstance(value, bool):
        raise MarketStateHubProtocolError(f"{name} must be a boolean")
    return value


def _require_datetime(payload: dict[str, object], name: str) -> datetime:
    value = payload.get(name)
    if not isinstance(value, str):
        raise MarketStateHubProtocolError(f"{name} must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise MarketStateHubProtocolError(f"{name} is not a valid timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise MarketStateHubProtocolError(f"{name} must include a timezone")
    return parsed


def _optional_datetime(
    payload: dict[str, object],
    name: str,
) -> datetime | None:
    if payload.get(name) is None:
        return None
    return _require_datetime(payload, name)


def _require_decimal(payload: dict[str, object], name: str) -> Decimal:
    value = payload.get(name)
    if not isinstance(value, str | int | float) or isinstance(value, bool):
        raise MarketStateHubProtocolError(f"{name} must be numeric")
    try:
        return Decimal(str(value))
    except Exception as error:
        raise MarketStateHubProtocolError(f"{name} is not numeric") from error


def _optional_decimal(
    payload: dict[str, object],
    name: str,
) -> Decimal | None:
    if payload.get(name) is None:
        return None
    return _require_decimal(payload, name)
