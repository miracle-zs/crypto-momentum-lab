import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, cast
from uuid import UUID, uuid4

import structlog
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

from crypto_momentum_lab.domain.market.models import (
    CaptureRoute,
    CaptureStream,
    ConnectionLifecycleEvent,
    JsonValue,
    RawEnvelope,
)
from crypto_momentum_lab.market_data.capture.queue import CaptureQueueFull
from crypto_momentum_lab.market_data.capture.subscriptions import (
    GLOBAL_BOOK_TICKER_STREAM_NAME,
)


class BinancePayloadError(ValueError):
    pass


class BinanceControlAckTimeout(TimeoutError):
    """A control command did not receive its ACK before the deadline."""


type EnvelopeSink = Callable[[RawEnvelope], Awaitable[None]]
type LifecycleSink = Callable[[ConnectionLifecycleEvent], Awaitable[None]]

log = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class BinanceWebSocketMetricsSnapshot:
    group_id: str
    stream: CaptureStream
    desired_subscriptions: int
    connection_attempts: int
    reconnect_count: int
    active: bool
    ready: bool
    ack_mismatch_count: int
    control_commands_sent: int
    received_messages: int
    received_bytes: int
    last_message_age_seconds: float | None
    last_close_code: int | None
    last_reason: str | None
    phase: str = "disconnected"
    pending_control_id: str | None = None
    pending_control_method: str | None = None
    ingress_queue_events: int = 0
    ingress_queue_dropped_events: int = 0


class _ConnectionPhase(StrEnum):
    DISCONNECTED = "disconnected"
    SYNCING = "syncing"
    READY = "ready"


@dataclass(frozen=True, slots=True)
class _PendingControl:
    control_id: str
    method: str
    names: tuple[str, ...]
    generation: int
    deadline_monotonic: float


class BinanceWebSocketConnection:
    def __init__(
        self,
        *,
        base_url: str,
        group_id: str = "",
        route: CaptureRoute,
        environment: str,
        desired_names: tuple[str, ...],
        generation: int,
        on_envelope: EnvelopeSink,
        on_lifecycle: LifecycleSink,
        reconnect_delays: tuple[float, ...],
        connection_lifetime_seconds: float,
        open_timeout_seconds: float,
        ping_interval_seconds: float,
        ping_timeout_seconds: float,
        silence_timeout_seconds: float,
        control_ack_timeout_seconds: float = 10.0,
        control_messages_per_second: float = 5,
        ingress_queue_max_events: int = 512,
        symbol_filter: Callable[[CaptureStream, str], bool] | None = None,
        on_realtime_envelope: EnvelopeSink | None = None,
    ) -> None:
        self._base_url = base_url
        self._group_id = group_id
        self._route = route
        self._environment = environment
        self._desired_names = tuple(sorted(desired_names))
        self._stream = _stream_from_name(self._desired_names[0])
        self._generation = generation
        self._on_envelope = on_envelope
        self._on_lifecycle = on_lifecycle
        self._reconnect_delays = reconnect_delays
        self._connection_lifetime_seconds = connection_lifetime_seconds
        self._open_timeout_seconds = open_timeout_seconds
        self._ping_interval_seconds = ping_interval_seconds
        self._ping_timeout_seconds = ping_timeout_seconds
        self._silence_timeout_seconds = silence_timeout_seconds
        if control_ack_timeout_seconds <= 0:
            raise ValueError("control_ack_timeout_seconds must be positive")
        if ingress_queue_max_events <= 0:
            raise ValueError("ingress_queue_max_events must be positive")
        self._control_ack_timeout_seconds = control_ack_timeout_seconds
        self._control_interval_seconds = 1 / control_messages_per_second
        self._ingress_queue_max_events = ingress_queue_max_events
        self._symbol_filter = symbol_filter
        self._on_realtime_envelope = on_realtime_envelope
        self._connection_lock = asyncio.Lock()
        self._desired_lock = asyncio.Lock()
        self._desired_event = asyncio.Event()
        self._stopping = False
        self._connection: ClientConnection | None = None
        self._control_id = 0
        self._task: asyncio.Task[None] | None = None
        self._phase = _ConnectionPhase.DISCONNECTED
        self._applied_names: tuple[str, ...] = ()
        self._active_generation = generation
        self._pending_control: _PendingControl | None = None
        self._connection_attempts = 0
        self._reconnect_count = 0
        self._ack_mismatch_count = 0
        self._control_commands_sent = 0
        self._received_messages = 0
        self._received_bytes = 0
        self._last_received_monotonic: float | None = None
        self._last_close_code: int | None = None
        self._last_reason: str | None = None
        self._ingress_queue_events = 0
        self._ingress_queue_dropped_events = 0

    def metrics_snapshot(self) -> BinanceWebSocketMetricsSnapshot:
        return BinanceWebSocketMetricsSnapshot(
            group_id=self._group_id,
            stream=self._stream,
            desired_subscriptions=len(self._desired_names),
            connection_attempts=self._connection_attempts,
            reconnect_count=self._reconnect_count,
            active=self._connection is not None,
            ready=self._phase is _ConnectionPhase.READY,
            ack_mismatch_count=self._ack_mismatch_count,
            control_commands_sent=self._control_commands_sent,
            received_messages=self._received_messages,
            received_bytes=self._received_bytes,
            last_message_age_seconds=(
                None
                if self._last_received_monotonic is None
                else max(
                    0.0,
                    time.monotonic() - self._last_received_monotonic,
                )
            ),
            last_close_code=self._last_close_code,
            last_reason=self._last_reason,
            phase=self._phase.value,
            pending_control_id=(
                None
                if self._pending_control is None
                else self._pending_control.control_id
            ),
            pending_control_method=(
                None
                if self._pending_control is None
                else self._pending_control.method
            ),
            ingress_queue_events=self._ingress_queue_events,
            ingress_queue_dropped_events=self._ingress_queue_dropped_events,
        )

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self.run())

    async def run(self) -> None:
        reconnect_attempt = 0
        while not self._stopping:
            session_id = uuid4()
            reason = "stopped"
            self._connection_attempts += 1
            if self._connection_attempts > 1:
                self._reconnect_count += 1
            try:
                reason = await self._run_once(session_id)
            except (
                CaptureQueueFull,
                ConnectionClosed,
                TimeoutError,
                OSError,
            ) as error:
                reason = error.__class__.__name__
                close_code = getattr(error, "code", None)
                self._last_close_code = (
                    close_code if isinstance(close_code, int) else None
                )
                log.warning(
                    "binance_websocket_session_ended",
                    group_id=self._group_id,
                    route=self._route.value,
                    stream=self._stream.value,
                    subscription_count=len(self._desired_names),
                    symbols=self._desired_names,
                    reason=reason,
                    error=str(error),
                    close_code=self._last_close_code,
                    reconnect_count=self._reconnect_count,
                )
            except Exception as error:
                reason = error.__class__.__name__
                log.exception(
                    "binance_websocket_session_failed",
                    group_id=self._group_id,
                    route=self._route.value,
                    stream=self._stream.value,
                    subscription_count=len(self._desired_names),
                    symbols=self._desired_names,
                    reason=reason,
                    error=str(error),
                )
            finally:
                async with self._connection_lock:
                    self._connection = None
                self._phase = _ConnectionPhase.DISCONNECTED
                self._applied_names = ()
                self._pending_control = None
                self._last_reason = reason
                try:
                    await self._emit_lifecycle(
                        session_id,
                        opened=False,
                        reason=reason,
                    )
                except Exception as error:
                    log.exception(
                        "binance_websocket_lifecycle_sink_failed",
                        route=self._route.value,
                        stream=self._stream.value,
                        reason=error.__class__.__name__,
                        error=str(error),
                    )

            if self._stopping:
                return
            delay = self._reconnect_delays[
                min(reconnect_attempt, len(self._reconnect_delays) - 1)
            ]
            reconnect_attempt += 1
            if delay > 0:
                await asyncio.sleep(delay)

    async def stop(self) -> None:
        self._stopping = True
        self._desired_event.set()
        async with self._connection_lock:
            connection = self._connection
        if connection is not None:
            await connection.close()
        if self._task is not None:
            await self._task

    async def subscribe(
        self,
        names: tuple[str, ...],
        *,
        generation: int,
    ) -> None:
        if not names:
            return
        async with self._desired_lock:
            self._desired_names = tuple(
                sorted(set((*self._desired_names, *names)))
            )
            self._generation = generation
        self._desired_event.set()

    async def unsubscribe(
        self,
        names: tuple[str, ...],
        *,
        generation: int,
    ) -> None:
        remove = set(names)
        async with self._desired_lock:
            self._desired_names = tuple(
                name for name in self._desired_names if name not in remove
            )
            self._generation = generation
        self._desired_event.set()

    async def _run_once(self, session_id: UUID) -> str:
        uri = self._base_url.rstrip("/")
        opened_at = time.monotonic()
        data_queue: asyncio.Queue[RawEnvelope] = asyncio.Queue(
            maxsize=self._ingress_queue_max_events
        )
        ack_queue: asyncio.Queue[object] = asyncio.Queue(maxsize=32)
        reader_task: asyncio.Task[None] | None = None
        dispatch_task: asyncio.Task[None] | None = None
        ack_task: asyncio.Task[object] | None = None
        async with connect(
            uri,
            proxy=None,
            open_timeout=self._open_timeout_seconds,
            ping_interval=self._ping_interval_seconds,
            ping_timeout=_ping_timeout_for_stream(
                self._stream,
                configured_timeout=self._ping_timeout_seconds,
            ),
            max_queue=16,
        ) as connection:
            async with self._connection_lock:
                self._connection = connection
            self._phase = _ConnectionPhase.SYNCING
            self._applied_names = ()
            self._pending_control = None
            self._last_received_monotonic = None
            await self._emit_lifecycle(session_id, opened=True, reason=None)
            # The socket reader is deliberately independent from the
            # downstream capture queue.  A high-frequency bookTicker/aggTrade
            # consumer must not delay a control ACK long enough to make the
            # Binance actor believe the connection is dead.
            reader_task = asyncio.create_task(
                self._read_messages(
                    connection,
                    session_id=session_id,
                    data_queue=data_queue,
                    ack_queue=ack_queue,
                )
            )
            dispatch_task = asyncio.create_task(
                self._dispatch_messages(data_queue)
            )
            initial_names, initial_generation = await self._desired_snapshot()
            await self._start_control(
                connection,
                "SUBSCRIBE",
                initial_names,
                generation=initial_generation,
            )
            ack_task = asyncio.create_task(ack_queue.get())
            desired_task = asyncio.create_task(self._desired_event.wait())
            silence_timeout = _silence_timeout_for_stream(
                self._stream,
                configured_timeout=self._silence_timeout_seconds,
            )
            try:
                while not self._stopping:
                    if should_replace_connection(
                        opened_at=opened_at,
                        now=time.monotonic(),
                        lifetime_seconds=self._connection_lifetime_seconds,
                    ):
                        return "lifetime_expired"

                    now = time.monotonic()
                    control_timeout = _remaining_control_timeout(
                        self._pending_control,
                        now=now,
                    )
                    if control_timeout is not None and control_timeout <= 0:
                        raise BinanceControlAckTimeout(
                            "Binance control ACK timed out"
                        )
                    timeout = (
                        None
                        if silence_timeout is None
                        else _remaining_silence_timeout(
                            self._last_received_monotonic,
                            now=now,
                            silence_timeout=silence_timeout,
                        )
                    )
                    if control_timeout is not None:
                        timeout = (
                            control_timeout
                            if timeout is None
                            else min(timeout, control_timeout)
                        )
                    done, _ = await asyncio.wait(
                        (reader_task, ack_task, desired_task),
                        timeout=timeout,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if not done:
                        if (
                            self._pending_control is not None
                            and _remaining_control_timeout(
                                self._pending_control,
                                now=time.monotonic(),
                            )
                            <= 0
                        ):
                            raise BinanceControlAckTimeout(
                                "Binance control ACK timed out"
                            )
                        raise TimeoutError("Binance WebSocket is silent")

                    if reader_task in done:
                        reader_task.result()
                        return "stopped"

                    if ack_task in done:
                        decoded = ack_task.result()
                        self._resolve_control_ack(decoded)
                        await self._maybe_start_next_control(connection)
                        ack_task = asyncio.create_task(ack_queue.get())

                    desired_changed = desired_task in done
                    if desired_changed:
                        self._desired_event.clear()
                        desired_task = asyncio.create_task(
                            self._desired_event.wait()
                        )

                    if desired_changed:
                        await self._maybe_start_next_control(connection)
            finally:
                child_tasks: tuple[asyncio.Future[Any], ...] = tuple(
                    task
                    for task in (
                        reader_task,
                        dispatch_task,
                        ack_task,
                        desired_task,
                    )
                    if task is not None
                )
                await _cancel_and_drain_tasks(child_tasks)
        return "closed"

    async def _read_messages(
        self,
        connection: ClientConnection,
        *,
        session_id: UUID,
        data_queue: asyncio.Queue[RawEnvelope],
        ack_queue: asyncio.Queue[object],
    ) -> None:
        local_sequence = 0
        while not self._stopping:
            message = await connection.recv()
            self._received_messages += 1
            self._received_bytes += (
                len(message)
                if isinstance(message, bytes)
                else len(message.encode("utf-8"))
            )
            self._last_received_monotonic = time.monotonic()
            decoded = _decode_message(message)
            if _is_control_ack(decoded):
                try:
                    ack_queue.put_nowait(decoded)
                except asyncio.QueueFull as exc:
                    raise CaptureQueueFull(
                        "control ACK queue is saturated"
                    ) from exc
                continue

            if self._symbol_filter is not None:
                symbol = _fast_symbol(decoded)
                if symbol is not None and not self._symbol_filter(
                    self._stream,
                    symbol,
                ):
                    continue

            received_at = datetime.now(UTC)
            received_monotonic_ns = time.monotonic_ns()
            try:
                envelope = _parse_decoded_message(
                    route=self._route,
                    decoded=decoded,
                    environment=self._environment,
                    connection_session_id=session_id,
                    local_sequence=local_sequence + 1,
                    subscription_generation=self._active_generation,
                    received_at=received_at,
                    received_monotonic_ns=received_monotonic_ns,
                    expected_stream=self._stream,
                )
            except BinancePayloadError:
                continue
            local_sequence += 1
            if self._on_realtime_envelope is not None:
                await self._on_realtime_envelope(envelope)
            try:
                data_queue.put_nowait(envelope)
            except asyncio.QueueFull as exc:
                self._ingress_queue_dropped_events += 1
                raise CaptureQueueFull(
                    "WebSocket ingress queue is saturated"
                ) from exc
            self._ingress_queue_events = data_queue.qsize()
            # websockets may return an already-buffered message without
            # suspending. Yield explicitly so the actor can process an ACK
            # and the dispatcher can make progress under bursty streams.
            await asyncio.sleep(0)

    async def _dispatch_messages(
        self,
        data_queue: asyncio.Queue[RawEnvelope],
    ) -> None:
        while not self._stopping or not data_queue.empty():
            try:
                envelope = await asyncio.wait_for(
                    data_queue.get(),
                    timeout=0.1,
                )
            except TimeoutError:
                continue
            self._ingress_queue_events = data_queue.qsize()
            try:
                await self._on_envelope(envelope)
            finally:
                data_queue.task_done()
            await asyncio.sleep(0)

    async def _send_control(
        self,
        connection: ClientConnection,
        method: str,
        names: tuple[str, ...],
    ) -> str:
        if not names:
            raise ValueError("control message must contain at least one name")
        await asyncio.sleep(self._control_interval_seconds)
        self._control_id += 1
        control_id = str(self._control_id)
        await connection.send(
            json.dumps(
                {
                    "method": method,
                    "params": list(names),
                    "id": control_id,
                }
            )
        )
        self._control_commands_sent += 1
        return control_id

    async def _start_control(
        self,
        connection: ClientConnection,
        method: str,
        names: tuple[str, ...],
        *,
        generation: int,
    ) -> None:
        if not names:
            self._phase = _ConnectionPhase.READY
            return
        if self._pending_control is not None:
            raise RuntimeError("a Binance control command is already pending")
        control_id = await self._send_control(connection, method, names)
        self._pending_control = _PendingControl(
            control_id=control_id,
            method=method,
            names=names,
            generation=generation,
            deadline_monotonic=(
                time.monotonic() + self._control_ack_timeout_seconds
            ),
        )
        self._phase = _ConnectionPhase.SYNCING

    async def _maybe_start_next_control(
        self,
        connection: ClientConnection,
    ) -> None:
        if self._pending_control is not None:
            return
        desired_names, generation = await self._desired_snapshot()
        applied_names = set(self._applied_names)
        desired_set = set(desired_names)
        additions = tuple(sorted(desired_set - applied_names))
        if additions:
            await self._start_control(
                connection,
                "SUBSCRIBE",
                additions,
                generation=generation,
            )
            return
        removals = tuple(sorted(applied_names - desired_set))
        if removals:
            await self._start_control(
                connection,
                "UNSUBSCRIBE",
                removals,
                generation=generation,
            )
            return
        self._phase = _ConnectionPhase.READY
        self._active_generation = generation

    async def _desired_snapshot(self) -> tuple[tuple[str, ...], int]:
        async with self._desired_lock:
            return self._desired_names, self._generation

    def _resolve_control_ack(self, decoded: object) -> bool:
        if not isinstance(decoded, dict) or "id" not in decoded:
            return False
        raw_control_id = decoded.get("id")
        if isinstance(raw_control_id, bool) or not isinstance(
            raw_control_id, str | int
        ):
            return False
        control_id = str(raw_control_id)
        pending = self._pending_control
        if pending is None or pending.control_id != control_id:
            self._ack_mismatch_count += 1
            log.warning(
                "binance_websocket_ack_unmatched",
                route=self._route.value,
                stream=self._stream.value,
                received_id=control_id,
                expected_id=(
                    None if pending is None else pending.control_id
                ),
            )
            return True
        code = decoded.get("code")
        if code is not None and code != 0:
            raise BinancePayloadError(
                f"Binance control command rejected: "
                f"{code} {decoded.get('msg', '')}".strip()
            )
        self._pending_control = None
        if pending.method == "SUBSCRIBE":
            self._applied_names = tuple(
                sorted(set((*self._applied_names, *pending.names)))
            )
        else:
            removed = set(pending.names)
            self._applied_names = tuple(
                name for name in self._applied_names if name not in removed
            )
        self._active_generation = pending.generation
        return True

    async def _emit_lifecycle(
        self,
        session_id: UUID,
        *,
        opened: bool,
        reason: str | None,
    ) -> None:
        await self._on_lifecycle(
            ConnectionLifecycleEvent(
                session_id=session_id,
                route=self._route,
                stream=self._stream,
                symbols=tuple(
                    sorted(
                        {
                            _symbol_from_stream_name(name)
                            for name in self._desired_names
                        }
                    )
                ),
                occurred_at=datetime.now(UTC),
                opened=opened,
                reason=reason,
            )
        )


def _silence_timeout_for_stream(
    stream: CaptureStream,
    *,
    configured_timeout: float,
) -> float | None:
    # A quiet interval is not a transport failure for event streams. The
    # WebSocket ping/pong watchdog owns transport liveness; otherwise an
    # inactive symbol set would be mistaken for a dead connection.
    if stream in {
        CaptureStream.AGG_TRADE,
        CaptureStream.BOOK_TICKER,
        CaptureStream.FORCE_ORDER,
    }:
        return None
    return configured_timeout


def _remaining_control_timeout(
    pending: _PendingControl | None,
    *,
    now: float,
) -> float | None:
    if pending is None:
        return None
    return pending.deadline_monotonic - now


def _remaining_silence_timeout(
    last_received_monotonic: float | None,
    *,
    now: float,
    silence_timeout: float,
) -> float:
    if last_received_monotonic is None:
        return silence_timeout
    return max(0.001, silence_timeout - (now - last_received_monotonic))


def _ping_timeout_for_stream(
    stream: CaptureStream,
    *,
    configured_timeout: float,
) -> float | None:
    # Binance can delay pong frames while a high-frequency multiplexed socket
    # is flowing normally. A delayed pong must not tear down the data path;
    # the WebSocket close signal and scheduled connection lifetime remain the
    # transport recovery mechanisms for these streams.
    if stream in {CaptureStream.AGG_TRADE, CaptureStream.BOOK_TICKER}:
        return None
    return configured_timeout


def route_for(stream: CaptureStream) -> CaptureRoute:
    return (
        CaptureRoute.PUBLIC
        if stream is CaptureStream.BOOK_TICKER
        else CaptureRoute.MARKET
    )


def parse_binance_message(
    *,
    route: CaptureRoute,
    message: str | bytes,
    environment: str,
    connection_session_id: UUID,
    local_sequence: int,
    subscription_generation: int,
    received_at: datetime,
    received_monotonic_ns: int,
    expected_stream: CaptureStream | None = None,
) -> RawEnvelope:
    return _parse_decoded_message(
        route=route,
        decoded=_decode_message(message),
        environment=environment,
        connection_session_id=connection_session_id,
        local_sequence=local_sequence,
        subscription_generation=subscription_generation,
        received_at=received_at,
        received_monotonic_ns=received_monotonic_ns,
        expected_stream=expected_stream,
    )


def _decode_message(message: str | bytes) -> object:
    try:
        return json.loads(message)
    except json.JSONDecodeError as exc:
        raise BinancePayloadError("message is not valid JSON") from exc


def _fast_symbol(decoded: object) -> str | None:
    """Extract a Binance symbol before allocating a RawEnvelope."""
    if not isinstance(decoded, dict):
        return None
    payload = decoded.get("data")
    if not isinstance(payload, dict):
        payload = decoded
    direct = payload.get("s")
    if isinstance(direct, str):
        return direct
    order = payload.get("o")
    if isinstance(order, dict):
        nested = order.get("s")
        if isinstance(nested, str):
            return nested
    return None


def _is_control_ack(decoded: object) -> bool:
    return isinstance(decoded, dict) and "id" in decoded


def _parse_decoded_message(
    *,
    route: CaptureRoute,
    decoded: object,
    environment: str,
    connection_session_id: UUID,
    local_sequence: int,
    subscription_generation: int,
    received_at: datetime,
    received_monotonic_ns: int,
    expected_stream: CaptureStream | None = None,
) -> RawEnvelope:
    if not isinstance(decoded, dict):
        raise BinancePayloadError("message must be an object")
    stream_name = decoded.get("stream")
    data = decoded.get("data")
    if isinstance(stream_name, str) and isinstance(data, dict):
        stream = _stream_from_name(stream_name)
        payload = data
    elif expected_stream is not None:
        stream = expected_stream
        payload = decoded
    else:
        raise BinancePayloadError("stream envelope is invalid")
    if route_for(stream) is not route:
        raise BinancePayloadError("stream arrived on unexpected route")

    symbol = _symbol(payload)
    event_ms = _event_time_ms(payload)
    exchange_sequence = _exchange_sequence(stream, payload)
    raw_payload = cast(dict[str, JsonValue], payload)
    return RawEnvelope(
        schema_version=1,
        exchange="binance-usdm",
        environment=environment,
        route=route,
        stream=stream,
        symbol=symbol,
        exchange_event_at=None if event_ms is None else _utc_from_ms(event_ms),
        received_at=received_at,
        received_monotonic_ns=received_monotonic_ns,
        connection_session_id=connection_session_id,
        local_sequence=local_sequence,
        exchange_sequence=exchange_sequence,
        subscription_generation=subscription_generation,
        raw_payload=raw_payload,
    )


def _stream_from_name(stream_name: str) -> CaptureStream:
    if stream_name == GLOBAL_BOOK_TICKER_STREAM_NAME:
        return CaptureStream.BOOK_TICKER
    _, separator, suffix = stream_name.partition("@")
    if not separator:
        raise BinancePayloadError("stream name is invalid")
    try:
        return CaptureStream(suffix)
    except ValueError as exc:
        raise BinancePayloadError(f"unsupported stream: {suffix}") from exc


def _symbol_from_stream_name(stream_name: str) -> str:
    if stream_name == GLOBAL_BOOK_TICKER_STREAM_NAME:
        return "*"
    symbol, separator, _ = stream_name.partition("@")
    if not separator or not symbol:
        raise BinancePayloadError("stream name is invalid")
    return symbol.upper()


def _symbol(payload: dict[object, object]) -> str:
    direct = payload.get("s")
    if isinstance(direct, str):
        return direct
    order = payload.get("o")
    if isinstance(order, dict):
        nested = order.get("s")
        if isinstance(nested, str):
            return nested
    raise BinancePayloadError("payload symbol is missing")


def _event_time_ms(payload: dict[object, object]) -> int | None:
    value = payload.get("E")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    order = payload.get("o")
    if isinstance(order, dict):
        nested = order.get("T")
        if isinstance(nested, int):
            return nested
    return None


def _exchange_sequence(
    stream: CaptureStream,
    payload: dict[object, object],
) -> str | None:
    if stream is CaptureStream.AGG_TRADE:
        return _required_sequence(payload, "a")
    if stream is CaptureStream.BOOK_TICKER:
        return _required_sequence(payload, "u")
    if stream is CaptureStream.KLINE_1M:
        kline = payload.get("k")
        if not isinstance(kline, dict):
            raise BinancePayloadError("kline payload is missing")
        return _required_sequence(kline, "t")
    return None


def _required_sequence(payload: dict[object, object], key: str) -> str:
    value = payload.get(key)
    if isinstance(value, int | str):
        return str(value)
    raise BinancePayloadError(f"payload sequence {key} is missing")


def _utc_from_ms(milliseconds: int) -> datetime:
    return datetime.fromtimestamp(milliseconds / 1000, tz=UTC)


async def _cancel_and_drain_tasks(
    tasks: tuple[asyncio.Future[Any], ...],
) -> None:
    for task in tasks:
        if not task.done():
            task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


def should_replace_connection(
    *,
    opened_at: float,
    now: float,
    lifetime_seconds: float,
) -> bool:
    return now - opened_at >= lifetime_seconds
