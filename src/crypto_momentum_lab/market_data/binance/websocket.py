import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import cast
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


class BinancePayloadError(ValueError):
    pass


type EnvelopeSink = Callable[[RawEnvelope], Awaitable[None]]
type LifecycleSink = Callable[[ConnectionLifecycleEvent], Awaitable[None]]

log = structlog.get_logger(__name__)


class BinanceWebSocketConnection:
    def __init__(
        self,
        *,
        base_url: str,
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
        control_messages_per_second: float = 5,
    ) -> None:
        self._base_url = base_url
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
        self._control_interval_seconds = 1 / control_messages_per_second
        self._send_lock = asyncio.Lock()
        self._connection_lock = asyncio.Lock()
        self._stopping = False
        self._connection: ClientConnection | None = None
        self._control_id = 0
        self._task: asyncio.Task[None] | None = None
        self._pending_acks: dict[int, asyncio.Future[None]] = {}

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self.run())

    async def run(self) -> None:
        reconnect_attempt = 0
        while not self._stopping:
            session_id = uuid4()
            reason = "stopped"
            try:
                reason = await self._run_once(session_id)
            except (
                CaptureQueueFull,
                ConnectionClosed,
                TimeoutError,
                OSError,
            ) as error:
                reason = error.__class__.__name__
            except Exception as error:
                reason = error.__class__.__name__
                log.exception(
                    "binance_websocket_session_failed",
                    route=self._route.value,
                    stream=self._stream.value,
                    symbols=self._desired_names,
                    reason=reason,
                    error=str(error),
                )
            finally:
                async with self._connection_lock:
                    self._connection = None
                    self._cancel_pending_acks()
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
        self._desired_names = tuple(sorted(set((*self._desired_names, *names))))
        self._generation = generation
        async with self._connection_lock:
            if self._connection is not None:
                await self._send_control(self._connection, "SUBSCRIBE", names)

    async def unsubscribe(
        self,
        names: tuple[str, ...],
        *,
        generation: int,
    ) -> None:
        remove = set(names)
        self._desired_names = tuple(
            name for name in self._desired_names if name not in remove
        )
        self._generation = generation
        async with self._connection_lock:
            if self._connection is not None:
                await self._send_control(self._connection, "UNSUBSCRIBE", names)

    async def _run_once(self, session_id: UUID) -> str:
        uri = self._base_url.rstrip("/")
        local_sequence = 0
        opened_at = time.monotonic()
        async with connect(
            uri,
            open_timeout=self._open_timeout_seconds,
            ping_interval=self._ping_interval_seconds,
            ping_timeout=self._ping_timeout_seconds,
            max_queue=16,
        ) as connection:
            async with self._connection_lock:
                self._connection = connection
            await self._emit_lifecycle(session_id, opened=True, reason=None)
            await self._send_control(
                connection,
                "SUBSCRIBE",
                self._desired_names,
                receive_ack_direct=True,
            )
            while not self._stopping:
                if should_replace_connection(
                    opened_at=opened_at,
                    now=time.monotonic(),
                    lifetime_seconds=self._connection_lifetime_seconds,
                ):
                    return "lifetime_expired"
                message = await asyncio.wait_for(
                    connection.recv(),
                    timeout=self._silence_timeout_seconds,
                )
                if self._resolve_control_ack(message):
                    continue
                received_at = datetime.now(UTC)
                received_monotonic_ns = time.monotonic_ns()
                try:
                    envelope = parse_binance_message(
                        route=self._route,
                        message=message,
                        environment=self._environment,
                        connection_session_id=session_id,
                        local_sequence=local_sequence + 1,
                        subscription_generation=self._generation,
                        received_at=received_at,
                        received_monotonic_ns=received_monotonic_ns,
                        expected_stream=self._stream,
                    )
                except BinancePayloadError:
                    continue
                local_sequence += 1
                await self._on_envelope(envelope)
        async with self._connection_lock:
            self._connection = None
        return "closed"

    async def _send_control(
        self,
        connection: ClientConnection,
        method: str,
        names: tuple[str, ...],
        *,
        receive_ack_direct: bool = False,
    ) -> None:
        if not names:
            return
        async with self._send_lock:
            await asyncio.sleep(self._control_interval_seconds)
            self._control_id += 1
            control_id = self._control_id
            future: asyncio.Future[None] | None = None
            if not receive_ack_direct:
                future = asyncio.get_running_loop().create_future()
                self._pending_acks[control_id] = future
            try:
                await connection.send(
                    json.dumps(
                        {
                            "method": method,
                            "params": list(names),
                            "id": control_id,
                        }
                    )
                )
                if receive_ack_direct:
                    await self._receive_ack(connection, control_id)
                elif future is not None:
                    await asyncio.wait_for(
                        future,
                        timeout=self._open_timeout_seconds,
                    )
            finally:
                if future is not None:
                    self._pending_acks.pop(control_id, None)

    def _cancel_pending_acks(self) -> None:
        pending = tuple(self._pending_acks.values())
        self._pending_acks.clear()
        for future in pending:
            if not future.done():
                future.cancel()

    async def _receive_ack(
        self,
        connection: ClientConnection,
        control_id: int,
    ) -> None:
        message = await connection.recv()
        decoded = json.loads(message)
        if not isinstance(decoded, dict) or decoded.get("id") != control_id:
            raise BinancePayloadError("control acknowledgement is invalid")

    def _resolve_control_ack(self, message: str | bytes) -> bool:
        try:
            decoded = json.loads(message)
        except json.JSONDecodeError:
            return False
        if not isinstance(decoded, dict) or "id" not in decoded:
            return False
        control_id = decoded.get("id")
        if not isinstance(control_id, int):
            return False
        future = self._pending_acks.pop(control_id, None)
        if future is not None and not future.done():
            future.set_result(None)
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
    decoded = json.loads(message)
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
    _, separator, suffix = stream_name.partition("@")
    if not separator:
        raise BinancePayloadError("stream name is invalid")
    try:
        return CaptureStream(suffix)
    except ValueError as exc:
        raise BinancePayloadError(f"unsupported stream: {suffix}") from exc


def _symbol_from_stream_name(stream_name: str) -> str:
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


def should_replace_connection(
    *,
    opened_at: float,
    now: float,
    lifetime_seconds: float,
) -> bool:
    return now - opened_at >= lifetime_seconds
