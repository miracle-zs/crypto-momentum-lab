import asyncio
import hashlib
import json
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Protocol
from urllib.parse import quote

import structlog
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

from crypto_momentum_lab.domain.market.models import JsonValue

DEFAULT_BINANCE_USDM_USER_DATA_WEBSOCKET_URL = (
    "wss://fstream.binance.com/private/ws"
)


class BinancePayloadError(ValueError):
    """A Binance user-data payload that cannot be safely interpreted."""


class BinanceUserDataListenKeyClient(Protocol):
    async def start_user_data_stream(self) -> str:
        pass

    async def keepalive_user_data_stream(self, listen_key: str) -> None:
        pass

    async def close_user_data_stream(self, listen_key: str) -> None:
        pass


@dataclass(frozen=True, slots=True)
class BinanceUserDataEvent:
    event_type: str
    event_at: datetime
    received_at: datetime
    payload: dict[str, JsonValue]
    event_id: str

    def __post_init__(self) -> None:
        if not self.event_type.strip():
            raise ValueError("event_type must not be empty")
        if self.event_at.tzinfo is None or self.event_at.utcoffset() is None:
            raise ValueError("event_at must be timezone-aware")
        if self.received_at.tzinfo is None or self.received_at.utcoffset() is None:
            raise ValueError("received_at must be timezone-aware")
        if len(self.event_id) != 64:
            raise ValueError("event_id must be a SHA-256 hex digest")


@dataclass(frozen=True, slots=True)
class BinanceUserDataStreamMetrics:
    connection_count: int
    received_message_count: int
    parsed_event_count: int
    malformed_message_count: int
    fill_event_count: int
    last_connected_at: datetime | None
    last_received_at: datetime | None
    last_event_received_at: datetime | None
    last_event_at: datetime | None
    last_event_type: str | None


def parse_user_data_event(
    message: str | bytes | Mapping[str, object],
    *,
    received_at: datetime | None = None,
) -> BinanceUserDataEvent:
    """Parse a direct or combined Binance account-event message.

    User data streams normally send the event object directly. Accepting the
    combined-stream wrapper here is harmless and makes the parser resilient to
    an accidentally configured ``/stream`` endpoint.
    """
    payload = _decode_mapping(message)
    if "data" in payload and "stream" in payload:
        payload = _require_mapping(payload["data"])
    normalized = _json_mapping(payload)
    event_type = normalized.get("e")
    if not isinstance(event_type, str) or not event_type.strip():
        raise BinancePayloadError("user data event is missing string field e")
    if event_type == "ACCOUNT_UPDATE" and not isinstance(
        normalized.get("a"),
        dict,
    ):
        raise BinancePayloadError("ACCOUNT_UPDATE is missing object field a")
    if event_type == "ORDER_TRADE_UPDATE" and not isinstance(
        normalized.get("o"),
        dict,
    ):
        raise BinancePayloadError("ORDER_TRADE_UPDATE is missing object field o")
    event_timestamp = normalized.get("E")
    if isinstance(event_timestamp, bool) or not isinstance(
        event_timestamp,
        int | float | str,
    ):
        raise BinancePayloadError("user data event is missing numeric field E")
    try:
        event_at = datetime.fromtimestamp(
            float(str(event_timestamp)) / 1000,
            tz=UTC,
        )
    except (TypeError, ValueError, OverflowError, OSError) as error:
        raise BinancePayloadError("user data event has invalid timestamp E") from error
    resolved_received_at = received_at or datetime.now(tz=UTC)
    if (
        resolved_received_at.tzinfo is None
        or resolved_received_at.utcoffset() is None
    ):
        raise ValueError("received_at must be timezone-aware")
    event_id = hashlib.sha256(
        json.dumps(
            normalized,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    ).hexdigest()
    return BinanceUserDataEvent(
        event_type=event_type,
        event_at=event_at,
        received_at=resolved_received_at,
        payload=normalized,
        event_id=event_id,
    )


type UserDataEventSink = Callable[[BinanceUserDataEvent], Awaitable[None]]

log = structlog.get_logger(__name__)


class BinanceUsdMUserDataStream:
    """Reconnectable USD-M account-event stream backed by a listen key."""

    def __init__(
        self,
        *,
        listen_key_client: BinanceUserDataListenKeyClient,
        on_event: UserDataEventSink | None = None,
        websocket_url: str = DEFAULT_BINANCE_USDM_USER_DATA_WEBSOCKET_URL,
        keepalive_interval_seconds: float = 30 * 60,
        reconnect_delays: tuple[float, ...] = (1.0, 2.0, 5.0, 15.0, 30.0),
        open_timeout_seconds: float = 15.0,
        ping_interval_seconds: float = 20.0,
        ping_timeout_seconds: float = 20.0,
    ) -> None:
        if not websocket_url.strip():
            raise ValueError("websocket_url must not be empty")
        if keepalive_interval_seconds <= 0:
            raise ValueError("keepalive_interval_seconds must be positive")
        if not reconnect_delays or any(delay < 0 for delay in reconnect_delays):
            raise ValueError("reconnect_delays must contain non-negative values")
        if open_timeout_seconds <= 0:
            raise ValueError("open_timeout_seconds must be positive")
        if ping_interval_seconds <= 0 or ping_timeout_seconds <= 0:
            raise ValueError("ping intervals must be positive")
        self._listen_key_client = listen_key_client
        self._on_event = on_event
        self._websocket_url = websocket_url.rstrip("/")
        self._keepalive_interval_seconds = keepalive_interval_seconds
        self._reconnect_delays = reconnect_delays
        self._open_timeout_seconds = open_timeout_seconds
        self._ping_interval_seconds = ping_interval_seconds
        self._ping_timeout_seconds = ping_timeout_seconds
        self._stopping = False
        self._connection: ClientConnection | None = None
        self._connection_lock = asyncio.Lock()
        self._connection_count = 0
        self._received_message_count = 0
        self._parsed_event_count = 0
        self._malformed_message_count = 0
        self._fill_event_count = 0
        self._last_connected_at: datetime | None = None
        self._last_received_at: datetime | None = None
        self._last_event_received_at: datetime | None = None
        self._last_event_at: datetime | None = None
        self._last_event_type: str | None = None

    @property
    def metrics(self) -> BinanceUserDataStreamMetrics:
        return BinanceUserDataStreamMetrics(
            connection_count=self._connection_count,
            received_message_count=self._received_message_count,
            parsed_event_count=self._parsed_event_count,
            malformed_message_count=self._malformed_message_count,
            fill_event_count=self._fill_event_count,
            last_connected_at=self._last_connected_at,
            last_received_at=self._last_received_at,
            last_event_received_at=self._last_event_received_at,
            last_event_at=self._last_event_at,
            last_event_type=self._last_event_type,
        )

    def set_handler(self, on_event: UserDataEventSink) -> None:
        self._on_event = on_event

    async def run(self) -> None:
        listen_key: str | None = None
        reconnect_attempt = 0
        try:
            while not self._stopping:
                reason = "closed"
                try:
                    if listen_key is None:
                        listen_key = (
                            await self._listen_key_client.start_user_data_stream()
                        )
                        if not listen_key.strip():
                            raise ValueError("Binance returned an empty listen key")
                        reconnect_attempt = 0
                        log.info("binance_user_data_listen_key_started")
                    reason = await self._run_connection(listen_key)
                    if reason in {"listen_key_expired", "keepalive_failed"}:
                        listen_key = None
                except (ConnectionClosed, TimeoutError, OSError) as error:
                    reason = error.__class__.__name__
                except Exception as error:
                    reason = error.__class__.__name__
                    log.exception(
                        "binance_user_data_stream_failed",
                        reason=reason,
                        error=str(error),
                    )
                    if reason in {"BinanceRateLimitError", "ValueError"}:
                        listen_key = None
                finally:
                    async with self._connection_lock:
                        self._connection = None

                if self._stopping:
                    break
                delay = self._reconnect_delays[
                    min(reconnect_attempt, len(self._reconnect_delays) - 1)
                ]
                reconnect_attempt += 1
                if delay > 0:
                    await asyncio.sleep(delay)
                log.warning(
                    "binance_user_data_stream_reconnecting",
                    reason=reason,
                    delay_seconds=delay,
                )
        finally:
            if listen_key is not None:
                try:
                    await self._listen_key_client.close_user_data_stream(listen_key)
                except Exception as error:
                    log.warning(
                        "binance_user_data_listen_key_close_failed",
                        reason=error.__class__.__name__,
                    )

    async def stop(self) -> None:
        self._stopping = True
        async with self._connection_lock:
            connection = self._connection
        if connection is not None:
            await connection.close()

    async def request_reconnect(self, reason: str) -> None:
        if not reason.strip():
            raise ValueError("reason must not be empty")
        async with self._connection_lock:
            connection = self._connection
        log.warning(
            "binance_user_data_stream_reconnect_requested",
            reason=reason,
            connection_present=connection is not None,
        )
        if connection is not None:
            await connection.close()

    async def _run_connection(self, listen_key: str) -> str:
        keepalive_failed = asyncio.Event()
        keepalive_task = asyncio.create_task(
            self._keepalive_loop(listen_key, keepalive_failed)
        )
        try:
            uri = f"{self._websocket_url}/{quote(listen_key, safe='')}"
            async with connect(
                uri,
                open_timeout=self._open_timeout_seconds,
                ping_interval=self._ping_interval_seconds,
                ping_timeout=self._ping_timeout_seconds,
                max_queue=64,
            ) as connection:
                async with self._connection_lock:
                    self._connection = connection
                    self._connection_count += 1
                    self._last_connected_at = datetime.now(tz=UTC)
                log.info("binance_user_data_stream_connected")
                while not self._stopping:
                    if keepalive_failed.is_set():
                        return "keepalive_failed"
                    try:
                        message = await asyncio.wait_for(connection.recv(), timeout=5)
                    except TimeoutError:
                        continue
                    received_at = datetime.now(tz=UTC)
                    self._received_message_count += 1
                    self._last_received_at = received_at
                    try:
                        event = parse_user_data_event(
                            message,
                            received_at=received_at,
                        )
                    except BinancePayloadError as error:
                        self._malformed_message_count += 1
                        log.warning(
                            "binance_user_data_malformed_event",
                            reason=str(error),
                        )
                        continue
                    self._parsed_event_count += 1
                    self._last_event_received_at = received_at
                    self._last_event_at = event.event_at
                    self._last_event_type = event.event_type
                    if _is_fill_event(event):
                        self._fill_event_count += 1
                    if self._on_event is not None:
                        try:
                            await self._on_event(event)
                        except Exception as error:
                            log.exception(
                                "binance_user_data_event_handler_failed",
                                event_type=event.event_type,
                                reason=error.__class__.__name__,
                                error=str(error),
                            )
                    if event.event_type == "listenKeyExpired":
                        return "listen_key_expired"
        finally:
            keepalive_task.cancel()
            try:
                await keepalive_task
            except asyncio.CancelledError:
                pass
            async with self._connection_lock:
                self._connection = None
        return "closed"

    async def _keepalive_loop(
        self,
        listen_key: str,
        failed: asyncio.Event,
    ) -> None:
        while not self._stopping:
            await asyncio.sleep(self._keepalive_interval_seconds)
            if self._stopping:
                return
            try:
                await self._listen_key_client.keepalive_user_data_stream(listen_key)
                log.debug("binance_user_data_listen_key_kept_alive")
            except Exception as error:
                failed.set()
                log.warning(
                    "binance_user_data_listen_key_keepalive_failed",
                    reason=error.__class__.__name__,
                    error=str(error),
                )
                return


def _decode_mapping(message: str | bytes | Mapping[str, object]) -> dict[str, object]:
    if isinstance(message, bytes):
        try:
            decoded: object = json.loads(message.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise BinancePayloadError("user data message is not valid JSON") from error
    elif isinstance(message, str):
        try:
            decoded = json.loads(message)
        except json.JSONDecodeError as error:
            raise BinancePayloadError("user data message is not valid JSON") from error
    else:
        decoded = message
    return _require_mapping(decoded)


def _require_mapping(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise BinancePayloadError("expected JSON object")
    return {str(key): item for key, item in value.items()}


def _json_mapping(value: Mapping[str, object]) -> dict[str, JsonValue]:
    return {str(key): _json_value(item) for key, item in value.items()}


def _json_value(value: object) -> JsonValue:
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_value(item) for item in value]
    raise BinancePayloadError(f"unsupported JSON value type: {type(value).__name__}")


def _is_fill_event(event: BinanceUserDataEvent) -> bool:
    if event.event_type != "ORDER_TRADE_UPDATE":
        return False
    order = event.payload.get("o")
    if not isinstance(order, dict) or order.get("x") != "TRADE":
        return False
    quantity = order.get("l")
    try:
        return Decimal(str(quantity)) != 0
    except (InvalidOperation, TypeError, ValueError):
        return False


__all__ = [
    "BinancePayloadError",
    "BinanceUsdMUserDataStream",
    "BinanceUserDataEvent",
    "BinanceUserDataListenKeyClient",
    "BinanceUserDataStreamMetrics",
    "parse_user_data_event",
]
