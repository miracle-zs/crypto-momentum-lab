"""Latest-value fan-out for realtime executable quotes.

The quote hub is deliberately separate from the closed-state hub.  Quotes are
not replayed as an ordered audit log: a reconnecting consumer only needs the
latest bid/ask for each symbol and then the next live update.  Durable event
ordering remains the responsibility of the capture and runtime-state paths.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import cast

import structlog
from websockets.asyncio.client import ClientConnection, connect
from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.exceptions import ConnectionClosed

from crypto_momentum_lab.domain.market.models import RealtimeMarketQuote
from crypto_momentum_lab.market_data.protocol_parsing import (
    require_datetime,
    require_string,
)
from crypto_momentum_lab.market_data.quote_volume import QuoteVolume24hSnapshot

log = structlog.get_logger()

_SCHEMA_VERSION = 1
_SUBSCRIBE_MESSAGE = "subscribe_market_quotes"
_SUBSCRIBE_VOLUME_MESSAGE = "subscribe_market_quote_volumes"
_READY_MESSAGE = "market_quote_hub_ready"
_VOLUME_READY_MESSAGE = "market_quote_volume_hub_ready"
_QUOTE_MESSAGE = "market_quote"
_VOLUME_MESSAGE = "market_quote_volume"


class MarketQuoteHubError(RuntimeError):
    """Base error for the realtime quote hub."""


class MarketQuoteHubProtocolError(MarketQuoteHubError):
    """Raised when a quote hub message is malformed."""


@dataclass(frozen=True, slots=True)
class MarketQuoteHubConfig:
    host: str = "0.0.0.0"
    port: int = 8768
    subscriber_queue_size: int = 256
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
    kind: str
    queue: asyncio.Queue[str]
    writer_task: asyncio.Task[None] | None = None


class MarketQuoteHub:
    """Fan out the newest quote without coupling consumers to persistence."""

    def __init__(self, config: MarketQuoteHubConfig | None = None) -> None:
        self._config = config or MarketQuoteHubConfig()
        self._server: Server | None = None
        self._bound_host: str | None = None
        self._bound_port: int | None = None
        self._subscribers: dict[int, _Subscriber] = {}
        self._subscriber_lock = asyncio.Lock()
        self._latest: dict[tuple[str, str], RealtimeMarketQuote] = {}
        self._latest_volumes: dict[tuple[str, str], QuoteVolume24hSnapshot] = {}
        self._published_quote_count = 0
        self._published_volume_count = 0
        self._dropped_quote_count = 0

    @property
    def url(self) -> str:
        if self._bound_host is None or self._bound_port is None:
            raise RuntimeError("market quote hub is not started")
        host = "127.0.0.1" if self._bound_host in {"0.0.0.0", ""} else self._bound_host
        return f"ws://{host}:{self._bound_port}"

    async def start(self) -> None:
        if self._server is not None:
            return
        self._server = await serve(
            self._handle_connection,
            self._config.host,
            self._config.port,
            ping_interval=20,
            ping_timeout=20,
            max_size=1024 * 1024,
        )
        sockets = self._server.sockets
        if not sockets:
            raise RuntimeError("market quote hub did not bind a socket")
        address = sockets[0].getsockname()
        self._bound_host = str(address[0])
        self._bound_port = int(address[1])
        log.info(
            "market_quote_hub_started",
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
            if subscriber.writer_task is not None and not subscriber.writer_task.done():
                subscriber.writer_task.cancel()
        writer_tasks = tuple(
            subscriber.writer_task
            for subscriber in subscribers
            if subscriber.writer_task is not None
        )
        if writer_tasks:
            await asyncio.gather(*writer_tasks, return_exceptions=True)
        self._bound_host = None
        self._bound_port = None

    async def publish(
        self,
        quote: RealtimeMarketQuote | tuple[RealtimeMarketQuote, ...],
    ) -> None:
        quotes = (quote,) if isinstance(quote, RealtimeMarketQuote) else quote
        for item in quotes:
            self._latest[(item.environment, item.symbol)] = item
            self._published_quote_count += 1
            message = encode_market_quote(item)
            async with self._subscriber_lock:
                subscribers = tuple(
                    subscriber
                    for subscriber in self._subscribers.values()
                    if subscriber.environment == item.environment
                )
            for subscriber in subscribers:
                if subscriber.kind == "quote":
                    self._enqueue_latest(subscriber, message)

    async def publish_volume(
        self,
        snapshot: QuoteVolume24hSnapshot | tuple[QuoteVolume24hSnapshot, ...],
    ) -> None:
        snapshots = (
            (snapshot,) if isinstance(snapshot, QuoteVolume24hSnapshot) else snapshot
        )
        for item in snapshots:
            self._latest_volumes[(item.environment, item.symbol)] = item
            self._published_volume_count += 1
            message = encode_quote_volume(item)
            async with self._subscriber_lock:
                subscribers = tuple(
                    subscriber
                    for subscriber in self._subscribers.values()
                    if (
                        subscriber.kind == "volume"
                        and subscriber.environment == item.environment
                    )
                )
            for subscriber in subscribers:
                self._enqueue_latest(subscriber, message)

    def metrics_snapshot(self) -> dict[str, int]:
        return {
            "connected_subscriber_count": len(self._subscribers),
            "published_quote_count": self._published_quote_count,
            "published_volume_count": self._published_volume_count,
            "dropped_quote_count": self._dropped_quote_count,
            "latest_quote_count": len(self._latest),
            "latest_volume_count": len(self._latest_volumes),
        }

    def _enqueue_latest(self, subscriber: _Subscriber, message: str) -> None:
        if subscriber.queue.full():
            self._dropped_quote_count += 1
            while True:
                try:
                    subscriber.queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
        try:
            subscriber.queue.put_nowait(message)
        except asyncio.QueueFull:
            self._dropped_quote_count += 1

    async def _handle_connection(self, connection: ServerConnection) -> None:
        subscriber: _Subscriber | None = None
        try:
            raw_message = await asyncio.wait_for(
                connection.recv(),
                timeout=self._config.handshake_timeout_seconds,
            )
            request = _decode_object(raw_message)
            request_type = request.get("type")
            if request_type == _SUBSCRIBE_MESSAGE:
                kind = "quote"
                ready_message = _READY_MESSAGE
            elif request_type == _SUBSCRIBE_VOLUME_MESSAGE:
                kind = "volume"
                ready_message = _VOLUME_READY_MESSAGE
            else:
                raise MarketQuoteHubProtocolError("invalid subscription message")
            environment = _require_string(request, "environment")
            consumer_id = _require_string(request, "consumer_id")
            async with self._subscriber_lock:
                if kind == "quote":
                    quote_snapshot = tuple(
                        quote
                        for (quote_environment, _symbol), quote in sorted(
                            self._latest.items(),
                            key=lambda item: item[0][1],
                        )
                        if quote_environment == environment
                    )
                    encoded_snapshot = tuple(
                        encode_market_quote(quote) for quote in quote_snapshot
                    )
                    snapshot_size = len(quote_snapshot)
                else:
                    volume_snapshot = tuple(
                        volume
                        for (volume_environment, _symbol), volume in sorted(
                            self._latest_volumes.items(),
                            key=lambda item: item[0][1],
                        )
                        if volume_environment == environment
                    )
                    encoded_snapshot = tuple(
                        encode_quote_volume(volume) for volume in volume_snapshot
                    )
                    snapshot_size = len(volume_snapshot)
                queue: asyncio.Queue[str] = asyncio.Queue(
                    maxsize=max(
                        self._config.subscriber_queue_size,
                        snapshot_size + 1,
                    )
                )
                for encoded_item in encoded_snapshot:
                    queue.put_nowait(encoded_item)
                subscriber = _Subscriber(
                    connection=connection,
                    environment=environment,
                    kind=kind,
                    queue=queue,
                )
                self._subscribers[id(connection)] = subscriber
            # Register the subscriber before the handshake completes so a
            # quote published during the network send is queued after the
            # initial snapshot.  The send itself stays outside the global
            # subscriber lock; a slow client must not pause the Binance reader.
            await connection.send(
                json.dumps(
                    {
                        "type": ready_message,
                        "schema_version": _SCHEMA_VERSION,
                        "environment": environment,
                        "snapshot_count": snapshot_size,
                    },
                    separators=(",", ":"),
                )
            )
            subscriber.writer_task = asyncio.create_task(
                self._write_messages(connection, queue)
            )
            log.info(
                "market_quote_hub_subscriber_connected",
                consumer_id=consumer_id,
                environment=environment,
            )
            await connection.wait_closed()
        except (ConnectionClosed, TimeoutError):
            return
        except MarketQuoteHubProtocolError as error:
            await connection.close(code=1008, reason=str(error))
        except Exception as error:
            log.exception("market_quote_hub_connection_failed", error=str(error))
        finally:
            if subscriber is not None:
                async with self._subscriber_lock:
                    self._subscribers.pop(id(connection), None)
                if (
                    subscriber.writer_task is not None
                    and not subscriber.writer_task.done()
                ):
                    subscriber.writer_task.cancel()
                if subscriber.writer_task is not None:
                    await asyncio.gather(
                        subscriber.writer_task,
                        return_exceptions=True,
                    )
            log.info("market_quote_hub_subscriber_disconnected")

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


class WebSocketMarketQuoteSource:
    """Async iterator with reconnect and latest-snapshot semantics."""

    def __init__(
        self,
        *,
        url: str,
        environment: str,
        consumer_id: str,
        config: MarketQuoteHubConfig | None = None,
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
        self._config = config or MarketQuoteHubConfig()
        self._on_connection_change = on_connection_change
        self._connection_available: bool | None = None
        self._stopping = False

    def stop(self) -> None:
        self._stopping = True

    def __aiter__(self) -> AsyncIterator[RealtimeMarketQuote]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[RealtimeMarketQuote]:
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
                    max_size=1024 * 1024,
                    max_queue=64,
                    proxy=None,
                ) as connection:
                    await connection.send(
                        json.dumps(
                            {
                                "type": _SUBSCRIBE_MESSAGE,
                                "schema_version": _SCHEMA_VERSION,
                                "environment": self._environment,
                                "consumer_id": self._consumer_id,
                            },
                            separators=(",", ":"),
                        )
                    )
                    ready = _decode_object(await connection.recv())
                    if ready.get("type") != _READY_MESSAGE:
                        raise MarketQuoteHubProtocolError(
                            "market quote hub did not acknowledge subscription"
                        )
                    if ready.get("environment") != self._environment:
                        raise MarketQuoteHubProtocolError(
                            "market quote hub environment mismatch"
                        )
                    self._notify_connection_change(True, None)
                    unavailable_since = time.monotonic()
                    reconnect_attempt = 0
                    latest_quotes: dict[str, RealtimeMarketQuote] = {}
                    quote_available = asyncio.Event()
                    reader_error: list[Exception | None] = [None]

                    reader_task = asyncio.create_task(
                        self._read_market_quotes(
                            connection,
                            latest_quotes,
                            quote_available,
                            reader_error,
                        ),
                        name=f"market-quote-reader:{self._consumer_id}",
                    )
                    try:
                        while not self._stopping:
                            while latest_quotes:
                                symbol = next(iter(latest_quotes))
                                yield latest_quotes.pop(symbol)
                            if reader_error[0] is not None:
                                raise reader_error[0]
                            quote_available.clear()
                            await quote_available.wait()
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
                MarketQuoteHubError,
            ) as error:
                self._notify_connection_change(
                    False,
                    f"{type(error).__name__}: {error}",
                )
                if (
                    time.monotonic() - unavailable_since
                    >= self._config.unavailable_timeout_seconds
                ):
                    raise MarketQuoteHubError(
                        "market quote hub unavailable for "
                        f"{self._config.unavailable_timeout_seconds:.1f} seconds"
                    ) from error
                delay = self._config.reconnect_delays[
                    min(reconnect_attempt, len(self._config.reconnect_delays) - 1)
                ]
                reconnect_attempt += 1
                if delay > 0:
                    await asyncio.sleep(delay)

    async def _read_market_quotes(
        self,
        connection: ClientConnection,
        latest_quotes: dict[str, RealtimeMarketQuote],
        quote_available: asyncio.Event,
        reader_error: list[Exception | None],
    ) -> None:
        try:
            while True:
                message = _decode_object(await connection.recv())
                message_type = message.get("type")
                if message_type != _QUOTE_MESSAGE:
                    raise MarketQuoteHubProtocolError(
                        "unexpected market quote message type"
                    )
                quote = decode_market_quote(
                    message,
                    expected_environment=self._environment,
                )
                # Quote delivery is latest-value by design. Keep one pending
                # value per symbol so a slow exit check cannot stop the socket
                # reader.
                latest_quotes[quote.symbol] = quote
                quote_available.set()
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            reader_error[0] = error
            quote_available.set()

    def _notify_connection_change(
        self,
        available: bool,
        reason: str | None,
    ) -> None:
        if self._connection_available == available:
            return
        self._connection_available = available
        log.warning(
            "market_quote_hub_connection_state_changed",
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
                "market_quote_hub_connection_callback_failed",
                available=available,
                environment=self._environment,
                consumer_id=self._consumer_id,
            )


class WebSocketMarketQuoteVolumeSource:
    """Async iterator for latest-value 24-hour volume snapshots."""

    def __init__(
        self,
        *,
        url: str,
        environment: str,
        consumer_id: str,
        config: MarketQuoteHubConfig | None = None,
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
        self._config = config or MarketQuoteHubConfig()
        self._stopping = False

    def stop(self) -> None:
        self._stopping = True

    def __aiter__(self) -> AsyncIterator[QuoteVolume24hSnapshot]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[QuoteVolume24hSnapshot]:
        unavailable_since = time.monotonic()
        reconnect_attempt = 0
        while not self._stopping:
            try:
                async with connect(
                    self._url,
                    open_timeout=self._config.handshake_timeout_seconds,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=1024 * 1024,
                    max_queue=64,
                    proxy=None,
                ) as connection:
                    await connection.send(
                        json.dumps(
                            {
                                "type": _SUBSCRIBE_VOLUME_MESSAGE,
                                "schema_version": _SCHEMA_VERSION,
                                "environment": self._environment,
                                "consumer_id": self._consumer_id,
                            },
                            separators=(",", ":"),
                        )
                    )
                    ready = _decode_object(await connection.recv())
                    if ready.get("type") != _VOLUME_READY_MESSAGE:
                        raise MarketQuoteHubProtocolError(
                            "market quote hub did not acknowledge volume subscription"
                        )
                    if ready.get("environment") != self._environment:
                        raise MarketQuoteHubProtocolError(
                            "market quote volume environment mismatch"
                        )
                    unavailable_since = time.monotonic()
                    reconnect_attempt = 0
                    latest: dict[str, QuoteVolume24hSnapshot] = {}
                    available = asyncio.Event()
                    reader_error: list[Exception | None] = [None]
                    reader_task = asyncio.create_task(
                        self._read_volume_snapshots(
                            connection,
                            latest,
                            available,
                            reader_error,
                        ),
                        name=f"market-quote-volume-reader:{self._consumer_id}",
                    )
                    try:
                        while not self._stopping:
                            while latest:
                                symbol = next(iter(latest))
                                yield latest.pop(symbol)
                            if reader_error[0] is not None:
                                raise reader_error[0]
                            available.clear()
                            await available.wait()
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
                MarketQuoteHubError,
            ) as error:
                if (
                    time.monotonic() - unavailable_since
                    >= self._config.unavailable_timeout_seconds
                ):
                    raise MarketQuoteHubError(
                        "market quote volume hub unavailable for "
                        f"{self._config.unavailable_timeout_seconds:.1f} seconds"
                    ) from error
                delay = self._config.reconnect_delays[
                    min(reconnect_attempt, len(self._config.reconnect_delays) - 1)
                ]
                reconnect_attempt += 1
                if delay > 0:
                    await asyncio.sleep(delay)

    async def _read_volume_snapshots(
        self,
        connection: ClientConnection,
        latest: dict[str, QuoteVolume24hSnapshot],
        available: asyncio.Event,
        reader_error: list[Exception | None],
    ) -> None:
        try:
            while True:
                snapshot = decode_quote_volume(
                    await connection.recv(),
                    expected_environment=self._environment,
                )
                latest[snapshot.symbol] = snapshot
                available.set()
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            reader_error[0] = error
            available.set()


def encode_market_quote(quote: RealtimeMarketQuote) -> str:
    return json.dumps(
        {
            "type": _QUOTE_MESSAGE,
            "schema_version": _SCHEMA_VERSION,
            "exchange": quote.exchange,
            "environment": quote.environment,
            "symbol": quote.symbol,
            "event_at": quote.event_at.isoformat(),
            "received_at": quote.received_at.isoformat(),
            "bid_price": str(quote.bid_price),
            "ask_price": str(quote.ask_price),
        },
        separators=(",", ":"),
    )


def decode_market_quote(
    payload: dict[str, object] | str | bytes,
    *,
    expected_environment: str | None = None,
) -> RealtimeMarketQuote:
    message = _decode_object(payload)
    if message.get("type") != _QUOTE_MESSAGE:
        raise MarketQuoteHubProtocolError("unexpected market quote message type")
    if message.get("schema_version") != _SCHEMA_VERSION:
        raise MarketQuoteHubProtocolError("unsupported market quote schema")
    environment = _require_string(message, "environment")
    if expected_environment is not None and environment != expected_environment:
        raise MarketQuoteHubProtocolError("market quote environment mismatch")
    return RealtimeMarketQuote(
        exchange=_require_string(message, "exchange"),
        environment=environment,
        symbol=_require_string(message, "symbol"),
        event_at=_require_datetime(message, "event_at"),
        received_at=_require_datetime(message, "received_at"),
        bid_price=_require_decimal(message, "bid_price"),
        ask_price=_require_decimal(message, "ask_price"),
    )


def encode_quote_volume(snapshot: QuoteVolume24hSnapshot) -> str:
    return json.dumps(
        {
            "type": _VOLUME_MESSAGE,
            "schema_version": _SCHEMA_VERSION,
            "exchange": snapshot.exchange,
            "environment": snapshot.environment,
            "symbol": snapshot.symbol,
            "quote_volume": str(snapshot.quote_volume),
            "source_at": snapshot.source_at.isoformat(),
            "fetched_at": snapshot.fetched_at.isoformat(),
            "quote_asset": snapshot.quote_asset,
            "source": snapshot.source,
        },
        separators=(",", ":"),
    )


def decode_quote_volume(
    payload: dict[str, object] | str | bytes,
    *,
    expected_environment: str | None = None,
) -> QuoteVolume24hSnapshot:
    message = _decode_object(payload)
    if message.get("type") != _VOLUME_MESSAGE:
        raise MarketQuoteHubProtocolError("unexpected market quote volume message type")
    if message.get("schema_version") != _SCHEMA_VERSION:
        raise MarketQuoteHubProtocolError("unsupported market quote volume schema")
    environment = _require_string(message, "environment")
    if expected_environment is not None and environment != expected_environment:
        raise MarketQuoteHubProtocolError("market quote volume environment mismatch")
    return QuoteVolume24hSnapshot(
        exchange=_require_string(message, "exchange"),
        environment=environment,
        symbol=_require_string(message, "symbol"),
        quote_volume=_require_decimal(message, "quote_volume"),
        source_at=_require_datetime(message, "source_at"),
        fetched_at=_require_datetime(message, "fetched_at"),
        quote_asset=_require_string(message, "quote_asset"),
        source=_require_string(message, "source"),
    )


def _decode_object(raw_message: str | bytes | dict[str, object]) -> dict[str, object]:
    if isinstance(raw_message, dict):
        return raw_message
    if isinstance(raw_message, bytes):
        raw_message = raw_message.decode("utf-8")
    if not isinstance(raw_message, str):
        raise MarketQuoteHubProtocolError("hub message must be text")
    try:
        payload = json.loads(raw_message)
    except json.JSONDecodeError as error:
        raise MarketQuoteHubProtocolError("hub message is not valid JSON") from error
    if not isinstance(payload, dict):
        raise MarketQuoteHubProtocolError("hub message must be an object")
    return cast(dict[str, object], payload)


def _require_string(payload: dict[str, object], name: str) -> str:
    return require_string(payload, name, error=MarketQuoteHubProtocolError)


def _require_decimal(payload: dict[str, object], name: str) -> Decimal:
    value = payload.get(name)
    if not isinstance(value, str):
        raise MarketQuoteHubProtocolError(f"{name} must be a decimal string")
    try:
        return Decimal(value)
    except ArithmeticError as error:
        raise MarketQuoteHubProtocolError(f"{name} must be a decimal") from error


def _require_datetime(payload: dict[str, object], name: str) -> datetime:
    return require_datetime(
        payload,
        name,
        error=MarketQuoteHubProtocolError,
    )
