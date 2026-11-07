"""Execution-critical stream of immutable Binance 15-minute candles.

The normal market-state capture path is optimized for all-symbol aggregation
and durable storage.  It is intentionally not used for candle exits.  This
module owns the smaller execution seam: subscribe to the symbols that may
have positions, discard open-kline updates, deduplicate final candles, and
and deliver final candles to the exit policy.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation

import structlog

from crypto_momentum_lab.domain.market.closed_candle import ClosedCandle15m
from crypto_momentum_lab.domain.market.models import (
    CaptureRoute,
    CaptureStream,
    ConnectionLifecycleEvent,
    RawEnvelope,
)
from crypto_momentum_lab.market_data.binance.websocket import (
    BinanceWebSocketConnection,
)

log = structlog.get_logger()

_CANDLE_INTERVAL = timedelta(minutes=15)


class ClosedCandleFeedError(RuntimeError):
    """Base error for the execution candle feed."""


class ClosedCandleFeedOverflow(ClosedCandleFeedError):
    """The consumer did not drain the bounded final-candle queue."""


@dataclass(frozen=True, slots=True)
class ClosedCandle15mEvent:
    candle: ClosedCandle15m
    exchange_event_at: datetime
    received_at: datetime

    def __post_init__(self) -> None:
        for value, field_name in (
            (self.exchange_event_at, "exchange_event_at"),
            (self.received_at, "received_at"),
        ):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"{field_name} must be timezone-aware")


@dataclass(frozen=True, slots=True)
class ClosedCandle15mFeedConfig:
    websocket_url: str
    environment: str = "live"
    consumer_id: str = "live-exit-candles"
    reconnect_delays: tuple[float, ...] = (0.0, 1.0, 5.0, 15.0)
    connection_lifetime_seconds: float = 23 * 60 * 60
    open_timeout_seconds: float = 10.0
    ping_interval_seconds: float = 20.0
    ping_timeout_seconds: float = 20.0
    control_ack_timeout_seconds: float = 10.0
    ingress_queue_max_events: int = 4096
    final_event_queue_size: int = 2048

    def __post_init__(self) -> None:
        for value, field_name in (
            (self.websocket_url, "websocket_url"),
            (self.environment, "environment"),
            (self.consumer_id, "consumer_id"),
        ):
            if not value.strip():
                raise ValueError(f"{field_name} must not be empty")
        if not self.reconnect_delays or any(
            delay < 0 for delay in self.reconnect_delays
        ):
            raise ValueError("reconnect_delays must contain non-negative values")
        for numeric_value, field_name in (
            (self.connection_lifetime_seconds, "connection_lifetime_seconds"),
            (self.open_timeout_seconds, "open_timeout_seconds"),
            (self.ping_interval_seconds, "ping_interval_seconds"),
            (self.ping_timeout_seconds, "ping_timeout_seconds"),
            (self.control_ack_timeout_seconds, "control_ack_timeout_seconds"),
        ):
            if numeric_value <= 0:
                raise ValueError(f"{field_name} must be positive")
        for queue_size, field_name in (
            (self.ingress_queue_max_events, "ingress_queue_max_events"),
            (self.final_event_queue_size, "final_event_queue_size"),
        ):
            if queue_size <= 0:
                raise ValueError(f"{field_name} must be positive")


def decode_closed_candle_event(
    envelope: RawEnvelope,
) -> ClosedCandle15mEvent | None:
    """Convert one Binance envelope into a final 15m candle, if applicable."""

    if envelope.stream is not CaptureStream.KLINE_15M:
        return None
    payload = envelope.raw_payload
    if not isinstance(payload, dict):
        raise ClosedCandleFeedError("kline payload must be an object")
    raw_kline = payload.get("k")
    if not isinstance(raw_kline, dict):
        raise ClosedCandleFeedError("kline payload is missing k")
    interval = raw_kline.get("i")
    if interval != "15m":
        raise ClosedCandleFeedError("kline interval is not 15m")
    if raw_kline.get("x") is not True:
        return None

    symbol = envelope.symbol or raw_kline.get("s")
    if not isinstance(symbol, str) or not symbol.strip():
        raise ClosedCandleFeedError("closed kline symbol is missing")
    open_ms = _required_int(raw_kline, "t")
    close_ms = _required_int(raw_kline, "T")
    if close_ms < open_ms:
        raise ClosedCandleFeedError("closed kline timestamps are inverted")
    candle_start = _from_milliseconds(open_ms)
    candle_end = _from_milliseconds(close_ms + 1)
    if candle_end - candle_start != _CANDLE_INTERVAL:
        raise ClosedCandleFeedError("closed kline interval is not 15 minutes")
    event_at = envelope.exchange_event_at
    if event_at is None:
        raise ClosedCandleFeedError("closed kline exchange event time is missing")
    return ClosedCandle15mEvent(
        candle=ClosedCandle15m(
            symbol=symbol.strip().upper(),
            candle_start=candle_start,
            candle_end=candle_end,
            open_price=_required_decimal(raw_kline, "o"),
            close_price=_required_decimal(raw_kline, "c"),
        ),
        exchange_event_at=event_at,
        received_at=envelope.received_at,
    )


class BinanceClosedCandle15mFeed:
    """Low-volume, dynamically subscribed final-candle feed.

    The public interface intentionally exposes only symbol reconciliation,
    and event iteration. WebSocket reconnects, control
    ACKs, duplicate suppression, and bounded buffering stay behind this seam.
    """

    def __init__(
        self,
        *,
        config: ClosedCandle15mFeedConfig,
    ) -> None:
        self._config = config
        self._symbols: frozenset[str] = frozenset()
        self._generation = 0
        self._connection: BinanceWebSocketConnection | None = None
        self._events: asyncio.Queue[ClosedCandle15mEvent] = asyncio.Queue(
            maxsize=config.final_event_queue_size
        )
        self._stopping = False
        self._seen_keys: set[tuple[str, datetime]] = set()
        self._seen_order: deque[tuple[str, datetime]] = deque()
        self._started = False
        self.connection_opened = asyncio.Event()

    @property
    def symbols(self) -> frozenset[str]:
        return self._symbols

    @property
    def connection_available(self) -> bool:
        """No exposure needs a stream; required subscriptions must be ACKed."""
        return not self._symbols or (
            self._connection is not None and self._connection.metrics_snapshot().ready
        )

    async def start(self) -> None:
        if self._started:
            return
        self._stopping = False
        self._started = True
        connection = BinanceWebSocketConnection(
            base_url=self._config.websocket_url,
            group_id=self._config.consumer_id,
            route=CaptureRoute.MARKET,
            environment=self._config.environment,
            desired_names=self._stream_names(self._symbols),
            stream=CaptureStream.KLINE_15M,
            generation=max(1, self._generation),
            on_envelope=self._discard_envelope,
            on_lifecycle=self._observe_lifecycle,
            reconnect_delays=self._config.reconnect_delays,
            connection_lifetime_seconds=self._config.connection_lifetime_seconds,
            open_timeout_seconds=self._config.open_timeout_seconds,
            ping_interval_seconds=self._config.ping_interval_seconds,
            ping_timeout_seconds=self._config.ping_timeout_seconds,
            silence_timeout_seconds=self._config.connection_lifetime_seconds,
            control_ack_timeout_seconds=self._config.control_ack_timeout_seconds,
            ingress_queue_max_events=self._config.ingress_queue_max_events,
            symbol_filter=self._symbol_filter,
            on_realtime_envelope=self._receive_envelope,
        )
        self._connection = connection
        await connection.start()

    async def stop(self) -> None:
        self._stopping = True
        connection = self._connection
        self._connection = None
        if connection is not None:
            await connection.stop()
        self._started = False

    def set_symbols(self, symbols: frozenset[str]) -> None:
        """Update the local subscription target without waiting for control I/O."""
        normalized = frozenset(
            symbol.strip().upper() for symbol in symbols if symbol.strip()
        )
        previous = self._symbols
        if normalized == previous:
            return
        self._symbols = normalized
        self._generation += 1
        connection = self._connection
        if connection is None:
            return
        connection.set_desired_subscriptions(
            self._stream_names(normalized), generation=max(1, self._generation)
        )

    def has_received(self, symbol: str, candle_start: datetime) -> bool:
        return (symbol, candle_start) in self._seen_keys

    def __aiter__(self) -> AsyncIterator[ClosedCandle15mEvent]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[ClosedCandle15mEvent]:
        await self.start()
        while not self._stopping:
            yield await self._events.get()

    async def _receive_envelope(self, envelope: RawEnvelope) -> None:
        try:
            event = decode_closed_candle_event(envelope)
        except ClosedCandleFeedError as error:
            log.warning(
                "live_closed_candle_payload_rejected",
                symbol=envelope.symbol,
                reason=str(error),
            )
            return
        if event is not None:
            await self._publish(event)

    async def _publish(self, event: ClosedCandle15mEvent) -> bool:
        key = (event.candle.symbol, event.candle.candle_start)
        if key in self._seen_keys:
            return False
        self._seen_keys.add(key)
        self._seen_order.append(key)
        while len(self._seen_order) > self._config.final_event_queue_size * 32:
            self._seen_keys.discard(self._seen_order.popleft())
        try:
            self._events.put_nowait(event)
        except asyncio.QueueFull as error:
            self._seen_keys.discard(key)
            if self._seen_order and self._seen_order[-1] == key:
                self._seen_order.pop()
            else:
                self._seen_order.remove(key)
            raise ClosedCandleFeedOverflow(
                "closed candle final-event queue is full"
            ) from error
        return True

    async def _discard_envelope(self, envelope: RawEnvelope) -> None:
        del envelope

    async def _observe_lifecycle(self, event: ConnectionLifecycleEvent) -> None:
        if event.opened:
            self.connection_opened.set()
        log.info(
            "live_closed_candle_feed_connection",
            consumer_id=self._config.consumer_id,
            opened=event.opened,
            reason=event.reason,
        )

    def _symbol_filter(self, stream: CaptureStream, symbol: str) -> bool:
        return stream is CaptureStream.KLINE_15M and symbol.upper() in self._symbols

    @staticmethod
    def _stream_names(symbols: frozenset[str]) -> tuple[str, ...]:
        return tuple(sorted(f"{symbol.lower()}@kline_15m" for symbol in symbols))


def _required_int(payload: Mapping[str, object], name: str) -> int:
    value = payload.get(name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ClosedCandleFeedError(f"kline field {name} must be an integer")
    return value


def _required_decimal(payload: Mapping[str, object], name: str) -> Decimal:
    value = payload.get(name)
    if isinstance(value, bool) or not isinstance(value, str | int | float):
        raise ClosedCandleFeedError(f"kline field {name} must be numeric")
    try:
        return Decimal(str(value))
    except InvalidOperation as error:
        raise ClosedCandleFeedError(f"kline field {name} is not a decimal") from error


def _from_milliseconds(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1000, tz=UTC)
