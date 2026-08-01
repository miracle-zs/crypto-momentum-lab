from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from uuid import UUID

type JsonScalar = str | int | float | bool | None
type JsonValue = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]


class CaptureRoute(StrEnum):
    MARKET = "market"
    PUBLIC = "public"


class CaptureStream(StrEnum):
    AGG_TRADE = "aggTrade"
    BOOK_TICKER = "bookTicker"
    FORCE_ORDER = "forceOrder"
    MARK_PRICE = "markPrice@1s"
    KLINE_1M = "kline_1m"


class MarketDataState(StrEnum):
    STARTING = "starting"
    SYNCING = "syncing"
    READY = "ready"
    DEGRADED = "degraded"
    HALTED = "halted"
    STOPPED = "stopped"


class QualityCategory(StrEnum):
    CONNECTION_OPENED = "connection_opened"
    CONNECTION_CLOSED = "connection_closed"
    RECONNECT_GAP = "reconnect_gap"
    ACK_FAILURE = "ack_failure"
    UNEXPECTED_STREAM = "unexpected_stream"
    DUPLICATE = "duplicate"
    SEQUENCE_GAP = "sequence_gap"
    EVENT_TIME_REGRESSION = "event_time_regression"
    SILENCE = "silence"
    MALFORMED_PAYLOAD = "malformed_payload"
    QUEUE_HIGH_WATERMARK = "queue_high_watermark"
    QUEUE_OVERFLOW = "queue_overflow"
    ARCHIVE_FAILURE = "archive_failure"
    MANIFEST_BACKLOG = "manifest_backlog"
    DISK_THRESHOLD = "disk_threshold"


class AggressorSide(StrEnum):
    BUY = "buy"
    SELL = "sell"


class OrderSide(StrEnum):
    BUY = "buy"
    SELL = "sell"


@dataclass(frozen=True, slots=True)
class ConnectionLifecycleEvent:
    session_id: UUID
    route: CaptureRoute
    stream: CaptureStream
    symbols: tuple[str, ...]
    occurred_at: datetime
    opened: bool
    reason: str | None

    def __post_init__(self) -> None:
        if not _is_aware(self.occurred_at):
            raise ValueError("occurred_at must be timezone-aware")
        if any(not symbol.strip() for symbol in self.symbols):
            raise ValueError("symbols must not contain empty values")


@dataclass(frozen=True, slots=True)
class RawEnvelope:
    schema_version: int
    exchange: str
    environment: str
    route: CaptureRoute
    stream: CaptureStream
    symbol: str | None
    exchange_event_at: datetime | None
    received_at: datetime
    received_monotonic_ns: int
    connection_session_id: UUID
    local_sequence: int
    exchange_sequence: str | None
    subscription_generation: int
    raw_payload: JsonValue

    def __post_init__(self) -> None:
        if not _is_aware(self.received_at):
            raise ValueError("received_at must be timezone-aware")
        if self.exchange_event_at is not None and not _is_aware(
            self.exchange_event_at
        ):
            raise ValueError("exchange_event_at must be timezone-aware")
        if self.local_sequence <= 0:
            raise ValueError("local_sequence must be positive")
        if self.subscription_generation <= 0:
            raise ValueError("subscription_generation must be positive")


@dataclass(frozen=True, slots=True)
class ArchiveManifest:
    manifest_id: UUID
    schema_version: int
    exchange: str
    environment: str
    route: CaptureRoute
    stream: CaptureStream
    symbol: str | None
    utc_date: date
    utc_hour: int
    relative_path: Path
    connection_session_id: UUID
    subscription_generation_min: int
    subscription_generation_max: int
    row_count: int
    compressed_bytes: int
    first_exchange_event_at: datetime | None
    last_exchange_event_at: datetime | None
    first_received_at: datetime
    last_received_at: datetime
    sha256: str
    capture_version: str
    recovery_status: str
    known_gap_count: int
    created_at: datetime

    def __post_init__(self) -> None:
        if self.schema_version <= 0:
            raise ValueError("schema_version must be positive")
        if not self.exchange.strip() or not self.environment.strip():
            raise ValueError("exchange and environment must not be empty")
        if self.symbol is not None and not self.symbol.strip():
            raise ValueError("symbol must not be empty")
        if not 0 <= self.utc_hour <= 23:
            raise ValueError("utc_hour must be between 0 and 23")
        if self.relative_path.is_absolute() or ".." in self.relative_path.parts:
            raise ValueError("relative_path must stay below the archive root")
        if self.subscription_generation_min <= 0:
            raise ValueError("subscription_generation_min must be positive")
        if self.subscription_generation_max < self.subscription_generation_min:
            raise ValueError(
                "subscription_generation_max must be >= subscription_generation_min"
            )
        if self.row_count <= 0:
            raise ValueError("row_count must be positive")
        if self.compressed_bytes <= 0:
            raise ValueError("compressed_bytes must be positive")
        if self.known_gap_count < 0:
            raise ValueError("known_gap_count must be non-negative")
        _require_aware(self.first_received_at, "first_received_at")
        _require_aware(self.last_received_at, "last_received_at")
        _require_aware(self.created_at, "created_at")


@dataclass(frozen=True, slots=True)
class DurableArchiveAcknowledgement:
    connection_session_id: UUID
    local_sequence: int
    relative_path: Path
    committed_at: datetime


@dataclass(frozen=True, slots=True)
class QualityEvent:
    event_id: UUID
    category: QualityCategory
    occurred_at: datetime
    route: CaptureRoute | None
    stream: CaptureStream | None
    symbol: str | None
    connection_session_id: UUID | None
    local_sequence: int | None
    details: dict[str, JsonValue]


@dataclass(frozen=True, slots=True)
class NormalizedEventSource:
    schema_version: int
    exchange: str
    environment: str
    symbol: str
    event_at: datetime
    received_at: datetime
    source_connection_session_id: UUID
    source_local_sequence: int
    source_stream: CaptureStream

    def __post_init__(self) -> None:
        if not _is_aware(self.event_at):
            raise ValueError("event_at must be timezone-aware")
        if not _is_aware(self.received_at):
            raise ValueError("received_at must be timezone-aware")
        if self.source_local_sequence <= 0:
            raise ValueError("source_local_sequence must be positive")


@dataclass(frozen=True, slots=True)
class NormalizedAggTrade(NormalizedEventSource):
    trade_id: str
    price: Decimal
    quantity: Decimal
    notional: Decimal
    aggressor_side: AggressorSide


@dataclass(frozen=True, slots=True)
class NormalizedBookTicker(NormalizedEventSource):
    update_id: str
    bid_price: Decimal
    bid_quantity: Decimal
    ask_price: Decimal
    ask_quantity: Decimal


@dataclass(frozen=True, slots=True)
class NormalizedMarkPrice(NormalizedEventSource):
    mark_price: Decimal
    index_price: Decimal | None
    estimated_settle_price: Decimal | None
    funding_rate: Decimal | None
    next_funding_at: datetime | None

    def __post_init__(self) -> None:
        NormalizedEventSource.__post_init__(self)
        if self.next_funding_at is not None and not _is_aware(
            self.next_funding_at
        ):
            raise ValueError("next_funding_at must be timezone-aware")


@dataclass(frozen=True, slots=True)
class NormalizedKline1m(NormalizedEventSource):
    open_time: datetime
    close_time: datetime
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: Decimal
    quote_volume: Decimal
    trade_count: int
    closed: bool

    def __post_init__(self) -> None:
        NormalizedEventSource.__post_init__(self)
        if not _is_aware(self.open_time):
            raise ValueError("open_time must be timezone-aware")
        if not _is_aware(self.close_time):
            raise ValueError("close_time must be timezone-aware")
        if self.trade_count < 0:
            raise ValueError("trade_count must be non-negative")


@dataclass(frozen=True, slots=True)
class NormalizedLiquidation(NormalizedEventSource):
    order_side: OrderSide
    price: Decimal
    average_price: Decimal
    quantity: Decimal
    notional: Decimal
    trade_time: datetime | None

    def __post_init__(self) -> None:
        NormalizedEventSource.__post_init__(self)
        if self.trade_time is not None and not _is_aware(self.trade_time):
            raise ValueError("trade_time must be timezone-aware")


type NormalizedMarketEvent = (
    NormalizedAggTrade
    | NormalizedBookTicker
    | NormalizedMarkPrice
    | NormalizedKline1m
    | NormalizedLiquidation
)


@dataclass(frozen=True, slots=True)
class MarketState15s:
    schema_version: int
    exchange: str
    environment: str
    symbol: str
    bucket_start: datetime
    bucket_end: datetime
    open_price: Decimal | None
    high_price: Decimal | None
    low_price: Decimal | None
    close_price: Decimal | None
    trade_count: int
    trade_notional: Decimal
    aggressive_buy_notional: Decimal
    aggressive_sell_notional: Decimal
    last_bid_price: Decimal | None
    last_ask_price: Decimal | None
    spread: Decimal | None
    midpoint: Decimal | None
    liquidation_count: int
    liquidation_notional: Decimal
    mark_price: Decimal | None
    closed_kline_count: int
    source_event_count: int
    first_received_at: datetime | None
    last_received_at: datetime | None
    closed_kline_1m_open_time: datetime | None = None
    closed_kline_1m_close_time: datetime | None = None
    closed_kline_1m_open_price: Decimal | None = None
    closed_kline_1m_close_price: Decimal | None = None

    def __post_init__(self) -> None:
        if not _is_aware(self.bucket_start):
            raise ValueError("bucket_start must be timezone-aware")
        if not _is_aware(self.bucket_end):
            raise ValueError("bucket_end must be timezone-aware")
        if self.bucket_end <= self.bucket_start:
            raise ValueError("bucket_end must be after bucket_start")
        if self.trade_count < 0:
            raise ValueError("trade_count must be non-negative")
        if self.liquidation_count < 0:
            raise ValueError("liquidation_count must be non-negative")
        if self.closed_kline_count < 0:
            raise ValueError("closed_kline_count must be non-negative")
        if self.source_event_count < 0:
            raise ValueError("source_event_count must be non-negative")
        if self.first_received_at is not None and not _is_aware(
            self.first_received_at
        ):
            raise ValueError("first_received_at must be timezone-aware")
        if self.last_received_at is not None and not _is_aware(
            self.last_received_at
        ):
            raise ValueError("last_received_at must be timezone-aware")
        kline_fields = (
            self.closed_kline_1m_open_time,
            self.closed_kline_1m_close_time,
            self.closed_kline_1m_open_price,
            self.closed_kline_1m_close_price,
        )
        if any(field is not None for field in kline_fields) and not all(
            field is not None for field in kline_fields
        ):
            raise ValueError("closed 1m kline fields must be complete")
        if self.closed_kline_1m_open_time is not None and not _is_aware(
            self.closed_kline_1m_open_time
        ):
            raise ValueError("closed_kline_1m_open_time must be timezone-aware")
        if self.closed_kline_1m_close_time is not None and not _is_aware(
            self.closed_kline_1m_close_time
        ):
            raise ValueError("closed_kline_1m_close_time must be timezone-aware")


def transition_market_data_state(
    current: MarketDataState,
    target: MarketDataState,
    *,
    recovery: bool = False,
) -> MarketDataState:
    if current is MarketDataState.HALTED:
        if not recovery or target is not MarketDataState.SYNCING:
            raise ValueError("HALTED state requires explicit recovery to SYNCING")
    if current is MarketDataState.STOPPED:
        raise ValueError("STOPPED state is terminal")
    return target


def _is_aware(value: datetime) -> bool:
    return value.tzinfo is not None and value.utcoffset() is not None


def _require_aware(value: datetime, field_name: str) -> None:
    if not _is_aware(value):
        raise ValueError(f"{field_name} must be timezone-aware")
