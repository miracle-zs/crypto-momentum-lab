from dataclasses import dataclass
from datetime import date, datetime
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


@dataclass(frozen=True, slots=True)
class ConnectionLifecycleEvent:
    session_id: UUID
    route: CaptureRoute
    stream: CaptureStream
    symbols: tuple[str, ...]
    occurred_at: datetime
    opened: bool
    reason: str | None


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
