"""Contracts used by the research market-state collector.

The collector deliberately works with the already-closed ``MarketState15s``
contract.  It does not know how Binance messages are normalized or how a
strategy calculates a feature.  That keeps the external seam small and makes
the collector useful for both live capture and deterministic backfill.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Protocol
from uuid import UUID

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.market_data.hub import MarketStateBatch


class CollectorError(RuntimeError):
    """Base error for collection and durable-storage failures."""


class CollectorPaused(CollectorError):
    """Raised when storage pressure requires collection to stop."""


class CollectorSequenceGap(CollectorError):
    """Raised when a live Hub batch is not contiguous."""


class CollectorStateConflict(CollectorError):
    """Raised when one natural state key has two different payloads."""


class SourceKind(StrEnum):
    HUB = "hub"
    POSTGRES_BACKFILL = "postgres_backfill"


def require_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class CollectorCheckpoint:
    """The last live batch known to be durably covered by Parquet.

    ``last_sequence`` is a Hub cursor, while the bucket/symbol cursor is the
    durable market-state cursor used when PostgreSQL backfill is required.
    The two cursors intentionally remain separate because backfill rows do not
    have a Hub sequence.
    """

    environment: str
    stream_id: str | None = None
    last_sequence: int | None = None
    last_bucket_start: datetime | None = None
    last_symbol: str | None = None
    schema_version: int = 1
    updated_at: datetime | None = None

    def __post_init__(self) -> None:
        if not self.environment.strip():
            raise ValueError("environment must not be empty")
        if self.stream_id is not None and not self.stream_id.strip():
            raise ValueError("stream_id must not be empty when present")
        if self.last_sequence is not None and self.last_sequence < 0:
            raise ValueError("last_sequence must not be negative")
        if self.schema_version <= 0:
            raise ValueError("schema_version must be positive")
        if self.last_bucket_start is not None:
            object.__setattr__(
                self,
                "last_bucket_start",
                require_utc(self.last_bucket_start, "last_bucket_start"),
            )
        if self.updated_at is not None:
            object.__setattr__(
                self,
                "updated_at",
                require_utc(self.updated_at, "updated_at"),
            )


@dataclass(frozen=True, slots=True)
class SelectedSymbol:
    """Point-in-time explanation for why a symbol was retained."""

    symbol: str
    reason: str
    rank: int | None = None
    utc_day_return: Decimal | None = None
    membership_status: str | None = None
    snapshot_id: UUID | None = None
    snapshot_observed_at: datetime | None = None

    def __post_init__(self) -> None:
        if not self.symbol.strip():
            raise ValueError("symbol must not be empty")
        if not self.reason.strip():
            raise ValueError("reason must not be empty")
        if self.rank is not None and self.rank <= 0:
            raise ValueError("rank must be positive")
        if self.snapshot_observed_at is not None:
            object.__setattr__(
                self,
                "snapshot_observed_at",
                require_utc(
                    self.snapshot_observed_at,
                    "snapshot_observed_at",
                ),
            )


@dataclass(frozen=True, slots=True)
class SelectionSnapshot:
    observed_at: datetime
    symbols: tuple[SelectedSymbol, ...]
    snapshot_id: UUID | None = None
    retain_all: bool = False

    def __post_init__(self) -> None:
        observed_at = require_utc(self.observed_at, "observed_at")
        object.__setattr__(self, "observed_at", observed_at)
        seen: set[str] = set()
        for selected in self.symbols:
            if selected.symbol in seen:
                raise ValueError(f"duplicate selected symbol: {selected.symbol}")
            seen.add(selected.symbol)

    @property
    def by_symbol(self) -> Mapping[str, SelectedSymbol]:
        return {item.symbol: item for item in self.symbols}


@dataclass(frozen=True, slots=True)
class CollectionBatch:
    """A batch plus its origin; the Hub wire contract remains unchanged."""

    batch: MarketStateBatch
    source_kind: SourceKind = SourceKind.HUB

    @property
    def environment(self) -> str:
        return self.batch.environment

    @property
    def states(self) -> tuple[MarketState15s, ...]:
        return self.batch.states

    @property
    def sequence(self) -> int:
        return self.batch.sequence

    @property
    def stream_id(self) -> str | None:
        return self.batch.stream_id


@dataclass(frozen=True, slots=True)
class CollectionReceipt:
    source_kind: SourceKind
    sequence: int | None
    selected_rows: int
    duplicate_rows: int = 0
    committed_rows: int = 0
    committed_sequence: int | None = None
    skipped_rows: int = 0


@dataclass(frozen=True, slots=True)
class CollectorHealth:
    environment: str
    connected: bool
    last_received_bucket: datetime | None
    last_persisted_bucket: datetime | None
    last_sequence: int | None
    selected_rows: int
    persisted_rows: int
    duplicate_rows: int
    gap_count: int
    pending_spool_files: int
    pending_spool_bytes: int
    collector_bytes: int
    disk_free_bytes: int
    warning: bool
    paused: bool


@dataclass(frozen=True, slots=True)
class CollectorConfig:
    """Runtime limits for the same-host, isolated collector."""

    environment: str
    root: Path
    soft_limit_bytes: int = 6 * 1024**3
    hard_limit_bytes: int = 8 * 1024**3
    global_warning_free_bytes: int = 15 * 1024**3
    global_pause_free_bytes: int = 10 * 1024**3
    window_seconds: int = 900
    late_tolerance_seconds: int = 30
    max_spool_bytes: int = 1024**3
    capacity_check_interval_seconds: float = 30.0

    def __post_init__(self) -> None:
        if not self.environment.strip():
            raise ValueError("environment must not be empty")
        if self.soft_limit_bytes <= 0:
            raise ValueError("soft_limit_bytes must be positive")
        if self.hard_limit_bytes <= self.soft_limit_bytes:
            raise ValueError("hard_limit_bytes must exceed soft_limit_bytes")
        if (
            self.global_warning_free_bytes <= 0
            or self.global_pause_free_bytes <= 0
        ):
            raise ValueError("global free-space limits must be positive")
        if self.global_warning_free_bytes <= self.global_pause_free_bytes:
            raise ValueError(
                "global_warning_free_bytes must exceed global_pause_free_bytes"
            )
        if self.window_seconds < 15 or self.window_seconds % 15 != 0:
            raise ValueError("window_seconds must be a multiple of 15")
        if self.late_tolerance_seconds < 0:
            raise ValueError("late_tolerance_seconds must not be negative")
        if self.max_spool_bytes <= 0:
            raise ValueError("max_spool_bytes must be positive")
        if self.capacity_check_interval_seconds <= 0:
            raise ValueError("capacity_check_interval_seconds must be positive")


class SymbolSelector(Protocol):
    async def selection_at(self, observed_at: datetime) -> SelectionSnapshot:
        """Return the symbols that should be retained at a market timestamp."""


class CollectionSource(Protocol):
    def batches(self) -> AsyncIterator[MarketStateBatch]:
        """Return an async iterator of ``MarketStateBatch`` objects."""


class CollectionSink(Protocol):
    def append(
        self,
        collection_batch: CollectionBatch,
        selection: SelectionSnapshot,
    ) -> object:
        """Append a batch to the sink's durable-window implementation."""

    def flush_ready(self, latest_bucket_start: datetime) -> object:
        """Flush windows that are beyond the late-event tolerance."""

    def flush_all(self) -> object:
        """Flush all buffered windows durably."""
