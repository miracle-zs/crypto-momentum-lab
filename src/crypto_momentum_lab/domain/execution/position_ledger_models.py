"""Domain models for immutable exchange facts and their derived position view.

Provides strict identity types:
- ``PositionKey``: Fully qualified identity across environment,
  account, symbol, and side;
- ``FactCoverageInterval``: Watermark and coverage boundaries of observed facts;
- ``PositionLedgerBatch``: An entry lot within a specific position episode;
- ``PositionEpisode``: A continuous non-zero holding lifecycle
  bounded by zero-crossings;
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Any

from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.trading import TradeSide as StrategySide


class FactCoverageStatus(StrEnum):
    """Integrity and completeness status of a fact coverage interval."""

    CONFIRMED = "CONFIRMED"
    GAP_DETECTED = "GAP_DETECTED"
    PENDING = "PENDING"


class PositionHealthStatus(StrEnum):
    """
    Authoritative trading health status for a PositionKey per architecture RFC
    2026-09-25.
    """

    READY = "READY"
    CATCHING_UP = "CATCHING_UP"
    INCOMPLETE = "INCOMPLETE"
    CONFLICT = "CONFLICT"


class PositionState(StrEnum):
    """Canonical 2-state position dimension according to REFACTOR_SPEC §3."""

    FLAT = "flat"
    OPEN = "open"


class FactCredibility(StrEnum):
    """Canonical 3-state fact credibility dimension according to REFACTOR_SPEC §3."""

    READY = "ready"
    PENDING = "pending"
    CONFLICT = "conflict"


class DiscrepancyKind(StrEnum):
    """Classification of divergences across models, observations, and fact journals."""

    INPUT_MISSING = "INPUT_MISSING"
    TIME_MISALIGNED = "TIME_MISALIGNED"
    QUANTITY_MISMATCH = "QUANTITY_MISMATCH"
    PRICE_MISMATCH = "PRICE_MISMATCH"
    IDENTITY_MISMATCH = "IDENTITY_MISMATCH"
    BOUNDARY_MISMATCH = "BOUNDARY_MISMATCH"
    PENDING_BINDING = "PENDING_BINDING"


@dataclass(frozen=True, slots=True)
class PositionDiscrepancy:
    """Structured audit record for a single model or observation discrepancy."""

    discrepancy_id: str
    key: PositionKey
    kind: DiscrepancyKind
    first_seen_at: datetime
    last_seen_at: datetime
    count: int
    input_hash: str
    details: str
    event_cut: datetime | None = None
    snapshot_at: datetime | None = None
    first_divergent_fact: str | None = None
    is_reconciled: bool = False
    resolution_evidence: str | None = None


@dataclass(frozen=True, slots=True)
class PositionKey:
    """Explicit identity key for a futures position.

    Prevents callers from relying on ambiguous symbol strings or omitting
    account, environment, or position_side dimensions.
    """

    environment: str
    account_label: str
    symbol: str
    position_side: FuturesPositionSide = FuturesPositionSide.BOTH

    def __post_init__(self) -> None:
        if not self.environment.strip():
            raise ValueError("environment must not be empty")
        if not self.account_label.strip():
            raise ValueError("account_label must not be empty")
        if not self.symbol.strip():
            raise ValueError("symbol must not be empty")
        if any(
            ":" in value
            for value in (self.environment, self.account_label, self.symbol)
        ):
            raise ValueError("position key fields must not contain ':'")

    @property
    def canonical_id(self) -> str:
        return (
            f"{self.environment}:{self.account_label}:{self.symbol}:"
            f"{self.position_side.value}"
        )


@dataclass(frozen=True, slots=True)
class AccountFactStreamScope:
    """Identity of the source stream that supplied account facts.

    ``stream_epoch`` distinguishes independent source continuity windows. A
    checkpoint from another epoch may still seed a quantity projection, but it
    cannot prove coverage for this scope by itself.
    """

    environment: str
    account_label: str
    symbol: str
    position_side: FuturesPositionSide
    stream_id: str
    stream_epoch: str

    def __post_init__(self) -> None:
        for name in (
            "environment",
            "account_label",
            "symbol",
            "stream_id",
            "stream_epoch",
        ):
            if not getattr(self, name).strip():
                raise ValueError(f"{name} must not be empty")

    @classmethod
    def for_position_key(
        cls,
        key: PositionKey,
        *,
        stream_id: str,
        stream_epoch: str,
    ) -> AccountFactStreamScope:
        return cls(
            environment=key.environment,
            account_label=key.account_label,
            symbol=key.symbol,
            position_side=key.position_side,
            stream_id=stream_id,
            stream_epoch=stream_epoch,
        )

    def matches(self, key: PositionKey) -> bool:
        return (
            self.environment == key.environment
            and self.account_label == key.account_label
            and self.symbol == key.symbol
            and self.position_side == key.position_side
        )

    @property
    def canonical_id(self) -> str:
        return json.dumps(
            [
                self.environment,
                self.account_label,
                self.symbol,
                self.position_side.value,
                self.stream_id,
                self.stream_epoch,
            ],
            separators=(",", ":"),
        )


@dataclass(frozen=True, slots=True)
class AccountFillLoadProvenance:
    """Durable continuity proof for paginated account-fill reconciliation.

    A scan origin alone does not establish historical completeness. Coverage
    also requires a trusted source anchor, an exhausted non-truncated page
    chain, and an exact checked-through cut.
    """

    stream_scope: AccountFactStreamScope
    load_id: str
    scan_origin_from_id: int | None
    scan_origin_start_time_ms: int | None
    request_from_id: int | None
    next_from_id: int | None
    page_count: int
    page_exhausted: bool
    truncated: bool
    checked_through: datetime | None
    observed_at: datetime
    source_anchor_id: str
    source_anchor_event_cut: datetime

    def __post_init__(self) -> None:
        if not self.load_id.strip():
            raise ValueError("fill load id must not be empty")
        if not self.source_anchor_id.strip():
            raise ValueError("fill scan requires a source anchor")
        if (self.scan_origin_from_id is None) == (
            self.scan_origin_start_time_ms is None
        ):
            raise ValueError("fill scan requires exactly one source origin")
        for name, value in (
            ("scan_origin_from_id", self.scan_origin_from_id),
            ("scan_origin_start_time_ms", self.scan_origin_start_time_ms),
            ("request_from_id", self.request_from_id),
            ("next_from_id", self.next_from_id),
        ):
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError(f"{name} must be a non-negative integer or null")
        if type(self.page_count) is not int or self.page_count < 0:
            raise ValueError("fill scan page_count must be a non-negative integer")
        if type(self.page_exhausted) is not bool or type(self.truncated) is not bool:
            raise ValueError("fill scan pagination flags must be booleans")
        if self.page_exhausted and (self.truncated or self.next_from_id is not None):
            raise ValueError("exhausted fill scan cannot be truncated or have a cursor")
        if self.page_exhausted and self.page_count == 0:
            raise ValueError("exhausted fill scan must record at least one page")
        if self.checked_through is not None and (
            self.checked_through.tzinfo is None
            or self.checked_through.utcoffset() is None
        ):
            raise ValueError("fill scan checked_through must be timezone-aware")
        for name, timestamp in (
            ("observed_at", self.observed_at),
            ("source_anchor_event_cut", self.source_anchor_event_cut),
        ):
            if timestamp.tzinfo is None or timestamp.utcoffset() is None:
                raise ValueError(f"fill scan {name} must be timezone-aware")
        if self.checked_through is not None and self.checked_through > self.observed_at:
            raise ValueError("fill scan checked_through is after its observation")

    @property
    def is_complete(self) -> bool:
        return (
            self.page_exhausted
            and not self.truncated
            and self.next_from_id is None
            and self.checked_through is not None
        )

    @property
    def origin_start_at(self) -> datetime | None:
        if self.scan_origin_start_time_ms is None:
            return None
        return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(
            milliseconds=self.scan_origin_start_time_ms
        )


@dataclass(frozen=True, slots=True)
class FactCoverageInterval:
    """Interval over which account facts are confirmed to be complete."""

    start_at: datetime
    end_at: datetime
    has_known_gaps: bool = False
    source_cursor: str | None = None
    status: FactCoverageStatus = FactCoverageStatus.CONFIRMED
    confirmed_revision: int | None = None
    stream_scope: AccountFactStreamScope | None = None
    evidence_observed_at: datetime | None = None
    load_provenance: AccountFillLoadProvenance | None = None

    def __post_init__(self) -> None:
        if self.start_at.tzinfo is None:
            raise ValueError("start_at must be timezone-aware")
        if self.end_at.tzinfo is None:
            raise ValueError("end_at must be timezone-aware")
        if self.end_at < self.start_at:
            raise ValueError("end_at must not precede start_at")
        if self.evidence_observed_at is not None and (
            self.evidence_observed_at.tzinfo is None
            or self.evidence_observed_at.utcoffset() is None
        ):
            raise ValueError("evidence_observed_at must be timezone-aware")
        if self.load_provenance is not None:
            if self.stream_scope != self.load_provenance.stream_scope:
                raise ValueError("coverage load provenance scope mismatch")
            if (
                self.evidence_observed_at is not None
                and self.load_provenance.observed_at > self.evidence_observed_at
            ):
                raise ValueError("coverage predates its load provenance")

    def covers(self, point_in_time: datetime) -> bool:
        """Returns True if point_in_time is within interval without known gaps."""
        if not self._is_authoritative():
            return False
        return self.start_at <= point_in_time <= self.end_at

    def covers_range(self, start: datetime, end: datetime) -> bool:
        """Returns True if [start, end] is within interval without known gaps."""
        if not self._is_authoritative():
            return False
        return self.start_at <= start and end <= self.end_at

    @property
    def is_authoritative(self) -> bool:
        """Whether this coverage can authorize the exact scoped live stream."""
        return self._is_authoritative()

    def _is_authoritative(self) -> bool:
        if self.has_known_gaps or self.status != FactCoverageStatus.CONFIRMED:
            return False
        if self.load_provenance is None:
            return self.stream_scope is None or self.stream_scope.environment != "live"
        return (
            self.load_provenance.is_complete
            and self.stream_scope == self.load_provenance.stream_scope
            and self.load_provenance.source_anchor_event_cut <= self.start_at
            and self.load_provenance.checked_through is not None
            and self.load_provenance.checked_through >= self.end_at
        )


@dataclass(frozen=True, slots=True)
class PositionLedgerBatch:
    """One discrete entry lot inside a position episode."""

    batch_id: str
    episode_id: str
    quantity: Decimal
    original_quantity: Decimal
    entry_price: Decimal
    opened_at: datetime
    order_id: str | None = None
    client_order_id: str | None = None
    is_external: bool = False
    exit_order_submitted_at: datetime | None = None
    # Ordered distinct opening identities; empty means count evidence is absent.
    entry_order_ids: tuple[str, ...] = ()
    entry_client_order_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.batch_id.strip():
            raise ValueError("batch_id must not be empty")
        if not self.episode_id.strip():
            raise ValueError("episode_id must not be empty")
        if self.quantity < 0:
            raise ValueError("quantity must be non-negative")
        if self.original_quantity <= 0:
            raise ValueError("original_quantity must be positive")
        if self.quantity > self.original_quantity:
            raise ValueError("quantity must not exceed original_quantity")
        if self.entry_price <= 0:
            raise ValueError("entry_price must be positive")
        for identities in (self.entry_order_ids, self.entry_client_order_ids):
            if len(set(identities)) != len(identities) or any(
                not v.strip() for v in identities
            ):
                raise ValueError("entry order identities must be unique and nonempty")
        if self.opened_at.tzinfo is None:
            raise ValueError("opened_at must be timezone-aware")
        if (
            self.exit_order_submitted_at is not None
            and self.exit_order_submitted_at.tzinfo is None
        ):
            raise ValueError("exit_order_submitted_at must be timezone-aware")


@dataclass(frozen=True, slots=True)
class PositionEpisode:
    """A continuous holding lifecycle bounded by zero-crossings or side reversals."""

    episode_id: str
    position_key: PositionKey
    side: StrategySide
    opened_at: datetime
    closed_at: datetime | None = None
    is_active: bool = True
    cumulative_bought: Decimal = Decimal("0")
    cumulative_sold: Decimal = Decimal("0")
    peak_quantity: Decimal = Decimal("0")
    batches: tuple[PositionLedgerBatch, ...] = ()

    def __post_init__(self) -> None:
        if not self.episode_id.strip():
            raise ValueError("episode_id must not be empty")
        if self.opened_at.tzinfo is None:
            raise ValueError("opened_at must be timezone-aware")
        if self.closed_at is not None and self.closed_at.tzinfo is None:
            raise ValueError("closed_at must be timezone-aware")

@dataclass(frozen=True, slots=True)
class PositionView:
    """Derived lot-level read model consumed by strategy and execution.

    Exchange snapshots and fills are the source facts. This view groups those
    facts into the batches needed for targeted exits; it is not a second source
    of account position truth.
    """

    key: PositionKey
    projection_version: str
    input_revision: int
    event_cut: datetime | None
    policy_version: str
    schema_version: str
    coverage: FactCoverageInterval | None
    active_episode: PositionEpisode | None
    batches: tuple[PositionLedgerBatch, ...]
    unallocated_quantity: Decimal
    reservations: tuple[Any, ...] = ()
    observation_id: str | None = None
    reconciliation_status: str = "OK"
    reconciliation_gap: Decimal | None = None
    health_status: PositionHealthStatus = PositionHealthStatus.READY
    diagnostics: tuple[str, ...] = ()
    discrepancy: PositionDiscrepancy | None = None
    is_comparable: bool = True
    zero_position_snapshot_confirmed: bool = False
    stream_scope: AccountFactStreamScope | None = None
    @property
    def total_quantity(self) -> Decimal:
        return sum((b.quantity for b in self.batches), start=Decimal("0"))

    @property
    def position_state(self) -> PositionState:
        return PositionState.OPEN if self.total_quantity > 0 else PositionState.FLAT

    @property
    def fact_credibility(self) -> FactCredibility:
        if (
            self.health_status == PositionHealthStatus.CONFLICT
            or self.discrepancy is not None
        ):
            return FactCredibility.CONFLICT
        has_confirmed_coverage = (
            self.coverage is not None
            and self.coverage.is_authoritative
            and self.coverage.status == FactCoverageStatus.CONFIRMED
            and (
                self.stream_scope is None
                or self.coverage.stream_scope == self.stream_scope
            )
            and (
                self.stream_scope is None
                or self.coverage.evidence_observed_at is not None
            )
        )
        has_clean_non_live = self.coverage is None and self.key.environment != "live"
        has_zero_snapshot = self.zero_position_snapshot_confirmed
        if (
            self.health_status == PositionHealthStatus.READY
            and self.is_comparable
            and (has_confirmed_coverage or has_clean_non_live or has_zero_snapshot)
            and (
                self.reconciliation_gap is None
                or self.reconciliation_gap == Decimal("0")
            )
            and self.unallocated_quantity == Decimal("0")
        ):
            return FactCredibility.READY
        return FactCredibility.PENDING

    @property
    def is_ready_for_trade(self) -> bool:
        return self.fact_credibility == FactCredibility.READY
