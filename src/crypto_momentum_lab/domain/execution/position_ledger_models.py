"""Domain models for authoritative position ledger and immutable facts.

Provides strict identity types:
- ``PositionKey``: Fully qualified identity across environment, account, symbol, and side;
- ``FactCoverageInterval``: Watermark and coverage boundaries of observed facts;
- ``AccountFacts``: Normalized container of immutable account-level facts;
- ``PositionLedgerBatch``: An entry lot within a specific position episode;
- ``PositionEpisode``: A continuous non-zero holding lifecycle bounded by zero-crossings;
- ``PositionLedgerProjection``: Point-in-time materialized ledger state with unallocated lots.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Any

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.strategy import StrategySide


class FactCoverageStatus(StrEnum):
    """Integrity and completeness status of a fact coverage interval."""

    CONFIRMED = "CONFIRMED"
    GAP_DETECTED = "GAP_DETECTED"
    PENDING = "PENDING"


@dataclass(frozen=True, slots=True)
class CoverageEvidence:
    """Durable proof that account facts were loaded completely.

    CONFIRMED coverage requires all of:
    - a fill-load cursor proving continuous ingestion through the end of
      the window (``fill_checked_through``);
    - a checkpoint whose event cut reaches the end of the window;
    - a load start that is not after the requested window start.
    Missing any piece leaves the interval unconfirmed.
    """

    fill_cursor_id: str | None = None
    fill_load_start: datetime | None = None
    fill_checked_through: datetime | None = None
    checkpoint_id: str | None = None
    checkpoint_event_cut: datetime | None = None

    def proves_complete(self, start: datetime, end: datetime) -> bool:
        if self.fill_load_start is None or self.fill_checked_through is None:
            return False
        if self.checkpoint_id is None or self.checkpoint_event_cut is None:
            return False
        return (
            self.fill_load_start <= start
            and self.fill_checked_through >= end
            and self.checkpoint_event_cut >= end
        )


def compose_fact_coverage(
    evidence: CoverageEvidence | None,
    *,
    start: datetime,
    end: datetime,
) -> FactCoverageInterval:
    """Build coverage only from proven evidence — never from empty attributes.

    A non-empty cursor or checkpoint id is not enough: the window must be
    bracketed by load start, fill check-through, and checkpoint event cut.
    """
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("coverage bounds must be timezone-aware")
    if end < start:
        raise ValueError("coverage end must not precede start")

    if evidence is not None and evidence.proves_complete(start, end):
        return FactCoverageInterval(
            start_at=max(start, evidence.fill_load_start or start),
            end_at=min(
                end,
                evidence.fill_checked_through,
                evidence.checkpoint_event_cut,
            ),
            source_cursor=evidence.fill_cursor_id,
            status=FactCoverageStatus.CONFIRMED,
            confirmed_revision=None,
        )

    return FactCoverageInterval(
        start_at=start,
        end_at=end,
        source_cursor=(
            evidence.fill_cursor_id if evidence is not None else None
        ),
        status=FactCoverageStatus.PENDING,
        confirmed_revision=None,
    )


class PositionHealthStatus(StrEnum):
    """Authoritative trading health status for a PositionKey per architecture RFC 2026-09-25."""

    READY = "READY"
    CATCHING_UP = "CATCHING_UP"
    INCOMPLETE = "INCOMPLETE"
    CONFLICT = "CONFLICT"


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
        if not isinstance(self.position_side, FuturesPositionSide):
            object.__setattr__(
                self,
                "position_side",
                FuturesPositionSide(self.position_side),
            )

    @property
    def canonical_id(self) -> str:
        return (
            f"{self.environment}:{self.account_label}:{self.symbol}:"
            f"{self.position_side.value}"
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

    def __post_init__(self) -> None:
        if self.start_at.tzinfo is None:
            raise ValueError("start_at must be timezone-aware")
        if self.end_at.tzinfo is None:
            raise ValueError("end_at must be timezone-aware")
        if self.end_at < self.start_at:
            raise ValueError("end_at must not precede start_at")
        if not isinstance(self.status, FactCoverageStatus):
            object.__setattr__(
                self,
                "status",
                FactCoverageStatus(self.status),
            )

    def covers(self, point_in_time: datetime) -> bool:
        """Returns True if point_in_time is within interval without known gaps."""
        if self.has_known_gaps or self.status != FactCoverageStatus.CONFIRMED:
            return False
        return self.start_at <= point_in_time <= self.end_at

    def covers_range(self, start: datetime, end: datetime) -> bool:
        """Returns True if [start, end] is within interval without known gaps."""
        if self.has_known_gaps or self.status != FactCoverageStatus.CONFIRMED:
            return False
        return self.start_at <= start and end <= self.end_at


@dataclass(frozen=True, slots=True)
class PositionCheckpoint:
    """Materialized checkpoint representing complete state at a known event cut."""

    checkpoint_id: str
    key: PositionKey
    event_cut: datetime
    net_quantity: Decimal
    entry_price: Decimal
    active_episode_id: str | None = None
    active_batches: tuple[PositionLedgerBatch, ...] = ()
    coverage_start: datetime | None = None
    coverage_end: datetime | None = None
    facts_hash: str = ""

    def __post_init__(self) -> None:
        if not self.checkpoint_id.strip():
            raise ValueError("checkpoint_id must not be empty")
        if self.event_cut.tzinfo is None:
            raise ValueError("event_cut must be timezone-aware")
        if self.net_quantity < 0:
            raise ValueError("net_quantity must be non-negative")
        if self.entry_price < 0:
            raise ValueError("entry_price must be non-negative")


@dataclass(frozen=True, slots=True)
class ExitOrderSubmissionFact:
    """The submission of an exit (reduce-only) order defining a batch boundary."""

    order_id: str
    submitted_at: datetime
    symbol: str
    position_side: FuturesPositionSide
    client_order_id: str | None = None
    target_batch_id: str | None = None

    def __post_init__(self) -> None:
        if self.submitted_at.tzinfo is None:
            raise ValueError("submitted_at must be timezone-aware")
        if not self.order_id.strip():
            raise ValueError("order_id must not be empty")


@dataclass(frozen=True, slots=True)
class AccountFacts:
    """Normalized immutable account facts for a given position key."""

    position_key: PositionKey
    fills: tuple[AccountFillEvent, ...] = ()
    snapshots: tuple[AccountPositionSnapshot, ...] = ()
    exit_boundaries: tuple[ExitOrderSubmissionFact, ...] = ()
    coverage: FactCoverageInterval | None = None
    checkpoint: PositionCheckpoint | None = None
    has_synthetic_fills: bool = False


@dataclass(frozen=True, slots=True)
class BatchReductionAttribution:
    """Attribution of a reduction (exit/close) to a specific lot."""

    batch_id: str
    quantity: Decimal


@dataclass(frozen=True, slots=True)
class ExternalReductionFact:
    """A position reduction fact (exit trade) with explicit lot attribution."""

    trade_id: str
    order_id: str
    quantity: Decimal
    price: Decimal
    reduced_at: datetime
    is_system: bool = False
    attributions: tuple[BatchReductionAttribution, ...] = ()

    def __post_init__(self) -> None:
        if self.quantity <= 0:
            raise ValueError("quantity must be positive")
        if self.price <= 0:
            raise ValueError("price must be positive")
        if self.reduced_at.tzinfo is None:
            raise ValueError("reduced_at must be timezone-aware")


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
        if self.opened_at.tzinfo is None:
            raise ValueError("opened_at must be timezone-aware")
        if self.exit_order_submitted_at is not None and self.exit_order_submitted_at.tzinfo is None:
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
    reductions: tuple[ExternalReductionFact, ...] = ()

    def __post_init__(self) -> None:
        if not self.episode_id.strip():
            raise ValueError("episode_id must not be empty")
        if self.opened_at.tzinfo is None:
            raise ValueError("opened_at must be timezone-aware")
        if self.closed_at is not None and self.closed_at.tzinfo is None:
            raise ValueError("closed_at must be timezone-aware")

    @property
    def active_batches(self) -> tuple[PositionLedgerBatch, ...]:
        return tuple(b for b in self.batches if b.quantity > 0)

    @property
    def remaining_quantity(self) -> Decimal:
        return sum((b.quantity for b in self.batches), start=Decimal("0"))


@dataclass(frozen=True, slots=True)
class PositionLedgerProjection:
    """Immutable point-in-time projection of a position ledger."""

    position_key: PositionKey
    active_episode: PositionEpisode | None
    active_batches: tuple[PositionLedgerBatch, ...]
    total_active_quantity: Decimal
    unallocated_quantity: Decimal
    reconciliation_gap: Decimal
    high_watermark_trade_at: datetime | None
    archived_episodes: tuple[PositionEpisode, ...] = ()
    diagnostics: tuple[str, ...] = ()
    health_status: PositionHealthStatus = PositionHealthStatus.READY
    event_cut: datetime | None = None
    discrepancy: PositionDiscrepancy | None = None
    is_comparable: bool = True


@dataclass(frozen=True, slots=True)
class FreshnessRequirement:
    """Freshness constraints for reading an authoritative PositionView."""

    max_staleness: timedelta = timedelta(seconds=15)
    min_event_cut: datetime | None = None
    require_comparable: bool = True


@dataclass(frozen=True, slots=True)
class PositionView:
    """Authoritative, immutable point-in-time view consumed by strategy and execution coordinators."""

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

    @property
    def total_quantity(self) -> Decimal:
        return sum((b.quantity for b in self.batches), start=Decimal("0"))

    @property
    def is_ready_for_trade(self) -> bool:
        return (
            self.health_status == PositionHealthStatus.READY
            and self.is_comparable
            and (self.reconciliation_gap is None or self.reconciliation_gap == Decimal("0"))
            and self.unallocated_quantity == Decimal("0")
        )

