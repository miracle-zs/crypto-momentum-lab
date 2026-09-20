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
from datetime import datetime
from decimal import Decimal

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.strategy import StrategySide


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

    def __post_init__(self) -> None:
        if self.start_at.tzinfo is None:
            raise ValueError("start_at must be timezone-aware")
        if self.end_at.tzinfo is None:
            raise ValueError("end_at must be timezone-aware")
        if self.end_at < self.start_at:
            raise ValueError("end_at must not precede start_at")


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
