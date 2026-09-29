"""Domain models for authoritative position ledger and immutable facts.

Provides strict identity types:
- ``PositionKey``: Fully qualified identity across environment,
  account, symbol, and side;
- ``FactCoverageInterval``: Watermark and coverage boundaries of observed facts;
- ``AccountFacts``: Normalized container of immutable account-level facts;
- ``PositionLedgerBatch``: An entry lot within a specific position episode;
- ``PositionEpisode``: A continuous non-zero holding lifecycle
  bounded by zero-crossings;
- ``PositionLedgerProjection``: Point-in-time materialized ledger
  state with unallocated lots.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field, fields, is_dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from functools import lru_cache
from typing import TYPE_CHECKING, Any

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountFillReconciliationCursor,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.strategy import StrategySide

if TYPE_CHECKING:
    from crypto_momentum_lab.domain.execution.recovery_models import (
        PositionRecoveryCheckpoint,
    )


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
    stream_scope: AccountFactStreamScope | None = None
    evidence_observed_at: datetime | None = None
    page_exhausted: bool = False
    not_truncated: bool = False
    load_provenance: AccountFillLoadProvenance | None = None

    def proves_complete(
        self,
        start: datetime,
        end: datetime,
        *,
        expected_scope: AccountFactStreamScope | None = None,
    ) -> bool:
        if (
            self.fill_load_start is None
            or self.fill_checked_through is None
            or type(self.page_exhausted) is not bool
            or type(self.not_truncated) is not bool
            or not self.page_exhausted
            or not self.not_truncated
            or self.load_provenance is None
        ):
            return False
        if self.checkpoint_id is None or self.checkpoint_event_cut is None:
            return False
        if not self.checkpoint_id.strip():
            return False
        if (
            self.checkpoint_event_cut.tzinfo is None
            or self.checkpoint_event_cut.utcoffset() is None
        ):
            return False
        provenance = self.load_provenance
        origin_start = provenance.origin_start_at
        return (
            provenance.is_complete
            and self.stream_scope == provenance.stream_scope
            and (expected_scope is None or provenance.stream_scope == expected_scope)
            and origin_start is not None
            and origin_start == self.fill_load_start
            and provenance.source_anchor_event_cut <= start
            and self.fill_load_start <= provenance.source_anchor_event_cut
            and self.fill_load_start <= start
            and self.fill_checked_through >= end
            and self.checkpoint_event_cut >= end
            and provenance.checked_through is not None
            and provenance.checked_through >= end
            and self.evidence_observed_at == provenance.observed_at
            and provenance.checked_through == self.checkpoint_event_cut
            and provenance.checked_through == provenance.observed_at
            and self.page_exhausted == provenance.page_exhausted
            and self.not_truncated == (not provenance.truncated)
            and self.fill_checked_through == provenance.checked_through
            and (expected_scope is None or self.stream_scope == expected_scope)
            and self.evidence_observed_at is not None
            and self.evidence_observed_at.tzinfo is not None
            and self.evidence_observed_at.utcoffset() is not None
        )


def compose_fact_coverage(
    evidence: CoverageEvidence | None,
    *,
    start: datetime,
    end: datetime,
    expected_scope: AccountFactStreamScope | None = None,
) -> FactCoverageInterval:
    """Build coverage only from proven evidence — never from empty attributes.

    A non-empty cursor or checkpoint id is not enough: the window must be
    bracketed by load start, fill check-through, and checkpoint event cut.
    """
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("coverage bounds must be timezone-aware")
    if end < start:
        raise ValueError("coverage end must not precede start")

    if evidence is not None and evidence.proves_complete(
        start,
        end,
        expected_scope=expected_scope,
    ):
        checked_through = evidence.fill_checked_through
        checkpoint_cut = evidence.checkpoint_event_cut
        load_start = evidence.fill_load_start
        assert checked_through is not None
        assert checkpoint_cut is not None
        assert load_start is not None
        return FactCoverageInterval(
            start_at=max(start, load_start),
            end_at=min(end, checked_through, checkpoint_cut),
            source_cursor=evidence.fill_cursor_id,
            status=FactCoverageStatus.CONFIRMED,
            confirmed_revision=None,
            stream_scope=evidence.stream_scope,
            evidence_observed_at=evidence.evidence_observed_at,
            checkpoint_id=evidence.checkpoint_id,
            checkpoint_event_cut=evidence.checkpoint_event_cut,
            load_provenance=evidence.load_provenance,
            page_exhausted=evidence.page_exhausted,
            not_truncated=evidence.not_truncated,
        )

    return FactCoverageInterval(
        start_at=start,
        end_at=end,
        source_cursor=(evidence.fill_cursor_id if evidence is not None else None),
        status=FactCoverageStatus.PENDING,
        confirmed_revision=None,
        stream_scope=(evidence.stream_scope if evidence is not None else None),
        evidence_observed_at=(
            evidence.evidence_observed_at if evidence is not None else None
        ),
        checkpoint_id=(
            evidence.checkpoint_id
            if evidence is not None
            and isinstance(evidence.checkpoint_id, str)
            and evidence.checkpoint_id.strip()
            and evidence.checkpoint_event_cut is not None
            and evidence.checkpoint_event_cut.tzinfo is not None
            and evidence.checkpoint_event_cut.utcoffset() is not None
            else None
        ),
        checkpoint_event_cut=(
            evidence.checkpoint_event_cut
            if evidence is not None
            and isinstance(evidence.checkpoint_id, str)
            and evidence.checkpoint_id.strip()
            and evidence.checkpoint_event_cut is not None
            and evidence.checkpoint_event_cut.tzinfo is not None
            and evidence.checkpoint_event_cut.utcoffset() is not None
            else None
        ),
        load_provenance=(evidence.load_provenance if evidence is not None else None),
        page_exhausted=(evidence.page_exhausted if evidence is not None else False),
        not_truncated=(evidence.not_truncated if evidence is not None else False),
    )


class PositionHealthStatus(StrEnum):
    """
    Authoritative trading health status for a PositionKey per architecture RFC
    2026-09-25.
    """

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
        if not isinstance(self.position_side, FuturesPositionSide):
            object.__setattr__(
                self,
                "position_side",
                FuturesPositionSide(self.position_side),
            )

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
    source_anchor_kind: str

    def __post_init__(self) -> None:
        if not self.load_id.strip():
            raise ValueError("fill load id must not be empty")
        if not self.source_anchor_id.strip():
            raise ValueError("fill scan requires a source anchor")
        if self.source_anchor_kind not in {"zero_snapshot", "recovery_checkpoint"}:
            raise ValueError("unsupported fill scan source anchor kind")
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
        for name, value in (
            ("observed_at", self.observed_at),
            ("source_anchor_event_cut", self.source_anchor_event_cut),
        ):
            if value.tzinfo is None or value.utcoffset() is None:
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
class AccountFactConflict:
    """A persisted fact identity or payload conflict that blocks authority."""

    event_kind: str
    event_id: str
    details: str
    event_at: datetime | None = None

    def __post_init__(self) -> None:
        if not self.event_kind.strip():
            raise ValueError("event_kind must not be empty")
        if not self.event_id.strip():
            raise ValueError("event_id must not be empty")
        if self.event_at is not None and self.event_at.tzinfo is None:
            raise ValueError("event_at must be timezone-aware")


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
    checkpoint_id: str | None = None
    checkpoint_event_cut: datetime | None = None
    load_provenance: AccountFillLoadProvenance | None = None
    page_exhausted: bool = False
    not_truncated: bool = False

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
        if (self.checkpoint_id is None) != (self.checkpoint_event_cut is None):
            raise ValueError(
                "coverage checkpoint id and event cut must appear together"
            )
        if self.checkpoint_id is not None and not self.checkpoint_id.strip():
            raise ValueError("coverage checkpoint id must not be empty")
        if self.checkpoint_event_cut is not None and (
            self.checkpoint_event_cut.tzinfo is None
            or self.checkpoint_event_cut.utcoffset() is None
        ):
            raise ValueError("coverage checkpoint cut must be timezone-aware")
        if (
            type(self.page_exhausted) is not bool
            or type(self.not_truncated) is not bool
        ):
            raise ValueError("coverage pagination flags must be booleans")
        if self.load_provenance is not None:
            if self.stream_scope != self.load_provenance.stream_scope:
                raise ValueError("coverage load provenance scope mismatch")
            if (
                self.evidence_observed_at is not None
                and self.load_provenance.observed_at > self.evidence_observed_at
            ):
                raise ValueError("coverage predates its load provenance")
            if self.page_exhausted != self.load_provenance.page_exhausted:
                raise ValueError("coverage pagination exhaustion mismatch")
            if self.not_truncated == self.load_provenance.truncated:
                raise ValueError("coverage truncation status mismatch")
        if not isinstance(self.status, FactCoverageStatus):
            object.__setattr__(
                self,
                "status",
                FactCoverageStatus(self.status),
            )

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
            self.page_exhausted
            and self.not_truncated
            and self.load_provenance.is_complete
            and self.stream_scope == self.load_provenance.stream_scope
            and self.load_provenance.source_anchor_event_cut <= self.start_at
            and self.load_provenance.checked_through is not None
            and self.load_provenance.checked_through >= self.end_at
        )


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
class JournalFactDelta:
    """Append-only fact events recorded since the last durable persist.

    Only append-only categories live here. Fills, snapshots and exit boundaries
    cannot change after they are recorded, so re-sending the whole history on
    every observation was duplicated client work. Coverage, checkpoints,
    cursor/load provenance, conflicts and integrity issues keep the full history
    because their rows carry current state.
    """

    fills: tuple[AccountFillEvent, ...] = ()
    snapshots: tuple[AccountPositionSnapshot, ...] = ()
    exit_boundaries: tuple[ExitOrderSubmissionFact, ...] = ()


@lru_cache(maxsize=None)
def _dataclass_field_names(cls: type) -> tuple[str, ...]:
    """Field names per dataclass type; the reflection is not free per fact."""
    return tuple(field.name for field in fields(cls))


def _canonical_value(value: object) -> object:
    """Canonical, JSON-ready projection of a facts field value."""
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, Decimal):
        exact = format(value, "f")
        if "." in exact:
            exact = exact.rstrip("0").rstrip(".")
        return "0" if exact in {"", "-0"} else exact
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if is_dataclass(value):
        return {
            name: _canonical_value(getattr(value, name))
            for name in _dataclass_field_names(type(value))
        }
    if isinstance(value, dict):
        return {
            str(key): _canonical_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, tuple | list):
        return [_canonical_value(item) for item in value]
    return value


def _canonical_sort_key(element: object) -> str:
    return json.dumps(element, sort_keys=True, separators=(",", ":"))


class CanonicalFactCache:
    """Canonical encoding per recorded fact, keyed by object identity.

    ``compute_facts_hash`` re-encodes every historical fact on each call, and
    profiling shows that encoding — not the sort or the digest — dominates the
    cost: canonicalising 10k facts spends ~0.16s in the walk itself and ~0.05s in
    JSON, against ~0.01s of sorting and far less for SHA-256.

    Recorded facts are append-only and never mutated, so their canonical element
    and sort key are computed once and reused. The same element object is handed
    out on every hit, so callers must treat it as read-only; the only consumer is
    ``json.dumps`` inside ``compute_facts_hash``.

    Entries key on ``id(fact)`` while holding a strong reference to that fact, so
    a recycled id can never alias an older entry. A transaction candidate shares
    the journal's cache; a rolled-back candidate can therefore leave entries for
    facts that are never published, which is bounded by a few hundred bytes per
    such fact and never affects correctness (entries are keyed by identity).
    """

    __slots__ = ("_entries", "hits", "misses")

    def __init__(self) -> None:
        self._entries: dict[int, tuple[object, object, str]] = {}
        self.hits = 0
        self.misses = 0

    def entry(self, fact: object) -> tuple[object, str]:
        key = id(fact)
        cached = self._entries.get(key)
        if cached is not None and cached[0] is fact:
            self.hits += 1
            return cached[1], cached[2]
        element = _canonical_value(fact)
        sort_key = _canonical_sort_key(element)
        self._entries[key] = (fact, element, sort_key)
        self.misses += 1
        return element, sort_key

    def element(self, fact: object) -> object:
        return self.entry(fact)[0]

    def ordered(self, values: tuple[object, ...]) -> list[object]:
        pairs = [self.entry(item) for item in values]
        return [element for element, _ in sorted(pairs, key=lambda pair: pair[1])]

    def __len__(self) -> int:
        return len(self._entries)


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
    conflicting_fills: tuple[AccountFillEvent, ...] = ()
    has_late_events: bool = False
    stream_scope: AccountFactStreamScope | None = None
    recovery_checkpoint: PositionRecoveryCheckpoint | None = None
    fact_conflicts: tuple[AccountFactConflict, ...] = ()
    integrity_issues: tuple[str, ...] = ()
    late_fills: tuple[AccountFillEvent, ...] = ()
    fill_cursor_provenance: AccountFillReconciliationCursor | None = None
    fill_load_provenance: AccountFillLoadProvenance | None = None
    prefix_facts_complete: bool = True
    _cached_facts_hash: str | None = field(
        default=None, init=False, repr=False, compare=False, hash=False
    )
    # Derived encoding cache owned by the journal that produced these facts; it
    # never participates in equality, hashing or identity.
    _canonical_fact_cache: CanonicalFactCache | None = field(
        default=None, repr=False, compare=False, hash=False
    )

    def __post_init__(self) -> None:
        if type(self.prefix_facts_complete) is not bool:
            raise ValueError("prefix_facts_complete must be a boolean")
        if (
            self.fill_load_provenance is None
            and self.coverage is not None
            and self.coverage.load_provenance is not None
        ):
            object.__setattr__(
                self, "fill_load_provenance", self.coverage.load_provenance
            )
        if self.fill_load_provenance is not None and (
            self.fill_load_provenance.stream_scope != self.stream_scope
        ):
            raise ValueError("fill load provenance scope does not match account facts")
        if self.coverage is not None:
            if (
                self.stream_scope is not None
                and self.coverage.stream_scope != self.stream_scope
            ):
                raise ValueError("coverage scope does not match account facts")

    def compute_facts_hash(self) -> str:
        """Hash every input field that can change identity or projection."""
        cached = getattr(self, "_cached_facts_hash", None)
        if cached is not None:
            return cached

        encoding_cache = self._canonical_fact_cache

        def canonical(value: object) -> object:
            if encoding_cache is None:
                return _canonical_value(value)
            return encoding_cache.element(value)

        def unordered(values: tuple[object, ...]) -> list[object]:
            if encoding_cache is not None:
                return encoding_cache.ordered(values)
            encoded = []
            for item in values:
                element = _canonical_value(item)
                encoded.append((element, _canonical_sort_key(element)))
            return [element for element, _ in sorted(encoded, key=lambda pair: pair[1])]

        fact_material = {
            "position_key": canonical(self.position_key),
            "stream_scope": canonical(self.stream_scope),
            "fills": unordered(self.fills),
            "conflicting_fills": unordered(self.conflicting_fills),
            "snapshots": unordered(self.snapshots),
            "exit_boundaries": unordered(self.exit_boundaries),
            "coverage": canonical(self.coverage),
            "legacy_checkpoint": canonical(self.checkpoint),
            "recovery_checkpoint": canonical(self.recovery_checkpoint),
            "has_synthetic_fills": self.has_synthetic_fills,
            "has_late_events": self.has_late_events,
            "fact_conflicts": unordered(self.fact_conflicts),
            "integrity_issues": sorted(self.integrity_issues),
            "late_fills": unordered(self.late_fills),
            "fill_cursor_provenance": canonical(self.fill_cursor_provenance),
            "fill_load_provenance": canonical(self.fill_load_provenance),
            "prefix_facts_complete": self.prefix_facts_complete,
        }
        encoded = json.dumps(
            fact_material,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        computed = hashlib.sha256(encoded).hexdigest()
        object.__setattr__(self, "_cached_facts_hash", computed)
        return computed


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
    projection_version: str | None = None
    stream_scope: AccountFactStreamScope | None = None


@dataclass(frozen=True, slots=True)
class FreshnessRequirement:
    """Freshness constraints for reading an authoritative PositionView."""

    max_staleness: timedelta = timedelta(seconds=15)
    min_event_cut: datetime | None = None
    require_comparable: bool = True


@dataclass(frozen=True, slots=True)
class PositionView:
    """
    Authoritative, immutable point-in-time view consumed by strategy and execution
    coordinators.
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
    def is_ready_for_trade(self) -> bool:
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
        is_ready_health = (
            self.health_status == PositionHealthStatus.READY
            and self.is_comparable
        )
        is_clean_stream_no_coverage = (
            self.stream_scope is not None
            and self.discrepancy is None
            and self.health_status == PositionHealthStatus.CATCHING_UP
            and self.coverage is None
            and bool(self.diagnostics)
            and all(
                d == "No durable coverage evidence exists for stream scope"
                for d in self.diagnostics
            )
        )
        return (
            (is_ready_health or is_clean_stream_no_coverage)
            and (
                self.reconciliation_gap is None
                or self.reconciliation_gap == Decimal("0")
            )
            and self.unallocated_quantity == Decimal("0")
            and (
                has_confirmed_coverage
                or (self.coverage is None and self.key.environment != "live")
                or self.zero_position_snapshot_confirmed
                or is_clean_stream_no_coverage
            )
        )
