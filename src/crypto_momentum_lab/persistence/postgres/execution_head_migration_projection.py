"""Canonical projection encoding used by the legacy execution-head migration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    PositionHealthStatus,
    PositionKey,
    PositionLedgerBatch,
)
from crypto_momentum_lab.domain.trading import TradeSide as StrategySide


class FactCoverageStatus(StrEnum):
    """Integrity status retained while converting legacy coverage records."""

    CONFIRMED = "CONFIRMED"
    GAP_DETECTED = "GAP_DETECTED"
    PENDING = "PENDING"


class DiscrepancyKind(StrEnum):
    """Legacy discrepancy categories accepted by the head migration."""

    INPUT_MISSING = "INPUT_MISSING"
    TIME_MISALIGNED = "TIME_MISALIGNED"
    QUANTITY_MISMATCH = "QUANTITY_MISMATCH"
    PRICE_MISMATCH = "PRICE_MISMATCH"
    IDENTITY_MISMATCH = "IDENTITY_MISMATCH"
    BOUNDARY_MISMATCH = "BOUNDARY_MISMATCH"
    PENDING_BINDING = "PENDING_BINDING"


@dataclass(frozen=True, slots=True)
class PositionDiscrepancy:
    """Legacy discrepancy payload retained for migration decoding."""

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
class AccountFactStreamScope:
    """Legacy source scope retained while converting execution heads."""

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


@dataclass(frozen=True, slots=True)
class AccountFillLoadProvenance:
    """Legacy paginated fill scan metadata retained for migration decoding."""

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

@dataclass(frozen=True, slots=True)
class FactCoverageInterval:
    """Legacy fact coverage payload retained for migration decoding."""

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


@dataclass(frozen=True, slots=True)
class LegacyPositionEpisode:
    """Schema-1 episode shape, including its nested batch copy."""

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
class LegacyPositionProjection:
    """Schema-1 projection shape consumed only while migrating execution heads."""

    position_key: PositionKey
    active_episode: LegacyPositionEpisode | None
    unallocated_quantity: Decimal
    reconciliation_gap: Decimal
    high_watermark_trade_at: datetime | None
    archived_episodes: tuple[LegacyPositionEpisode, ...] = ()
    diagnostics: tuple[str, ...] = ()
    health_status: PositionHealthStatus = PositionHealthStatus.READY
    event_cut: datetime | None = None
    discrepancy: PositionDiscrepancy | None = None
    is_comparable: bool = True
    projection_version: str | None = None
    stream_scope: AccountFactStreamScope | None = None

    @property
    def active_batches(self) -> tuple[PositionLedgerBatch, ...]:
        if self.active_episode is None:
            return ()
        return tuple(
            batch for batch in self.active_episode.batches if batch.quantity > 0
        )

    @property
    def total_active_quantity(self) -> Decimal:
        return sum(
            (batch.quantity for batch in self.active_batches), start=Decimal("0")
        )


def encode_scope(scope: AccountFactStreamScope) -> dict[str, object]:
    return {
        "environment": scope.environment,
        "account_label": scope.account_label,
        "symbol": scope.symbol,
        "position_side": scope.position_side.value,
        "stream_id": scope.stream_id,
        "stream_epoch": scope.stream_epoch,
    }


def encode_position_key(key: PositionKey) -> dict[str, object]:
    return {
        "environment": key.environment,
        "account_label": key.account_label,
        "symbol": key.symbol,
        "position_side": key.position_side.value,
    }


def encode_batch(batch: PositionLedgerBatch) -> dict[str, object]:
    return {
        "batch_id": batch.batch_id,
        "episode_id": batch.episode_id,
        "quantity": _decimal(batch.quantity),
        "original_quantity": _decimal(batch.original_quantity),
        "entry_price": _decimal(batch.entry_price),
        "opened_at": _datetime(batch.opened_at),
        "order_id": batch.order_id,
        "client_order_id": batch.client_order_id,
        "entry_order_ids": list(batch.entry_order_ids),
        "entry_client_order_ids": list(batch.entry_client_order_ids),
        "is_external": batch.is_external,
        "exit_order_submitted_at": (
            _datetime(batch.exit_order_submitted_at)
            if batch.exit_order_submitted_at is not None
            else None
        ),
    }


def encode_episode(episode: LegacyPositionEpisode | None) -> object:
    if episode is None:
        return None
    return {
        "episode_id": episode.episode_id,
        "position_key": encode_position_key(episode.position_key),
        "side": episode.side.value,
        "opened_at": _datetime(episode.opened_at),
        "closed_at": (
            _datetime(episode.closed_at) if episode.closed_at is not None else None
        ),
        "is_active": episode.is_active,
        "cumulative_bought": _decimal(episode.cumulative_bought),
        "cumulative_sold": _decimal(episode.cumulative_sold),
        "peak_quantity": _decimal(episode.peak_quantity),
        "batches": [encode_batch(batch) for batch in episode.batches],
        "reductions": [],
    }


def encode_discrepancy(
    discrepancy: PositionDiscrepancy | None,
) -> object:
    if discrepancy is None:
        return None
    return {
        "discrepancy_id": discrepancy.discrepancy_id,
        "key": encode_position_key(discrepancy.key),
        "kind": discrepancy.kind.value,
        "first_seen_at": _datetime(discrepancy.first_seen_at),
        "last_seen_at": _datetime(discrepancy.last_seen_at),
        "count": discrepancy.count,
        "input_hash": discrepancy.input_hash,
        "details": discrepancy.details,
        "event_cut": (
            _datetime(discrepancy.event_cut)
            if discrepancy.event_cut is not None
            else None
        ),
        "snapshot_at": (
            _datetime(discrepancy.snapshot_at)
            if discrepancy.snapshot_at is not None
            else None
        ),
        "first_divergent_fact": discrepancy.first_divergent_fact,
        "is_reconciled": discrepancy.is_reconciled,
        "resolution_evidence": discrepancy.resolution_evidence,
    }


def encode_projection(
    projection: LegacyPositionProjection,
    *,
    include_derived: bool = True,
) -> dict[str, object]:
    payload = {
        "position_key": encode_position_key(projection.position_key),
        "active_episode": encode_episode(projection.active_episode),
        "unallocated_quantity": _decimal(projection.unallocated_quantity),
        "reconciliation_gap": _decimal(projection.reconciliation_gap),
        "high_watermark_trade_at": (
            _datetime(projection.high_watermark_trade_at)
            if projection.high_watermark_trade_at is not None
            else None
        ),
        "archived_episodes": [
            encode_episode(item) for item in projection.archived_episodes
        ],
        "diagnostics": list(projection.diagnostics),
        "health_status": projection.health_status.value,
        "event_cut": (
            _datetime(projection.event_cut)
            if projection.event_cut is not None
            else None
        ),
        "discrepancy": encode_discrepancy(projection.discrepancy),
        "is_comparable": projection.is_comparable,
        "projection_version": projection.projection_version,
        "stream_scope": (
            encode_scope(projection.stream_scope)
            if projection.stream_scope is not None
            else None
        ),
    }
    if include_derived:
        payload["active_batches"] = [
            encode_batch(item) for item in projection.active_batches
        ]
        payload["total_active_quantity"] = _decimal(projection.total_active_quantity)
    return payload


def _decimal(value: Decimal) -> str:
    return format(value, "f")


def _datetime(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("recovery datetimes must be timezone-aware")
    return value.isoformat()


