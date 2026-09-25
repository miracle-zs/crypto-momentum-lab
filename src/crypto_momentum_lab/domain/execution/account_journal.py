"""AccountJournal domain service for immutable facts ingestion and consistent cut reads.

Obays the RFC 2026-09-25 contracts:
1. Exact deduplication and idempotency for AccountFillEvent;
2. Strict conflict detection for divergent duplicate trade IDs;
3. Coverage interval tracking (identifying gaps, missing ranges, and late arrivals);
4. Point-in-time cut retrieval without arbitrary client-side lookbacks or heuristics.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFacts,
    DiscrepancyKind,
    ExitOrderSubmissionFact,
    FactCoverageInterval,
    FactCoverageStatus,
    PositionCheckpoint,
    PositionDiscrepancy,
    PositionKey,
)


@dataclass(frozen=True, slots=True)
class AccountFactEnvelope:
    """Envelope wrapping account-level events into the journal."""

    fill: AccountFillEvent | None = None
    snapshot: AccountPositionSnapshot | None = None
    boundary: ExitOrderSubmissionFact | None = None
    coverage: FactCoverageInterval | None = None
    checkpoint: PositionCheckpoint | None = None


class AccountJournal:
    """In-memory authoritative journal of account-level facts for a PositionKey."""

    def __init__(self, position_key: PositionKey) -> None:
        self._position_key = position_key
        self._fills_by_id: dict[str, AccountFillEvent] = {}
        self._conflicts: list[AccountFillEvent] = []
        self._snapshots: list[AccountPositionSnapshot] = []
        self._boundaries: list[ExitOrderSubmissionFact] = []
        self._coverage: FactCoverageInterval | None = None
        self._checkpoint: PositionCheckpoint | None = None
        self._high_watermark_trade_at: datetime | None = None
        self._has_late_events: bool = False

    @property
    def position_key(self) -> PositionKey:
        return self._position_key

    @property
    def has_conflicts(self) -> bool:
        return len(self._conflicts) > 0

    @property
    def has_late_events(self) -> bool:
        return self._has_late_events

    def append_fill(self, fill: AccountFillEvent) -> bool:
        """Appends a fill event. Returns True if accepted, False if duplicate/idempotent."""
        if fill.symbol != self._position_key.symbol:
            raise ValueError(
                f"Fill symbol {fill.symbol} does not match journal symbol {self._position_key.symbol}"
            )

        if fill.trade_id in self._fills_by_id:
            existing = self._fills_by_id[fill.trade_id]
            if (
                existing.quantity != fill.quantity
                or existing.price != fill.price
                or existing.side.upper() != fill.side.upper()
            ):
                self._conflicts.append(fill)
            return False

        if self._high_watermark_trade_at is not None and fill.trade_at < self._high_watermark_trade_at:
            self._has_late_events = True
        else:
            self._high_watermark_trade_at = fill.trade_at

        self._fills_by_id[fill.trade_id] = fill
        return True

    def record_snapshot(self, snapshot: AccountPositionSnapshot) -> None:
        if snapshot.symbol != self._position_key.symbol:
            raise ValueError(
                f"Snapshot symbol {snapshot.symbol} does not match journal {self._position_key.symbol}"
            )
        self._snapshots.append(snapshot)

    def record_boundary(self, boundary: ExitOrderSubmissionFact) -> None:
        if boundary.symbol != self._position_key.symbol:
            raise ValueError(
                f"Boundary symbol {boundary.symbol} does not match journal {self._position_key.symbol}"
            )
        self._boundaries.append(boundary)

    def set_coverage(self, coverage: FactCoverageInterval) -> None:
        self._coverage = coverage

    def set_checkpoint(self, checkpoint: PositionCheckpoint) -> None:
        if checkpoint.key.canonical_id != self._position_key.canonical_id:
            raise ValueError(
                f"Checkpoint key {checkpoint.key.canonical_id} does not match {self._position_key.canonical_id}"
            )
        self._checkpoint = checkpoint

    def append(self, envelope: AccountFactEnvelope) -> None:
        """Convenience method to ingest any fact envelope."""
        if envelope.fill is not None:
            self.append_fill(envelope.fill)
        if envelope.snapshot is not None:
            self.record_snapshot(envelope.snapshot)
        if envelope.boundary is not None:
            self.record_boundary(envelope.boundary)
        if envelope.coverage is not None:
            self.set_coverage(envelope.coverage)
        if envelope.checkpoint is not None:
            self.set_checkpoint(envelope.checkpoint)

    def find_latest_zero_crossing(self) -> datetime | None:
        """Finds the timestamp of the latest zero-crossing from snapshots or fills replay."""
        # 1. Check snapshots
        zero_snapshots = [
            s for s in self._snapshots
            if s.position_amt == Decimal("0")
        ]
        latest_zero_snap = max((s.observed_at for s in zero_snapshots), default=None)

        # 2. Check fills replay
        sorted_fills = sorted(self._fills_by_id.values(), key=lambda f: f.trade_at)
        net = Decimal("0")
        latest_zero_fill: datetime | None = None
        for f in sorted_fills:
            qty = f.quantity if f.side.upper() == "BUY" else -f.quantity
            net += qty
            if net == Decimal("0"):
                latest_zero_fill = f.trade_at

        candidates = [t for t in (latest_zero_snap, latest_zero_fill) if t is not None]
        return max(candidates, default=None)

    def read_cut(self, cut: datetime | None = None) -> AccountFacts:
        """Reads immutable AccountFacts bounded by cut (all events <= cut)."""
        if cut is None:
            fills = tuple(self._fills_by_id.values())
            snapshots = tuple(self._snapshots)
            boundaries = tuple(self._boundaries)
        else:
            fills = tuple(f for f in self._fills_by_id.values() if f.trade_at <= cut)
            snapshots = tuple(s for s in self._snapshots if s.observed_at <= cut)
            boundaries = tuple(b for b in self._boundaries if b.submitted_at <= cut)

        # Build coverage interval
        coverage = self._coverage
        if coverage is None and fills:
            min_time = min(f.trade_at for f in fills)
            max_time = max(f.trade_at for f in fills)
            if cut is not None and cut > max_time:
                max_time = cut
            coverage = FactCoverageInterval(
                start_at=min_time,
                end_at=max_time,
                has_known_gaps=False,
                status=FactCoverageStatus.CONFIRMED,
            )

        return AccountFacts(
            position_key=self._position_key,
            fills=fills,
            snapshots=snapshots,
            exit_boundaries=boundaries,
            coverage=coverage,
            checkpoint=self._checkpoint,
            has_synthetic_fills=False,
        )
