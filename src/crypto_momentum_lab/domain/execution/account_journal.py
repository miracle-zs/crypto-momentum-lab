"""AccountJournal domain service for immutable facts ingestion and consistent cut reads.

Obays the RFC 2026-09-25 contracts:
1. Exact deduplication and idempotency for AccountFillEvent;
2. Strict conflict detection for divergent duplicate trade IDs;
3. Coverage interval tracking (identifying gaps, missing ranges, and late arrivals);
4. Point-in-time cut retrieval without arbitrary client-side lookbacks or heuristics.
"""

from __future__ import annotations

import copy

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountFillReconciliationCursor,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactConflict,
    AccountFacts,
    AccountFactStreamScope,
    AccountFillLoadProvenance,
    ExitOrderSubmissionFact,
    FactCoverageInterval,
    JournalFactDelta,
    PositionCheckpoint,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.recovery_models import (
    DurableJournalCut,
    PositionRecoveryCheckpoint,
)


@dataclass(frozen=True, slots=True)
class AccountFactEnvelope:
    """Envelope wrapping account-level events into the journal."""

    fill: AccountFillEvent | None = None
    snapshot: AccountPositionSnapshot | None = None
    boundary: ExitOrderSubmissionFact | None = None
    coverage: FactCoverageInterval | None = None
    checkpoint: PositionCheckpoint | None = None
    recovery_checkpoint: PositionRecoveryCheckpoint | None = None
    conflict: AccountFactConflict | None = None
    fill_load_provenance: AccountFillLoadProvenance | None = None


class AccountJournal:
    """In-memory authoritative journal of account-level facts for a PositionKey."""

    def __init__(
        self,
        position_key: PositionKey,
        *,
        stream_scope: AccountFactStreamScope | None = None,
    ) -> None:
        if stream_scope is not None and not stream_scope.matches(position_key):
            raise ValueError("stream scope does not match journal position key")
        self._position_key = position_key
        self._stream_scope = stream_scope
        self._fills_by_id: dict[str, AccountFillEvent] = {}
        self._conflicts: list[AccountFillEvent] = []
        self._fact_conflicts: list[AccountFactConflict] = []
        self._integrity_issues: list[str] = []
        self._snapshots: list[AccountPositionSnapshot] = []
        self._boundaries: list[ExitOrderSubmissionFact] = []
        self._coverage: FactCoverageInterval | None = None
        self._checkpoint: PositionCheckpoint | None = None
        self._recovery_checkpoint: PositionRecoveryCheckpoint | None = None
        self._high_watermark_trade_at: datetime | None = None
        self._has_late_events: bool = False
        self._late_trade_ids: set[str] = set()
        self._has_synthetic_fills: bool = False
        self._prefix_facts_complete: bool = True
        self._fill_cursor_provenance: AccountFillReconciliationCursor | None = None
        self._fill_load_provenance: AccountFillLoadProvenance | None = None
        self._revision: int = 0
        self._latest_event_at: datetime | None = None
        self._cached_facts_none: AccountFacts | None = None
        # Append-only events recorded since the last successful durable persist.
        # They are re-sent after a failed or lost transaction and dropped once
        # the commit published this journal.
        self._pending_fills: list[AccountFillEvent] = []
        self._pending_snapshots: list[AccountPositionSnapshot] = []
        self._pending_boundaries: list[ExitOrderSubmissionFact] = []

    @property
    def revision(self) -> int:
        return self._revision

    @property
    def latest_event_at(self) -> datetime | None:
        return self._latest_event_at

    def _update_latest_event_at(self, dt: datetime | None) -> None:
        if dt is not None:
            if self._latest_event_at is None or dt > self._latest_event_at:
                self._latest_event_at = dt

    @property
    def position_key(self) -> PositionKey:
        return self._position_key

    @property
    def has_conflicts(self) -> bool:
        return len(self._conflicts) > 0 or len(self._fact_conflicts) > 0

    @property
    def has_late_events(self) -> bool:
        return self._has_late_events

    @property
    def stream_scope(self) -> AccountFactStreamScope | None:
        return self._stream_scope

    def append_fill(self, fill: AccountFillEvent) -> bool:
        """
        Appends a fill event. Returns True if accepted, False if
        duplicate/idempotent.
        """
        if fill.symbol != self._position_key.symbol:
            raise ValueError(
                f"Fill symbol {fill.symbol} does not match journal "
                f"symbol {self._position_key.symbol}"
            )
        if (
            fill.environment != self._position_key.environment
            or fill.account_label != self._position_key.account_label
        ):
            raise ValueError(
                "Fill account identity does not match journal position key"
            )
        raw_position_side = fill.raw_position_side
        if (
            raw_position_side is not None
            and str(raw_position_side).upper() != self._position_key.position_side.value
        ):
            raise ValueError("Fill position side does not match journal position key")
        if (
            raw_position_side is None
            and self._position_key.position_side != FuturesPositionSide.BOTH
        ):
            issue = (
                f"Fill {fill.trade_id} has no positionSide for side-specific journal"
            )
            if issue not in self._integrity_issues:
                self._integrity_issues.append(issue)
                self._revision += 1

        if fill.trade_id in self._fills_by_id:
            existing = self._fills_by_id[fill.trade_id]
            if not _same_fill(existing, fill):
                self._conflicts.append(fill)
                self._has_synthetic_fills = self._has_synthetic_fills or bool(
                    (fill.raw_payload or {}).get("synthetic_from_order", False)
                )
                self._revision += 1
            return False

        self._has_synthetic_fills = self._has_synthetic_fills or bool(
            (fill.raw_payload or {}).get("synthetic_from_order", False)
        )

        checkpoint_cut = (
            self._recovery_checkpoint.event_cut
            if self._recovery_checkpoint is not None
            else None
        )
        if checkpoint_cut is not None and fill.trade_at <= checkpoint_cut:
            self._has_late_events = True
            self._late_trade_ids.add(fill.trade_id)
        if (
            self._high_watermark_trade_at is not None
            and fill.trade_at < self._high_watermark_trade_at
        ):
            self._has_late_events = True
            self._late_trade_ids.add(fill.trade_id)
        else:
            self._high_watermark_trade_at = max(
                filter(
                    None,
                    (self._high_watermark_trade_at, fill.trade_at),
                )
            )

        self._fills_by_id[fill.trade_id] = fill
        self._pending_fills.append(fill)
        self._update_latest_event_at(fill.trade_at)
        self._cached_facts_none = None
        self._revision += 1
        return True

    def record_snapshot(self, snapshot: AccountPositionSnapshot) -> None:
        if snapshot.symbol != self._position_key.symbol:
            raise ValueError(
                f"Snapshot symbol {snapshot.symbol} does not match "
                f"journal {self._position_key.symbol}"
            )
        if (
            snapshot.environment != self._position_key.environment
            or snapshot.account_label != self._position_key.account_label
        ):
            raise ValueError("Snapshot account identity does not match journal key")
        if snapshot.position_side.upper() != self._position_key.position_side.value:
            raise ValueError(
                "Snapshot position side does not match journal position key"
            )
        self._snapshots.append(snapshot)
        self._pending_snapshots.append(snapshot)
        self._update_latest_event_at(snapshot.observed_at)
        self._cached_facts_none = None
        self._revision += 1

    def record_boundary(self, boundary: ExitOrderSubmissionFact) -> None:
        if boundary.symbol != self._position_key.symbol:
            raise ValueError(
                f"Boundary symbol {boundary.symbol} does not match "
                f"journal {self._position_key.symbol}"
            )
        if boundary.position_side != self._position_key.position_side:
            raise ValueError("Boundary position side does not match journal key")
        self._boundaries.append(boundary)
        self._pending_boundaries.append(boundary)
        self._update_latest_event_at(boundary.submitted_at)
        self._cached_facts_none = None
        self._revision += 1

    def set_coverage(self, coverage: FactCoverageInterval) -> None:
        if coverage.stream_scope is not None:
            if not coverage.stream_scope.matches(self._position_key):
                raise ValueError("Coverage scope does not match journal position key")
            if (
                self._stream_scope is not None
                and coverage.stream_scope != self._stream_scope
            ):
                raise ValueError("Coverage scope does not match journal stream scope")
        if coverage.load_provenance is not None:
            self.record_fill_load_provenance(coverage.load_provenance)
        if self._coverage != coverage:
            self._coverage = coverage
            if coverage.end_at is not None:
                self._update_latest_event_at(coverage.end_at)
            self._cached_facts_none = None
            self._revision += 1

    def set_checkpoint(self, checkpoint: PositionCheckpoint) -> None:
        if checkpoint.key.canonical_id != self._position_key.canonical_id:
            raise ValueError(
                f"Checkpoint key {checkpoint.key.canonical_id} does not "
                f"match {self._position_key.canonical_id}"
            )
        self._checkpoint = checkpoint
        self._update_latest_event_at(checkpoint.event_cut)
        self._cached_facts_none = None
        self._revision += 1

    def set_recovery_checkpoint(
        self,
        checkpoint: PositionRecoveryCheckpoint,
    ) -> None:
        if checkpoint.key.canonical_id != self._position_key.canonical_id:
            raise ValueError("Recovery checkpoint position key does not match journal")
        if (
            self._stream_scope is not None
            and checkpoint.stream_scope != self._stream_scope
        ):
            raise ValueError("Recovery checkpoint stream scope does not match journal")
        if checkpoint.coverage is not None:
            self.set_coverage(checkpoint.coverage)
        self._recovery_checkpoint = checkpoint
        self._update_latest_event_at(checkpoint.event_cut)
        self._cached_facts_none = None
        self._high_watermark_trade_at = max(
            filter(
                None,
                (
                    self._high_watermark_trade_at,
                    checkpoint.projection.high_watermark_trade_at,
                ),
            ),
            default=None,
        )
        self._has_late_events = self._has_late_events or checkpoint.has_late_events
        self._revision = max(self._revision, checkpoint.source_revision)

    def record_conflict(self, conflict: AccountFactConflict) -> None:
        if conflict.event_at is not None and conflict.event_at.tzinfo is None:
            raise ValueError("Conflict event time must be timezone-aware")
        self._fact_conflicts.append(conflict)
        if conflict.event_at is not None:
            self._update_latest_event_at(conflict.event_at)
        self._cached_facts_none = None
        self._revision += 1

    def record_integrity_issue(self, issue: str) -> None:
        normalized = issue.strip()
        if not normalized:
            raise ValueError("integrity issue must not be empty")
        self._integrity_issues.append(normalized)
        self._cached_facts_none = None
        self._revision += 1

    def record_fill_cursor(
        self,
        cursor: AccountFillReconciliationCursor,
    ) -> None:
        if (
            cursor.environment != self._position_key.environment
            or cursor.account_label != self._position_key.account_label
            or cursor.symbol != self._position_key.symbol
        ):
            raise ValueError("fill cursor identity does not match journal position key")
        if (
            self._fill_cursor_provenance is not None
            and cursor.last_checked_at < self._fill_cursor_provenance.last_checked_at
        ):
            raise ValueError("fill cursor provenance cannot move backwards")
        if cursor != self._fill_cursor_provenance:
            self._fill_cursor_provenance = cursor
            self._update_latest_event_at(cursor.last_checked_at)
            self._cached_facts_none = None
            self._revision += 1

    def record_fill_load_provenance(
        self,
        provenance: AccountFillLoadProvenance,
    ) -> None:
        if self._stream_scope is None or provenance.stream_scope != self._stream_scope:
            raise ValueError("fill load provenance does not match journal stream scope")
        previous = self._fill_load_provenance
        if previous is not None and previous.load_id == provenance.load_id:
            if provenance == previous:
                return
            if previous.page_exhausted:
                raise ValueError("completed fill load cannot resume under the same id")
            if previous.next_from_id is None:
                raise ValueError("incomplete fill load has no durable resume cursor")
            if provenance.request_from_id != previous.next_from_id:
                raise ValueError("fill load resume cursor is discontinuous")
            if (
                provenance.scan_origin_from_id != previous.scan_origin_from_id
                or provenance.scan_origin_start_time_ms
                != previous.scan_origin_start_time_ms
                or provenance.source_anchor_id != previous.source_anchor_id
                or provenance.source_anchor_event_cut
                != previous.source_anchor_event_cut
                or provenance.source_anchor_kind != previous.source_anchor_kind
                or provenance.observed_at < previous.observed_at
            ):
                raise ValueError("fill load resume changed its origin or source anchor")
        elif provenance.scan_origin_from_id is not None:
            if provenance.request_from_id != provenance.scan_origin_from_id:
                raise ValueError("new fill load did not start at its declared origin")
        elif provenance.request_from_id is not None:
            raise ValueError("new time-origin fill load must start without a cursor")
        self._fill_load_provenance = provenance
        self._update_latest_event_at(provenance.observed_at)
        self._cached_facts_none = None
        self._revision += 1

    def copy_for_transaction(self) -> AccountJournal:
        """Copy the mutable containers, sharing the immutable recorded facts.

        Every fill, snapshot, boundary, coverage interval and checkpoint is a
        frozen dataclass that is treated as read-only once appended, so a
        transaction candidate only needs its own containers to stay isolated
        from the published journal. Rebuilding containers is O(container
        sizes) pointer work instead of the O(history) deepcopy that used to run
        on the event loop for every mutation.

        Any mutable container field added to this class must be copied here.
        """
        candidate = copy.copy(self)
        candidate._fills_by_id = dict(self._fills_by_id)
        candidate._conflicts = list(self._conflicts)
        candidate._fact_conflicts = list(self._fact_conflicts)
        candidate._integrity_issues = list(self._integrity_issues)
        candidate._snapshots = list(self._snapshots)
        candidate._boundaries = list(self._boundaries)
        candidate._late_trade_ids = set(self._late_trade_ids)
        candidate._pending_fills = list(self._pending_fills)
        candidate._pending_snapshots = list(self._pending_snapshots)
        candidate._pending_boundaries = list(self._pending_boundaries)
        return candidate

    def pending_fact_delta(self) -> JournalFactDelta:
        """Append-only facts recorded since the last successful durable persist."""
        return JournalFactDelta(
            fills=tuple(self._pending_fills),
            snapshots=tuple(self._pending_snapshots),
            exit_boundaries=tuple(self._pending_boundaries),
        )

    def mark_facts_persisted(self) -> None:
        """Drop the pending delta after its transaction committed.

        The durable rows are idempotent (``ON CONFLICT DO NOTHING`` on the
        derived event identity), so a delta that is never dropped is only ever
        re-sent, never lost. Callers must invoke this exactly when the
        candidate that recorded these events is published.
        """
        self._pending_fills.clear()
        self._pending_snapshots.clear()
        self._pending_boundaries.clear()

    @classmethod
    def from_durable_cut(cls, cut: DurableJournalCut) -> AccountJournal:
        """Restore the exact persisted cut without manufacturing missing facts."""
        journal = cls(cut.facts.position_key, stream_scope=cut.scope)
        for fill in cut.facts.fills:
            existing = journal._fills_by_id.get(fill.trade_id)
            if existing is None:
                journal._fills_by_id[fill.trade_id] = fill
            elif not _same_fill(existing, fill):
                journal._conflicts.append(fill)
        journal._conflicts.extend(cut.facts.conflicting_fills)
        journal._snapshots = list(cut.facts.snapshots)
        journal._boundaries = list(cut.facts.exit_boundaries)
        journal._coverage = cut.facts.coverage
        journal._checkpoint = cut.facts.checkpoint
        journal._recovery_checkpoint = cut.checkpoint
        journal._fact_conflicts = list(cut.facts.fact_conflicts)
        journal._fact_conflicts = list(
            dict.fromkeys((*journal._fact_conflicts, *cut.conflicts))
        )
        journal._integrity_issues = list(
            dict.fromkeys((*cut.facts.integrity_issues, *cut.integrity_issues))
        )
        journal._high_watermark_trade_at = max(
            (fill.trade_at for fill in journal._fills_by_id.values()),
            default=None,
        )
        journal._has_late_events = cut.facts.has_late_events or bool(
            cut.checkpoint and cut.checkpoint.has_late_events
        )
        journal._late_trade_ids = {fill.trade_id for fill in cut.facts.late_fills}
        journal._has_synthetic_fills = cut.facts.has_synthetic_fills or bool(
            cut.checkpoint and cut.checkpoint.has_synthetic_fills
        )
        journal._prefix_facts_complete = cut.facts.prefix_facts_complete
        journal._fill_cursor_provenance = (
            cut.facts.fill_cursor_provenance or cut.cursor_provenance
        )
        journal._fill_load_provenance = cut.facts.fill_load_provenance
        journal._revision = max(
            cut.revision,
            cut.checkpoint.source_revision if cut.checkpoint is not None else 0,
        )
        timestamps = [
            *(fill.trade_at for fill in journal._fills_by_id.values()),
            *(s.observed_at for s in journal._snapshots),
            *(b.submitted_at for b in journal._boundaries),
            *(c.event_at for c in journal._fact_conflicts if c.event_at is not None),
        ]
        if journal._recovery_checkpoint is not None:
            timestamps.append(journal._recovery_checkpoint.event_cut)
        if journal._checkpoint is not None:
            timestamps.append(journal._checkpoint.event_cut)
        if journal._coverage is not None and journal._coverage.end_at is not None:
            timestamps.append(journal._coverage.end_at)
        journal._latest_event_at = max(timestamps, default=None)
        return journal

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
        if envelope.recovery_checkpoint is not None:
            self.set_recovery_checkpoint(envelope.recovery_checkpoint)
        if envelope.conflict is not None:
            self.record_conflict(envelope.conflict)
        if envelope.fill_load_provenance is not None:
            self.record_fill_load_provenance(envelope.fill_load_provenance)

    def find_latest_zero_crossing(self) -> datetime | None:
        """
        Finds the timestamp of the latest zero-crossing from snapshots or fills
        replay.
        """
        # 1. Check snapshots
        zero_snapshots = [s for s in self._snapshots if s.position_amt == Decimal("0")]
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
        if self._recovery_checkpoint is not None:
            candidates.extend(
                episode.closed_at
                for episode in self._recovery_checkpoint.projection.archived_episodes
                if episode.closed_at is not None
            )
        return max(candidates, default=None)

    def read_cut(self, cut: datetime | None = None) -> AccountFacts:
        """Reads immutable AccountFacts bounded by cut (all events <= cut)."""
        if cut is not None and (cut.tzinfo is None or cut.utcoffset() is None):
            raise ValueError("cut must be timezone-aware")

        latest = self._latest_event_at
        cov = self._coverage
        cov_end = cov.end_at if cov is not None else None
        max_ts = latest
        if cov_end is not None:
            max_ts = max(max_ts, cov_end) if max_ts is not None else cov_end

        if cut is not None and (max_ts is None or cut >= max_ts):
            cut = None

        if cut is None:
            if self._cached_facts_none is not None:
                return self._cached_facts_none
            fills = tuple(self._fills_by_id.values())
            snapshots = tuple(self._snapshots)
            boundaries = tuple(self._boundaries)
            conflicts = tuple(self._conflicts)
            fact_conflicts = tuple(self._fact_conflicts)
            recovery_checkpoint = self._recovery_checkpoint
            checkpoint = self._checkpoint
            cursor_provenance = self._fill_cursor_provenance
            fill_load_provenance = self._fill_load_provenance
        else:
            fills = tuple(f for f in self._fills_by_id.values() if f.trade_at <= cut)
            snapshots = tuple(s for s in self._snapshots if s.observed_at <= cut)
            boundaries = tuple(b for b in self._boundaries if b.submitted_at <= cut)
            conflicts = tuple(f for f in self._conflicts if f.trade_at <= cut)
            fact_conflicts = tuple(
                conflict
                for conflict in self._fact_conflicts
                if conflict.event_at is None or conflict.event_at <= cut
            )
            recovery_checkpoint = (
                self._recovery_checkpoint
                if self._recovery_checkpoint is not None
                and self._recovery_checkpoint.event_cut <= cut
                else None
            )
            checkpoint = (
                self._checkpoint
                if self._checkpoint is not None and self._checkpoint.event_cut <= cut
                else None
            )
            cursor_provenance = (
                self._fill_cursor_provenance
                if self._fill_cursor_provenance is not None
                and self._fill_cursor_provenance.last_checked_at <= cut
                else None
            )
            fill_load_provenance = (
                self._fill_load_provenance
                if self._fill_load_provenance is not None
                and self._fill_load_provenance.observed_at <= cut
                else None
            )

        coverage = self._coverage
        if cut is not None:
            if coverage is not None and (
                coverage.evidence_observed_at is None
                or coverage.evidence_observed_at > cut
                or (
                    coverage.checkpoint_event_cut is not None
                    and coverage.checkpoint_event_cut > cut
                )
            ):
                coverage = None
            elif coverage is not None:
                if coverage.start_at > cut:
                    coverage = None
                elif coverage.end_at > cut:
                    coverage = FactCoverageInterval(
                        start_at=coverage.start_at,
                        end_at=cut,
                        has_known_gaps=coverage.has_known_gaps,
                        source_cursor=coverage.source_cursor,
                        status=coverage.status,
                        confirmed_revision=coverage.confirmed_revision,
                        stream_scope=coverage.stream_scope,
                        evidence_observed_at=coverage.evidence_observed_at,
                        checkpoint_id=coverage.checkpoint_id,
                        checkpoint_event_cut=coverage.checkpoint_event_cut,
                        load_provenance=coverage.load_provenance,
                        page_exhausted=coverage.page_exhausted,
                        not_truncated=coverage.not_truncated,
                    )

        has_late_events = (
            self._has_late_events
            or any(
                fill.trade_id in self._late_trade_ids for fill in (*fills, *conflicts)
            )
            or bool(recovery_checkpoint and recovery_checkpoint.has_late_events)
        )
        late_fills = tuple(
            fill
            for fill in (*fills, *conflicts)
            if fill.trade_id in self._late_trade_ids
        )
        has_synthetic_fills = (
            self._has_synthetic_fills
            or any(
                bool((fill.raw_payload or {}).get("synthetic_from_order", False))
                for fill in fills
            )
            or bool(recovery_checkpoint and recovery_checkpoint.has_synthetic_fills)
        )

        facts = AccountFacts(
            position_key=self._position_key,
            fills=fills,
            snapshots=snapshots,
            exit_boundaries=boundaries,
            coverage=coverage,
            checkpoint=checkpoint,
            has_synthetic_fills=has_synthetic_fills,
            conflicting_fills=conflicts,
            has_late_events=has_late_events,
            stream_scope=self._stream_scope,
            recovery_checkpoint=recovery_checkpoint,
            fact_conflicts=fact_conflicts,
            integrity_issues=tuple(self._integrity_issues),
            late_fills=late_fills,
            prefix_facts_complete=self._prefix_facts_complete,
            fill_cursor_provenance=cursor_provenance,
            fill_load_provenance=fill_load_provenance,
        )
        if cut is None:
            self._cached_facts_none = facts
        return facts


def _same_fill(first: AccountFillEvent, second: AccountFillEvent) -> bool:
    return (
        first.environment == second.environment
        and first.account_label == second.account_label
        and first.symbol == second.symbol
        and first.trade_id == second.trade_id
        and first.order_id == second.order_id
        and first.side.upper() == second.side.upper()
        and first.price == second.price
        and first.quantity == second.quantity
        and first.realized_pnl == second.realized_pnl
        and first.fee == second.fee
        and first.fee_asset == second.fee_asset
        and first.trade_at == second.trade_at
        and first.raw_payload == second.raw_payload
    )
