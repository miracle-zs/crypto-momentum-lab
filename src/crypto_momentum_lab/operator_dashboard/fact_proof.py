"""Two-cut fact proof: recovery checkpoint versus current head coverage.

A durable head answers two different questions, and conflating them made every
head whose checkpoint was older than its last update look unverified:

* the **recovery cut** is the facts cut a checkpoint was built from. It must be
  internally sound (no conflicts, no synthetic fills, a READY projection) and
  its coverage must be authoritative for that cut. Its age is irrelevant to
  correctness.
* the **current proof** is the head's own fact cut, proven by that checkpoint
  plus the journal suffix already persisted after it and the coverage
  watermark that scanned through it. The checkpoint does not have to reach the
  head's wall-clock update time; the persisted suffix does.

Only the current proof gates current risk. Closed, fully resolved objects keep
their own evidence in a separate bucket so a stale historical checkpoint cannot
mark a live account unhealthy, while genuine conflicts stay invalid and objects
with unresolved work remain in the current scope.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

PROVEN = "proven"
UNKNOWN = "unknown"
INVALID = "invalid"

# Current-proof reasons: what specific evidence is missing.
REASON_NO_RELEVANT_HEAD = "no_relevant_head"
REASON_CHECKPOINT_MISSING = "checkpoint_binding_missing"
REASON_CHECKPOINT_ROW_MISSING = "checkpoint_row_missing"
REASON_CHECKPOINT_MISMATCH = "checkpoint_identity_mismatch"
REASON_CHECKPOINT_CONFLICT = "checkpoint_conflict"
REASON_PROJECTION_NOT_READY = "projection_not_ready"
REASON_RECONCILIATION_GAP = "reconciliation_gap"
REASON_CHECKPOINT_COVERAGE_MISSING = "checkpoint_coverage_missing"
REASON_HEAD_NOT_COVERED = "head_not_covered_by_checkpoint"
REASON_HEAD_REVISION_BEHIND = "head_revision_behind_checkpoint"
REASON_JOURNAL_SUFFIX_INCOMPLETE = "journal_suffix_incomplete"
REASON_COVERAGE_STALE = "coverage_stale"
REASON_PROOF_TOO_OLD = "proof_too_old"
REASON_ACTIVE_POSITION_COUNT_MISMATCH = "active_position_count_mismatch"


@dataclass(frozen=True, slots=True)
class HeadFacts:
    """Durable head facts relevant to proving the current cut."""

    scope: str
    updated_at: datetime
    journal_revision: int | None
    active_quantity: Decimal
    unresolved_work: bool
    reconciliation_gap: Decimal | None = None


@dataclass(frozen=True, slots=True)
class RecoveryCut:
    """The facts cut a recovery checkpoint was built from."""

    event_cut: datetime
    source_revision: int
    identity_matches: bool
    binding_matches: bool
    coverage_authoritative: bool
    coverage_covers_cut: bool
    coverage_end_at: datetime | None
    has_conflicts: bool
    has_synthetic_fills: bool
    has_late_events: bool
    has_integrity_issues: bool
    projection_ready: bool


@dataclass(frozen=True, slots=True)
class SuffixFacts:
    """Persisted journal facts after the recovery cut, as stored."""

    max_source_revision: int | None = None
    newest_occurred_at: datetime | None = None
    newest_coverage_end_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class HeadProof:
    """Outcome of proving one head, with the missing evidence named."""

    status: str
    reason: str
    cut: datetime | None
    current: bool


def is_current_scope(head: HeadFacts) -> bool:
    """Whether a head still carries current risk or unresolved work."""
    return (
        head.active_quantity != 0
        or head.unresolved_work
        or head.reconciliation_gap not in (None, Decimal("0"))
    )


def evaluate_head_proof(
    *,
    head: HeadFacts,
    recovery: RecoveryCut,
    suffix: SuffixFacts,
    now: datetime,
    max_age_seconds: float,
) -> HeadProof:
    """Prove one head from checkpoint + persisted suffix + coverage watermark."""
    current = is_current_scope(head)
    if not recovery.identity_matches or not recovery.binding_matches:
        return HeadProof(
            UNKNOWN, REASON_CHECKPOINT_MISMATCH, None, current
        )
    if (
        recovery.has_conflicts
        or recovery.has_synthetic_fills
        or recovery.has_late_events
        or recovery.has_integrity_issues
    ):
        return HeadProof(INVALID, REASON_CHECKPOINT_CONFLICT, None, current)
    if not recovery.projection_ready:
        return HeadProof(INVALID, REASON_PROJECTION_NOT_READY, None, current)
    if head.reconciliation_gap not in (None, Decimal("0")):
        return HeadProof(INVALID, REASON_RECONCILIATION_GAP, None, current)
    if not recovery.coverage_authoritative or not recovery.coverage_covers_cut:
        return HeadProof(
            UNKNOWN, REASON_CHECKPOINT_COVERAGE_MISSING, None, current
        )

    if head.journal_revision is None:
        # Older payloads carry no suffix claim: the checkpoint itself must
        # cover the head, which is the historical criterion.
        if recovery.event_cut < head.updated_at:
            return HeadProof(UNKNOWN, REASON_HEAD_NOT_COVERED, None, current)
        proof_cut = recovery.event_cut
    else:
        if head.journal_revision < recovery.source_revision:
            return HeadProof(
                UNKNOWN, REASON_HEAD_REVISION_BEHIND, None, current
            )
        if head.journal_revision > recovery.source_revision:
            # A journal revision is advanced per fact mutation, but one
            # persistence transaction writes all changed facts at the final
            # revision. Revisions between the checkpoint and that final value
            # therefore need not each have a row; only the final persisted
            # revision can prove that the current transaction reached the head.
            if suffix.max_source_revision != head.journal_revision:
                return HeadProof(
                    UNKNOWN, REASON_JOURNAL_SUFFIX_INCOMPLETE, None, current
                )
            newest = suffix.newest_occurred_at or recovery.event_cut
            proof_cut = max(recovery.event_cut, newest)
        else:
            proof_cut = recovery.event_cut

    # Use the newest coverage end_at from either the checkpoint or the suffix.
    # A newer coverage event in the suffix can prove a later cut than the
    # checkpoint's own coverage.
    effective_coverage_end_at = recovery.coverage_end_at
    if (
        suffix.newest_coverage_end_at is not None
        and (
            effective_coverage_end_at is None
            or suffix.newest_coverage_end_at > effective_coverage_end_at
        )
    ):
        effective_coverage_end_at = suffix.newest_coverage_end_at
    if (
        effective_coverage_end_at is None
        or effective_coverage_end_at < proof_cut
    ):
        return HeadProof(UNKNOWN, REASON_COVERAGE_STALE, None, current)
    if (now - proof_cut).total_seconds() > max_age_seconds:
        return HeadProof(UNKNOWN, REASON_PROOF_TOO_OLD, None, current)
    return HeadProof(PROVEN, PROVEN, proof_cut, current)


__all__ = [
    "HeadFacts",
    "HeadProof",
    "INVALID",
    "PROVEN",
    "REASON_ACTIVE_POSITION_COUNT_MISMATCH",
    "REASON_CHECKPOINT_CONFLICT",
    "REASON_CHECKPOINT_COVERAGE_MISSING",
    "REASON_CHECKPOINT_MISMATCH",
    "REASON_CHECKPOINT_MISSING",
    "REASON_CHECKPOINT_ROW_MISSING",
    "REASON_COVERAGE_STALE",
    "REASON_HEAD_NOT_COVERED",
    "REASON_HEAD_REVISION_BEHIND",
    "REASON_JOURNAL_SUFFIX_INCOMPLETE",
    "REASON_NO_RELEVANT_HEAD",
    "REASON_PROJECTION_NOT_READY",
    "REASON_PROOF_TOO_OLD",
    "REASON_RECONCILIATION_GAP",
    "RecoveryCut",
    "SuffixFacts",
    "UNKNOWN",
    "evaluate_head_proof",
    "is_current_scope",
]
