"""Fact proof matrix: recovery cut versus current head proof."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.operator_dashboard.fact_proof import (
    INVALID,
    PROVEN,
    REASON_CHECKPOINT_COVERAGE_MISSING,
    REASON_CHECKPOINT_MISMATCH,
    REASON_COVERAGE_STALE,
    REASON_HEAD_NOT_COVERED,
    REASON_HEAD_REVISION_BEHIND,
    REASON_JOURNAL_SUFFIX_INCOMPLETE,
    REASON_PROJECTION_NOT_READY,
    REASON_PROOF_TOO_OLD,
    UNKNOWN,
    HeadFacts,
    RecoveryCut,
    SuffixFacts,
    evaluate_head_proof,
    is_current_scope,
)

NOW = datetime(2026, 10, 12, 18, 0, tzinfo=UTC)
CHECKPOINT_CUT = NOW - timedelta(seconds=400)


def _recovery(**overrides: object) -> RecoveryCut:
    values: dict[str, object] = {
        "event_cut": CHECKPOINT_CUT,
        "source_revision": 10,
        "identity_matches": True,
        "binding_matches": True,
        "coverage_authoritative": True,
        "coverage_covers_cut": True,
        "coverage_end_at": CHECKPOINT_CUT,
        "has_conflicts": False,
        "has_synthetic_fills": False,
        "has_late_events": False,
        "has_integrity_issues": False,
        "projection_ready": True,
    }
    values.update(overrides)
    assert values["coverage_end_at"] <= values["event_cut"]
    return RecoveryCut(**values)  # type: ignore[arg-type]


def _head(**overrides: object) -> HeadFacts:
    values: dict[str, object] = {
        "scope": "WUSDT:LONG",
        "updated_at": NOW - timedelta(seconds=5),
        "journal_revision": 13,
        "active_quantity": Decimal("3184.7"),
        "unresolved_work": False,
        "reconciliation_gap": Decimal("0"),
    }
    values.update(overrides)
    return HeadFacts(**values)  # type: ignore[arg-type]


def test_complete_suffix_within_verified_cut_proves_a_newer_publication() -> None:
    """Publication time is not a fact cut; do not fake future coverage."""
    proof = evaluate_head_proof(
        head=_head(),
        recovery=_recovery(
            event_cut=NOW - timedelta(seconds=30),
            coverage_end_at=NOW - timedelta(seconds=30),
        ),
        suffix=SuffixFacts(
            max_source_revision=13,
            newest_occurred_at=NOW - timedelta(seconds=30),
        ),
        now=NOW,
        max_age_seconds=180.0,
    )

    assert proof.status == PROVEN
    assert proof.cut == NOW - timedelta(seconds=30)
    assert proof.current


def test_sparse_revisions_are_valid_when_latest_revision_reaches_head() -> None:
    """One transaction may advance several journal revisions before persisting."""
    proof = evaluate_head_proof(
        head=_head(),
        recovery=_recovery(
            event_cut=NOW - timedelta(seconds=30),
            coverage_end_at=NOW - timedelta(seconds=30),
        ),
        suffix=SuffixFacts(
            max_source_revision=13,
            newest_occurred_at=NOW - timedelta(seconds=30),
        ),
        now=NOW,
        max_age_seconds=180.0,
    )

    assert proof.status == PROVEN
    assert proof.current


def test_incomplete_suffix_is_unknown_and_names_the_reason() -> None:
    missing_revision = evaluate_head_proof(
        head=_head(),
        recovery=_recovery(),
        suffix=SuffixFacts(max_source_revision=12),
        now=NOW,
        max_age_seconds=180.0,
    )
    assert missing_revision.status == UNKNOWN
    assert missing_revision.reason == REASON_JOURNAL_SUFFIX_INCOMPLETE

    behind = evaluate_head_proof(
        head=_head(journal_revision=9),
        recovery=_recovery(),
        suffix=SuffixFacts(),
        now=NOW,
        max_age_seconds=180.0,
    )
    assert behind.status == UNKNOWN
    assert behind.reason == REASON_HEAD_REVISION_BEHIND


def test_coverage_watermark_must_reach_the_proven_cut() -> None:
    stale_coverage = evaluate_head_proof(
        head=_head(),
        recovery=_recovery(coverage_end_at=CHECKPOINT_CUT),
        suffix=SuffixFacts(
            max_source_revision=13,
            newest_occurred_at=NOW - timedelta(seconds=30),
        ),
        now=NOW,
        max_age_seconds=180.0,
    )
    assert stale_coverage.status == UNKNOWN
    assert stale_coverage.reason == REASON_COVERAGE_STALE


def test_old_proof_and_missing_checkpoint_coverage_are_unknown() -> None:
    too_old = evaluate_head_proof(
        head=_head(),
        recovery=_recovery(
            event_cut=NOW - timedelta(minutes=10),
            coverage_end_at=NOW - timedelta(minutes=10),
            coverage_covers_cut=True,
        ),
        suffix=SuffixFacts(
            max_source_revision=13,
            newest_occurred_at=NOW - timedelta(minutes=10),
        ),
        now=NOW,
        max_age_seconds=180.0,
    )
    assert too_old.status == UNKNOWN
    assert too_old.reason == REASON_PROOF_TOO_OLD

    unproven_coverage = evaluate_head_proof(
        head=_head(),
        recovery=_recovery(coverage_authoritative=False),
        suffix=SuffixFacts(
            max_source_revision=13, newest_occurred_at=NOW
        ),
        now=NOW,
        max_age_seconds=180.0,
    )
    assert unproven_coverage.status == UNKNOWN
    assert unproven_coverage.reason == REASON_CHECKPOINT_COVERAGE_MISSING


def test_conflicts_and_unready_projection_stay_invalid() -> None:
    conflict = evaluate_head_proof(
        head=_head(),
        recovery=_recovery(has_conflicts=True),
        suffix=SuffixFacts(max_source_revision=13),
        now=NOW,
        max_age_seconds=180.0,
    )
    assert conflict.status == INVALID

    not_ready = evaluate_head_proof(
        head=_head(),
        recovery=_recovery(projection_ready=False),
        suffix=SuffixFacts(max_source_revision=13),
        now=NOW,
        max_age_seconds=180.0,
    )
    assert not_ready.status == INVALID
    assert not_ready.reason == REASON_PROJECTION_NOT_READY


def test_mismatched_checkpoint_identity_is_unknown() -> None:
    proof = evaluate_head_proof(
        head=_head(),
        recovery=_recovery(binding_matches=False),
        suffix=SuffixFacts(max_source_revision=13),
        now=NOW,
        max_age_seconds=180.0,
    )
    assert proof.status == UNKNOWN
    assert proof.reason == REASON_CHECKPOINT_MISMATCH


def test_legacy_head_without_revision_needs_checkpoint_coverage() -> None:
    legacy = _head(journal_revision=None, updated_at=NOW)
    uncovered = evaluate_head_proof(
        head=legacy,
        recovery=_recovery(),
        suffix=SuffixFacts(),
        now=NOW,
        max_age_seconds=180.0,
    )
    assert uncovered.status == UNKNOWN
    assert uncovered.reason == REASON_HEAD_NOT_COVERED

    fresh_cut = NOW - timedelta(seconds=30)
    covered = evaluate_head_proof(
        head=_head(journal_revision=None, updated_at=fresh_cut),
        recovery=_recovery(event_cut=fresh_cut, coverage_end_at=fresh_cut),
        suffix=SuffixFacts(),
        now=NOW,
        max_age_seconds=180.0,
    )
    assert covered.status == PROVEN
    assert covered.cut == fresh_cut


def test_closed_resolved_position_is_not_current_scope() -> None:
    closed = _head(
        active_quantity=Decimal("0"),
        unresolved_work=False,
        reconciliation_gap=Decimal("0"),
    )
    assert is_current_scope(closed) is False

    assert is_current_scope(_head(active_quantity=Decimal("0"), unresolved_work=True))
    assert is_current_scope(
        _head(
            active_quantity=Decimal("0"),
            reconciliation_gap=Decimal("1"),
        )
    )
