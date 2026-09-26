"""Coverage CONFIRMED requires proven cursor + checkpoint, not empty attrs."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from crypto_momentum_lab.domain.execution.position_ledger_models import (
    CoverageEvidence,
    FactCoverageStatus,
    compose_fact_coverage,
)

START = datetime(2026, 9, 25, 7, 0, tzinfo=UTC)
END = datetime(2026, 9, 25, 8, 0, tzinfo=UTC)


def test_empty_evidence_is_pending() -> None:
    cov = compose_fact_coverage(None, start=START, end=END)
    assert cov.status == FactCoverageStatus.PENDING
    assert not cov.covers(END)


def test_nonempty_ids_alone_are_not_proof() -> None:
    evidence = CoverageEvidence(
        fill_cursor_id="fill_from_id:1",
        checkpoint_id="recon-1",
    )
    cov = compose_fact_coverage(evidence, start=START, end=END)
    assert cov.status == FactCoverageStatus.PENDING


def test_cursor_without_checkpoint_stays_pending() -> None:
    evidence = CoverageEvidence(
        fill_cursor_id="fill_from_id:1",
        fill_load_start=START,
        fill_checked_through=END,
    )
    cov = compose_fact_coverage(evidence, start=START, end=END)
    assert cov.status == FactCoverageStatus.PENDING


def test_checkpoint_without_fill_cursor_stays_pending() -> None:
    evidence = CoverageEvidence(
        checkpoint_id="recon-1",
        checkpoint_event_cut=END,
    )
    cov = compose_fact_coverage(evidence, start=START, end=END)
    assert cov.status == FactCoverageStatus.PENDING


def test_fill_check_short_of_window_end_stays_pending() -> None:
    evidence = CoverageEvidence(
        fill_cursor_id="fill_from_id:1",
        fill_load_start=START,
        fill_checked_through=datetime(2026, 9, 25, 7, 30, tzinfo=UTC),
        checkpoint_id="recon-1",
        checkpoint_event_cut=END,
    )
    cov = compose_fact_coverage(evidence, start=START, end=END)
    assert cov.status == FactCoverageStatus.PENDING


def test_load_start_after_window_start_stays_pending() -> None:
    evidence = CoverageEvidence(
        fill_cursor_id="fill_from_id:1",
        fill_load_start=datetime(2026, 9, 25, 7, 30, tzinfo=UTC),
        fill_checked_through=END,
        checkpoint_id="recon-1",
        checkpoint_event_cut=END,
    )
    cov = compose_fact_coverage(evidence, start=START, end=END)
    assert cov.status == FactCoverageStatus.PENDING


def test_full_evidence_confirms_window() -> None:
    evidence = CoverageEvidence(
        fill_cursor_id="fill_from_id:1",
        fill_load_start=datetime(2026, 9, 25, 6, 0, tzinfo=UTC),
        fill_checked_through=datetime(2026, 9, 25, 9, 0, tzinfo=UTC),
        checkpoint_id="recon-1",
        checkpoint_event_cut=datetime(2026, 9, 25, 8, 30, tzinfo=UTC),
    )
    cov = compose_fact_coverage(evidence, start=START, end=END)
    assert cov.status == FactCoverageStatus.CONFIRMED
    assert cov.covers(END)
    assert cov.source_cursor == "fill_from_id:1"


def test_compose_rejects_naive_bounds() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        compose_fact_coverage(
            None,
            start=datetime(2026, 9, 25, 7, 0),
            end=END,
        )
