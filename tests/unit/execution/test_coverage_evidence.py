"""Coverage CONFIRMED requires proven cursor + checkpoint, not empty attrs."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import pytest

from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    AccountFillLoadProvenance,
    CoverageEvidence,
    FactCoverageStatus,
    PositionKey,
    compose_fact_coverage,
)

START = datetime(2026, 9, 25, 7, 0, tzinfo=UTC)
END = datetime(2026, 9, 25, 8, 0, tzinfo=UTC)


def _scope() -> AccountFactStreamScope:
    return AccountFactStreamScope.for_position_key(
        PositionKey("live", "primary", "BTCUSDT", FuturesPositionSide.BOTH),
        stream_id="trades",
        stream_epoch="test-epoch",
    )


def _complete_provenance() -> AccountFillLoadProvenance:
    return AccountFillLoadProvenance(
        stream_scope=_scope(),
        load_id="load-1",
        scan_origin_from_id=None,
        scan_origin_start_time_ms=int(START.timestamp() * 1000),
        request_from_id=None,
        next_from_id=None,
        page_count=1,
        page_exhausted=True,
        truncated=False,
        checked_through=END,
        observed_at=END,
        source_anchor_id="anchor-1",
        source_anchor_event_cut=START,
        source_anchor_kind="recovery_checkpoint",
    )


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
    provenance = _complete_provenance()
    evidence = CoverageEvidence(
        fill_cursor_id="fill_from_id:1",
        fill_load_start=START,
        fill_checked_through=END,
        checkpoint_id="recon-1",
        checkpoint_event_cut=END,
        stream_scope=_scope(),
        evidence_observed_at=END,
        page_exhausted=True,
        not_truncated=True,
        load_provenance=provenance,
    )
    cov = compose_fact_coverage(evidence, start=START, end=END)
    assert cov.status == FactCoverageStatus.CONFIRMED
    assert cov.covers(END)
    assert cov.source_cursor == "fill_from_id:1"


def test_incomplete_or_truncated_pages_cannot_confirm_coverage() -> None:
    provenance = _complete_provenance()
    evidence = CoverageEvidence(
        fill_cursor_id="fill_from_id:1",
        fill_load_start=START,
        fill_checked_through=END,
        checkpoint_id="recon-1",
        checkpoint_event_cut=END,
        stream_scope=_scope(),
        evidence_observed_at=END,
        page_exhausted=False,
        not_truncated=False,
        load_provenance=replace(
            provenance,
            next_from_id=10,
            page_exhausted=False,
            truncated=True,
        ),
    )
    coverage = compose_fact_coverage(evidence, start=START, end=END)
    assert coverage.status == FactCoverageStatus.PENDING
    assert not coverage.covers(END)


def test_compose_rejects_naive_bounds() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        compose_fact_coverage(
            None,
            start=datetime(2026, 9, 25, 7, 0),
            end=END,
        )


@pytest.mark.parametrize(
    "missing_field", ["fill_load_start", "fill_checked_through", "checkpoint_event_cut"]
)
def test_incomplete_bounds_remain_pending_when_completion_check_claims_success(
    missing_field: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    evidence = CoverageEvidence(
        fill_load_start=START,
        fill_checked_through=END,
        checkpoint_event_cut=END,
    )
    evidence = replace(evidence, **{missing_field: None})
    monkeypatch.setattr(
        CoverageEvidence, "proves_complete", lambda *args, **kwargs: True
    )

    coverage = compose_fact_coverage(evidence, start=START, end=END)

    assert coverage.status == FactCoverageStatus.PENDING
    assert coverage.start_at == START
    assert coverage.end_at == END
