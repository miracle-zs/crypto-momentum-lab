"""Bind transport scan results to an exact Book stream and snapshot cut."""

from crypto_momentum_lab.domain.account.models import (
    AccountFillLoadScan,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    AccountFillLoadProvenance,
    CoverageEvidence,
)


def coverage_from_scan(
    scan: AccountFillLoadScan,
    *,
    snapshot: AccountPositionSnapshot,
    scope: AccountFactStreamScope,
) -> CoverageEvidence:
    if (scan.environment, scan.account_label, scan.symbol, scan.position_side) != (
        scope.environment,
        scope.account_label,
        scope.symbol,
        scope.position_side.value,
    ):
        raise ValueError("fill scan does not match the position stream")
    if snapshot.observed_at != scan.observed_at:
        raise ValueError("fill scan cut does not match account snapshot")
    page = scan.page_scan
    provenance = AccountFillLoadProvenance(
        stream_scope=scope,
        load_id=page.load_id,
        scan_origin_from_id=None,
        scan_origin_start_time_ms=page.scan_origin_start_time_ms,
        request_from_id=None,
        next_from_id=page.next_from_id,
        page_count=page.page_count,
        page_exhausted=page.page_exhausted,
        truncated=page.truncated,
        checked_through=page.checked_through,
        observed_at=scan.observed_at,
        source_anchor_id=scan.source_anchor_id,
        source_anchor_event_cut=scan.source_anchor_event_cut,
        source_anchor_kind=scan.source_anchor_kind,
    )
    return CoverageEvidence(
        fill_cursor_id=page.load_id,
        fill_load_start=provenance.origin_start_at,
        fill_checked_through=page.checked_through,
        checkpoint_id=page.load_id,
        checkpoint_event_cut=scan.observed_at,
        stream_scope=scope,
        evidence_observed_at=scan.observed_at,
        page_exhausted=page.page_exhausted,
        not_truncated=not page.truncated,
        load_provenance=provenance,
    )
