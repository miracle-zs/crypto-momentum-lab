"""Evidence identity and coverage normalization without mutable Book state."""

from dataclasses import asdict, replace

from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    FactCoverageInterval,
    FactCoverageStatus,
    PositionKey,
    compose_fact_coverage,
)


def _canonical_evidence_payload(evidence: ExecutionEvidence) -> dict[str, object]:
    payload = asdict(evidence)
    # Existing durable receipt hashes must remain replay-compatible when no
    # historical settlement proof was supplied.
    if not evidence.settlement_fills:
        payload.pop("settlement_fills", None)
    payload.pop("observed_at", None)
    order_event = payload.get("order_event")
    if isinstance(order_event, dict):
        order_event.pop("occurred_at", None)
    cumulative_order = payload.get("cumulative_order")
    if isinstance(cumulative_order, dict):
        cumulative_order.pop("observed_at", None)
    return payload


def _evidence_identity(evidence: ExecutionEvidence) -> str:
    if evidence.stream_id is None or evidence.stream_epoch is None:
        return evidence.evidence_id
    key = evidence.scope.to_position_key()
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id=evidence.stream_id, stream_epoch=evidence.stream_epoch
    )
    return _scoped_evidence_identity(scope, evidence.evidence_id)


def _scoped_evidence_identity(
    scope: AccountFactStreamScope,
    evidence_id: str,
) -> str:
    key = PositionKey(
        environment=scope.environment,
        account_label=scope.account_label,
        symbol=scope.symbol,
        position_side=scope.position_side,
    )
    return (
        f"{key.canonical_id}\x1f{scope.stream_id}\x1f"
        f"{scope.stream_epoch}\x1f{evidence_id}"
    )


def _coverage_for_scope(
    evidence: ExecutionEvidence,
    scope: AccountFactStreamScope,
) -> ExecutionEvidence:
    proof = evidence.coverage_evidence
    if evidence.fill_load_provenance is not None:
        if proof is not None and proof.load_provenance != evidence.fill_load_provenance:
            raise ValueError("coverage and fill-load provenance disagree")
        if proof is None:
            return evidence
    if proof is not None and evidence.fill_load_provenance is None:
        if proof.load_provenance is None:
            raise ValueError("coverage proof has no durable fill-load provenance")
        evidence = replace(
            evidence,
            fill_load_provenance=proof.load_provenance,
        )
    if proof is None:
        if evidence.coverage is not None and (
            evidence.coverage.stream_scope is not None
            and evidence.coverage.stream_scope != scope
        ):
            raise ValueError("coverage interval does not match the event stream")
        return evidence
    if proof.stream_scope != scope:
        raise ValueError("fill coverage proof does not match the event stream")
    if proof.load_provenance != evidence.fill_load_provenance:
        raise ValueError("coverage proof does not bind the persisted fill scan")

    fill_start = proof.fill_load_start
    checkpoint_cut = proof.checkpoint_event_cut
    if (
        fill_start is not None
        and checkpoint_cut is not None
        and checkpoint_cut >= fill_start
    ):
        start = (
            evidence.fill_load_provenance.source_anchor_event_cut
            if evidence.fill_load_provenance is not None
            and evidence.fill_load_provenance.source_anchor_kind
            == "recovery_checkpoint"
            else fill_start
        )
        end = checkpoint_cut
    else:
        start = evidence.observed_at
        end = evidence.observed_at
    is_page_complete = bool(getattr(proof, "page_exhausted", False)) and bool(
        getattr(proof, "not_truncated", False)
    )
    if is_page_complete:
        derived = compose_fact_coverage(
            proof,
            start=start,
            end=end,
            expected_scope=scope,
        )
    else:
        derived = FactCoverageInterval(
            start_at=start,
            end_at=end,
            source_cursor=proof.fill_cursor_id,
            status=FactCoverageStatus.PENDING,
            stream_scope=scope,
            evidence_observed_at=proof.evidence_observed_at,
        )
    if evidence.coverage is not None and evidence.coverage != derived:
        raise ValueError(
            "supplied coverage interval disagrees with its typed source proof"
        )
    return replace(evidence, coverage=derived)
