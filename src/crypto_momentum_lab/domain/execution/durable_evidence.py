"""Pure durable evidence admission and changed-watermark persistence plans."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
from crypto_momentum_lab.domain.execution.evidence_rules import _coverage_for_scope
from crypto_momentum_lab.domain.execution.ports import ExecutionWatermark
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    FactCoverageStatus,
    PositionKey,
)


class DurableEvidenceConflict(ValueError):
    """A source rejection that Book returns as EvidenceConflict."""


@dataclass(frozen=True, slots=True)
class DurableEvidenceInput:
    evidence: ExecutionEvidence
    scope: AccountFactStreamScope


def prepare_durable_evidence(evidence: ExecutionEvidence) -> DurableEvidenceInput:
    if evidence.stream_id is None or evidence.stream_epoch is None:
        raise DurableEvidenceConflict(
            "durable execution evidence requires stream identity"
        )
    fills = evidence.fills or ((evidence.fill,) if evidence.fill else ())
    if any(
        isinstance(fill.raw_payload, dict)
        and (fill.raw_payload.get("is_cumulative") or "cum_qty" in fill.raw_payload)
        for fill in fills
    ):
        raise DurableEvidenceConflict(
            "cumulative order reports cannot be recorded as account trades; "
            "use cumulative_order"
        )
    scope = AccountFactStreamScope.for_position_key(
        evidence.scope.to_position_key(),
        stream_id=evidence.stream_id,
        stream_epoch=evidence.stream_epoch,
    )
    try:
        evidence = _coverage_for_scope(evidence, scope)
    except ValueError as err:
        raise DurableEvidenceConflict(str(err)) from err
    if (
        evidence.coverage is not None
        and evidence.coverage.status == FactCoverageStatus.CONFIRMED
        and evidence.coverage_evidence is None
        and evidence.coverage.stream_scope is not None
        and evidence.coverage.stream_scope.environment == "live"
    ):
        raise DurableEvidenceConflict(
            "durable live coverage requires typed pagination provenance"
        )
    return DurableEvidenceInput(evidence, scope)


def changed_order_watermarks(
    key: PositionKey,
    *,
    before_quantities: Mapping[str, Decimal],
    before_quotes: Mapping[str, Decimal],
    after_quantities: Mapping[str, Decimal],
    after_quotes: Mapping[str, Decimal],
    observed_at: datetime,
) -> tuple[ExecutionWatermark, ...]:
    prefix = f"{key.canonical_id}\x1f"
    changed_orders = {
        watermark_key[len(prefix) :]
        for watermark_key, quantity in after_quantities.items()
        if watermark_key.startswith(prefix)
        and (
            quantity != before_quantities.get(watermark_key, Decimal("0"))
            or after_quotes.get(watermark_key, Decimal("0"))
            != before_quotes.get(watermark_key, Decimal("0"))
        )
    }
    return tuple(
        ExecutionWatermark(
            order_id=order_id,
            cumulative_quantity=after_quantities[prefix + order_id],
            cumulative_quote=after_quotes.get(prefix + order_id, Decimal("0")),
            updated_at=observed_at,
        )
        for order_id in sorted(changed_orders)
    )
