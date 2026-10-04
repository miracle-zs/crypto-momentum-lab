"""Sequential grouped evidence processing against one caller-owned candidate.

This coordinator owns no lock, database transaction or published state. The
caller must provide callbacks bound to the same execution candidate.
"""

from collections.abc import Awaitable, Callable
from dataclasses import replace
from decimal import Decimal

from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
from crypto_momentum_lab.domain.execution.evidence_rules import _evidence_identity
from crypto_momentum_lab.domain.execution.observation_models import (
    Applied,
    Duplicate,
    EvidenceConflict,
    ExecutionObserveResult,
    WaitingForEvidence,
)


async def observe_evidence_group(
    evidence: ExecutionEvidence,
    *,
    observe_one: Callable[[ExecutionEvidence], Awaitable[ExecutionObserveResult]],
    forget_identity: Callable[[str], None],
) -> ExecutionObserveResult:
    fills = evidence.fills or ((evidence.fill,) if evidence.fill else ())
    if not fills:
        return await observe_one(evidence)
    consumed = Decimal("0")
    released = Decimal("0")
    recovery_required = False
    diagnostics: list[str] = []
    for fill in fills:
        internal_id = f"{evidence.evidence_id}\x1ftrade:{fill.trade_id}"
        one_fill = replace(
            evidence,
            evidence_id=internal_id,
            fill=fill,
            fills=(),
            snapshot=None,
            boundary=None,
            order_event=None,
            coverage=None,
            coverage_evidence=None,
            fill_load_provenance=None,
            stream_checkpoint_adoption=None,
            cumulative_order=None,
            source_anchor_snapshot=None,
            settlement_fills=(),
        )
        fill_result = await observe_one(one_fill)
        forget_identity(_evidence_identity(one_fill))
        if isinstance(fill_result, (EvidenceConflict, WaitingForEvidence)):
            return replace(fill_result, evidence_id=evidence.evidence_id)
        if isinstance(fill_result, Applied):
            consumed += fill_result.consumed_quantity
            released += fill_result.released_quantity
            recovery_required = recovery_required or fill_result.recovery_required
            diagnostics.extend(fill_result.diagnostics)

    remainder = replace(evidence, fill=None, fills=())
    base_result = await observe_one(remainder)
    if isinstance(base_result, (EvidenceConflict, WaitingForEvidence)):
        return base_result
    if isinstance(base_result, Duplicate):
        return base_result
    consumed += base_result.consumed_quantity
    released += base_result.released_quantity
    recovery_required = recovery_required or base_result.recovery_required
    diagnostics.extend(base_result.diagnostics)
    return replace(
        base_result,
        consumed_quantity=consumed,
        released_quantity=released,
        recovery_required=recovery_required,
        diagnostics=tuple(dict.fromkeys(diagnostics)),
    )
