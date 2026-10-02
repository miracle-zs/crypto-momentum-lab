"""Pure cumulative report settlement and watermark publication plans."""

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal

from crypto_momentum_lab.domain.account.models import AccountFillEvent
from crypto_momentum_lab.domain.execution.command_models import OutboxEntry
from crypto_momentum_lab.domain.execution.evidence_models import (
    ExecutionCumulativeOrderReport,
    ExecutionEvidence,
)
from crypto_momentum_lab.domain.execution.evidence_settlement import (
    cumulative_order_delta,
)


@dataclass(frozen=True, slots=True)
class CumulativeReportPlan:
    watermark: tuple[Decimal, Decimal] | None
    settlement_quantity: Decimal
    reported_quantity: Decimal


def plan_cumulative_report(
    report: ExecutionCumulativeOrderReport,
    *,
    previous_watermark: tuple[Decimal, Decimal],
    account_fills: tuple[AccountFillEvent, ...],
    outbox: OutboxEntry | None,
    has_active_reservations: bool,
) -> CumulativeReportPlan:
    delta = cumulative_order_delta(
        report,
        previous_quantity=previous_watermark[0],
        previous_quote=previous_watermark[1],
        account_fills=account_fills,
        order_ids=(frozenset({outbox.command_id, outbox.external_order_id})
                   if outbox is not None and outbox.external_order_id is not None
                   else frozenset({report.order_id})),
    )
    exit_report = has_active_reservations or (
        outbox is not None and outbox.command.reduce_only
    )
    return CumulativeReportPlan(
        watermark=delta.watermark,
        settlement_quantity=delta.quantity if exit_report else Decimal("0"),
        reported_quantity=delta.watermark[0]
        if delta.watermark is not None
        else previous_watermark[0],
    )


@dataclass(frozen=True, slots=True)
class WatermarkPublicationPlan:
    command_id: str
    outbox: OutboxEntry | None
    recovery_diagnostic: str | None


def plan_watermark_publication(
    evidence: ExecutionEvidence,
    *,
    outbox_by_command_id: Mapping[str, OutboxEntry],
) -> WatermarkPublicationPlan:
    """Resolve the existing command identity and missing cumulative-fill authority."""
    command_id = (
        evidence.cumulative_order.order_id
        if evidence.cumulative_order is not None
        else evidence.order_event.client_order_id
        if evidence.order_event is not None
        else evidence.fill.order_id
        if evidence.fill is not None
        else evidence.fills[0].order_id
        if evidence.fills
        else ""
    )
    outbox = outbox_by_command_id.get(command_id)
    if outbox is None:
        outbox = next((entry for entry in outbox_by_command_id.values()
                       if entry.scope == evidence.scope
                       and entry.external_order_id == command_id), None)
    if outbox is not None:
        command_id = outbox.command_id
    observed_fills = evidence.fills or (
        (evidence.fill,) if evidence.fill is not None else ()
    )
    cumulative_fill = any(
        isinstance(fill.raw_payload, dict)
        and (fill.raw_payload.get("is_cumulative") or "cum_qty" in fill.raw_payload)
        for fill in observed_fills
    )
    return WatermarkPublicationPlan(
        command_id=command_id,
        outbox=outbox,
        recovery_diagnostic=(
            f"No outbox command exists for cumulative fill {command_id}"
            if outbox is None and cumulative_fill
            else None
        ),
    )
