"""Pure cumulative report settlement and watermark publication plans."""

from dataclasses import dataclass
from decimal import Decimal

from crypto_momentum_lab.domain.execution.command_models import OutboxEntry
from crypto_momentum_lab.domain.execution.evidence_models import (
    ExecutionCumulativeOrderReport,
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
    outbox: OutboxEntry | None,
    has_active_reservations: bool,
) -> CumulativeReportPlan:
    delta = cumulative_order_delta(
        report,
        previous_quantity=previous_watermark[0],
        previous_quote=previous_watermark[1],
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
