"""Cumulative order progress used to settle command reservations."""

from dataclasses import dataclass
from decimal import Decimal

from crypto_momentum_lab.domain.execution.evidence_models import (
    ExecutionCumulativeOrderReport,
)


@dataclass(frozen=True, slots=True)
class CumulativeOrderDelta:
    quantity: Decimal
    watermark: tuple[Decimal, Decimal] | None


def cumulative_order_delta(
    report: ExecutionCumulativeOrderReport,
    *,
    previous_quantity: Decimal,
    previous_quote: Decimal,
) -> CumulativeOrderDelta:
    """Advance the order watermark without inferring account position facts."""
    if report.cumulative_quantity < previous_quantity:
        return CumulativeOrderDelta(Decimal("0"), None)
    if report.cumulative_quantity == previous_quantity:
        if report.cumulative_quote != previous_quote:
            raise ValueError("cumulative order quote changed without quantity")
        return CumulativeOrderDelta(Decimal("0"), None)
    if report.cumulative_quote <= previous_quote:
        raise ValueError("cumulative order quote did not increase with quantity")
    return CumulativeOrderDelta(
        report.cumulative_quantity - previous_quantity,
        (report.cumulative_quantity, report.cumulative_quote),
    )
