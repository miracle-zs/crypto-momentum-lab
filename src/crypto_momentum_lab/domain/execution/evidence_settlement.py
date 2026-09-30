"""Pure cumulative evidence calculation. It never writes holdings or reservations."""

from dataclasses import dataclass, replace
from decimal import Decimal

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.domain.execution.evidence_models import (
    ExecutionCumulativeOrderReport,
)


@dataclass(frozen=True, slots=True)
class CumulativeFillDelta:
    fill: AccountFillEvent
    quantity: Decimal
    cumulative_quantity: Decimal
    watermark: tuple[Decimal, Decimal] | None


def cumulative_fill_delta(
    fill: AccountFillEvent,
    *,
    previous_quantity: Decimal,
    previous_quote: Decimal,
    trade_seen: bool,
) -> CumulativeFillDelta:
    raw = fill.raw_payload if isinstance(fill.raw_payload, dict) else {}
    quantity = Decimal(str(raw.get("cum_qty", fill.quantity)))
    quote = Decimal(str(raw.get("cum_quote", quantity * fill.price)))
    if not quantity.is_finite() or not quote.is_finite() or quantity < 0 or quote < 0:
        raise ValueError("Cumulative fill quantity or quote is invalid")
    delta = quantity - previous_quantity
    if delta < 0:
        return CumulativeFillDelta(fill, Decimal("0"), quantity, None)
    if delta == 0:
        if quote != previous_quote:
            raise ValueError("Cumulative quote changed without a quantity change")
        return CumulativeFillDelta(fill, Decimal("0"), quantity, None)
    delta_quote = quote - previous_quote
    if delta_quote <= 0:
        raise ValueError("Cumulative quote did not increase with cumulative quantity")
    if trade_seen:
        raise ValueError(
            f"Cumulative fill identity {fill.trade_id} was reused with "
            "a higher cumulative quantity"
        )
    return CumulativeFillDelta(
        replace(fill, quantity=delta, price=delta_quote / delta),
        delta,
        quantity,
        (quantity, quote),
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
    account_fills: tuple[AccountFillEvent, ...],
) -> CumulativeOrderDelta:
    real_fills = tuple(
        fill for fill in account_fills if fill.order_id == report.order_id
    )
    real_quantity = sum((fill.quantity for fill in real_fills), Decimal("0"))
    real_quote = sum((fill.quantity * fill.price for fill in real_fills), Decimal("0"))
    target_quantity = max(previous_quantity, report.cumulative_quantity, real_quantity)
    if target_quantity <= previous_quantity:
        return CumulativeOrderDelta(Decimal("0"), None)
    target_quote = (
        report.cumulative_quote
        if report.cumulative_quantity >= real_quantity
        else real_quote
    )
    if target_quote <= previous_quote:
        raise ValueError("cumulative order quote watermark did not advance")
    return CumulativeOrderDelta(
        target_quantity - previous_quantity, (target_quantity, target_quote)
    )


def account_trade_delta(
    order_id: str,
    *,
    previous_quantity: Decimal,
    previous_quote: Decimal,
    account_fills: tuple[AccountFillEvent, ...],
) -> CumulativeOrderDelta:
    """Advance settlement from real trades without counting a report twice."""
    fills = tuple(fill for fill in account_fills if fill.order_id == order_id)
    quantity = sum((fill.quantity for fill in fills), Decimal("0"))
    quote = sum((fill.quantity * fill.price for fill in fills), Decimal("0"))
    delta = max(Decimal("0"), quantity - previous_quantity)
    if delta <= 0:
        return CumulativeOrderDelta(Decimal("0"), None)
    if quote <= previous_quote:
        raise ValueError(
            "real account trade quote does not advance its durable order watermark"
        )
    return CumulativeOrderDelta(delta, (quantity, quote))
