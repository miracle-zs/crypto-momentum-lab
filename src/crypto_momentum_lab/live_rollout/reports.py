from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class LiveReportInput:
    signal_count: int
    approved_intent_count: int
    rejected_intent_count: int
    submitted_order_count: int
    filled_order_count: int
    partially_filled_order_count: int
    canceled_order_count: int
    rejected_order_count: int
    fees: tuple[Decimal, ...]
    expected_fill_prices: tuple[Decimal, ...]
    actual_fill_prices: tuple[Decimal, ...]
    realized_pnl_events: tuple[Decimal, ...]
    equity_curve: tuple[Decimal, ...]
    risk_halt_count: int
    reconciliation_mismatch_count: int
    account_flat: bool
    lease_released: bool


@dataclass(frozen=True, slots=True)
class LiveFinalReport:
    status: str
    signal_count: int
    approved_intent_count: int
    rejected_intent_count: int
    submitted_order_count: int
    filled_order_count: int
    partially_filled_order_count: int
    canceled_order_count: int
    rejected_order_count: int
    realized_fees: Decimal
    estimated_slippage: Decimal
    realized_pnl: Decimal
    max_drawdown: Decimal
    risk_halt_count: int
    reconciliation_mismatch_count: int
    account_flat: bool
    lease_released: bool


def build_live_final_report(data: LiveReportInput) -> LiveFinalReport:
    if len(data.expected_fill_prices) != len(data.actual_fill_prices):
        raise ValueError("expected and actual fill price counts must match")
    slippage = sum(
        (
            (actual - expected).copy_abs()
            for expected, actual in zip(
                data.expected_fill_prices,
                data.actual_fill_prices,
                strict=True,
            )
        ),
        start=Decimal("0"),
    )
    blocked = (
        data.reconciliation_mismatch_count > 0
        or not data.account_flat
        or not data.lease_released
    )
    return LiveFinalReport(
        status="blocked" if blocked else "completed",
        signal_count=data.signal_count,
        approved_intent_count=data.approved_intent_count,
        rejected_intent_count=data.rejected_intent_count,
        submitted_order_count=data.submitted_order_count,
        filled_order_count=data.filled_order_count,
        partially_filled_order_count=data.partially_filled_order_count,
        canceled_order_count=data.canceled_order_count,
        rejected_order_count=data.rejected_order_count,
        realized_fees=sum(data.fees, start=Decimal("0")),
        estimated_slippage=slippage,
        realized_pnl=sum(data.realized_pnl_events, start=Decimal("0")),
        max_drawdown=_max_drawdown(data.equity_curve),
        risk_halt_count=data.risk_halt_count,
        reconciliation_mismatch_count=data.reconciliation_mismatch_count,
        account_flat=data.account_flat,
        lease_released=data.lease_released,
    )


def _max_drawdown(equity_curve: tuple[Decimal, ...]) -> Decimal:
    if not equity_curve:
        return Decimal("0")
    peak = equity_curve[0]
    drawdown = Decimal("0")
    for equity in equity_curve:
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return drawdown
