from dataclasses import replace
from decimal import Decimal

from crypto_momentum_lab.live_rollout.reports import (
    LiveReportInput,
    build_live_final_report,
)


def test_live_report_includes_fees_slippage_and_drawdown() -> None:
    report = build_live_final_report(_input())

    assert report.realized_fees == Decimal("0.03")
    assert report.estimated_slippage == Decimal("2")
    assert report.realized_pnl == Decimal("3")
    assert report.max_drawdown == Decimal("4")


def test_live_report_flags_reconciliation_mismatch() -> None:
    report = build_live_final_report(
        replace(_input(), reconciliation_mismatch_count=1)
    )

    assert report.status == "blocked"


def _input() -> LiveReportInput:
    return LiveReportInput(
        signal_count=2,
        approved_intent_count=1,
        rejected_intent_count=1,
        submitted_order_count=1,
        filled_order_count=1,
        partially_filled_order_count=0,
        canceled_order_count=0,
        rejected_order_count=0,
        fees=(Decimal("0.01"), Decimal("0.02")),
        expected_fill_prices=(Decimal("100"),),
        actual_fill_prices=(Decimal("102"),),
        realized_pnl_events=(Decimal("3"),),
        equity_curve=(Decimal("100"), Decimal("105"), Decimal("101")),
        risk_halt_count=0,
        reconciliation_mismatch_count=0,
        account_flat=True,
        lease_released=True,
    )
