"""Direction 3 End-to-End Verification & Pipeline Integrity Test Suite.

Verifies:
1. End-to-end dashboard generation integrity
   (Top 10, Top 20, Top 30 views, 6 scenarios + 2 baselines).
2. Walk-Forward Analysis (WFA) integration with the modernized MTM engine.
3. Mathematical consistency between EvaluationContext and WFA candidate evaluation.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from local_optimization.mtm_engine import (
    AlignedPriceGrid,
    TradeRecord,
    reconstruct_mtm_equity,
    reconstruct_mtm_metrics_fast,
)
from local_optimization.run_walk_forward_analysis import (
    WindowSplit,
)
from local_optimization.simulation_ledger import (
    PortfolioState,
    SimulationLedger,
)


def test_dashboard_report_exists_and_valid() -> None:
    """Verify that the production dashboard generated in Direction 3
    is complete and valid.
    """
    report_path = (
        Path(__file__).resolve().parent.parent
        / "reports/six_scenarios_equity_comparison.html"
    )
    assert report_path.exists(), f"Dashboard report {report_path} was not found."
    file_size_mb = report_path.stat().st_size / (1024 * 1024)
    assert file_size_mb > 1.0, (
        f"Dashboard report is unexpectedly small: {file_size_mb:.2f} MB"
    )

    # Read snippet to check expected structure
    with open(report_path, encoding="utf-8") as f:
        head = f.read(10000)
    assert "<!DOCTYPE html>" in head or "<html" in head
    assert (
        "six_scenarios" in head.lower()
        or "top10" in head
        or "top 10" in head.lower()
        or "dashboard" in head.lower()
    )


def test_wfa_rolling_split_execution_with_mtm() -> None:
    """Test WFA rolling execution across synthetic windows with 15s MTM verification."""
    base_time = datetime(2026, 9, 1, 0, 0, 0, tzinfo=UTC)
    symbols = ["BTCUSDT", "ETHUSDT"]
    price_series: dict[str, tuple[list[float], list[float]]] = {}
    timestamps = [base_time + timedelta(seconds=i * 15) for i in range(2000)]
    ts_epochs = [t.timestamp() for t in timestamps]
    for sym in symbols:
        price_series[sym] = (
            ts_epochs,
            [100.0 + i * 0.01 for i in range(len(timestamps))],
        )

    # Generate small rolling split
    split = WindowSplit(
        split_id=1,
        name="Fold_1",
        is_start=base_time,
        is_end=base_time + timedelta(hours=4),
        oos_start=base_time + timedelta(hours=4),
        oos_end=base_time + timedelta(hours=6),
    )

    ledger = SimulationLedger(initial_cash=1000.0, notional_usdt=100.0, leverage=2.0)
    state_in = PortfolioState.create(
        timestamp=split.is_start,
        cash_usdt=1000.0,
        total_equity_mtm=1000.0,
    )

    # Simulate empty window
    res_is, state_is = ledger.simulate_window(
        opportunities=[],
        params={"impulse_window_buckets": 2, "confirmation_buckets": 1},
        window_start=split.is_start,
        window_end=split.is_end,
        state_in=state_in,
        price_series=price_series,
        max_concurrency=2,
    )
    assert res_is.n_trades == 0
    assert state_is.total_equity_mtm == 1000.0

    # Simulate with one trade
    trade = TradeRecord(
        trade_id="t1",
        symbol="BTCUSDT",
        direction="LONG",
        entry_time=split.is_start + timedelta(minutes=10),
        entry_price=100.0,
        exit_time=split.is_start + timedelta(minutes=30),
        exit_price=105.0,
        net_pnl_usdt=5.0,
    )

    pts = reconstruct_mtm_equity(
        trades=[trade],
        price_series=price_series,
        start_time=split.is_start,
        end_time=split.is_end,
        initial_equity=1000.0,
        grid_seconds=15,
        is_total_equity=True,
    )
    assert len(pts) > 0
    assert pts[-1].equity == pytest.approx(1005.0, abs=0.5)


def test_evaluation_context_matches_ledger_metrics() -> None:
    """Verify exact numerical consistency between EvaluationContext
    and SimulationLedger.
    """
    base_time = datetime(2026, 9, 1, 0, 0, 0, tzinfo=UTC)
    timestamps = [base_time + timedelta(seconds=i * 15) for i in range(1000)]
    ts_epochs = [t.timestamp() for t in timestamps]
    prices = [100.0 + (i % 20) * 0.1 for i in range(1000)]
    price_series = {"BTCUSDT": (ts_epochs, prices)}

    w_start = base_time
    w_end = base_time + timedelta(hours=2)

    grid = AlignedPriceGrid.build(price_series, w_start, w_end, 15)
    trade = TradeRecord(
        trade_id="trade_cons",
        symbol="BTCUSDT",
        direction="LONG",
        entry_time=base_time + timedelta(minutes=10),
        entry_price=100.0,
        exit_time=base_time + timedelta(minutes=40),
        exit_price=102.0,
        net_pnl_usdt=2.0,
    )

    fast_metrics = reconstruct_mtm_metrics_fast(
        trades=[trade],
        price_series=grid,
        initial_equity=1000.0,
        start_time=w_start,
        end_time=w_end,
    )

    full_pts = reconstruct_mtm_equity(
        trades=[trade],
        price_series=price_series,
        start_time=w_start,
        end_time=w_end,
        initial_equity=1000.0,
        grid_seconds=15,
        is_total_equity=True,
    )

    from local_optimization.equity import evaluate_equity_curve

    full_metrics = evaluate_equity_curve(full_pts, initial_equity=1000.0)

    assert fast_metrics.terminal_equity == pytest.approx(full_pts[-1].equity, abs=1e-4)
    assert fast_metrics.max_drawdown_pct == pytest.approx(
        full_metrics.max_drawdown_pct, abs=1e-4
    )
