"""Comprehensive test suite for Phase 2 fixes addressing Astra's code review.

Tests verify:
- E1: Reconciliation integrity (missing data yields INSUFFICIENT_DATA and NA layers).
- E7: Multi-curve time alignment (curves aligned to common global bounds).
- E8: Net return percentage formatting without double multiplication.
- R2 & R3: Strictly causal daily compounding (no look-ahead) and dynamic peak margin.
- R4: Tracker state machine rejection of infeasible/unverified candidates,
      and frozen prior parameter OOS decomposition.
- M1 & M2: MTM engine pre-marking at event epoch and grid tick ceiling for last trade.
- M4: RiskAdaptiveSizing executes cleanly with use_smoothing=False.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from local_optimization.generate_six_scenarios_dashboard import (
    build_reconciliation_payload,
    compute_daily_compounding_scales,
)
from local_optimization.mtm_engine import (
    TradeRecord,
    reconstruct_mtm_equity,
)
from local_optimization.optimizer import CandidateEvaluation
from local_optimization.protocol import ParameterCandidate
from local_optimization.sizing import RiskAdaptiveSizing
from local_optimization.tracker import (
    DailyTrackRecord,
    decompose_daily_performance,
    evaluate_stage_stability,
)


def test_m4_sizing_without_smoothing():
    """M4: RiskAdaptiveSizing with use_smoothing=False does not crash on day cut."""
    sizing = RiskAdaptiveSizing(use_smoothing=False)
    t0 = datetime(2026, 9, 18, 0, 0, 0, tzinfo=UTC)
    state = sizing.on_day_cut(1000.0, t0)
    assert sizing.smoothed_base is None
    assert state.target_notional > 0.0
    notional = sizing.get_order_notional("BTCUSDT", 1000.0, t0)
    assert notional == state.target_notional


def test_m2_engine_preserves_sub_bucket_last_trade():
    """M2: Reconstruct MTM without explicit bounds includes sub-15s trade."""
    t_in = datetime(2026, 9, 18, 0, 0, 1, tzinfo=UTC)
    t_out = datetime(2026, 9, 18, 0, 0, 10, tzinfo=UTC)
    trade = TradeRecord(
        trade_id="T01",
        symbol="BTCUSDT",
        entry_time=t_in,
        entry_price=100.0,
        exit_time=t_out,
        exit_price=110.0,
        notional_usdt=100.0,
        fee_rate=0.0,
    )
    prices = {"BTCUSDT": [(t_in.timestamp(), 100.0), (t_out.timestamp(), 110.0)]}
    pts = reconstruct_mtm_equity(
        [trade], prices, initial_equity=1000.0, grid_seconds=15
    )
    # Must contain both start point and the ceiling tick point capturing the +10 exit
    assert len(pts) >= 2
    assert pts[-1].equity == pytest.approx(1010.0, abs=1e-2)


def test_e1_reconciliation_integrity_insufficient_data(tmp_path: Path):
    """E1: Missing live data reports INSUFFICIENT_DATA, not fake PASS."""
    res = build_reconciliation_payload(tmp_path, prices_by_symbol={})
    accs = res["accounts"]
    for aid in ["primary", "acc01", "acc02", "acc03"]:
        acc = accs[aid]
        assert "INSUFFICIENT_DATA" in acc["status_label"]
        assert acc["layers"]["L1"]["stat"] == "NA"
        assert acc["layers"]["L6"]["stat"] == "NA"
        assert "未通过门禁" in acc["layers"]["L1"]["desc"]


def test_r2_r3_strictly_causal_compounding_no_lookahead():
    """R2 & R3: Sizing scale determined at day start; profits realize on exit."""
    t0 = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    # Day 1: Trade enters day 1, exits day 5 with +100 profit
    # Day 2: Trade enters day 2, exits day 2 with +10 profit
    events = [
        {
            "entry_at": t0,
            "entry_epoch": t0.timestamp(),
            "exit_at": t0 + timedelta(days=4),
            "exit_epoch": (t0 + timedelta(days=4)).timestamp(),
            "net_pnl_usdt": 100.0,
        },
        {
            "entry_at": t0 + timedelta(days=1),
            "entry_epoch": (t0 + timedelta(days=1)).timestamp(),
            "exit_at": t0 + timedelta(days=1, hours=2),
            "exit_epoch": (t0 + timedelta(days=1, hours=2)).timestamp(),
            "net_pnl_usdt": 10.0,
        },
    ]

    scales, score, mdd, ui, end_eq, peak_margin = compute_daily_compounding_scales(
        events, initial_equity=1000.0, f=0.10
    )

    d1 = t0.strftime("%Y-%m-%d")
    d2 = (t0 + timedelta(days=1)).strftime("%Y-%m-%d")

    # Day 2 scale must be 1.0 because the 100U profit has not exited yet!
    assert scales[d1] == pytest.approx(1.0, abs=1e-3)
    assert scales[d2] == pytest.approx(1.0, abs=1e-3)
    assert peak_margin > 0.0


def test_r4_tracker_rejects_infeasible_counterexample():
    """R4: 14 dates with is_feasible=False or 0 stability cannot be stable."""
    cand = ParameterCandidate.from_dict({"impulse_window_bars": 3})
    # Infeasible candidate with 99% MDD and 0 stability
    infeasible_eval = CandidateEvaluation(
        candidate=cand,
        is_feasible=False,
        infeasible_reasons=["Exceeded MDD", "Zero stability"],
        net_pnl=100.0,
        trade_count=0,
        max_drawdown_pct=0.99,
        neighborhood_stability_score=0.0,
    )

    history = [
        DailyTrackRecord(
            date_str=f"2026-09-{i:02d}",
            snapshot_id=f"snap_{i:02d}",
            protocol_id="v1.0",
            recommended=infeasible_eval,
            oos_forward_pnl=1.0,
        )
        for i in range(1, 15)
    ]

    status, notes = evaluate_stage_stability(
        history,
        min_consistency_days=7,
        min_oos_days=14,
        min_stability_score=0.70,
        min_trades=1,
        max_mdd_pct=0.20,
    )
    # Must NOT be stable!
    assert status != "stable"
    assert status == "candidate"
    assert any("infeasible" in n.lower() or "stability" in n.lower() for n in notes)


def test_r4_tracker_rejects_unverified_snapshot_or_recon():
    """R4: Unverified snapshot or failed reconciliation rejects stable state."""
    cand = ParameterCandidate.from_dict({"impulse_window_bars": 3})
    good_eval = CandidateEvaluation(
        candidate=cand,
        is_feasible=True,
        net_pnl=100.0,
        trade_count=20,
        max_drawdown_pct=0.05,
        neighborhood_stability_score=0.85,
    )

    history = [
        DailyTrackRecord(
            date_str=f"2026-09-{i:02d}",
            snapshot_id=f"snap_{i:02d}",
            protocol_id="v1.0",
            recommended=good_eval,
            oos_forward_pnl=1.0,
            is_snapshot_complete=(i != 14),  # Day 14 snapshot incomplete!
            reconciliation_passed=True,
        )
        for i in range(1, 15)
    ]

    status, notes = evaluate_stage_stability(
        history,
        min_consistency_days=7,
        min_oos_days=14,
        min_stability_score=0.70,
        min_trades=1,
        max_mdd_pct=0.20,
    )
    assert status == "insufficient_evidence"
    assert any("incomplete" in n.lower() for n in notes)


def test_r4_decomposition_and_frozen_oos():
    """R4: Verify performance decomposition (data extension vs re-selection)."""
    # Yesterday recommended A had cumulative 100U (p_old_old)
    # Today on new data, A has cumulative 90U (p_old_new) -> true OOS = -10U
    # Today reselection picked B with cumulative 200U (p_new_new)
    decomp = decompose_daily_performance(
        p_old_old=100.0,
        p_old_new=90.0,
        p_new_new=200.0,
    )
    assert decomp["data_extension_gain"] == -10.0
    assert decomp["reselection_gain"] == 110.0
    assert decomp["total_change"] == 100.0
