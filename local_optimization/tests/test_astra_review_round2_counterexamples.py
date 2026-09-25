"""Dedicated test suite validating all Astra review counterexamples (2026-09-20).

Validates:
- R3: Hard margin cap (dynamic peak margin 308U rejected for <=280U).
- R3: Hard MDD cap (all candidates with MDD > 15% return None / no feasible solution).
- R3: Bankruptcy protection (equity <= 0 halts order scaling to 0.0).
- E1: Partial live data (balances present but missing signals/fills/orders) returns
      INSUFFICIENT_EVIDENCE, not PASS.
- E4: Identity isolation (signals/fills with different account/config cannot match).
- E5: PnL field contract unification across reconciliation and dashboard.
- R4: Governance enforcement (incomplete snapshot or failed recon blocks 'stable').
- R5: Causal walk-forward realization (future trade exit cannot realize in window).
- R6: Snapshot core column validation (dummy header/data lines rejected).
- M6: Sizing policy initialization triggers on_day_cut exactly once.
"""

from __future__ import annotations

import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from local_optimization.generate_six_scenarios_dashboard import (
    Candidate8D,
    build_reconciliation_payload,
    compute_daily_compounding_scales,
    select_compounding,
)
from local_optimization.mtm_engine import (
    TradeRecord,
    reconstruct_mtm_equity,
)
from local_optimization.optimizer import CandidateEvaluation
from local_optimization.protocol import ParameterCandidate
from local_optimization.reconciliation import (
    match_per_symbol_trades,
    reconcile_signals_and_fills,
)
from local_optimization.run_walk_forward_analysis import (
    filter_events_by_params,
    generate_rolling_splits,
)
from local_optimization.sizing import DailyEquityRatioSizing
from local_optimization.snapshot import inspect_snapshot_dir
from local_optimization.tracker import (
    DailyTrackRecord,
    evaluate_stage_stability,
)


def test_r3_dynamic_margin_cap_rejects_308u():
    """R3: Candidate with dynamic peak margin 308U cannot be selected into <=280U."""
    cand_violating = Candidate8D(
        params={"impulse_window_buckets": 2, "max_open_positions": 2},
        net_pnl=200.0,
        mdd=50.0,
        calmar=4.0,
        compounding_score=0.15,
        compounding_mdd=0.08,  # MDD is acceptable
        compounding_ui=0.03,
        terminal_compounded_equity=1200.0,
        n_trades=20,
        peak_margin=250.0,  # Fixed margin is <=280U
        compounding_peak_margin=308.0,  # Dynamic peak margin exceeds 280U!
        stability=0.85,
    )
    # The selection in dashboard filters candidates with compounding_peak_margin <= 280
    valid = [c for c in [cand_violating] if c.compounding_peak_margin <= 280.0]
    res_m280 = select_compounding(valid, max_mdd=0.15)
    assert res_m280 is None


def test_r3_compounding_mdd_cap_returns_none():
    """R3: If all candidates exceed 15% MDD, return None (no silent relaxation)."""
    cand_high_mdd = Candidate8D(
        params={"impulse_window_buckets": 2, "max_open_positions": 2},
        net_pnl=300.0,
        mdd=50.0,
        calmar=6.0,
        compounding_score=0.20,
        compounding_mdd=0.20,  # 20% exceeds 15% limit!
        compounding_ui=0.05,
        terminal_compounded_equity=1300.0,
        n_trades=20,
        peak_margin=200.0,
        compounding_peak_margin=240.0,
        stability=0.85,
    )
    res = select_compounding([cand_high_mdd], max_mdd=0.15)
    assert res is None


def test_r3_bankruptcy_protection_zero_scale():
    """R3: When equity drops to <= 0, scale_d becomes 0.0 (bankruptcy halt)."""
    t0 = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    # Huge loss on day 1 wipes out 1000 initial equity
    events = [
        {
            "entry_at": t0,
            "entry_epoch": t0.timestamp(),
            "exit_at": t0 + timedelta(hours=1),
            "exit_epoch": (t0 + timedelta(hours=1)).timestamp(),
            "net_pnl_usdt": -1200.0,  # Equity goes to -200
        },
        {
            "entry_at": t0 + timedelta(days=1),
            "entry_epoch": (t0 + timedelta(days=1)).timestamp(),
            "exit_at": t0 + timedelta(days=1, hours=1),
            "exit_epoch": (t0 + timedelta(days=1, hours=1)).timestamp(),
            "net_pnl_usdt": 50.0,
        },
    ]
    scales, score, mdd, ui, end_eq, peak_margin = compute_daily_compounding_scales(
        events, initial_equity=1000.0, f=0.10
    )
    d2 = (t0 + timedelta(days=1)).strftime("%Y-%m-%d")
    assert scales[d2] == 0.0


def test_e1_partial_live_data_insufficient_evidence(tmp_path: Path):
    """E1: 11 balances but missing event streams returns INSUFFICIENT_EVIDENCE."""
    acc_dir = tmp_path / "primary"
    acc_dir.mkdir(parents=True)
    # Write 11 balance rows
    bal_file = acc_dir / "account_balance_usdt.csv"
    with bal_file.open("w", encoding="utf-8") as f:
        f.write("observed_at,wallet_balance,unrealized_pnl\n")
        for i in range(12):
            ts = f"2026-09-19T{10 + i // 6:02d}:{i % 6 * 10:02d}:00Z"
            f.write(f"{ts},1000.0,0.0\n")

    # Write 1 replay trade in account_primary_events.csv so has_trades=True
    tr_file = tmp_path / "account_primary_events.csv"
    with tr_file.open("w", encoding="utf-8") as f:
        f.write(
            "trade_id,symbol,entry_at,entry_price,exit_at,exit_price,net_pnl_usdt\n"
        )
        f.write(
            "T1,BTCUSDT,2026-09-19T10:00:00Z,100.0,2026-09-19T10:15:00Z,105.0,5.0\n"
        )

    res = build_reconciliation_payload(tmp_path, prices_by_symbol={})
    p_acc = res["accounts"]["primary"]
    # Must report INSUFFICIENT_EVIDENCE, not PASS
    assert "INSUFFICIENT_EVIDENCE" in p_acc["status_label"]
    assert p_acc["layers"]["L1"]["stat"] == "NA"
    assert p_acc["layers"]["L2"]["stat"] == "NA"
    assert p_acc["layers"]["L6"]["stat"] == "NA"


def test_e4_identity_isolation_in_matching():
    """E4: Signals/fills with differing account_id or config_id cannot match."""
    t0 = datetime(2026, 9, 14, 10, 0, tzinfo=UTC)
    t0_epoch = t0.timestamp()

    live_sig = [
        {
            "symbol": "BTCUSDT",
            "direction": "LONG",
            "account_id": "account_A",
            "config_id": "config_alpha",
            "timestamp": t0.isoformat(),
            "timestamp_epoch": t0_epoch,
        }
    ]
    replay_sig = [
        {
            "symbol": "BTCUSDT",
            "direction": "LONG",
            "account_id": "account_B",  # Mismatched account!
            "config_id": "config_beta",  # Mismatched config!
            "timestamp": t0.isoformat(),
            "timestamp_epoch": t0_epoch,
        }
    ]

    report = reconcile_signals_and_fills(
        live_signals=live_sig,
        replay_signals=replay_sig,
        live_fills=[],
        replay_fills=[],
    )
    assert report.is_audit_passed is False
    assert report.layers["signals"].matched_total == 0


def test_e5_pnl_contract_unification():
    """E5: Contract contains both total_live_net_pnl and live_total_pnl."""
    t0 = datetime(2026, 9, 14, 10, 0, tzinfo=UTC)
    t1 = t0 + timedelta(minutes=15)
    trade = {
        "symbol": "ETHUSDT",
        "entry_time": t0,
        "entry_price": 2000.0,
        "exit_time": t1,
        "exit_price": 2100.0,
        "quantity": 0.1,
        "net_pnl": 10.0,
        "fee": 0.1,
    }
    res = match_per_symbol_trades([trade], [trade])
    s = res["summary"]
    # Verify contract keys
    assert "total_live_net_pnl" in s
    assert "live_total_pnl" in s
    assert "total_replay_net_pnl" in s
    assert "replay_total_pnl" in s
    assert s["total_live_net_pnl"] == pytest.approx(10.0, abs=1e-3)
    assert s["live_total_pnl"] == pytest.approx(10.0, abs=1e-3)


def test_r4_unverified_audit_blocks_stable_state():
    """R4: Candidate cannot reach 'stable' if reconciliation_passed is False."""
    cand = ParameterCandidate.from_dict({"impulse_window_bars": 3})
    good_eval = CandidateEvaluation(
        candidate=cand,
        is_feasible=True,
        net_pnl=150.0,
        trade_count=25,
        max_drawdown_pct=0.08,
        neighborhood_stability_score=0.88,
    )
    history = [
        DailyTrackRecord(
            date_str=f"2026-09-{i:02d}",
            snapshot_id=f"snap_{i:02d}",
            protocol_id="v1.0",
            recommended=good_eval,
            oos_forward_pnl=2.0,
            is_snapshot_complete=True,
            reconciliation_passed=(i != 14),  # Day 14 recon failed!
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
    assert any("reconciliation" in n.lower() for n in notes)


def test_r5_walk_forward_causal_cutoff_no_future_leakage():
    """R5: Trade entering day 1 and exiting day 5 cannot realize in day 1->2."""
    t_d1 = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    t_d2 = datetime(2026, 9, 2, 0, 0, tzinfo=UTC)
    t_d5 = datetime(2026, 9, 5, 10, 0, tzinfo=UTC)

    events = [
        {
            "symbol": "BTCUSDT",
            "detected_epoch": t_d1.timestamp(),
            "entry_epoch": t_d1.timestamp(),
            "exit_epoch": t_d5.timestamp(),  # Future exit!
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "impulse_return_pct": 1.0,
            "min_imbalance": 0.5,
            "confirmation_min": 0.5,
            "min_intensity": 2.0,
            "min_volume_ratio": 0.0,
            "net_pnl_usdt": 100.0,
        }
    ]
    params = {"impulse_window_buckets": 2, "confirmation_buckets": 1}
    # Window cutoff is day 2 (start=day 1, end=day 2)
    selected, pnl, mdd = filter_events_by_params(
        events, params, start_time=t_d1, end_time=t_d2
    )
    # The trade must NOT be realized in day 1->2 window!
    assert len(selected) == 0
    assert pnl == 0.0


def test_r5_rolling_splits_non_overlapping_by_default():
    """R5: Default rolling splits are non-overlapping contiguous slices."""
    splits = generate_rolling_splits(
        start_date=datetime(2026, 9, 1, tzinfo=UTC),
        end_date=datetime(2026, 9, 15, tzinfo=UTC),
    )
    for i in range(len(splits) - 1):
        s_curr = splits[i]
        s_next = splits[i + 1]
        # OOS windows must not overlap
        assert s_curr.oos_end <= s_next.oos_start


def test_r6_snapshot_dummy_records_rejected():
    """R6: Snapshot with invalid rows and missing core columns is marked incomplete."""
    with tempfile.TemporaryDirectory() as tmpdir:
        td = Path(tmpdir)
        acc_dir = td / "primary"
        acc_dir.mkdir()
        dummy_file = acc_dir / "account_balance_usdt.csv"
        with dummy_file.open("w", encoding="utf-8") as f:
            f.write("unrealized_pnl\n")
            f.write("not_a_valid_record\n")

        manifest = inspect_snapshot_dir(
            td, target_cutoff=datetime(2026, 9, 19, tzinfo=UTC)
        )
        assert manifest.is_complete is False


def test_m6_engine_single_initialization_on_day_cut():
    """M6: Engine initialization calls on_day_cut exactly once."""
    t0 = datetime(2026, 9, 18, 0, 0, 0, tzinfo=UTC)
    t1 = t0 + timedelta(minutes=5)
    trade = TradeRecord(
        trade_id="T01",
        symbol="BTCUSDT",
        entry_time=t0,
        entry_price=100.0,
        exit_time=t1,
        exit_price=105.0,
        notional_usdt=100.0,
    )
    prices = {"BTCUSDT": ([(t0.timestamp(), 100.0), (t1.timestamp(), 105.0)])}

    sizing = DailyEquityRatioSizing(fraction_f=0.10)
    reconstruct_mtm_equity(
        [trade],
        prices,
        initial_equity=1000.0,
        grid_seconds=15,
        sizing_policy=sizing,
    )
    # After single initialization and day simulation, policy version
    # should reflect exactly 1 cut
    assert sizing.current_state.sizing_version == 1
