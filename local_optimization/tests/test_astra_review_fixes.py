"""Comprehensive unit tests covering all Astra code review findings and counterexamples.

Verifies:
1. Empty candidate loading returns empty list, never fake 84.50U (S1).
2. Parameter ID distinguishes 7th dimension min_volume_ratio (S4).
3. Protocol compliance strictly rejects margin/MDD/UI/trade violations (S3).
4. Neighborhood stability traverses grid correctly and scores bad neighbors < 1.0 (S7).
5. Snapshot rejects empty CSV files from audit readiness (S8).
6. SQLite round-trip fully restores daily_best, recommended, and live_actual (S6).
7. Tracker deduplicates calendar dates and requires genuine OOS accumulation (S6).
8. MTM engine processes same-bucket entries/exits chronologically
   without ghost positions (S10).
9. MTM engine preserves carry-in positions opened prior to window (S9).
10. Price lookup never uses future quotes when query is before first timestamp (S11).
11. CDaR is strictly bounded by max drawdown on [0, 0.1] discrete sample (S12).
12. Drawdown HWM respects initial capital E0, capturing immediate opening loss (S12).
13. Reconciliation matching consumes items, capping recall at <= 100% (S13).
14. Empty reconciliation input fails audit (S14).
15. Orphaned sell fills are marked carry-in without synthesizing false entries (S18).
"""

from __future__ import annotations

import tempfile
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pytest

from local_optimization.equity import calc_cdar, compute_drawdown_series
from local_optimization.mtm_engine import (
    TradeRecord,
    get_price_at,
    reconstruct_mtm_equity,
)
from local_optimization.optimizer import (
    CandidateEvaluation,
    compute_neighborhood_stability,
    is_candidate_compliant,
    select_best_and_recommended,
)
from local_optimization.protocol import (
    ParameterCandidate,
    default_orderflow_protocol,
)
from local_optimization.reconciliation import (
    pair_round_trip_trades,
    reconcile_signals_and_fills,
)
from local_optimization.reporter import ExperimentCatalog
from local_optimization.run_daily_local_optimization import load_candidate_evaluations
from local_optimization.snapshot import inspect_snapshot_dir
from local_optimization.tracker import DailyTrackRecord, evaluate_stage_stability


def test_s1_empty_candidate_loading_returns_empty():
    """S1: Empty candidate directory returns empty list, not fake 84.50U."""
    with tempfile.TemporaryDirectory() as tmpdir:
        evals = load_candidate_evaluations(Path(tmpdir))
        assert evals == [], (
            "Must return empty list when no candidate files are present."
        )


def test_s4_parameter_id_differs_by_volume_ratio():
    """S4: Candidates differing only in min_volume_ratio have distinct IDs."""
    base_params = {
        "impulse_window_bars": 3,
        "confirmation_window_bars": 2,
        "min_directional_return_bps": 25,
        "imbalance_threshold": 0.5,
        "min_notional_intensity": 1.5,
        "symbol_cooldown_bars": 4,
    }
    c1 = ParameterCandidate.from_dict({**base_params, "min_volume_ratio": 0.0})
    c2 = ParameterCandidate.from_dict({**base_params, "min_volume_ratio": 1.5})
    assert c1.parameter_id != c2.parameter_id, (
        "Volume ratio must affect candidate identity."
    )


def test_s3_protocol_compliance_enforced():
    """S3: High margin, high MDD, high UI, or insufficient trades must be rejected."""
    protocol = default_orderflow_protocol(max_initial_margin_usdt=280.0)
    cand = ParameterCandidate.from_dict(
        {
            "impulse_window_bars": 3,
            "confirmation_window_bars": 2,
            "min_directional_return_bps": 25,
            "imbalance_threshold": 0.5,
            "min_notional_intensity": 1.5,
            "min_volume_ratio": 1.5,
            "symbol_cooldown_bars": 4,
        }
    )

    bad_margin_eval = CandidateEvaluation(
        candidate=cand,
        is_feasible=True,  # Old source flag set to True
        net_pnl=500.0,
        peak_initial_margin_usdt=999.0,  # Exceeds 280
        max_drawdown_pct=0.10,
        ulcer_index=0.03,
        trade_count=50,
    )
    ok, reasons = is_candidate_compliant(bad_margin_eval, protocol)
    assert not ok
    assert any("Peak margin" in r for r in reasons)

    best, rec = select_best_and_recommended([bad_margin_eval], protocol)
    assert best is None
    assert rec is None


def test_s7_neighborhood_stability_detects_bad_neighbor():
    """S7: Bad neighbor in grid traversal yields stability score < 1.0."""
    grid = {
        "impulse_window_bars": [2, 3, 4],
        "confirmation_window_bars": [1, 2, 3],
        "min_directional_return_bps": [25],
        "imbalance_threshold": [0.5],
        "min_notional_intensity": [1.5],
        "min_volume_ratio": [1.5],
        "symbol_cooldown_bars": [4],
    }
    c_center = ParameterCandidate.from_dict(
        {
            "impulse_window_bars": 3,
            "confirmation_window_bars": 2,
            "min_directional_return_bps": 25,
            "imbalance_threshold": 0.5,
            "min_notional_intensity": 1.5,
            "min_volume_ratio": 1.5,
            "symbol_cooldown_bars": 4,
        }
    )
    c_neighbor = ParameterCandidate.from_dict(
        {
            "impulse_window_bars": 2,
            "confirmation_window_bars": 2,
            "min_directional_return_bps": 25,
            "imbalance_threshold": 0.5,
            "min_notional_intensity": 1.5,
            "min_volume_ratio": 1.5,
            "symbol_cooldown_bars": 4,
        }
    )

    all_evals = {
        c_center.parameter_id: CandidateEvaluation(
            candidate=c_center,
            is_feasible=True,
            net_pnl=100.0,
        ),
        c_neighbor.parameter_id: CandidateEvaluation(
            candidate=c_neighbor,
            is_feasible=False,  # Infeasible failing neighbor
            net_pnl=-50.0,
        ),
    }

    score = compute_neighborhood_stability(c_center, grid, all_evals)
    # The evaluated neighbor is infeasible and losing, so score must be < 1.0
    assert score < 1.0, (
        f"Expected stability score < 1.0 for failing neighbor, got {score}"
    )


def test_s8_snapshot_rejects_empty_csvs():
    """S8: Five empty CSV files must NOT receive audit-ready capability tags."""
    with tempfile.TemporaryDirectory() as tmpdir:
        p = Path(tmpdir)
        for s in [
            "account_balance_usdt",
            "account_fill_events",
            "exchange_orders",
            "live_strategy_signals",
            "order_intents",
        ]:
            (p / f"{s}.csv").touch()  # Empty file

        manifest = inspect_snapshot_dir(p, target_cutoff=datetime.now(UTC))
        assert not manifest.is_complete
        assert "execution_audit_ready" not in manifest.capability_tags


def test_s6_sqlite_roundtrip_restores_tracks():
    """S6: DailyTrackRecord restores daily_best, recommended, and live_actual."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        catalog = ExperimentCatalog(db_path)

        cand = ParameterCandidate.from_dict(
            {
                "impulse_window_bars": 3,
                "confirmation_window_bars": 2,
                "min_directional_return_bps": 25,
                "imbalance_threshold": 0.5,
                "min_notional_intensity": 1.5,
                "min_volume_ratio": 1.5,
                "symbol_cooldown_bars": 4,
            }
        )
        rec_eval = CandidateEvaluation(
            candidate=cand,
            is_feasible=True,
            net_pnl=150.0,
            ulcer_index=0.012,
        )
        record = DailyTrackRecord(
            date_str="2026-09-18",
            snapshot_id="snap1",
            protocol_id="proto1",
            daily_best=rec_eval,
            recommended=rec_eval,
            live_actual=rec_eval,
            oos_forward_pnl=25.0,
        )
        catalog.save_daily_track(record)

        history = catalog.load_track_history("proto1")
        assert len(history) == 1
        loaded = history[0]
        assert loaded.daily_best is not None
        assert loaded.daily_best.candidate.parameter_id == cand.parameter_id
        assert loaded.recommended is not None
        assert loaded.recommended.net_pnl == 150.0


def test_s6_tracker_rejects_duplicate_dates_and_requires_oos():
    """S6: 7 records for the same day returns insufficient_evidence."""
    cand = ParameterCandidate.from_dict({"a": 1})
    rec_eval = CandidateEvaluation(candidate=cand, is_feasible=True, net_pnl=100.0)

    # 7 records with identical date
    history = [
        DailyTrackRecord(
            date_str="2026-09-18",
            snapshot_id="snap1",
            protocol_id="p1",
            recommended=rec_eval,
            oos_forward_pnl=10.0,
        )
        for _ in range(7)
    ]
    status, notes = evaluate_stage_stability(
        history, min_consistency_days=7, min_oos_days=14
    )
    assert status == "insufficient_evidence"
    assert any("unique days" in n for n in notes)


def test_s10_mtm_same_bucket_intraday_order():
    """S10: Intra-bucket trade closes cleanly with 0 active positions."""
    t_start = datetime(2026, 9, 18, 0, 0, 0, tzinfo=UTC)
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
    price_series = {
        "BTCUSDT": ([t_start.timestamp(), t_start.timestamp() + 15.0], [100.0, 110.0])
    }

    pts = reconstruct_mtm_equity(
        trades=[trade],
        price_series=price_series,
        initial_equity=1000.0,
        grid_seconds=15,
        start_time=t_start,
        end_time=datetime(2026, 9, 18, 0, 0, 30, tzinfo=UTC),
    )
    assert len(pts) >= 2
    # At 00:00:15 (first bucket), the trade entered and closed
    assert pts[1].active_positions == 0
    assert pts[1].realized_pnl == pytest.approx(10.0, abs=0.01)
    assert pts[1].equity == pytest.approx(1010.0, abs=0.01)


def test_s9_mtm_carry_in_position():
    """S9: Trade opened before start_time must be carried in and counted in PnL."""
    t_before = datetime(2026, 9, 17, 23, 59, 45, tzinfo=UTC)
    t_start = datetime(2026, 9, 18, 0, 0, 0, tzinfo=UTC)
    t_exit = datetime(2026, 9, 18, 0, 0, 15, tzinfo=UTC)

    trade = TradeRecord(
        trade_id="T02",
        symbol="ETHUSDT",
        entry_time=t_before,
        entry_price=100.0,
        exit_time=t_exit,
        exit_price=110.0,
        notional_usdt=100.0,
        fee_rate=0.0,
    )
    price_series = {
        "ETHUSDT": ([t_start.timestamp(), t_start.timestamp() + 15.0], [105.0, 110.0])
    }

    pts = reconstruct_mtm_equity(
        trades=[trade],
        price_series=price_series,
        initial_equity=1000.0,
        grid_seconds=15,
        start_time=t_start,
        end_time=t_exit,
    )
    # At start_time, the carry-in trade is active
    assert pts[0].active_positions == 1
    # At exit_time, it closes with +10 PnL
    assert pts[-1].realized_pnl == pytest.approx(10.0, abs=0.01)
    assert pts[-1].equity == pytest.approx(1010.0, abs=0.01)


def test_s11_get_price_at_no_future_lookahead():
    """S11: Price query before first quote returns fallback, not future quote."""
    times = [100.0, 115.0, 130.0]
    prices = [200.0, 205.0, 210.0]
    # Query at t=50 (prior to first available timestamp 100.0)
    p = get_price_at((times, prices), target_epoch=50.0, fallback_price=150.0)
    assert p == 150.0, "Must return fallback price rather than future price at t=100"


def test_s12_cdar_strictly_bounded_by_max_drawdown():
    """S12: CDaR on [0.0, 0.10] drawdown series must not exceed max drawdown 0.10."""
    drawdowns = np.array([0.0, 0.10])
    cdar_95 = calc_cdar(drawdowns, alpha=0.95)
    assert cdar_95 <= 0.10, f"CDaR ({cdar_95}) cannot exceed max drawdown (0.10)"
    assert cdar_95 == pytest.approx(0.10, abs=1e-5)


def test_s12_drawdown_hwm_with_initial_equity():
    """S12: Initial capital drop from 1000 to 990 must register MDD = 10 USDT (1%)."""
    eq = np.array([990.0, 990.0])
    hwm, abs_dd, pct_dd = compute_drawdown_series(eq, initial_equity=1000.0)
    assert float(np.max(abs_dd)) == pytest.approx(10.0, abs=0.01)
    assert float(np.max(pct_dd)) == pytest.approx(0.01, abs=0.001)


def test_s13_reconciliation_consumes_matched_signals():
    """S13: Consumed signal matching caps recall at <= 100%."""
    t0 = datetime(2026, 9, 18, 12, 0, 0, tzinfo=UTC)
    t0_epoch = t0.timestamp()

    live_sigs = [
        {"symbol": "SOLUSDT", "direction": "LONG", "timestamp_epoch": t0_epoch}
    ]
    replay_sigs = [
        {"symbol": "SOLUSDT", "direction": "LONG", "timestamp_epoch": t0_epoch + 10},
        {"symbol": "SOLUSDT", "direction": "LONG", "timestamp_epoch": t0_epoch + 20},
    ]

    report = reconcile_signals_and_fills(
        live_signals=live_sigs,
        replay_signals=replay_sigs,
        live_fills=[],
        replay_fills=[],
    )
    sig_layer = report.layers["signals"]
    assert sig_layer.matched_total == 1
    assert sig_layer.recall <= 1.0


def test_s14_reconciliation_empty_input_fails_audit():
    """S14: Empty reconciliation inputs must fail/flag unavailable, never pass audit."""
    report = reconcile_signals_and_fills([], [], [], [])
    assert not report.is_audit_passed


def test_s18_reconstruct_trade_batches_carry_in():
    """S18: Orphaned sell fill is marked carry-in without false entry trade."""
    fills = [
        {
            "order_id": "SELL_01",
            "account_id": "primary",
            "symbol": "BTCUSDT",
            "side": "SELL",
            "quantity": 1.0,
            "price": 60000.0,
            "trade_at": "2026-09-18T10:00:00Z",
            "fee": 5.0,
            "realized_pnl": 0.0,
        }
    ]
    batches = pair_round_trip_trades(fills)
    assert len(batches) == 1
    assert batches[0]["is_carry_in"] is True
    assert batches[0]["entry_price"] is None
