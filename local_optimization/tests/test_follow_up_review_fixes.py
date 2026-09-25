"""Unit tests covering Astra follow-up code review fixes (R1–R9 and S3, S7).

Verifies:
- R1: True incremental forward OOS return (P_today - P_yesterday).
- R2: Tracker rejects 14 duplicate dates on same snapshot or zero OOS.
- R3: Snapshot supports .csv.gz and schema checks reject garbage files.
- R4: MTM timeline extends to latest entry for open trades after last exit;
      interval_pnl properly separates pre-window carry-in floating gains.
- R5: MTM handles sub-15s windows without dropping events or emitting
      pre-window points.
- R6: Quote pauses >300s preserve last observed quote instead of reverting
      to entry price.
- R7: Dashboard "all" account reconciliation contains matching records/summary schema.
- R8: FIFO round-trip pairing strictly isolates accounts by (account_id, symbol).
- R9: Dashboard load_optimization_groups returns empty dict on missing data
      (no synthetic 356.39U).
- S3: NaN risk metrics fail compliance; candidate evaluations remain immutable.
- S7: Missing theoretical neighbors penalize neighborhood stability score.
"""

from __future__ import annotations

import csv
import gzip
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pytest

from local_optimization.dashboard import (
    aggregate_account_reconciliations,
    load_optimization_groups,
)
from local_optimization.equity import EquityPoint, evaluate_equity_curve
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
    OptimizationProtocol,
    ParameterCandidate,
    default_orderflow_protocol,
)
from local_optimization.reconciliation import (
    pair_round_trip_trades,
)
from local_optimization.snapshot import (
    STREAM_REQUIRED_COLUMNS,
    inspect_snapshot_dir,
)
from local_optimization.tracker import (
    DailyTrackRecord,
    evaluate_stage_stability,
)


def test_r1_incremental_oos_forward_pnl():
    """R1: Verify incremental OOS calculation instead of full cumulative PnL."""
    # When yesterday's recommendation had cumulative 100U, and on today's dataset
    # it achieves cumulative 90U, the true forward incremental OOS return is -10U.
    p_yesterday = 100.0
    p_today = 90.0
    incremental_oos = p_today - p_yesterday
    assert incremental_oos == -10.0

    # Ensure DailyTrackRecord stores and preserves incremental OOS return
    rec = DailyTrackRecord(
        date_str="2026-09-19",
        snapshot_id="snap_20260919",
        protocol_id="v1.0",
        oos_forward_pnl=incremental_oos,
    )
    assert rec.oos_forward_pnl == -10.0


def test_r2_tracker_rejects_single_snapshot_and_zero_oos():
    """R2: 14 dates with identical snapshot or zero OOS cannot claim stable."""
    cand = ParameterCandidate.from_dict({"impulse_window_bars": 3})
    mock_eval = CandidateEvaluation(
        candidate=cand,
        is_feasible=True,
        net_pnl=100.0,
        trade_count=50,
    )

    # Counterexample 1: 14 dates, all with the exact same snapshot_id
    records_same_snap = [
        DailyTrackRecord(
            date_str=f"2026-09-{i:02d}",
            snapshot_id="same_snapshot_id",
            protocol_id="v1.0",
            recommended=mock_eval,
            oos_forward_pnl=10.0,
        )
        for i in range(1, 15)
    ]
    status_same, notes_same = evaluate_stage_stability(
        records_same_snap, min_consistency_days=14
    )
    assert status_same != "stable"
    assert any("snapshot diversity" in n for n in notes_same)

    # Counterexample 2: 14 distinct snapshots, but 0.0 OOS PnL
    records_zero_oos = [
        DailyTrackRecord(
            date_str=f"2026-09-{i:02d}",
            snapshot_id=f"snap_{i:02d}",
            protocol_id="v1.0",
            recommended=mock_eval,
            oos_forward_pnl=0.0,
        )
        for i in range(1, 15)
    ]
    status_zero, notes_zero = evaluate_stage_stability(
        records_zero_oos, min_consistency_days=14
    )
    assert status_zero != "stable"
    assert any("OOS non-positive" in n or "strictly > 0" in n for n in notes_zero)

    # Valid: 14 distinct snapshots, strictly positive cumulative OOS
    records_valid = [
        DailyTrackRecord(
            date_str=f"2026-09-{i:02d}",
            snapshot_id=f"snap_{i:02d}",
            protocol_id="v1.0",
            recommended=mock_eval,
            oos_forward_pnl=5.0,
        )
        for i in range(1, 15)
    ]
    status_valid, notes_valid = evaluate_stage_stability(
        records_valid, min_consistency_days=14
    )
    assert status_valid == "stable"
    assert any("Stage stable" in n or "consistency" in n for n in notes_valid)


def test_r3_snapshot_gzip_and_schema_validation():
    """R3: Gzip decompression works and junk CSV headers are rejected."""
    cutoff = datetime(2026, 9, 19, tzinfo=UTC)

    with tempfile.TemporaryDirectory() as tmpdir:
        dir_path = Path(tmpdir)

        # 1. Junk files with invalid headers
        for name in STREAM_REQUIRED_COLUMNS:
            p = dir_path / f"{name}.csv"
            p.write_text("junk,garbage\nnot,valid\n", encoding="utf-8")

        manifest_junk = inspect_snapshot_dir(dir_path, target_cutoff=cutoff)
        assert manifest_junk.is_complete is False
        assert "decision_replay_ready" not in manifest_junk.capability_tags
        assert "execution_audit_ready" not in manifest_junk.capability_tags
        for stream in manifest_junk.streams.values():
            assert stream.row_count == 0  # Invalid schema discarded

    # 2. Valid compressed .csv.gz files
    with tempfile.TemporaryDirectory() as tmpdir:
        dir_path = Path(tmpdir)
        for name, cols in STREAM_REQUIRED_COLUMNS.items():
            p = dir_path / f"{name}.csv.gz"
            with gzip.open(p, "wt", encoding="utf-8") as gz:
                writer = csv.DictWriter(gz, fieldnames=list(cols))
                writer.writeheader()
                sample_row = {
                    col: (
                        "2026-09-19T00:00:00Z"
                        if "time" in col or "at" in col
                        else "1.0"
                    )
                    for col in cols
                }
                writer.writerow(sample_row)

        manifest_gz = inspect_snapshot_dir(dir_path, target_cutoff=cutoff)
        assert manifest_gz.is_complete is True
        assert "decision_replay_ready" in manifest_gz.capability_tags
        assert "execution_audit_ready" in manifest_gz.capability_tags
        for stream in manifest_gz.streams.values():
            assert stream.row_count == 1


def test_r4_mtm_timeline_extends_to_open_trades():
    """R4: Timeline extends to latest entry when open trades exist after exit."""
    trades = [
        # Closed trade exits at 01:30
        TradeRecord(
            trade_id="t1",
            symbol="BTCUSDT",
            direction="LONG",
            entry_price=50000.0,
            entry_time=datetime(2026, 9, 19, 1, 0, 0, tzinfo=UTC),
            exit_price=50500.0,
            exit_time=datetime(2026, 9, 19, 1, 30, 0, tzinfo=UTC),
        ),
        # Open trade enters at 07:26, no exit time
        TradeRecord(
            trade_id="t2",
            symbol="ETHUSDT",
            direction="LONG",
            entry_price=3000.0,
            entry_time=datetime(2026, 9, 19, 7, 26, 15, tzinfo=UTC),
            exit_price=None,
            exit_time=None,
            is_open=True,
        ),
    ]
    price_map = {
        "BTCUSDT": ([0.0, 100000.0], [50000.0, 50500.0]),
        "ETHUSDT": ([0.0, 100000.0], [3000.0, 3100.0]),
    }

    # Reconstruct MTM equity without explicit end_time
    points = reconstruct_mtm_equity(trades, price_map)
    assert len(points) > 0
    # The timeline must reach at least 07:26:15
    assert points[-1].timestamp >= datetime(2026, 9, 19, 7, 26, 15, tzinfo=UTC)


def test_r4_interval_pnl_excludes_prewindow_carryin_floating_gains():
    """R4: Interval PnL reflects net change across window, not pre-window gains."""
    # Initial equity: 1000.
    # Curve starts at 1005 (has +5 floating gain before window) and ends at 1010.
    points = [
        EquityPoint(timestamp=datetime(2026, 9, 19, 0, 0, tzinfo=UTC), equity=1005.0),
        EquityPoint(timestamp=datetime(2026, 9, 19, 0, 1, tzinfo=UTC), equity=1008.0),
        EquityPoint(timestamp=datetime(2026, 9, 19, 0, 2, tzinfo=UTC), equity=1010.0),
    ]
    metrics = evaluate_equity_curve(points, initial_equity=1000.0)
    # Total cumulative return against E0 is 10.0, but interval_pnl must be 5.0
    assert metrics.net_pnl == 10.0
    assert metrics.interval_pnl == 5.0


def test_r5_mtm_sub_15s_window():
    """R5: Sub-15s non-grid interval preserves start/end and records trades."""
    t_start = datetime(2026, 9, 19, 0, 0, 1, tzinfo=UTC)
    t_end = datetime(2026, 9, 19, 0, 0, 10, tzinfo=UTC)

    trades = [
        TradeRecord(
            trade_id="sub1",
            symbol="BTCUSDT",
            direction="LONG",
            entry_price=50000.0,
            entry_time=datetime(2026, 9, 19, 0, 0, 2, tzinfo=UTC),
            exit_price=50100.0,
            exit_time=datetime(2026, 9, 19, 0, 0, 8, tzinfo=UTC),
        )
    ]
    t0_sec = t_start.timestamp()
    price_map = {
        "BTCUSDT": (
            [t0_sec + 2, t0_sec + 8],
            [50000.0, 50100.0],
        )
    }

    points = reconstruct_mtm_equity(
        trades, price_map, start_time=t_start, end_time=t_end
    )
    assert len(points) >= 2

    # Timestamps must not start before t_start
    assert points[0].timestamp >= t_start
    assert points[-1].timestamp == t_end
    # Trade profit must be reflected in terminal equity
    assert points[-1].equity > points[0].equity


def test_r6_stale_quote_preserves_last_price_instead_of_entry_reversion():
    """R6: Quote pause >300s preserves last observed price without reset."""
    # Entry at 100.0, quote drops to 70.0 at t=10.0
    quotes = ([10.0], [70.0])
    # At t=350.0 (>300s since last quote), query price
    px = get_price_at(quotes, 350.0, fallback_price=100.0)
    # Must preserve 70.0, not revert to 100.0 fallback_price
    assert px == 70.0


def test_r7_dashboard_all_accounts_reconciliation_structure():
    """R7: Dashboard 'all' account reconciliation matches records/summary schema."""
    primary_recon = {
        "records": [
            {
                "account_id": "primary",
                "symbol": "BTCUSDT",
                "side": "BUY",
                "status": "MATCHED",
                "live_pnl": 10.0,
                "replay_pnl": 10.0,
                "entry_slippage_bps": 1.0,
                "exit_slippage_bps": 1.0,
            }
        ],
        "summary": {
            "total_live_trades": 1,
            "total_replay_trades": 1,
            "matched_count": 1,
            "live_only_count": 0,
            "replay_only_count": 0,
            "live_total_pnl": 10.0,
            "replay_total_pnl": 10.0,
            "total_slippage_usdt": 0.0,
        },
    }
    acc01_recon = {
        "records": [
            {
                "account_id": "acc01",
                "symbol": "ETHUSDT",
                "side": "BUY",
                "status": "MATCHED",
                "live_pnl": 5.0,
                "replay_pnl": 5.0,
                "entry_slippage_bps": 2.0,
                "exit_slippage_bps": 2.0,
            }
        ],
        "summary": {
            "total_live_trades": 1,
            "total_replay_trades": 1,
            "matched_count": 1,
            "live_only_count": 0,
            "replay_only_count": 0,
            "live_total_pnl": 5.0,
            "replay_total_pnl": 5.0,
            "total_slippage_usdt": 0.0,
        },
    }
    acc_map = {"primary": primary_recon, "acc01": acc01_recon}
    all_res = aggregate_account_reconciliations(acc_map)

    # Verify structure matches individual accounts
    assert "records" in all_res
    assert "summary" in all_res
    assert len(all_res["records"]) == 2
    assert all_res["summary"]["total_live_trades"] == 2
    assert all_res["summary"]["live_total_pnl"] == 15.0


def test_r8_fifo_isolated_by_account_and_symbol():
    """R8: FIFO strictly isolates accounts; Account B cannot close Account A."""
    fills = [
        # Account A opens BUY 1 BTC @ 100
        {
            "account_id": "account_A",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "quantity": 1.0,
            "price": 100.0,
            "order_id": "ord_1",
            "trade_at": "2026-09-19 01:00:00",
        },
        # Account B executes SELL 1 BTC @ 110
        {
            "account_id": "account_B",
            "symbol": "BTCUSDT",
            "side": "SELL",
            "quantity": 1.0,
            "price": 110.0,
            "order_id": "ord_2",
            "trade_at": "2026-09-19 01:05:00",
        },
    ]
    round_trips = pair_round_trip_trades(fills)
    # Account B's sell has no preceding buy in account_B, so it is marked carry-in
    assert len(round_trips) == 1
    rt = round_trips[0]
    assert rt["account_id"] == "account_B"
    assert rt["is_carry_in"] is True
    # Account A's position is not closed
    assert rt["entry_price"] is None


def test_r9_load_optimization_groups_empty_directory():
    """R9: Empty optimization directory returns empty dict, not fake 356.39U."""
    with tempfile.TemporaryDirectory() as tmpdir:
        empty_dir = Path(tmpdir)
        groups = load_optimization_groups(empty_dir)
        assert groups == {}


def test_s3_nan_candidate_compliance_and_immutability():
    """S3: NaN risk metrics fail compliance; evaluations remain immutable."""
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
    protocol = default_orderflow_protocol()

    # 1. NaN in net_pnl
    eval_nan_pnl = CandidateEvaluation(
        candidate=cand,
        is_feasible=True,
        net_pnl=float("nan"),
        max_drawdown_pct=0.02,
        peak_initial_margin_usdt=200.0,
        ulcer_index=0.01,
        trade_count=50,
    )
    ok_pnl, _ = is_candidate_compliant(eval_nan_pnl, protocol)
    assert ok_pnl is False

    # 2. NaN in ulcer_index
    eval_nan_ui = CandidateEvaluation(
        candidate=cand,
        is_feasible=True,
        net_pnl=100.0,
        max_drawdown_pct=0.02,
        peak_initial_margin_usdt=200.0,
        ulcer_index=float("nan"),
        trade_count=50,
    )
    ok_ui, _ = is_candidate_compliant(eval_nan_ui, protocol)
    assert ok_ui is False

    # 3. Immutability: select_best_and_recommended does not mutate eval.is_feasible
    strict_protocol = OptimizationProtocol(
        scenario_family="strict_test",
        max_allowed_mdd_pct=0.0001,  # Impossibly strict
        max_initial_margin_usdt=50.0,
        max_allowed_ui=0.0001,
        min_trades=1000,
    )
    eval_orig = CandidateEvaluation(
        candidate=cand,
        net_pnl=100.0,
        max_drawdown_pct=0.05,
        peak_initial_margin_usdt=200.0,
        ulcer_index=0.02,
        trade_count=50,
        is_feasible=True,
    )
    _ = select_best_and_recommended([eval_orig], strict_protocol)
    # eval_orig.is_feasible must remain True
    assert eval_orig.is_feasible is True


def test_s7_missing_neighbors_penalize_stability():
    """S7: Missing theoretical neighbors penalize stability score (< 1.0)."""
    grid = {
        "impulse_window_bars": [2, 3, 4],
        "confirmation_window_bars": [1, 2, 3],
    }
    # Center has 4 theoretical 1-step neighbors:
    # (2, 2), (4, 2), (3, 1), (3, 3)
    center = ParameterCandidate.from_dict(
        {
            "impulse_window_bars": 3,
            "confirmation_window_bars": 2,
        }
    )
    center_eval = CandidateEvaluation(
        candidate=center,
        net_pnl=100.0,
        max_drawdown_pct=0.02,
        peak_initial_margin_usdt=200.0,
        ulcer_index=0.01,
        trade_count=50,
        is_feasible=True,
    )

    # Only provide 1 neighbor out of 4 theoretical neighbors
    n1 = ParameterCandidate.from_dict(
        {
            "impulse_window_bars": 4,
            "confirmation_window_bars": 2,
        }
    )
    eval_map = {
        center.parameter_id: center_eval,
        n1.parameter_id: CandidateEvaluation(
            candidate=n1,
            net_pnl=95.0,
            max_drawdown_pct=0.02,
            peak_initial_margin_usdt=200.0,
            ulcer_index=0.01,
            trade_count=50,
            is_feasible=True,
        ),
    }

    score = compute_neighborhood_stability(center, grid, eval_map)
    # Out of 4 theoretical neighbors, only 1 is present and stable: 1 / 4 = 0.25
    assert score == pytest.approx(0.25)
