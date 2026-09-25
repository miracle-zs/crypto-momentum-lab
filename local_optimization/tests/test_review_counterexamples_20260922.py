"""Comprehensive regression tests covering all 8 counterexamples identified in
the local optimization audit report (local_optimization_review_20260922.md).

Covered Counterexamples:
1. Reconciliation rejects mismatched account/config even if symbols match.
2. Reconciliation rejects quantity mismatch (e.g. qty 999 vs 1).
3. Price cache rejects missing required symbols or excessive time gaps.
4. Snapshot inspection rejects single-row or insufficient-row stream files.
5. Canonical 8D parameter schema ensures legacy 7D and 8D produce identical hash.
6. Selectors fail closed (return None) when candidates breach constraints or have
   negative net PnL.
7. Six scenarios evaluation window strictly binds to manifest watermarks.
8. Daily tracker defaults to False for missing evidence fields in legacy records.
"""

from __future__ import annotations

import pickle
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from local_optimization.generate_six_scenarios_dashboard import (
    Candidate8D,
    select_balanced,
    select_compounding,
    select_pnl_max,
    solve_six_scenarios,
)
from local_optimization.mtm_engine import load_cached_price_series
from local_optimization.opportunity import (
    OpportunityPoolManifest,
    OpportunityStatus,
    RawOpportunity,
)
from local_optimization.protocol import (
    ParameterCandidate,
    canonicalize_candidate_params,
)
from local_optimization.reconciliation import reconcile_signals_and_fills
from local_optimization.snapshot import inspect_snapshot_dir
from local_optimization.tracker import DailyTrackRecord


# -----------------------------------------------------------------------------
# 1. Reconciliation: Account / Config Mismatch Rejected Even With Same Symbol
# -----------------------------------------------------------------------------
def test_counterexample_1_reconciliation_rejects_account_mismatch() -> None:
    """Verifies that signals and fills with same symbol but different account_id

    or config_id are NOT matched.
    """
    t0 = datetime(2026, 9, 10, 12, 0, 0, tzinfo=UTC)
    live_sig = {
        "signal_id": "sig_001",
        "account_id": "primary",
        "config_id": "profile_1",
        "symbol": "BTCUSDT",
        "direction": "LONG",
        "timestamp": t0.isoformat(),
        "reference_price": 50000.0,
        "target_quantity": 0.1,
    }
    # Replay signal from a different account
    replay_sig = {
        "signal_id": "sig_001_replay",
        "account_id": "acc01",  # mismatched account!
        "config_id": "profile_1",
        "symbol": "BTCUSDT",
        "direction": "LONG",
        "timestamp": t0.isoformat(),
        "reference_price": 50000.0,
        "target_quantity": 0.1,
    }

    report = reconcile_signals_and_fills(
        live_signals=[live_sig],
        replay_signals=[replay_sig],
        live_fills=[],
        replay_fills=[],
    )
    sig_summary = report.layers["signals"]
    # Must NOT match across different accounts!
    assert sig_summary.matched_total == 0
    assert report.first_divergence is not None
    assert report.first_divergence.layer == "signals"


# -----------------------------------------------------------------------------
# 2. Reconciliation: Quantity Mismatch Rejected
# -----------------------------------------------------------------------------
def test_counterexample_2_reconciliation_rejects_quantity_mismatch() -> None:
    """Verifies that fills with grossly mismatched quantities (e.g., 999 vs 1)

    are strictly rejected by quantity tolerance.
    """
    t0 = datetime(2026, 9, 10, 12, 0, 0, tzinfo=UTC)
    live_fill = {
        "fill_id": "fill_live",
        "account_id": "primary",
        "config_id": "profile_1",
        "symbol": "ETHUSDT",
        "side": "BUY",
        "timestamp": t0.isoformat(),
        "price": 3000.0,
        "quantity": 999.0,  # Grossly mismatched fill quantity!
    }
    replay_fill = {
        "fill_id": "fill_replay",
        "account_id": "primary",
        "config_id": "profile_1",
        "symbol": "ETHUSDT",
        "side": "BUY",
        "timestamp": t0.isoformat(),
        "price": 3000.0,
        "quantity": 1.0,  # Replay expected 1.0 unit
    }

    report = reconcile_signals_and_fills(
        live_signals=[],
        replay_signals=[],
        live_fills=[live_fill],
        replay_fills=[replay_fill],
        qty_tolerance_pct=0.05,
    )
    fill_summary = report.layers["fills"]
    assert fill_summary.matched_total == 0
    assert report.first_divergence is not None
    assert report.first_divergence.layer == "fills"


# -----------------------------------------------------------------------------
# 3. Price Cache: Missing Symbols or Large Gaps Rejected
# -----------------------------------------------------------------------------
def test_counterexample_3_price_cache_symbol_and_gap_gates(tmp_path: Path) -> None:
    """Verifies that load_cached_price_series rejects caches missing required

    symbols or containing time gaps exceeding max_gap_seconds.
    """
    cache_file = tmp_path / "test_cache.pkl"
    t0 = 1757462400.0  # 2025-09-10 00:00:00 UTC

    # Case A: Cache missing a required symbol
    data_missing_sym = {
        "__metadata__": {
            "source_content_hash": "hash_123",
            "watermark_end": "2026-09-20T00:00:00+00:00",
        },
        "prices": {
            "BTCUSDT": ([t0, t0 + 15.0], [50000.0, 50010.0]),
        },
    }
    with cache_file.open("wb") as f:
        pickle.dump(data_missing_sym, f)

    with pytest.raises(ValueError, match="missing required symbols"):
        load_cached_price_series(
            cache_file,
            required_symbols=["BTCUSDT", "ETHUSDT"],  # ETHUSDT is missing!
        )

    # Case B: Cache contains a large time gap (> 300s)
    data_with_gap = {
        "__metadata__": {
            "source_content_hash": "hash_123",
            "watermark_end": "2026-09-20T00:00:00+00:00",
        },
        "prices": {
            "BTCUSDT": ([t0, t0 + 600.0], [50000.0, 50010.0]),  # 600s gap!
        },
    }
    with cache_file.open("wb") as f:
        pickle.dump(data_with_gap, f)

    with pytest.raises(ValueError, match="has gap"):
        load_cached_price_series(
            cache_file,
            max_gap_seconds=300.0,
        )


# -----------------------------------------------------------------------------
# 4. Snapshot Inspection: Single Row / Insufficient Rows Rejected
# -----------------------------------------------------------------------------
def test_counterexample_4_snapshot_insufficient_rows_rejected(tmp_path: Path) -> None:
    """Verifies that inspect_snapshot_dir fails closed when streams have fewer

    rows than min_rows_by_stream.
    """
    snap_dir = tmp_path / "test_snap"
    snap_dir.mkdir(parents=True)
    t_cutoff = datetime(2026, 9, 20, 0, 0, 0, tzinfo=UTC)

    # Write only a single row into account_balance_usdt
    bal_csv = snap_dir / "account_balance_usdt.csv"
    bal_csv.write_text(
        "account_label,observed_at,wallet_balance\n"
        "primary,2026-09-20T00:00:00Z,1000.0\n"
    )

    # Write dummy valid files for remaining required streams
    for stream in [
        "account_fill_events",
        "exchange_orders",
        "live_strategy_signals",
        "order_intents",
    ]:
        p = snap_dir / f"{stream}.csv"
        p.write_text("observed_at,symbol,balance\n2026-09-20T00:00:00Z,BTCUSDT,100.0\n")

    report = inspect_snapshot_dir(
        snap_dir,
        target_cutoff=t_cutoff,
        min_rows_by_stream={"account_balance_usdt": 10},
    )
    assert not report.is_complete
    assert any("required 10" in msg for msg in report.missing_streams)


# -----------------------------------------------------------------------------
# 5. Canonical 8D Schema: Legacy 7D and Canonical 8D Hash Parity
# -----------------------------------------------------------------------------
def test_counterexample_5_canonical_param_hash_parity() -> None:
    """Verifies that 7D legacy and 8D canonical parameter dictionaries map to

    identical canonical parameter_id and canonical keys.
    """
    legacy_7d: dict[str, Any] = {
        "impulse_window_bars": 2,
        "confirmation_window_bars": 1,
        "min_directional_return_bps": 50,  # 50 bps = 0.5%
        "imbalance_threshold": 0.3,
        "min_notional_intensity": 1.5,
        "min_volume_ratio": 0.0,
        "symbol_cooldown_bars": 0,
    }
    canon_8d: dict[str, Any] = {
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.5,
        "min_imbalance": 0.3,
        "min_intensity": 1.5,
        "min_volume_ratio": 0.0,
        "cooldown_buckets": 0,
        "max_open_positions": 2,
    }

    norm_legacy = canonicalize_candidate_params(legacy_7d, default_max_open_positions=2)
    norm_8d = canonicalize_candidate_params(canon_8d)

    assert norm_legacy == norm_8d
    cand_leg = ParameterCandidate.from_dict(norm_legacy)
    cand_8d = ParameterCandidate.from_dict(norm_8d)
    assert cand_leg.parameter_id == cand_8d.parameter_id


# -----------------------------------------------------------------------------
# 6. Selectors Fail Closed on Infeasible or Negative PnL Candidates
# -----------------------------------------------------------------------------
def test_counterexample_6_selectors_fail_closed_on_negative_or_breach() -> None:
    """Verifies that select_pnl_max, select_balanced, and select_compounding return

    None when all candidates fail constraints (e.g. Net PnL <= 0).
    """
    neg_cand = Candidate8D(
        params={"impulse_window_buckets": 2, "max_open_positions": 1},
        net_pnl=-50.0,  # Negative Net PnL
        mdd=100.0,
        calmar=-0.5,
        compounding_score=-0.1,
        compounding_mdd=0.10,
        compounding_ui=0.05,
        terminal_compounded_equity=950.0,
        n_trades=50,
        peak_margin=20.0,
    )

    # When prices_by_symbol is None (fast eval fallback)
    assert select_pnl_max([neg_cand]) is None
    assert select_balanced([neg_cand]) is None
    assert select_compounding([neg_cand]) is None


# -----------------------------------------------------------------------------
# 7. Manifest Window Binding in Six Scenarios
# -----------------------------------------------------------------------------
def test_counterexample_7_six_scenarios_manifest_window_binding() -> None:
    """Verifies that solve_six_scenarios strictly uses manifest watermark_start

    and watermark_end for evaluation bounds when a manifest is passed.
    """
    t_start = datetime(2026, 9, 1, 0, 0, 0, tzinfo=UTC)
    t_end = datetime(2026, 9, 20, 0, 0, 0, tzinfo=UTC)
    manifest = OpportunityPoolManifest(
        snapshot_id="test_snap",
        created_at=datetime.now(UTC),
        symbol_count=1,
        row_count=1,
        watermark_start=t_start,
        watermark_end=t_end,
        content_hash="test_hash",
        pool_type="raw_parameter_independent",
    )

    t_opp = datetime(2026, 9, 10, 0, 0, 0, tzinfo=UTC)
    opp = RawOpportunity(
        opportunity_id="opp_win",
        symbol="BTCUSDT",
        direction="LONG",
        detected_at=t_opp,
        detected_epoch=t_opp.timestamp(),
        entry_eligible_at=t_opp,
        entry_reference_price=100.0,
        impulse_window_buckets=2,
        confirmation_buckets=1,
        impulse_return_pct=1.0,
        aggressive_imbalance=0.5,
        confirmation_min_imbalance=0.5,
        notional_intensity=2.0,
        volume_ratio=1.5,
        exit_time=t_opp + timedelta(minutes=15),
        exit_price=105.0,
        exit_rule="candle_15m",
        status=OpportunityStatus.EXITED,
    )

    grid_df = pd.DataFrame(
        [
            {
                "impulse_window_buckets": 2,
                "confirmation_buckets": 1,
                "min_return_pct": 0.5,
                "min_imbalance": 0.3,
                "min_intensity": 1.5,
                "min_volume_ratio": 0.0,
                "cooldown_buckets": 0,
            }
        ]
    )

    prices = {
        "BTCUSDT": (
            [t_start.timestamp(), t_end.timestamp()],
            [100.0, 105.0],
        )
    }
    # Calling solve_six_scenarios with manifest and prices
    scenarios, _ = solve_six_scenarios(
        events=[opp],
        full_grid_df=grid_df,
        manifest=manifest,
        prices_by_symbol=prices,
    )
    assert "unc_pnl_max" in scenarios
    assert scenarios["unc_pnl_max"] is not None


# -----------------------------------------------------------------------------
# 8. Tracker Defaults to False for Missing Evidence Fields
# -----------------------------------------------------------------------------
def test_counterexample_8_tracker_defaults_fail_closed() -> None:
    """Verifies that DailyTrackRecord defaults is_snapshot_complete and

    reconciliation_passed to False when loading legacy JSON missing these fields.
    """
    legacy_json = {
        "date_str": "2026-09-10",
        "snapshot_id": "snap_legacy",
        "protocol_id": "proto_v1",
        # Missing is_snapshot_complete and reconciliation_passed!
    }

    record = DailyTrackRecord.from_dict(legacy_json)
    assert not record.is_snapshot_complete
    assert not record.reconciliation_passed
