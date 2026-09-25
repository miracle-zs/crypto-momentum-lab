"""Regression test suite for the three residual issues:
1. Unclosed trades preserved with exit_time=None, is_open=True (never fake-closed).
2. Formal MTM full candidate space optimization without top-50/100 truncation.
3. Strict cache invalidation and provenance binding against snapshot manifest.
"""

from __future__ import annotations

import pickle
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from local_optimization.generate_six_scenarios_dashboard import (
    Candidate8D,
    compute_daily_compounding_scales,
    select_balanced,
    select_compounding,
    select_pnl_max,
    to_trade_records,
)
from local_optimization.generate_six_scenarios_dashboard import (
    build_arg_parser as build_dashboard_arg_parser,
)
from local_optimization.mtm_engine import (
    load_cached_price_series,
    reconstruct_mtm_equity,
)
from local_optimization.opportunity import OpportunityPoolManifest, RawOpportunity
from local_optimization.run_daily_local_optimization import (
    build_arg_parser as build_daily_arg_parser,
)
from local_optimization.run_two_stage_grid_optimization import DIMS_8D
from local_optimization.run_walk_forward_analysis import (
    WindowSplit,
    run_walk_forward_analysis,
)
from local_optimization.run_walk_forward_analysis import (
    build_arg_parser as build_wfa_arg_parser,
)


def test_unclosed_trades_preserved_without_fake_15m_exit() -> None:
    """Issue 1: An open trade with exit_time=None must remain open (is_open=True).

    It must NOT be artificially forced to exit after 900 seconds at entry price.
    """
    t_entry = datetime(2026, 9, 10, 12, 0, 0, tzinfo=UTC)
    open_opp = {
        "opportunity_id": "OPP_OPEN_001",
        "symbol": "BTCUSDT",
        "entry_at": t_entry,
        "entry_reference_price": 60000.0,
        "exit_time": None,
        "exit_epoch": None,
        "exit_price": None,
        "net_pnl_usdt": None,
    }

    trades = to_trade_records([open_opp], compounding_scale=False)
    assert len(trades) == 1
    t = trades[0]
    assert t.exit_time is None
    assert t.exit_price is None
    assert t.is_open is True
    assert t.net_pnl_usdt is None

    # Verify MTM: Position must remain active across 2 hours (not exited after 15m)
    t_end = t_entry + timedelta(hours=2)
    times = [
        t_entry.timestamp(),
        (t_entry + timedelta(minutes=30)).timestamp(),
        t_end.timestamp(),
    ]
    prices = [60000.0, 61000.0, 62000.0]
    price_series = {"BTCUSDT": (times, prices)}

    pts = reconstruct_mtm_equity(
        trades,
        price_series,
        initial_equity=1000.0,
        start_time=t_entry,
        end_time=t_end,
        grid_seconds=900,  # 15m ticks
    )
    assert len(pts) > 2

    # At +30m and +2h, equity must reflect positive unrealized gain
    pt_30m = [p for p in pts if p.timestamp == t_entry + timedelta(minutes=30)][0]
    pt_end = pts[-1]
    assert pt_30m.unrealized_pnl > 0.0
    assert pt_end.unrealized_pnl > pt_30m.unrealized_pnl
    assert pt_end.equity > 1000.0


def test_cross_day_unclosed_trade_compounding_scale() -> None:
    """Issue 1 & 2: A position held across midnight must have its floating PnL

    evaluated at midnight cut for day sizing, and remain open in active_trades.
    """
    t0 = datetime(2026, 9, 5, 22, 0, 0, tzinfo=UTC)  # 22:00 Day 1
    t_exit = datetime(2026, 9, 6, 10, 0, 0, tzinfo=UTC)  # 10:00 Day 2
    midnight = datetime(2026, 9, 6, 0, 0, 0, tzinfo=UTC)

    trade_cross = {
        "symbol": "ETHUSDT",
        "entry_at": t0,
        "entry_reference_price": 2000.0,
        "exit_at": t_exit,
        "exit_price": 2200.0,
        "net_pnl_usdt": 10.0,
    }

    price_series = {
        "ETHUSDT": (
            [t0.timestamp(), midnight.timestamp(), t_exit.timestamp()],
            [2000.0, 2100.0, 2200.0],
        )
    }

    day_scales, score, mdd, ui, end_eq, peak_margin = compute_daily_compounding_scales(
        [trade_cross],
        initial_equity=1000.0,
        f=0.10,
        prices_by_symbol=price_series,
    )

    # Day 2 cut at midnight should reflect positive floating gain from Day 1
    assert "2026-09-06" in day_scales
    assert day_scales["2026-09-06"] > 1.0
    assert end_eq > 1000.0


def test_full_candidate_space_mtm_optimization_no_truncation() -> None:
    """Issue 2: verify_depth=0 must evaluate all contenders in sorted_contenders

    and select the global optimum according to true 15s MTM metrics.
    """
    t0 = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)

    # Create 60 candidates with differing proxy vs true MTM performance
    cands: list[Candidate8D] = []
    events: list[dict] = []

    # Two opportunities:
    # OPP_COMMON: return 0.5%, detected for (w=2, c=1), gives 5.0 USDT profit
    # OPP_CAND55: return 1.5%, detected for (w=3, c=1), gives 35.0 USDT profit
    events.append(
        {
            "opportunity_id": "OPP_COMMON",
            "symbol": "SYM_A",
            "detected_at": t0 + timedelta(hours=1),
            "entry_eligible_at": t0 + timedelta(hours=1),
            "entry_reference_price": 100.0,
            "exit_time": t0 + timedelta(hours=2),
            "exit_price": 105.0,
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "impulse_return_pct": 0.5,
            "aggressive_imbalance": 0.5,
            "confirmation_min_imbalance": 0.5,
            "notional_intensity": 2.0,
            "volume_ratio": 1.5,
            "net_pnl_usdt": 4.9,
        }
    )
    events.append(
        {
            "opportunity_id": "OPP_CAND55",
            "symbol": "SYM_B",
            "detected_at": t0 + timedelta(hours=1),
            "entry_eligible_at": t0 + timedelta(hours=1),
            "entry_reference_price": 100.0,
            "exit_time": t0 + timedelta(hours=2),
            "exit_price": 135.0,
            "impulse_window_buckets": 3,
            "confirmation_buckets": 1,
            "impulse_return_pct": 1.5,
            "aggressive_imbalance": 0.5,
            "confirmation_min_imbalance": 0.5,
            "notional_intensity": 2.0,
            "volume_ratio": 1.5,
            "net_pnl_usdt": 34.9,
        }
    )

    price_series = {
        "SYM_A": (
            [
                (t0 + timedelta(hours=1)).timestamp(),
                (t0 + timedelta(hours=2)).timestamp(),
            ],
            [100.0, 105.0],
        ),
        "SYM_B": (
            [
                (t0 + timedelta(hours=1)).timestamp(),
                (t0 + timedelta(hours=2)).timestamp(),
            ],
            [100.0, 135.0],
        ),
    }

    # Generate 60 candidates:
    # Candidates 0..54 have w=2 and proxy net_pnl = 100.0 down to 46.0
    # Candidate 55 has w=3 and proxy net_pnl = 45.0 (previously pruned by plateau)
    # But in true MTM, candidate 55 captures OPP_CAND55 and achieves higher equity!
    for i in range(60):
        w_bucket = 3 if i == 55 else 2
        cands.append(
            Candidate8D(
                params={
                    "impulse_window_buckets": w_bucket,
                    "confirmation_buckets": 1,
                    "min_return_pct": 0.5,
                    "min_imbalance": 0.3,
                    "min_intensity": 1.5,
                    "min_volume_ratio": 0.0,
                    "cooldown_buckets": 0,
                    "max_open_positions": 2,
                    "cand_idx": i,
                },
                net_pnl=float(100 - i),  # candidate 0 has highest proxy net_pnl (100.0)
                mdd=5.0,
                calmar=10.0,
                compounding_score=0.1,
                compounding_mdd=0.05,
                compounding_ui=0.01,
                terminal_compounded_equity=1000.0,
                n_trades=150,
                peak_margin=10.0,
            )
        )

    # 1. When verify_depth=0 (full space, no plateau truncation):
    # Candidate 55 must be evaluated and selected as the winner!
    selected = select_compounding(
        cands,
        max_mdd=0.15,
        margin_cap=280.0,
        events=events,
        prices_by_symbol=price_series,
        verify_depth=0,
    )
    assert selected is not None
    assert selected.params["cand_idx"] == 55, "Candidate 55 must be selected"
    assert selected.terminal_compounded_equity > 1030.0

    # Omitting verify_depth defaults to 0 and also selects candidate 55
    selected_default = select_compounding(
        cands,
        max_mdd=0.15,
        margin_cap=280.0,
        events=events,
        prices_by_symbol=price_series,
    )
    assert selected_default is not None
    assert selected_default.params["cand_idx"] == 55

    # 2. Strict hard margin_cap constraint enforcement:
    # If margin_cap is 5.0 (less than 10.0 peak margin), it must return None
    rejected_margin = select_compounding(
        cands,
        max_mdd=0.15,
        margin_cap=5.0,
        events=events,
        prices_by_symbol=price_series,
        verify_depth=0,
    )
    assert rejected_margin is None, "Hard margin cap must reject candidates"


def test_cache_provenance_validation_and_invalidation(tmp_path: Path) -> None:
    """Issue 3: Cache loading must validate manifest content_hash and watermark_end.

    Any mismatch or missing metadata must raise ValueError (Fail-Closed).
    """
    cache_file = tmp_path / "cache_15s_price_series.pkl"
    t_start = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)
    t_end = datetime(2026, 9, 18, 0, 0, tzinfo=UTC)

    manifest = OpportunityPoolManifest(
        snapshot_id="test_snapshot_01",
        created_at=datetime.now(UTC),
        symbol_count=1,
        row_count=100,
        watermark_start=t_start,
        watermark_end=t_end,
        content_hash="correct_sha256_hash_12345",
        pool_type="raw_parameter_independent",
        strategy_version="v1.0",
        schema_version="1.0",
    )

    valid_payload = {
        "__metadata__": {
            "cache_version": "v2",
            "source_content_hash": "correct_sha256_hash_12345",
            "watermark_end": t_end.isoformat(),
            "cleaning_rules": "data_complete=True,missing_agg_trade_count=0",
        },
        "prices": {"BTCUSDT": ([1.0, 2.0], [100.0, 101.0])},
    }

    with cache_file.open("wb") as f:
        pickle.dump(valid_payload, f)

    # 1. Loading with correct manifest succeeds
    prices = load_cached_price_series(cache_file, expected_manifest=manifest)
    assert "BTCUSDT" in prices

    # 2. Loading with mismatched content_hash fails closed
    manifest_tampered = OpportunityPoolManifest(
        snapshot_id="test_snapshot_01",
        created_at=datetime.now(UTC),
        symbol_count=1,
        row_count=100,
        watermark_start=t_start,
        watermark_end=t_end,
        content_hash="tampered_hash_99999",
        pool_type="raw_parameter_independent",
        strategy_version="v1.0",
        schema_version="1.0",
    )
    with pytest.raises(ValueError, match="content hash mismatch"):
        load_cached_price_series(cache_file, expected_manifest=manifest_tampered)

    # 3. Loading with mismatched watermark_end fails closed
    manifest_wrong_date = OpportunityPoolManifest(
        snapshot_id="test_snapshot_01",
        created_at=datetime.now(UTC),
        symbol_count=1,
        row_count=100,
        watermark_start=t_start,
        watermark_end=datetime(2026, 9, 19, 0, 0, tzinfo=UTC),
        content_hash="correct_sha256_hash_12345",
        pool_type="raw_parameter_independent",
        strategy_version="v1.0",
        schema_version="1.0",
    )
    with pytest.raises(ValueError, match="watermark mismatch"):
        load_cached_price_series(cache_file, expected_manifest=manifest_wrong_date)

    # 4. Loading legacy cache without metadata when manifest passed fails closed
    legacy_cache_file = tmp_path / "legacy_cache.pkl"
    with legacy_cache_file.open("wb") as f:
        pickle.dump({"BTCUSDT": ([1.0, 2.0], [100.0, 101.0])}, f)

    with pytest.raises(ValueError, match="legacy unverified format"):
        load_cached_price_series(legacy_cache_file, expected_manifest=manifest)


def test_default_verify_depth_zero_and_cli_defaults() -> None:
    """Issue 1: CLI and selector functions must default to verify_depth=0."""
    import inspect

    p1 = build_dashboard_arg_parser()
    assert p1.get_default("verify_depth") == 0

    p2 = build_daily_arg_parser()
    assert p2.get_default("verify_depth") == 0

    p3 = build_wfa_arg_parser()
    assert p3.get_default("mtm_verify_depth") == 0

    # Ensure selectors default to 0 (evaluating 100% of contenders without truncation)
    sig_pnl = inspect.signature(select_pnl_max)
    assert sig_pnl.parameters["verify_depth"].default == 0

    sig_bal = inspect.signature(select_balanced)
    assert sig_bal.parameters["verify_depth"].default == 0

    sig_comp = inspect.signature(select_compounding)
    assert sig_comp.parameters["verify_depth"].default == 0


def test_compounding_relies_on_true_mtm_no_coarse_prefilter() -> None:
    """Issue 2: select_compounding must not pre-filter by coarse compounding_mdd.

    A candidate whose coarse proxy compounding_mdd > max_mdd must be evaluated
    by real 15s MTM and selected if true 15s MTM max drawdown is acceptable.
    """
    t0 = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    t1 = t0 + timedelta(hours=1)
    events = [
        {
            "opportunity_id": "OPP_01",
            "symbol": "BTCUSDT",
            "detected_at": t0,
            "entry_eligible_at": t0,
            "entry_at": t0,
            "entry_epoch": t0.timestamp(),
            "entry_reference_price": 60000.0,
            "exit_time": t1,
            "exit_at": t1,
            "exit_epoch": t1.timestamp(),
            "exit_price": 61000.0,
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "impulse_return_pct": 0.8,
            "aggressive_imbalance": 0.5,
            "confirmation_min_imbalance": 0.5,
            "notional_intensity": 2.0,
            "volume_ratio": 1.5,
            "net_pnl_usdt": 50.0,
        }
    ]
    times = [t0.timestamp(), (t0 + timedelta(minutes=30)).timestamp(), t1.timestamp()]
    prices = [60000.0, 60500.0, 61000.0]  # Smooth upward climb, virtually 0% MDD
    price_series = {"BTCUSDT": (times, prices)}

    # Candidate with coarse compounding_mdd = 0.35 (> 0.15 limit)
    cand = Candidate8D(
        params={
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "min_return_pct": 0.5,
            "min_imbalance": 0.3,
            "min_intensity": 1.5,
            "min_volume_ratio": 0.0,
            "cooldown_buckets": 0,
            "max_open_positions": 2,
        },
        net_pnl=50.0,
        mdd=1.0,
        calmar=50.0,
        compounding_score=0.10,
        compounding_mdd=0.35,  # Coarse estimate was 35%!
        compounding_ui=0.01,
        terminal_compounded_equity=1050.0,
        n_trades=50,
        peak_margin=20.0,
    )

    # When evaluated with true 15s MTM, it should NOT be rejected
    selected = select_compounding(
        [cand],
        max_mdd=0.15,
        margin_cap=280.0,
        events=events,
        prices_by_symbol=price_series,
    )
    assert selected is not None, "Candidate must not be killed by coarse pre-filter"
    assert selected.compounding_mdd <= 0.15, "True MTM MDD must be evaluated"


def test_walk_forward_8d_space_and_dynamic_oos_slots() -> None:
    """Issue 3: WFA must operate over 8D space and use candidate slots."""
    t_start = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)
    t_is_end = datetime(2026, 9, 8, 0, 0, tzinfo=UTC)
    t_oos_end = datetime(2026, 9, 10, 0, 0, tzinfo=UTC)

    split = WindowSplit(
        split_id=1,
        name="Split_1",
        is_start=t_start,
        is_end=t_is_end,
        oos_start=t_is_end,
        oos_end=t_oos_end,
    )

    # 8D candidate pool with max_open_positions = 3
    cand_row = {
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.5,
        "min_imbalance": 0.3,
        "min_intensity": 1.5,
        "min_volume_ratio": 0.0,
        "cooldown_buckets": 0,
        "max_open_positions": 3,
    }
    cand_df = pd.DataFrame([cand_row])

    # 8D grid values
    grid_values = {d: [cand_row[d]] for d in DIMS_8D}

    opp = RawOpportunity(
        opportunity_id="OPP_WFA_01",
        symbol="BTCUSDT",
        direction="LONG",
        detected_at=t_start + timedelta(days=1),
        detected_epoch=(t_start + timedelta(days=1)).timestamp(),
        entry_eligible_at=t_start + timedelta(days=1),
        entry_reference_price=50000.0,
        impulse_window_buckets=2,
        confirmation_buckets=1,
        impulse_return_pct=0.8,
        aggressive_imbalance=0.35,
        confirmation_min_imbalance=0.35,
        notional_intensity=2.0,
        volume_ratio=0.5,
        exit_time=t_start + timedelta(days=1, hours=2),
        exit_price=51000.0,
        net_pnl_usdt=19.0,
    )

    results = run_walk_forward_analysis(
        events=[opp],
        splits=[split],
        candidate_pool_df=cand_df,
        grid_values=grid_values,
        wfa_mode="independent",
    )
    assert len(results) == 1
    res = results[0]
    # Check that candidate params preserved 8th dimension max_open_positions = 3
    assert res.rec_candidate.params.get("max_open_positions") == 3
    assert res.best_candidate.params.get("max_open_positions") == 3
