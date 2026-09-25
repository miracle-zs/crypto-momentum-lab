"""Unit tests for parallelized 15s MTM verification and candidate selectors."""

from __future__ import annotations

import multiprocessing as mp
from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd

from local_optimization.generate_six_scenarios_dashboard import (
    Candidate8D,
    _init_verification_worker,
    _verify_single_contender,
    solve_six_scenarios,
    verify_contenders_batch,
)
from local_optimization.tests.test_opportunity_and_wfa_repair import (
    make_mock_opportunity,
)


def _build_test_environment() -> tuple[
    dict[str, Any], dict[tuple[int, int], list[Any]], datetime, datetime
]:
    t0 = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)
    t1 = t0 + timedelta(days=5)
    # Generate 15s series for BTCUSDT as (epochs, prices)
    timestamps = []
    prices = []
    cur = t0
    p = 50000.0
    while cur <= t1:
        timestamps.append(cur.timestamp())
        prices.append(p)
        cur += timedelta(seconds=15)
        p += 0.05  # slight upward drift

    prices_by_symbol = {"BTCUSDT": (timestamps, prices)}

    # Generate test opportunities with RawOpportunity
    opps = []
    for i in range(25):
        t_entry = t0 + timedelta(hours=i * 3 + 1)
        t_exit = t_entry + timedelta(minutes=30)
        opp = make_mock_opportunity(
            opp_id=f"opp_{i}",
            symbol="BTCUSDT",
            detected_at=t_entry - timedelta(seconds=15),
            entry_at=t_entry,
            entry_price=50000.0 + i * 10,
            exit_at=t_exit,
            exit_price=50200.0 + i * 10,
            net_pnl=8.0,
            w=2,
            c=1,
            r=0.8,
            imb=0.4,
            inten=3.5,
            vol=1.5,
        )
        opps.append(opp)

    opps_by_wc = {(2, 1): opps}
    return prices_by_symbol, opps_by_wc, t0, t1


def test_init_and_single_verify() -> None:
    """Verify single candidate verification in initialized worker state."""
    prices, opps_by_wc, w_start, w_end = _build_test_environment()
    _init_verification_worker(prices, opps_by_wc, w_start, w_end)

    params = {
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.5,
        "min_imbalance": 0.3,
        "min_intensity": 3.0,
        "min_volume_ratio": 1.0,
        "cooldown_buckets": 0,
        "max_open_positions": 2,
    }

    task = (0, params, 280.0, False, None)
    idx, res = _verify_single_contender(task)

    assert idx == 0
    assert res is not None
    assert "net_pnl" in res
    assert "mdd" in res
    assert "calmar" in res
    assert "peak_margin" in res
    assert res["peak_margin"] <= 280.0


def test_batch_verification_equivalence_and_memoization() -> None:
    """Verify single vs multi-worker equivalence and memoization hit rate."""
    prices, opps_by_wc, w_start, w_end = _build_test_environment()
    _init_verification_worker(prices, opps_by_wc, w_start, w_end)

    cands = []
    for ret in [0.5, 0.6, 0.7]:
        p = {
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "min_return_pct": ret,
            "min_imbalance": 0.3,
            "min_intensity": 3.0,
            "min_volume_ratio": 1.0,
            "cooldown_buckets": 0,
            "max_open_positions": 2,
        }
        c = Candidate8D(
            params=p,
            net_pnl=10.0,
            mdd=1.0,
            calmar=10.0,
            compounding_score=10.0,
            compounding_mdd=0.01,
            compounding_ui=0.005,
            terminal_compounded_equity=1010.0,
            n_trades=20,
            peak_margin=100.0,
            stability=0.9,
        )
        cands.append(c)

    # 1. Single worker execution with memo cache
    memo_cache: dict[tuple[tuple, float | None, bool], dict[str, Any] | None] = {}
    seq_results = verify_contenders_batch(
        cands,
        margin_cap=280.0,
        compounding_scale=False,
        pool=None,
        memo_cache=memo_cache,
    )
    assert len(seq_results) == 3
    assert len(memo_cache) == 3

    # 2. Re-running should hit memo cache directly (without tasks to run)
    cached_results = verify_contenders_batch(
        cands,
        margin_cap=280.0,
        compounding_scale=False,
        pool=None,
        memo_cache=memo_cache,
    )
    assert len(cached_results) == 3
    for (_, r1), (_, r2) in zip(seq_results, cached_results, strict=True):
        assert r1 == r2

    # 3. Multi-worker execution equivalence
    ctx = mp.get_context("spawn")
    with ctx.Pool(
        2,
        initializer=_init_verification_worker,
        initargs=(prices, opps_by_wc, w_start, w_end),
    ) as pool:
        parallel_results = verify_contenders_batch(
            cands,
            margin_cap=280.0,
            compounding_scale=False,
            pool=pool,
            memo_cache={},
        )

    assert len(parallel_results) == 3
    # Compare candidate PnL bit-by-bit
    seq_pnl_map = {
        tuple(sorted(c.params.items())): r["net_pnl"] for c, r in seq_results
    }
    par_pnl_map = {
        tuple(sorted(c.params.items())): r["net_pnl"] for c, r in parallel_results
    }
    assert seq_pnl_map == par_pnl_map


def test_solve_six_scenarios_smoke() -> None:
    """Smoke test solve_six_scenarios with parallel workers and verify_depth."""
    prices, opps_by_wc, _, _ = _build_test_environment()
    events = opps_by_wc[(2, 1)]

    # Create dummy candidate grid DataFrame
    rows = []
    for ret in [0.5, 0.6]:
        rows.append(
            {
                "impulse_window_buckets": 2,
                "confirmation_buckets": 1,
                "min_return_pct": ret,
                "min_imbalance": 0.3,
                "min_intensity": 3.0,
                "min_volume_ratio": 1.0,
                "cooldown_buckets": 0,
            }
        )
    df_grid = pd.DataFrame(rows)

    scenarios, candidates_dict = solve_six_scenarios(
        events=events,
        full_grid_df=df_grid,
        prices_by_symbol=prices,
        verify_depth=2,
        max_workers=2,
    )

    assert len(scenarios) == 6
    assert "m280_pnl_max" in scenarios
    assert "m280_balanced" in scenarios
    assert "m280_compounding" in scenarios
    assert "unc_pnl_max" in scenarios
    assert "unc_balanced" in scenarios
    assert "unc_compounding" in scenarios


def test_cross_margin_cache_reuse_bidirectional_and_soundness() -> None:
    """Verify cross-margin cache reuse in both directions without precision loss."""
    prices, opps_by_wc, w_start, w_end = _build_test_environment()
    _init_verification_worker(prices, opps_by_wc, w_start, w_end)

    cands = []
    for ret in [0.5, 0.6, 0.7]:
        p = {
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "min_return_pct": ret,
            "min_imbalance": 0.3,
            "min_intensity": 3.0,
            "min_volume_ratio": 1.0,
            "cooldown_buckets": 0,
            "max_open_positions": 2,
        }
        c = Candidate8D(
            params=p,
            net_pnl=10.0,
            mdd=1.0,
            calmar=10.0,
            compounding_score=10.0,
            compounding_mdd=0.01,
            compounding_ui=0.005,
            terminal_compounded_equity=1010.0,
            n_trades=20,
            peak_margin=40.0,
            stability=0.9,
        )
        cands.append(c)

    # 1. Forward direction: 280.0 evaluated first, then None requested
    memo_cache_fwd: dict[tuple[tuple, float | None, bool], dict[str, Any] | None] = {}
    res_280 = verify_contenders_batch(
        cands,
        margin_cap=280.0,
        compounding_scale=False,
        pool=None,
        memo_cache=memo_cache_fwd,
    )
    assert len(res_280) == 3

    # To prove None hits cache without running worker, temporarily unset worker
    import local_optimization.generate_six_scenarios_dashboard as gsd

    saved_worker = gsd._worker_context
    try:
        gsd._worker_context = None  # Any task run will fail and return None
        res_none_from_cache = verify_contenders_batch(
            cands,
            margin_cap=None,
            compounding_scale=False,
            pool=None,
            memo_cache=memo_cache_fwd,
        )
        assert len(res_none_from_cache) == 3
        for (_, r1), (_, r2) in zip(res_280, res_none_from_cache, strict=True):
            assert r1 == r2  # Bit-for-bit identical metrics
    finally:
        gsd._worker_context = saved_worker

    # 2. Reverse direction: None evaluated first, then 280.0 requested
    memo_cache_rev: dict[tuple[tuple, float | None, bool], dict[str, Any] | None] = {}
    res_none = verify_contenders_batch(
        cands,
        margin_cap=None,
        compounding_scale=False,
        pool=None,
        memo_cache=memo_cache_rev,
    )
    assert len(res_none) == 3

    try:
        gsd._worker_context = None
        res_280_from_cache = verify_contenders_batch(
            cands,
            margin_cap=280.0,
            compounding_scale=False,
            pool=None,
            memo_cache=memo_cache_rev,
        )
        assert len(res_280_from_cache) == 3
        for (_, r1), (_, r2) in zip(res_none, res_280_from_cache, strict=True):
            assert r1 == r2
    finally:
        gsd._worker_context = saved_worker

    # 3. Compounding mode cross-margin reuse
    memo_cache_comp: dict[tuple[tuple, float | None, bool], dict[str, Any] | None] = {}
    res_comp_280 = verify_contenders_batch(
        cands,
        margin_cap=280.0,
        compounding_scale=True,
        pool=None,
        memo_cache=memo_cache_comp,
    )
    assert len(res_comp_280) == 3

    try:
        gsd._worker_context = None
        res_comp_none_from_cache = verify_contenders_batch(
            cands,
            margin_cap=None,
            compounding_scale=True,
            pool=None,
            memo_cache=memo_cache_comp,
        )
        assert len(res_comp_none_from_cache) == 3
        for (_, r1), (_, r2) in zip(
            res_comp_280, res_comp_none_from_cache, strict=True
        ):
            assert r1 == r2
    finally:
        gsd._worker_context = saved_worker
