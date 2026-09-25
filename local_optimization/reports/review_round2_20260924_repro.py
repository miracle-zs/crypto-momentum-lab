"""Synthetic audit cases.
Run from repo root with:
    python -m local_optimization.reports.review_round2_20260924_repro

Prints evidence only. Does not edit production data or regenerate dashboards.
"""

import io
import json
import sys
from contextlib import redirect_stdout
from dataclasses import replace
from datetime import UTC, datetime, time, timedelta
from types import SimpleNamespace

import pandas as pd

import local_optimization.generate_six_scenarios_dashboard as g
from crypto_momentum_lab.live_rollout.scheduled_risk_window import (
    ScheduledRiskWindowConfig,
)
from local_optimization.equity import evaluate_equity_curve
from local_optimization.simulation_ledger import PortfolioState, SimulationLedger
from local_optimization.tests.test_opportunity_and_wfa_repair import (
    make_mock_opportunity as mk,
)


def collect_evidence():
    start = datetime(2026, 9, 1, tzinfo=UTC)
    end = start + timedelta(minutes=15)
    opp = mk(
        "trade",
        "BTCUSDT",
        start,
        start + timedelta(minutes=1),
        100,
        start + timedelta(minutes=10),
        110,
    )
    prices = {
        "BTCUSDT": (
            [
                start.timestamp(),
                (start + timedelta(minutes=5)).timestamp(),
                end.timestamp(),
            ],
            [100, 105, 110],
        )
    }
    risk = ScheduledRiskWindowConfig(
        timezone="UTC",
        entry_stop_at=time(0, 5),
        flatten_start_at=time(0, 5),
        flatten_deadline_at=time(0, 5, 15),
        verify_at=time(0, 5, 30),
        reopen_at=time(0, 6),
    )
    evidence = {}

    g._init_verification_worker(prices, {(2, 1): [opp]}, start, end, risk)
    verified = g._verify_single_contender((0, {}, None, False, None))[1]
    displayed = g.get_scenario_events(
        "s_m280_pnl_max", {}, None, None, [opp], scheduled_risk_window=risk
    )
    evidence["scheduled_flatten"] = {
        "verified_pnl": verified["net_pnl"],
        "displayed_trade_pnl": displayed[0].calculated_net_pnl,
        "displayed_exit_price": displayed[0].exit_price,
        "expected_exit_price": 105,
    }

    state = PortfolioState.create(start, 900, 900)
    flat = replace(opp, exit_price=100)
    result, _ = SimulationLedger().simulate_window(
        [flat],
        {},
        start,
        end,
        state_in=state,
        price_series={"BTCUSDT": ([start.timestamp(), end.timestamp()], [100, 100])},
    )
    correct = evaluate_equity_curve(result.equity_points, initial_equity=900)
    evidence["stateful_drawdown"] = {
        "ledger_mdd": result.mdd_usdt,
        "window_mdd": correct.max_drawdown_usdt,
        "window_pnl": result.oos_pnl,
    }

    opps = [replace(opp, opportunity_id=f"b{i}", symbol=f"S{i}") for i in range(11)]
    stress_prices = {
        item.symbol: (
            [
                start.timestamp(),
                (start + timedelta(minutes=2)).timestamp(),
                (start + timedelta(minutes=10)).timestamp(),
            ],
            [100, 1, 110],
        )
        for item in opps
    }
    pool = {(2, 1): opps}
    g._init_verification_worker(stress_prices, pool, start, end)
    verified = g._verify_single_contender((0, {}, 280, False, None))[1]
    result, _ = SimulationLedger().simulate_window(
        opps,
        {},
        start,
        end,
        price_series=stress_prices,
        margin_cap=280,
    )
    cand = g.Candidate8D({}, 100, 1, 100, 0, 0, 0, 1100, 11, 220)
    selected = g.select_pnl_max(
        [cand],
        margin_cap=280,
        prices_by_symbol=stress_prices,
        events=opps,
        opps_by_wc=pool,
        w_start=start,
        w_end=end,
    )
    evidence["insolvent_selection"] = {
        "min_equity": min(point.equity for point in result.equity_points),
        "metrics_feasible": evaluate_equity_curve(result.equity_points).is_feasible,
        "verified_pnl": verified["net_pnl"] if verified else None,
        "selected": selected is not None,
    }

    cutoff = start + timedelta(minutes=5)
    first = mk(
        "first",
        "BTCUSDT",
        start,
        start + timedelta(minutes=1),
        100,
        start + timedelta(minutes=3),
        110,
    )
    second = mk(
        "second",
        "BTCUSDT",
        start + timedelta(minutes=3),
        start + timedelta(minutes=4),
        100,
        start + timedelta(minutes=10),
        110,
    )
    view_prices = {
        "BTCUSDT": (
            [
                start.timestamp(),
                cutoff.timestamp(),
                (start + timedelta(minutes=10)).timestamp(),
            ],
            [100, 100, 110],
        )
    }
    params = dict(
        impulse_window_buckets=2,
        confirmation_buckets=1,
        min_return_pct=0.5,
        min_imbalance=0.3,
        min_intensity=1.5,
        min_volume_ratio=0,
        cooldown_buckets=0,
    )
    manifest = SimpleNamespace(watermark_start=start, watermark_end=cutoff)
    result = g.compute_six_scenarios_view(
        [first, second],
        pd.DataFrame([params]),
        view_prices,
        manifest,
        max_workers=1,
    )
    evidence["dashboard_window"] = {
        "selected_pnl": result[0]["m280_pnl_max"].net_pnl,
        "displayed_pnl": result[2]["s_m280_pnl_max"]["net_pnl"],
        "curve_end": result[4]["s_m280_pnl_max"][-1].timestamp.isoformat(),
        "manifest_end": cutoff.isoformat(),
    }
    try:
        g.compute_six_scenarios_view(
            [replace(first, exit_time=None, exit_price=None)],
            pd.DataFrame([params]),
            view_prices,
            manifest,
            max_workers=1,
        )
    except Exception as exc:
        evidence["all_open_dashboard"] = {
            "error": type(exc).__name__,
            "message": str(exc),
        }
    else:
        evidence["all_open_dashboard"] = {"error": None}
    return evidence


def profile_verifier():
    import cProfile
    import pstats

    start = datetime(2026, 9, 1, tzinfo=UTC)
    end = start + timedelta(days=2)
    opps = []
    for index in range(500):
        entry = start + timedelta(minutes=5 * index)
        opps.append(
            mk(
                str(index),
                f"S{index % 10}",
                entry - timedelta(seconds=15),
                entry,
                100,
                entry + timedelta(minutes=30),
                101,
            )
        )
    epochs = [start.timestamp() + 15 * index for index in range(11521)]
    prices = {f"S{index}": (epochs, [100.0] * len(epochs)) for index in range(10)}
    g._init_verification_worker(prices, {(2, 1): opps}, start, end)
    profiler = cProfile.Profile()
    profiler.enable()
    for index in range(5):
        g._verify_single_contender((index, {}, None, False, None))
    profiler.disable()
    pstats.Stats(profiler).strip_dirs().sort_stats("cumulative").print_stats(18)


if __name__ == "__main__":
    if "--profile" in sys.argv:
        profile_verifier()
    else:
        with redirect_stdout(io.StringIO()):
            evidence = collect_evidence()
        print(json.dumps(evidence, indent=2, ensure_ascii=False))
