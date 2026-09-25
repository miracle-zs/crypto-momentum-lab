"""Tests for Direction 2: Unified EvaluationContext and evaluate_candidate pipeline."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from local_optimization.evaluation_context import (
    EvaluationContext,
    ScenarioSpec,
    evaluate_candidate,
)
from local_optimization.tests.test_opportunity_and_wfa_repair import (
    make_mock_opportunity as mk,
)


def test_evaluate_candidate_fast_vs_full_curve_consistency() -> None:
    """Verify include_curve=False (fast) matches include_curve=True (full) exactly."""
    start = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)
    end = start + timedelta(hours=2)

    opp1 = mk(
        "opp1",
        "BTCUSDT",
        start,
        start + timedelta(minutes=5),
        100.0,
        start + timedelta(minutes=30),
        105.0,
    )
    opp2 = mk(
        "opp2",
        "ETHUSDT",
        start + timedelta(minutes=10),
        start + timedelta(minutes=15),
        200.0,
        start + timedelta(minutes=50),
        190.0,
    )
    prices = {
        "BTCUSDT": (
            [
                start.timestamp(),
                (start + timedelta(minutes=30)).timestamp(),
                end.timestamp(),
            ],
            [100.0, 105.0, 106.0],
        ),
        "ETHUSDT": (
            [
                start.timestamp(),
                (start + timedelta(minutes=50)).timestamp(),
                end.timestamp(),
            ],
            [200.0, 190.0, 195.0],
        ),
    }

    ctx = EvaluationContext(
        window_start=start,
        window_end=end,
        price_series=prices,
        events=[opp1, opp2],
    )

    params = {
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "max_open_positions": 2,
    }
    scenario = ScenarioSpec(name="test_scenario", margin_cap=280.0, compounding=False)

    res_fast = evaluate_candidate(ctx, params, scenario, include_curve=False)
    res_full = evaluate_candidate(ctx, params, scenario, include_curve=True)

    assert res_fast.is_feasible is True
    assert res_full.is_feasible is True
    assert res_fast.curve is None
    assert res_full.curve is not None
    assert len(res_full.curve) > 0

    assert res_fast.net_pnl == pytest.approx(res_full.net_pnl, abs=1e-4)
    assert res_fast.mdd_usdt == pytest.approx(res_full.mdd_usdt, abs=1e-4)
    assert res_fast.mdd_pct == pytest.approx(res_full.mdd_pct, abs=1e-4)
    assert res_fast.peak_margin == pytest.approx(res_full.peak_margin, abs=1e-2)
    assert res_fast.calmar == pytest.approx(res_full.calmar, abs=1e-3)
    assert res_fast.cdar_95 == pytest.approx(res_full.cdar_95, abs=1e-4)
    assert res_fast.ulcer_index == pytest.approx(res_full.ulcer_index, abs=1e-5)


def test_evaluate_candidate_stateful_ledger_continuation() -> None:
    """Verify evaluation carries forward portfolio state across sequential windows."""
    t0 = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)
    t1 = t0 + timedelta(hours=1)
    t2 = t1 + timedelta(hours=1)

    opp1 = mk(
        "opp1",
        "BTCUSDT",
        t0,
        t0 + timedelta(minutes=5),
        100.0,
        t0 + timedelta(minutes=40),
        105.0,
    )
    prices1 = {
        "BTCUSDT": (
            [t0.timestamp(), (t0 + timedelta(minutes=40)).timestamp(), t1.timestamp()],
            [100.0, 105.0, 105.0],
        )
    }
    ctx1 = EvaluationContext(
        window_start=t0, window_end=t1, price_series=prices1, events=[opp1]
    )
    res1 = evaluate_candidate(ctx1, {}, ScenarioSpec(), include_curve=True)
    assert res1.is_feasible is True
    assert res1.state_out is not None

    opp2 = mk(
        "opp2",
        "BTCUSDT",
        t1,
        t1 + timedelta(minutes=5),
        105.0,
        t1 + timedelta(minutes=40),
        110.0,
    )
    prices2 = {
        "BTCUSDT": (
            [t1.timestamp(), (t1 + timedelta(minutes=40)).timestamp(), t2.timestamp()],
            [105.0, 110.0, 110.0],
        )
    }
    ctx2 = EvaluationContext(
        window_start=t1,
        window_end=t2,
        price_series=prices2,
        events=[opp2],
        state_in=res1.state_out,
        initial_equity=res1.state_out.total_equity_mtm,
    )
    res2 = evaluate_candidate(ctx2, {}, ScenarioSpec(), include_curve=True)
    assert res2.is_feasible is True
    assert res2.net_pnl > 0


def test_evaluate_candidate_infeasible_rejection_rules() -> None:
    """Verify evaluate_candidate rejects margin breaches and empty events gracefully."""
    start = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)
    end = start + timedelta(hours=1)

    # 1. Empty events
    ctx_empty = EvaluationContext(
        window_start=start, window_end=end, price_series={}, events=[]
    )
    res_empty = evaluate_candidate(ctx_empty, {}, ScenarioSpec())
    assert res_empty.is_feasible is False
    assert res_empty.to_verification_dict() is None

    # 2. Margin cap exceeded
    opp = mk(
        "opp",
        "BTCUSDT",
        start,
        start + timedelta(minutes=5),
        100.0,
        start + timedelta(minutes=30),
        105.0,
    )
    prices = {"BTCUSDT": ([start.timestamp(), end.timestamp()], [100.0, 105.0])}
    ctx = EvaluationContext(
        window_start=start, window_end=end, price_series=prices, events=[opp]
    )

    # Required margin = 100 / 5 = 20 USDT. Cap = 10 USDT -> Infeasible!
    res_breach = evaluate_candidate(ctx, {}, ScenarioSpec(margin_cap=10.0))
    assert res_breach.is_feasible is False
    assert res_breach.to_verification_dict() is None
