"""Tests for Astra Review Round 4 Fixes.

Covers:
1. Issue 1: Fail-closed governance when price streams are absent
   (prices_by_symbol=None or events=None), specifically preventing selection
   of unverified candidates (including the 11-symbol flash crash counterexample).
2. Issue 2: Unified evaluation entrance preserves candidate's max_open_positions
   (e.g. slots=4) when ScenarioSpec.slots is not explicitly overridden.
3. Issue 3: AlignedPriceGrid compatibility with scheduled_risk_window and
   simulation ledger, eliminating TypeError.
"""

from __future__ import annotations

from datetime import UTC, datetime, time, timedelta

import pytest

from crypto_momentum_lab.live_rollout.scheduled_risk_window import (
    ScheduledRiskWindowConfig,
)
from local_optimization.evaluation_context import (
    EvaluationContext,
    ScenarioSpec,
    evaluate_candidate,
)
from local_optimization.generate_six_scenarios_dashboard import (
    Candidate8D,
    select_balanced,
    select_compounding,
    select_pnl_max,
)
from local_optimization.mtm_engine import AlignedPriceGrid
from local_optimization.protocol import ParameterCandidate
from local_optimization.tests.test_opportunity_and_wfa_repair import (
    make_mock_opportunity as mk,
)


def test_issue1_missing_price_series_fails_closed_11_symbol_counterexample() -> None:
    """Issue 1: When prices_by_symbol is None, selectors must fail-closed,

    rather than returning an unverified candidate whose mid-trade drawdown
    is unmeasured.
    """
    start = datetime(2026, 9, 1, 0, 0, 0, tzinfo=UTC)
    end = start + timedelta(minutes=15)

    # 11 opportunities across 11 symbols, price drops from 100 to 1,
    # then recovers to 110
    opps = [
        mk(
            f"opp_{i}",
            f"SYM_{i}",
            start,
            start + timedelta(minutes=1),
            100.0,
            start + timedelta(minutes=10),
            110.0,
        )
        for i in range(11)
    ]
    stress_prices = {
        item.symbol: (
            [
                start.timestamp(),
                (start + timedelta(minutes=2)).timestamp(),
                (start + timedelta(minutes=10)).timestamp(),
            ],
            [100.0, 1.0, 110.0],
        )
        for item in opps
    }
    pool = {(2, 1): opps}

    # Unverified candidate claim: +108.38U PnL, apparent mdd=0.0
    # (based only on entry/exit)
    cand = Candidate8D(
        params={
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "max_open_positions": 2,
        },
        net_pnl=108.38,
        mdd=0.0,
        calmar=10.0,
        compounding_score=1.0,
        compounding_mdd=0.05,
        compounding_ui=0.01,
        terminal_compounded_equity=1108.38,
        n_trades=11,
        peak_margin=220.0,
        compounding_peak_margin=220.0,
        stability=0.9,
    )

    # 1. When prices_by_symbol=None, selectors MUST return None (fail closed)
    assert select_pnl_max([cand], margin_cap=280.0, prices_by_symbol=None) is None
    assert select_balanced([cand], margin_cap=280.0, prices_by_symbol=None) is None
    assert (
        select_compounding(
            [cand], max_mdd=0.15, margin_cap=280.0, prices_by_symbol=None
        )
        is None
    )

    # 2. When stress_prices ARE provided, MTM verification verifies that
    # account became insolvent (< 0) and selectors must also reject the candidate
    selected_pnl = select_pnl_max(
        [cand],
        margin_cap=280.0,
        prices_by_symbol=stress_prices,
        events=opps,
        opps_by_wc=pool,
        w_start=start,
        w_end=end,
    )
    assert selected_pnl is None, (
        "Insolvent candidate must be rejected by MTM verification"
    )


def test_issue2_scenario_spec_preserves_candidate_max_open_positions() -> None:
    """Issue 2: Unified evaluation entrance must not force slots=2 when
    ScenarioSpec() is used with default slots=None. A candidate with
    max_open_positions=4 must be allowed to open up to 4 concurrent positions.
    """
    start = datetime(2026, 9, 1, 0, 0, 0, tzinfo=UTC)
    end = start + timedelta(hours=2)

    # 4 concurrent opportunities for the SAME symbol at staggered entry
    # times before exit
    opps = [
        mk(
            f"opp_{i}",
            "BTCUSDT",
            start + timedelta(minutes=i * 2),
            start + timedelta(minutes=i * 2 + 1),
            100.0,
            start + timedelta(minutes=50),
            105.0,
        )
        for i in range(4)
    ]
    prices = {
        "BTCUSDT": (
            [start.timestamp(), end.timestamp()],
            [100.0, 105.0],
        )
    }
    pool = {(2, 1): opps}

    context = EvaluationContext(
        window_start=start,
        window_end=end,
        price_series=prices,
        opps_by_wc=pool,
        events=opps,
    )

    # Candidate with max_open_positions = 4
    cand_4slots = ParameterCandidate(
        parameter_id="cand_4",
        params={
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "max_open_positions": 4,
            "min_return_pct": 0.5,
            "min_imbalance": 0.3,
            "min_intensity": 1.5,
            "min_volume_ratio": 0.0,
            "cooldown_buckets": 0,
        },
    )

    # Default ScenarioSpec (slots=None): should respect candidate's slots=4!
    default_scenario = ScenarioSpec()
    assert default_scenario.slots is None

    res_4 = evaluate_candidate(context, cand_4slots, scenario=default_scenario)
    assert len(res_4.admitted_trades) == 4, (
        f"Expected all 4 trades admitted with max_open_positions=4, "
        f"got {len(res_4.admitted_trades)}"
    )

    # When scenario explicitly specifies slots=2, it overrides candidate
    explicit_2slots = ScenarioSpec(slots=2)
    res_2 = evaluate_candidate(context, cand_4slots, scenario=explicit_2slots)
    assert len(res_2.admitted_trades) == 2, (
        f"Expected 2 trades admitted when scenario specifies slots=2, "
        f"got {len(res_2.admitted_trades)}"
    )


def test_issue3_aligned_price_grid_with_scheduled_risk_window() -> None:
    """Issue 3: Passing AlignedPriceGrid to EvaluationContext with scheduled_risk_window

    must not raise TypeError: argument of type 'AlignedPriceGrid' is not iterable.
    """
    start = datetime(2026, 9, 1, 0, 0, 0, tzinfo=UTC)
    end = start + timedelta(hours=1)

    opp = mk(
        "opp_sched",
        "BTCUSDT",
        start,
        start + timedelta(minutes=1),
        100.0,
        start + timedelta(minutes=45),
        110.0,
    )
    ts_list = [
        start.timestamp(),
        (start + timedelta(minutes=5)).timestamp(),
        (start + timedelta(minutes=10)).timestamp(),
        end.timestamp(),
    ]
    p_list = [100.0, 105.0, 108.0, 110.0]
    raw_prices = {"BTCUSDT": (ts_list, p_list)}

    # Build AlignedPriceGrid
    grid = AlignedPriceGrid.build(raw_prices, start, end, grid_seconds=15)

    # Verify dict-like methods on AlignedPriceGrid
    assert "BTCUSDT" in grid
    assert "ETHUSDT" not in grid
    assert len(grid) == 1
    assert list(grid.keys()) == ["BTCUSDT"]
    item_epochs, item_prices = grid["BTCUSDT"]
    assert len(item_epochs) == len(item_prices)
    assert grid.get("ETHUSDT", None) is None
    assert grid.get("BTCUSDT") is not None

    # Scheduled risk window flattening at 00:05:00
    risk = ScheduledRiskWindowConfig(
        timezone="UTC",
        entry_stop_at=time(0, 5),
        flatten_start_at=time(0, 5),
        flatten_deadline_at=time(0, 5, 15),
        verify_at=time(0, 5, 30),
        reopen_at=time(0, 6),
    )

    # Context using AlignedPriceGrid and scheduled_risk_window
    context = EvaluationContext(
        window_start=start,
        window_end=end,
        price_series=grid,
        opps_by_wc={(2, 1): [opp]},
        events=[opp],
        scheduled_risk_window=risk,
    )

    cand = ParameterCandidate(
        parameter_id="c1",
        params={
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "max_open_positions": 2,
        },
    )

    # This evaluate_candidate call MUST NOT raise TypeError!
    res = evaluate_candidate(context, cand, scenario=ScenarioSpec(), include_curve=True)

    assert len(res.admitted_trades) == 1
    trade = res.admitted_trades[0]
    # Trade exit should be flattened to 00:05:00
    assert trade.exit_time == start + timedelta(minutes=5)
    # Flattened price should match price at 00:05:00 (105.0)
    assert trade.exit_price == pytest.approx(105.0, abs=1e-2)
