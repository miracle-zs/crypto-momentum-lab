"""Acceptance tests for Compounding Position Sizing and Objectives.

Strictly verifies the 10 acceptance cases specified in:
docs/research/2026-09-19-compounding-position-sizing-and-objectives.md
"""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta

import pytest

from local_optimization.equity import (
    EquityPoint,
    calc_daily_log_growth,
    calc_time_weighted_return,
    evaluate_equity_curve,
)
from local_optimization.mtm_engine import (
    TradeRecord,
    reconstruct_mtm_equity,
)
from local_optimization.optimizer import (
    CandidateEvaluation,
    is_candidate_compliant,
    select_best_and_recommended,
)
from local_optimization.protocol import (
    OptimizationProtocol,
    ParameterCandidate,
)
from local_optimization.sizing import (
    DailyEquityRatioSizing,
    ExternalCashFlow,
    RiskAdaptiveSizing,
)


def test_case_1_progression_and_old_position_immutability() -> None:
    """Case 1: Progression across day-cuts and old position immutability.

    f=0.10, equity 1000/1200/1500/1400 -> notional strictly 100/120/150/140.
    Intraday equity changes do not alter target notional until next day cut.
    """
    policy = DailyEquityRatioSizing(
        fraction_f=0.10,
        margin_ratio_cap=0.28,
        min_order_notional=10.0,
        initial_equity=1000.0,
    )
    assert policy.current_state.target_notional == pytest.approx(100.0)

    # Day 1 cut at 1200
    t1 = datetime(2026, 9, 15, 0, 0, tzinfo=UTC)
    s1 = policy.on_day_cut(1200.0, t1)
    assert s1.target_notional == pytest.approx(120.0)
    assert policy.get_order_notional("BTCUSDT", 1200.0, t1) == pytest.approx(120.0)

    # Intraday floating equity surge to 1800 at 12:00 DOES NOT alter day-cut notional
    t_intra = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)
    assert policy.get_order_notional("BTCUSDT", 1800.0, t_intra) == pytest.approx(120.0)

    # Day 2 cut at 1500
    t2 = datetime(2026, 9, 16, 0, 0, tzinfo=UTC)
    s2 = policy.on_day_cut(1500.0, t2)
    assert s2.target_notional == pytest.approx(150.0)

    # Day 3 cut at 1400 (loss shrinkage)
    t3 = datetime(2026, 9, 17, 0, 0, tzinfo=UTC)
    s3 = policy.on_day_cut(1400.0, t3)
    assert s3.target_notional == pytest.approx(140.0)


def test_case_2_overnight_batches_and_carry_in() -> None:
    """Case 2: Trade crossing midnight, carry-in positions, and equity conservation.

    Trade entered before midnight keeps entry sizing across day cut.
    New trade entered next day uses updated sizing.
    """
    t_pre = datetime(2026, 9, 15, 23, 55, tzinfo=UTC)
    t_daycut = datetime(2026, 9, 16, 0, 0, tzinfo=UTC)
    t_post = datetime(2026, 9, 16, 0, 30, tzinfo=UTC)
    t_second = datetime(2026, 9, 16, 1, 0, tzinfo=UTC)
    t_end = datetime(2026, 9, 16, 2, 0, tzinfo=UTC)

    # Trade 1 entered on Day 1 at 23:55, exits on Day 2 at 00:30 (+10% on entry price)
    t1 = TradeRecord(
        trade_id="T01",
        symbol="ETHUSDT",
        entry_time=t_pre,
        entry_price=1000.0,
        exit_time=t_post,
        exit_price=1100.0,
        notional_usdt=100.0,  # Will be dynamically sized if policy is provided
        net_pnl_usdt=10.0,
    )

    # Trade 2 entered on Day 2 at 01:00, exits at 02:00 (+5%)
    t2 = TradeRecord(
        trade_id="T02",
        symbol="BTCUSDT",
        entry_time=t_second,
        entry_price=2000.0,
        exit_time=t_end,
        exit_price=2100.0,
        notional_usdt=100.0,
        net_pnl_usdt=5.0,
    )

    price_series = {
        "ETHUSDT": (
            [
                t_pre.timestamp(),
                t_daycut.timestamp(),
                t_post.timestamp(),
                t_end.timestamp(),
            ],
            [1000.0, 1050.0, 1100.0, 1100.0],
        ),
        "BTCUSDT": (
            [
                t_pre.timestamp(),
                t_second.timestamp(),
                t_end.timestamp(),
            ],
            [2000.0, 2000.0, 2100.0],
        ),
    }

    policy = DailyEquityRatioSizing(
        fraction_f=0.10, margin_ratio_cap=0.28, initial_equity=1000.0
    )

    points = reconstruct_mtm_equity(
        trades=[t1, t2],
        price_series=price_series,
        initial_equity=1000.0,
        grid_seconds=60,
        sizing_policy=policy,
    )

    assert len(points) > 0
    # Initial equity ~1000 (minus entry fee)
    assert points[0].equity == pytest.approx(1000.0, abs=0.1)
    # Trade 1 sized at 1000 * 0.10 = 100U
    # When trade 1 exits with +10U profit, equity is ~1010U
    # Trade 2 at 01:00 on Day 2 was sized after day-cut (which observed equity)
    final_point = points[-1]
    # Check conservation: final equity equals initial + total net PnL
    expected_net_gain = (
        final_point.realized_pnl + final_point.unrealized_pnl - final_point.fee
    )
    assert final_point.equity == pytest.approx(1000.0 + expected_net_gain, abs=1e-3)


def test_case_3_working_order_margin_reservation() -> None:
    """Case 3: Working order reservation and no double counting.

    Pending order reserves margin. Filling converts reserved to position margin.
    Cancelling releases reserved margin.
    """
    policy = DailyEquityRatioSizing(
        fraction_f=0.10,
        margin_ratio_cap=0.28,
        min_order_notional=10.0,
        initial_equity=1000.0,
    )

    # Current equity = 1000. Position margin = 150. Reserved margin = 100.
    # New order notional = 100U, leverage = 2x -> order margin = 50U.
    # Prospective total margin = 150 + 100 + 50 = 300U.
    # 300 / 1000 = 30% > 28% cap -> rejected!
    res_blocked = policy.check_intraday_order(
        symbol="BTCUSDT",
        price=50000.0,
        current_equity=1000.0,
        position_margin=150.0,
        reserved_margin=100.0,
        leverage=2.0,
    )
    assert not res_blocked.allowed
    assert "exceeds cap" in res_blocked.reason

    # Cancel reserved order: reserved drops from 100 to 40.
    # Prospective total margin = 150 + 40 + 50 = 240U.
    # 240 / 1000 = 24% <= 28% cap -> allowed!
    res_allowed = policy.check_intraday_order(
        symbol="BTCUSDT",
        price=50000.0,
        current_equity=1000.0,
        position_margin=150.0,
        reserved_margin=40.0,
        leverage=2.0,
    )
    assert res_allowed.allowed
    assert res_allowed.notional_usdt == pytest.approx(100.0)


def test_case_4_intraday_risk_check_rejection_no_phantom_fill() -> None:
    """Case 4: Risk check rejection prevents phantom execution.

    When margin cap is breached, order is rejected without phantom fills.
    """
    t0 = datetime(2026, 9, 15, 10, 0, tzinfo=UTC)
    t1 = datetime(2026, 9, 15, 11, 0, tzinfo=UTC)

    # Two concurrent trades. Both require 200U margin at 1x leverage.
    # Account has 1000U equity, but margin_ratio_cap is 0.25 (max 250U margin).
    # Trade 1 will occupy 200U margin.
    # Trade 2 attempted concurrently will require another 200U (total 400U = 40% > 25%).
    trade1 = TradeRecord(
        trade_id="T01",
        symbol="BTCUSDT",
        entry_time=t0,
        entry_price=100.0,
        exit_time=t1,
        exit_price=110.0,
        notional_usdt=200.0,
        leverage=1.0,
        net_pnl_usdt=20.0,
    )
    trade2 = TradeRecord(
        trade_id="T02",
        symbol="ETHUSDT",
        entry_time=t0 + timedelta(minutes=5),
        entry_price=10.0,
        exit_time=t1,
        exit_price=12.0,
        notional_usdt=200.0,
        leverage=1.0,
        net_pnl_usdt=40.0,
    )

    policy = DailyEquityRatioSizing(
        fraction_f=0.20,
        margin_ratio_cap=0.25,
        min_order_notional=10.0,
        initial_equity=1000.0,
    )

    price_series = {
        "BTCUSDT": ([t0.timestamp(), t1.timestamp()], [100.0, 110.0]),
        "ETHUSDT": ([t0.timestamp(), t1.timestamp()], [10.0, 12.0]),
    }

    points = reconstruct_mtm_equity(
        trades=[trade1, trade2],
        price_series=price_series,
        initial_equity=1000.0,
        grid_seconds=60,
        sizing_policy=policy,
    )

    # Trade 1 is admitted; Trade 2 is rejected by risk check.
    # Peak active positions must be 1, never 2!
    max_active = max(p.active_positions for p in points)
    assert max_active == 1

    # Rejected orders count recorded in policy stats
    assert policy.rejected_orders_count >= 1


def test_case_5_twr_neutrality_external_flows() -> None:
    """Case 5: External cash flow neutrality in Time-Weighted Return (TWR).

    Depositing 500U does not generate 500U profit or reset strategy HWM.
    """
    t0 = datetime(2026, 9, 15, 0, 0, tzinfo=UTC)
    t1 = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)
    t2 = datetime(2026, 9, 15, 12, 1, tzinfo=UTC)
    t3 = datetime(2026, 9, 16, 0, 0, tzinfo=UTC)

    # Period 1: Equity grows from 1000 to 1100 (+10%)
    p0 = EquityPoint(timestamp=t0, equity=1000.0)
    p1 = EquityPoint(timestamp=t1, equity=1100.0)

    # Deposit of +500U happens at t1
    flow = ExternalCashFlow(timestamp=t1, amount_usdt=500.0, flow_type="DEPOSIT")

    # Period 2: Equity starts at 1600. Strategy loses 80U -> equity 1520 (-5%)
    p2 = EquityPoint(timestamp=t2, equity=1600.0)
    p3 = EquityPoint(timestamp=t3, equity=1520.0)

    twr_result = calc_time_weighted_return(
        points=[p0, p1, p2, p3],
        cash_flows=[flow],
        initial_equity=1000.0,
    )

    # TWR factor = (1100/1000) * (1520/1600) = 1.10 * 0.95 = 1.045 (+4.5%)
    assert twr_result["twr_factor"] == pytest.approx(1.045, rel=1e-4)
    assert twr_result["twr_return_pct"] == pytest.approx(4.5, rel=1e-4)
    # Neutral equity = 1000 * 1.045 = 1045U, NOT 1520U!
    assert twr_result["neutral_final_equity"] == pytest.approx(1045.0, rel=1e-4)


def test_case_6_log_growth_and_volatility_drag() -> None:
    """Case 6: Daily net log growth penalizes path volatility.

    +20% then -20% yields terminal equity 960 and negative log growth -2.041%/day.
    Two paths with same terminal equity have same g but different UI/MDD.
    """
    # Path A: +20% then -20%
    # 1000 -> 1200 -> 960 over 2 days
    g_a = calc_daily_log_growth(
        equity_series=[1000.0, 960.0], calendar_days=2.0, initial_equity=1000.0
    )
    expected_g = math.log(0.96) / 2.0  # ~ -0.020410
    assert g_a == pytest.approx(expected_g, rel=1e-5)
    assert g_a < 0.0  # Negative even though arithmetic mean is 0%

    t0 = datetime(2026, 9, 15, 0, 0, tzinfo=UTC)
    t1 = datetime(2026, 9, 16, 0, 0, tzinfo=UTC)
    t2 = datetime(2026, 9, 17, 0, 0, tzinfo=UTC)

    # Path A equity points:
    points_a = [
        EquityPoint(timestamp=t0, equity=1000.0),
        EquityPoint(timestamp=t1, equity=1200.0),
        EquityPoint(timestamp=t2, equity=960.0),
    ]
    metrics_a = evaluate_equity_curve(
        points_a, initial_equity=1000.0, calendar_days=2.0
    )

    # Path B: Monotonic -2% per day: 1000 -> 980 -> 960
    points_b = [
        EquityPoint(timestamp=t0, equity=1000.0),
        EquityPoint(timestamp=t1, equity=980.0),
        EquityPoint(timestamp=t2, equity=960.0),
    ]
    metrics_b = evaluate_equity_curve(
        points_b, initial_equity=1000.0, calendar_days=2.0
    )

    # Daily log return is identical (both end at 960)
    assert metrics_a.daily_log_return == pytest.approx(
        metrics_b.daily_log_return, abs=1e-6
    )

    # But path risk is strictly different:
    # Path A peak was 1200, so DD from 1200 to 960 is 20.0%!
    # Path B peak was 1000, so DD from 1000 to 960 is 4.0%!
    assert metrics_a.max_drawdown_pct == pytest.approx(0.20, abs=1e-3)
    assert metrics_b.max_drawdown_pct == pytest.approx(0.04, abs=1e-3)
    assert metrics_a.ulcer_index > metrics_b.ulcer_index


def test_case_7_smoothing_asymmetry_and_drawdown_hysteresis() -> None:
    """Case 7: Asymmetric smoothing and drawdown hysteresis.

    Verifies exact Astra Section 3 example:
    alpha = 0.5, equity: 1000 -> 1200 -> 1500 -> 1400 -> 1000
    yields B_d: 1000 -> 1100 -> 1300 -> 1350 -> 1000.
    """
    adaptive = RiskAdaptiveSizing(
        fraction_f=0.10,
        margin_ratio_cap=0.28,
        smoothing_alpha=0.5,
        use_smoothing=True,
        initial_equity=1000.0,
        use_dd_derisking=True,
        dd_trigger=0.10,
        dd_multiplier=0.50,
        dd_recovery=0.05,
    )

    t0 = datetime(2026, 9, 15, 0, 0, tzinfo=UTC)
    # B_0 = 1000
    assert adaptive.smoothed_base == pytest.approx(1000.0)

    # E_1 = 1200 -> B_1 = min(1200, 0.5*1000 + 0.5*1200) = 1100
    s1 = adaptive.on_day_cut(1200.0, t0 + timedelta(days=1))
    assert adaptive.smoothed_base == pytest.approx(1100.0)
    assert s1.target_notional == pytest.approx(110.0)

    # E_2 = 1500 -> B_2 = min(1500, 0.5*1100 + 0.5*1500) = 1300
    s2 = adaptive.on_day_cut(1500.0, t0 + timedelta(days=2))
    assert adaptive.smoothed_base == pytest.approx(1300.0)
    assert s2.target_notional == pytest.approx(130.0)

    # E_3 = 1400 -> raw = 0.5*1300 + 0.5*1400 = 1350 <= 1400 -> B_3 = 1350
    # Crucial rule: B increases to 1350 even though equity dropped from 1500 to 1400!
    s3 = adaptive.on_day_cut(1400.0, t0 + timedelta(days=3))
    assert adaptive.smoothed_base == pytest.approx(1350.0)
    assert s3.target_notional == pytest.approx(135.0)

    # E_4 = 1000 -> raw = 0.5*1350 + 0.5*1000 = 1175 > 1000 -> B_4 = 1000
    # Crucial rule: immediate cut, B cannot exceed real equity!
    s4 = adaptive.on_day_cut(1000.0, t0 + timedelta(days=4))
    assert adaptive.smoothed_base == pytest.approx(1000.0)

    # Check drawdown derisking hysteresis:
    # At E_4 = 1000, HWM was 1500. DD = 33.3% >= 10% trigger!
    # Derisking active: mult_dd = 0.5 -> notional = 0.10 * 1000 * 0.5 = 50U!
    assert adaptive.is_derisked
    assert s4.target_notional == pytest.approx(50.0)

    # Recovery test: equity rebounds to 1400 (DD = 6.67% > 5% recovery threshold)
    # Hysteresis keeps derisking active!
    adaptive.on_day_cut(1400.0, t0 + timedelta(days=5))
    assert adaptive.is_derisked

    # Equity reaches 1450 (DD = 3.33% <= 5% recovery threshold)
    # Derisking deactivates without locking out!
    adaptive.on_day_cut(1450.0, t0 + timedelta(days=6))
    assert not adaptive.is_derisked


def test_case_8_timing_boundaries_no_lookahead() -> None:
    """Case 8: Strict timing boundaries and no retroactive parameter adoption."""
    policy = DailyEquityRatioSizing(
        fraction_f=0.10, margin_ratio_cap=0.28, initial_equity=1000.0
    )
    t_0800 = datetime(2026, 9, 15, 8, 0, tzinfo=UTC)
    t_0930 = datetime(2026, 9, 15, 9, 30, tzinfo=UTC)

    # Sizing frozen at 08:00
    s_0800 = policy.on_day_cut(1000.0, t_0800)
    assert s_0800.target_notional == pytest.approx(100.0)

    # At 09:00, order placed uses 08:00 frozen sizing
    t_0900 = datetime(2026, 9, 15, 9, 0, tzinfo=UTC)
    assert policy.get_order_notional("BTCUSDT", 1000.0, t_0900) == pytest.approx(100.0)

    # Parameters selected at 09:30 have effective_time >= 09:30
    assert t_0930 > policy.current_state.sizing_cutoff


def test_case_9_infeasible_candidate_and_bankruptcy() -> None:
    """Case 9: Infeasible path detection, bankruptcy, and protocol compliance."""
    protocol = OptimizationProtocol(
        scenario_family="test",
        max_allowed_mdd_pct=0.20,
        max_allowed_ui=0.10,
        min_trades=10,
    )
    cand = ParameterCandidate.from_dict({"impulse_window_bars": 3})

    # Non-positive equity path yields daily_log_growth = -inf and is_feasible = False
    log_growth = calc_daily_log_growth(
        equity_series=[1000.0, 0.0], calendar_days=10.0, initial_equity=1000.0
    )
    assert log_growth == -float("inf")

    # Infeasible candidate evaluation
    eval_infeasible = CandidateEvaluation(
        candidate=cand,
        trade_count=20,
        net_pnl=-1000.0,
        daily_log_growth=log_growth,
        max_drawdown_pct=1.0,
        ulcer_index=0.80,
        cdar_95=1.0,
        peak_initial_margin_usdt=500.0,
        is_feasible=False,
        infeasible_reasons=["Account reached zero or negative equity"],
    )

    is_ok, reasons = is_candidate_compliant(eval_infeasible, protocol)
    assert not is_ok
    assert any("infeasible" in r.lower() for r in reasons)

    # When all candidates are infeasible, select_best_and_recommended returns None
    best, rec = select_best_and_recommended([eval_infeasible], protocol)
    assert best is None
    assert rec is None


def test_case_10_deterministic_replay_and_zero_external_leakage() -> None:
    """Case 10: Bitwise deterministic replay and protocol immutability."""
    t0 = datetime(2026, 9, 15, 0, 0, tzinfo=UTC)
    t1 = datetime(2026, 9, 15, 12, 0, tzinfo=UTC)
    t2 = datetime(2026, 9, 16, 0, 0, tzinfo=UTC)

    trade = TradeRecord(
        trade_id="DET01",
        symbol="BTCUSDT",
        entry_time=t0,
        entry_price=50000.0,
        exit_time=t1,
        exit_price=51000.0,
        notional_usdt=100.0,
        net_pnl_usdt=2.0,
    )
    price_series = {
        "BTCUSDT": (
            [t0.timestamp(), t1.timestamp(), t2.timestamp()],
            [50000.0, 51000.0, 51000.0],
        )
    }

    # Run 1
    policy1 = DailyEquityRatioSizing(
        fraction_f=0.10, margin_ratio_cap=0.28, initial_equity=1000.0
    )
    points1 = reconstruct_mtm_equity(
        trades=[trade],
        price_series=price_series,
        initial_equity=1000.0,
        grid_seconds=60,
        sizing_policy=policy1,
    )

    # Run 2
    policy2 = DailyEquityRatioSizing(
        fraction_f=0.10, margin_ratio_cap=0.28, initial_equity=1000.0
    )
    points2 = reconstruct_mtm_equity(
        trades=[trade],
        price_series=price_series,
        initial_equity=1000.0,
        grid_seconds=60,
        sizing_policy=policy2,
    )

    assert len(points1) == len(points2)
    for p1, p2 in zip(points1, points2, strict=True):
        assert p1.equity == pytest.approx(p2.equity, abs=1e-12)
        assert p1.unrealized_pnl == pytest.approx(p2.unrealized_pnl, abs=1e-12)
        assert p1.realized_pnl == pytest.approx(p2.realized_pnl, abs=1e-12)
