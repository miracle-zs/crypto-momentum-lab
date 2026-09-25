"""Regression tests for Astra Review (2026-09-24) Findings.

Covers:
1. P1: to_trade_records preserves notional, leverage, direction semantics.
2. P1: evaluate_candidate advances state and evaluates carry-in positions.
3. P1: AlignedPriceGrid window slicing and array length consistency.
4. P2: ScenarioSpec.max_mdd strictly enforced in evaluate_candidate.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta

import pytest

from local_optimization.evaluation_context import (
    EvaluationContext,
    ScenarioSpec,
    evaluate_candidate,
    to_trade_records,
)
from local_optimization.mtm_engine import (
    AlignedPriceGrid,
    TradeRecord,
    reconstruct_mtm_equity,
    reconstruct_mtm_metrics_fast,
)
from local_optimization.simulation_ledger import PortfolioState, SimulationLedger


# -----------------------------------------------------------------------------
# 1. P1: to_trade_records Preserves Notional, Leverage, and Direction
# -----------------------------------------------------------------------------
def test_to_trade_records_preserves_notional_leverage_direction() -> None:
    """Verifies that to_trade_records retains TradeRecord's original notional,

    leverage, direction, and trade_id instead of hardcoding 100U / 5x / LONG.
    """
    t_entry = datetime(2026, 9, 1, 10, 0, 0, tzinfo=UTC)
    t_exit = datetime(2026, 9, 1, 10, 30, 0, tzinfo=UTC)

    orig_trade = TradeRecord(
        trade_id="CUSTOM_TR_999",
        symbol="ETHUSDT",
        entry_time=t_entry,
        entry_price=2000.0,
        exit_time=t_exit,
        exit_price=1900.0,  # 5% drop
        notional_usdt=250.0,
        leverage=2.0,
        fee_rate=0.0006,
        slippage_rate=0.0003,
        funding_cost_usdt=0.15,
        direction="SHORT",
        net_pnl_usdt=11.85,
        is_open=False,
    )

    converted = to_trade_records([orig_trade], compounding_scale=False)
    assert len(converted) == 1
    tr = converted[0]

    assert tr.trade_id == "CUSTOM_TR_999"
    assert tr.symbol == "ETHUSDT"
    assert tr.notional_usdt == 250.0
    assert tr.leverage == 2.0
    assert tr.direction == "SHORT"
    assert tr.fee_rate == 0.0006
    assert tr.slippage_rate == 0.0003
    assert tr.funding_cost_usdt == 0.15
    assert tr.calculated_net_pnl == pytest.approx(11.85, abs=0.01)


def test_to_trade_records_from_dict_preserves_semantics() -> None:
    """Verifies that dictionary inputs with custom notional and direction are

    preserved.
    """
    t_entry = datetime(2026, 9, 1, 10, 0, 0, tzinfo=UTC)
    t_exit = datetime(2026, 9, 1, 10, 30, 0, tzinfo=UTC)

    d_trade = {
        "trade_id": "DICT_TR_001",
        "symbol": "SOLUSDT",
        "entry_at": t_entry,
        "entry_price": 100.0,
        "exit_at": t_exit,
        "exit_price": 110.0,
        "notional_usdt": 300.0,
        "leverage": 3.0,
        "direction": "SHORT",
        "net_pnl_usdt": -32.0,
    }

    converted = to_trade_records([d_trade], compounding_scale=False)
    assert len(converted) == 1
    tr = converted[0]

    assert tr.trade_id == "DICT_TR_001"
    assert tr.notional_usdt == 300.0
    assert tr.leverage == 3.0
    assert tr.direction == "SHORT"


# -----------------------------------------------------------------------------
# 2. P1: evaluate_candidate Handles Carry-In Positions Without New Opps
# -----------------------------------------------------------------------------
def test_evaluate_candidate_advances_and_evaluates_carry_in_without_new_opps() -> None:
    """Verifies that when a window has NO new opportunities, but has an active

    carry-in position that closes in the window, evaluate_candidate advances the
    state and calculates the realized PnL correctly.
    """
    w_start = datetime(2026, 9, 1, 10, 0, 0, tzinfo=UTC)
    w_end = datetime(2026, 9, 1, 11, 0, 0, tzinfo=UTC)

    # Position opened at 09:30, closes inside window at 10:30 with +10U profit
    t_entry = datetime(2026, 9, 1, 9, 30, 0, tzinfo=UTC)
    t_exit = datetime(2026, 9, 1, 10, 30, 0, tzinfo=UTC)

    carry_in = TradeRecord(
        trade_id="CARRY_01",
        symbol="BTCUSDT",
        entry_time=t_entry,
        entry_price=100.0,
        exit_time=t_exit,
        exit_price=110.0,
        notional_usdt=100.0,
        leverage=5.0,
        fee_rate=0.0005,
        slippage_rate=0.0002,
        direction="LONG",
        net_pnl_usdt=9.72,
        is_open=True,
    )

    state_in = PortfolioState.create(
        timestamp=w_start,
        cash_usdt=1000.0,
        total_equity_mtm=1000.0,
        active_positions=(carry_in,),
    )

    price_series = {
        "BTCUSDT": (
            [
                w_start.timestamp(),
                t_exit.timestamp(),
                w_end.timestamp(),
            ],
            [100.0, 110.0, 110.0],
        )
    }

    ledger = SimulationLedger(initial_cash=1000.0, notional_usdt=100.0, leverage=5.0)
    context = EvaluationContext(
        window_start=w_start,
        window_end=w_end,
        price_series=price_series,
        events=[],  # No new opportunities!
        state_in=state_in,
        ledger=ledger,
        initial_equity=1000.0,
    )

    res = evaluate_candidate(
        context=context,
        candidate={
            "impulse_window_buckets": 2,
            "confirmation_buckets": 1,
            "max_open_positions": 2,
        },
        scenario=ScenarioSpec(),
        include_curve=True,
    )

    # State out timestamp must advance to window_end
    assert res.state_out is not None
    assert res.state_out.timestamp == w_end
    # Net PnL must reflect the closed carry-in trade (~9.72U)
    assert res.net_pnl == pytest.approx(9.72, abs=0.5)
    assert res.is_feasible is True
    assert len(res.admitted_trades) == 1
    assert res.admitted_trades[0].trade_id == "CARRY_01"


# -----------------------------------------------------------------------------
# 3. P1: AlignedPriceGrid Slicing and Window Dimension Alignment
# -----------------------------------------------------------------------------
def test_aligned_price_grid_slicing_and_dimension_consistency() -> None:
    """Verifies that when AlignedPriceGrid is resampled to a narrower sub-window,

    sampling_epochs and aligned_prices have identical lengths.
    """
    t0 = datetime(2026, 9, 1, 0, 0, 0, tzinfo=UTC)
    t_full_end = datetime(
        2026, 9, 1, 0, 10, 0, tzinfo=UTC
    )  # 10 minutes = 41 ticks @ 15s

    # Create full 10m grid
    epochs_10m = [t0.timestamp() + i * 15 for i in range(41)]
    prices_10m = [100.0 + i * 0.1 for i in range(41)]
    raw_dict = {"BTCUSDT": (epochs_10m, prices_10m)}

    grid_10m = AlignedPriceGrid.build(
        raw_dict,
        start_time=t0,
        end_time=t_full_end,
        grid_seconds=15,
    )
    assert len(grid_10m.sampling_epochs) == 41
    assert len(grid_10m.aligned_prices["BTCUSDT"]) == 41

    # Request narrower sub-window: 00:00 to 00:05 (21 ticks)
    t_sub_end = datetime(2026, 9, 1, 0, 5, 0, tzinfo=UTC)
    grid_5m = AlignedPriceGrid.build(
        grid_10m,  # Pass existing grid!
        start_time=t0,
        end_time=t_sub_end,
        grid_seconds=15,
    )

    # Both time points and price array must have length 21!
    assert len(grid_5m.sampling_epochs) == 21
    assert len(grid_5m.aligned_prices["BTCUSDT"]) == 21
    # Check end value matches
    assert grid_5m.aligned_prices["BTCUSDT"][-1] == pytest.approx(
        prices_10m[20], abs=1e-5
    )


def test_reconstruct_mtm_equity_bounds_to_requested_window() -> None:
    """Verifies that reconstruct_mtm_equity bounds its output points to the

    explicitly requested start_time and end_time even when passed a wider grid.
    """
    t0 = datetime(2026, 9, 1, 0, 0, 0, tzinfo=UTC)
    t_end = datetime(2026, 9, 1, 0, 5, 0, tzinfo=UTC)

    # Full 10m grid
    epochs_10m = [t0.timestamp() + i * 15 for i in range(41)]
    prices_10m = [100.0 for _ in range(41)]
    grid_10m = AlignedPriceGrid.build(
        {"BTCUSDT": (epochs_10m, prices_10m)},
        start_time=t0,
        end_time=t0 + timedelta(minutes=10),
        grid_seconds=15,
    )

    tr = TradeRecord(
        trade_id="TR_1",
        symbol="BTCUSDT",
        entry_time=t0 + timedelta(minutes=1),
        entry_price=100.0,
        exit_time=t0 + timedelta(minutes=3),
        exit_price=101.0,
        notional_usdt=100.0,
        leverage=5.0,
    )

    pts = reconstruct_mtm_equity(
        trades=[tr],
        price_series=grid_10m,
        start_time=t0,
        end_time=t_end,  # Explicitly 5m!
    )

    assert len(pts) == 21
    assert pts[-1].timestamp == t_end


# -----------------------------------------------------------------------------
# 4. P2: ScenarioSpec.max_mdd Enforced in evaluate_candidate
# -----------------------------------------------------------------------------
def test_scenario_max_mdd_enforced_in_evaluate_candidate() -> None:
    """Verifies that ScenarioSpec.max_mdd marks candidates exceeding max_mdd as

    infeasible.
    """
    w_start = datetime(2026, 9, 1, 0, 0, 0, tzinfo=UTC)
    w_end = datetime(2026, 9, 1, 1, 0, 0, tzinfo=UTC)

    # Trade drops 10% during hold: 100 -> 90 -> 105
    epochs = [
        w_start.timestamp(),
        (w_start + timedelta(minutes=10)).timestamp(),
        (w_start + timedelta(minutes=20)).timestamp(),
        (w_start + timedelta(minutes=40)).timestamp(),
        w_end.timestamp(),
    ]
    prices = [100.0, 100.0, 90.0, 105.0, 105.0]
    prices_by_symbol = {"BTCUSDT": (epochs, prices)}

    opp = {
        "opportunity_id": "opp_dd_01",
        "symbol": "BTCUSDT",
        "direction": "LONG",
        "detected_at": w_start + timedelta(minutes=9),
        "entry_eligible_at": w_start + timedelta(minutes=10),
        "entry_reference_price": 100.0,
        "exit_time": w_start + timedelta(minutes=40),
        "exit_price": 105.0,
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "impulse_return_pct": 1.0,
        "aggressive_imbalance": 0.5,
        "confirmation_min_imbalance": 0.5,
        "notional_intensity": 2.0,
        "volume_ratio": 1.0,
    }

    context = EvaluationContext(
        window_start=w_start,
        window_end=w_end,
        price_series=prices_by_symbol,
        events=[opp],
        ledger=SimulationLedger(initial_cash=1000.0, notional_usdt=100.0, leverage=5.0),
        initial_equity=1000.0,
    )

    cand_params = {
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "max_open_positions": 2,
    }

    # 1. Strict max_mdd = 0.005 (0.5%). Realized DD is ~1.0% (10U / 1000U).
    # Must fail feasibility!
    res_strict = evaluate_candidate(
        context=context,
        candidate=cand_params,
        scenario=ScenarioSpec(max_mdd=0.005),
        include_curve=False,
    )
    assert res_strict.is_feasible is False
    assert "exceeds scenario limit" in (res_strict.infeasible_reason or "")

    # 2. Looser max_mdd = 0.05 (5.0%). Must pass!
    res_loose = evaluate_candidate(
        context=context,
        candidate=cand_params,
        scenario=ScenarioSpec(max_mdd=0.05),
        include_curve=False,
    )
    assert res_loose.is_feasible is True
    assert res_loose.infeasible_reason is None


def test_evaluation_context_constructor_validation() -> None:
    """Verify EvaluationContext constructor validates contradictory inputs and enforces
    precedence rules (Architecture Recommendation #2).
    """
    t0 = datetime(2026, 9, 20, 0, 0, tzinfo=UTC)
    t1 = t0 + timedelta(minutes=10)

    # 1. Inverted time window
    with pytest.raises(ValueError, match="must be strictly earlier than window_end"):
        EvaluationContext(
            window_start=t1,
            window_end=t0,
            price_series={},
        )

    # 2. Conflicting notional between context and explicit ledger
    ledger_conflict = SimulationLedger(notional_usdt=200.0, leverage=5.0)
    with pytest.raises(ValueError, match="Conflicting notional"):
        EvaluationContext(
            window_start=t0,
            window_end=t1,
            price_series={},
            notional_per_entry=100.0,
            ledger=ledger_conflict,
        )

    # 3. Conflicting leverage between context and explicit ledger
    ledger_conflict_lev = SimulationLedger(notional_usdt=100.0, leverage=10.0)
    with pytest.raises(ValueError, match="Conflicting leverage"):
        EvaluationContext(
            window_start=t0,
            window_end=t1,
            price_series={},
            leverage=5.0,
            ledger=ledger_conflict_lev,
        )

    # 4. State-in auto-aligns initial_equity when kept at default
    state = PortfolioState(
        cash_usdt=1000.0,
        total_equity_mtm=1250.0,
        timestamp=t0,
        active_positions=(),
    )
    ctx_state = EvaluationContext(
        window_start=t0,
        window_end=t1,
        price_series={},
        state_in=state,
    )
    assert ctx_state.initial_equity == 1250.0


def test_fast_mtm_metrics_independent_event_by_event_reference() -> None:
    """Verify fast array MTM against an independent, un-vectorized discrete event
    reference covering irregular timestamps, SHORT positions, non-default amounts,
    and carry-in positions (Architecture Recommendation #4).
    """
    import bisect

    t0 = datetime(2026, 9, 20, 0, 0, tzinfo=UTC)
    t_end = t0 + timedelta(minutes=10)  # 41 points at 15s interval
    epochs = [t0.timestamp() + i * 15 for i in range(41)]

    # Dynamic prices
    # SOL: rises then falls
    sol_px = [150.0 + i * 0.5 - ((i - 20) ** 2) * 0.05 for i in range(41)]
    # BTC: fluctuates
    btc_px = [50000.0 - i * 50.0 + (i % 5) * 20.0 for i in range(41)]
    price_series = {"SOLUSDT": (epochs, sol_px), "BTCUSDT": (epochs, btc_px)}

    # 3 diverse trades:
    # Tr1: Carry-in LONG on SOL (entered before t0, exits mid-window at index 12 = 180s)
    tr1 = TradeRecord(
        trade_id="TR_CARRY",
        symbol="SOLUSDT",
        entry_time=t0 - timedelta(minutes=5),
        entry_price=140.0,
        exit_time=t0 + timedelta(seconds=180),
        exit_price=sol_px[12],
        notional_usdt=300.0,
        leverage=3.0,
        direction="LONG",
        fee_rate=0.0005,
        slippage_rate=0.0002,
    )

    # Tr2: Intra-window SHORT on BTC (irregular seconds: 75s to 375s)
    tr2 = TradeRecord(
        trade_id="TR_SHORT",
        symbol="BTCUSDT",
        entry_time=t0 + timedelta(seconds=75),  # index 5
        entry_price=btc_px[5],
        exit_time=t0 + timedelta(seconds=375),  # index 25
        exit_price=btc_px[25],
        notional_usdt=500.0,
        leverage=2.0,
        direction="SHORT",
        fee_rate=0.0005,
        slippage_rate=0.0002,
    )

    # Tr3: Still open LONG on SOL (enters at 300s, still open at window end)
    tr3 = TradeRecord(
        trade_id="TR_OPEN",
        symbol="SOLUSDT",
        entry_time=t0 + timedelta(seconds=300),  # index 20
        entry_price=sol_px[20],
        exit_time=None,
        notional_usdt=200.0,
        leverage=5.0,
        direction="LONG",
        is_open=True,
        fee_rate=0.0005,
        slippage_rate=0.0002,
    )

    trades = [tr1, tr2, tr3]
    initial_equity = 2500.0

    # Independent discrete reference calculation (step-by-step)
    ref_equities = []
    ref_margins = []
    t_start_ep = t0.timestamp()

    for k, ep in enumerate(epochs):
        cum_realized = 0.0
        cur_unrealized = 0.0
        cur_margin = 0.0

        for tr in trades:
            e_ep = tr.entry_time.timestamp()
            x_ep = tr.exit_time.timestamp() if tr.exit_time is not None else None
            p_t = sol_px[k] if tr.symbol == "SOLUSDT" else btc_px[k]

            # Realized PnL: booked at exit
            if x_ep is not None and ep >= x_ep and x_ep >= t_start_ep:
                cum_realized += tr.calculated_net_pnl

            # Active position
            k_entry = 0 if e_ep < t_start_ep else bisect.bisect_left(epochs, e_ep)
            k_exit = 41 if x_ep is None else bisect.bisect_left(epochs, x_ep)
            if k_entry <= k < k_exit:
                cur_margin += tr.initial_margin_usdt
                if tr.direction == "LONG":
                    gross = tr.notional_usdt * (p_t - tr.entry_price) / tr.entry_price
                else:
                    gross = tr.notional_usdt * (tr.entry_price - p_t) / tr.entry_price
                friction = tr.notional_usdt * (tr.fee_rate + tr.slippage_rate)
                cur_unrealized += gross - friction

        equity_k = initial_equity + cum_realized + cur_unrealized
        ref_equities.append(equity_k)
        ref_margins.append(cur_margin)

    # Compute reference drawdown metrics
    ref_peak_equity = initial_equity
    ref_max_dd_usdt = 0.0
    ref_max_dd_pct = 0.0
    sq_dds = []
    for eq in ref_equities:
        if eq > ref_peak_equity:
            ref_peak_equity = eq
        dd_u = ref_peak_equity - eq
        dd_p = dd_u / ref_peak_equity if ref_peak_equity > 0 else 0.0
        if dd_u > ref_max_dd_usdt:
            ref_max_dd_usdt = dd_u
        if dd_p > ref_max_dd_pct:
            ref_max_dd_pct = dd_p
        sq_dds.append(dd_p**2)

    ref_ui = math.sqrt(sum(sq_dds) / len(sq_dds))
    ref_peak_margin = max(ref_margins)
    ref_net_pnl = ref_equities[-1] - initial_equity

    # Run reconstruct_mtm_metrics_fast
    grid = AlignedPriceGrid.build(price_series, t0, t_end, grid_seconds=15)
    fast_m = reconstruct_mtm_metrics_fast(
        trades=trades,
        price_series=grid,
        initial_equity=initial_equity,
        grid_seconds=15,
        start_time=t0,
        end_time=t_end,
    )

    assert fast_m is not None
    assert fast_m.is_feasible is True
    assert fast_m.net_pnl == pytest.approx(ref_net_pnl, abs=1e-4)
    assert fast_m.max_drawdown_usdt == pytest.approx(ref_max_dd_usdt, abs=1e-4)
    assert fast_m.max_drawdown_pct == pytest.approx(ref_max_dd_pct, abs=1e-4)
    assert fast_m.peak_margin == pytest.approx(ref_peak_margin, abs=1e-2)
    assert fast_m.ulcer_index == pytest.approx(ref_ui, abs=1e-5)
    assert fast_m.terminal_equity == pytest.approx(ref_equities[-1], abs=1e-4)


# -----------------------------------------------------------------------------
# 5. Architecture: WFA Pipeline Migrated to Unified evaluate_candidate
# -----------------------------------------------------------------------------
def test_wfa_migrated_to_unified_evaluate_candidate() -> None:
    """Verifies that WFA employs evaluate_candidate for both IS MTM verification
    and OOS forward replay, properly threading stateful carry-in positions.
    """
    import pandas as pd

    from local_optimization.opportunity import RawOpportunity
    from local_optimization.run_walk_forward_analysis import (
        WindowSplit,
        run_walk_forward_analysis,
    )

    t0 = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)
    s1 = WindowSplit(
        split_id=1,
        name="Split 1",
        is_start=t0,
        is_end=t0 + timedelta(days=7),
        oos_start=t0 + timedelta(days=7),
        oos_end=t0 + timedelta(days=9),
    )
    s2 = WindowSplit(
        split_id=2,
        name="Split 2",
        is_start=t0 + timedelta(days=2),
        is_end=t0 + timedelta(days=9),
        oos_start=t0 + timedelta(days=9),
        oos_end=t0 + timedelta(days=11),
    )

    # Opportunity 1: enters & exits in IS
    t_is_entry = t0 + timedelta(hours=10)
    opp_is = RawOpportunity(
        opportunity_id="opp_is_1",
        symbol="BTCUSDT",
        direction="LONG",
        detected_at=t_is_entry,
        detected_epoch=t_is_entry.timestamp(),
        entry_eligible_at=t_is_entry,
        entry_reference_price=50000.0,
        exit_time=t0 + timedelta(hours=20),
        exit_price=51000.0,
        impulse_window_buckets=2,
        confirmation_buckets=1,
        impulse_return_pct=0.8,
        aggressive_imbalance=0.35,
        confirmation_min_imbalance=0.35,
        notional_intensity=3.5,
        volume_ratio=1.5,
        net_pnl_usdt=10.0,
    )

    # Opportunity 2: enters in Split 1 OOS (at day 8) and exits in Split 2 OOS
    # (at day 9.5). Spans the OOS boundary between Split 1 and Split 2
    t_oos_entry = t0 + timedelta(days=8)
    opp_cross_oos = RawOpportunity(
        opportunity_id="opp_cross_oos",
        symbol="BTCUSDT",
        direction="LONG",
        detected_at=t_oos_entry,
        detected_epoch=t_oos_entry.timestamp(),
        entry_eligible_at=t_oos_entry,
        entry_reference_price=50000.0,
        exit_time=t0 + timedelta(days=9, hours=12),
        exit_price=52000.0,
        impulse_window_buckets=2,
        confirmation_buckets=1,
        impulse_return_pct=0.8,
        aggressive_imbalance=0.35,
        confirmation_min_imbalance=0.35,
        notional_intensity=3.5,
        volume_ratio=1.5,
        net_pnl_usdt=20.0,
    )

    cand_params = {
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.5,
        "min_imbalance": 0.3,
        "min_intensity": 3.0,
        "min_volume_ratio": 1.0,
        "cooldown_buckets": 0,
        "max_open_positions": 2,
    }
    cand_df = pd.DataFrame([cand_params])
    grid_vals = {d: [cand_params[d]] for d in cand_params}

    # Run stateful WFA
    res_stateful = run_walk_forward_analysis(
        events=[opp_is, opp_cross_oos],
        splits=[s1, s2],
        candidate_pool_df=cand_df,
        grid_values=grid_vals,
        wfa_mode="stateful",
    )

    assert len(res_stateful) == 2

    # Split 1: Trade entered at day 8, still open at day 9 (window end)
    # evaluate_candidate carries out the active position in state_out
    assert res_stateful[0].rec_oos_trades == 1

    # Split 2: Inherited active position carries in and exits at day 9.5
    # Since no new entry in Split 2, rec_oos_trades is 0, but realized PnL > 0
    assert res_stateful[1].rec_oos_trades == 0
    assert res_stateful[1].rec_oos_pnl > 0.0

    # Total PnL across both OOS splits captures the 100U * 4% gross gain
    # minus fees (~3.79U)
    total_oos_pnl = res_stateful[0].rec_oos_pnl + res_stateful[1].rec_oos_pnl
    assert total_oos_pnl == pytest.approx(3.79, abs=0.1)

    # Run independent WFA: Split 2 starts clean with state_in=None
    res_indep = run_walk_forward_analysis(
        events=[opp_is, opp_cross_oos],
        splits=[s1, s2],
        candidate_pool_df=cand_df,
        grid_values=grid_vals,
        wfa_mode="independent",
    )
    assert len(res_indep) == 2
    # In independent mode, Split 2 has no carry-in and opp entered at day 8
    # (before split 2 OOS), so split 2 rec_oos_pnl is 0.0
    assert res_indep[1].rec_oos_pnl == 0.0


def test_dashboard_reuses_evaluation_context_and_aligned_grid() -> None:
    """Verify that compute_six_scenarios_view reuses a single unified
    EvaluationContext and AlignedPriceGrid across all 8 curves, and respects
    is_sorted=True pre-sorted opportunities.
    """
    from types import SimpleNamespace
    from unittest.mock import patch

    import pandas as pd

    from local_optimization.generate_six_scenarios_dashboard import (
        compute_six_scenarios_view,
    )
    from local_optimization.mtm_engine import AlignedPriceGrid
    from local_optimization.opportunity import RawOpportunity

    t0 = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)
    t1 = t0 + timedelta(hours=2)
    opp1 = RawOpportunity(
        opportunity_id="opp1",
        symbol="BTCUSDT",
        direction="LONG",
        detected_at=t0 + timedelta(minutes=10),
        detected_epoch=(t0 + timedelta(minutes=10)).timestamp(),
        entry_eligible_at=t0 + timedelta(minutes=10),
        entry_reference_price=100.0,
        exit_time=t0 + timedelta(minutes=30),
        exit_price=105.0,
        impulse_window_buckets=2,
        confirmation_buckets=1,
        impulse_return_pct=1.0,
        aggressive_imbalance=0.5,
        confirmation_min_imbalance=0.5,
        notional_intensity=4.0,
        volume_ratio=1.5,
        net_pnl_usdt=5.0,
    )
    opp2 = RawOpportunity(
        opportunity_id="opp2",
        symbol="BTCUSDT",
        direction="LONG",
        detected_at=t0 + timedelta(minutes=40),
        detected_epoch=(t0 + timedelta(minutes=40)).timestamp(),
        entry_eligible_at=t0 + timedelta(minutes=40),
        entry_reference_price=105.0,
        exit_time=t0 + timedelta(minutes=60),
        exit_price=110.0,
        impulse_window_buckets=2,
        confirmation_buckets=1,
        impulse_return_pct=1.0,
        aggressive_imbalance=0.5,
        confirmation_min_imbalance=0.5,
        notional_intensity=4.0,
        volume_ratio=1.5,
        net_pnl_usdt=5.0,
    )

    epochs = [t0.timestamp() + i * 15 for i in range(481)]
    prices = [100.0 + (i / 480.0) * 10.0 for i in range(481)]
    price_series = {"BTCUSDT": (epochs, prices)}

    cand_params = {
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.5,
        "min_imbalance": 0.3,
        "min_intensity": 3.0,
        "min_volume_ratio": 1.0,
        "cooldown_buckets": 0,
    }
    grid_df = pd.DataFrame([cand_params])
    manifest = SimpleNamespace(watermark_start=t0, watermark_end=t1)

    # Monitor AlignedPriceGrid.build calls
    original_build = AlignedPriceGrid.build
    build_calls: list[tuple] = []

    def mock_build(*args, **kwargs):
        build_calls.append((args, kwargs))
        return original_build(*args, **kwargs)

    with patch.object(AlignedPriceGrid, "build", side_effect=mock_build):
        res = compute_six_scenarios_view(
            events=[opp1, opp2],
            full_grid_df=grid_df,
            prices_by_symbol=price_series,
            manifest=manifest,
            max_workers=1,
            verify_depth=1,
        )

    (
        scenarios,
        cands_dict,
        curves_meta,
        timeline_series,
        reconstructed_pts,
        sorted_indices,
    ) = res

    # AlignedPriceGrid.build from raw price dictionary must be called at most once
    # (and NOT 8+ times from scratch for each curve individually)
    dict_build_calls = [c for c in build_calls if isinstance(c[0][0], dict)]
    assert len(dict_build_calls) <= 1

    # Verify curves were populated
    assert "s_m280_pnl_max" in curves_meta
    assert "s_m280_balanced" in curves_meta
    assert "b_profile1" in curves_meta
    assert "b_profile2" in curves_meta

    # S1 points must match timeline
    s1_pts = reconstructed_pts["s_m280_pnl_max"]
    assert len(s1_pts) > 0
    assert s1_pts[0].timestamp == t0
    assert s1_pts[-1].timestamp == t1

    # Final equity and PnL must be mathematically sound
    meta_s1 = curves_meta["s_m280_pnl_max"]
    assert meta_s1["final_equity"] > 1000.0
    assert meta_s1["net_pnl"] > 0.0
    assert meta_s1["total_trades"] == 2


def test_stateful_carry_in_floating_pnl_not_double_counted() -> None:
    """Regression test: verify that when evaluating a window with carry-in
    state (state_in), the baseline unrealized floating PnL at window start is
    properly deducted so that incremental net PnL and equity are not
    inflated/double-counted.

    Scenario:
    - Position entered before window at 100.0 (100U notional).
    - At window start, price is 110.0. Accrued floating gain is 10.00 - 0.07 = 9.93U.
    - Starting total_equity_mtm in state_in is 1009.93.
    - Price rises to 120.0 and trade exits at 120.0.
    - Incremental gain in this window is strictly +10.00 gross - 0.07 exit friction
      = +9.93U.
    - Both fast evaluation and curve evaluation must report net_pnl = +9.93U
      (NOT 19.86U).
    """
    t_start = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    t_end = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)
    t_entry = t_start - timedelta(hours=1)

    tr = TradeRecord(
        trade_id="tr1",
        symbol="BTCUSDT",
        entry_time=t_entry,
        entry_price=100.0,
        exit_time=t_end,
        exit_submitted_time=t_end,
        exit_price=120.0,
        notional_usdt=100.0,
        leverage=5.0,
        fee_rate=0.0005,
        slippage_rate=0.0002,
        direction="LONG",
        net_pnl_usdt=19.86,  # Lifetime net PnL from entry at 100
    )

    epochs = [t_start.timestamp(), t_end.timestamp()]
    prices = [110.0, 120.0]
    price_series = {"BTCUSDT": (epochs, prices)}

    # Starting portfolio state anchored to total_equity_mtm = 1009.93
    state_in = PortfolioState.create(
        timestamp=t_start,
        cash_usdt=980.0,
        total_equity_mtm=1009.93,
        active_positions=(tr,),
    )

    ctx = EvaluationContext(
        window_start=t_start,
        window_end=t_end,
        price_series=price_series,
        state_in=state_in,
    )

    # Automatic defense: EvaluationContext with state_in must have is_total_equity=True
    assert ctx.is_total_equity is True
    assert ctx.initial_equity == pytest.approx(1009.93, abs=0.01)

    res_fast = evaluate_candidate(ctx, {}, ScenarioSpec(), include_curve=False)
    res_curve = evaluate_candidate(ctx, {}, ScenarioSpec(), include_curve=True)

    # Both must report the incremental PnL (+9.93U), NOT the lifetime PnL (+19.86U)
    assert res_fast.net_pnl == pytest.approx(9.93, abs=0.05)
    assert res_curve.net_pnl == pytest.approx(9.93, abs=0.05)

    # Full curve checks
    assert res_curve.curve is not None
    assert len(res_curve.curve) > 0
    assert res_curve.curve[0].equity == pytest.approx(1009.93, abs=0.05)
    assert res_curve.curve[-1].equity == pytest.approx(1019.86, abs=0.05)

    # Test unclosed carry-in position case:
    tr_open = TradeRecord(
        trade_id="tr_open",
        symbol="BTCUSDT",
        entry_time=t_entry,
        entry_price=100.0,
        exit_time=t_end + timedelta(hours=2),
        exit_submitted_time=t_end + timedelta(hours=2),
        exit_price=130.0,
        notional_usdt=100.0,
        leverage=5.0,
        fee_rate=0.0005,
        slippage_rate=0.0002,
        direction="LONG",
        net_pnl_usdt=None,
        is_open=True,
    )
    state_in_open = PortfolioState.create(
        timestamp=t_start,
        cash_usdt=980.0,
        total_equity_mtm=1009.93,
        active_positions=(tr_open,),
    )
    ctx_open = EvaluationContext(
        window_start=t_start,
        window_end=t_end,
        price_series=price_series,
        state_in=state_in_open,
    )
    res_open_fast = evaluate_candidate(
        ctx_open, {}, ScenarioSpec(), include_curve=False
    )
    res_open_curve = evaluate_candidate(
        ctx_open, {}, ScenarioSpec(), include_curve=True
    )

    # Unclosed position incremental move is +10.00 (no exit friction yet)
    assert res_open_fast.net_pnl == pytest.approx(10.0, abs=0.05)
    assert res_open_curve.net_pnl == pytest.approx(10.0, abs=0.05)
    assert res_open_curve.curve[0].equity == pytest.approx(1009.93, abs=0.05)
    assert res_open_curve.curve[-1].equity == pytest.approx(1019.93, abs=0.05)
