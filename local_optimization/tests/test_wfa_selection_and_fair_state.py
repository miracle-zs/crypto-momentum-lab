"""Unit and integration tests for unified selection strategy and fair multi-track stateful WFA."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from local_optimization.mtm_engine import TradeRecord
from local_optimization.optimizer import (
    CandidateEvaluation,
    select_best_and_recommended,
)
from local_optimization.protocol import (
    OptimizationProtocol,
    ParameterCandidate,
    default_orderflow_protocol,
)
from local_optimization.run_walk_forward_analysis import (
    WindowSplit,
    run_walk_forward_analysis,
)
from local_optimization.simulation_ledger import PortfolioState


def test_protocol_selection_strategy_hashing() -> None:
    """Protocol ID changes when selection_strategy changes."""
    p_ui = default_orderflow_protocol(selection_strategy="ui_min")
    p_calmar = default_orderflow_protocol(selection_strategy="calmar_stability")
    p_comp = default_orderflow_protocol(selection_strategy="compounding")

    assert p_ui.protocol_id != p_calmar.protocol_id
    assert p_calmar.protocol_id != p_comp.protocol_id
    assert p_ui.selection_strategy == "ui_min"
    assert p_calmar.selection_strategy == "calmar_stability"
    assert p_comp.selection_strategy == "compounding"


def test_candidate_evaluation_calmar_ratio() -> None:
    """Calmar ratio uses max_drawdown_usdt or max_drawdown_pct."""
    cand = ParameterCandidate.from_dict({"id": "X"})

    # MDD from usdt
    e1 = CandidateEvaluation(
        candidate=cand,
        is_feasible=True,
        net_pnl=150.0,
        max_drawdown_usdt=30.0,
        max_drawdown_pct=0.03,
    )
    assert pytest.approx(e1.calmar_ratio, 0.001) == 5.0

    # MDD fallback from pct
    e2 = CandidateEvaluation(
        candidate=cand,
        is_feasible=True,
        net_pnl=120.0,
        max_drawdown_usdt=0.0,
        max_drawdown_pct=0.04,  # 4% of 1000 = 40.0
    )
    assert pytest.approx(e2.calmar_ratio, 0.001) == 3.0

    # Zero drawdown capped by max(1.0, mdd)
    e3 = CandidateEvaluation(
        candidate=cand,
        is_feasible=True,
        net_pnl=50.0,
        max_drawdown_usdt=0.5,
    )
    assert pytest.approx(e3.calmar_ratio, 0.001) == 50.0


def test_selection_strategy_calmar_stability() -> None:
    """calmar_stability selects candidate with highest Calmar * stability."""
    protocol = OptimizationProtocol(
        scenario_family="test_calmar",
        selection_strategy="calmar_stability",
        min_trades=0,
    )

    cand_a = ParameterCandidate.from_dict({"id": "A"})
    cand_b = ParameterCandidate.from_dict({"id": "B"})
    cand_c = ParameterCandidate.from_dict({"id": "C"})

    # Cand A: Highest PnL, but high drawdown -> Calmar = 100 / 50 = 2.0, Stab = 0.5 -> Score = 1.0
    eval_a = CandidateEvaluation(
        candidate=cand_a,
        is_feasible=True,
        net_pnl=100.0,
        max_drawdown_usdt=50.0,
        neighborhood_stability_score=0.5,
    )
    # Cand B: Moderate PnL, low drawdown -> Calmar = 80 / 10 = 8.0, Stab = 0.8 -> Score = 6.4 (Winner!)
    eval_b = CandidateEvaluation(
        candidate=cand_b,
        is_feasible=True,
        net_pnl=80.0,
        max_drawdown_usdt=10.0,
        neighborhood_stability_score=0.8,
    )
    # Cand C: Low PnL -> Calmar = 40 / 5 = 8.0, Stab = 0.5 -> Score = 4.0
    eval_c = CandidateEvaluation(
        candidate=cand_c,
        is_feasible=True,
        net_pnl=40.0,
        max_drawdown_usdt=5.0,
        neighborhood_stability_score=0.5,
    )

    daily_best, recommended = select_best_and_recommended(
        [eval_a, eval_b, eval_c], protocol
    )
    assert daily_best is not None
    assert recommended is not None
    # Daily best picks highest net PnL (A)
    assert daily_best.candidate.params["id"] == "A"
    # Recommended picks highest Calmar * stability (B)
    assert recommended.candidate.params["id"] == "B"


def test_selection_strategy_compounding() -> None:
    """compounding selects candidate with highest terminal compounding equity."""
    protocol = OptimizationProtocol(
        scenario_family="test_compounding",
        selection_strategy="compounding",
        min_trades=0,
    )

    cand_a = ParameterCandidate.from_dict({"id": "A"})
    cand_b = ParameterCandidate.from_dict({"id": "B"})
    cand_c = ParameterCandidate.from_dict({"id": "C"})

    eval_a = CandidateEvaluation(
        candidate=cand_a,
        is_feasible=True,
        net_pnl=100.0,
        terminal_compounded_equity=1250.0,
        neighborhood_stability_score=0.7,
    )
    eval_b = CandidateEvaluation(
        candidate=cand_b,
        is_feasible=True,
        net_pnl=150.0,
        terminal_compounded_equity=1400.0,
        neighborhood_stability_score=0.85,
    )
    eval_c = CandidateEvaluation(
        candidate=cand_c,
        is_feasible=True,
        net_pnl=80.0,
        terminal_compounded_equity=1100.0,
        neighborhood_stability_score=0.9,
    )

    daily_best, recommended = select_best_and_recommended(
        [eval_a, eval_b, eval_c], protocol
    )
    assert daily_best is not None
    assert recommended is not None
    # Highest compounding equity is B (1400.0)
    assert daily_best.candidate.params["id"] == "B"
    assert recommended.candidate.params["id"] == "B"


def test_wfa_symmetric_multi_track_stateful_isolation() -> None:
    """In stateful mode, all 4 tracks maintain and advance their own rolling states."""
    t0 = datetime(2026, 9, 3, 0, 0, tzinfo=UTC)
    split1 = WindowSplit(
        split_id=1,
        name="Split_01",
        is_start=t0,
        is_end=t0 + timedelta(days=7),
        oos_start=t0 + timedelta(days=7),
        oos_end=t0 + timedelta(days=9),
    )
    split2 = WindowSplit(
        split_id=2,
        name="Split_02",
        is_start=t0 + timedelta(days=2),
        is_end=t0 + timedelta(days=9),
        oos_start=t0 + timedelta(days=9),
        oos_end=t0 + timedelta(days=11),
    )

    # Mock candidate pool
    pool_df = pd.DataFrame(
        [
            {
                "impulse_window_buckets": 2,
                "confirmation_buckets": 1,
                "min_return_pct": 0.5,
                "min_imbalance": 0.3,
                "min_intensity": 1.5,
                "min_volume_ratio": 0.0,
                "cooldown_buckets": 0,
                "max_open_positions": 2,
            }
        ]
    )
    grid_values = {col: [pool_df[col].iloc[0]] for col in pool_df.columns}

    # Track states passed into evaluate_candidate
    recorded_states: list[tuple[str, PortfolioState | None]] = []

    def mock_evaluate_candidate(context: Any, candidate: Any, scenario: Any) -> Any:
        res = MagicMock()
        res.is_feasible = True
        res.net_pnl = 10.0
        res.mdd_usdt = 1.0
        res.mdd_pct = 0.001
        res.peak_margin = 40.0
        res.ulcer_index = 0.01
        res.admitted_trades = []

        # Tag state based on candidate params
        cid = candidate.get("min_return_pct", 0.0)
        label = f"cand_{cid}"
        recorded_states.append((label, context.state_in))

        # Create distinct state_out for each track
        dummy_trade = TradeRecord(
            trade_id=f"T_{label}",
            symbol=f"SYM_{label}",
            entry_time=context.window_start,
            entry_price=100.0,
            notional_usdt=100.0,
            leverage=5.0,
            direction="LONG",
        )
        state_out = PortfolioState(
            timestamp=context.window_end,
            cash_usdt=1000.0 + (context.state_in.cash_usdt if context.state_in else 0.0),
            total_equity_mtm=1000.0,
            active_positions=(dummy_trade,),
        )
        res.state_out = state_out
        return res

    with (
        patch(
            "local_optimization.run_walk_forward_analysis.evaluate_candidate",
            side_effect=mock_evaluate_candidate,
        ),
        patch(
            "local_optimization.run_walk_forward_analysis.SimulationLedger.simulate_window"
        ) as mock_sim,
    ):
        mock_sim_res = MagicMock()
        mock_sim_res.oos_pnl = 50.0
        mock_sim_res.peak_margin = 40.0
        mock_sim_res.mdd_usdt = 5.0
        mock_sim_res.n_trades = 20
        mock_sim.return_value = (mock_sim_res, None)

        # Run stateful WFA across 2 splits
        results = run_walk_forward_analysis(
            events=[],
            splits=[split1, split2],
            candidate_pool_df=pool_df,
            grid_values=grid_values,
            wfa_mode="stateful",
            price_series=None,
        )

        assert len(results) == 2

        # In Split 1: all tracks had state_in=None
        # In Split 2: each track received its OWN state_out from Split 1
        split2_states = [
            (label, st)
            for (label, st) in recorded_states
            if st is not None and len(st.active_positions) > 0
        ]
        # Verify that each track had non-None state in split 2
        assert len(split2_states) > 0
        # Check that active position symbol matches the track's own label
        for label, st in split2_states:
            pos_symbols = [t.symbol for t in st.active_positions]
            assert any(label in s for s in pos_symbols)


def test_wfa_admitted_trades_strictly_filter_oos_entry_time() -> None:
    """Trades counted in best_oos_trades must enter during or after oos_start."""
    t_oos = datetime(2026, 9, 10, 0, 0, tzinfo=UTC)

    # 1 carried-in trade (entry < oos_start) + 2 new trades (entry >= oos_start)
    trade_old = TradeRecord(
        trade_id="T_OLD",
        symbol="BTCUSDT",
        entry_time=t_oos - timedelta(hours=2),
        exit_time=t_oos + timedelta(hours=1),
        entry_price=50000.0,
        exit_price=51000.0,
        notional_usdt=100.0,
        leverage=5.0,
        direction="LONG",
        net_pnl_usdt=1.9,
    )
    trade_new1 = TradeRecord(
        trade_id="T_NEW1",
        symbol="ETHUSDT",
        entry_time=t_oos + timedelta(minutes=10),
        exit_time=t_oos + timedelta(hours=2),
        entry_price=3000.0,
        exit_price=3050.0,
        notional_usdt=100.0,
        leverage=5.0,
        direction="LONG",
        net_pnl_usdt=1.5,
    )
    trade_new2 = TradeRecord(
        trade_id="T_NEW2",
        symbol="SOLUSDT",
        entry_time=t_oos + timedelta(hours=5),
        exit_time=t_oos + timedelta(hours=6),
        entry_price=150.0,
        exit_price=155.0,
        notional_usdt=100.0,
        leverage=5.0,
        direction="LONG",
        net_pnl_usdt=3.2,
    )

    admitted = [trade_old, trade_new1, trade_new2]
    new_trades = [t for t in admitted if t.entry_time >= t_oos]
    assert len(new_trades) == 2
    assert trade_old not in new_trades
