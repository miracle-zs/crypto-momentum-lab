"""Unit tests for optimization protocol, Pareto frontier, and robust selection."""

from __future__ import annotations

from local_optimization.optimizer import (
    CandidateEvaluation,
    extract_pareto_frontier,
    select_best_and_recommended,
)
from local_optimization.protocol import (
    OptimizationProtocol,
    ParameterCandidate,
    default_orderflow_protocol,
)


def test_protocol_deterministic_id() -> None:
    """Protocol ID is deterministic and changes when any constraint changes."""
    p1 = default_orderflow_protocol(max_initial_margin_usdt=280.0)
    p2 = default_orderflow_protocol(max_initial_margin_usdt=280.0)
    p3 = default_orderflow_protocol(max_initial_margin_usdt=350.0)

    assert p1.protocol_id == p2.protocol_id
    assert p1.protocol_id != p3.protocol_id


def test_candidate_canonical_id() -> None:
    """Key ordering does not affect parameter_id hash."""
    cand1 = ParameterCandidate.from_dict({"a": 1, "b": 2})
    cand2 = ParameterCandidate.from_dict({"b": 2, "a": 1})
    assert cand1.parameter_id == cand2.parameter_id


def test_pareto_frontier_extraction() -> None:
    """Verify Pareto dominance filtering in 3D (PnL, UI, Margin)."""
    cand_a = ParameterCandidate.from_dict({"id": "A"})
    cand_b = ParameterCandidate.from_dict({"id": "B"})
    cand_c = ParameterCandidate.from_dict({"id": "C"})

    # A dominates B in all 3 objectives
    eval_a = CandidateEvaluation(
        candidate=cand_a,
        is_feasible=True,
        net_pnl=100.0,
        ulcer_index=0.05,
        peak_initial_margin_usdt=200.0,
    )
    eval_b = CandidateEvaluation(
        candidate=cand_b,
        is_feasible=True,
        net_pnl=90.0,
        ulcer_index=0.06,
        peak_initial_margin_usdt=220.0,
    )
    # C is non-dominated (lower PnL, but much lower UI and Margin)
    eval_c = CandidateEvaluation(
        candidate=cand_c,
        is_feasible=True,
        net_pnl=80.0,
        ulcer_index=0.02,
        peak_initial_margin_usdt=140.0,
    )

    pareto = extract_pareto_frontier([eval_a, eval_b, eval_c])
    pareto_ids = {e.candidate.params["id"] for e in pareto}

    assert "A" in pareto_ids
    assert "C" in pareto_ids
    assert "B" not in pareto_ids


def test_robust_recommendation_vs_daily_best() -> None:
    """Verify daily_best chooses highest profit,
    while recommended chooses lowest UI in near-optimal set.
    """
    protocol = OptimizationProtocol(
        scenario_family="test",
        near_optimal_delta_usdt=20.0,
        min_trades=0,
    )

    cand_a = ParameterCandidate.from_dict({"id": "A"})
    cand_b = ParameterCandidate.from_dict({"id": "B"})
    cand_c = ParameterCandidate.from_dict({"id": "C"})

    # Candidate A: Highest profit ($100), but rocky drawdown (UI 0.08)
    eval_a = CandidateEvaluation(
        candidate=cand_a,
        is_feasible=True,
        net_pnl=100.0,
        ulcer_index=0.08,
    )
    # Candidate B: Close profit ($95, within $20 delta), but smooth curve (UI 0.015)
    eval_b = CandidateEvaluation(
        candidate=cand_b,
        is_feasible=True,
        net_pnl=95.0,
        ulcer_index=0.015,
    )
    # Candidate C: Outside near-optimal set ($70)
    eval_c = CandidateEvaluation(
        candidate=cand_c,
        is_feasible=True,
        net_pnl=70.0,
        ulcer_index=0.005,
    )

    daily_best, recommended = select_best_and_recommended(
        [eval_a, eval_b, eval_c],
        protocol,
    )

    assert daily_best is not None
    assert recommended is not None
    assert daily_best.candidate.params["id"] == "A"
    assert recommended.candidate.params["id"] == "B"
