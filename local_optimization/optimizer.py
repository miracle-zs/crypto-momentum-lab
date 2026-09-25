"""Two-stage optimization engine, Pareto frontier extraction, and robust selection.

Implements the multi-criteria selection hierarchy:
1. Feasibility: Margin, MDD, UI, and trade count constraints
2. Daily Best: Pure maximum net profit in feasible set
3. Near-Optimal Set: P(theta) >= P_best - delta_P
4. Recommended: Minimizes UI and maximizes neighborhood stability
5. Pareto Frontier: Net PnL vs UI vs Peak Margin non-dominated set
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from local_optimization.protocol import (
    OptimizationProtocol,
    ParameterCandidate,
)


@dataclass
class CandidateEvaluation:
    """Evaluation result for a single parameter candidate."""

    candidate: ParameterCandidate
    is_feasible: bool
    infeasible_reasons: list[str] = field(default_factory=list)
    trade_count: int = 0
    net_pnl: float = 0.0
    net_return_pct: float = 0.0
    daily_log_growth: float = 0.0
    ulcer_index: float = 0.0
    cdar_95: float = 0.0
    max_drawdown_pct: float = 0.0
    max_drawdown_usdt: float = 0.0
    peak_initial_margin_usdt: float = 0.0
    terminal_compounded_equity: float = 0.0
    neighborhood_stability_score: float = 1.0
    is_pareto_optimal: bool = False

    @property
    def calmar_ratio(self) -> float:
        mdd = (
            self.max_drawdown_usdt
            if self.max_drawdown_usdt > 0
            else (self.max_drawdown_pct * 1000.0)
        )
        return self.net_pnl / max(1.0, mdd)

    def to_dict(self) -> dict[str, Any]:
        return {
            "parameter_id": self.candidate.parameter_id,
            "params": self.candidate.params,
            "is_feasible": self.is_feasible,
            "infeasible_reasons": self.infeasible_reasons,
            "trade_count": self.trade_count,
            "net_pnl": round(self.net_pnl, 4),
            "net_return_pct": round(self.net_return_pct, 4),
            "daily_log_growth": round(self.daily_log_growth, 6),
            "ulcer_index": round(self.ulcer_index, 6),
            "cdar_95": round(self.cdar_95, 6),
            "max_drawdown_pct": round(self.max_drawdown_pct, 6),
            "max_drawdown_usdt": round(self.max_drawdown_usdt, 4),
            "peak_initial_margin_usdt": round(self.peak_initial_margin_usdt, 2),
            "terminal_compounded_equity": round(self.terminal_compounded_equity, 2),
            "calmar_ratio": round(self.calmar_ratio, 4),
            "neighborhood_stability_score": round(self.neighborhood_stability_score, 4),
            "is_pareto_optimal": self.is_pareto_optimal,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> CandidateEvaluation:
        params = d.get("params", {})
        cand = ParameterCandidate.from_dict(params)
        return cls(
            candidate=cand,
            is_feasible=bool(d.get("is_feasible", True)),
            infeasible_reasons=list(d.get("infeasible_reasons", [])),
            trade_count=int(d.get("trade_count", 0)),
            net_pnl=float(d.get("net_pnl", 0.0)),
            net_return_pct=float(d.get("net_return_pct", 0.0)),
            daily_log_growth=float(d.get("daily_log_growth", 0.0)),
            ulcer_index=float(d.get("ulcer_index", 0.0)),
            cdar_95=float(d.get("cdar_95", 0.0)),
            max_drawdown_pct=float(d.get("max_drawdown_pct", 0.0)),
            max_drawdown_usdt=float(d.get("max_drawdown_usdt", 0.0)),
            peak_initial_margin_usdt=float(d.get("peak_initial_margin_usdt", 0.0)),
            terminal_compounded_equity=float(d.get("terminal_compounded_equity", 0.0)),
            neighborhood_stability_score=float(
                d.get("neighborhood_stability_score", 0.0)
            ),
            is_pareto_optimal=bool(d.get("is_pareto_optimal", False)),
        )


def is_candidate_compliant(
    e: CandidateEvaluation, protocol: OptimizationProtocol
) -> tuple[bool, list[str]]:
    """Strictly evaluate compliance against all protocol risk and trade limits."""
    reasons = []
    for field_name, val in [
        ("net_pnl", e.net_pnl),
        ("peak_initial_margin_usdt", e.peak_initial_margin_usdt),
        ("max_drawdown_pct", e.max_drawdown_pct),
        ("ulcer_index", e.ulcer_index),
    ]:
        if not math.isfinite(val):
            reasons.append(f"Non-finite value in {field_name}: {val}")
    if reasons:
        return False, reasons

    if not e.is_feasible:
        reasons.append("Marked infeasible in source evaluation")
    if protocol.max_initial_margin_usdt is not None:
        if e.peak_initial_margin_usdt > protocol.max_initial_margin_usdt + 1e-4:
            cap = protocol.max_initial_margin_usdt
            reasons.append(
                f"Peak margin {e.peak_initial_margin_usdt:.1f}U exceeds cap {cap:.1f}U"
            )
    if protocol.max_allowed_mdd_pct is not None:
        if e.max_drawdown_pct > protocol.max_allowed_mdd_pct + 1e-4:
            mdd_lim = protocol.max_allowed_mdd_pct
            reasons.append(f"MDD {e.max_drawdown_pct:.1%} exceeds limit {mdd_lim:.1%}")
    if protocol.max_allowed_ui is not None:
        if e.ulcer_index > protocol.max_allowed_ui + 1e-4:
            reasons.append(
                f"UI {e.ulcer_index:.4f} exceeds limit {protocol.max_allowed_ui:.4f}"
            )
    if protocol.min_trades is not None and protocol.min_trades > 0:
        if e.trade_count < protocol.min_trades:
            reasons.append(
                f"Trade count {e.trade_count} below minimum {protocol.min_trades}"
            )
    return len(reasons) == 0, reasons


def extract_pareto_frontier(
    evaluations: Sequence[CandidateEvaluation],
    protocol: OptimizationProtocol | None = None,
) -> list[CandidateEvaluation]:
    """Extract the 3D Pareto frontier (maximize PnL, minimize UI, minimize Margin).

    A candidate A dominates candidate B if:
    - PnL_A >= PnL_B
    - UI_A <= UI_B
    - Margin_A <= Margin_B
    and at least one inequality is strict.
    """
    if protocol is not None:
        feasible = [e for e in evaluations if is_candidate_compliant(e, protocol)[0]]
    else:
        feasible = [e for e in evaluations if e.is_feasible]
    if not feasible:
        return []

    pareto_set: list[CandidateEvaluation] = []
    # Sort descending by net_pnl: points with higher PnL are evaluated first
    # and can fast-prune subsequently evaluated points.
    feasible_sorted = sorted(feasible, key=lambda x: x.net_pnl, reverse=True)
    for a in feasible_sorted:
        is_dominated = False
        # Fast path: check against already admitted Pareto points
        for b in pareto_set:
            if (
                b.net_pnl >= a.net_pnl
                and b.ulcer_index <= a.ulcer_index
                and b.peak_initial_margin_usdt <= a.peak_initial_margin_usdt
                and (
                    b.net_pnl > a.net_pnl
                    or b.ulcer_index < a.ulcer_index
                    or b.peak_initial_margin_usdt < a.peak_initial_margin_usdt
                )
            ):
                is_dominated = True
                break
        if is_dominated:
            continue

        # Full check against remaining candidates
        for b in feasible_sorted:
            if a.candidate.parameter_id == b.candidate.parameter_id:
                continue
            if (
                b.net_pnl >= a.net_pnl
                and b.ulcer_index <= a.ulcer_index
                and b.peak_initial_margin_usdt <= a.peak_initial_margin_usdt
                and (
                    b.net_pnl > a.net_pnl
                    or b.ulcer_index < a.ulcer_index
                    or b.peak_initial_margin_usdt < a.peak_initial_margin_usdt
                )
            ):
                is_dominated = True
                break
        if not is_dominated:
            a.is_pareto_optimal = True
            pareto_set.append(a)

    # Sort pareto set by net_pnl descending
    pareto_set.sort(key=lambda x: x.net_pnl, reverse=True)
    return pareto_set


def compute_neighborhood_stability(
    candidate: ParameterCandidate,
    grid: dict[str, list[Any]],
    all_evaluations: dict[str, CandidateEvaluation],
) -> float:
    """Compute local stability by assessing immediate 1-step grid neighbors.

    Returns fraction of valid neighbors that are feasible and maintain >= 30%
    of peak return.
    """
    key_mapping = {
        "impulse_window_bars": "impulse_window_buckets",
        "confirmation_window_bars": "confirmation_buckets",
        "min_directional_return_bps": "min_return_pct",
        "imbalance_threshold": "min_imbalance",
        "min_notional_intensity": "min_intensity",
        "symbol_cooldown_bars": "cooldown_buckets",
        "concurrency_slots": "max_open_positions",
    }
    canon_grid = {key_mapping.get(k, k): v for k, v in grid.items()}
    params = candidate.params
    neighbor_ids: list[str] = []

    for key, values in canon_grid.items():
        if key not in params:
            continue
        val = params[key]
        if val not in values:
            continue
        curr_idx = values.index(val)
        for step in (-1, 1):
            next_idx = curr_idx + step
            if 0 <= next_idx < len(values):
                neighbor_params = dict(params)
                neighbor_params[key] = values[next_idx]
                n_cand = ParameterCandidate.from_dict(neighbor_params)
                neighbor_ids.append(n_cand.parameter_id)

    if not neighbor_ids:
        return 0.0

    valid_neighbors = 0
    stable_neighbors = 0
    cand_eval = all_evaluations.get(candidate.parameter_id)
    cand_pnl = cand_eval.net_pnl if cand_eval else 0.0

    for n_id in neighbor_ids:
        n_eval = all_evaluations.get(n_id)
        if n_eval is not None:
            valid_neighbors += 1
            # Stable if feasible and doesn't experience severe collapse
            if n_eval.is_feasible:
                if cand_pnl <= 0:
                    if n_eval.net_pnl >= 0:
                        stable_neighbors += 1
                else:
                    if n_eval.net_pnl >= 0.3 * cand_pnl:
                        stable_neighbors += 1

    # Missing neighbors penalize stability
    # (denominator is all theoretical grid neighbors)
    return stable_neighbors / len(neighbor_ids) if neighbor_ids else 0.0


def select_best_and_recommended(
    evaluations: Sequence[CandidateEvaluation],
    protocol: OptimizationProtocol,
    all_eval_map: dict[str, CandidateEvaluation] | None = None,
) -> tuple[CandidateEvaluation | None, CandidateEvaluation | None]:
    """Select (daily_best, recommended) candidates according to the protocol rules.

    Supports configurable selection strategies via protocol.selection_strategy:
    1. "ui_min" (default / WFA baseline):
       - daily_best: compliant candidate with highest net_pnl (or daily_log_growth).
       - recommended: within near-optimal delta band from daily_best,
         minimizes ulcer_index, breaking ties by neighborhood stability.
    2. "calmar_stability" (Six Scenarios S2):
       - daily_best: compliant candidate with highest net_pnl (tie-break by calmar).
       - recommended: compliant candidate maximizing (calmar_ratio * stability),
         breaking ties by calmar_ratio, then net_pnl, then parameter_id.
    3. "compounding" (Six Scenarios S3):
       - daily_best: compliant candidate with highest terminal_compounded_equity (or log_growth / net_pnl).
       - recommended: compliant candidate maximizing terminal_compounded_equity (or log_growth / net_pnl),
         breaking ties by stability, then net_pnl, then parameter_id.
    """
    compliant_evals: list[CandidateEvaluation] = []
    for e in evaluations:
        ok, _ = is_candidate_compliant(e, protocol)
        if ok:
            compliant_evals.append(e)

    if not compliant_evals:
        return None, None

    # Populate stability scores if grid is available
    if all_eval_map and protocol.parameter_grid:
        for item in compliant_evals:
            item.neighborhood_stability_score = compute_neighborhood_stability(
                item.candidate, protocol.parameter_grid, all_eval_map
            )

    strategy = getattr(protocol, "selection_strategy", "ui_min")

    if strategy == "calmar_stability":
        # S2: Maximize Calmar * Stability
        daily_best = max(
            compliant_evals,
            key=lambda x: (x.net_pnl, x.calmar_ratio, -x.ulcer_index),
        )
        recommended = max(
            compliant_evals,
            key=lambda x: (
                x.calmar_ratio * x.neighborhood_stability_score,
                x.calmar_ratio,
                x.net_pnl,
            ),
        )
        return daily_best, recommended

    if strategy == "compounding":
        # S3: Maximize Terminal Compounding Equity / Log Growth
        def _comp_metric(x: CandidateEvaluation) -> float:
            if x.terminal_compounded_equity > 0.0:
                return x.terminal_compounded_equity
            if x.daily_log_growth != 0.0:
                return x.daily_log_growth
            return x.net_pnl

        daily_best = max(
            compliant_evals,
            key=lambda x: (_comp_metric(x), x.net_pnl),
        )
        recommended = max(
            compliant_evals,
            key=lambda x: (
                _comp_metric(x),
                x.neighborhood_stability_score,
                x.net_pnl,
            ),
        )
        return daily_best, recommended

    # Default: "ui_min"
    use_log_growth = any(e.daily_log_growth != 0.0 for e in compliant_evals)
    if use_log_growth:
        compliant_evals.sort(
            key=lambda x: (x.daily_log_growth, -x.ulcer_index), reverse=True
        )
    else:
        compliant_evals.sort(key=lambda x: (x.net_pnl, -x.ulcer_index), reverse=True)
    daily_best = compliant_evals[0]

    # Near-optimal subset
    if use_log_growth:
        threshold_growth = (
            daily_best.daily_log_growth - protocol.near_optimal_delta_log_growth
        )
        near_optimal = [
            e for e in compliant_evals if e.daily_log_growth >= threshold_growth
        ]
    else:
        threshold_pnl = daily_best.net_pnl - protocol.near_optimal_delta_usdt
        near_optimal = [e for e in compliant_evals if e.net_pnl >= threshold_pnl]

    # In near-optimal set: sort by UI asc, stability desc, parameter_id asc
    def recommended_sort_key(e: CandidateEvaluation) -> tuple[float, float, str]:
        return (
            e.ulcer_index,
            -e.neighborhood_stability_score,
            e.candidate.parameter_id,
        )

    near_optimal.sort(key=recommended_sort_key)
    recommended = near_optimal[0]

    return daily_best, recommended
