"""Longitudinal stability tracking, three-track observer, and stage stability criteria.

Tracks three parallel parameter tracks:
1. daily_best: Historical peak performer for research observation
2. recommended: Robust recommendation evaluated on frozen out-of-sample forward data
3. live_actual: Parameter set currently deployed on live trading servers

Evaluates stage stability:
- Winner consistency across recent valid days
- Out-of-sample forward constraint compliance
- Performance decomposition: Data-extension gain vs re-selection gain
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from local_optimization.optimizer import CandidateEvaluation


@dataclass
class DailyTrackRecord:
    """Daily record capturing the three parallel tracks."""

    date_str: str  # YYYY-MM-DD (UTC)
    snapshot_id: str
    protocol_id: str
    daily_best: CandidateEvaluation | None = None
    recommended: CandidateEvaluation | None = None
    live_actual: CandidateEvaluation | None = None
    oos_forward_pnl: float | None = None
    is_snapshot_complete: bool = True
    reconciliation_passed: bool = True
    stability_status: str = (
        "insufficient_evidence"  # insufficient_evidence | candidate | stable | degraded
    )
    stability_notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "date_str": self.date_str,
            "snapshot_id": self.snapshot_id,
            "protocol_id": self.protocol_id,
            "daily_best": self.daily_best.to_dict() if self.daily_best else None,
            "recommended": self.recommended.to_dict() if self.recommended else None,
            "live_actual": self.live_actual.to_dict() if self.live_actual else None,
            "oos_forward_pnl": (
                round(self.oos_forward_pnl, 4)
                if self.oos_forward_pnl is not None
                else None
            ),
            "is_snapshot_complete": self.is_snapshot_complete,
            "reconciliation_passed": self.reconciliation_passed,
            "stability_status": self.stability_status,
            "stability_notes": self.stability_notes,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> DailyTrackRecord:
        db = (
            CandidateEvaluation.from_dict(d["daily_best"])
            if d.get("daily_best")
            else None
        )
        rec = (
            CandidateEvaluation.from_dict(d["recommended"])
            if d.get("recommended")
            else None
        )
        live = (
            CandidateEvaluation.from_dict(d["live_actual"])
            if d.get("live_actual")
            else None
        )
        raw_oos = d.get("oos_forward_pnl")
        return cls(
            date_str=str(d.get("date_str", "")),
            snapshot_id=str(d.get("snapshot_id", "")),
            protocol_id=str(d.get("protocol_id", "")),
            daily_best=db,
            recommended=rec,
            live_actual=live,
            oos_forward_pnl=float(raw_oos) if raw_oos is not None else None,
            is_snapshot_complete=bool(d.get("is_snapshot_complete", False)),
            reconciliation_passed=bool(d.get("reconciliation_passed", False)),
            stability_status=str(d.get("stability_status", "insufficient_evidence")),
            stability_notes=list(d.get("stability_notes", [])),
        )


def decompose_daily_performance(
    p_old_old: float,
    p_old_new: float,
    p_new_new: float,
) -> dict[str, float]:
    """Decompose historical equity change into data extension vs parameter re-selection.

    Data addition impact = P(old_param, new_window) - P(old_param, old_window)
    Re-selection gain   = P(new_param, new_window) - P(old_param, new_window)
    """
    data_extension_gain = p_old_new - p_old_old
    reselection_gain = p_new_new - p_old_new
    total_change = p_new_new - p_old_old

    return {
        "data_extension_gain": round(data_extension_gain, 4),
        "reselection_gain": round(reselection_gain, 4),
        "total_change": round(total_change, 4),
    }


def evaluate_stage_stability(
    history: list[DailyTrackRecord],
    min_consistency_days: int = 7,
    min_oos_days: int = 14,
    min_stability_score: float = 0.70,
    min_trades: int = 0,
    max_mdd_pct: float | None = 0.20,
) -> tuple[str, list[str]]:
    """Evaluate whether the recommended parameter has reached stage stability.

    Stage stability requires:
    1. Deduplicated unique calendar dates spanning at least min_consistency_days.
    2. Consistent recommended candidate across recent days (>= 80%).
    3. Out-of-sample forward window maintained positive cumulative return.
    4. At least min_oos_days to reach fully authorized 'stable' state.
    5. Feasible recommendations meeting neighborhood stability (>= 70%) and risk limits.
    """
    notes = []
    # Deduplicate history by distinct date_str (keeping latest per date)
    date_map: dict[str, DailyTrackRecord] = {}
    for r in history:
        date_map[r.date_str] = r
    unique_history = [date_map[k] for k in sorted(date_map.keys())]

    if len(unique_history) < min_consistency_days:
        notes.append(
            f"Insufficient history: {len(unique_history)} unique days "
            f"< required {min_consistency_days}d."
        )
        return "insufficient_evidence", notes

    # Check snapshot diversity: distinct data snapshots required
    unique_snapshots = {r.snapshot_id for r in unique_history if r.snapshot_id}
    if len(unique_snapshots) < min(len(unique_history), min_consistency_days):
        notes.append(
            f"Insufficient snapshot diversity: only {len(unique_snapshots)} "
            f"unique snapshot(s) across {len(unique_history)} days."
        )
        return "insufficient_evidence", notes

    recent = unique_history[-min_consistency_days:]

    # Check compliance, feasibility, stability, and audit of recommendations
    for r in recent:
        if not r.is_snapshot_complete:
            notes.append(f"Data snapshot on {r.date_str} is incomplete or unverified.")
            return "insufficient_evidence", notes
        if not r.reconciliation_passed:
            notes.append(
                f"Live reconciliation on {r.date_str} failed or has "
                f"insufficient evidence."
            )
            return "insufficient_evidence", notes
        if not r.recommended:
            notes.append("Missing recommended parameter records in recent window.")
            return "insufficient_evidence", notes
        if not r.recommended.is_feasible:
            reasons = (
                ", ".join(r.recommended.infeasible_reasons)
                if r.recommended.infeasible_reasons
                else "failed risk/constraint limits"
            )
            notes.append(f"Candidate on {r.date_str} is marked infeasible: {reasons}.")
            return "candidate", notes
        if r.recommended.neighborhood_stability_score < min_stability_score:
            notes.append(
                f"Candidate on {r.date_str} neighborhood stability "
                f"{r.recommended.neighborhood_stability_score:.1%} "
                f"< required {min_stability_score:.1%}."
            )
            return "candidate", notes
        if max_mdd_pct is not None and r.recommended.max_drawdown_pct > max_mdd_pct:
            notes.append(
                f"Candidate on {r.date_str} MDD "
                f"{r.recommended.max_drawdown_pct:.1%} exceeds limit {max_mdd_pct:.1%}."
            )
            return "candidate", notes
        if min_trades > 0 and r.recommended.trade_count < min_trades:
            notes.append(
                f"Candidate on {r.date_str} has insufficient trade activity: "
                f"{r.recommended.trade_count} trades < required {min_trades}."
            )
            return "candidate", notes

    rec_ids = [r.recommended.candidate.parameter_id for r in recent if r.recommended]

    if not rec_ids or len(rec_ids) < min_consistency_days:
        notes.append("Missing recommended parameter records in recent window.")
        return "insufficient_evidence", notes

    # Check winner consistency
    dominant_id = max(set(rec_ids), key=rec_ids.count)
    consistency_ratio = rec_ids.count(dominant_id) / len(rec_ids)

    if consistency_ratio < 0.80:
        notes.append(
            f"Recommendation churning: dominant parameter is {consistency_ratio:.1%} "
            f"of recent days (< 80%)."
        )
        return "candidate", notes

    # Check forward OOS performance
    valid_oos = [r.oos_forward_pnl for r in recent if r.oos_forward_pnl is not None]
    if len(valid_oos) < min_consistency_days:
        notes.append(
            f"Missing OOS evidence: {len(valid_oos)} valid OOS observations "
            f"< required {min_consistency_days}d."
        )
        return "insufficient_evidence", notes

    cum_oos = sum(valid_oos)
    negative_oos_days = sum(1 for pnl in valid_oos if pnl < 0)

    if cum_oos <= 0:
        notes.append(
            f"OOS non-positive: Cumulative forward return is {cum_oos:+.2f}U "
            "(must be strictly > 0)."
        )
        return "degraded" if cum_oos < 0 else "candidate", notes
    if negative_oos_days > (len(recent) // 3):
        notes.append(
            f"OOS degradation: {negative_oos_days} days with negative forward return."
        )
        return "degraded", notes

    # Check whether full min_oos_days requirement is satisfied
    all_valid_oos = [
        r.oos_forward_pnl for r in unique_history if r.oos_forward_pnl is not None
    ]
    if len(unique_history) < min_oos_days or len(all_valid_oos) < min_oos_days:
        notes.append(
            f"Candidate: {dominant_id} consistent {consistency_ratio:.1%} "
            f"over {min_consistency_days}d, accumulating OOS evidence "
            f"({len(all_valid_oos)}d/{min_oos_days}d)."
        )
        return "candidate", notes

    notes.append(
        f"Stage stable: {dominant_id} maintained {consistency_ratio:.1%} consistency "
        f"over {len(unique_history)} unique days with positive cumulative OOS "
        f"(+{cum_oos:.2f}U)."
    )
    return "stable", notes
