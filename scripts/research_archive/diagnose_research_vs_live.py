#!/usr/bin/env python3
"""Fail loudly when an event-study report is treated as a live replay."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--optimization-report", type=Path, required=True)
    parser.add_argument("--live-report", type=Path, required=True)
    parser.add_argument("--comparison-report", type=Path, required=True)
    args = parser.parse_args()

    optimization = load(args.optimization_report)
    live = load(args.live_report)
    comparison = load(args.comparison_report)
    checks = [
        {
            "check": "time_window_overlaps",
            "pass": comparison["overlap"]["duration_hours"] > 0,
            "evidence": comparison["overlap"],
        },
        {
            "check": "exit_model_matches",
            "pass": optimization["assumptions"]["primary_horizon_minutes"] == 15.0,
            "evidence": {
                "research_forward_horizon_minutes": optimization["assumptions"]["primary_horizon_minutes"],
                "live_exit": live["benchmark"]["exit"],
            },
        },
        {
            "check": "capital_model_matches",
            "pass": optimization["assumptions"]["capital_model"] == "account_equity",
            "evidence": {
                "research_capital_model": optimization["assumptions"]["capital_model"],
                "live_equity_definition": live["benchmark"]["equity_definition"],
            },
        },
        {
            "check": "uses_actual_account_inputs",
            "pass": "account_balance_snapshots" in optimization["source_root"],
            "evidence": {
                "research_source": optimization["source_root"],
                "live_source": live["benchmark"]["equity_definition"],
            },
        },
        {
            "check": "comparison_labels_event_study_as_live_replay",
            "pass": comparison["research"]["interpretation"] == "live_replay",
            "evidence": comparison["research"]["interpretation"],
        },
    ]
    failed = [check for check in checks if not check["pass"]]
    result = {
        "verdict": "NOT_A_LIVE_REPLAY" if failed else "LIVE_REPLAY_INPUTS_MATCH",
        "failed_check_count": len(failed),
        "checks": checks,
        "observed_same_window": {
            "live_raw_change_pct": comparison["live"]["raw_change_pct"],
            "research_candidate_index_change_pct": comparison["research"]["candidate_index_change_pct"],
        },
        "explanation": "The research curve cannot be expected to reproduce the live endpoint because its exit, capital, and input semantics differ." if failed else "Inputs are equivalent enough for endpoint reproduction testing.",
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
