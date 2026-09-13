#!/usr/bin/env python3
# ruff: noqa: E501
"""Materialize constrained A-G profiles from one completed filtered grid.

The expensive event simulation and the full parameter grid are shared by all
profiles.  A completed unconstrained grid is sufficient because B-G only add
profile-level feasibility and objective rules on top of the same candidate
metrics.  The selected candidates are then replayed once to emit their event
and equity artifacts.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import optimize_local_live_constrained as optimizer  # noqa: E402
import replay_frozen_local_live_strategies as frozen  # noqa: E402
from optimize_research_orderflow import Split  # noqa: E402

PROFILE_SPECS: dict[str, dict[str, object]] = {
    "A": {"cap": None, "cooldown": None, "drawdown_weight": 0.0, "label": "无保证金上限"},
    "B": {"cap": 350.0, "cooldown": None, "drawdown_weight": 0.0, "label": "保证金≤350U，自由 cooldown"},
    "C": {"cap": 350.0, "cooldown": 0, "drawdown_weight": 0.0, "label": "保证金≤350U，cooldown=0"},
    "D": {"cap": 280.0, "cooldown": None, "drawdown_weight": 0.0, "label": "保证金≤280U，自由 cooldown"},
    "E": {"cap": 280.0, "cooldown": 0, "drawdown_weight": 0.0, "label": "保证金≤280U，cooldown=0"},
    "F": {"cap": 280.0, "cooldown": 0, "drawdown_weight": 0.10, "label": "保证金≤280U，回撤惩罚，cooldown=0"},
    "G": {"cap": 280.0, "cooldown": None, "drawdown_weight": 0.10, "label": "保证金≤280U，回撤惩罚，自由 cooldown"},
}

BASELINE_CONFIG: dict[str, object] = {
    "impulse_window_buckets": 3,
    "confirmation_buckets": 1,
    "min_return_pct": 1.00,
    "min_imbalance": 0.40,
    "min_intensity": 2.0,
    "min_notional_5m_vs_30m": 0.0,
    "cooldown_buckets": 0,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--grid-dir", type=Path, required=True)
    parser.add_argument("--live-signals", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument(
        "--output-suffix",
        default="20260909-exclude-0800-1000-asia",
        help="suffix for each materialized optimization-full-pnl-current-* directory",
    )
    parser.add_argument("--exclude-entry-hours", default="08:00-10:00")
    parser.add_argument(
        "--exclude-entry-timezone",
        choices=("UTC", "Asia/Shanghai"),
        default="Asia/Shanghai",
    )
    parser.add_argument("--environment", default="research")
    parser.add_argument("--top-count", type=int, default=10)
    parser.add_argument("--fee-rate", type=float, default=0.0005)
    parser.add_argument("--min-validation-trades", type=int, default=10)
    return parser.parse_args()


def config_from_row(row: dict[str, str]) -> dict[str, object]:
    return {
        "impulse_window_buckets": int(row["impulse_window_buckets"]),
        "confirmation_buckets": int(row["confirmation_buckets"]),
        "min_return_pct": float(row["min_return_pct"]),
        "min_imbalance": float(row["min_imbalance"]),
        "min_intensity": float(row["min_intensity"]),
        "min_notional_5m_vs_30m": float(row["min_notional_5m_vs_30m"]),
        "cooldown_buckets": int(row["cooldown_buckets"]),
    }


def row_number(row: dict[str, str], key: str) -> float:
    return float(row[key]) if row[key] else -math.inf


def choose_profile_row(
    rows: list[dict[str, str]],
    *,
    cap: float | None,
    cooldown: int | None,
    drawdown_weight: float,
    min_validation_trades: int,
) -> tuple[dict[str, str], list[dict[str, str]]]:
    eligible: list[tuple[tuple[float, float, float, int], dict[str, str]]] = []
    for row in rows:
        if int(row["validation_n_closed"]) < min_validation_trades:
            continue
        if cap is not None and float(row["initial_margin_peak_usdt"]) > cap + 1e-9:
            continue
        if cooldown is not None and int(row["cooldown_buckets"]) != cooldown:
            continue
        pnl = row_number(row, "full_net_pnl_usdt")
        drawdown = row_number(row, "full_max_drawdown_usdt")
        score = pnl - drawdown_weight * drawdown
        key = (score, pnl, -drawdown, int(row["full_n_closed"]))
        eligible.append((key, row))
    if not eligible:
        raise SystemExit("profile has no eligible candidate rows")
    eligible.sort(key=lambda item: item[0], reverse=True)
    return eligible[0][1], [row for _key, row in eligible]


def selected_for_config(
    config: dict[str, object],
    pools: dict[tuple[int, int], list[optimizer.EventObservation]],
    simulated: dict[int, optimizer.SimulatedEvent],
    exclusion: optimizer.EntryTimeExclusion,
) -> list[optimizer.EventObservation]:
    return optimizer.select_observations(
        pools[
            (
                int(config["impulse_window_buckets"]),
                int(config["confirmation_buckets"]),
            )
        ],
        min_return=optimizer.Decimal(str(float(config["min_return_pct"]) / 100.0)),
        min_imbalance=optimizer.Decimal(str(config["min_imbalance"])),
        min_intensity=optimizer.Decimal(str(config["min_intensity"])),
        cooldown_buckets=int(config["cooldown_buckets"]),
        min_notional_5m_vs_30m=optimizer.Decimal(
            str(config.get("min_notional_5m_vs_30m", 0.0))
        ),
        simulated=simulated,
        entry_time_exclusion=exclusion,
    )


def profile_report(
    *,
    config: dict[str, object],
    selected: list[optimizer.EventObservation],
    simulated: dict[int, optimizer.SimulatedEvent],
    splits: tuple[Split, ...],
    optimization_start: Any,
    full_end: Any,
    cap: float | None,
    drawdown_weight: float,
) -> dict[str, object]:
    split_metrics = optimizer.all_metrics(selected, simulated, splits=splits)
    full_metrics = optimizer.metrics_for_split(
        selected,
        simulated,
        split=Split("full", optimization_start, full_end),
    )
    pnl = full_metrics["net_pnl_usdt"]
    drawdown = full_metrics["max_drawdown_usdt"]
    score = None if pnl is None or drawdown is None else round(float(pnl) - drawdown_weight * float(drawdown), 8)
    peak = optimizer.initial_margin_peak(selected, simulated)
    return {
        "config": config,
        "natural_initial_margin_peak_usdt": peak,
        "margin_constraint_feasible": cap is None or peak <= cap + 1e-9,
        "selection_scope": "full",
        "selection_score": score,
        "metrics": {**split_metrics, "full": full_metrics},
    }


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def write_profile_output(
    *,
    output_dir: Path,
    report: dict[str, object],
    baseline_report: dict[str, object],
    selected: list[optimizer.EventObservation],
    baseline_selected: list[optimizer.EventObservation],
    simulated: dict[int, optimizer.SimulatedEvent],
    full_start: Any,
    full_end: Any,
    source_grid_report: Path,
    exclusion: optimizer.EntryTimeExclusion,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(
        output_dir / "best_candidate_events.csv",
        [optimizer.event_csv_row(observation, simulated[id(observation)]) for observation in selected],
    )
    write_csv(
        output_dir / "baseline_events.csv",
        [optimizer.event_csv_row(observation, simulated[id(observation)]) for observation in baseline_selected],
    )
    write_csv(
        output_dir / "equity_series.csv",
        optimizer.build_pnl_series(
            {"baseline": baseline_selected, "best_validation": selected},
            simulated,
            start=full_start,
            end=full_end,
        ),
    )
    report["source_grid_report"] = str(source_grid_report)
    report["entry_time_exclusion"] = {
        "window": exclusion.window_text,
        "timezone": exclusion.timezone_label,
        "offset_hours": exclusion.offset_hours,
        "interval": "[start, end)",
        "applies_to": "filled entry_at",
    }
    report["baseline"] = baseline_report
    report["holdout_summary"] = {
        "best_validation": report["best_validation"]["metrics"]["holdout"],
        "baseline": baseline_report["metrics"]["holdout"],
    }
    (output_dir / "optimization_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    best = report["best_validation"]
    lines = [
        "# 排除北京时间 08:00–10:00 开仓后的本地全量寻优",
        "",
        f"- 过滤：实际成交 `entry_at` 的 `{exclusion.window_text}`（{exclusion.timezone_label}，左闭右开）。",
        f"- 数据窗口：`{report['data_start']}` 至 `{report['data_end']}`。",
        f"- 参数：`{json.dumps(best['config'], ensure_ascii=False)}`。",
        f"- 全窗口净 PnL：`{best['metrics']['full']['net_pnl_usdt']}`U。",
        f"- 全窗口最大回撤：`{best['metrics']['full']['max_drawdown_usdt']}`U。",
        f"- 自然峰值保证金：`{best['natural_initial_margin_peak_usdt']}`U。",
        "",
        "本目录由已完成的排除时段 A 组全网格派生；B–G 仅改变候选可行性和选择目标，未重复进行另一套数据模拟。",
    ]
    (output_dir / "optimization_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    grid_path = args.grid_dir / "grid_results.csv"
    with grid_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise SystemExit("filtered grid_results.csv is empty")

    parsed_window = optimizer.parse_entry_time_window(args.exclude_entry_hours)
    if parsed_window is None:
        raise SystemExit("an entry-time exclusion window is required")
    offsets = {"UTC": 0, "Asia/Shanghai": 8}
    exclusion = optimizer.EntryTimeExclusion(
        start_minute=parsed_window[0],
        end_minute=parsed_window[1],
        timezone_label=args.exclude_entry_timezone,
        offset_hours=offsets[args.exclude_entry_timezone],
    )

    chosen: dict[str, dict[str, str]] = {}
    eligible_rows: dict[str, list[dict[str, str]]] = {}
    for name, spec in PROFILE_SPECS.items():
        chosen[name], eligible_rows[name] = choose_profile_row(
            rows,
            cap=spec["cap"],
            cooldown=spec["cooldown"],
            drawdown_weight=float(spec["drawdown_weight"]),
            min_validation_trades=args.min_validation_trades,
        )

    configs = {name: config_from_row(row) for name, row in chosen.items()}
    configs["baseline"] = BASELINE_CONFIG
    frozen.STRATEGIES = {
        name: {"config": config} for name, config in configs.items() if name != "baseline"
    }
    replay_args = SimpleNamespace(
        input_root=args.input_root,
        live_signals=args.live_signals,
        environment=args.environment,
        top_count=args.top_count,
        fee_rate=args.fee_rate,
    )
    metadata, pools, simulated, splits, full_start, full_end, optimization_start = frozen.prepare_replay(
        replay_args
    )
    baseline_selected = selected_for_config(BASELINE_CONFIG, pools, simulated, exclusion)
    args.output_root.mkdir(parents=True, exist_ok=True)
    summary: dict[str, object] = {
        "data_start": full_start.isoformat(),
        "data_end": full_end.isoformat(),
        "entry_time_exclusion": {
            "window": exclusion.window_text,
            "timezone": exclusion.timezone_label,
            "offset_hours": exclusion.offset_hours,
            "interval": "[start, end)",
            "applies_to": "filled entry_at",
        },
        "profiles": {},
    }
    for name, spec in PROFILE_SPECS.items():
        selected = selected_for_config(configs[name], pools, simulated, exclusion)
        profile_baseline_report = profile_report(
            config=BASELINE_CONFIG,
            selected=baseline_selected,
            simulated=simulated,
            splits=splits,
            optimization_start=optimization_start,
            full_end=full_end,
            cap=None if spec["cap"] is None else float(spec["cap"]),
            drawdown_weight=float(spec["drawdown_weight"]),
        )
        best_report = profile_report(
            config=configs[name],
            selected=selected,
            simulated=simulated,
            splits=splits,
            optimization_start=optimization_start,
            full_end=full_end,
            cap=None if spec["cap"] is None else float(spec["cap"]),
            drawdown_weight=float(spec["drawdown_weight"]),
        )
        source_report = json.loads(
            (args.grid_dir / "optimization_report.json").read_text(encoding="utf-8")
        )
        report = {
            **source_report,
            "analysis": "live-constrained local order-flow optimization with entry-time exclusion",
            "evaluation_mode": "full_grid_selection_with_entry_time_exclusion",
            "selection_scope": "full",
            "selection_objective": (
                f"full_net_pnl_usdt_minus_{float(spec['drawdown_weight']):g}_times_full_max_drawdown_usdt"
            ),
            "drawdown_weight": float(spec["drawdown_weight"]),
            "margin_constraint": {
                "mode": "parameter_set_natural_peak" if spec["cap"] is not None else "none",
                "max_initial_margin_usdt": spec["cap"],
                "initial_margin_per_entry_usdt": 20.0,
                "max_full_entries_at_once": (
                    None if spec["cap"] is None else math.floor(float(spec["cap"]) / 20.0)
                ),
            },
            "fixed_cooldown_buckets": spec["cooldown"],
            "entry_time_exclusion": None,
            "best_validation": best_report,
            "top_candidates": [
                {
                    **row,
                    "profile_selection_score": round(
                        row_number(row, "full_net_pnl_usdt")
                        - float(spec["drawdown_weight"]) * row_number(row, "full_max_drawdown_usdt"),
                        8,
                    ),
                }
                for row in eligible_rows[name][:20]
            ],
        }
        report["load"] = metadata["load"]
        report["contiguous_segments"] = len(metadata["segments"])
        report["symbols"] = metadata["symbols"]
        report["data_start"] = full_start.isoformat()
        report["data_end"] = full_end.isoformat()
        report["optimization_window"] = {
            "start": optimization_start.isoformat(),
            "end": full_end.isoformat(),
            "first_partial_utc_day": metadata["proxy"].first_partial_utc_day.isoformat(),
        }
        report["entry_time_exclusion"] = {
            "window": exclusion.window_text,
            "timezone": exclusion.timezone_label,
            "offset_hours": exclusion.offset_hours,
            "interval": "[start, end)",
            "applies_to": "filled entry_at",
        }
        report["assumptions"] = {
            **dict(source_report.get("assumptions", {})),
            "entry_time_exclusion": (
                f"filled entry_at in {exclusion.window_text} {exclusion.timezone_label} is excluded "
                "before cooldown, PnL, drawdown, and natural margin calculations"
            ),
        }
        write_profile_output(
            output_dir=args.output_root
            / f"optimization-full-pnl-current-{name}-{args.output_suffix}",
            report=report,
            baseline_report=profile_baseline_report,
            selected=selected,
            baseline_selected=baseline_selected,
            simulated=simulated,
            full_start=full_start,
            full_end=full_end,
            source_grid_report=args.grid_dir / "optimization_report.json",
            exclusion=exclusion,
        )
        summary["profiles"][name] = {
            "config": configs[name],
            "label": spec["label"],
            "selection_score": best_report["selection_score"],
            "natural_initial_margin_peak_usdt": best_report["natural_initial_margin_peak_usdt"],
            "metrics": best_report["metrics"],
        }
    (args.output_root / "excluded-optimization-summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
