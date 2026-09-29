#!/usr/bin/env python3
"""Check whether breakout-acceptance time windows are driving the result.

The exit policy is held fixed at the live candle_15m policy.  Only the causal
profile, migration, retest, chain, and absorption observation windows change.
This imports the shared analysis functions so the sensitivity run cannot
silently drift from the primary report's definitions.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from analyze_breakout_acceptance import (
    SameExitConfig,
    analyze_signal,
    build_entry_events,
    closed_candles,
    format_time,
    load_signals,
    load_states,
    run_same_exit,
    summarize_same_exit_run,
)


BASE_PARAMS: dict[str, Any] = {
    "profile_minutes": 60,
    "early_profile_minutes": 5,
    "retest_start_minutes": 5,
    "retest_end_minutes": 20,
    "chain_end_minutes": 30,
    "bin_pct": 0.001,
    "value_area_pct": 0.70,
    "min_imbalance": 0.40,
    "touch_tolerance_pct": 0.0015,
    "breach_tolerance_pct": 0.0020,
    "absorption_before_buckets": 4,
    "absorption_after_buckets": 9,
    "absorption_sell_share": 0.55,
    "absorption_min_multiple": 1.0,
    "reclaim_buffer_pct": 0.0005,
}

SCENARIOS = (
    (
        "value_up_stage",
        "突破 + POC 上移 + Value 上移",
        "stage_breakout_buy_poc_value",
        "signal",
    ),
    (
        "acceptance_posthoc",
        "完整 acceptance（事后筛选，原信号入场）",
        "acceptance_chain",
        "signal",
    ),
    (
        "acceptance_reclaim",
        "完整 acceptance 后 reclaim 入场",
        "acceptance_chain",
        "reclaim",
    ),
)


def annotate_stages(rows: list[dict[str, Any]]) -> dict[str, int]:
    stage_definitions = (
        ("stage_breakout", ("breakout",)),
        ("stage_breakout_buy", ("breakout", "buy_imbalance")),
        (
            "stage_breakout_buy_poc",
            ("breakout", "buy_imbalance", "poc_up"),
        ),
        (
            "stage_breakout_buy_poc_value",
            ("breakout", "buy_imbalance", "poc_up", "value_up"),
        ),
        (
            "stage_retest_hold",
            (
                "breakout",
                "buy_imbalance",
                "poc_up",
                "value_up",
                "retest_hold",
            ),
        ),
        (
            "stage_absorption",
            (
                "breakout",
                "buy_imbalance",
                "poc_up",
                "value_up",
                "retest_hold",
                "absorption_proxy",
            ),
        ),
        (
            "stage_acceptance_chain",
            (
                "breakout",
                "buy_imbalance",
                "poc_up",
                "value_up",
                "retest_hold",
                "absorption_proxy",
                "reclaim",
            ),
        ),
    )
    for row in rows:
        for key, requirements in stage_definitions:
            row[key] = all(bool(row.get(requirement)) for requirement in requirements)
        row["strict_acceptance_chain"] = bool(
            row.get("stage_acceptance_chain") and row.get("reclaim_buy_confirmed")
        )
    return {
        "signals": len(rows),
        "breakout": sum(bool(row.get("stage_breakout")) for row in rows),
        "value_up": sum(
            bool(row.get("stage_breakout_buy_poc_value")) for row in rows
        ),
        "retest_hold": sum(bool(row.get("stage_retest_hold")) for row in rows),
        "absorption": sum(bool(row.get("stage_absorption")) for row in rows),
        "acceptance": sum(
            bool(row.get("stage_acceptance_chain")) for row in rows
        ),
        "strict": sum(bool(row.get("strict_acceptance_chain")) for row in rows),
    }


def analyze_configuration(
    signals: list[dict[str, Any]],
    states: dict[str, Any],
    params: dict[str, Any],
    exit_config: SameExitConfig,
    candle_cache: dict[str, list[Any]],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for signal in signals:
        rows.append(
            analyze_signal(
                signal,
                states.get(signal["symbol"]),
                **params,
            )
        )
    funnel = annotate_stages(rows)
    scenarios: dict[str, dict[str, Any]] = {}
    for scenario, label, condition, entry_mode in SCENARIOS:
        events = build_entry_events(
            rows,
            scenario=scenario,
            label=label,
            condition=condition,
            entry_mode=entry_mode,
        )
        run = run_same_exit(
            events,
            states,
            exit_config,
            enforce_no_overlap=True,
            candle_cache=candle_cache,
        )
        scenarios[scenario] = summarize_same_exit_run(run)
    return {"funnel": funnel, "scenarios": scenarios}


def sensitivity_specs() -> list[tuple[str, str, dict[str, Any]]]:
    specs: list[tuple[str, str, dict[str, Any]]] = []
    for value in (30, 45, 60, 75, 90, 105, 120, 135, 150, 180, 240):
        specs.append(("profile", f"{value}m", {"profile_minutes": value}))
    for value in (3, 5, 10, 15):
        specs.append(("migration", f"{value}m", {"early_profile_minutes": value}))
    for start, end in ((3, 15), (5, 20), (5, 30), (10, 30)):
        specs.append(
            (
                "retest",
                f"{start}-{end}m",
                {
                    "retest_start_minutes": start,
                    "retest_end_minutes": end,
                },
            )
        )
    for value in (30, 45, 60, 90):
        specs.append(("chain", f"{value}m", {"chain_end_minutes": value}))
    for before, after, label in (
        (2, 3, "1m15s"),
        (4, 9, "3m15s"),
        (8, 17, "6m15s"),
        (16, 33, "12m15s"),
    ):
        specs.append(
            (
                "absorption",
                label,
                {
                    "absorption_before_buckets": before,
                    "absorption_after_buckets": after,
                },
            )
        )
    return specs


def pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.2f}%"


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Breakout acceptance cycle sensitivity",
        "",
        f"- Window: `{report['window']['start']}` to `{report['window']['end']}` UTC",
        f"- Signals: **{report['counts']['signals']}**; state rows: **{report['counts']['state_rows']}**",
        "- Exit held fixed: `candle_15m`, 1 bearish confirmation, +0.10% direct-close threshold, +0.88% recovery limit, 1 grace bar.",
        "- Primary execution view: same-symbol overlapping entries are skipped; all values are based on the same 15s-state replay and 100 USDT notional assumption.",
        "",
        "## 结果",
        "",
        "`reclaim` 是更接近可执行的延迟入场；`posthoc` 只是检验完整链条是否具备筛选能力。平均净收益只统计已按同一退出规则平仓的样本。",
        "",
        "| 周期维度 | 设置 | Value-up数量 | Acceptance数量 | Reclaim已平仓 | Reclaim平均净收益 | Reclaim胜率 | Reclaim累计净PnL | Posthoc平均净收益 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for item in report["results"]:
        value = item["analysis"]["scenarios"]["acceptance_reclaim"]
        posthoc = item["analysis"]["scenarios"]["acceptance_posthoc"]
        lines.append(
            "| {dimension} | {setting} | {value_up} | {acceptance} | {closed} | {avg} | {win} | {pnl} | {posthoc} |".format(
                dimension=item["dimension"],
                setting=item["setting"],
                value_up=item["analysis"]["funnel"]["value_up"],
                acceptance=item["analysis"]["funnel"]["acceptance"],
                closed=value["n_closed"],
                avg=pct(value["avg_net_return_pct"]),
                win=(
                    "n/a"
                    if value["win_rate"] is None
                    else f"{value['win_rate']:.1%}"
                ),
                pnl=(
                    "n/a"
                    if value["total_net_pnl_usdt_at_100"] is None
                    else f"{value['total_net_pnl_usdt_at_100']:.2f}"
                ),
                posthoc=pct(posthoc["avg_net_return_pct"]),
            )
        )
    lines.extend(
        [
            "",
            "## 按前后 24 小时分段核验",
            "",
            "这不是独立样本外回测，只用来检查较长 profile 的改善是否只集中在某一半样本。退出条件仍完全相同。",
            "",
            "| 分段 | Profile | 信号数 | Acceptance数 | Reclaim已平仓 | Reclaim平均净收益 | Reclaim胜率 |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for item in report["split_results"]:
        reclaim = item["analysis"]["scenarios"]["acceptance_reclaim"]
        lines.append(
            "| {cohort} | {profile}m | {signals} | {acceptance} | {closed} | {avg} | {win} |".format(
                cohort=item["cohort"],
                profile=item["profile_minutes"],
                signals=item["analysis"]["funnel"]["signals"],
                acceptance=item["analysis"]["funnel"]["acceptance"],
                closed=reclaim["n_closed"],
                avg=pct(reclaim["avg_net_return_pct"]),
                win=(
                    "n/a"
                    if reclaim["win_rate"] is None
                    else f"{reclaim['win_rate']:.1%}"
                ),
            )
        )
    lines.extend(
        [
            "",
            "## 读法",
            "",
            "- 如果某个周期只在单点取得最好结果，而相邻周期明显变差，通常更像样本噪声或过拟合；连续一段周期都改善，才值得继续验证。",
            "- 该报告只改变入场观察周期，退出规则没有跟着调参；Sell Absorption 仍是成交额卖压占比 + VAH守住的代理，不是盘口真吸收。",
            "- 详细逐笔结果仍见主报告目录下的 `same_exit_comparison_trades.csv`；本报告的 JSON 保存每个周期的完整指标。",
        ]
    )
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--signals", type=Path, required=True)
    parser.add_argument("--states", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    signals = load_signals(args.signals)
    states = load_states(args.states)
    candle_cache = {
        symbol: closed_candles(series) for symbol, series in states.items()
    }
    exit_config = SameExitConfig()
    results: list[dict[str, Any]] = []
    epochs = [signal["detected_at"] for signal in signals]
    for dimension, setting, overrides in sensitivity_specs():
        params = {**BASE_PARAMS, **overrides}
        analysis = analyze_configuration(
            signals,
            states,
            params,
            exit_config,
            candle_cache,
        )
        results.append(
            {
                "dimension": dimension,
                "setting": setting,
                "params": params,
                "analysis": analysis,
            }
        )

    split_results: list[dict[str, Any]] = []
    if epochs:
        split_at = min(epochs) + 24.0 * 60.0 * 60.0
        cohorts = (
            ("前24小时", [signal for signal in signals if signal["detected_at"] < split_at]),
            ("后24小时", [signal for signal in signals if signal["detected_at"] >= split_at]),
        )
        for cohort, cohort_signals in cohorts:
            for profile_minutes in (60, 90, 120, 135, 150, 180):
                params = {**BASE_PARAMS, "profile_minutes": profile_minutes}
                analysis = analyze_configuration(
                    cohort_signals,
                    states,
                    params,
                    exit_config,
                    candle_cache,
                )
                split_results.append(
                    {
                        "cohort": cohort,
                        "profile_minutes": profile_minutes,
                        "analysis": analysis,
                    }
                )

    report = {
        "window": {
            "start": format_time(min(epochs)) if epochs else None,
            "end": format_time(max(epochs)) if epochs else None,
        },
        "counts": {
            "signals": len(signals),
            "state_symbols": len(states),
            "state_rows": sum(len(series.rows) for series in states.values()),
        },
        "exit_config": {
            "exit_mode": "candle_15m",
            "candle_confirmation_count": 1,
            "grace_bars": exit_config.grace_bars,
            "decision_profit_pct": exit_config.decision_profit_pct,
            "recovery_profit_pct": exit_config.recovery_profit_pct,
            "fee_rate": exit_config.fee_rate,
            "notional_usdt_per_trade": exit_config.notional_usdt,
        },
        "results": results,
        "split_results": split_results,
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "cycle_sensitivity_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    )
    (args.output_dir / "cycle_sensitivity_report.md").write_text(
        render_markdown(report)
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
