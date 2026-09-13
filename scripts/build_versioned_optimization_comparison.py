#!/usr/bin/env python3
"""Build a versioned optimization comparison page without replacing prior HTML."""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from argparse import Namespace
from bisect import bisect_right
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import optimize_local_live_constrained as optimizer  # noqa: E402
import replay_frozen_local_live_strategies as frozen  # noqa: E402

SCENARIOS = ("B", "C", "D", "E", "F", "G")
SCENARIO_FAMILIES = {
    "B": "B/C",
    "C": "B/C",
    "D": "D/G",
    "E": "E/F",
    "F": "E/F",
    "G": "D/G",
}
SCENARIO_TITLES = {
    "B": "B · 350U / 自由 cooldown",
    "C": "C · 350U / cooldown = 0",
    "D": "D · 280U / 自由 cooldown",
    "E": "E · 280U / cooldown = 0",
    "F": "F · 回撤惩罚 / cooldown = 0",
    "G": "G · 回撤惩罚 / 280U",
}
ACCOUNT_CONFIGS = {
    "primary": {
        "title": "实盘 Primary",
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.50,
        "min_imbalance": 0.60,
        "min_intensity": 2.0,
        "cooldown_buckets": 0,
    },
    "acc02": {
        "title": "实盘 acc02 / account-2",
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.50,
        "min_imbalance": 0.60,
        "min_intensity": 2.0,
        "cooldown_buckets": 0,
    },
    "acc03": {
        "title": "实盘 acc03 / account-3",
        "impulse_window_buckets": 3,
        "confirmation_buckets": 1,
        "min_return_pct": 1.50,
        "min_imbalance": 0.30,
        "min_intensity": 2.0,
        "cooldown_buckets": 0,
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--template",
        type=Path,
        default=Path("reports/optimization-comparison.html"),
    )
    parser.add_argument(
        "--previous-root",
        type=Path,
        default=Path("server_exports/cml-research-data-20260906-234620"),
    )
    parser.add_argument(
        "--current-root",
        type=Path,
        default=Path("server_exports/cml-research-data-20260907-224442"),
    )
    parser.add_argument(
        "--frozen-root",
        type=Path,
        default=Path(
            "server_exports/cml-research-data-20260907-224442/"
            "frozen-replay-from-20260906-234620-20260907-224442"
        ),
    )
    parser.add_argument(
        "--fixed-report-source",
        choices=("frozen", "previous"),
        default="frozen",
        help="use a frozen replay tree or the previous reports for the fixed column",
    )
    parser.add_argument(
        "--previous-report-suffix",
        default="20260906-234620",
        help="Suffix used by the previous optimization report directories.",
    )
    parser.add_argument(
        "--current-report-suffix",
        default="20260907-224442",
        help="Suffix used by the current optimization report directories.",
    )
    parser.add_argument(
        "--history-href",
        default="optimization-comparison.html",
        help="Href for the oldest preserved report page.",
    )
    parser.add_argument(
        "--history-version-date",
        default="2026-09-06",
        help="Display date for the oldest preserved report page.",
    )
    parser.add_argument(
        "--previous-version-href",
        default="optimization-comparison-20260907.html",
        help="Href for the immediately previous report page.",
    )
    parser.add_argument(
        "--previous-version-date",
        default="2026-09-07",
        help="Display date for the immediately previous report page.",
    )
    parser.add_argument(
        "--current-version-date",
        default="2026-09-07",
        help="Display date for the report page being generated.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("reports/optimization-comparison-20260907.html"),
    )
    return parser.parse_args()


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def report_path(root: Path, label: str, suffix: str) -> Path:
    return root / f"optimization-full-pnl-current-{label}-{suffix}" / "optimization_report.json"


def previous_report_path(root: Path, label: str, suffix: str) -> Path:
    return root / f"optimization-full-pnl-current-{label}-{suffix}" / "optimization_report.json"


def event_path_from_report(report_path_value: Path) -> Path:
    return report_path_value.parent / "best_candidate_events.csv"


def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def bool_value(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes"}


def float_value(value: Any) -> float:
    return float(value)


def config_text(config: dict[str, Any]) -> str:
    parts = [
        f"{int(config['impulse_window_buckets'])} / "
        f"{int(config['confirmation_buckets'])} / "
        f"{float(config['min_return_pct']):.2f}% / "
        f"{float(config['min_imbalance']):.2f} / "
        f"{float(config['min_intensity']):.1f}"
    ]
    if "min_notional_5m_vs_30m" in config:
        parts.append(f"{float(config['min_notional_5m_vs_30m']):.2f}x")
    parts.append(f"{int(config['cooldown_buckets'])}")
    return " / ".join(parts)


def load_event_rows(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def curve_from_event_rows(
    rows: list[dict[str, Any]],
    *,
    start: datetime,
    end: datetime,
) -> dict[str, list[Any]]:
    """Build step curves from entry/exit events, anchored at the optimization start."""

    timeline: dict[datetime, dict[str, float]] = {
        start: {"entries": 0.0, "exits": 0.0, "pnl": 0.0}
    }
    active_before_start = 0
    for row in rows:
        entry_at = parse_time(row.get("entry_at"))
        exit_at = parse_time(row.get("exit_at")) if bool_value(row.get("closed")) else None
        pnl = float_value(row.get("net_pnl_usdt") or 0.0)

        if entry_at is not None:
            if entry_at < start:
                active_before_start += 1
            elif start <= entry_at < end:
                timeline.setdefault(entry_at, {"entries": 0.0, "exits": 0.0, "pnl": 0.0})[
                    "entries"
                ] += 1.0
        if exit_at is not None:
            if exit_at < start:
                active_before_start -= 1
            elif start <= exit_at < end:
                timeline.setdefault(exit_at, {"entries": 0.0, "exits": 0.0, "pnl": 0.0})[
                    "exits"
                ] += 1.0
                timeline[exit_at]["pnl"] += pnl

    active = max(active_before_start, 0)
    cumulative = 0.0
    peak = 0.0
    timestamps: list[str] = []
    equity: list[float] = []
    drawdown: list[float] = []
    margin: list[float] = []
    for timestamp in sorted(timeline):
        change = timeline[timestamp]
        active -= int(change["exits"])
        active += int(change["entries"])
        cumulative += change["pnl"]
        peak = max(peak, cumulative)
        timestamps.append(timestamp.isoformat().replace("+00:00", "Z"))
        equity.append(round(cumulative, 8))
        drawdown.append(round(peak - cumulative, 8))
        margin.append(round(max(active, 0) * 20.0, 8))

    if not timestamps or timestamps[-1] != end.isoformat().replace("+00:00", "Z"):
        timestamps.append(end.isoformat().replace("+00:00", "Z"))
        equity.append(round(cumulative, 8))
        drawdown.append(round(peak - cumulative, 8))
        margin.append(round(max(active, 0) * 20.0, 8))
    return {"timestamps": timestamps, "equity": equity, "drawdown": drawdown, "margin": margin}


def align_curves(
    old: dict[str, list[Any]],
    best: dict[str, list[Any]],
) -> dict[str, Any]:
    timestamps = sorted(set(old["timestamps"]) | set(best["timestamps"]))

    def step_values(curve: dict[str, list[Any]], metric: str) -> list[float]:
        curve_times = curve["timestamps"]
        values = curve[metric]
        result: list[float] = []
        for timestamp in timestamps:
            index = bisect_right(curve_times, timestamp) - 1
            result.append(float(values[max(index, 0)]))
        return result

    return {
        "timestamps": timestamps,
        "old": {metric: step_values(old, metric) for metric in ("equity", "drawdown", "margin")},
        "best": {metric: step_values(best, metric) for metric in ("equity", "drawdown", "margin")},
    }


def metric_summary(block: dict[str, Any]) -> dict[str, float | int]:
    full = block["best_validation"]["metrics"]["full"]
    return {
        "pnl": float(full["net_pnl_usdt"]),
        "dd": float(full["max_drawdown_usdt"]),
        "closed": int(full["n_closed"]),
        "margin": float(block["best_validation"]["natural_initial_margin_peak_usdt"]),
    }


def current_replay_context(current_root: Path) -> tuple[Any, ...]:
    # prepare_replay needs the impulse/confirmation pairs present in STRATEGIES.
    for profile in frozen.STRATEGIES.values():
        profile["config"] = {"impulse_window_buckets": 2, "confirmation_buckets": 1}
    args = Namespace(
        input_root=current_root / "parquet",
        environment="research",
        top_count=10,
        fee_rate=0.0005,
        live_signals=current_root / "missing-live-signals.csv.gz",
    )
    return frozen.prepare_replay(args)


def explicit_account_replay(
    config: dict[str, Any],
    *,
    pools: dict[tuple[int, int], list[Any]],
    simulated: dict[int, Any],
    splits: tuple[Any, ...],
    optimization_start: datetime,
    full_end: datetime,
) -> tuple[dict[str, Any], dict[str, Any]]:
    selected = frozen.selected_for_config(config, pools)
    report = frozen.report_for(
        config=config,
        selected=selected,
        simulated=simulated,
        splits=splits,
        optimization_start=optimization_start,
        full_end=full_end,
        cap=None,
        drawdown_weight=0.0,
    )
    rows = [optimizer.event_csv_row(item, simulated[id(item)]) for item in selected]
    return report, curve_from_event_rows(rows, start=optimization_start, end=full_end)


def build_payloads(args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    previous_reports = {
        label: read_json(previous_report_path(args.previous_root, label, args.previous_report_suffix))
        for label in SCENARIOS
    }
    current_reports = {
        label: read_json(report_path(args.current_root, label, args.current_report_suffix))
        for label in SCENARIOS
    }
    frozen_reports = (
        previous_reports
        if args.fixed_report_source == "previous"
        else {
            label: read_json(
                args.frozen_root / f"replay-{label}" / "optimization_report.json"
            )
            for label in SCENARIOS
        }
    )

    _, pools, simulated, splits, _full_start, full_end, optimization_start = current_replay_context(
        args.current_root
    )
    account_reports: dict[str, dict[str, Any]] = {}
    account_curves: dict[str, dict[str, Any]] = {}
    for name, config in ACCOUNT_CONFIGS.items():
        report, curve = explicit_account_replay(
            config,
            pools=pools,
            simulated=simulated,
            splits=splits,
            optimization_start=optimization_start,
            full_end=full_end,
        )
        account_reports[name] = report
        account_curves[name] = curve

    data: dict[str, Any] = {}
    curve_data: dict[str, Any] = {}
    for label in SCENARIOS:
        previous = previous_reports[label]
        current = current_reports[label]
        fixed = frozen_reports[label]
        old_config = previous["best_validation"]["config"]
        current_config = current["best_validation"]["config"]
        old_summary = metric_summary(previous)
        fixed_summary = metric_summary(fixed)
        best_summary = metric_summary(current)
        data[label] = {
            "family": SCENARIO_FAMILIES[label],
            "title": SCENARIO_TITLES[label],
            "cap": current["margin_constraint"].get("max_initial_margin_usdt") or 350,
            "old": {**old_summary, "config": config_text(old_config)},
            "fixed": {**fixed_summary},
            "best": {
                **best_summary,
                "score": float(current["best_validation"]["selection_score"]),
                "config": config_text(current_config),
            },
        }

        previous_events = load_event_rows(
            event_path_from_report(
                previous_report_path(args.previous_root, label, args.previous_report_suffix)
            )
        )
        current_events = load_event_rows(
            event_path_from_report(report_path(args.current_root, label, args.current_report_suffix))
        )
        old_curve = curve_from_event_rows(
            previous_events,
            start=optimization_start,
            end=parse_time(previous["data_end"]) or optimization_start,
        )
        best_curve = curve_from_event_rows(
            current_events,
            start=optimization_start,
            end=parse_time(current["data_end"]) or optimization_start,
        )
        curve_data[label] = align_curves(old_curve, best_curve)

    account_payload: dict[str, Any] = {}
    for name, report in account_reports.items():
        metrics = report["metrics"]["full"]
        account_payload[name] = {
            "title": ACCOUNT_CONFIGS[name]["title"],
            "pnl": float(metrics["net_pnl_usdt"]),
            "dd": float(metrics["max_drawdown_usdt"]),
            "margin": float(report["natural_initial_margin_peak_usdt"]),
            "config": config_text(ACCOUNT_CONFIGS[name]),
        }
    return data, curve_data, {"accounts": account_payload, "curves": account_curves}


def replace_once(text: str, pattern: str, replacement: str) -> str:
    updated, count = re.subn(pattern, replacement, text, count=1, flags=re.DOTALL)
    if count != 1:
        raise SystemExit(f"template replacement failed: {pattern}")
    return updated


def format_window(value: str) -> str:
    timestamp = parse_time(value)
    if timestamp is None:
        return "—"
    return timestamp.strftime("%m-%d %H:%M UTC")


def duration_text(start_value: str, end_value: str) -> str:
    start = parse_time(start_value)
    end = parse_time(end_value)
    if start is None or end is None:
        return "—"
    total_minutes = round((end - start).total_seconds() / 60)
    return f"{total_minutes // 60}h {total_minutes % 60:02d}m"


def version_nav_markup(args: argparse.Namespace) -> str:
    return (
        '<nav class="version-nav" aria-label="报告版本">\n'
        '      <span class="version-nav-label">报告版本</span>\n'
        f'      <a href="{args.history_href}">历史版 · {args.history_version_date}</a>\n'
        f'      <a href="{args.previous_version_href}">上一轮 · {args.previous_version_date}</a>\n'
        f'      <a href="{args.output.name}" aria-current="page">本轮 · {args.current_version_date}</a>\n'
        '      <a href="optimization-comparison-index.html">版本索引</a>\n'
        '    </nav>'
    )


def build_html(args: argparse.Namespace) -> str:
    data, curve_data, accounts = build_payloads(args)
    template = args.template.read_text(encoding="utf-8")
    previous_b = read_json(
        previous_report_path(args.previous_root, "B", args.previous_report_suffix)
    )
    current_b = read_json(report_path(args.current_root, "B", args.current_report_suffix))

    rendered = template
    # The filter is optional so older, unfiltered report pages keep their
    # original wording when this generator is reused.
    current_exclusion = current_b.get("entry_time_exclusion")
    is_volume_joint = "min_notional_5m_vs_30m" in current_b["best_validation"]["config"]
    rendered = replace_once(
        rendered,
        r"LOCAL RESEARCH REPLAY / \d{4}-\d{2}-\d{2}",
        f"LOCAL RESEARCH REPLAY / {args.current_version_date}",
    )
    rendered = rendered.replace(
        "<h1>加一天数据，收益真的变高了吗？</h1>",
        (
            "<h1>加入持续放量后，七维联合结果更好吗？</h1>"
            if is_volume_joint
            else "<h1>排除早盘开仓后，参数还稳定吗？</h1>"
            if current_exclusion
            else "<h1>再加一段数据，历史推荐还稳定吗？</h1>"
        ),
        1,
    )
    rendered = replace_once(
        rendered,
        r'<p class="dek">.*?</p>',
        (
            '<p class="dek">上一轮六维参数作为基线，在同一完整窗口加入持续放量的第七维后重新联合寻优。收益、回撤和保证金占用统一用同一套研究回放口径。</p>'
            if is_volume_joint
            else '<p class="dek">保留上一轮页面不变，在新页面里把历史最优参数原样延伸到最新数据，再与本轮完整窗口重新寻优结果并排比较。收益、回撤和保证金占用统一用同一套研究回放口径。</p>'
        ),
    )
    if is_volume_joint:
        for old_text, new_text in {
            "上一轮参数固定回放与本轮网格使用同一套本地研究数据模拟": "上一轮六维参数与本轮七维网格使用同一套本地研究数据模拟",
            "新增尾段贡献为“旧参数新窗口 − 上一轮结果”": "七维增量为“本轮七维结果 − 上一轮六维结果”",
            "旧参数 · 新窗口": "上一轮六维参数",
            "新增尾段 ${fmt(fixedDelta)}": "相对上一轮 ${fmt(fixedDelta)}",
            "旧参数 + 新窗口": "上一轮六维参数",
            "新增尾段净贡献": "七维联合收益增量",
            "固定旧参数": "上一轮六维参数",
            "固定回放": "上一轮六维",
            "旧参数 + 新数据": "上一轮六维",
            "尾段贡献": "七维增量",
            "新增尾段仍然使旧参数收益下降。": "七维结果与上一轮六维结果的差异见上方对比。",
            "固定旧参数在新增尾段全部下降": "上一轮六维参数与本轮七维联合结果的差异见下方",
        }.items():
            rendered = rendered.replace(old_text, new_text)
    rendered = replace_once(
        rendered,
        r'<div class="window-note">.*?</div>\n    </header>',
        (
            '<div class="window-note">\n'
            '        <div class="section-label">样本窗口</div>\n'
            f'        <p><strong>上一轮</strong><br><span class="mono">{format_window(previous_b["optimization_window"]["start"])} → {format_window(previous_b["data_end"])}</span></p>\n'
            f'        <p style="margin-top:10px"><strong>本轮</strong><br><span class="mono">{format_window(current_b["optimization_window"]["start"])} → {format_window(current_b["data_end"])}</span></p>\n'
            + (
                '        <p style="margin-top:10px;color:var(--coral)">本轮变化：同一完整窗口加入第七维持续放量阈值，重新联合寻优</p>\n'
                if "min_notional_5m_vs_30m" in current_b["best_validation"]["config"]
                else f'        <p style="margin-top:10px;color:var(--coral)">新增尾段：约 <span class="mono">{duration_text(previous_b["data_end"], current_b["data_end"])}</span></p>\n'
            )
            + (
                f'        <p style="margin-top:10px;color:var(--teal)">开仓过滤：实际成交 <span class="mono">{current_exclusion["window"]}</span>（{current_exclusion["timezone"]}，左闭右开）</p>\n'
                if current_exclusion
                else ""
            )
            + (
                '        <p style="margin-top:10px;color:var(--teal)">第七维：最近 5 分钟成交额 ÷ 前 30 分钟等时长均量，阈值在网格中联合寻优</p>\n'
                if "min_notional_5m_vs_30m" in current_b["best_validation"]["config"]
                else ""
            )
            + '      </div>\n'
            '    </header>'
        ),
    )
    rendered = replace_once(
        rendered,
        r'<nav class="version-nav" aria-label="报告版本">.*?</nav>',
        version_nav_markup(args),
    )

    rendered = replace_once(
        rendered,
        r"    const DATA = \{.*?\n    \};\n\n\n    const CURVE_DATA",
        f"    const DATA = {json.dumps(data, ensure_ascii=False, separators=(',', ':'))};\n\n\n    const CURVE_DATA",
    )
    rendered = replace_once(
        rendered,
        r"    const CURVE_DATA = .*?;\n    const PRIMARY",
        f"    const CURVE_DATA = {json.dumps(curve_data, ensure_ascii=False, separators=(',', ':'))};\n    const PRIMARY",
    )

    primary = accounts["accounts"]["primary"]
    acc02 = accounts["accounts"]["acc02"]
    acc03 = accounts["accounts"]["acc03"]
    rendered = replace_once(
        rendered,
        r"    const PRIMARY = \{.*?\n    \};\n    const LIVE_ACC02",
        f"    const PRIMARY = {json.dumps(primary, ensure_ascii=False, separators=(',', ':'))};\n    const LIVE_ACC02",
    )
    rendered = replace_once(
        rendered,
        r"    const LIVE_ACC02 = \{.*?\n    \};\n    const LIVE_ACC03",
        f"    const LIVE_ACC02 = {json.dumps(acc02, ensure_ascii=False, separators=(',', ':'))};\n    const LIVE_ACC03",
    )
    rendered = replace_once(
        rendered,
        r"    const LIVE_ACC03 = \{.*?\n    \};\n    // The E/F",
        f"    const LIVE_ACC03 = {json.dumps(acc03, ensure_ascii=False, separators=(',', ':'))};\n    // The E/F",
    )
    rendered = replace_once(
        rendered,
        r"    const LIVE_ACCOUNT_CURVES = \{.*?\n    \};",
        f"    const LIVE_ACCOUNT_CURVES = {json.dumps(accounts['curves'], ensure_ascii=False, separators=(',', ':'))};",
    )
    return rendered


def main() -> None:
    args = parse_args()
    rendered = build_html(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rendered, encoding="utf-8")
    print(args.output)


if __name__ == "__main__":
    main()
