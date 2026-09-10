#!/usr/bin/env python3
# ruff: noqa: E501
"""Build a versioned HTML report for the latest full seven-dimensional search."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))


PROFILE_ORDER = tuple("ABCDEFG")
PROFILE_CAPS: dict[str, float | None] = {
    "A": None,
    "B": 350.0,
    "C": 350.0,
    "D": 280.0,
    "E": 280.0,
    "F": 280.0,
    "G": 280.0,
}
ACCOUNT_CONFIGS: dict[str, dict[str, Any]] = {
    "primary": {
        "title": "实盘 Primary",
        "label": "primary",
        "impulse_window_buckets": 4,
        "confirmation_buckets": 1,
        "min_return_pct": 0.50,
        "min_imbalance": 0.30,
        "min_intensity": 1.5,
        "min_volume_ratio": 1.50,
        "cooldown_buckets": 0,
        "color": "#315776",
    },
    "acc01": {
        "title": "实盘 acc01 / account-2",
        "label": "account-2",
        "impulse_window_buckets": 4,
        "confirmation_buckets": 1,
        "min_return_pct": 0.50,
        "min_imbalance": 0.30,
        "min_intensity": 1.5,
        "min_volume_ratio": 1.50,
        "cooldown_buckets": 0,
        "color": "#a25555",
    },
    "acc02": {
        "title": "实盘 acc02 / account-3",
        "label": "account-3",
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.50,
        "min_imbalance": 0.30,
        "min_intensity": 4.0,
        "min_volume_ratio": 1.50,
        "cooldown_buckets": 0,
        "color": "#6d63a3",
    },
    "acc03": {
        "title": "实盘 acc03 / account-4",
        "label": "account-4",
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.50,
        "min_imbalance": 0.30,
        "min_intensity": 4.0,
        "min_volume_ratio": 1.50,
        "cooldown_buckets": 0,
        "color": "#b77a1c",
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--optimization-dir",
        type=Path,
        default=Path(
            "server_exports/cml-research-data-20260910-001859/"
            "optimization-volume-feature-7d-20260910/notional_5m_vs_30m-v2"
        ),
    )
    parser.add_argument(
        "--input-root",
        type=Path,
        default=Path("server_exports/cml-research-data-20260910-001859/parquet"),
    )
    parser.add_argument(
        "--live-signals",
        type=Path,
        default=Path(
            "server_exports/cml-live-current-20260910-001859/"
            "live_strategy_signals.csv.gz"
        ),
    )
    parser.add_argument(
        "--template",
        type=Path,
        default=Path("reports/optimization-comparison-20260909-exclude-0800-1000-volume7.html"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("reports/optimization-comparison-20260910-exclude-0800-1000-volume7.html"),
    )
    parser.add_argument("--workers", type=int, default=8)
    return parser.parse_args()


def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def number(value: Any, default: float = 0.0) -> float:
    if value in (None, "", "None", "null"):
        return default
    return float(value)


def bool_value(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "t"}


def config_text(config: dict[str, Any]) -> str:
    parts = [
        f"{int(config['impulse_window_buckets'])}",
        f"{int(config['confirmation_buckets'])}",
        f"{float(config['min_return_pct']):.2f}%",
        f"{float(config['min_imbalance']):.2f}",
        f"{float(config['min_intensity']):.1f}",
    ]
    if "min_volume_ratio" in config:
        parts.append(f"{float(config['min_volume_ratio']):.2f}x")
    parts.append(f"{int(config['cooldown_buckets'])}")
    return " / ".join(parts)


def profile_metrics(block: dict[str, Any]) -> dict[str, Any]:
    metrics = block["metrics"]
    full = metrics["full"]
    validation = metrics["validation"]
    holdout = metrics["holdout"]
    return {
        "full_pnl": number(full["net_pnl_usdt"]),
        "full_dd": number(full["max_drawdown_usdt"]),
        "full_closed": int(full["n_closed"]),
        "full_selected": int(full["n_selected"]),
        "validation_pnl": number(validation["net_pnl_usdt"]),
        "holdout_pnl": number(holdout["net_pnl_usdt"]),
        "win_rate": number(full.get("win_rate_pct")),
        "profit_factor": number(full.get("profit_factor")),
        "margin": number(block["natural_initial_margin_peak_usdt"]),
        "score": number(block.get("selection_score")),
    }


def add_event(
    timeline: dict[datetime, list[float]],
    timestamp: datetime,
    *,
    entries: float = 0.0,
    exits: float = 0.0,
    pnl: float = 0.0,
) -> None:
    change = timeline.setdefault(timestamp, [0.0, 0.0, 0.0])
    change[0] += entries
    change[1] += exits
    change[2] += pnl


def finalize_curve(
    timeline: dict[datetime, list[float]],
    *,
    start: datetime,
    end: datetime,
) -> dict[str, list[Any]]:
    active = 0
    cumulative = 0.0
    peak = 0.0
    timestamps: list[str] = []
    equity: list[float] = []
    drawdown: list[float] = []
    margin: list[float] = []
    for timestamp in sorted(timeline):
        entries, exits, pnl = timeline[timestamp]
        active -= int(exits)
        active += int(entries)
        cumulative += pnl
        peak = max(peak, cumulative)
        timestamps.append(iso(timestamp))
        equity.append(round(cumulative, 8))
        drawdown.append(round(peak - cumulative, 8))
        margin.append(round(max(active, 0) * 20.0, 8))
    end_iso = iso(end)
    if not timestamps or timestamps[-1] != end_iso:
        timestamps.append(end_iso)
        equity.append(round(cumulative, 8))
        drawdown.append(round(peak - cumulative, 8))
        margin.append(round(max(active, 0) * 20.0, 8))
    return {
        "timestamps": timestamps,
        "equity": equity,
        "drawdown": drawdown,
        "margin": margin,
    }


def curve_from_csv(path: Path, *, start: datetime, end: datetime) -> dict[str, list[Any]]:
    timeline: dict[datetime, list[float]] = {start: [0.0, 0.0, 0.0]}
    active_before_start = 0
    with path.open(encoding="utf-8", newline="") as handle:
        rows = csv.DictReader(handle)
        for row in rows:
            entry_at = parse_time(row.get("entry_at"))
            exit_at = parse_time(row.get("exit_at")) if bool_value(row.get("closed")) else None
            pnl = number(row.get("net_pnl_usdt"))
            if entry_at is not None:
                if entry_at < start:
                    active_before_start += 1
                elif start <= entry_at < end:
                    add_event(timeline, entry_at, entries=1.0)
            if exit_at is not None:
                if exit_at < start:
                    active_before_start -= 1
                elif start <= exit_at < end:
                    add_event(timeline, exit_at, exits=1.0, pnl=pnl)
    if active_before_start:
        timeline[start][0] += float(active_before_start)
    return finalize_curve(timeline, start=start, end=end)


def curve_from_selected(
    selected: list[Any],
    simulated: dict[int, Any],
    *,
    start: datetime,
    end: datetime,
) -> dict[str, list[Any]]:
    timeline: dict[datetime, list[float]] = {start: [0.0, 0.0, 0.0]}
    active_before_start = 0
    for observation in selected:
        trade = simulated[id(observation)].trade
        if not trade or trade.get("entry_epoch") is None:
            continue
        entry_at = datetime.fromtimestamp(float(trade["entry_epoch"]), tz=UTC)
        exit_at = (
            datetime.fromtimestamp(float(trade["exit_epoch"]), tz=UTC)
            if trade.get("closed") and trade.get("exit_epoch") is not None
            else None
        )
        pnl = number(trade.get("net_pnl_usdt"))
        if entry_at < start:
            active_before_start += 1
        elif entry_at < end:
            add_event(timeline, entry_at, entries=1.0)
        if exit_at is not None:
            if exit_at < start:
                active_before_start -= 1
            elif exit_at < end:
                add_event(timeline, exit_at, exits=1.0, pnl=pnl)
    if active_before_start:
        timeline[start][0] += float(active_before_start)
    return finalize_curve(timeline, start=start, end=end)


def curve_summary(curve: dict[str, list[Any]]) -> dict[str, Any]:
    return {
        "pnl": round(float(curve["equity"][-1]), 8),
        "dd": round(max(map(float, curve["drawdown"])), 8),
        "margin": round(max(map(float, curve["margin"])), 8),
    }


def display_window(value: str) -> str:
    parsed = parse_time(value)
    return "—" if parsed is None else parsed.strftime("%m-%d %H:%M UTC")


def build_payload(args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, Any]]:
    report = json.loads((args.optimization_dir / "optimization_report.json").read_text(encoding="utf-8"))
    account_replay_path = args.optimization_dir / "account_replay_report.json"
    if not account_replay_path.exists():
        raise SystemExit(
            "account replay report is required; run scripts/replay_account_configs_fast.py first"
        )
    account_replay = json.loads(account_replay_path.read_text(encoding="utf-8"))
    # Keep the chart window separate from the scoring window.  The optimizer
    # starts at the first complete UTC day because the local Top10 proxy needs
    # a full prior day, but the report still owns raw states from data_start.
    # Hiding that warm-up interval made a full-data report look truncated.
    data_start = parse_time(report["data_start"])
    optimization_start = parse_time(report["optimization_window"]["start"])
    end = parse_time(report["data_end"])
    if data_start is None or optimization_start is None or end is None:
        raise SystemExit("optimization report has no valid data or optimization window")
    profiles: dict[str, Any] = {}
    curves: dict[str, Any] = {}
    baseline_curve = curve_from_csv(
        args.optimization_dir / "baseline_events.csv", start=data_start, end=end
    )
    curves["baseline"] = baseline_curve
    baseline_block = report["baseline"]
    baseline_summary = {
        "title": "六维基线（放量关闭）",
        "config": config_text(baseline_block["config"]),
        **profile_metrics(baseline_block),
        **curve_summary(baseline_curve),
        "cap": None,
    }
    for name in PROFILE_ORDER:
        block = report["profiles"][name]
        curve = curve_from_csv(
            args.optimization_dir / f"profile_{name}_events.csv", start=data_start, end=end
        )
        curves[name] = curve
        profiles[name] = {
            "label": block["label"],
            "config": config_text(block["config"]),
            "config_raw": block["config"],
            "cap": PROFILE_CAPS[name],
            "feasible": bool(block["margin_constraint_feasible"]),
            **profile_metrics(block),
            "curve": curve_summary(curve),
        }

    account_summaries: dict[str, Any] = {}
    for name, config in ACCOUNT_CONFIGS.items():
        replay_block = account_replay["accounts"].get(name)
        if replay_block is None:
            raise SystemExit(f"account replay report is missing {name}")
        event_path = args.optimization_dir / replay_block["event_file"]
        if not event_path.exists():
            raise SystemExit(f"account replay event file is missing: {event_path}")
        curve = curve_from_csv(event_path, start=data_start, end=end)
        curves[name] = curve
        full = replay_block["metrics"]["full"]
        account_summaries[name] = {
            "title": config["title"],
            "config": config_text(config),
            "pnl": number(full["net_pnl_usdt"]),
            "dd": number(full["max_drawdown_usdt"]),
            "margin": number(replay_block["natural_initial_margin_peak_usdt"]),
            "closed": int(full["n_closed"]),
            "selected": int(full["n_selected"]),
            "curve": curve_summary(curve),
        }
    payload = {
        "meta": {
            "data_start": report["data_start"],
            "data_end": report["data_end"],
            "optimization_start": report["optimization_window"]["start"],
            "curve_start": report["data_start"],
            "states": int(report["load"]["usable_states"]),
            "source_rows": int(report["load"]["source_rows"]),
            "skipped": int(report["load"]["skipped_incomplete_or_unpriced"]),
            "symbols": int(report["symbols"]),
            "segments": int(report["contiguous_segments"]),
            "candidate_count": int(report["parameter_grid"]["candidate_count"]),
            "workers": int(args.workers),
            "feature": "最近 5 分钟平均成交额 ÷ 前 30 分钟平均成交额",
            "feature_definition": report["volume_windows"]["ratio_definition"],
            "exclusion": report["entry_time_exclusion"],
            "splits": report["splits"],
            "selection_objective": report["selection_objective"],
            "drawdown_weight": report["drawdown_weight"],
        },
        "baseline": baseline_summary,
        "profiles": profiles,
        "accounts": account_summaries,
    }
    return payload, curves


CUSTOM_STYLE = r"""
    <style>
      .latest-summary { margin: 18px 0 24px; }
      .recommend-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; margin-top: 16px; }
      .recommend-card { padding: 18px; border: 1px solid var(--line); border-top: 3px solid var(--teal); background: rgba(255, 252, 246, .7); }
      .recommend-card.caution { border-top-color: var(--amber); }
      .recommend-card h4 { margin: 7px 0 5px; font-size: 17px; letter-spacing: -.02em; }
      .recommend-card .headline { color: var(--teal); font: 500 24px var(--mono); }
      .recommend-card.caution .headline { color: var(--amber); }
      .recommend-card p { color: var(--muted); font-size: 12px; line-height: 1.65; }
      .recommend-card code { margin: 12px 0; }
      .latest-grid { display: grid; grid-template-columns: minmax(0, 1.45fr) minmax(310px, .55fr); gap: 18px; align-items: start; margin-top: 18px; }
      .profile-table { min-width: 1050px; }
      .profile-table th, .profile-table td { white-space: nowrap; }
      .profile-table .selected-row { background: rgba(25, 123, 105, .08); }
      .profile-table .recommend-row { background: rgba(183, 122, 28, .08); }
      .profile-table td.config { text-align: left; color: var(--muted); }
      .profile-badge { display: inline-block; margin-right: 6px; padding: 2px 5px; border-radius: 999px; background: var(--ink); color: var(--paper); font: 500 10px var(--mono); }
      .profile-badge.risk { background: var(--teal); }
      .account-grid { display: grid; grid-template-columns: 1fr; gap: 10px; margin-top: 16px; }
      .account-card { padding: 12px; border-left: 3px solid var(--line-strong); background: rgba(255, 252, 246, .55); }
      .account-card h4 { margin: 0 0 6px; font-size: 13px; }
      .account-card code { margin-top: 7px; font-size: 11px; }
      .account-stat { display: flex; justify-content: space-between; gap: 10px; color: var(--muted); font: 400 11px var(--mono); }
      .latest-toolbar { display: flex; flex-wrap: wrap; gap: 8px; margin: 18px 0 4px; }
      .latest-toolbar button { border: 1px solid var(--line-strong); border-radius: 999px; padding: 8px 12px; background: transparent; color: var(--muted); cursor: pointer; font: 500 11px var(--mono); }
      .latest-toolbar button:hover, .latest-toolbar button:focus-visible { color: var(--ink); border-color: var(--ink); outline: none; }
      .latest-toolbar button[aria-pressed="true"] { color: var(--paper); background: var(--ink); border-color: var(--ink); }
      .latest-bars { margin-top: 20px; }
      .latest-bar-row { display: grid; grid-template-columns: 128px minmax(0, 1fr) 90px; gap: 12px; align-items: center; margin: 13px 0; }
      .latest-bar-name { color: var(--muted); font-size: 12px; }
      .latest-bar-track { height: 18px; position: relative; overflow: hidden; background: var(--paper-deep); }
      .latest-bar-zero { position: absolute; top: -4px; width: 1px; height: 26px; background: var(--line-strong); z-index: 1; }
      .latest-bar-fill { position: absolute; top: 0; height: 100%; min-width: 2px; }
      .latest-bar-number { text-align: right; white-space: nowrap; font: 500 12px var(--mono); }
      .latest-legend { display: flex; flex-wrap: wrap; gap: 13px; margin: 20px 0 0 140px; color: var(--muted); font-size: 11px; }
      .latest-legend span { display: inline-flex; align-items: center; gap: 6px; }
      .latest-legend i { display: inline-block; width: 9px; height: 9px; }
      .latest-chart { display: block; width: 100%; height: auto; margin-top: 12px; overflow: visible; }
      .latest-chart .grid-line { stroke: var(--curve-grid); stroke-width: 1; }
      .latest-chart .axis-line { stroke: var(--line-strong); stroke-width: 1; }
      .latest-chart text { fill: var(--muted); font: 400 10px var(--mono); }
      .latest-chart .endpoint-label { fill: var(--ink); font-weight: 500; }
      .method-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; margin-top: 16px; }
      .method-item { padding-top: 12px; border-top: 1px solid var(--line); }
      .method-item strong { display: block; margin-bottom: 5px; font-size: 12px; }
      .method-item p { color: var(--muted); font-size: 11px; line-height: 1.65; }
      @media (max-width: 850px) { .latest-grid, .recommend-grid, .method-grid { grid-template-columns: 1fr; } }
      @media (max-width: 520px) { .latest-bar-row { grid-template-columns: 92px minmax(0, 1fr) 76px; gap: 7px; } .latest-legend { margin-left: 99px; } }
    </style>
"""


BODY_TEMPLATE = r"""
<body>
  <main class="shell" id="latest-optimization">
    <nav class="version-nav" aria-label="报告版本">
      <span class="version-nav-label">报告版本</span>
      <a href="optimization-comparison.html">历史版</a>
      <a href="optimization-comparison-20260909-exclude-0800-1000-volume7.html">上一轮 · 2026-09-09</a>
      <a href="optimization-comparison-20260910-exclude-0800-1000-volume7.html" aria-current="page">本轮 · 2026-09-10</a>
      <a href="optimization-comparison-index.html">版本索引</a>
    </nav>

    <header class="masthead">
      <div>
        <div class="eyebrow">LOCAL RESEARCH REPLAY / 2026-09-10 · 七维联合</div>
        <h1>完整回补后，七维参数怎么选？</h1>
        <p class="dek">使用最新完整本地研究数据，把最近 5 分钟成交额 ÷ 前 30 分钟成交额作为第七维，和原来的六个参数在同一个 25,200 组网格里联合寻优。所有结果都按绝对 PnL、最大回撤和自然峰值保证金同时展示。</p>
      </div>
      <div class="window-note">
        <div class="section-label">样本窗口</div>
        <p><strong>原始数据</strong><br><span class="mono" id="raw-window">—</span></p>
        <p style="margin-top:10px"><strong>有效寻优</strong><br><span class="mono" id="optimization-window">—</span></p>
        <p style="margin-top:10px;color:var(--coral)">开仓过滤：<span class="mono">08:00–10:00 Asia/Shanghai</span>（左闭右开）</p>
      </div>
    </header>

    <section class="metric-grid latest-summary" aria-label="本轮总览">
      <article class="metric"><div class="metric-label">有效状态</div><div class="metric-value" id="summary-states">—</div><div class="metric-sub" id="summary-symbols">—</div></article>
      <article class="metric"><div class="metric-label">联合候选</div><div class="metric-value" id="summary-candidates">—</div><div class="metric-sub" id="summary-workers">—</div></article>
      <article class="metric"><div class="metric-label">A · 无上限</div><div class="metric-value positive" id="summary-a-pnl">—</div><div class="metric-sub" id="summary-a-risk">—</div></article>
      <article class="metric"><div class="metric-label">B–G · 风险约束推荐</div><div class="metric-value positive" id="summary-b-pnl">—</div><div class="metric-sub" id="summary-b-risk">—</div></article>
    </section>

    <section class="panel" aria-labelledby="recommend-heading">
      <div class="section-label">00 / DECISION</div>
      <h3 id="recommend-heading">本轮推荐分成两档</h3>
      <div class="recommend-grid">
        <article class="recommend-card">
          <div class="section-label">收益优先 · A</div>
          <h4>无保证金上限</h4>
          <div class="headline" id="recommend-a-headline">—</div>
          <code id="recommend-a-config">—</code>
          <p id="recommend-a-copy">—</p>
        </article>
        <article class="recommend-card caution">
          <div class="section-label">收益 / 风险 / 占用平衡 · B–G</div>
          <h4>约束后共同解</h4>
          <div class="headline" id="recommend-b-headline">—</div>
          <code id="recommend-b-config">—</code>
          <p id="recommend-b-copy">—</p>
        </article>
      </div>
    </section>

    <div class="latest-grid">
      <section class="panel" aria-labelledby="profile-heading">
        <div class="section-label">01 / PROFILE GRID</div>
        <h3 id="profile-heading">A–G 约束场景的完整结果</h3>
        <p class="panel-intro">A–G 都来自同一次七维联合搜索；B–G 只是在同一网格结果上叠加保证金上限、cooldown 和回撤权重。六维基线（放量关闭）参数：<code id="baseline-config">—</code>。表中验证集、留出集和全量 PnL 使用同一套因果回放口径。</p>
        <div class="table-wrap">
          <table class="profile-table">
            <thead><tr><th>组别</th><th>参数（I/C/R/B/N/V/CD）</th><th>约束</th><th>验证 PnL</th><th>留出 PnL</th><th>全量 PnL</th><th>最大回撤</th><th>峰值保证金</th><th>胜率</th><th>已平仓</th></tr></thead>
            <tbody id="profile-table-body"></tbody>
          </table>
        </div>
      </section>

      <aside class="panel" aria-labelledby="account-heading">
        <div class="section-label">02 / LIVE CONFIG</div>
        <h3 id="account-heading">当前实盘参数</h3>
        <p class="panel-intro">四个账户的当前部署参数和同口径本地回放结果放在这里对照。下方时间曲线同时叠加六维基线、四个实盘账户和当前选中的 A–G 方案。</p>
        <div class="account-grid" id="account-grid"></div>
        <div class="status-line"><span class="status-mark"></span><span id="account-note">—</span></div>
      </aside>
    </div>

    <section class="panel curve-panel" aria-labelledby="curve-heading">
      <div class="curve-head">
        <div>
          <div class="section-label">03 / TIME SERIES</div>
          <h3 id="curve-heading">时间曲线：权益、回撤与保证金占用</h3>
          <p class="panel-intro">三个图按一列排列。默认显示六维基线、四个实盘账户和当前选择的 A–G 方案；切换上方方案即可比较不同七维参数，账户参数和回放结果保持在同一时间轴上。</p>
        </div>
      </div>
      <div class="latest-toolbar" id="profile-toolbar" aria-label="选择寻优方案"></div>
      <div class="latest-legend" id="latest-legend" aria-label="曲线图例"></div>
      <div class="curve-grid">
        <article class="curve-card"><h4>权益曲线</h4><p>覆盖完整原始数据窗口；虚线标记有效寻优起点，曲线起始权益锚定为 0U</p><div id="latest-equity-chart" role="img" aria-label="权益曲线"></div></article>
        <article class="curve-card"><h4>回撤曲线</h4><p>覆盖完整原始数据窗口；相对历史峰值的绝对回撤，单位 U</p><div id="latest-drawdown-chart" role="img" aria-label="回撤曲线"></div></article>
        <article class="curve-card"><h4>保证金占用曲线</h4><p>覆盖完整原始数据窗口；每笔 100U 名义仓位按 5 倍杠杆折算为 20U 初始保证金</p><div id="latest-margin-chart" role="img" aria-label="保证金占用曲线"></div></article>
      </div>
      <p class="curve-note" id="curve-note">—</p>
    </section>

    <section class="panel" aria-labelledby="method-heading">
      <div class="section-label">04 / METHOD & SCOPE</div>
      <h3 id="method-heading">这份页面记录了什么</h3>
      <div class="method-grid">
        <div class="method-item"><strong>第七维</strong><p id="method-feature">—</p></div>
        <div class="method-item"><strong>搜索目标</strong><p id="method-objective">—</p></div>
        <div class="method-item"><strong>数据边界</strong><p id="method-scope">—</p></div>
      </div>
      <p class="curve-note">研究回放仍使用本地可用币种的因果 Top10 代理，未建模完整历史实盘候选宇宙、真实成交延迟、资金费和动态账户风控；因此页面用于参数相对比较，不是未来收益承诺。</p>
    </section>

    <footer class="footer-note">
      <p>本页面是新版本文件，未覆盖上一轮 HTML。数据来自完整回补后的本地 Parquet，实际成交开仓时间在北京时间 08:00–10:00 的记录已排除。</p>
      <p>推荐结论：无上限收益优先看 A；如果同时考虑回撤和保证金占用，B–G 的共同解更稳健。</p>
    </footer>
  </main>

  <script>
    const REPORT = __REPORT__;
    const CURVES = __CURVES__;
    const PROFILE_ORDER = ['A', 'B', 'C', 'D', 'E', 'F', 'G'];
    const ACCOUNT_ORDER = ['primary', 'acc01', 'acc02', 'acc03'];
    const COLORS = {
      baseline: '#b77a1c',
      A: '#197b69', B: '#1769aa', C: '#2f855a', D: '#6b46c1', E: '#b5482f', F: '#087f8c', G: '#315776',
      primary: '#315776', acc01: '#a25555', acc02: '#6d63a3', acc03: '#b77a1c'
    };
    const LABELS = {
      baseline: '六维基线', A: 'A 无上限', B: 'B 350U', C: 'C 350U/CD0', D: 'D 280U', E: 'E 280U/CD0', F: 'F DD/280U', G: 'G DD/280U自由',
      primary: 'Primary', acc01: 'acc01', acc02: 'acc02', acc03: 'acc03'
    };
    let selectedProfile = 'B';
    const money = value => `${Number(value) >= 0 ? '+' : ''}${Number(value).toFixed(2)}U`;
    const plainMoney = value => `${Number(value).toFixed(2)}U`;
    const integer = value => Number(value).toLocaleString('en-US');
    const signedClass = value => Number(value) >= 0 ? 'up' : 'down';
    const fmtTime = timestamp => String(timestamp).replace('T', ' ').replace('Z', ' UTC');
    const windowText = (start, end) => `${fmtTime(start)} → ${fmtTime(end)}`;

    function renderSummary() {
      const meta = REPORT.meta;
      const a = REPORT.profiles.A;
      const b = REPORT.profiles.B;
      document.getElementById('raw-window').textContent = windowText(meta.data_start, meta.data_end);
      document.getElementById('optimization-window').textContent = windowText(meta.optimization_start, meta.data_end);
      document.getElementById('baseline-config').textContent = REPORT.baseline.config;
      document.getElementById('summary-states').textContent = integer(meta.states);
      document.getElementById('summary-symbols').textContent = `${meta.symbols} 个币种 · ${meta.segments.toLocaleString('en-US')} 个连续片段`;
      document.getElementById('summary-candidates').textContent = integer(meta.candidate_count);
      document.getElementById('summary-workers').textContent = `${meta.workers} 个并行进程 · 完整七维联合`;
      document.getElementById('summary-a-pnl').textContent = money(a.full_pnl);
      document.getElementById('summary-a-risk').textContent = `回撤 ${plainMoney(a.full_dd)} · 峰值保证金 ${plainMoney(a.margin)}`;
      document.getElementById('summary-b-pnl').textContent = money(b.full_pnl);
      document.getElementById('summary-b-risk').textContent = `回撤 ${plainMoney(b.full_dd)} · 峰值保证金 ${plainMoney(b.margin)}`;
      document.getElementById('recommend-a-headline').textContent = `${money(a.full_pnl)} / DD ${plainMoney(a.full_dd)}`;
      document.getElementById('recommend-a-config').textContent = a.config;
      document.getElementById('recommend-a-copy').textContent = `全量绝对 PnL 最高，已平仓 ${a.full_closed} 笔；自然峰值保证金 ${plainMoney(a.margin)}，不适合作为 280–350U 占用约束下的默认方案。`;
      document.getElementById('recommend-b-headline').textContent = `${money(b.full_pnl)} / DD ${plainMoney(b.full_dd)}`;
      document.getElementById('recommend-b-config').textContent = b.config;
      document.getElementById('recommend-b-copy').textContent = `B–G 在相同网格上都收敛到这一组，峰值保证金 ${plainMoney(b.margin)}；相对 A 少占用 ${plainMoney(a.margin - b.margin)}，回撤也更低。`;
      document.getElementById('method-feature').textContent = `${meta.feature}。${meta.feature_definition}。`;
      document.getElementById('method-objective').textContent = `${meta.selection_objective}；F/G 的回撤权重为 ${meta.drawdown_weight}。`;
      document.getElementById('method-scope').textContent = `${meta.states.toLocaleString('en-US')} 条可用状态，${meta.symbols} 个币种，过滤北京时间 08:00–10:00 的实际成交开仓。`;
      document.getElementById('account-note').textContent = '四个实盘账户均使用七维放量阈值 1.50x，当前配置统一为最新推荐参数。';
    }

    function renderToolbar() {
      const toolbar = document.getElementById('profile-toolbar');
      toolbar.innerHTML = PROFILE_ORDER.map(key => `<button type="button" data-profile="${key}" aria-pressed="${key === selectedProfile}">${LABELS[key]}</button>`).join('');
      toolbar.querySelectorAll('[data-profile]').forEach(button => button.addEventListener('click', () => {
        selectedProfile = button.dataset.profile;
        toolbar.querySelectorAll('[data-profile]').forEach(item => item.setAttribute('aria-pressed', String(item === button)));
        renderProfileTable();
        renderBars();
        renderCharts();
        document.getElementById('curve-note').textContent = curveWindowNote();
      }));
    }

    function renderProfileTable() {
      document.getElementById('profile-table-body').innerHTML = PROFILE_ORDER.map(key => {
        const item = REPORT.profiles[key];
        const cap = item.cap == null ? '无上限' : `≤${item.cap.toFixed(0)}U`;
        const badge = key === 'A' ? '<span class="profile-badge">收益</span>' : '<span class="profile-badge risk">约束</span>';
        const rowClass = key === selectedProfile ? 'selected-row' : key === 'A' ? 'recommend-row' : '';
        return `<tr class="${rowClass}">
          <td class="family">${badge}${key}</td>
          <td class="config">${item.config}</td>
          <td>${cap}</td>
          <td class="${signedClass(item.validation_pnl)}">${money(item.validation_pnl)}</td>
          <td class="${signedClass(item.holdout_pnl)}">${money(item.holdout_pnl)}</td>
          <td class="${signedClass(item.full_pnl)}">${money(item.full_pnl)}</td>
          <td>${plainMoney(item.full_dd)}</td>
          <td>${plainMoney(item.margin)}</td>
          <td>${item.win_rate.toFixed(2)}%</td>
          <td>${integer(item.full_closed)}</td>
        </tr>`;
      }).join('');
    }

    function renderAccounts() {
      const colors = {primary: COLORS.primary, acc01: COLORS.acc01, acc02: COLORS.acc02, acc03: COLORS.acc03};
      document.getElementById('account-grid').innerHTML = ACCOUNT_ORDER.map(name => {
        const item = REPORT.accounts[name];
        return `<article class="account-card" style="border-left-color:${colors[name]}">
          <h4>${item.title}</h4>
          <div class="account-stat"><span>本轮全量 PnL</span><strong>${money(item.pnl)}</strong></div>
          <div class="account-stat"><span>回撤 / 峰值保证金</span><span>${plainMoney(item.dd)} / ${plainMoney(item.margin)}</span></div>
          <div class="account-stat"><span>选中 / 已平仓</span><span>${integer(item.selected)} / ${integer(item.closed)}</span></div>
          <code>${item.config}</code>
        </article>`;
      }).join('');
    }

    function renderBars() {
      const selected = REPORT.profiles[selectedProfile];
      const rows = [
        ['六维基线', REPORT.baseline.pnl, COLORS.baseline],
        ...ACCOUNT_ORDER.map(name => [LABELS[name], REPORT.accounts[name].pnl, COLORS[name]]),
        [`${LABELS[selectedProfile]} 当前`, selected.full_pnl, COLORS[selectedProfile]],
      ];
      const values = rows.map(row => Number(row[1]));
      const low = Math.min(...values, 0), high = Math.max(...values, 0);
      const span = Math.max(high - low, 1);
      const domainMin = low - span * .08, domainMax = high + span * .08, domainSpan = domainMax - domainMin;
      const zero = (0 - domainMin) / domainSpan * 100;
      document.getElementById('latest-bars').innerHTML = `
        <div class="bar-scale"><span>${plainMoney(domainMin)}</span><span>0U</span><span>${plainMoney(domainMax)}</span></div>
        ${rows.map(([label, value, color]) => {
          const start = Number(value) >= 0 ? zero : zero - Math.abs(Number(value)) / domainSpan * 100;
          const width = Math.abs(Number(value)) / domainSpan * 100;
          return `<div class="latest-bar-row"><div class="latest-bar-name">${label}</div><div class="latest-bar-track"><span class="latest-bar-zero" style="left:${zero.toFixed(2)}%"></span><span class="latest-bar-fill" style="left:${start.toFixed(2)}%;width:${width.toFixed(2)}%;background:${color}"></span></div><div class="latest-bar-number ${signedClass(value)}">${money(value)}</div></div>`;
        }).join('')}
        <div class="latest-legend">${rows.map(([label, _value, color]) => `<span><i style="background:${color}"></i>${label}</span>`).join('')}</div>`;
      const target = document.getElementById('bar-panel-intro');
      if (target) target.textContent = `当前选择 ${LABELS[selectedProfile]}；收益柱使用本轮已落盘的基线与七维事件，右侧单独列出实盘账户参数。`;
    }

    function profilePanelMarkup() {
      return `<div class="section-label">01A / PNL TRACE</div><h3>基线、实盘账户与当前方案</h3><p class="panel-intro" id="bar-panel-intro"></p><div class="latest-bars" id="latest-bars"></div>`;
    }

    function chartSeries() {
      return [
        {key: 'baseline', label: LABELS.baseline, color: COLORS.baseline, curve: CURVES.baseline},
        ...ACCOUNT_ORDER.map((key, index) => ({
          key,
          label: LABELS[key],
          color: COLORS[key],
          dash: index % 2 ? '6 4' : '',
          curve: CURVES[key],
        })),
        {key: selectedProfile, label: LABELS[selectedProfile], color: COLORS[selectedProfile], curve: CURVES[selectedProfile]},
      ].filter(item => item.curve);
    }

    function curveWindowNote() {
      return `当前曲线：${LABELS[selectedProfile]}。橙色为六维基线，蓝灰/红/紫/金色为 Primary、acc01、acc02、acc03，彩色为当前选择的七维方案；横轴覆盖原始数据 ${fmtTime(REPORT.meta.curve_start)} → ${fmtTime(REPORT.meta.data_end)}，虚线为有效寻优起点 ${fmtTime(REPORT.meta.optimization_start)}，此前为 Top10 代理预热段，不进入参数排名。`;
    }

    function renderMetricChart(targetId, metric, title) {
      const target = document.getElementById(targetId);
      const series = chartSeries().filter(item => item.curve && item.curve.timestamps && item.curve.timestamps.length > 1);
      if (!series.length) { target.innerHTML = '<p class="panel-intro">暂无足够的时间序列。</p>'; return; }
      const times = series.flatMap(item => item.curve.timestamps.map(timestamp => Date.parse(timestamp)));
      const timeMin = Math.min(...times), timeMax = Math.max(...times), timeSpan = Math.max(timeMax - timeMin, 1);
      const values = series.flatMap(item => item.curve[metric].map(Number));
      let yMin = metric === 'equity' ? Math.min(...values, 0) : 0;
      let yMax = Math.max(...values, 0);
      if (metric === 'equity') { const span = Math.max(yMax - yMin, 1); yMin -= span * .08; yMax += span * .08; }
      else yMax = Math.max(yMax * 1.12, 10);
      if (yMax - yMin < 1e-6) { yMin -= 1; yMax += 1; }
      const width = 980, height = 270, margin = {top: 18, right: 90, bottom: 35, left: 58};
      const plotWidth = width - margin.left - margin.right, plotHeight = height - margin.top - margin.bottom;
      const x = timestamp => margin.left + ((Date.parse(timestamp) - timeMin) / timeSpan) * plotWidth;
      const y = value => margin.top + ((yMax - Number(value)) / (yMax - yMin)) * plotHeight;
      const optimizationTime = Date.parse(REPORT.meta.optimization_start);
      const optimizationMarker = optimizationTime >= timeMin && optimizationTime <= timeMax
        ? `<line x1="${x(new Date(optimizationTime).toISOString())}" x2="${x(new Date(optimizationTime).toISOString())}" y1="${margin.top}" y2="${height - margin.bottom}" stroke="#7d8793" stroke-width="1.2" stroke-dasharray="5 4"></line><text x="${x(new Date(optimizationTime).toISOString()) + 5}" y="${margin.top + 11}" fill="#687481">有效寻优起点</text>`
        : '';
      const ticks = Array.from({length: 5}, (_, index) => yMin + (yMax - yMin) * index / 4);
      const xTicks = [timeMin, timeMin + timeSpan / 2, timeMax];
      const stepPath = item => item.curve.timestamps.map((timestamp, index) => {
        const current = `${x(timestamp).toFixed(2)},${y(item.curve[metric][index]).toFixed(2)}`;
        if (!index) return `M${current}`;
        const previousY = y(item.curve[metric][index - 1]).toFixed(2);
        return `L${x(timestamp).toFixed(2)},${previousY} L${current}`;
      }).join(' ');
      const labels = series.map(item => ({key: item.key, value: Number(item.curve[metric].at(-1)), y: y(item.curve[metric].at(-1))})).sort((a, b) => a.y - b.y);
      labels.forEach((label, index) => { label.y = Math.max(label.y, index ? labels[index - 1].y + 13 : margin.top + 10); });
      const bottom = height - margin.bottom - 4;
      if (labels.at(-1).y > bottom) { const shift = labels.at(-1).y - bottom; labels.forEach(label => { label.y -= shift; }); }
      const labelY = Object.fromEntries(labels.map(label => [label.key, label.y]));
      target.innerHTML = `<svg class="latest-chart" viewBox="0 0 ${width} ${height}" role="img" aria-label="${title}"><title>${title}</title>
        ${ticks.map(tick => `<line class="grid-line" x1="${margin.left}" x2="${width - margin.right}" y1="${y(tick)}" y2="${y(tick)}"></line><text x="${margin.left - 8}" y="${y(tick) + 3}" text-anchor="end">${Number(tick).toFixed(0)}U</text>`).join('')}
        <line class="axis-line" x1="${margin.left}" x2="${width - margin.right}" y1="${height - margin.bottom}" y2="${height - margin.bottom}"></line>
        ${xTicks.map((timestamp, index) => `<text x="${margin.left + (timestamp - timeMin) / timeSpan * plotWidth}" y="${height - 10}" text-anchor="${index === 0 ? 'start' : index === 2 ? 'end' : 'middle'}">${fmtTime(new Date(timestamp).toISOString())}</text>`).join('')}
        ${optimizationMarker}
        ${series.map(item => `<path d="${stepPath(item)}" fill="none" stroke="${item.color}" stroke-width="${item.key === selectedProfile ? 2.8 : 2.1}"${item.dash ? ` stroke-dasharray="${item.dash}"` : ''}></path>`).join('')}
        ${series.map(item => { const last = item.curve.timestamps.length - 1; return `<circle cx="${x(item.curve.timestamps[last])}" cy="${y(item.curve[metric][last])}" r="3.2" fill="${item.color}"></circle><text class="endpoint-label" x="${width - margin.right + 6}" y="${labelY[item.key]}">${item.label} ${Number(item.curve[metric][last]).toFixed(0)}U</text>`; }).join('')}
      </svg>`;
    }

    function renderLegend() {
      document.getElementById('latest-legend').innerHTML = chartSeries().map(item => `<span><i style="width:24px;height:2px;background:${item.color}"></i>${item.label}</span>`).join('');
    }

    function renderCharts() {
      renderLegend();
      renderMetricChart('latest-equity-chart', 'equity', '权益曲线');
      renderMetricChart('latest-drawdown-chart', 'drawdown', '回撤曲线');
      renderMetricChart('latest-margin-chart', 'margin', '保证金占用曲线');
    }

    const profilePanel = document.querySelector('[aria-labelledby="profile-heading"]');
    const barPanel = document.createElement('section');
    barPanel.className = 'panel';
    barPanel.setAttribute('aria-label', '收益柱状图');
    barPanel.innerHTML = profilePanelMarkup();
    profilePanel.parentElement.insertBefore(barPanel, profilePanel);

    renderSummary();
    renderToolbar();
    renderProfileTable();
    renderAccounts();
    renderBars();
    renderCharts();
    document.getElementById('curve-note').textContent = curveWindowNote();
  </script>
</body>
</html>
"""


def build_html(args: argparse.Namespace) -> str:
    payload, curves = build_payload(args)
    template = args.template.read_text(encoding="utf-8")
    head = template.split("<body>", 1)[0]
    head = head.replace(
        "<title>新增数据后的参数回放对比</title>",
        "<title>完整回补后的七维联合寻优 · 2026-09-10</title>",
        1,
    )
    head = head.replace("</head>", CUSTOM_STYLE + "</head>", 1)
    body = BODY_TEMPLATE.replace(
        "__REPORT__", json.dumps(payload, ensure_ascii=False, separators=(",", ":")), 1
    ).replace(
        "__CURVES__", json.dumps(curves, ensure_ascii=False, separators=(",", ":")), 1
    )
    return head + body


def main() -> None:
    args = parse_args()
    rendered = build_html(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rendered, encoding="utf-8")
    print(json.dumps({"output": str(args.output), "bytes": len(rendered.encode("utf-8"))}, ensure_ascii=False))


if __name__ == "__main__":
    main()
