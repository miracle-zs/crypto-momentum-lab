#!/usr/bin/env python3
"""Build an interactive comparison of newly optimized and frozen A-G strategies.

The new A-G reports are selected on the current full local Parquet window.  The
old A-G reports are deliberately *not* optimized again: their parameters are
replayed over that same current window so the chart isolates parameter changes
from changes in the observed market period.

The chart uses absolute simulated PnL anchored to the live account equity at
the requested comparison start.  It is a fixed-notional research replay, not a
claim that the exchange account could have followed every simulated fill.
"""

from __future__ import annotations

import argparse
import csv
import html
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from build_research_vs_live_visual import (
    drawdown,
    interpolate,
    iso,
    load_live_margin_events,
    load_simulated_margin_events,
    margin_summary,
    number,
    parse_time,
    read_csv,
    sample_grid,
    step_value,
)


LABELS = tuple("ABCDEFG")
COLOR_VARS = {
    "A": "var(--viz-series-1)",
    "B": "var(--viz-series-2)",
    "C": "var(--viz-series-3)",
    "D": "var(--viz-series-4)",
    "E": "var(--viz-series-5)",
    "F": "var(--viz-series-6)",
    "G": "var(--viz-series-7)",
}


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_equity_points(path: Path) -> list[tuple[float, float]]:
    points: list[tuple[float, float]] = []
    for row in read_csv(path):
        timestamp = row.get("timestamp")
        if not timestamp:
            continue
        try:
            epoch = parse_time(timestamp).timestamp()
        except ValueError:
            continue
        value = number(row.get("best_validation_cumulative_pnl_usdt"))
        if points and points[-1][0] == epoch:
            points[-1] = (epoch, value)
        else:
            points.append((epoch, value))
    points.sort()
    return points


def load_live_equity_points(path: Path) -> list[tuple[float, float]]:
    points: list[tuple[float, float]] = []
    for row in read_csv(path):
        timestamp = row.get("observed_at")
        if not timestamp:
            continue
        try:
            epoch = parse_time(timestamp).timestamp()
        except ValueError:
            continue
        value = number(row.get("wallet_balance")) + number(row.get("unrealized_pnl"))
        if points and points[-1][0] == epoch:
            points[-1] = (epoch, value)
        else:
            points.append((epoch, value))
    points.sort()
    return points


def config_text(config: dict[str, Any]) -> str:
    return (
        f"I{config['impulse_window_buckets']} / C{config['confirmation_buckets']} / "
        f"R{float(config['min_return_pct']):.2f}% / "
        f"B{float(config['min_imbalance']):.2f} / "
        f"N{float(config['min_intensity']):.1f} / "
        f"CD{config['cooldown_buckets']}"
    )


def fmt_money(value: float) -> str:
    return f"{value:+.2f}U"


def fmt_optional(value: float | None, suffix: str = "") -> str:
    return "—" if value is None else f"{value:.2f}{suffix}"


def report_config(report: dict[str, Any]) -> dict[str, Any]:
    return dict(report["best_validation"]["config"])


def report_metrics(report: dict[str, Any]) -> dict[str, Any]:
    best = report["best_validation"]
    metrics = best["metrics"]
    constraint = report.get("margin_constraint", {})
    cap = constraint.get("max_initial_margin_usdt")
    return {
        "validation_pnl_usdt": number(str(metrics["validation"]["net_pnl_usdt"])),
        "validation_drawdown_usdt": number(str(metrics["validation"]["max_drawdown_usdt"])),
        "holdout_pnl_usdt": number(str(metrics["holdout"]["net_pnl_usdt"])),
        "holdout_drawdown_usdt": number(str(metrics["holdout"]["max_drawdown_usdt"])),
        "full_pnl_usdt": number(str(metrics["full"]["net_pnl_usdt"])),
        "full_drawdown_usdt": number(str(metrics["full"]["max_drawdown_usdt"])),
        "selection_score": number(str(best.get("selection_score", metrics["validation"]["net_pnl_usdt"]))),
        "natural_peak_margin_usdt": number(str(best["natural_initial_margin_peak_usdt"])),
        "cap_usdt": None if cap in (None, "") else number(str(cap)),
        "feasible": bool(best.get("margin_constraint_feasible", True)),
    }


def get_paths(
    root: Path,
    label: str,
    *,
    frozen: bool,
    new_dir_prefix: str = "optimization-full-current-",
    new_dir_suffix: str = "20260905",
) -> tuple[Path, Path]:
    directory = root / (
        f"replay-{label}"
        if frozen
        else f"{new_dir_prefix}{label}-{new_dir_suffix}"
    )
    return directory / "optimization_report.json", directory / "equity_series.csv"


def make_csv(path: Path, columns: list[str], rows: list[list[Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(rows)


def table_rows(records: list[dict[str, Any]]) -> str:
    chunks: list[str] = []
    for record in records:
        row_class = "new-row" if record["generation"] == "new" else "old-row"
        cap = record["cap_usdt"]
        cap_text = "无上限" if cap is None else f"≤{cap:.0f}U"
        feasible = "可行" if record["feasible"] else "超限"
        if record["cap_usdt"] is not None and record["chart_peak_margin_usdt"] > record["cap_usdt"] + 1e-8:
            feasible = "超限"
        chunks.append(
            "<tr class=\"{}\"><td><span class=\"badge\">{}</span>{}</td>"
            "<td class=\"mono\">{}</td><td>{}</td><td>{}</td>"
            "<td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>".format(
                row_class,
                "新" if record["generation"] == "new" else "旧",
                html.escape(record["strategy"]),
                html.escape(config_text(record["config"])),
                cap_text,
                feasible,
                fmt_money(record["validation_pnl_usdt"]),
                fmt_money(record["holdout_pnl_usdt"]),
                fmt_money(record["chart_pnl_usdt"]),
                f"{record['chart_drawdown_usdt']:.2f}U",
                f"{record['chart_peak_margin_usdt']:.0f}U",
            )
        )
    return "\n".join(chunks)


def build_html(report: dict[str, Any]) -> str:
    chart = report["chart"]
    payload = json.dumps(chart, ensure_ascii=False, separators=(",", ":"))
    live = report["live"]
    new_records = [item for item in report["strategies"] if item["generation"] == "new"]
    old_records = [item for item in report["strategies"] if item["generation"] == "old"]
    new_best = max(new_records, key=lambda item: item["chart_pnl_usdt"])
    old_best = max(old_records, key=lambda item: item["chart_pnl_usdt"])
    cards = {
        "window": f"{report['comparison']['start_utc']} → {report['comparison']['end_utc']}",
        "data_end": report["data_scope"]["full_data_end_utc"],
        "live": f"{live['start_equity_usdt']:.2f}U → {live['end_equity_usdt']:.2f}U ({fmt_money(live['change_usdt'])})",
        "new_best": f"{new_best['strategy']} {fmt_money(new_best['chart_pnl_usdt'])}",
        "old_best": f"{old_best['strategy']} {fmt_money(old_best['chart_pnl_usdt'])}",
        "delta": fmt_money(new_best["chart_pnl_usdt"] - old_best["chart_pnl_usdt"]),
    }
    template = '''<div id="seven-vs-seven-comparison" aria-label="实盘与当前重寻优 A-G 对上一轮 A-G 的权益和保证金对比">
  <style>
    #seven-vs-seven-comparison {
      --fg: light-dark(#17202a, #edf2f7);
      --muted: light-dark(#64748b, #aab6c3);
      --border: light-dark(#cbd5e1, #465568);
      --grid: light-dark(#e2e8f0, #334155);
      --card: light-dark(#f8fafc, #1c2733);
      --live: light-dark(#111827, #f8fafc);
      --viz-series-1: light-dark(#1769aa, #63b3ed);
      --viz-series-2: light-dark(#c05621, #f6ad55);
      --viz-series-3: light-dark(#2f855a, #68d391);
      --viz-series-4: light-dark(#6b46c1, #b794f4);
      --viz-series-5: light-dark(#b5482f, #f28f79);
      --viz-series-6: light-dark(#087f8c, #76e4e8);
      --viz-series-7: light-dark(#b7791f, #f6e05e);
      color: var(--fg);
      display: block;
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      font-size: 12px;
      line-height: 1.4;
      position: relative;
      width: 100%;
    }
    #seven-vs-seven-comparison .title { font-size: 17px; font-weight: 700; margin: 0 0 3px; }
    #seven-vs-seven-comparison .subtitle, #seven-vs-seven-comparison .note { color: var(--muted); margin: 0 0 10px; }
    #seven-vs-seven-comparison .cards { display: grid; grid-template-columns: repeat(6, minmax(120px, 1fr)); gap: 7px; margin: 10px 0 12px; }
    #seven-vs-seven-comparison .card { background: var(--card); border: 1px solid var(--border); border-radius: 7px; padding: 8px 9px; }
    #seven-vs-seven-comparison .label { color: var(--muted); font-size: 11px; }
    #seven-vs-seven-comparison .value { font-size: 14px; font-weight: 700; margin-top: 2px; }
    #seven-vs-seven-comparison .toolbar { display: flex; flex-wrap: wrap; gap: 6px; margin: 4px 0 7px; }
    #seven-vs-seven-comparison button { background: var(--card); border: 1px solid var(--border); border-radius: 5px; color: var(--fg); cursor: pointer; font: inherit; padding: 4px 8px; }
    #seven-vs-seven-comparison button:hover { border-color: var(--viz-series-1); }
    #seven-vs-seven-comparison .legend { display: flex; flex-wrap: wrap; gap: 5px; margin: 6px 0 8px; }
    #seven-vs-seven-comparison .legend button { align-items: center; display: inline-flex; gap: 5px; padding: 4px 6px; }
    #seven-vs-seven-comparison .legend button.off { opacity: .42; }
    #seven-vs-seven-comparison .swatch { display: inline-block; height: 3px; width: 18px; }
    #seven-vs-seven-comparison .dash { border-top: 2px dashed currentColor; height: 0; }
    #seven-vs-seven-comparison svg { display: block; height: auto; overflow: visible; width: 100%; }
    #seven-vs-seven-comparison text { fill: var(--fg); font-size: 11px; }
    #seven-vs-seven-comparison .muted { fill: var(--muted); }
    #seven-vs-seven-comparison .frame { fill: none; stroke: var(--border); stroke-width: 1; }
    #seven-vs-seven-comparison .grid-line { stroke: var(--grid); stroke-dasharray: 2 3; opacity: .75; }
    #seven-vs-seven-comparison .split-line { stroke: var(--viz-series-4); stroke-dasharray: 5 4; opacity: .8; }
    #seven-vs-seven-comparison .tooltip { background: light-dark(#fff, #1c2733); border: 1px solid var(--border); border-radius: 5px; color: var(--fg); display: none; max-width: 360px; padding: 6px 8px; pointer-events: none; position: absolute; z-index: 3; }
    #seven-vs-seven-comparison .section-title { font-size: 14px; font-weight: 600; margin: 15px 0 3px; }
    #seven-vs-seven-comparison .table-wrap { border: 1px solid var(--border); border-radius: 7px; margin-top: 8px; overflow-x: auto; }
    #seven-vs-seven-comparison table { border-collapse: collapse; min-width: 980px; width: 100%; }
    #seven-vs-seven-comparison th, #seven-vs-seven-comparison td { border-bottom: 1px solid var(--border); padding: 6px 7px; text-align: right; white-space: nowrap; }
    #seven-vs-seven-comparison th:first-child, #seven-vs-seven-comparison td:first-child, #seven-vs-seven-comparison th:nth-child(2), #seven-vs-seven-comparison td:nth-child(2) { text-align: left; }
    #seven-vs-seven-comparison th { background: var(--card); color: var(--muted); font-weight: 600; position: sticky; top: 0; }
    #seven-vs-seven-comparison tr:last-child td { border-bottom: 0; }
    #seven-vs-seven-comparison .old-row { opacity: .78; }
    #seven-vs-seven-comparison .badge { border: 1px solid var(--border); border-radius: 4px; color: var(--muted); font-size: 10px; margin-right: 4px; padding: 1px 3px; }
    #seven-vs-seven-comparison .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
    @media (max-width: 900px) { #seven-vs-seven-comparison .cards { grid-template-columns: repeat(3, minmax(120px, 1fr)); } }
    @media (max-width: 500px) { #seven-vs-seven-comparison .cards { grid-template-columns: repeat(2, minmax(120px, 1fr)); } }
  </style>
  <div class="title">实盘 + 当前重寻优 A–G vs 上一轮 A–G</div>
  <p class="subtitle">新 A–G 使用当前本地全量数据重新选参；旧 A–G 保持上一轮参数冻结，只回放到同一数据终点。本轮目标：__OBJECTIVE__。权益是绝对模拟 PnL 加到作图起点实盘权益。</p>
  <div class="cards">
    <div class="card"><div class="label">共同作图区间</div><div class="value">__WINDOW__</div></div>
    <div class="card"><div class="label">本地全量数据终点</div><div class="value">__DATA_END__</div></div>
    <div class="card"><div class="label">同时段实盘权益</div><div class="value">__LIVE__</div></div>
    <div class="card"><div class="label">新七组最佳终点 PnL</div><div class="value">__NEW_BEST__</div></div>
    <div class="card"><div class="label">旧七组最佳终点 PnL</div><div class="value">__OLD_BEST__</div></div>
    <div class="card"><div class="label">两组最佳差值</div><div class="value">__DELTA__</div></div>
  </div>
  <div class="toolbar" aria-label="曲线显示预设"><button data-preset="all">全部</button><button data-preset="new">只看新 A–G</button><button data-preset="old">只看旧 A–G</button><button data-preset="pairs">新旧配对</button></div>
  <div class="legend" id="seven-vs-seven-legend" aria-label="曲线开关"></div>
  <svg id="seven-vs-seven-equity-svg" viewBox="0 0 960 590" role="img" aria-label="实盘与当前重寻优、上一轮 A-G 的绝对权益曲线"><g id="seven-vs-seven-equity-chart"></g></svg>
  <div class="section-title">保证金占用时序</div>
  <svg id="seven-vs-seven-margin-svg" viewBox="0 0 960 390" role="img" aria-label="实盘与当前重寻优、上一轮 A-G 的初始保证金占用曲线"><g id="seven-vs-seven-margin-chart"></g></svg>
  <p class="note">实盘保证金按实际成交记录、同币种 FIFO 开平仓估算；模拟保证金按每笔 100U 名义仓位 ÷ 5x = 20U/笔。旧约束策略若在当前扩展回放中超过其历史上限，会在表格中标为“超限”；这不改变旧参数，只是如实展示它在新数据上的占用。</p>
  <div class="section-title">参数与结果对比</div>
  <div class="table-wrap"><table><thead><tr><th>策略</th><th>参数（I/C/R/B/N/CD）</th><th>上限</th><th>可行性</th><th>验证集 PnL</th><th>留出集 PnL</th><th>共同终点 PnL</th><th>共同区间回撤</th><th>共同区间峰值保证金</th></tr></thead><tbody>__TABLE_ROWS__</tbody></table></div>
  <p class="note">参数缩写：I=impulse_window_buckets，C=confirmation_buckets，R=min_return_pct，B=min_imbalance，N=min_intensity，CD=cooldown_buckets。新七组的验证/留出数值来自本次全量窗口切分；旧七组的验证/留出数值来自冻结参数在同一当前窗口的回放。</p>
  <div class="tooltip" id="seven-vs-seven-tooltip" role="tooltip"></div>
  <script>
    (() => {
      const root = document.getElementById("seven-vs-seven-comparison");
      const tooltip = document.getElementById("seven-vs-seven-tooltip");
      const legend = document.getElementById("seven-vs-seven-legend");
      const data = __DATA__;
      const NS = "http://www.w3.org/2000/svg";
      const W = 960, left = 72, right = 20;
      const visible = data.columns.map(() => true);
      const fmtTime = value => String(value).replace("T", " ").replace("Z", " UTC");
      const money = value => `${Number(value).toFixed(2)}U`;
      const add = (name, attrs, parent) => {
        const node = document.createElementNS(NS, name);
        Object.entries(attrs || {}).forEach(([key, value]) => node.setAttribute(key, String(value)));
        (parent || document.body).appendChild(node);
        return node;
      };
      const putText = (parent, x, y, value, attrs = {}) => {
        const node = add("text", {x, y, ...attrs}, parent);
        node.textContent = value;
        return node;
      };
      const epoch = row => Date.parse(row[0]);
      const valuesFor = (dataset, index) => dataset.points.map(row => Number(row[index + 1]));
      const xScale = (value, start, end, x0, x1) => x0 + (epoch(value) - start) / Math.max(1, end - start) * (x1 - x0);
      const valueDomain = dataset => {
        const values = dataset.points.flatMap(row => row.slice(1).map(Number)).filter(Number.isFinite);
        const low = Math.min(...values), high = Math.max(...values);
        const span = Math.max(1, high - low);
        return [low - span * .08, high + span * .08];
      };
      const drawPanel = (dataset, groupId, svgId, height, stepMode) => {
        const group = document.getElementById(groupId);
        group.replaceChildren();
        const top = 28, bottom = 55, plotH = height - top - bottom, x0 = left, x1 = W - right, y0 = top, y1 = top + plotH;
        const start = epoch(dataset.points[0]), end = epoch(dataset.points[dataset.points.length - 1]);
        const domain = valueDomain(dataset);
        const y = value => y1 - (Number(value) - domain[0]) / (domain[1] - domain[0] || 1) * plotH;
        add("rect", {x:x0, y:y0, width:x1-x0, height:plotH, class:"frame"}, group);
        Array.from({length: 6}, (_, i) => domain[0] + (domain[1] - domain[0]) * i / 5).forEach(value => {
          const yy = y(value);
          add("line", {x1:x0, x2:x1, y1:yy, y2:yy, class:"grid-line"}, group);
          putText(group, x0 - 8, yy + 4, value.toFixed(0), {"text-anchor":"end"});
        });
        data.splits.forEach(split => {
          const timestamp = Date.parse(split.timestamp);
          if (timestamp <= start || timestamp >= end) return;
          const xx = xScale(split.timestamp, start, end, x0, x1);
          add("line", {x1:xx, x2:xx, y1:y0, y2:y1, class:"split-line"}, group);
          putText(group, xx + 4, y0 + 14, split.label, {class:"muted"});
        });
        const paths = data.columns.map((column, index) => {
          const values = valuesFor(dataset, index);
          let d = "";
          values.forEach((value, pointIndex) => {
            const xx = xScale(dataset.points[pointIndex][0], start, end, x0, x1);
            const yy = y(value);
            if (!pointIndex) d = `M${xx},${yy}`;
            else if (stepMode) d += ` L${xx},${y(values[pointIndex - 1])} L${xx},${yy}`;
            else d += ` L${xx},${yy}`;
          });
          const attrs = {d, fill:"none", stroke:column.color, "stroke-width":column.generation === "live" ? 2.8 : 2.0, "data-series-index":index};
          if (column.dash) attrs["stroke-dasharray"] = column.dash;
          return add("path", attrs, group);
        });
        [dataset.points[0][0], dataset.points[Math.floor((dataset.points.length - 1) / 2)][0], dataset.points[dataset.points.length - 1][0]].forEach((timestamp, index) => {
          putText(group, xScale(timestamp, start, end, x0, x1), y1 + 23, fmtTime(timestamp), {"text-anchor":index === 0 ? "start" : index === 2 ? "end" : "middle", class:"muted"});
        });
        putText(group, x0 + 3, y0 + 15, dataset.yLabel, {class:"muted"});
        const overlay = add("rect", {x:x0, y:y0, width:x1-x0, height:plotH, fill:"transparent", "data-chart-hit":"true", "data-chart-hover-overlay":"cross-series"}, group);
        overlay.addEventListener("pointermove", event => {
          const bounds = overlay.getBoundingClientRect();
          const ratio = Math.max(0, Math.min(1, (event.clientX - bounds.left) / Math.max(1, bounds.width)));
          const index = Math.max(0, Math.min(dataset.points.length - 1, Math.round(ratio * (dataset.points.length - 1))));
          const row = dataset.points[index];
          const lines = data.columns.map((column, columnIndex) => visible[columnIndex] ? `<div><span style="color:${column.color}">━</span> ${column.label}：${money(row[columnIndex + 1])}</div>` : "").join("");
          tooltip.innerHTML = `<strong>${fmtTime(row[0])}</strong>${lines}`;
          const box = root.getBoundingClientRect();
          tooltip.style.display = "block";
          tooltip.style.left = `${event.clientX - box.left + 10}px`;
          tooltip.style.top = `${event.clientY - box.top + 10}px`;
        });
        overlay.addEventListener("pointerleave", () => { tooltip.style.display = "none"; });
        return paths;
      };
      data.columns.forEach((column, index) => {
        const button = document.createElement("button");
        button.type = "button";
        button.dataset.seriesIndex = String(index);
        button.innerHTML = `<span class="swatch" style="background:${column.color}"></span>${column.label}`;
        button.addEventListener("click", () => {
          visible[index] = !visible[index];
          button.classList.toggle("off", !visible[index]);
          [equityPaths, marginPaths].forEach(paths => { paths[index].style.display = visible[index] ? "" : "none"; });
        });
        legend.appendChild(button);
      });
      const equityPaths = drawPanel(data.equity, "seven-vs-seven-equity-chart", "seven-vs-seven-equity-svg", 590, false);
      const marginPaths = drawPanel(data.margin, "seven-vs-seven-margin-chart", "seven-vs-seven-margin-svg", 390, true);
      const setPreset = preset => {
        visible.forEach((_, index) => {
          const column = data.columns[index];
          visible[index] = preset === "all" || preset === "pairs" || (preset === "new" && column.generation !== "old") || (preset === "old" && column.generation !== "new");
          const button = legend.querySelector(`[data-series-index="${index}"]`);
          button.classList.toggle("off", !visible[index]);
          equityPaths[index].style.display = visible[index] ? "" : "none";
          marginPaths[index].style.display = visible[index] ? "" : "none";
        });
      };
      root.querySelectorAll("[data-preset]").forEach(button => button.addEventListener("click", () => setPreset(button.dataset.preset)));
    })();
  </script>
</div>
'''
    replacements = {
        "__DATA__": payload,
        "__WINDOW__": html.escape(cards["window"]),
        "__DATA_END__": html.escape(cards["data_end"]),
        "__LIVE__": html.escape(cards["live"]),
        "__NEW_BEST__": html.escape(cards["new_best"]),
        "__OLD_BEST__": html.escape(cards["old_best"]),
        "__DELTA__": html.escape(cards["delta"]),
        "__OBJECTIVE__": html.escape(report["data_scope"].get("new_selection_objective", "当前全量绝对 PnL")),
        "__TABLE_ROWS__": table_rows(report["strategies"]),
    }
    for key, value in replacements.items():
        template = template.replace(key, str(value))
    return template


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    live_dir = args.live_dir
    balance_path = live_dir / "account_balance_snapshots.csv.gz"
    fill_path = live_dir / "account_fill_events.csv.gz"
    orders_path = live_dir / "exchange_orders.csv.gz"
    live_points = load_live_equity_points(balance_path)
    if not live_points:
        raise SystemExit("live balance snapshots are required")

    loaded: dict[str, dict[str, Any]] = {}
    all_reports: list[dict[str, Any]] = []
    for label in LABELS:
        for generation, root, frozen in (
            ("new", args.new_root, False),
            ("old", args.old_root, True),
        ):
            report_path, equity_path = get_paths(
                root,
                label,
                frozen=frozen,
                new_dir_prefix=args.new_dir_prefix,
                new_dir_suffix=args.new_dir_suffix,
            )
            report = load_json(report_path)
            points = load_equity_points(equity_path)
            if not points:
                raise SystemExit(f"empty equity series: {equity_path}")
            loaded[f"{generation}-{label}"] = {
                "report": report,
                "points": points,
                "equity_path": equity_path,
            }
            all_reports.append(report)

    requested_start = parse_time(args.comparison_start)
    data_start = max(parse_time(report["data_start"]) for report in all_reports)
    full_data_end = min(parse_time(report["data_end"]) for report in all_reports)
    live_start = datetime.fromtimestamp(live_points[0][0], tz=UTC)
    live_end = datetime.fromtimestamp(live_points[-1][0], tz=UTC)
    start = max(requested_start, data_start, live_start)
    end = min(full_data_end, live_end)
    if end <= start:
        raise SystemExit("no common chart window between local data and live snapshots")

    grid = sample_grid(start, end, limit=args.points)
    live_start_equity = interpolate(live_points, start.timestamp())
    live_end_equity = interpolate(live_points, end.timestamp())
    live_values = [interpolate(live_points, timestamp.timestamp()) for timestamp in grid]
    live_dd, live_dd_pct = drawdown(live_values)
    leverage = 5.0
    entry_notional = 100.0
    for report in all_reports:
        settings = report.get("fixed_live_settings", {})
        leverage = max(number(str(settings.get("entry_leverage")), leverage), 1e-12)
        entry_notional = max(number(str(settings.get("entry_notional_usdt")), entry_notional), 0.0)
        break

    live_margin_curve, live_margin_load = load_live_margin_events(
        fill_path,
        orders_path,
        leverage=leverage,
    )
    live_margin_values = [step_value(live_margin_curve, timestamp.timestamp()) for timestamp in grid]
    live_margin = margin_summary(live_margin_curve, start=start, end=end)

    columns = [{"id": "live", "label": "实盘", "generation": "live", "strategy": "live", "color": "var(--live)", "dash": ""}]
    sampled_values: dict[str, dict[str, list[float]]] = {
        "live": {"equity": live_values, "margin": live_margin_values}
    }
    equity_rows: list[list[Any]] = []
    margin_rows: list[list[Any]] = []
    series_by_id: dict[str, dict[str, Any]] = {}
    for label in LABELS:
        for generation in ("new", "old"):
            item = loaded[f"{generation}-{label}"]
            report = item["report"]
            raw_points = item["points"]
            pnl_at_start = step_value(raw_points, start.timestamp())
            values = [live_start_equity + step_value(raw_points, timestamp.timestamp()) - pnl_at_start for timestamp in grid]
            curve_dd, curve_dd_pct = drawdown(values)
            margin_curve, margin_load = load_simulated_margin_events(
                report_path := item["equity_path"].parent / "best_candidate_events.csv",
                entry_notional=entry_notional,
                leverage=leverage,
            )
            margin_values = [step_value(margin_curve, timestamp.timestamp()) for timestamp in grid]
            margin_info = margin_summary(margin_curve, start=start, end=end)
            config = report_config(report)
            metrics = report_metrics(report)
            series_id = f"{generation}-{label}"
            sampled_values[series_id] = {"equity": values, "margin": margin_values}
            columns.append(
                {
                    "id": series_id,
                    "label": f"{'新' if generation == 'new' else '旧'}{label}",
                    "generation": generation,
                    "strategy": label,
                    "color": COLOR_VARS[label],
                    "dash": "" if generation == "new" else "7 5",
                }
            )
            series_by_id[series_id] = {
                "strategy": label,
                "generation": generation,
                "config": config,
                "config_text": config_text(config),
                **metrics,
                "chart_start_equity_usdt": values[0],
                "chart_end_equity_usdt": values[-1],
                "chart_pnl_usdt": values[-1] - live_start_equity,
                "chart_drawdown_usdt": curve_dd,
                "chart_drawdown_pct": curve_dd_pct,
                "chart_peak_margin_usdt": max(margin_values, default=0.0),
                "chart_margin_start_usdt": margin_values[0] if margin_values else 0.0,
                "chart_margin_end_usdt": margin_values[-1] if margin_values else 0.0,
                "margin_load": margin_load,
            }

    for index, timestamp in enumerate(grid):
        equity_row = [iso(timestamp), round(live_values[index], 8)]
        margin_row = [iso(timestamp), round(live_margin_values[index], 8)]
        for label in LABELS:
            for generation in ("new", "old"):
                series_id = f"{generation}-{label}"
                equity_row.append(round(sampled_values[series_id]["equity"][index], 8))
                margin_row.append(round(sampled_values[series_id]["margin"][index], 8))
        equity_rows.append(equity_row)
        margin_rows.append(margin_row)

    # Keep the report's strategy order identical to the visual's new/old pairs.
    records: list[dict[str, Any]] = []
    for label in LABELS:
        for generation in ("new", "old"):
            records.append(series_by_id[f"{generation}-{label}"])

    splits: list[dict[str, str]] = []
    split_source = loaded["new-A"]["report"].get("splits", {})
    for key, label in (("train", "训练/验证"), ("validation", "验证/留出")):
        boundary = split_source.get(key, {}).get("end")
        if boundary:
            timestamp = parse_time(boundary)
            if start < timestamp < end:
                splits.append({"timestamp": iso(timestamp), "label": label})

    chart_columns = [{**columns[0], "chart_index": 0}]
    chart_index = 1
    for label in LABELS:
        for generation in ("new", "old"):
            chart_columns.append({**next(item for item in columns if item["id"] == f"{generation}-{label}"), "chart_index": chart_index})
            chart_index += 1

    report: dict[str, Any] = {
        "data_scope": {
            "local_research_root": str(args.new_root),
            "full_data_start_utc": iso(data_start),
            "full_data_end_utc": iso(full_data_end),
            "live_snapshot_start_utc": iso(live_start),
            "live_snapshot_end_utc": iso(live_end),
            "fresh_optimization": "new A-G selected by current full local data",
            "old_comparison": "old A-G frozen parameters replayed on the same current local data",
            "new_selection_scope": loaded["new-A"]["report"].get("selection_scope"),
            "new_selection_objective": loaded["new-A"]["report"].get("selection_objective"),
            "old_selection_scope": loaded["old-A"]["report"].get("selection_scope"),
            "old_selection_objective": loaded["old-A"]["report"].get("selection_objective"),
        },
        "comparison": {
            "start_utc": iso(start),
            "end_utc": iso(end),
            "duration_hours": (end - start).total_seconds() / 3600.0,
            "requested_start_rule": "2026-09-04 00:00 UTC or later if data coverage requires",
            "end_rule": "minimum of current full research end and local live snapshot end",
            "equity_anchor": "live raw account equity at comparison start",
            "simulated_equity_definition": "anchor + frozen/local-replay cumulative PnL change since comparison start",
        },
        "live": {
            "start_equity_usdt": live_start_equity,
            "end_equity_usdt": live_end_equity,
            "change_usdt": live_end_equity - live_start_equity,
            "change_pct": (live_end_equity / live_start_equity - 1.0) * 100.0 if live_start_equity else None,
            "max_drawdown_usdt": live_dd,
            "max_drawdown_pct": live_dd_pct,
            "margin": {**live_margin, **live_margin_load},
        },
        "settings": {
            "entry_notional_usdt": entry_notional,
            "leverage": leverage,
            "initial_margin_per_entry_usdt": entry_notional / leverage if leverage else None,
        },
        "strategies": records,
        "chart": {
            "start": iso(start),
            "end": iso(end),
            "splits": splits,
            "columns": chart_columns,
            "equity": {
                "yLabel": "绝对权益（U）",
                "columns": chart_columns,
                "points": equity_rows,
            },
            "margin": {
                "yLabel": "初始保证金占用（U）",
                "columns": chart_columns,
                "points": margin_rows,
            },
        },
    }
    return report


def write_outputs(report: dict[str, Any], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    chart = report["chart"]
    columns = [column["label"] for column in chart["columns"]]
    make_csv(output_dir / "equity_comparison.csv", ["timestamp", *columns], chart["equity"]["points"])
    make_csv(output_dir / "margin_comparison.csv", ["timestamp", *columns], chart["margin"]["points"])
    (output_dir / "comparison_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    rows = report["strategies"]
    new_rows = [row for row in rows if row["generation"] == "new"]
    old_rows = [row for row in rows if row["generation"] == "old"]
    lines = [
        "# 当前全量重寻优 A-G vs 上一轮 A-G",
        "",
        f"- 全量本地采集：`{report['data_scope']['full_data_start_utc']}` → `{report['data_scope']['full_data_end_utc']}`",
        f"- 共同作图区间：`{report['comparison']['start_utc']}` → `{report['comparison']['end_utc']}`，{report['comparison']['duration_hours']:.2f} 小时",
        "- 新 A-G：在当前全量本地数据上重新寻优。",
        "- 旧 A-G：上一轮参数冻结，在同一当前本地数据上回放；没有重新选参。",
        f"- 本轮新参数选择目标：`{report['data_scope'].get('new_selection_objective')}`。",
        "- 模拟权益：共同起点实盘权益 + 模拟绝对累计 PnL 变化；不是完整账户仿真。",
        "",
        "## 实盘",
        "",
        f"- 权益：{report['live']['start_equity_usdt']:.2f}U → {report['live']['end_equity_usdt']:.2f}U，变化 {fmt_money(report['live']['change_usdt'])}（{report['live']['change_pct']:+.2f}%）",
        f"- 共同区间最大回撤：{report['live']['max_drawdown_usdt']:.2f}U（{report['live']['max_drawdown_pct']:.2f}%）",
        f"- 保证金估算：峰值 {report['live']['margin']['peak_usdt']:.2f}U，终点 {report['live']['margin']['end_usdt']:.2f}U",
        "",
        "## 参数与结果",
        "",
        "| 策略 | 参数 | 上限 | 验证集 PnL | 留出集 PnL | 共同终点 PnL | 共同区间回撤 | 共同峰值保证金 |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        cap = "无" if row["cap_usdt"] is None else f"{row['cap_usdt']:.0f}U"
        lines.append(
            f"| {'新' if row['generation'] == 'new' else '旧'}{row['strategy']} | `{row['config_text']}` | {cap} | "
            f"{fmt_money(row['validation_pnl_usdt'])} | {fmt_money(row['holdout_pnl_usdt'])} | {fmt_money(row['chart_pnl_usdt'])} | "
            f"{row['chart_drawdown_usdt']:.2f}U | {row['chart_peak_margin_usdt']:.0f}U |"
        )
    lines.extend(["", "## 新旧终点差异", ""])
    for label in LABELS:
        new = next(row for row in new_rows if row["strategy"] == label)
        old = next(row for row in old_rows if row["strategy"] == label)
        lines.append(
            f"- {label}：新 {fmt_money(new['chart_pnl_usdt'])} vs 旧 {fmt_money(old['chart_pnl_usdt'])}，差 {fmt_money(new['chart_pnl_usdt'] - old['chart_pnl_usdt'])}；参数新 `{new['config_text']}`，旧 `{old['config_text']}`。"
        )
    lines.extend(["", "详细交互图见 `seven-vs-seven-equity-margin.html`。", ""])
    (output_dir / "comparison_report.md").write_text("\n".join(lines), encoding="utf-8")
    (output_dir / "seven-vs-seven-equity-margin.html").write_text(build_html(report), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--new-root", type=Path, required=True)
    parser.add_argument("--old-root", type=Path, required=True)
    parser.add_argument("--live-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--new-dir-prefix",
        default="optimization-full-current-",
        help="directory prefix for the newly optimized A-G outputs",
    )
    parser.add_argument(
        "--new-dir-suffix",
        default="20260905",
        help="directory suffix for the newly optimized A-G outputs",
    )
    parser.add_argument("--comparison-start", default="2026-09-04T00:00:00Z")
    parser.add_argument("--points", type=int, default=600)
    args = parser.parse_args()
    report = build_report(args)
    write_outputs(report, args.output_dir)
    print(json.dumps({"comparison": report["comparison"], "live": report["live"], "strategies": report["strategies"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
