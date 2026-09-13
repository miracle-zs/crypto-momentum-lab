#!/usr/bin/env python3
# ruff: noqa: E501
"""Build an interactive time-of-day loss-rate analysis from replay events."""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

STRATEGIES = {
    "baseline": {
        "label": "当前基线",
        "config": "3 / 1 / 1.00% / 0.40 / 2.0 / 0",
        "path": "optimization-full-pnl-current-F-20260909-backfill/baseline_events.csv",
    },
    "pnl_cap": {
        "label": "收益优先 + 280/350U 约束（B–E）",
        "config": "3 / 1 / 1.50% / 0.30 / 3.0 / 0",
        "path": "optimization-full-pnl-current-B-20260909-backfill/best_candidate_events.csv",
    },
    "risk_adjusted": {
        "label": "收益 + 回撤惩罚（F–G）",
        "config": "4 / 1 / 0.75% / 0.60 / 1.5 / 0",
        "path": "optimization-full-pnl-current-F-20260909-backfill/best_candidate_events.csv",
    },
    "pnl_only": {
        "label": "无约束收益优先（A）",
        "config": "2 / 1 / 0.50% / 0.30 / 4.0 / 0",
        "path": "optimization-full-pnl-current-A-20260909-backfill/best_candidate_events.csv",
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("server_exports/cml-research-data-20260908-170354"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("reports/time-of-day-analysis-20260909.html"),
    )
    return parser.parse_args()


def parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def load_events(path: Path) -> list[tuple[datetime, float]]:
    events: list[tuple[datetime, float]] = []
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row.get("closed", "").strip().lower() != "true":
                continue
            if not row.get("entry_at") or not row.get("net_pnl_usdt"):
                continue
            events.append((parse_time(row["entry_at"]), float(row["net_pnl_usdt"])))
    return events


def wilson_interval(losses: int, total: int) -> list[float | None]:
    if not total:
        return [None, None]
    z = 1.959963984540054
    proportion = losses / total
    denominator = 1.0 + z * z / total
    center = (proportion + z * z / (2.0 * total)) / denominator
    half = (
        z
        * math.sqrt(proportion * (1.0 - proportion) / total + z * z / (4.0 * total * total))
        / denominator
    )
    return [round(center - half, 6), round(center + half, 6)]


def fisher_two_sided(a: int, b: int, c: int, d: int) -> float:
    """Two-sided Fisher exact p-value for [[a, b], [c, d]]."""

    from math import comb

    total = a + b + c + d
    if not total:
        return 1.0
    row_total = a + b
    column_total = a + c
    low = max(0, row_total - (total - column_total))
    high = min(row_total, column_total)

    def probability(value: int) -> float:
        return comb(column_total, value) * comb(total - column_total, row_total - value) / comb(
            total, row_total
        )

    observed = probability(a)
    p_value = sum(
        probability(value)
        for value in range(low, high + 1)
        if probability(value) <= observed * (1.0 + 1e-12)
    )
    return round(min(1.0, p_value), 6)


def stats(events: list[tuple[datetime, float]]) -> dict[str, Any]:
    total = len(events)
    losses = sum(pnl < 0 for _, pnl in events)
    pnl = sum(pnl for _, pnl in events)
    return {
        "n": total,
        "losses": losses,
        "loss_rate": round(losses / total, 6) if total else None,
        "loss_rate_pct": round(100.0 * losses / total, 4) if total else None,
        "pnl": round(pnl, 6),
        "avg_pnl": round(pnl / total, 6) if total else None,
    }


def summarize(events: list[tuple[datetime, float]], offset_hours: int) -> dict[str, Any]:
    offset = timedelta(hours=offset_hours)
    shifted = [(at + offset, pnl) for at, pnl in events]
    by_hour: dict[int, list[tuple[datetime, float]]] = defaultdict(list)
    by_date: dict[str, list[tuple[datetime, float]]] = defaultdict(list)
    for at, pnl in shifted:
        by_hour[at.hour].append((at, pnl))
        by_date[at.date().isoformat()].append((at, pnl))

    hourly: list[dict[str, Any]] = []
    for hour in range(24):
        values = by_hour[hour]
        summary = stats(values)
        hourly.append(
            {
                "hour": hour,
                **summary,
                "loss_ci": wilson_interval(summary["losses"], summary["n"]),
            }
        )

    rolling_4h: list[dict[str, Any]] = []
    for start in range(21):
        values = [(at, pnl) for at, pnl in shifted if start <= at.hour < start + 4]
        rolling_4h.append(
            {"start": start, "label": f"{start:02d}:00–{start + 4:02d}:00", **stats(values)}
        )

    selected = [(at, pnl) for at, pnl in shifted if 8 <= at.hour < 12]
    other = [(at, pnl) for at, pnl in shifted if not 8 <= at.hour < 12]
    selected_stats = stats(selected)
    other_stats = stats(other)
    compare = {
        "selected": selected_stats,
        "other": other_stats,
        "loss_rate_diff_pp": round(
            (selected_stats["loss_rate_pct"] or 0.0) - (other_stats["loss_rate_pct"] or 0.0),
            4,
        ),
        "fisher_p": fisher_two_sided(
            selected_stats["losses"],
            selected_stats["n"] - selected_stats["losses"],
            other_stats["losses"],
            other_stats["n"] - other_stats["losses"],
        ),
    }

    daily: list[dict[str, Any]] = []
    for date, values in sorted(by_date.items()):
        day_selected = [(at, pnl) for at, pnl in values if 8 <= at.hour < 12]
        day_other = [(at, pnl) for at, pnl in values if not 8 <= at.hour < 12]
        daily.append(
            {
                "date": date,
                "selected": stats(day_selected),
                "other": stats(day_other),
            }
        )

    return {
        "overall": stats(shifted),
        "hourly": hourly,
        "rolling_4h": rolling_4h,
        "compare_08_12": compare,
        "daily": daily,
    }


def read_window(data_root: Path) -> dict[str, Any]:
    report_path = data_root / "optimization-full-pnl-current-F-20260909-backfill" / "optimization_report.json"
    if not report_path.exists():
        return {}
    report = json.loads(report_path.read_text(encoding="utf-8"))
    return {
        "start": report.get("optimization_window", {}).get("start"),
        "end": report.get("optimization_window", {}).get("end"),
        "parquet_files": report.get("load", {}).get("parquet_files"),
        "source_rows": report.get("load", {}).get("source_rows"),
        "backfill": "2026-09-08 08:45–13:30 UTC · PostgreSQL backfill",
    }


def build_data(data_root: Path) -> dict[str, Any]:
    zones = {"utc": {"label": "UTC", "offset_hours": 0}, "asia_shanghai": {"label": "Asia/Shanghai（UTC+8）", "offset_hours": 8}}
    payload: dict[str, Any] = {"window": read_window(data_root), "strategies": {}}
    for key, spec in STRATEGIES.items():
        events = load_events(data_root / spec["path"])
        payload["strategies"][key] = {
            "label": spec["label"],
            "config": spec["config"],
            "zones": {
                zone_key: summarize(events, zone_spec["offset_hours"])
                for zone_key, zone_spec in zones.items()
            },
        }
    payload["zones"] = {key: {"label": value["label"]} for key, value in zones.items()}
    return payload


HTML_TEMPLATE = r'''<div id="time-of-day-analysis">
  <style>
    #time-of-day-analysis {
      --tod-ink: var(--foreground);
      --tod-muted: var(--muted-foreground);
      --tod-border: var(--border);
      --tod-loss: var(--viz-series-1);
      --tod-focus: var(--viz-series-2);
      --tod-pnl: var(--viz-series-3);
      --tod-other: var(--viz-series-4);
      color: var(--tod-ink);
      font-size: var(--font-size-base, 14px);
      line-height: 1.45;
      width: 100%;
    }
    #time-of-day-analysis .tod-heading {
      font-weight: 500;
      margin-bottom: 4px;
    }
    #time-of-day-analysis .tod-caption,
    #time-of-day-analysis .tod-detail {
      color: var(--tod-muted);
    }
    #time-of-day-analysis .tod-caption {
      margin: 0 0 12px;
    }
    #time-of-day-analysis .tod-controls {
      align-items: end;
      display: flex;
      flex-wrap: wrap;
      gap: 12px 20px;
      margin: 0 0 8px;
    }
    #time-of-day-analysis .tod-field {
      display: grid;
      gap: 4px;
    }
    #time-of-day-analysis .tod-field label {
      color: var(--tod-muted);
      font-size: 12px;
    }
    #time-of-day-analysis select {
      background: var(--background);
      border: 1px solid var(--tod-border);
      color: var(--tod-ink);
      font: inherit;
      min-height: 32px;
      padding: 4px 8px;
    }
    #time-of-day-analysis .tod-detail {
      min-height: 24px;
      margin: 0 0 8px;
    }
    #time-of-day-analysis .tod-chart {
      margin: 16px 0 0;
    }
    #time-of-day-analysis .tod-chart-title {
      color: var(--tod-ink);
      font-weight: 500;
      margin: 0 0 2px;
    }
    #time-of-day-analysis svg {
      display: block;
      height: auto;
      overflow: visible;
      width: 100%;
    }
    #time-of-day-analysis .tod-axis,
    #time-of-day-analysis .tod-label {
      fill: var(--tod-muted);
      font-size: 12px;
    }
    #time-of-day-analysis .tod-axis-title {
      fill: var(--tod-muted);
      font-size: 12px;
    }
    #time-of-day-analysis .tod-grid {
      stroke: var(--tod-border);
      stroke-width: 1;
      opacity: 0.65;
    }
    #time-of-day-analysis .tod-zero {
      stroke: var(--tod-ink);
      stroke-width: 1;
      opacity: 0.75;
    }
    #time-of-day-analysis .tod-highlight {
      fill: var(--tod-focus);
      opacity: 0.10;
    }
    #time-of-day-analysis .tod-tooltip {
      background: var(--popover);
      color: var(--popover-foreground);
      border: 1px solid var(--tod-border);
      display: none;
      font-size: 12px;
      max-width: 260px;
      padding: 6px 8px;
      pointer-events: none;
      position: absolute;
      z-index: 2;
    }
    #time-of-day-analysis .tod-tooltip.is-visible {
      display: block;
    }
    #time-of-day-analysis .tod-shell {
      position: relative;
    }
    @media (max-width: 420px) {
      #time-of-day-analysis .tod-controls {
        gap: 8px 12px;
      }
      #time-of-day-analysis .tod-field {
        flex: 1 1 140px;
      }
    }
  </style>
  <div class="tod-heading">按入场时间检查亏损概率是否有稳定的日内差异</div>
  <p class="tod-caption">亏损概率 = 已成交且已平仓交易中 net PnL &lt; 0 的比例；阴影为每天 08:00–12:00，时间轴按所选时区显示。</p>
  <div class="tod-controls" aria-label="分析口径">
    <div class="tod-field">
      <label for="tod-strategy">回放策略</label>
      <select id="tod-strategy"></select>
    </div>
    <div class="tod-field">
      <label for="tod-zone">时间口径</label>
      <select id="tod-zone"></select>
    </div>
  </div>
  <p id="tod-detail" class="tod-detail" aria-live="polite"></p>
  <div class="tod-shell">
    <figure class="tod-chart">
      <figcaption class="tod-chart-title">每小时亏损概率（含 95% Wilson 区间）</figcaption>
      <svg id="tod-loss-chart" role="img" aria-labelledby="tod-loss-title tod-loss-desc"></svg>
    </figure>
    <figure class="tod-chart">
      <figcaption class="tod-chart-title">每小时平均单笔净 PnL</figcaption>
      <svg id="tod-pnl-chart" role="img" aria-labelledby="tod-pnl-title tod-pnl-desc"></svg>
    </figure>
    <figure class="tod-chart">
      <figcaption class="tod-chart-title">4 小时滚动亏损概率</figcaption>
      <svg id="tod-rolling-chart" role="img" aria-labelledby="tod-rolling-title tod-rolling-desc"></svg>
    </figure>
    <figure class="tod-chart">
      <figcaption class="tod-chart-title">逐日 08:00–12:00 与其他时段</figcaption>
      <svg id="tod-daily-chart" role="img" aria-labelledby="tod-daily-title tod-daily-desc"></svg>
    </figure>
    <div id="tod-tooltip" class="tod-tooltip" role="tooltip"></div>
  </div>
  <script>
    (() => {
      const root = document.getElementById("time-of-day-analysis");
      if (!root) return;
      const DATA = __DATA__;
      const strategySelect = root.querySelector("#tod-strategy");
      const zoneSelect = root.querySelector("#tod-zone");
      const detail = root.querySelector("#tod-detail");
      const tooltip = root.querySelector("#tod-tooltip");
      const strategyKeys = Object.keys(DATA.strategies);
      const pct = (value, digits = 1) => value == null ? "—" : `${value.toFixed(digits)}%`;
      const signedU = (value, digits = 2) => value == null ? "—" : `${value >= 0 ? "+" : ""}${value.toFixed(digits)}U`;
      const css = (name) => getComputedStyle(root).getPropertyValue(name).trim();
      const palette = () => ({
        ink: css("--tod-ink"), muted: css("--tod-muted"), border: css("--tod-border"),
        loss: css("--tod-loss"), focus: css("--tod-focus"), pnl: css("--tod-pnl"), other: css("--tod-other")
      });
      const svgNode = (tag, attrs, parent) => {
        const node = document.createElementNS("http://www.w3.org/2000/svg", tag);
        Object.entries(attrs || {}).forEach(([key, value]) => node.setAttribute(key, String(value)));
        if (parent) parent.appendChild(node);
        return node;
      };
      const clear = (svg, titleId, title, descId, desc) => {
        const width = Math.max(root.clientWidth || 736, 320);
        const height = 278;
        svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
        svg.innerHTML = "";
        svgNode("title", { id: titleId }, svg).textContent = title;
        svgNode("desc", { id: descId }, svg).textContent = desc;
        return { width, height };
      };
      const pathFor = (points) => points.map((point, index) => `${index ? "L" : "M"}${point[0]},${point[1]}`).join(" ");
      const showTip = (event, text) => {
        tooltip.textContent = text;
        tooltip.classList.add("is-visible");
        const shellRect = root.querySelector(".tod-shell").getBoundingClientRect();
        tooltip.style.left = `${event.clientX - shellRect.left + 10}px`;
        tooltip.style.top = `${event.clientY - shellRect.top + 10}px`;
      };
      const hideTip = () => tooltip.classList.remove("is-visible");
      const bindTip = (node, text) => {
        node.addEventListener("pointerenter", (event) => showTip(event, text));
        node.addEventListener("pointermove", (event) => showTip(event, text));
        node.addEventListener("pointerleave", hideTip);
      };
      const scales = (values, min, max, top, bottom) => {
        const safeMin = min == null ? 0 : min;
        const safeMax = max == null || max === safeMin ? safeMin + 1 : max;
        return { y: (value) => bottom - ((value - safeMin) / (safeMax - safeMin)) * (bottom - top), min: safeMin, max: safeMax };
      };
      const addAxes = (svg, width, height, yScale, yTicks, xTitle, yTitle, xTicks, xScale, formatY) => {
        const m = { left: 54, right: 14, top: 24, bottom: 42 };
        yTicks.forEach((tick) => {
          const y = yScale.y(tick);
          svgNode("line", { x1: m.left, x2: width - m.right, y1: y, y2: y, class: "tod-grid" }, svg);
          svgNode("text", { x: m.left - 8, y: y + 4, "text-anchor": "end", class: "tod-axis" }, svg).textContent = formatY(tick);
        });
        xTicks.forEach((tick) => {
          const x = xScale(tick.value);
          svgNode("text", { x, y: height - 22, "text-anchor": "middle", class: "tod-axis" }, svg).textContent = tick.label;
        });
        svgNode("text", { x: (m.left + width - m.right) / 2, y: height - 4, "text-anchor": "middle", class: "tod-axis-title", "data-axis": "x" }, svg).textContent = xTitle;
        const yLabel = svgNode("text", { x: 12, y: (yScale.y(yScale.min) + yScale.y(yScale.max)) / 2, "text-anchor": "middle", class: "tod-axis-title", "data-axis": "y", transform: `rotate(-90 12 ${(yScale.y(yScale.min) + yScale.y(yScale.max)) / 2})` }, svg);
        yLabel.textContent = yTitle;
        return m;
      };
      const highlight = (svg, width, m, start, end, count = 24) => {
        svgNode("rect", { x: m.left + width * start / count, y: m.top, width: width * (end - start) / count, height: 212, class: "tod-highlight" }, svg);
      };
      const drawLoss = (svg, series, zone) => {
        const { width, height } = clear(svg, "tod-loss-title", "每小时亏损概率", "tod-loss-desc", "按入场小时统计已平仓交易亏损概率和 Wilson 置信区间。");
        const m = { left: 54, right: 14, top: 24, bottom: 42 };
        const plotWidth = width - m.left - m.right;
        const values = series.hourly.flatMap((d) => [d.loss_rate || 0, d.loss_ci[1] || 0]);
        const max = Math.min(1, Math.max(0.5, Math.max(...values) * 1.12));
        const y = scales(values, 0, max, m.top, 236);
        const x = (hour) => m.left + plotWidth * (hour + 0.5) / 24;
        const axis = addAxes(svg, width, height, y, [0, max / 2, max], `${DATA.zones[zone].label} 入场小时`, "亏损概率 (%)", [0, 3, 6, 9, 12, 15, 18, 21].map((h) => ({ value: h, label: String(h).padStart(2, "0") })), x, (v) => pct(v * 100, 0));
        highlight(svg, plotWidth, axis, 8, 12);
        series.hourly.forEach((d) => {
          const value = d.loss_rate || 0;
          const barWidth = Math.max(4, plotWidth / 24 * 0.68);
          const top = y.y(value);
          const bar = svgNode("rect", { x: x(d.hour) - barWidth / 2, y: top, width: barWidth, height: Math.max(1, 236 - top), fill: d.hour >= 8 && d.hour < 12 ? palette().focus : palette().loss, opacity: d.n ? 0.86 : 0.18 }, svg);
          bindTip(bar, `${String(d.hour).padStart(2, "0")}:00 · ${pct(value * 100)} · ${d.losses}/${d.n} 亏损 · PnL ${signedU(d.pnl)}`);
          if (d.n && d.loss_ci[0] != null) {
            const low = y.y(d.loss_ci[0]);
            const high = y.y(d.loss_ci[1]);
            svgNode("line", { x1: x(d.hour), x2: x(d.hour), y1: high, y2: low, stroke: palette().ink, "stroke-width": 1.2, opacity: 0.7 }, svg);
            svgNode("line", { x1: x(d.hour) - 3, x2: x(d.hour) + 3, y1: high, y2: high, stroke: palette().ink, "stroke-width": 1.2, opacity: 0.7 }, svg);
            svgNode("line", { x1: x(d.hour) - 3, x2: x(d.hour) + 3, y1: low, y2: low, stroke: palette().ink, "stroke-width": 1.2, opacity: 0.7 }, svg);
          }
        });
      };
      const drawPnl = (svg, series, zone) => {
        const { width, height } = clear(svg, "tod-pnl-title", "每小时平均单笔净 PnL", "tod-pnl-desc", "按入场小时统计每笔已平仓交易的平均净 PnL。");
        const m = { left: 54, right: 14, top: 24, bottom: 42 };
        const plotWidth = width - m.left - m.right;
        const maxAbs = Math.max(1, ...series.hourly.map((d) => Math.abs(d.avg_pnl || 0))) * 1.18;
        const y = scales(series.hourly.map((d) => d.avg_pnl || 0), -maxAbs, maxAbs, m.top, 236);
        const x = (hour) => m.left + plotWidth * (hour + 0.5) / 24;
        const axis = addAxes(svg, width, height, y, [-maxAbs, 0, maxAbs], `${DATA.zones[zone].label} 入场小时`, "平均净 PnL (U)", [0, 3, 6, 9, 12, 15, 18, 21].map((h) => ({ value: h, label: String(h).padStart(2, "0") })), x, (v) => signedU(v, 1));
        highlight(svg, plotWidth, axis, 8, 12);
        const zero = y.y(0);
        svgNode("line", { x1: m.left, x2: width - m.right, y1: zero, y2: zero, class: "tod-zero" }, svg);
        series.hourly.forEach((d) => {
          const value = d.avg_pnl || 0;
          const barWidth = Math.max(4, plotWidth / 24 * 0.68);
          const top = Math.min(zero, y.y(value));
          const bar = svgNode("rect", { x: x(d.hour) - barWidth / 2, y: top, width: barWidth, height: Math.max(1, Math.abs(zero - y.y(value))), fill: d.hour >= 8 && d.hour < 12 ? palette().focus : palette().pnl, opacity: d.n ? 0.86 : 0.18 }, svg);
          bindTip(bar, `${String(d.hour).padStart(2, "0")}:00 · 平均 ${signedU(value)} · 总计 ${signedU(d.pnl)} · n=${d.n}`);
        });
      };
      const drawRolling = (svg, series, zone) => {
        const { width, height } = clear(svg, "tod-rolling-title", "4 小时滚动亏损概率", "tod-rolling-desc", "每个连续四小时入场窗口中的亏损概率，用于识别持续的时段效应。");
        const m = { left: 54, right: 14, top: 24, bottom: 42 };
        const plotWidth = width - m.left - m.right;
        const max = Math.min(1, Math.max(0.5, Math.max(...series.rolling_4h.map((d) => d.loss_rate || 0)) * 1.12));
        const y = scales(series.rolling_4h.map((d) => d.loss_rate || 0), 0, max, m.top, 236);
        const x = (start) => m.left + plotWidth * (start + 0.5) / 21;
        addAxes(svg, width, height, y, [0, max / 2, max], `${DATA.zones[zone].label} 窗口起始小时`, "亏损概率 (%)", [0, 4, 8, 12, 16, 20].map((h) => ({ value: h, label: String(h).padStart(2, "0") })), x, (v) => pct(v * 100, 0));
        const points = series.rolling_4h.map((d) => [x(d.start), y.y(d.loss_rate || 0)]);
        svgNode("path", { d: pathFor(points), fill: "none", stroke: palette().loss, "stroke-width": 2 }, svg);
        series.rolling_4h.forEach((d) => {
          const point = svgNode("circle", { cx: x(d.start), cy: y.y(d.loss_rate || 0), r: d.start === 8 ? 4 : 3, fill: d.start === 8 ? palette().focus : palette().loss }, svg);
          bindTip(point, `${d.label} · ${pct((d.loss_rate || 0) * 100)} · ${d.losses}/${d.n} 亏损 · PnL ${signedU(d.pnl)}`);
        });
        svgNode("text", { x: x(8), y: y.y(series.rolling_4h[8].loss_rate || 0) - 10, "text-anchor": "middle", class: "tod-label" }, svg).textContent = "08–12";
      };
      const drawDaily = (svg, series, zone) => {
        const { width, height } = clear(svg, "tod-daily-title", "逐日 08:00–12:00 与其他时段", "tod-daily-desc", "逐日比较 08:00–12:00 和其他时段的亏损概率，检查是否每天方向一致。");
        const m = { left: 54, right: 14, top: 24, bottom: 42 };
        const plotWidth = width - m.left - m.right;
        const dates = series.daily;
        const max = 1;
        const y = scales([], 0, max, m.top, 236);
        const x = (index) => dates.length <= 1 ? m.left + plotWidth / 2 : m.left + plotWidth * index / (dates.length - 1);
        addAxes(svg, width, height, y, [0, 0.5, 1], `${DATA.zones[zone].label} 日期`, "亏损概率 (%)", dates.map((d, i) => ({ value: i, label: d.date.slice(5) })), x, (v) => pct(v * 100, 0));
        const selectedPoints = dates.map((d, i) => [x(i), d.selected.loss_rate == null ? null : y.y(d.selected.loss_rate)]).filter((p) => p[1] != null);
        const otherPoints = dates.map((d, i) => [x(i), d.other.loss_rate == null ? null : y.y(d.other.loss_rate)]).filter((p) => p[1] != null);
        if (selectedPoints.length) svgNode("path", { d: pathFor(selectedPoints), fill: "none", stroke: palette().focus, "stroke-width": 2 }, svg);
        if (otherPoints.length) svgNode("path", { d: pathFor(otherPoints), fill: "none", stroke: palette().other, "stroke-width": 2 }, svg);
        dates.forEach((d, i) => {
          [[d.selected, palette().focus, "08–12"], [d.other, palette().other, "其他"]].forEach(([item, color, label]) => {
            if (item.loss_rate == null) return;
            const point = svgNode("circle", { cx: x(i), cy: y.y(item.loss_rate), r: 4, fill: color }, svg);
            bindTip(point, `${d.date} · ${label} · ${pct(item.loss_rate * 100)} · ${item.losses}/${item.n} 亏损 · PnL ${signedU(item.pnl)}`);
          });
        });
        const last = dates.length - 1;
        if (dates[last]?.selected.loss_rate != null) svgNode("text", { x: x(last) - 4, y: y.y(dates[last].selected.loss_rate) - 9, "text-anchor": "end", class: "tod-label" }, svg).textContent = "08–12";
        if (dates[last]?.other.loss_rate != null) svgNode("text", { x: x(last) - 4, y: y.y(dates[last].other.loss_rate) + 15, "text-anchor": "end", class: "tod-label" }, svg).textContent = "其他";
      };
      const render = () => {
        const strategyKey = strategySelect.value;
        const zoneKey = zoneSelect.value;
        const strategy = DATA.strategies[strategyKey];
        const series = strategy.zones[zoneKey];
        const compare = series.compare_08_12;
        const pText = compare.fisher_p < 0.05 ? `Fisher p=${compare.fisher_p.toFixed(3)}` : `Fisher p=${compare.fisher_p.toFixed(3)}，未达显著`;
        detail.textContent = `${strategy.label} · 参数 ${strategy.config} · 08:00–12:00 亏损率 ${pct(compare.selected.loss_rate_pct)}（n=${compare.selected.n}） vs 其他 ${pct(compare.other.loss_rate_pct)}（n=${compare.other.n}），差 ${compare.loss_rate_diff_pp >= 0 ? "+" : ""}${compare.loss_rate_diff_pp.toFixed(2)} 个百分点；${pText}。`;
        drawLoss(root.querySelector("#tod-loss-chart"), series, zoneKey);
        drawPnl(root.querySelector("#tod-pnl-chart"), series, zoneKey);
        drawRolling(root.querySelector("#tod-rolling-chart"), series, zoneKey);
        drawDaily(root.querySelector("#tod-daily-chart"), series, zoneKey);
      };
      strategyKeys.forEach((key) => {
        const option = document.createElement("option");
        option.value = key;
        option.textContent = DATA.strategies[key].label;
        strategySelect.appendChild(option);
      });
      Object.entries(DATA.zones).forEach(([key, zone]) => {
        const option = document.createElement("option");
        option.value = key;
        option.textContent = zone.label;
        zoneSelect.appendChild(option);
      });
      strategySelect.value = "risk_adjusted";
      zoneSelect.value = "asia_shanghai";
      strategySelect.addEventListener("change", render);
      zoneSelect.addEventListener("change", render);
      render();
      if (typeof ResizeObserver !== "undefined") new ResizeObserver(render).observe(root);
    })();
  </script>
</div>
'''


def main() -> None:
    args = parse_args()
    payload = build_data(args.data_root)
    rendered = HTML_TEMPLATE.replace(
        "__DATA__", json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rendered, encoding="utf-8")
    print(args.output)


if __name__ == "__main__":
    main()
