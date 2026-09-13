#!/usr/bin/env python3
"""Align server account equity curves at 08:00 Asia/Shanghai.

The live account is represented by wallet balance plus unrealized PnL. Paper
accounts already expose mark-to-market equity. The input files are deliberately
small server-side downsampled exports; this script resamples both sources to a
common 30-minute grid for a comparable view.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
import statistics
from bisect import bisect_left
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable


UTC = timezone.utc
CN_TZ = timezone(timedelta(hours=8))
GRID_MINUTES = 30
DAY_MINUTES = 24 * 60


def parse_datetime(value: str) -> datetime:
    text = value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def to_float(value: str | None) -> float | None:
    if value is None or not value.strip():
        return None
    try:
        parsed = float(value)
    except ValueError:
        return None
    return parsed if math.isfinite(parsed) else None


def open_text(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", newline="")
    return path.open("r", encoding="utf-8", newline="")


def compact_label(run_id: str, strategy_name: str | None = None) -> str:
    if run_id.startswith("paper-account-"):
        short = run_id.removeprefix("paper-account-").removesuffix("-v1")
        return f"P{short}"
    if run_id.startswith("live-"):
        return run_id
    return strategy_name or run_id


def load_run_metadata(path: Path | None) -> dict[str, dict[str, str]]:
    if path is None or not path.exists():
        return {}
    metadata: dict[str, dict[str, str]] = {}
    with open_text(path) as handle:
        for row in csv.DictReader(handle):
            run_id = row.get("run_id", "").strip()
            if run_id:
                metadata[run_id] = {
                    "strategy_name": row.get("strategy_name", "").strip(),
                    "run_mode": row.get("run_mode", "").strip(),
                }
    return metadata


def add_observation(
    target: dict[str, dict[str, Any]],
    account_id: str,
    label: str,
    source: str,
    run_id: str,
    observed_at: datetime,
    equity: float,
) -> None:
    if not math.isfinite(equity):
        return
    account = target.setdefault(
        account_id,
        {
            "account_id": account_id,
            "label": label,
            "source": source,
            "run_id": run_id,
            "observations": [],
        },
    )
    account["observations"].append((observed_at, equity))


def load_observations(
    live_path: Path,
    paper_path: Path,
    runs_path: Path | None,
) -> dict[str, dict[str, Any]]:
    accounts: dict[str, dict[str, Any]] = {}
    with open_text(live_path) as handle:
        for row in csv.DictReader(handle):
            if row.get("environment") != "live" or row.get("asset") != "USDT":
                continue
            label = row.get("account_label", "primary").strip() or "primary"
            wallet = to_float(row.get("wallet_balance"))
            unrealized = to_float(row.get("unrealized_pnl"))
            observed_at = row.get("observed_at")
            if wallet is None or unrealized is None or not observed_at:
                continue
            add_observation(
                accounts,
                f"live/{label}",
                f"L/{label}",
                "live",
                f"live-{label}",
                parse_datetime(observed_at),
                wallet + unrealized,
            )

    metadata = load_run_metadata(runs_path)
    with open_text(paper_path) as handle:
        for row in csv.DictReader(handle):
            run_id = row.get("run_id", "").strip()
            observed_at = row.get("observed_at")
            equity = to_float(row.get("equity"))
            if not run_id or not observed_at or equity is None:
                continue
            strategy_name = metadata.get(run_id, {}).get("strategy_name")
            add_observation(
                accounts,
                f"paper/{run_id}",
                compact_label(run_id, strategy_name),
                "paper",
                run_id,
                parse_datetime(observed_at),
                equity,
            )

    for account in accounts.values():
        deduplicated: dict[datetime, float] = {}
        for observed_at, equity in account["observations"]:
            deduplicated[observed_at] = equity
        account["observations"] = sorted(deduplicated.items())
    return accounts


def interpolate(
    timestamps: list[datetime], values: list[float], target: datetime
) -> float | None:
    if not timestamps or target < timestamps[0] or target > timestamps[-1]:
        return None
    right = bisect_left(timestamps, target)
    if right == 0:
        return values[0]
    if right == len(timestamps):
        return values[-1]
    if timestamps[right] == target:
        return values[right]
    left = right - 1
    span = (timestamps[right] - timestamps[left]).total_seconds()
    if span <= 0:
        return values[left]
    weight = (target - timestamps[left]).total_seconds() / span
    return values[left] + weight * (values[right] - values[left])


def add_grid_point(
    points: list[dict[str, float]], minutes: float, equity: float, baseline: float
) -> None:
    delta = equity - baseline
    point = {
        "m": round(minutes, 3),
        "delta": round(delta, 6),
        "pct": round(delta / baseline * 100, 6) if baseline else None,
    }
    if points and abs(points[-1]["m"] - point["m"]) < 0.001:
        points[-1] = point
    else:
        points.append(point)


def daily_series(account: dict[str, Any]) -> list[dict[str, Any]]:
    observations: list[tuple[datetime, float]] = account["observations"]
    if not observations:
        return []
    timestamps = [row[0] for row in observations]
    values = [row[1] for row in observations]
    local_dates = sorted({ts.astimezone(CN_TZ).date() for ts in timestamps})
    result: list[dict[str, Any]] = []
    for local_day in local_dates:
        local_start = datetime.combine(local_day, time(8), tzinfo=CN_TZ)
        start = local_start.astimezone(UTC)
        end = (local_start + timedelta(days=1)).astimezone(UTC)
        if start < timestamps[0] or start > timestamps[-1]:
            continue
        baseline = interpolate(timestamps, values, start)
        if baseline is None or not math.isfinite(baseline) or abs(baseline) < 1e-9:
            continue
        available_end = min(end, timestamps[-1])
        if available_end <= start:
            continue
        points: list[dict[str, float]] = []
        for minutes in range(0, DAY_MINUTES + 1, GRID_MINUTES):
            target = start + timedelta(minutes=minutes)
            if target > available_end:
                break
            equity = interpolate(timestamps, values, target)
            if equity is not None:
                add_grid_point(points, minutes, equity, baseline)
        elapsed = (available_end - start).total_seconds() / 60
        last_equity = interpolate(timestamps, values, available_end)
        if last_equity is not None:
            add_grid_point(points, elapsed, last_equity, baseline)
        if len(points) < 2:
            continue
        deltas = [point["delta"] for point in points]
        running_peak = deltas[0]
        max_drawdown = 0.0
        max_drawdown_at = points[0]["m"]
        for point in points:
            running_peak = max(running_peak, point["delta"])
            drawdown = running_peak - point["delta"]
            if drawdown > max_drawdown:
                max_drawdown = drawdown
                max_drawdown_at = point["m"]
        max_gain = max(deltas)
        max_loss = min(deltas)
        max_gain_at = points[deltas.index(max_gain)]["m"]
        max_loss_at = points[deltas.index(max_loss)]["m"]
        result.append(
            {
                "series_id": f"{account['account_id']}|{local_day.isoformat()}",
                "account_id": account["account_id"],
                "label": account["label"],
                "source": account["source"],
                "run_id": account["run_id"],
                "date": local_day.isoformat(),
                "baseline_equity": round(baseline, 6),
                "latest_equity": round(baseline + deltas[-1], 6),
                "change_usdt": round(deltas[-1], 6),
                "change_pct": round(deltas[-1] / baseline * 100, 6),
                "max_gain_usdt": round(max_gain, 6),
                "max_gain_at_minute": round(max_gain_at, 3),
                "max_loss_usdt": round(max_loss, 6),
                "max_loss_at_minute": round(max_loss_at, 3),
                "max_drawdown_usdt": round(max_drawdown, 6),
                "max_drawdown_at_minute": round(max_drawdown_at, 3),
                "duration_minutes": round(elapsed, 3),
                "complete_day": elapsed >= 23 * 60,
                "points": points,
            }
        )
    return result


def account_summary(account: dict[str, Any], series: list[dict[str, Any]]) -> dict[str, Any]:
    complete = [row for row in series if row["complete_day"]]
    changes = [row["change_usdt"] for row in complete]
    drawdowns = [row["max_drawdown_usdt"] for row in complete]
    latest = max(series, key=lambda row: row["date"]) if series else None
    return {
        "account_id": account["account_id"],
        "label": account["label"],
        "source": account["source"],
        "run_id": account["run_id"],
        "observation_count": len(account["observations"]),
        "first_observed_at": account["observations"][0][0].isoformat(),
        "last_observed_at": account["observations"][-1][0].isoformat(),
        "days": len(series),
        "complete_days": len(complete),
        "partial_days": len(series) - len(complete),
        "positive_days": sum(change > 0 for change in changes),
        "negative_days": sum(change < 0 for change in changes),
        "flat_days": sum(abs(change) < 1e-9 for change in changes),
        "sum_complete_day_change_usdt": round(sum(changes), 6),
        "mean_complete_day_change_usdt": round(statistics.mean(changes), 6)
        if changes
        else None,
        "median_complete_day_change_usdt": round(statistics.median(changes), 6)
        if changes
        else None,
        "mean_complete_day_drawdown_usdt": round(statistics.mean(drawdowns), 6)
        if drawdowns
        else None,
        "worst_complete_day_change_usdt": round(min(changes), 6) if changes else None,
        "best_complete_day_change_usdt": round(max(changes), 6) if changes else None,
        "latest_day": latest["date"] if latest else None,
        "latest_day_partial": bool(latest and not latest["complete_day"]),
    }


def format_minutes(minutes: float | None) -> str:
    if minutes is None:
        return "—"
    total = int(round(minutes))
    hour = (8 + total // 60) % 24
    minute = total % 60
    return f"{hour:02d}:{minute:02d}"


def round_metrics(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result.pop("points", None)
    return result


def write_csv(path: Path, series: Iterable[dict[str, Any]]) -> None:
    fields = [
        "account_id",
        "label",
        "source",
        "run_id",
        "date",
        "minutes_from_0800",
        "observed_equity",
        "delta_usdt",
        "delta_pct",
        "baseline_equity",
        "complete_day",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for daily in series:
            for point in daily["points"]:
                writer.writerow(
                    {
                        "account_id": daily["account_id"],
                        "label": daily["label"],
                        "source": daily["source"],
                        "run_id": daily["run_id"],
                        "date": daily["date"],
                        "minutes_from_0800": point["m"],
                        "observed_equity": round(
                            daily["baseline_equity"] + point["delta"], 6
                        ),
                        "delta_usdt": point["delta"],
                        "delta_pct": point["pct"],
                        "baseline_equity": daily["baseline_equity"],
                        "complete_day": daily["complete_day"],
                    }
                )


def write_report(
    path: Path,
    accounts: list[dict[str, Any]],
    series: list[dict[str, Any]],
    data: dict[str, Any],
) -> None:
    complete_series = [row for row in series if row["complete_day"]]
    ranked = sorted(complete_series, key=lambda row: row["change_usdt"], reverse=True)
    ranked_accounts = sorted(
        accounts,
        key=lambda row: row["sum_complete_day_change_usdt"]
        if row["sum_complete_day_change_usdt"] is not None
        else float("-inf"),
        reverse=True,
    )
    lines = [
        "# 每日账户权益波动（08:00 对齐）",
        "",
        f"- 时间口径：Asia/Shanghai（UTC+8），每日窗口为 08:00 到次日 08:00。",
        f"- 账户权益口径：实盘为 `wallet_balance + unrealized_pnl`；虚拟盘直接使用 `equity`。",
        f"- 曲线采样：共同重采样到 {data['grid_minutes']} 分钟；虚线/标记为尚未结束的当前日。",
        f"- 覆盖：{len(accounts)} 个账户，{len(series)} 个账户日，其中完整日 {len(complete_series)} 个。",
        "",
        "## 完整日账户汇总",
        "",
        "| 排名 | 账户 | 完整日 | 盈利日/亏损日 | 完整日变化合计（USDT） | 日均变化（USDT） | 平均日内最大回撤（USDT） |",
        "|---:|---|---:|---:|---:|---:|---:|",
    ]
    for rank, account in enumerate(ranked_accounts, start=1):
        lines.append(
            "| {rank} | {label} | {days} | {positive}/{negative} | {total:.2f} | {mean:.2f} | {mdd:.2f} |".format(
                rank=rank,
                label=account["label"],
                days=account["complete_days"],
                positive=account["positive_days"],
                negative=account["negative_days"],
                total=account["sum_complete_day_change_usdt"] or 0,
                mean=account["mean_complete_day_change_usdt"] or 0,
                mdd=account["mean_complete_day_drawdown_usdt"] or 0,
            )
        )
    lines.extend(
        [
            "",
            "## 完整日表现最强/最弱的账户日",
            "",
            "| 日期 | 账户 | 08:00 起点 | 收盘变化（USDT） | 最大浮盈 | 最大浮亏 | 日内最大回撤 |",
            "|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    for row in ranked[:10] + ranked[-10:]:
        lines.append(
            "| {date} | {label} | {base:.2f} | {change:+.2f} | {gain:+.2f}（{gain_at}） | {loss:+.2f}（{loss_at}） | {mdd:.2f}（{mdd_at}） |".format(
                date=row["date"],
                label=row["label"],
                base=row["baseline_equity"],
                change=row["change_usdt"],
                gain=row["max_gain_usdt"],
                gain_at=format_minutes(row["max_gain_at_minute"]),
                loss=row["max_loss_usdt"],
                loss_at=format_minutes(row["max_loss_at_minute"]),
                mdd=row["max_drawdown_usdt"],
                mdd_at=format_minutes(row["max_drawdown_at_minute"]),
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_html(path: Path, payload: dict[str, Any]) -> None:
    data_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    template = r'''<div id="daily-account-aligned">
  <h1>每日账户权益波动（08:00 对齐）</h1>
  <p class="subtitle">纵轴为相对当日 08:00 的权益变化（USDT）；0 线为共同起点，时间为 UTC+8。</p>
  <div class="date-legend" aria-label="日期图例"></div>
  <div class="account-grid"></div>
  <div class="tooltip" role="tooltip" aria-hidden="true"></div>
</div>
<style>
#daily-account-aligned {
  --series-1: var(--viz-series-1);
  --series-2: var(--viz-series-2);
  --series-3: var(--viz-series-3);
  --series-4: var(--viz-series-4);
  --series-5: var(--viz-series-5);
  --series-6: var(--viz-series-6);
  position: relative;
  color: var(--foreground);
  font-size: var(--font-size-base);
  line-height: 1.35;
  width: 100%;
}
#daily-account-aligned h1,
#daily-account-aligned h2,
#daily-account-aligned p {
  margin: 0;
}
#daily-account-aligned h1 {
  font-weight: 500;
}
#daily-account-aligned .subtitle {
  color: var(--muted-foreground);
  margin-top: 4px;
}
#daily-account-aligned .date-legend {
  display: flex;
  flex-wrap: wrap;
  gap: 4px 12px;
  margin: 14px 0 18px;
}
#daily-account-aligned .date-legend button {
  appearance: none;
  border: 0;
  background: transparent;
  color: var(--foreground);
  cursor: pointer;
  padding: 2px 0;
  font: inherit;
  display: inline-flex;
  align-items: center;
  gap: 5px;
}
#daily-account-aligned .date-legend button[aria-pressed="false"] {
  color: var(--muted-foreground);
}
#daily-account-aligned .date-legend button:focus-visible {
  outline: 2px solid var(--ring);
  outline-offset: 2px;
}
#daily-account-aligned .swatch,
#daily-account-aligned .tooltip-swatch {
  display: inline-block;
  width: 9px;
  height: 9px;
  flex: 0 0 9px;
  background: currentColor;
}
#daily-account-aligned .account-grid {
  display: grid;
  grid-template-columns: minmax(0, 1fr);
  gap: 18px;
}
#daily-account-aligned .account-section {
  min-width: 0;
}
#daily-account-aligned .account-heading {
  display: flex;
  flex-wrap: wrap;
  align-items: baseline;
  gap: 6px 10px;
  margin-bottom: 2px;
}
#daily-account-aligned h2 {
  font-weight: 500;
  overflow-wrap: anywhere;
}
#daily-account-aligned .account-run {
  color: var(--muted-foreground);
  overflow-wrap: anywhere;
}
#daily-account-aligned .account-summary {
  color: var(--muted-foreground);
  margin-bottom: 4px;
}
#daily-account-aligned svg {
  display: block;
  width: 100%;
  height: auto;
  overflow: visible;
}
#daily-account-aligned svg text {
  fill: var(--foreground);
  font-size: 12px;
}
#daily-account-aligned svg .tick text,
#daily-account-aligned svg .axis-title {
  fill: var(--muted-foreground);
}
#daily-account-aligned svg .axis-title {
  font-size: 12px;
}
#daily-account-aligned svg .grid line {
  stroke: var(--border);
  stroke-opacity: .55;
  shape-rendering: crispEdges;
}
#daily-account-aligned svg .chart-frame,
#daily-account-aligned svg .axis path,
#daily-account-aligned svg .axis line {
  stroke: var(--border);
  fill: none;
  shape-rendering: crispEdges;
}
#daily-account-aligned svg .zero-line {
  stroke: var(--muted-foreground);
  stroke-dasharray: 3 3;
  stroke-opacity: .75;
}
#daily-account-aligned svg .equity-line {
  fill: none;
  stroke-width: 1.7;
  vector-effect: non-scaling-stroke;
}
#daily-account-aligned svg .equity-line.is-partial {
  stroke-dasharray: 5 3;
}
#daily-account-aligned svg .hover-guide {
  stroke: var(--muted-foreground);
  stroke-dasharray: 2 3;
  pointer-events: none;
}
#daily-account-aligned svg .hover-marker {
  fill: var(--background);
  stroke-width: 1.5;
  vector-effect: non-scaling-stroke;
  pointer-events: none;
}
#daily-account-aligned .tooltip {
  position: absolute;
  z-index: 2;
  pointer-events: none;
  display: none;
  max-width: min(360px, calc(100% - 12px));
  padding: 7px 9px;
  border: 1px solid var(--border);
  background: var(--popover);
  color: var(--popover-foreground);
  font-size: 12px;
}
#daily-account-aligned .tooltip .tooltip-title {
  margin-bottom: 4px;
  font-weight: 500;
}
#daily-account-aligned .tooltip-row {
  display: flex;
  align-items: center;
  gap: 5px;
  white-space: nowrap;
}
@media (max-width: 420px) {
  #daily-account-aligned .date-legend {
    gap: 3px 9px;
  }
  #daily-account-aligned svg text,
  #daily-account-aligned svg .axis-title {
    font-size: 11px;
  }
}
</style>
<script src="https://cdn.jsdelivr.net/npm/d3@7.9.0/dist/d3.min.js"></script>
<script>
(() => {
  const root = document.getElementById("daily-account-aligned");
  const data = __DATA_JSON__;
  if (!root || !window.d3) return;

  const dates = Array.from(new Set(data.series.map(series => series.date))).sort();
  const enabled = new Map(dates.map(date => [date, true]));
  const colors = ["var(--series-1)", "var(--series-2)", "var(--series-3)", "var(--series-4)", "var(--series-5)", "var(--series-6)"];
  const colorForDate = date => colors[Math.max(0, dates.indexOf(date)) % colors.length];
  const formatMoney = value => `${value >= 0 ? "+" : ""}${value.toFixed(2)} USDT`;
  const formatTime = minutes => {
    const total = Math.round(minutes);
    const hour = (8 + Math.floor(total / 60)) % 24;
    const minute = total % 60;
    return `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`;
  };
  const allDeltas = data.series.flatMap(series => series.points.map(point => point.delta));
  const extent = d3.extent([...allDeltas, 0]);
  const span = Math.max(1, extent[1] - extent[0]);
  const yDomain = [extent[0] - span * 0.08, extent[1] + span * 0.08];
  const seriesByAccount = d3.group(data.series, series => series.account_id);
  const tooltip = root.querySelector(".tooltip");
  const legend = root.querySelector(".date-legend");
  const grid = root.querySelector(".account-grid");

  dates.forEach(date => {
    const button = document.createElement("button");
    button.type = "button";
    button.setAttribute("aria-pressed", "true");
    button.dataset.date = date;
    button.innerHTML = `<span class="swatch" style="color:${colorForDate(date)}"></span><span>${date}</span>`;
    button.addEventListener("click", () => {
      const next = !enabled.get(date);
      enabled.set(date, next);
      button.setAttribute("aria-pressed", String(next));
      drawAll();
    });
    legend.appendChild(button);
  });

  data.accounts.forEach(account => {
    const section = document.createElement("section");
    section.className = "account-section";
    section.dataset.accountId = account.account_id;
    const heading = document.createElement("div");
    heading.className = "account-heading";
    const title = document.createElement("h2");
    title.textContent = account.label;
    const run = document.createElement("span");
    run.className = "account-run";
    run.textContent = account.run_id;
    heading.append(title, run);
    const summary = document.createElement("p");
    summary.className = "account-summary";
    const total = account.sum_complete_day_change_usdt;
    const mean = account.mean_complete_day_change_usdt;
    summary.textContent = `${account.complete_days} 个完整日 · 盈利 ${account.positive_days} / 亏损 ${account.negative_days} · 完整日合计 ${total == null ? "—" : formatMoney(total)} · 日均 ${mean == null ? "—" : formatMoney(mean)}`;
    const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    svg.classList.add("account-chart");
    svg.setAttribute("role", "img");
    svg.setAttribute("aria-label", `${account.label} 每日 08:00 对齐权益变化`);
    section.append(heading, summary, svg);
    grid.appendChild(section);
  });

  function interpolatePoint(points, minutes) {
    if (!points.length) return null;
    const nearest = d3.bisector(point => point.m).center(points, minutes);
    const right = d3.bisector(point => point.m).left(points, minutes);
    if (right <= 0) return points[0].delta;
    if (right >= points.length) return points[points.length - 1].delta;
    const leftPoint = points[right - 1];
    const rightPoint = points[right];
    const distance = rightPoint.m - leftPoint.m;
    if (distance <= 0) return points[nearest].delta;
    const weight = (minutes - leftPoint.m) / distance;
    return leftPoint.delta + weight * (rightPoint.delta - leftPoint.delta);
  }

  function showTooltip(section, event, x, y, visibleSeries, overlay) {
    const [pointerX] = d3.pointer(event, overlay.node());
    const minutes = Math.max(0, Math.min(1440, x.invert(pointerX)));
    section.querySelector("[data-chart-hover-guide]").setAttribute("x1", x(minutes));
    section.querySelector("[data-chart-hover-guide]").setAttribute("x2", x(minutes));
    const rows = [];
    section.querySelectorAll("[data-chart-hover-marker]").forEach(marker => marker.remove());
    const svg = section.querySelector("svg");
    const markerLayer = d3.select(svg).select(".hover-markers");
    visibleSeries.forEach(series => {
      const delta = interpolatePoint(series.points, minutes);
      if (delta == null) return;
      markerLayer.append("circle")
        .attr("class", "hover-marker")
        .attr("data-chart-hover-marker", series.series_id)
        .attr("cx", x(minutes))
        .attr("cy", y(delta))
        .attr("r", 3.2)
        .style("stroke", colorForDate(series.date));
      rows.push(`<div class="tooltip-row"><span class="tooltip-swatch" style="color:${colorForDate(series.date)}"></span><span>${series.date}: ${formatMoney(delta)}</span></div>`);
    });
    const rootRect = root.getBoundingClientRect();
    const chartRect = svg.getBoundingClientRect();
    tooltip.innerHTML = `<div class="tooltip-title">${formatTime(minutes)} · ${section.querySelector("h2").textContent}</div>${rows.join("")}`;
    tooltip.style.display = rows.length ? "block" : "none";
    tooltip.setAttribute("aria-hidden", rows.length ? "false" : "true");
    const left = chartRect.right - rootRect.left + 10;
    const top = chartRect.top - rootRect.top + 12;
    tooltip.style.left = `${Math.max(4, Math.min(root.clientWidth - tooltip.offsetWidth - 4, left))}px`;
    tooltip.style.top = `${Math.max(4, top)}px`;
  }

  function draw(section) {
    const svg = d3.select(section.querySelector("svg"));
    const width = Math.max(320, section.clientWidth || 320);
    const height = width < 420 ? 220 : 232;
    const margin = { top: 12, right: 12, bottom: 42, left: width < 420 ? 62 : 68 };
    const innerWidth = width - margin.left - margin.right;
    const innerHeight = height - margin.top - margin.bottom;
    svg.attr("viewBox", `0 0 ${width} ${height}`);
    svg.selectAll("*").remove();
    const accountSeries = (seriesByAccount.get(section.dataset.accountId) || []).slice().sort((a, b) => a.date.localeCompare(b.date));
    const visibleSeries = accountSeries.filter(series => enabled.get(series.date));
    const x = d3.scaleLinear().domain([0, 1440]).range([margin.left, width - margin.right]);
    const y = d3.scaleLinear().domain(yDomain).nice().range([height - margin.bottom, margin.top]);
    const clipId = `daily-account-clip-${Array.from(grid.children).indexOf(section)}`;
    svg.append("defs").append("clipPath").attr("id", clipId).append("rect")
      .attr("x", margin.left).attr("y", margin.top).attr("width", innerWidth).attr("height", innerHeight);
    svg.append("rect").attr("class", "chart-frame").attr("data-chart-frame", "true")
      .attr("x", margin.left).attr("y", margin.top).attr("width", innerWidth).attr("height", innerHeight);
    const yAxis = d3.axisLeft(y).ticks(width < 420 ? 4 : 5).tickSize(-innerWidth).tickFormat(value => `${value.toFixed(0)}`);
    const xAxis = d3.axisBottom(x).ticks(width < 420 ? 4 : 6).tickFormat(value => formatTime(value));
    svg.append("g").attr("class", "grid").attr("transform", `translate(${margin.left},0)`).call(yAxis);
    svg.select(".grid .domain").remove();
    svg.append("line").attr("class", "zero-line").attr("x1", margin.left).attr("x2", width - margin.right).attr("y1", y(0)).attr("y2", y(0));
    svg.append("g").attr("class", "axis").attr("transform", `translate(0,${height - margin.bottom})`).call(xAxis);
    svg.append("g").attr("class", "axis").attr("transform", `translate(${margin.left},0)`).call(d3.axisLeft(y).ticks(width < 420 ? 4 : 5).tickFormat(value => `${value.toFixed(0)}`));
    svg.append("text").attr("class", "axis-title").attr("data-axis", "x").attr("x", margin.left + innerWidth / 2).attr("y", height - 5).attr("text-anchor", "middle").text("本地时间（UTC+8）");
    svg.append("text").attr("class", "axis-title").attr("data-axis", "y").attr("transform", `translate(14,${margin.top + innerHeight / 2}) rotate(-90)`).attr("text-anchor", "middle").text("权益变化（USDT）");
    const plot = svg.append("g").attr("clip-path", `url(#${clipId})`);
    const line = d3.line().x(point => x(point.m)).y(point => y(point.delta));
    visibleSeries.forEach(series => {
      plot.append("path").datum(series.points).attr("class", `equity-line${series.complete_day ? "" : " is-partial"}`)
        .attr("data-series-id", series.series_id).attr("d", line).style("stroke", colorForDate(series.date));
    });
    const markerLayer = svg.append("g").attr("class", "hover-markers");
    const guide = svg.append("line").attr("class", "hover-guide").attr("data-chart-hover-guide", "true")
      .attr("y1", margin.top).attr("y2", height - margin.bottom).attr("x1", margin.left).attr("x2", margin.left).style("display", "none");
    const overlay = svg.append("rect").attr("data-chart-hit", "true").attr("data-chart-hover-overlay", "cross-series")
      .attr("x", margin.left).attr("y", margin.top).attr("width", innerWidth).attr("height", innerHeight).attr("fill", "transparent")
      .on("pointermove", event => { guide.style("display", null); showTooltip(section, event, x, y, visibleSeries, overlay); })
      .on("pointerleave", () => { guide.style("display", "none"); markerLayer.selectAll("[data-chart-hover-marker]").remove(); tooltip.style.display = "none"; tooltip.setAttribute("aria-hidden", "true"); });
  }

  function drawAll() {
    root.querySelectorAll(".account-section").forEach(draw);
  }
  const observer = new ResizeObserver(drawAll);
  observer.observe(root);
  drawAll();
})();
</script>
'''
    path.write_text(template.replace("__DATA_JSON__", data_json), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", type=Path, required=True)
    parser.add_argument("--paper", type=Path, required=True)
    parser.add_argument("--runs", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--html", type=Path, required=True)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    args.html.parent.mkdir(parents=True, exist_ok=True)
    accounts_by_id = load_observations(args.live, args.paper, args.runs)
    daily_by_account = {
        account_id: daily_series(account)
        for account_id, account in accounts_by_id.items()
    }
    all_series = [series for rows in daily_by_account.values() for series in rows]
    summaries = [
        account_summary(accounts_by_id[account_id], daily_by_account[account_id])
        for account_id in sorted(accounts_by_id)
        if daily_by_account[account_id]
    ]
    summaries.sort(key=lambda row: (row["source"], row["label"]))
    all_series.sort(key=lambda row: (row["account_id"], row["date"]))
    payload = {
        "timezone": "Asia/Shanghai",
        "grid_minutes": GRID_MINUTES,
        "metric": "equity_delta_from_0800_usdt",
        "accounts": summaries,
        "series": all_series,
    }
    write_csv(args.output_dir / "daily_account_alignment.csv", all_series)
    (args.output_dir / "daily_account_alignment.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    write_report(
        args.output_dir / "daily_account_alignment_report.md",
        summaries,
        all_series,
        payload,
    )
    build_html(args.html, payload)
    complete = [row for row in all_series if row["complete_day"]]
    ranked = sorted(complete, key=lambda row: row["change_usdt"], reverse=True)
    print(
        json.dumps(
            {
                "accounts": len(summaries),
                "series": len(all_series),
                "complete_days": len(complete),
                "best": round_metrics(ranked[0]) if ranked else None,
                "worst": round_metrics(ranked[-1]) if ranked else None,
                "html": str(args.html),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
