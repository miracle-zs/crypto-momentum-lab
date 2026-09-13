#!/usr/bin/env python3
"""Build one 08:00-aligned equity chart per day with all accounts overlaid."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def prepare_payload(source: Path) -> dict:
    raw = json.loads(source.read_text(encoding="utf-8"))
    accounts = sorted(raw["accounts"], key=lambda row: row["account_id"])
    account_ids = {row["account_id"] for row in accounts}
    grouped: dict[str, list[dict]] = {}
    for series in raw["series"]:
        if series["account_id"] not in account_ids:
            continue
        grouped.setdefault(series["date"], []).append(
            {
                "series_id": series["series_id"],
                "account_id": series["account_id"],
                "label": series["label"],
                "date": series["date"],
                "complete_day": series["complete_day"],
                "change_usdt": series["change_usdt"],
                "points": series["points"],
            }
        )
    dates = [
        {"date": date, "series": sorted(rows, key=lambda row: row["account_id"])}
        for date, rows in sorted(grouped.items())
    ]
    deltas = [
        point["delta"]
        for day in dates
        for series in day["series"]
        for point in series["points"]
    ]
    y_min = min([0.0, *deltas])
    y_max = max([0.0, *deltas])
    return {
        "timezone": raw["timezone"],
        "grid_minutes": raw["grid_minutes"],
        "accounts": [
            {
                "account_id": row["account_id"],
                "label": row["label"],
                "source": row["source"],
                "run_id": row["run_id"],
            }
            for row in accounts
        ],
        "dates": dates,
        "y_extent": [y_min, y_max],
    }


def build_html(path: Path, payload: dict) -> None:
    data_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    template = r'''<div id="daily-all-accounts-aligned">
  <h1>每日账户权益变化（08:00 对齐，全部账户叠加）</h1>
  <p class="subtitle">每张图代表一个北京时间自然日；纵轴为相对当日 08:00 的权益变化（USDT），虚线为尚未结束的当前日。</p>
  <div class="account-legend" aria-label="账户图例"></div>
  <div class="daily-charts"></div>
  <div class="tooltip" role="tooltip" aria-hidden="true"></div>
</div>
<style>
#daily-all-accounts-aligned {
  --series-1: var(--viz-series-1);
  --series-2: var(--viz-series-2);
  --series-3: var(--viz-series-3);
  --series-4: var(--viz-series-4);
  --series-5: var(--viz-series-5);
  --series-6: var(--viz-series-6);
  color: var(--foreground);
  font-size: var(--font-size-base);
  line-height: 1.35;
  position: relative;
  width: 100%;
}
#daily-all-accounts-aligned h1,
#daily-all-accounts-aligned h2,
#daily-all-accounts-aligned p {
  margin: 0;
}
#daily-all-accounts-aligned h1,
#daily-all-accounts-aligned h2 {
  font-weight: 500;
}
#daily-all-accounts-aligned .subtitle {
  color: var(--muted-foreground);
  margin-top: 4px;
}
#daily-all-accounts-aligned .account-legend {
  display: flex;
  flex-wrap: wrap;
  gap: 4px 12px;
  margin: 14px 0 18px;
}
#daily-all-accounts-aligned .account-legend button {
  appearance: none;
  background: transparent;
  border: 0;
  color: var(--foreground);
  cursor: pointer;
  display: inline-flex;
  align-items: center;
  gap: 5px;
  padding: 2px 0;
  font: inherit;
}
#daily-all-accounts-aligned .account-legend button[aria-pressed="false"] {
  color: var(--muted-foreground);
}
#daily-all-accounts-aligned .account-legend button:focus-visible {
  outline: 2px solid var(--ring);
  outline-offset: 2px;
}
#daily-all-accounts-aligned .swatch,
#daily-all-accounts-aligned .tooltip-swatch {
  background: currentColor;
  display: inline-block;
  flex: 0 0 9px;
  height: 9px;
  width: 9px;
}
#daily-all-accounts-aligned .daily-charts {
  display: grid;
  gap: 20px;
  grid-template-columns: minmax(0, 1fr);
}
#daily-all-accounts-aligned .day-chart {
  min-width: 0;
}
#daily-all-accounts-aligned .day-heading {
  align-items: baseline;
  display: flex;
  flex-wrap: wrap;
  gap: 6px 10px;
  margin-bottom: 3px;
}
#daily-all-accounts-aligned .day-count {
  color: var(--muted-foreground);
}
#daily-all-accounts-aligned svg {
  display: block;
  height: auto;
  overflow: visible;
  width: 100%;
}
#daily-all-accounts-aligned svg text {
  fill: var(--foreground);
  font-size: 12px;
}
#daily-all-accounts-aligned svg .tick text,
#daily-all-accounts-aligned svg .axis-title {
  fill: var(--muted-foreground);
}
#daily-all-accounts-aligned svg .axis-title {
  font-size: 12px;
}
#daily-all-accounts-aligned svg .grid line {
  stroke: var(--border);
  stroke-opacity: .55;
  shape-rendering: crispEdges;
}
#daily-all-accounts-aligned svg .chart-frame,
#daily-all-accounts-aligned svg .axis path,
#daily-all-accounts-aligned svg .axis line {
  fill: none;
  shape-rendering: crispEdges;
  stroke: var(--border);
}
#daily-all-accounts-aligned svg .zero-line {
  stroke: var(--muted-foreground);
  stroke-dasharray: 3 3;
  stroke-opacity: .75;
}
#daily-all-accounts-aligned svg .account-line {
  fill: none;
  stroke-width: 1.5;
  vector-effect: non-scaling-stroke;
}
#daily-all-accounts-aligned svg .account-line.is-partial {
  stroke-dasharray: 5 3;
}
#daily-all-accounts-aligned svg .hover-guide {
  pointer-events: none;
  stroke: var(--muted-foreground);
  stroke-dasharray: 2 3;
}
#daily-all-accounts-aligned svg .hover-marker {
  fill: var(--background);
  pointer-events: none;
  stroke-width: 1.5;
  vector-effect: non-scaling-stroke;
}
#daily-all-accounts-aligned .tooltip {
  background: var(--popover);
  border: 1px solid var(--border);
  color: var(--popover-foreground);
  display: none;
  font-size: 12px;
  max-width: min(380px, calc(100% - 12px));
  padding: 7px 9px;
  pointer-events: none;
  position: absolute;
  z-index: 2;
}
#daily-all-accounts-aligned .tooltip-title {
  font-weight: 500;
  margin-bottom: 4px;
}
#daily-all-accounts-aligned .tooltip-row {
  align-items: center;
  display: flex;
  gap: 5px;
  white-space: nowrap;
}
@media (max-width: 420px) {
  #daily-all-accounts-aligned .account-legend {
    gap: 3px 9px;
  }
  #daily-all-accounts-aligned svg text,
  #daily-all-accounts-aligned svg .axis-title {
    font-size: 11px;
  }
}
</style>
<script src="https://cdn.jsdelivr.net/npm/d3@7.9.0/dist/d3.min.js"></script>
<script>
(() => {
  const root = document.getElementById("daily-all-accounts-aligned");
  const data = __DATA_JSON__;
  if (!root || !window.d3) return;

  const colors = ["var(--series-1)", "var(--series-2)", "var(--series-3)", "var(--series-4)", "var(--series-5)", "var(--series-6)"];
  const accounts = data.accounts;
  const enabled = new Map(accounts.map((account, index) => [account.account_id, true]));
  const colorForAccount = accountId => colors[Math.max(0, accounts.findIndex(account => account.account_id === accountId)) % colors.length];
  const formatMoney = value => `${value >= 0 ? "+" : ""}${value.toFixed(2)} USDT`;
  const formatTime = minutes => {
    const total = Math.round(minutes);
    const hour = (8 + Math.floor(total / 60)) % 24;
    const minute = total % 60;
    return `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`;
  };
  const extent = d3.extent([...data.dates.flatMap(day => day.series.flatMap(series => series.points.map(point => point.delta))), 0]);
  const span = Math.max(1, extent[1] - extent[0]);
  const yDomain = [extent[0] - span * 0.08, extent[1] + span * 0.08];
  const legend = root.querySelector(".account-legend");
  const charts = root.querySelector(".daily-charts");
  const tooltip = root.querySelector(".tooltip");

  accounts.forEach(account => {
    const button = document.createElement("button");
    button.type = "button";
    button.setAttribute("aria-pressed", "true");
    button.dataset.accountId = account.account_id;
    button.innerHTML = `<span class="swatch" style="color:${colorForAccount(account.account_id)}"></span><span>${account.label}</span>`;
    button.addEventListener("click", () => {
      const next = !enabled.get(account.account_id);
      enabled.set(account.account_id, next);
      button.setAttribute("aria-pressed", String(next));
      drawAll();
    });
    legend.appendChild(button);
  });

  data.dates.forEach(day => {
    const section = document.createElement("section");
    section.className = "day-chart";
    section.dataset.date = day.date;
    const heading = document.createElement("div");
    heading.className = "day-heading";
    const title = document.createElement("h2");
    title.textContent = day.date;
    const count = document.createElement("span");
    count.className = "day-count";
    count.textContent = `${day.series.length} 个账户`;
    heading.append(title, count);
    const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    svg.setAttribute("role", "img");
    svg.setAttribute("aria-label", `${day.date} 全部账户 08:00 对齐权益变化`);
    section.append(heading, svg);
    charts.appendChild(section);
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
    const svg = section.querySelector("svg");
    const guide = section.querySelector("[data-chart-hover-guide]");
    guide.setAttribute("x1", x(minutes));
    guide.setAttribute("x2", x(minutes));
    guide.style.display = null;
    section.querySelectorAll("[data-chart-hover-marker]").forEach(marker => marker.remove());
    const markerLayer = d3.select(svg).select(".hover-markers");
    const rows = [];
    visibleSeries.forEach(series => {
      const delta = interpolatePoint(series.points, minutes);
      if (delta == null) return;
      markerLayer.append("circle")
        .attr("class", "hover-marker")
        .attr("data-chart-hover-marker", series.series_id)
        .attr("cx", x(minutes))
        .attr("cy", y(delta))
        .attr("r", 3)
        .style("stroke", colorForAccount(series.account_id));
      rows.push(`<div class="tooltip-row"><span class="tooltip-swatch" style="color:${colorForAccount(series.account_id)}"></span><span>${series.label}: ${formatMoney(delta)}</span></div>`);
    });
    const rootRect = root.getBoundingClientRect();
    const chartRect = svg.getBoundingClientRect();
    tooltip.innerHTML = `<div class="tooltip-title">${section.dataset.date} · ${formatTime(minutes)}</div>${rows.join("")}`;
    tooltip.style.display = rows.length ? "block" : "none";
    tooltip.setAttribute("aria-hidden", rows.length ? "false" : "true");
    const left = chartRect.right - rootRect.left + 10;
    const top = chartRect.top - rootRect.top + 12;
    tooltip.style.left = `${Math.max(4, Math.min(root.clientWidth - tooltip.offsetWidth - 4, left))}px`;
    tooltip.style.top = `${Math.max(4, top)}px`;
  }

  function draw(section, day) {
    const svg = d3.select(section.querySelector("svg"));
    const width = Math.max(320, section.clientWidth || 320);
    const height = width < 420 ? 220 : 232;
    const margin = { top: 12, right: 12, bottom: 42, left: width < 420 ? 62 : 68 };
    const innerWidth = width - margin.left - margin.right;
    const innerHeight = height - margin.top - margin.bottom;
    svg.attr("viewBox", `0 0 ${width} ${height}`);
    svg.selectAll("*").remove();
    const visibleSeries = day.series.filter(series => enabled.get(series.account_id));
    const x = d3.scaleLinear().domain([0, 1440]).range([margin.left, width - margin.right]);
    const y = d3.scaleLinear().domain(yDomain).nice().range([height - margin.bottom, margin.top]);
    const clipId = `daily-overlay-clip-${day.date.replace(/[^0-9]/g, "")}`;
    svg.append("defs").append("clipPath").attr("id", clipId).append("rect")
      .attr("x", margin.left).attr("y", margin.top).attr("width", innerWidth).attr("height", innerHeight);
    svg.append("rect").attr("class", "chart-frame").attr("data-chart-frame", "true")
      .attr("x", margin.left).attr("y", margin.top).attr("width", innerWidth).attr("height", innerHeight);
    const yGrid = d3.axisLeft(y).ticks(width < 420 ? 4 : 5).tickSize(-innerWidth).tickFormat(value => `${value.toFixed(0)}`);
    const xAxis = d3.axisBottom(x).ticks(width < 420 ? 4 : 6).tickFormat(value => formatTime(value));
    svg.append("g").attr("class", "grid").attr("transform", `translate(${margin.left},0)`).call(yGrid);
    svg.select(".grid .domain").remove();
    svg.append("line").attr("class", "zero-line").attr("x1", margin.left).attr("x2", width - margin.right).attr("y1", y(0)).attr("y2", y(0));
    svg.append("g").attr("class", "axis").attr("transform", `translate(0,${height - margin.bottom})`).call(xAxis);
    svg.append("g").attr("class", "axis").attr("transform", `translate(${margin.left},0)`).call(d3.axisLeft(y).ticks(width < 420 ? 4 : 5).tickFormat(value => `${value.toFixed(0)}`));
    svg.append("text").attr("class", "axis-title").attr("data-axis", "x").attr("x", margin.left + innerWidth / 2).attr("y", height - 5).attr("text-anchor", "middle").text("本地时间（UTC+8）");
    svg.append("text").attr("class", "axis-title").attr("data-axis", "y").attr("transform", `translate(14,${margin.top + innerHeight / 2}) rotate(-90)`).attr("text-anchor", "middle").text("权益变化（USDT）");
    const plot = svg.append("g").attr("clip-path", `url(#${clipId})`);
    const line = d3.line().x(point => x(point.m)).y(point => y(point.delta));
    visibleSeries.forEach(series => {
      plot.append("path").datum(series.points)
        .attr("class", `account-line${series.complete_day ? "" : " is-partial"}`)
        .attr("data-series-id", series.series_id)
        .attr("d", line)
        .style("stroke", colorForAccount(series.account_id));
    });
    svg.append("g").attr("class", "hover-markers");
    svg.append("line").attr("class", "hover-guide").attr("data-chart-hover-guide", "true")
      .attr("y1", margin.top).attr("y2", height - margin.bottom).attr("x1", margin.left).attr("x2", margin.left).style("display", "none");
    const overlay = svg.append("rect").attr("data-chart-hit", "true").attr("data-chart-hover-overlay", "cross-series")
      .attr("x", margin.left).attr("y", margin.top).attr("width", innerWidth).attr("height", innerHeight).attr("fill", "transparent")
      .on("pointermove", event => showTooltip(section, event, x, y, visibleSeries, overlay))
      .on("pointerleave", () => {
        section.querySelector("[data-chart-hover-guide]").style.display = "none";
        section.querySelectorAll("[data-chart-hover-marker]").forEach(marker => marker.remove());
        tooltip.style.display = "none";
        tooltip.setAttribute("aria-hidden", "true");
      });
  }

  function drawAll() {
    data.dates.forEach(day => {
      const section = root.querySelector(`.day-chart[data-date="${day.date}"]`);
      if (section) draw(section, day);
    });
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
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    build_html(args.output, prepare_payload(args.input))
    print(args.output)


if __name__ == "__main__":
    main()
