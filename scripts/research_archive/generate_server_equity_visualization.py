from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


ACCOUNT_ORDER = [
    "paper-account-01-compression-original-fixed-v1",
    "paper-account-02-compression-original-candle15m-v1",
    "paper-account-02-orderflow-v1",
    "paper-account-05-orderflow-candle15m-v1",
    "paper-account-07-orderflow-candle45m-v1",
    "paper-account-03-liquidation-v1",
    "paper-account-06-liquidation-candle15m-v1",
    "paper-account-08-liquidation-candle2confirm-v1",
]

ACCOUNT_LABELS = {
    ACCOUNT_ORDER[0]: "01 压缩突破｜固定止盈止损",
    ACCOUNT_ORDER[1]: "02 压缩突破｜15 分钟反向收线",
    ACCOUNT_ORDER[2]: "02 订单流｜固定止盈止损",
    ACCOUNT_ORDER[3]: "05 订单流｜15 分钟反向收线",
    ACCOUNT_ORDER[4]: "07 订单流｜45 分钟后反向收线",
    ACCOUNT_ORDER[5]: "03 清算级联｜固定止盈止损",
    ACCOUNT_ORDER[6]: "06 清算级联｜15 分钟反向收线",
    ACCOUNT_ORDER[7]: "08 清算级联｜连续 2 根反向收线",
}


def parse_timestamp(value: str) -> datetime:
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


def load_payload(input_path: Path) -> list[dict[str, object]]:
    grouped: dict[str, list[list[object]]] = defaultdict(list)
    with input_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            run_id = row["run_id"]
            observed_at = parse_timestamp(row["observed_at"])
            grouped[run_id].append(
                [
                    int(observed_at.timestamp() * 1000),
                    round(float(row["equity"]), 4),
                ]
            )

    ordered_ids = [run_id for run_id in ACCOUNT_ORDER if run_id in grouped]
    ordered_ids.extend(sorted(set(grouped).difference(ordered_ids)))
    payload: list[dict[str, object]] = []
    for run_id in ordered_ids:
        values = sorted(grouped[run_id], key=lambda item: item[0])
        payload.append(
            {
                "id": run_id,
                "label": ACCOUNT_LABELS.get(run_id, run_id),
                "values": values,
            }
        )
    if len(payload) != 8:
        raise ValueError(f"expected 8 accounts, found {len(payload)}")
    return payload


def build_html(payload: list[dict[str, object]]) -> str:
    data_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return f"""<div id="server-paper-equity-visualization" class="server-paper-equity">
  <h2>八个虚拟账户权益走势</h2>
  <p class="equity-caption">服务器最新快照｜每 5 分钟取该账户最新权益｜时间轴：UTC+8｜单位：USDT</p>
  <div class="equity-legend" aria-label="账户图例"></div>
  <div class="equity-chart-shell">
    <svg class="equity-chart" role="img" aria-label="八个虚拟账户的权益时序图"></svg>
    <div class="equity-tooltip" role="tooltip" aria-hidden="true"></div>
  </div>
  <p class="equity-accessible-summary sr-only">图中展示服务器上八个虚拟账户从各自启动时间到最新快照的权益变化；悬停图线可查看同一时刻各账户权益。</p>
</div>
<style>
#server-paper-equity-visualization {{
  color: var(--foreground);
  font-family: inherit;
  width: 100%;
}}
#server-paper-equity-visualization h2 {{
  color: var(--foreground);
  font-size: 18px;
  font-weight: 500;
  line-height: 1.35;
  margin: 0 0 4px;
}}
#server-paper-equity-visualization .equity-caption {{
  color: var(--muted-foreground);
  font-size: 12px;
  line-height: 1.4;
  margin: 0 0 10px;
}}
#server-paper-equity-visualization .equity-legend {{
  display: flex;
  flex-wrap: wrap;
  gap: 4px 14px;
  margin: 0 0 6px;
}}
#server-paper-equity-visualization .equity-legend button {{
  align-items: center;
  background: transparent;
  border: 0;
  color: var(--foreground);
  cursor: pointer;
  display: inline-flex;
  font: inherit;
  font-size: 12px;
  gap: 5px;
  padding: 2px 0;
}}
#server-paper-equity-visualization .equity-legend button:focus-visible {{
  outline: 2px solid var(--ring);
  outline-offset: 2px;
}}
#server-paper-equity-visualization .equity-legend button[aria-pressed="false"] {{
  color: var(--muted-foreground);
  text-decoration: line-through;
}}
#server-paper-equity-visualization .equity-swatch {{
  background: var(--series-color);
  display: inline-block;
  height: 3px;
  width: 18px;
}}
#server-paper-equity-visualization .equity-chart-shell {{
  position: relative;
  width: 100%;
}}
#server-paper-equity-visualization .equity-chart {{
  display: block;
  max-width: 100%;
  overflow: visible;
  width: 100%;
}}
#server-paper-equity-visualization .equity-chart text {{
  fill: var(--foreground);
  font-size: 12px;
}}
#server-paper-equity-visualization .equity-chart .axis path,
#server-paper-equity-visualization .equity-chart .axis line {{
  stroke: var(--border);
  shape-rendering: crispEdges;
}}
#server-paper-equity-visualization .equity-chart .grid line {{
  stroke: var(--border);
  opacity: 0.45;
  shape-rendering: crispEdges;
}}
#server-paper-equity-visualization .equity-chart .grid path {{
  display: none;
}}
#server-paper-equity-visualization .equity-chart .chart-frame {{
  fill: none;
  stroke: var(--border);
  stroke-width: 1;
}}
#server-paper-equity-visualization .equity-chart .equity-line {{
  fill: none;
  stroke-linecap: round;
  stroke-linejoin: round;
  stroke-width: 1.5;
}}
#server-paper-equity-visualization .equity-chart .initial-line {{
  stroke: var(--muted-foreground);
  stroke-dasharray: 4 4;
  stroke-width: 1;
}}
#server-paper-equity-visualization .equity-chart .initial-label {{
  fill: var(--muted-foreground);
  font-size: 11px;
}}
#server-paper-equity-visualization .equity-chart .hover-guide {{
  stroke: var(--muted-foreground);
  stroke-dasharray: 2 3;
  pointer-events: none;
}}
#server-paper-equity-visualization .equity-chart .hover-marker {{
  fill: var(--background);
  pointer-events: none;
  stroke-width: 2;
}}
#server-paper-equity-visualization .equity-tooltip {{
  background: var(--popover);
  border: 1px solid var(--border);
  color: var(--popover-foreground);
  display: none;
  font-size: 12px;
  line-height: 1.4;
  max-width: min(330px, calc(100% - 16px));
  padding: 7px 9px;
  pointer-events: none;
  position: absolute;
  z-index: 2;
}}
#server-paper-equity-visualization .equity-tooltip.is-visible {{
  display: block;
}}
#server-paper-equity-visualization .tooltip-time {{
  border-bottom: 1px solid var(--border);
  color: var(--muted-foreground);
  margin-bottom: 4px;
  padding-bottom: 3px;
}}
#server-paper-equity-visualization .tooltip-row {{
  align-items: baseline;
  display: flex;
  gap: 8px;
  justify-content: space-between;
}}
#server-paper-equity-visualization .tooltip-name {{
  color: var(--popover-foreground);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}}
#server-paper-equity-visualization .tooltip-value {{
  color: var(--popover-foreground);
  font-variant-numeric: tabular-nums;
  white-space: nowrap;
}}
#server-paper-equity-visualization .sr-only {{
  height: 1px;
  margin: -1px;
  overflow: hidden;
  position: absolute;
  width: 1px;
  clip: rect(0, 0, 0, 0);
}}
</style>
<script src="https://cdn.jsdelivr.net/npm/d3@7.9.0/dist/d3.min.js"></script>
<script>
(() => {{
  const root = document.getElementById("server-paper-equity-visualization");
  if (!root || !window.d3) return;

  const rawSeries = {data_json};
  const series = rawSeries.map((item) => ({{
    ...item,
    values: item.values.map(([time, equity]) => [new Date(time), equity]),
  }}));
  const visible = series.map(() => true);
  const legend = root.querySelector(".equity-legend");
  const shell = root.querySelector(".equity-chart-shell");
  const svgNode = root.querySelector(".equity-chart");
  const tooltip = root.querySelector(".equity-tooltip");
  const initialEquity = 1000;
  const colorIndex = (index) => (index % 6) + 1;
  let chartState = null;
  let lastHoverTime = null;

  const timeFormatter = new Intl.DateTimeFormat("zh-CN", {{
    timeZone: "Asia/Shanghai",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }});
  const numberFormatter = new Intl.NumberFormat("zh-CN", {{
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }});

  series.forEach((item, index) => {{
    const button = document.createElement("button");
    button.type = "button";
    button.setAttribute("aria-pressed", "true");
    button.setAttribute("aria-label", item.label + "：切换显示");
    const swatch = document.createElement("span");
    swatch.className = "equity-swatch";
    swatch.style.setProperty("--series-color", "var(--viz-series-" + colorIndex(index) + ")");
    const text = document.createElement("span");
    text.textContent = item.label;
    button.append(swatch, text);
    button.addEventListener("click", () => {{
      visible[index] = !visible[index];
      button.setAttribute("aria-pressed", String(visible[index]));
      if (chartState) {{
        chartState.paths[index].style("display", visible[index] ? null : "none");
        chartState.markers[index].style("display", visible[index] ? null : "none");
        if (lastHoverTime !== null) updateHover(lastHoverTime);
      }}
    }});
    legend.append(button);
  }});

  function interpolate(item, time) {{
    const values = item.values;
    if (!values.length) return null;
    const nearest = d3.bisector((point) => point[0]).center(values, new Date(time));
    const leftIndex = Math.max(0, Math.min(values.length - 1, nearest));
    const rightIndex = Math.min(values.length - 1, leftIndex + 1);
    const left = values[leftIndex];
    const right = values[rightIndex];
    if (leftIndex === rightIndex || right[0].getTime() === left[0].getTime()) return left[1];
    const ratio = (time - left[0].getTime()) / (right[0].getTime() - left[0].getTime());
    return left[1] + (right[1] - left[1]) * Math.max(0, Math.min(1, ratio));
  }}

  function positionTooltip(event) {{
    const [rootX, rootY] = d3.pointer(event, root);
    const tooltipWidth = tooltip.offsetWidth;
    const tooltipHeight = tooltip.offsetHeight;
    const left = Math.min(Math.max(8, rootX + 14), root.clientWidth - tooltipWidth - 8);
    const top = Math.min(Math.max(8, rootY - tooltipHeight - 10), shell.clientHeight - tooltipHeight - 8);
    tooltip.style.left = left + "px";
    tooltip.style.top = top + "px";
  }}

  function updateTooltip(time) {{
    tooltip.replaceChildren();
    const header = document.createElement("div");
    header.className = "tooltip-time";
    header.textContent = timeFormatter.format(new Date(time)) + " UTC+8";
    tooltip.append(header);
    series.forEach((item, index) => {{
      if (!visible[index]) return;
      const value = interpolate(item, time);
      if (value === null) return;
      const row = document.createElement("div");
      row.className = "tooltip-row";
      const name = document.createElement("span");
      name.className = "tooltip-name";
      name.textContent = item.label;
      const amount = document.createElement("span");
      amount.className = "tooltip-value";
      amount.textContent = numberFormatter.format(value) + " USDT";
      row.append(name, amount);
      tooltip.append(row);
    }});
    tooltip.classList.add("is-visible");
    tooltip.setAttribute("aria-hidden", "false");
  }}

  function updateHover(time, event) {{
    if (!chartState) return;
    lastHoverTime = time;
    const xPosition = chartState.x(new Date(time));
    chartState.guide
      .attr("x1", xPosition)
      .attr("x2", xPosition)
      .style("display", null);
    series.forEach((item, index) => {{
      const value = visible[index] ? interpolate(item, time) : null;
      chartState.markers[index]
        .attr("cx", xPosition)
        .attr("cy", value === null ? 0 : chartState.y(value))
        .style("display", value === null ? "none" : null);
    }});
    updateTooltip(time);
    if (event) positionTooltip(event);
  }}

  function hideHover() {{
    lastHoverTime = null;
    if (!chartState) return;
    chartState.guide.style("display", "none");
    chartState.markers.forEach((marker) => marker.style("display", "none"));
    tooltip.classList.remove("is-visible");
    tooltip.setAttribute("aria-hidden", "true");
  }}

  function render() {{
    const width = Math.max(320, shell.getBoundingClientRect().width);
    const height = Math.max(300, Math.min(500, width * 0.56));
    const narrow = width < 520;
    const margin = {{top: 18, right: 16, bottom: narrow ? 58 : 52, left: narrow ? 62 : 70}};
    const innerWidth = width - margin.left - margin.right;
    const innerHeight = height - margin.top - margin.bottom;
    const allValues = series.flatMap((item) => item.values);
    const xDomain = d3.extent(allValues, (point) => point[0]);
    const observedEquities = allValues.map((point) => point[1]).concat([initialEquity]);
    const equityExtent = d3.extent(observedEquities);
    const equityPadding = Math.max(5, (equityExtent[1] - equityExtent[0]) * 0.08);
    const x = d3.scaleTime().domain(xDomain).range([0, innerWidth]);
    const y = d3.scaleLinear()
      .domain([equityExtent[0] - equityPadding, equityExtent[1] + equityPadding])
      .nice()
      .range([innerHeight, 0]);
    const line = d3.line()
      .defined((point) => Number.isFinite(point[1]))
      .x((point) => x(point[0]))
      .y((point) => y(point[1]));
    const svg = d3.select(svgNode);
    svg.selectAll("*").remove();
    svg.attr("viewBox", "0 0 " + width + " " + height);
    const plot = svg.append("g").attr("transform", "translate(" + margin.left + "," + margin.top + ")");
    plot.append("rect")
      .attr("class", "chart-frame")
      .attr("data-chart-frame", "true")
      .attr("width", innerWidth)
      .attr("height", innerHeight);
    plot.append("g")
      .attr("class", "grid")
      .call(d3.axisLeft(y).ticks(narrow ? 5 : 7).tickSize(-innerWidth).tickFormat(""));
    const xAxis = plot.append("g")
      .attr("class", "axis")
      .attr("transform", "translate(0," + innerHeight + ")")
      .call(d3.axisBottom(x).ticks(narrow ? 4 : 7).tickFormat((value) => timeFormatter.format(value)));
    xAxis.selectAll("text").style("font-size", narrow ? "11px" : "12px");
    plot.append("g")
      .attr("class", "axis")
      .call(d3.axisLeft(y).ticks(narrow ? 5 : 7).tickFormat((value) => d3.format(",.0f")(value)));
    plot.append("text")
      .attr("class", "axis-title")
      .attr("data-axis", "x")
      .attr("x", innerWidth / 2)
      .attr("y", innerHeight + margin.bottom - 12)
      .attr("text-anchor", "middle")
      .text("时间（UTC+8）");
    plot.append("text")
      .attr("class", "axis-title")
      .attr("data-axis", "y")
      .attr("transform", "rotate(-90)")
      .attr("x", -innerHeight / 2)
      .attr("y", -margin.left + 16)
      .attr("text-anchor", "middle")
      .text("权益（USDT）");
    plot.append("line")
      .attr("class", "initial-line")
      .attr("x1", 0)
      .attr("x2", innerWidth)
      .attr("y1", y(initialEquity))
      .attr("y2", y(initialEquity));
    plot.append("text")
      .attr("class", "initial-label")
      .attr("x", innerWidth - 4)
      .attr("y", y(initialEquity) - 5)
      .attr("text-anchor", "end")
      .text("初始 1,000");
    const paths = series.map((item, index) => plot.append("path")
      .datum(item.values)
      .attr("class", "equity-line")
      .attr("d", line)
      .attr("stroke", "var(--viz-series-" + colorIndex(index) + ")")
      .attr("stroke-dasharray", index >= 6 ? (index === 6 ? "5 3" : "2 2") : null)
      .style("display", visible[index] ? null : "none"));
    const guide = plot.append("line")
      .attr("class", "hover-guide")
      .attr("y1", 0)
      .attr("y2", innerHeight)
      .style("display", "none");
    const markers = series.map((item, index) => plot.append("circle")
      .attr("class", "hover-marker")
      .attr("r", 4)
      .attr("stroke", "var(--viz-series-" + colorIndex(index) + ")")
      .style("display", "none"));
    const overlay = plot.append("rect")
      .attr("data-chart-hit", "true")
      .attr("data-chart-hover-overlay", "cross-series")
      .attr("x", 0)
      .attr("y", 0)
      .attr("width", innerWidth)
      .attr("height", innerHeight)
      .attr("fill", "transparent")
      .on("pointermove", (event) => {{
        const [pointerX] = d3.pointer(event, overlay.node());
        updateHover(x.invert(pointerX).getTime(), event);
      }})
      .on("pointerleave", hideHover);
    chartState = {{x, y, paths, markers, guide, overlay}};
    if (lastHoverTime !== null) updateHover(lastHoverTime);
  }}

  render();
  if (typeof ResizeObserver !== "undefined") {{
    new ResizeObserver(render).observe(shell);
  }} else {{
    window.addEventListener("resize", render);
  }}
}})();
</script>
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    payload = load_payload(args.input)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(build_html(payload), encoding="utf-8")
    print(f"wrote {args.output} ({len(payload)} accounts)")


if __name__ == "__main__":
    main()
