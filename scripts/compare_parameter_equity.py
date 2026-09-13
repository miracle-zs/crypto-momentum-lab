#!/usr/bin/env python3
"""Build a local, cost-aware equity-curve comparison for two gate configs.

The curves use the same event set as the three-day momentum study: only
approximate full-entry events are included.  Each valid event is treated as a
fixed-notional trade and can overlap with other symbols.  This is an event
study comparison, not a capital-constrained broker simulator.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
from array import array
from bisect import bisect_left, bisect_right
from collections import Counter
from collections import defaultdict
from datetime import UTC, datetime
from html import escape
from pathlib import Path


STATE_INTERVAL_MS = 15_000
CANDLE_INTERVAL_MS = 15 * 60 * 1_000
MAX_HOLDING_MS = 24 * 60 * 60 * 1_000


def parse_bool(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "t", "yes"}


def iso_from_ms(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, UTC).isoformat()


def parse_utc_ms(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"timestamp must include a timezone: {value!r}")
    return int(parsed.astimezone(UTC).timestamp() * 1000)


def load_events(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [
            row
            for row in csv.DictReader(handle)
            if parse_bool(row.get("full_entry_pass_approx"))
        ]


def _row_float(row: dict[str, str], *keys: str) -> float | None:
    for key in keys:
        raw = (row.get(key) or "").strip()
        if not raw:
            continue
        try:
            value = float(raw)
        except ValueError:
            continue
        if value > 0:
            return value
    return None


def load_market_data(
    paths: list[Path],
    *,
    start_ms: int,
    end_ms: int,
) -> tuple[dict[str, list[dict[str, float | int]]], dict[str, tuple[array, array]]]:
    """Load local 15s states and derive complete official 15m candles.

    The caller supplies non-overlapping state exports.  Candle completeness is
    checked from the 60 expected 15s timestamps and the source completeness
    flags, so an incomplete candle is never used as an exit trigger.
    """

    aggregates: dict[tuple[str, int], dict[str, object]] = {}
    marks: dict[str, tuple[array, array]] = {}
    for path in paths:
        with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                symbol = (row.get("symbol") or "").strip().upper()
                if not symbol:
                    continue
                try:
                    timestamp_ms = parse_utc_ms(row["bucket_start"])
                except (KeyError, TypeError, ValueError):
                    continue
                if timestamp_ms < start_ms or timestamp_ms >= end_ms:
                    continue
                price = _row_float(row, "close_price", "midpoint", "mark_price")
                if price is not None:
                    timestamp_values, price_values = marks.setdefault(
                        symbol,
                        (array("q"), array("d")),
                    )
                    timestamp_values.append(timestamp_ms)
                    price_values.append(price)
                candle_start = (
                    timestamp_ms // CANDLE_INTERVAL_MS
                ) * CANDLE_INTERVAL_MS
                part = aggregates.setdefault(
                    (symbol, candle_start),
                    {
                        "count": 0,
                        "sum_t": 0,
                        "min_t": None,
                        "max_t": None,
                        "all_complete": True,
                        "open": None,
                        "close": None,
                    },
                )
                part["count"] = int(part["count"]) + 1
                part["sum_t"] = int(part["sum_t"]) + timestamp_ms
                part["min_t"] = (
                    timestamp_ms
                    if part["min_t"] is None
                    else min(int(part["min_t"]), timestamp_ms)
                )
                part["max_t"] = (
                    timestamp_ms
                    if part["max_t"] is None
                    else max(int(part["max_t"]), timestamp_ms)
                )
                raw_complete = (row.get("data_complete") or "").strip().lower()
                try:
                    missing = int((row.get("missing_agg_trade_count") or "0").strip() or "0")
                except ValueError:
                    missing = 1
                part["all_complete"] = bool(part["all_complete"]) and (
                    raw_complete in {"1", "true", "t"} and missing == 0
                )
                if timestamp_ms == candle_start and price is not None:
                    part["open"] = price
                if (
                    timestamp_ms == candle_start + 59 * STATE_INTERVAL_MS
                    and price is not None
                ):
                    part["close"] = price

    for symbol, (timestamp_values, price_values) in list(marks.items()):
        if all(
            timestamp_values[index] <= timestamp_values[index + 1]
            for index in range(len(timestamp_values) - 1)
        ):
            continue
        order = sorted(range(len(timestamp_values)), key=timestamp_values.__getitem__)
        marks[symbol] = (
            array("q", (timestamp_values[index] for index in order)),
            array("d", (price_values[index] for index in order)),
        )

    candles: defaultdict[str, list[dict[str, float | int]]] = defaultdict(list)
    expected_offset_sum = sum(index * STATE_INTERVAL_MS for index in range(60))
    for (symbol, candle_start), part in aggregates.items():
        expected_sum = 60 * candle_start + expected_offset_sum
        if (
            int(part["count"]) != 60
            or int(part["sum_t"]) != expected_sum
            or int(part["min_t"]) != candle_start
            or int(part["max_t"]) != candle_start + 59 * STATE_INTERVAL_MS
            or not bool(part["all_complete"])
            or part["open"] is None
            or part["close"] is None
        ):
            continue
        candles[symbol].append(
            {
                "start": candle_start,
                "end": candle_start + CANDLE_INTERVAL_MS,
                "open": float(part["open"]),
                "close": float(part["close"]),
            }
        )
    for values in candles.values():
        values.sort(key=lambda candle: int(candle["start"]))
    return dict(candles), marks


def _price_at_or_after(
    marks: tuple[array, array] | None,
    timestamp_ms: int,
) -> tuple[int, float] | None:
    if marks is None:
        return None
    timestamp_values, price_values = marks
    index = bisect_left(timestamp_values, timestamp_ms)
    if index >= len(timestamp_values):
        return None
    return int(timestamp_values[index]), float(price_values[index])


def _price_at_or_before(
    marks: tuple[array, array] | None,
    timestamp_ms: int,
) -> tuple[int, float] | None:
    if marks is None:
        return None
    timestamp_values, price_values = marks
    index = bisect_right(timestamp_values, timestamp_ms) - 1
    if index < 0:
        return None
    return int(timestamp_values[index]), float(price_values[index])


def resolve_exit(
    row: dict[str, str],
    *,
    candles: dict[str, list[dict[str, float | int]]],
    marks: dict[str, tuple[array, array]],
    end_ms: int,
) -> tuple[int, float, str] | None:
    """Apply the common paper-account candle exit to one long entry."""

    try:
        detected_ms = int(row["detected_ts"]) * 1_000
        entry_price = float(row["entry_price"])
    except (KeyError, TypeError, ValueError):
        return None
    if entry_price <= 0:
        return None
    symbol = (row.get("symbol") or "").strip().upper()
    first_eligible_start = (
        detected_ms // CANDLE_INTERVAL_MS + 1
    ) * CANDLE_INTERVAL_MS
    max_holding_ms = detected_ms + MAX_HOLDING_MS
    for candle in candles.get(symbol, []):
        candle_start = int(candle["start"])
        candle_end = int(candle["end"])
        if candle_start < first_eligible_start:
            continue
        if candle_end > max_holding_ms:
            break
        if float(candle["close"]) < float(candle["open"]):
            return candle_end, float(candle["close"]), "candle_15m_bearish"

    if max_holding_ms < end_ms:
        max_hold_mark = _price_at_or_after(marks.get(symbol), max_holding_ms)
        if max_hold_mark is not None and max_hold_mark[0] < end_ms:
            return max_hold_mark[0], max_hold_mark[1], "max_holding_period"
    end_mark = _price_at_or_before(marks.get(symbol), end_ms - 1)
    if end_mark is not None:
        return end_ms, end_mark[1], "end_of_sample_mark"
    return None


def max_drawdown(points: list[dict[str, float | int]]) -> tuple[float, float]:
    peak = float(points[0]["equity"])
    worst = 0.0
    worst_pct = 0.0
    for point in points:
        equity = float(point["equity"])
        if equity > peak:
            peak = equity
        drawdown = equity - peak
        drawdown_pct = drawdown / peak if peak else 0.0
        worst = min(worst, drawdown)
        worst_pct = min(worst_pct, drawdown_pct)
    return worst, worst_pct


def build_curve(
    events: list[dict[str, str]],
    *,
    candles: dict[str, list[dict[str, float | int]]],
    marks: dict[str, tuple[array, array]],
    initial_equity: float,
    notional: float,
    cost: float,
    start_ms: int,
    end_ms: int,
) -> dict[str, object]:
    grouped_pnl: defaultdict[int, float] = defaultdict(float)
    exit_reasons: Counter[str] = Counter()
    gross_returns: list[float] = []
    net_returns: list[float] = []
    unresolved = 0
    for row in events:
        try:
            detected_ms = int(row["detected_ts"]) * 1000
            entry_price = float(row["entry_price"])
        except (KeyError, TypeError, ValueError):
            continue
        if detected_ms < start_ms or detected_ms >= end_ms or entry_price <= 0:
            continue
        exit_data = resolve_exit(
            row,
            candles=candles,
            marks=marks,
            end_ms=end_ms,
        )
        if exit_data is None:
            unresolved += 1
            continue
        exit_ms, exit_price, exit_reason = exit_data
        gross_return = (exit_price - entry_price) / entry_price
        net_return = gross_return - cost
        grouped_pnl[exit_ms] += notional * net_return
        gross_returns.append(gross_return)
        net_returns.append(net_return)
        exit_reasons[exit_reason] += 1

    equity = initial_equity
    points: list[dict[str, float | int]] = [{"t": start_ms, "equity": equity}]
    for exit_ms in sorted(grouped_pnl):
        equity += grouped_pnl[exit_ms]
        points.append({"t": exit_ms, "equity": equity})
    if points[-1]["t"] < end_ms:
        points.append({"t": end_ms, "equity": equity})
    drawdown, drawdown_pct = max_drawdown(points)
    mean_gross = sum(gross_returns) / len(gross_returns) if gross_returns else None
    mean_net = sum(net_returns) / len(net_returns) if net_returns else None
    return {
        "points": points,
        "trades": len(net_returns),
        "mean_gross_pct": mean_gross * 100 if mean_gross is not None else None,
        "mean_net_pct": mean_net * 100 if mean_net is not None else None,
        "final_equity": equity,
        "total_return_pct": (equity / initial_equity - 1) * 100,
        "max_drawdown": drawdown,
        "max_drawdown_pct": drawdown_pct * 100,
        "exit_reason_counts": dict(exit_reasons),
        "marked_to_sample_end": exit_reasons.get("end_of_sample_mark", 0),
        "unresolved": unresolved,
    }


def render_fragment(payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    labels = payload.get("series_labels", {})
    baseline_label = escape(str(labels.get("baseline", "代码默认基线")))  # type: ignore[union-attr]
    candidate_label = escape(str(labels.get("candidate", "候选参数")))  # type: ignore[union-attr]
    return f'''<div id="equity-curve-comparison" class="equity-viz">
  <h1>两套入场参数｜统一退出策略下的权益走势</h1>
  <p class="text-small">两套参数使用同一批近似完整开单信号口径、每次名义金额 100 USDT、初始权益 1,000 USDT 和 20 bps 往返成本；允许跨 symbol 信号重叠。</p>
  <div class="equity-legend" role="group" aria-label="策略曲线">
    <button type="button" class="series-toggle" data-series="baseline" aria-pressed="true"><span class="series-swatch series-baseline" aria-hidden="true"></span><span>{baseline_label}</span></button>
    <button type="button" class="series-toggle" data-series="candidate" aria-pressed="true"><span class="series-swatch series-candidate" aria-hidden="true"></span><span>{candidate_label}</span></button>
  </div>
  <div class="equity-panels">
    <section class="equity-panel" data-panel="candle15m">
      <h2>统一退出：第一根反向 15m 完整收线（最长 24 小时）</h2>
      <svg role="img" aria-labelledby="equity-candle15m-title equity-candle15m-desc"><title id="equity-candle15m-title">统一 15 分钟反向收线退出的成本后权益曲线</title><desc id="equity-candle15m-desc">比较代码默认基线和候选入场参数在约 72 小时内的事件级权益变化。</desc></svg>
    </section>
  </div>
  <p class="sr-only" aria-live="polite">两套参数均采用第一根反向 15 分钟完整 K 线收盘退出，多头为 close 小于 open；最长持有 24 小时。样本结束仍未退出的仓位按最后一个 15 秒价格标记；这不是限制同时持仓数量的账户级撮合回测。</p>
  <div class="tooltip" role="tooltip" aria-hidden="true"></div>
</div>
<style>
#equity-curve-comparison {{
  color: var(--foreground);
  position: relative;
}}
#equity-curve-comparison .equity-legend {{
  display: flex;
  flex-wrap: wrap;
  gap: 0.8rem 1.2rem;
  margin: 0.4rem 0 0.8rem;
}}
#equity-curve-comparison .series-toggle {{
  align-items: center;
  background: transparent;
  border: 0;
  color: var(--foreground);
  cursor: pointer;
  display: inline-flex;
  gap: 0.4rem;
  padding: 0;
}}
#equity-curve-comparison .series-toggle:focus-visible {{
  outline: 2px solid var(--ring);
  outline-offset: 3px;
}}
#equity-curve-comparison .series-swatch {{
  display: inline-block;
  height: 0.7rem;
  width: 1.4rem;
}}
#equity-curve-comparison .series-baseline {{ background: var(--viz-series-1); }}
#equity-curve-comparison .series-candidate {{ background: var(--viz-series-2); }}
#equity-curve-comparison .equity-panels {{
  display: grid;
  gap: 1.3rem;
  grid-template-columns: repeat(2, minmax(0, 1fr));
}}
#equity-curve-comparison .equity-panel h2 {{
  margin: 0 0 0.15rem;
}}
#equity-curve-comparison svg {{
  display: block;
  height: auto;
  overflow: visible;
  width: 100%;
}}
#equity-curve-comparison .chart-frame {{ fill: none; stroke: var(--border); stroke-width: 1; }}
#equity-curve-comparison .chart-grid {{ stroke: var(--border); stroke-opacity: 0.45; stroke-width: 1; }}
#equity-curve-comparison .axis-label,
#equity-curve-comparison .axis-title,
#equity-curve-comparison .end-label {{ fill: var(--foreground); font-size: 12px; }}
#equity-curve-comparison .axis-title {{ font-weight: 500; }}
#equity-curve-comparison .series-line {{ fill: none; stroke-width: 2; }}
#equity-curve-comparison .hover-guide {{ stroke: var(--muted-foreground); stroke-width: 1; stroke-dasharray: 3 3; }}
#equity-curve-comparison .hover-marker {{ stroke: var(--background); stroke-width: 2; }}
#equity-curve-comparison .chart-hit {{ cursor: crosshair; }}
#equity-curve-comparison > .tooltip {{
  background: var(--popover);
  border: 1px solid var(--border);
  color: var(--popover-foreground);
  display: none;
  max-width: 220px;
  padding: 0.45rem 0.6rem;
  pointer-events: none;
  position: absolute;
  z-index: 2;
}}
#equity-curve-comparison > .tooltip .tooltip-row {{ white-space: nowrap; }}
@media (max-width: 640px) {{
  #equity-curve-comparison .equity-panels {{ grid-template-columns: 1fr; }}
}}
</style>
<script src="https://cdn.jsdelivr.net/npm/d3@7.9.0/dist/d3.min.js"></script>
<script>
(() => {{
  const payload = {encoded};
  const root = document.getElementById("equity-curve-comparison");
  if (!root || typeof d3 === "undefined") return;
  const visible = {{ baseline: true, candidate: true }};
  const seriesMeta = {{
    baseline: {{ label: payload.series_labels.baseline, color: "var(--viz-series-1)" }},
    candidate: {{ label: payload.series_labels.candidate, color: "var(--viz-series-2)" }}
  }};
  const panels = new Map(payload.panels.map((panel) => [panel.key, panel]));
  const tooltip = root.querySelector(":scope > .tooltip");
  const money = (value) => `${{Number(value).toFixed(1)}} USDT`;
  const signedPct = (value) => `${{value >= 0 ? "+" : ""}}${{Number(value).toFixed(2)}}%`;
  const formatTime = d3.utcFormat("%m-%d %H:%M");

  function element(name, attributes = {{}}) {{
    const node = document.createElementNS("http://www.w3.org/2000/svg", name);
    for (const [key, value] of Object.entries(attributes)) node.setAttribute(key, value);
    return node;
  }}

  function interpolate(values, timestamp) {{
    if (!values.length) return null;
    const center = d3.bisector((item) => item.t).center(values, timestamp);
    if (center <= 0) return values[0].equity;
    if (center >= values.length - 1) return values[values.length - 1].equity;
    const left = values[center - 1];
    const right = values[center];
    if (right.t === left.t) return right.equity;
    const ratio = (timestamp - left.t) / (right.t - left.t);
    return left.equity + (right.equity - left.equity) * ratio;
  }}

  function drawPanel(panel, svg, holder) {{
    const width = Math.max(320, holder.clientWidth || 320);
    const height = width < 420 ? 248 : 276;
    const margin = {{ top: 32, right: 16, bottom: 42, left: 64 }};
    const plotWidth = width - margin.left - margin.right;
    const plotHeight = height - margin.top - margin.bottom;
    svg.replaceChildren();
    svg.setAttribute("viewBox", `0 0 ${{width}} ${{height}}`);
    const allPoints = Object.values(panel.series).flatMap((series) => series.points);
    const timeExtent = d3.extent(allPoints, (point) => point.t);
    const x = d3.scaleUtc().domain([new Date(timeExtent[0]), new Date(timeExtent[1])]).range([margin.left, width - margin.right]);
    const equityExtent = d3.extent(allPoints, (point) => point.equity);
    const span = Math.max(1, equityExtent[1] - equityExtent[0]);
    const y = d3.scaleLinear()
      .domain([Math.min(payload.initial_equity, equityExtent[0]) - span * 0.08, Math.max(payload.initial_equity, equityExtent[1]) + span * 0.08])
      .nice()
      .range([height - margin.bottom, margin.top]);
    const frame = element("rect", {{ class: "chart-frame", "data-chart-frame": "true", x: margin.left, y: margin.top, width: plotWidth, height: plotHeight }});
    svg.append(frame);
    const yTicks = y.ticks(width < 420 ? 4 : 5);
    for (const tick of yTicks) {{
      const yPos = y(tick);
      svg.append(element("line", {{ class: "chart-grid", x1: margin.left, x2: width - margin.right, y1: yPos, y2: yPos }}));
      const label = element("text", {{ class: "axis-label", x: margin.left - 8, y: yPos + 4, "text-anchor": "end" }});
      label.textContent = `$${{Math.round(tick)}}`;
      svg.append(label);
    }}
    const tickDates = d3.utcTicks(new Date(timeExtent[0]), new Date(timeExtent[1]), width < 420 ? 3 : 5);
    for (const tick of tickDates) {{
      const xPos = x(tick);
      svg.append(element("line", {{ class: "chart-grid", x1: xPos, x2: xPos, y1: margin.top, y2: height - margin.bottom }}));
      const label = element("text", {{ class: "axis-label", x: xPos, y: height - margin.bottom + 18, "text-anchor": "middle" }});
      label.textContent = formatTime(tick);
      svg.append(label);
    }}
    const xTitle = element("text", {{ class: "axis-title", "data-axis": "x", x: margin.left + plotWidth / 2, y: height - 4, "text-anchor": "middle" }});
    xTitle.textContent = "时间（UTC）";
    svg.append(xTitle);
    const yTitle = element("text", {{ class: "axis-title", "data-axis": "y", transform: `translate(14 ${{margin.top + plotHeight / 2}}) rotate(-90)`, "text-anchor": "middle" }});
    yTitle.textContent = "权益（USDT）";
    svg.append(yTitle);

    const line = d3.line().x((point) => x(new Date(point.t))).y((point) => y(point.equity));
    for (const [key, meta] of Object.entries(seriesMeta)) {{
      if (!visible[key]) continue;
      const path = element("path", {{ class: "series-line", "data-series": key, d: line(panel.series[key].points), stroke: meta.color }});
      svg.append(path);
    }}
    const guide = element("line", {{ class: "hover-guide", "data-chart-hover-guide": "true", x1: 0, x2: 0, y1: margin.top, y2: height - margin.bottom, visibility: "hidden" }});
    svg.append(guide);
    const markers = {{}};
    for (const key of Object.keys(seriesMeta)) {{
      const marker = element("circle", {{ class: "hover-marker", "data-chart-hover-marker": "true", "data-series": key, r: 4.5, fill: seriesMeta[key].color, visibility: "hidden" }});
      svg.append(marker);
      markers[key] = marker;
    }}
    const hit = element("rect", {{ class: "chart-hit", "data-chart-hit": "true", "data-chart-hover-overlay": "cross-series", x: margin.left, y: margin.top, width: plotWidth, height: plotHeight, fill: "transparent" }});
    svg.append(hit);
    hit.addEventListener("pointermove", (event) => {{
      const [pointerX] = d3.pointer(event, svg);
      const clampedX = Math.max(margin.left, Math.min(width - margin.right, pointerX));
      const timestamp = x.invert(clampedX).getTime();
      guide.setAttribute("x1", clampedX);
      guide.setAttribute("x2", clampedX);
      guide.setAttribute("visibility", "visible");
      const rows = [];
      for (const [key, meta] of Object.entries(seriesMeta)) {{
        if (!visible[key]) {{ markers[key].setAttribute("visibility", "hidden"); continue; }}
        const value = interpolate(panel.series[key].points, timestamp);
        markers[key].setAttribute("cx", clampedX);
        markers[key].setAttribute("cy", y(value));
        markers[key].setAttribute("visibility", "visible");
        rows.push(`<div class="tooltip-row">${{meta.label}}：${{money(value)}}</div>`);
      }}
      const svgRect = svg.getBoundingClientRect();
      const rootRect = root.getBoundingClientRect();
      tooltip.innerHTML = `<div class="tooltip-row">${{formatTime(new Date(timestamp))}} UTC</div>${{rows.join("")}}`;
      tooltip.style.display = "block";
      tooltip.setAttribute("aria-hidden", "false");
      tooltip.style.left = `${{Math.min(root.clientWidth - 232, svgRect.left - rootRect.left + clampedX + 12)}}px`;
      tooltip.style.top = `${{Math.max(8, svgRect.top - rootRect.top + y(payload.initial_equity) - 20)}}px`;
    }});
    hit.addEventListener("pointerleave", () => {{
      guide.setAttribute("visibility", "hidden");
      for (const marker of Object.values(markers)) marker.setAttribute("visibility", "hidden");
      tooltip.style.display = "none";
      tooltip.setAttribute("aria-hidden", "true");
    }});
    const title = holder.querySelector("h2");
    const base = panel.series.baseline;
    const candidate = panel.series.candidate;
    title.textContent = `${{panel.title}} · ${{seriesMeta.baseline.label}} $${{base.final_equity.toFixed(1)}}（${{signedPct(base.total_return_pct)}}） / ${{seriesMeta.candidate.label}} $${{candidate.final_equity.toFixed(1)}}（${{signedPct(candidate.total_return_pct)}}）`;
  }}

  function renderAll() {{
    for (const holder of root.querySelectorAll(".equity-panel")) {{
      const key = holder.dataset.panel;
      const svg = holder.querySelector("svg");
      drawPanel(panels.get(key), svg, holder);
    }}
  }}

  for (const button of root.querySelectorAll(".series-toggle")) {{
    button.addEventListener("click", () => {{
      const key = button.dataset.series;
      visible[key] = !visible[key];
      button.setAttribute("aria-pressed", String(visible[key]));
      renderAll();
    }});
  }}
  renderAll();
  new ResizeObserver(() => renderAll()).observe(root);
}})();
</script>
'''


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument(
        "--states",
        nargs="+",
        type=Path,
        required=True,
        help="Non-overlapping local 15s state exports used for the common exit replay.",
    )
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--json-output", type=Path)
    parser.add_argument(
        "--baseline-label",
        default="代码默认基线：1% / 0.50 / 2x",
    )
    parser.add_argument(
        "--candidate-label",
        default="候选参数：1% / 0.40 / 3x",
    )
    parser.add_argument("--initial-equity", type=float, default=1000.0)
    parser.add_argument("--notional", type=float, default=100.0)
    parser.add_argument("--round-trip-cost", type=float, default=0.002)
    args = parser.parse_args()
    start_ms = parse_utc_ms(args.start)
    end_ms = parse_utc_ms(args.end)
    if end_ms <= start_ms:
        raise ValueError("end must be after start")
    event_sets = {
        "baseline": load_events(args.baseline),
        "candidate": load_events(args.candidate),
    }
    candles, marks = load_market_data(
        args.states,
        start_ms=start_ms,
        end_ms=end_ms,
    )
    panels = [
        {
            "key": "candle15m",
            "title": "统一退出：第一根反向 15m 完整收线（最长 24 小时）",
            "series": {
                name: build_curve(
                    events,
                    candles=candles,
                    marks=marks,
                    initial_equity=args.initial_equity,
                    notional=args.notional,
                    cost=args.round_trip_cost,
                    start_ms=start_ms,
                    end_ms=end_ms,
                )
                for name, events in event_sets.items()
            },
        }
    ]
    payload = {
        "coverage_start": iso_from_ms(start_ms),
        "coverage_end": iso_from_ms(end_ms),
        "initial_equity": args.initial_equity,
        "notional": args.notional,
        "round_trip_cost_bps": args.round_trip_cost * 10_000,
        "series_labels": {
            "baseline": args.baseline_label,
            "candidate": args.candidate_label,
        },
        "exit_policy": {
            "mode": "candle_15m",
            "label": "第一根反向 15m 完整收线，最长持有 24 小时",
            "long_adverse_candle": "close < open",
            "sample_end_mark": "最后一个可用 15s close",
        },
        "panels": panels,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(render_fragment(payload), encoding="utf-8")
    if args.json_output is not None:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    print(
        json.dumps(
            {
                "output": str(args.output),
                "panels": [
                    {
                        "key": panel["key"],
                        "series": {
                            name: {
                                key: value
                                for key, value in details.items()
                                if key != "points"
                            }
                            for name, details in panel["series"].items()
                        },
                    }
                    for panel in panels
                ],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
