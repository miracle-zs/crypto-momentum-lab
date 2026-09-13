#!/usr/bin/env python3
"""Compare a local research candidate with the live equity and margin paths.

The research equity curve is an absolute fixed-notional replay anchored to the
live equity at the local collection start.  It is not a claim that the local
Parquet data reproduces live fills.  The live equity curve comes from exported
account balance snapshots; live margin is reconstructed from exported fills.
"""

from __future__ import annotations

import argparse
import bisect
import csv
import gzip
import json
import math
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


def parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def iso(value: datetime) -> str:
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def number(value: str | None, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        result = float(value)
    except ValueError:
        return default
    return result if math.isfinite(result) else default


def read_csv(path: Path) -> list[dict[str, str]]:
    opener = gzip.open if path.suffix == ".gz" else Path.open
    with opener(path, "rt", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def interpolate(points: list[tuple[float, float]], timestamp: float) -> float:
    if not points:
        return float("nan")
    if timestamp <= points[0][0]:
        return points[0][1]
    if timestamp >= points[-1][0]:
        return points[-1][1]
    right = bisect.bisect_right([item[0] for item in points], timestamp)
    left = right - 1
    t0, v0 = points[left]
    t1, v1 = points[right]
    ratio = (timestamp - t0) / (t1 - t0) if t1 != t0 else 0.0
    return v0 + (v1 - v0) * ratio


def step_value(points: list[tuple[float, float]], timestamp: float) -> float:
    if not points:
        return 0.0
    index = bisect.bisect_right([item[0] for item in points], timestamp) - 1
    return points[index][1] if index >= 0 else 0.0


def truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "t", "yes", "y"}


def aggregate_margin_events(deltas: list[tuple[float, float]]) -> list[tuple[float, float]]:
    """Convert margin deltas into a step function of open initial margin."""
    current = 0.0
    points: list[tuple[float, float]] = []
    for timestamp, delta in sorted(deltas):
        current = max(0.0, current + delta)
        if points and points[-1][0] == timestamp:
            points[-1] = (timestamp, current)
        else:
            points.append((timestamp, current))
    return points


def load_live_margin_events(
    fill_events_path: Path,
    exchange_orders_path: Path,
    *,
    leverage: float,
) -> tuple[list[tuple[float, float]], dict[str, int]]:
    """Estimate live long initial-margin occupancy from actual fill records."""
    orders = {
        row.get("exchange_order_id", ""): row
        for row in read_csv(exchange_orders_path)
        if row.get("exchange_order_id")
    }
    fills: list[tuple[float, str, str, float, float, str]] = []
    for row in read_csv(fill_events_path):
        if row.get("environment") != "live" or row.get("account_label") != "primary":
            continue
        order = orders.get(row.get("order_id", ""))
        if order is None:
            continue
        try:
            timestamp = parse_time(row["trade_at"]).timestamp()
            quantity = number(row.get("quantity"))
            price = number(row.get("price"))
        except (KeyError, ValueError):
            continue
        if quantity <= 0 or price <= 0:
            continue
        side = (row.get("side") or order.get("side") or "").upper()
        position_side = (order.get("position_side") or "").upper()
        reduce_only = truthy(order.get("reduce_only"))
        if position_side != "LONG":
            continue
        if side == "BUY" and not reduce_only:
            kind = "entry"
        elif side == "SELL" and reduce_only:
            kind = "exit"
        else:
            continue
        fills.append(
            (
                timestamp,
                row.get("symbol", ""),
                kind,
                quantity,
                price,
                row.get("order_id", ""),
            )
        )
    fills.sort(key=lambda item: (item[0], item[5]))

    lots: dict[str, list[list[float]]] = {}
    deltas: list[tuple[float, float]] = []
    entry_fill_rows = 0
    exit_fill_rows = 0
    entry_order_ids: set[str] = set()
    unmatched_exit_quantity = 0.0
    for timestamp, symbol, kind, quantity, price, order_id in fills:
        if kind == "entry":
            margin = quantity * price / leverage
            lots.setdefault(symbol, []).append([quantity, margin])
            deltas.append((timestamp, margin))
            entry_fill_rows += 1
            entry_order_ids.add(order_id)
            continue

        remaining = quantity
        released = 0.0
        symbol_lots = lots.setdefault(symbol, [])
        while remaining > 1e-12 and symbol_lots:
            lot_quantity, lot_margin = symbol_lots[0]
            consumed = min(remaining, lot_quantity)
            released += lot_margin * consumed / lot_quantity
            remaining -= consumed
            lot_quantity -= consumed
            if lot_quantity <= 1e-12:
                symbol_lots.pop(0)
            else:
                symbol_lots[0] = [lot_quantity, lot_margin]
        if released > 0:
            deltas.append((timestamp, -released))
        unmatched_exit_quantity += remaining
        exit_fill_rows += 1

    return aggregate_margin_events(deltas), {
        "entry_fill_rows": entry_fill_rows,
        "entry_order_count": len(entry_order_ids),
        "exit_fill_rows": exit_fill_rows,
        "unmatched_exit_quantity": round(unmatched_exit_quantity, 12),
    }


def load_simulated_margin_events(
    candidate_events_path: Path,
    *,
    entry_notional: float,
    leverage: float,
) -> tuple[list[tuple[float, float]], dict[str, int]]:
    """Build simulated margin occupancy from filled candidate entries."""
    margin_per_entry = entry_notional / leverage
    deltas: list[tuple[float, float]] = []
    filled_entries = 0
    closed_entries = 0
    for row in read_csv(candidate_events_path):
        entry_at = row.get("entry_at")
        if not entry_at:
            continue
        try:
            entry_timestamp = parse_time(entry_at).timestamp()
        except ValueError:
            continue
        deltas.append((entry_timestamp, margin_per_entry))
        filled_entries += 1
        exit_at = row.get("exit_at")
        if exit_at:
            try:
                deltas.append((parse_time(exit_at).timestamp(), -margin_per_entry))
            except ValueError:
                pass
            else:
                closed_entries += 1
    return aggregate_margin_events(deltas), {
        "filled_entries": filled_entries,
        "closed_entries": closed_entries,
    }


def margin_summary(
    points: list[tuple[float, float]],
    *,
    start: datetime,
    end: datetime,
) -> dict[str, float]:
    values = [step_value(points, start.timestamp()), step_value(points, end.timestamp())]
    values.extend(
        value for timestamp, value in points if start.timestamp() <= timestamp <= end.timestamp()
    )
    return {
        "start_usdt": round(values[0], 8),
        "peak_usdt": round(max(values), 8),
        "end_usdt": round(values[1], 8),
    }


def drawdown(values: list[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    peak = values[0]
    best = 0.0
    best_pct = 0.0
    for value in values:
        if value > peak:
            peak = value
        dd = peak - value
        dd_pct = dd / peak * 100.0 if peak else 0.0
        if dd > best:
            best = dd
            best_pct = dd_pct
    return best, best_pct


def sample_grid(start: datetime, end: datetime, limit: int = 720) -> list[datetime]:
    if end <= start:
        return [start]
    count = min(limit, max(2, int((end - start).total_seconds() / 180) + 1))
    step = (end - start).total_seconds() / (count - 1)
    return [start + timedelta(seconds=step * index) for index in range(count)]


def fmt_money(value: float) -> str:
    return f"{value:+.2f}U"


def build_html(report: dict[str, Any]) -> str:
    live = report["live"]
    research = report["research"]
    margin = report["margin"]
    live_start = float(live["raw_start_usdt"])
    live_end = float(live["raw_end_usdt"])
    live_change = float(live["raw_change_usdt"])
    candidate_pnl = float(research["full_candidate_pnl_usdt"])
    candidate_end = live_start + candidate_pnl
    payload = json.dumps(report["chart"], ensure_ascii=False, separators=(",", ":"))
    cards = {
        "live_raw": f"{live_start:.2f}U → {live_end:.2f}U",
        "live_change": fmt_money(live_change),
        "research_candidate": f"{live_start:.2f}U → {candidate_end:.2f}U",
        "research_change": fmt_money(candidate_pnl),
        "validation": fmt_money(research["validation_net_pnl_usdt"]),
        "holdout": fmt_money(research["holdout_net_pnl_usdt"]),
        "duration": report["collection"]["duration_text"],
        "collection_start": report["collection"]["start_utc"],
    }
    template = '''<div id="research-vs-live-equity" aria-label="本地采集寻优与实盘账户权益对比">
  <style>
    #research-vs-live-equity {
      --fg: light-dark(#17202a, #edf2f7);
      --muted: light-dark(#64748b, #aab6c3);
      --border: light-dark(#cbd5e1, #465568);
      --grid: light-dark(#e2e8f0, #334155);
      --blue: light-dark(#1769aa, #63b3ed);
      --orange: light-dark(#c05621, #f6ad55);
      --green: light-dark(#2f855a, #68d391);
      --red: light-dark(#b5482f, #f28f79);
      --violet: light-dark(#6b46c1, #b794f4);
      --card: light-dark(#f8fafc, #1c2733);
      color: var(--fg);
      display: block;
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      font-size: 12px;
      line-height: 1.4;
      position: relative;
      width: 100%;
    }
    #research-vs-live-equity .title { font-size: 17px; font-weight: 700; margin: 0 0 3px; }
    #research-vs-live-equity .subtitle { color: var(--muted); margin: 0 0 12px; }
    #research-vs-live-equity .section-title { font-size: 14px; font-weight: 600; margin: 14px 0 3px; }
    #research-vs-live-equity .cards { display: grid; grid-template-columns: repeat(5, minmax(105px, 1fr)); gap: 7px; margin-bottom: 12px; }
    #research-vs-live-equity .card { background: var(--card); border: 1px solid var(--border); border-radius: 7px; padding: 8px 9px; }
    #research-vs-live-equity .label { color: var(--muted); font-size: 11px; }
    #research-vs-live-equity .value { font-size: 15px; font-weight: 700; margin-top: 2px; }
    #research-vs-live-equity .negative { color: var(--red); }
    #research-vs-live-equity .legend { color: var(--muted); display: flex; flex-wrap: wrap; gap: 14px; margin: 5px 0 8px; }
    #research-vs-live-equity .swatch { display: inline-block; height: 3px; margin: 0 5px 3px 0; width: 16px; }
    #research-vs-live-equity svg { display: block; height: auto; overflow: visible; width: 100%; }
    #research-vs-live-equity text { fill: var(--fg); font-size: 11px; }
    #research-vs-live-equity .muted { fill: var(--muted); }
    #research-vs-live-equity .frame { fill: none; stroke: var(--border); stroke-width: 1; }
    #research-vs-live-equity .grid-line { stroke: var(--grid); stroke-dasharray: 2 3; opacity: .75; }
    #research-vs-live-equity .split-line { stroke: var(--violet); stroke-dasharray: 5 4; opacity: .8; }
    #research-vs-live-equity .tooltip { background: light-dark(#fff, #1c2733); border: 1px solid var(--border); border-radius: 5px; color: var(--fg); display: none; max-width: 290px; padding: 6px 8px; pointer-events: none; position: absolute; z-index: 3; }
    #research-vs-live-equity .note { color: var(--muted); margin: 8px 0 0; }
    @media (max-width: 760px) { #research-vs-live-equity .cards { grid-template-columns: repeat(3, minmax(110px, 1fr)); } }
    @media (max-width: 460px) { #research-vs-live-equity .cards { grid-template-columns: repeat(2, minmax(120px, 1fr)); } }
  </style>
  <div class="title">本地采集寻优 vs 同时段实盘账户权益</div>
  <p class="subtitle">纵轴单位：USDT。两条主线从本地采集起点的实盘权益开始；橙线 = 该起始权益 + 本地绝对模拟累计 PnL（__COLLECTION_START__）。</p>
  <div class="cards">
    <div class="card"><div class="label">采集区间</div><div class="value">__DURATION__</div></div>
    <div class="card"><div class="label">实盘账户权益</div><div class="value __LIVE_RAW_CLASS__">__LIVE_RAW__</div></div>
    <div class="card"><div class="label">实盘实际变化</div><div class="value __LIVE_CHANGE_CLASS__">__LIVE_CHANGE__</div></div>
    <div class="card"><div class="label">寻优模拟权益</div><div class="value __RESEARCH_CLASS__">__RESEARCH__</div></div>
    <div class="card"><div class="label">寻优模拟累计 PnL</div><div class="value __RESEARCH_CHANGE_CLASS__">__RESEARCH_CHANGE__</div></div>
    <div class="card"><div class="label">验证集净 PnL</div><div class="value">__VALIDATION__</div></div>
    <div class="card"><div class="label">留出集净 PnL</div><div class="value negative">__HOLDOUT__</div></div>
  </div>
  <div class="legend"><span><span class="swatch" style="background:var(--blue)"></span>实盘原始权益</span><span><span class="swatch" style="background:var(--orange)"></span>本地寻优候选</span><span><span class="swatch" style="background:var(--violet)"></span>训练/验证边界</span></div>
  <svg id="research-vs-live-equity-svg" viewBox="0 0 960 560" role="img" aria-label="真实实盘账户权益与绝对模拟权益两条 USDT 曲线"><g id="research-vs-live-equity-chart"></g></svg>
  <div class="tooltip" id="research-vs-live-equity-tooltip" role="tooltip"></div>
  <p class="note">蓝线是账户余额快照中的真实实盘权益；橙线把寻优回放的绝对累计 PnL 加到同一个实盘起始权益上，便于直接比较金额。橙线仍不是完整账户仿真。</p>
  <div class="section-title">保证金占用时序（初始保证金估算）</div>
  <div class="legend"><span><span class="swatch" style="background:var(--blue)"></span>实盘实际占用</span><span><span class="swatch" style="background:var(--orange)"></span>寻优模拟占用</span></div>
  <svg id="research-vs-live-margin-svg" viewBox="0 0 960 360" role="img" aria-label="真实实盘与本地寻优回放保证金占用时序"><g id="research-vs-live-margin-chart"></g></svg>
  <p class="note">实盘线按实际成交记录、同一币种 FIFO 开平仓估算持仓初始保证金；寻优线按每笔 100U 名义仓位 ÷ 5 倍杠杆（约 20U/笔）计算。这里是初始保证金占用，不是交易所维持保证金或完整账户风险保证金。</p>
  <script>
    (() => {
      const root = document.getElementById("research-vs-live-equity");
      const chart = document.getElementById("research-vs-live-equity-chart");
      const marginChart = document.getElementById("research-vs-live-margin-chart");
      const tooltip = document.getElementById("research-vs-live-equity-tooltip");
      const data = __DATA__;
      const NS = "http://www.w3.org/2000/svg";
      const W = 960, H = 560, left = 66, right = 20, top = 28, bottom = 50;
      const plotH = 390, x0 = left, x1 = W - right, y0 = top, y1 = top + plotH;
      const add = (name, attrs, parent = chart) => { const node = document.createElementNS(NS, name); Object.entries(attrs || {}).forEach(([key, value]) => node.setAttribute(key, String(value))); parent.appendChild(node); return node; };
      const text = (parent, x, y, value, attrs = {}) => { const node = add("text", {x, y, ...attrs}, parent); node.appendChild(document.createTextNode(value)); return node; };
      const finite = value => Number.isFinite(Number(value));
      const values = data.points.flatMap(row => [Number(row.live_equity_usdt), Number(row.research_equity_usdt)]);
      const minValue = Math.min(...values), maxValue = Math.max(...values);
      const span = Math.max(1, maxValue - minValue), domain = [minValue - span * .08, maxValue + span * .08];
      const y = value => y1 - (Number(value) - domain[0]) / (domain[1] - domain[0] || 1) * plotH;
      const firstTime = Date.parse(data.start), lastTime = Date.parse(data.end);
      const x = value => x0 + (Date.parse(value) - firstTime) / Math.max(1, lastTime - firstTime) * (x1 - x0);
      const ticks = Array.from({length: 5}, (_, index) => domain[0] + (domain[1] - domain[0]) * index / 4);
      add("rect", {x:x0, y:y0, width:x1-x0, height:plotH, class:"frame"});
      ticks.forEach(value => { const yy = y(value); add("line", {x1:x0, x2:x1, y1:yy, y2:yy, class:"grid-line"}); text(chart, x0-8, yy+4, value.toFixed(0), {"text-anchor":"end"}); });
      const split = (timestamp, label) => { const xx = x(timestamp); add("line", {x1:xx, x2:xx, y1:y0, y2:y1, class:"split-line"}); text(chart, xx+4, y0+14, label, {class:"muted"}); };
      split(data.train_end, "训练/验证");
      split(data.validation_end, "验证/留出");
      const line = key => data.points.map((row, index) => `${index ? "L" : "M"}${x(row.timestamp)},${y(row[key])}`).join(" ");
      add("path", {d:line("live_equity_usdt"), fill:"none", stroke:"var(--blue)", "stroke-width":2.4});
      add("path", {d:line("research_equity_usdt"), fill:"none", stroke:"var(--orange)", "stroke-width":2.2});
      [data.start, data.mid, data.end].forEach((timestamp, index) => text(chart, x(timestamp), y1+22, timestamp.replace("T", " ").replace("Z", " UTC"), {"text-anchor": index === 0 ? "start" : index === 2 ? "end" : "middle", class:"muted"}));
      text(chart, x0+3, y0+15, "账户权益 / 模拟权益（U）", {class:"muted"});
      const overlay = add("rect", {x:x0, y:y0, width:x1-x0, height:plotH, fill:"transparent"});
      overlay.addEventListener("pointermove", event => { const bounds = overlay.getBoundingClientRect(); const ratio = Math.max(0, Math.min(1, (event.clientX - bounds.left) / bounds.width)); const index = Math.max(0, Math.min(data.points.length-1, Math.round(ratio * (data.points.length-1)))); const row = data.points[index]; tooltip.innerHTML = `<strong>${row.timestamp.replace("T", " ").replace("Z", " UTC")}</strong><br>实盘账户权益：${Number(row.live_equity_usdt).toFixed(2)}U<br>寻优模拟权益：${Number(row.research_equity_usdt).toFixed(2)}U<br>模拟累计 PnL：${Number(row.research_pnl).toFixed(2)}U`; const box = root.getBoundingClientRect(); tooltip.style.display = "block"; tooltip.style.left = `${event.clientX-box.left+10}px`; tooltip.style.top = `${event.clientY-box.top+10}px`; });
      overlay.addEventListener("pointerleave", () => tooltip.style.display = "none");

      const marginData = data.margin_points;
      const marginValues = marginData.flatMap(row => [Number(row.live_margin_usdt), Number(row.research_margin_usdt)]);
      const marginMin = Math.min(0, ...marginValues), marginMax = Math.max(0, ...marginValues);
      const marginSpan = Math.max(1, marginMax - marginMin);
      const marginDomain = [Math.max(0, marginMin - marginSpan * .08), marginMax + Math.max(1, marginSpan * .08)];
      const marginTop = 24, marginPlotH = 245, marginY0 = marginTop, marginY1 = marginTop + marginPlotH;
      const marginY = value => marginY1 - (Number(value) - marginDomain[0]) / (marginDomain[1] - marginDomain[0] || 1) * marginPlotH;
      const marginTicks = Array.from({length: 5}, (_, index) => marginDomain[0] + (marginDomain[1] - marginDomain[0]) * index / 4);
      add("rect", {x:x0, y:marginY0, width:x1-x0, height:marginPlotH, class:"frame"}, marginChart);
      marginTicks.forEach(value => { const yy = marginY(value); add("line", {x1:x0, x2:x1, y1:yy, y2:yy, class:"grid-line"}, marginChart); text(marginChart, x0-8, yy+4, value.toFixed(0), {"text-anchor":"end"}); });
      const marginLine = key => {
        let path = "";
        marginData.forEach((row, index) => {
          const xx = x(row.timestamp), yy = marginY(row[key]);
          if (!index) path = `M${xx},${yy}`;
          else path += ` L${xx},${marginY(marginData[index-1][key])} L${xx},${yy}`;
        });
        return path;
      };
      add("path", {d:marginLine("live_margin_usdt"), fill:"none", stroke:"var(--blue)", "stroke-width":2.4}, marginChart);
      add("path", {d:marginLine("research_margin_usdt"), fill:"none", stroke:"var(--orange)", "stroke-width":2.2}, marginChart);
      [data.start, data.mid, data.end].forEach((timestamp, index) => text(marginChart, x(timestamp), marginY1+22, timestamp.replace("T", " ").replace("Z", " UTC"), {"text-anchor": index === 0 ? "start" : index === 2 ? "end" : "middle", class:"muted"}));
      text(marginChart, x0+3, marginY0+15, "初始保证金占用（U）", {class:"muted"});
      const marginOverlay = add("rect", {x:x0, y:marginY0, width:x1-x0, height:marginPlotH, fill:"transparent"}, marginChart);
      marginOverlay.addEventListener("pointermove", event => { const bounds = marginOverlay.getBoundingClientRect(); const ratio = Math.max(0, Math.min(1, (event.clientX - bounds.left) / bounds.width)); const index = Math.max(0, Math.min(marginData.length-1, Math.round(ratio * (marginData.length-1)))); const row = marginData[index]; tooltip.innerHTML = `<strong>${row.timestamp.replace("T", " ").replace("Z", " UTC")}</strong><br>实盘保证金占用：${Number(row.live_margin_usdt).toFixed(2)}U<br>寻优模拟占用：${Number(row.research_margin_usdt).toFixed(2)}U`; const box = root.getBoundingClientRect(); tooltip.style.display = "block"; tooltip.style.left = `${event.clientX-box.left+10}px`; tooltip.style.top = `${event.clientY-box.top+10}px`; });
      marginOverlay.addEventListener("pointerleave", () => tooltip.style.display = "none");
    })();
  </script>
</div>
'''
    replacements = {
        "__DATA__": payload,
        "__DURATION__": cards["duration"],
        "__LIVE_RAW__": cards["live_raw"],
        "__LIVE_CHANGE__": cards["live_change"],
        "__RESEARCH__": cards["research_candidate"],
        "__RESEARCH_CHANGE__": cards["research_change"],
        "__VALIDATION__": cards["validation"],
        "__HOLDOUT__": cards["holdout"],
        "__COLLECTION_START__": cards["collection_start"],
        "__LIVE_RAW_CLASS__": "negative" if live_change < 0 else "",
        "__LIVE_CHANGE_CLASS__": "negative" if live_change < 0 else "",
        "__RESEARCH_CLASS__": "negative" if candidate_pnl < 0 else "",
        "__RESEARCH_CHANGE_CLASS__": "negative" if candidate_pnl < 0 else "",
    }
    for key, value in replacements.items():
        template = template.replace(key, str(value))
    return template


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    optimization = json.loads(args.optimization_report.read_text(encoding="utf-8"))
    live_report = json.loads(args.live_report.read_text(encoding="utf-8"))
    research_rows = read_csv(args.equity_series)
    research_rows.sort(key=lambda row: parse_time(row["timestamp"]))
    grid_rows = read_csv(args.grid_results)
    balance_rows = read_csv(args.balance_snapshots)
    balance_rows.sort(key=lambda row: parse_time(row["observed_at"]))
    if not research_rows or not balance_rows:
        raise SystemExit("research equity series and live balance snapshots are required")

    data_start = parse_time(optimization["data_start"])
    data_end = parse_time(optimization["data_end"])
    live_start = parse_time(balance_rows[0]["observed_at"])
    live_end = parse_time(balance_rows[-1]["observed_at"])
    collection_start = data_start
    collection_end = data_end
    overlap_start = max(collection_start, live_start)
    overlap_end = min(collection_end, live_end)
    if overlap_end <= overlap_start:
        raise SystemExit("local research and live account periods do not overlap")
    if live_start > collection_start or live_end < collection_end:
        raise SystemExit(
            "live balance snapshots must cover the full local collection window "
            "to normalize at the collection start"
        )

    cashflow_events = [
        (parse_time(event["timestamp"]), number(str(event["wallet_delta_usdt"])))
        for event in live_report.get("capital_flow", {}).get("events", [])
    ]
    cashflow_events.sort()
    cumulative = 0.0
    balance_points: list[dict[str, Any]] = []
    event_index = 0
    for row in balance_rows:
        timestamp = parse_time(row["observed_at"])
        while event_index < len(cashflow_events) and cashflow_events[event_index][0] <= timestamp:
            cumulative += cashflow_events[event_index][1]
            event_index += 1
        equity = number(row.get("wallet_balance")) + number(row.get("unrealized_pnl"))
        balance_points.append(
            {
                "epoch": timestamp.timestamp(),
                "equity": equity,
                "adjusted_equity": equity - cumulative,
            }
        )
    raw_points = [(item["epoch"], item["equity"]) for item in balance_points]
    adjusted_points = [(item["epoch"], item["adjusted_equity"]) for item in balance_points]
    raw_window = [item["equity"] for item in balance_points if collection_start.timestamp() <= item["epoch"] <= collection_end.timestamp()]
    adjusted_window = [item["adjusted_equity"] for item in balance_points if collection_start.timestamp() <= item["epoch"] <= collection_end.timestamp()]
    raw_start = interpolate(raw_points, collection_start.timestamp())
    raw_end = interpolate(raw_points, collection_end.timestamp())
    adjusted_start = interpolate(adjusted_points, collection_start.timestamp())
    adjusted_end = interpolate(adjusted_points, collection_end.timestamp())
    raw_dd, raw_dd_pct = drawdown(raw_window)
    adjusted_dd, adjusted_dd_pct = drawdown(adjusted_window)

    event_points: list[tuple[float, float, float]] = [
        (
            parse_time(row["timestamp"]).timestamp(),
            number(row.get("best_validation_cumulative_pnl_usdt")),
            number(row.get("baseline_cumulative_pnl_usdt")),
        )
        for row in research_rows
    ]
    event_best = [(item[0], item[1]) for item in event_points]
    event_baseline = [(item[0], item[2]) for item in event_points]
    contributed_capital = number(str(live_report.get("equity", {}).get("contributed_capital_usdt", 500.0)), 500.0)
    grid = sample_grid(collection_start, collection_end)
    fixed_settings = optimization.get("fixed_live_settings", {})
    leverage = max(number(str(fixed_settings.get("entry_leverage")), 5.0), 1e-12)
    entry_notional = max(number(str(fixed_settings.get("entry_notional_usdt")), 100.0), 0.0)
    best_candidate_events_path = args.best_candidate_events or args.optimization_report.parent / "best_candidate_events.csv"
    live_margin_curve, live_margin_load = load_live_margin_events(
        args.fill_events,
        args.exchange_orders,
        leverage=leverage,
    )
    research_margin_curve, research_margin_load = load_simulated_margin_events(
        best_candidate_events_path,
        entry_notional=entry_notional,
        leverage=leverage,
    )
    margin_timestamps = {timestamp.timestamp() for timestamp in grid}
    for margin_curve in (live_margin_curve, research_margin_curve):
        margin_timestamps.update(
            timestamp
            for timestamp, _ in margin_curve
            if collection_start.timestamp() <= timestamp <= collection_end.timestamp()
        )
    margin_grid = [datetime.fromtimestamp(timestamp, tz=UTC) for timestamp in sorted(margin_timestamps)]
    margin_points = [
        {
            "timestamp": iso(timestamp),
            "live_margin_usdt": round(step_value(live_margin_curve, timestamp.timestamp()), 8),
            "research_margin_usdt": round(step_value(research_margin_curve, timestamp.timestamp()), 8),
        }
        for timestamp in margin_grid
    ]
    points: list[dict[str, Any]] = []
    for timestamp in grid:
        best_pnl = step_value(event_best, timestamp.timestamp())
        baseline_pnl = step_value(event_baseline, timestamp.timestamp())
        live_raw = interpolate(raw_points, timestamp.timestamp())
        points.append(
            {
                "timestamp": iso(timestamp),
                "live_equity_usdt": round(live_raw, 8),
                "research_equity_usdt": round(raw_start + best_pnl, 8),
                "research_pnl": round(best_pnl, 8),
                "research_baseline_pnl": round(baseline_pnl, 8),
            }
        )
    candidate = optimization["best_validation"]
    validation = candidate["metrics"]["validation"]
    holdout = candidate["metrics"]["holdout"]
    best_final_pnl = points[-1]["research_pnl"]
    parsed_grid: list[dict[str, Any]] = []
    for row in grid_rows:
        validation_mean = row.get("validation_mean_net_return_pct")
        holdout_mean = row.get("holdout_mean_net_return_pct")
        if not validation_mean or not holdout_mean:
            continue
        parsed_grid.append(
            {
                **row,
                "validation_mean": float(validation_mean),
                "holdout_mean": float(holdout_mean),
                "validation_n": int(row.get("validation_n_labeled") or 0),
                "holdout_n": int(row.get("holdout_n_labeled") or 0),
            }
        )
    best_holdout = max(parsed_grid, key=lambda row: row["holdout_mean"], default=None)
    report: dict[str, Any] = {
        "source": {
            "research_root": optimization["source_root"],
            "research_files": optimization["load"]["parquet_files"],
            "research_states": optimization["load"]["usable_states"],
            "optimization_report": str(args.optimization_report),
            "live_report": str(args.live_report),
            "fill_events": str(args.fill_events),
            "exchange_orders": str(args.exchange_orders),
            "best_candidate_events": str(best_candidate_events_path),
        },
        "overlap": {
            "start_utc": iso(overlap_start),
            "end_utc": iso(overlap_end),
            "duration_hours": (overlap_end - overlap_start).total_seconds() / 3600.0,
            "duration_text": f"{(overlap_end - overlap_start).total_seconds() / 3600.0:.1f} 小时",
            "comparison_start_rule": "max(local research start, live balance start)",
            "comparison_end_rule": "min(local research end, live balance end)",
        },
        "collection": {
            "start_utc": iso(collection_start),
            "end_utc": iso(collection_end),
            "duration_hours": (collection_end - collection_start).total_seconds() / 3600.0,
            "duration_text": f"{(collection_end - collection_start).total_seconds() / 3600.0:.1f} 小时",
            "normalization_start_rule": "first valid local research collection state",
            "live_balance_covers_full_window": True,
        },
        "live": {
            "benchmark": "live/primary actual account equity",
            "raw_start_usdt": raw_start,
            "raw_end_usdt": raw_end,
            "raw_change_usdt": raw_end - raw_start,
            "raw_change_pct": (raw_end / raw_start - 1.0) * 100.0 if raw_start else None,
            "raw_max_drawdown_usdt": raw_dd,
            "raw_max_drawdown_pct": raw_dd_pct,
            "adjusted_start_usdt": adjusted_start,
            "adjusted_end_usdt": adjusted_end,
            "adjusted_change_usdt": adjusted_end - adjusted_start,
            "adjusted_change_pct": (adjusted_end / adjusted_start - 1.0) * 100.0 if adjusted_start else None,
            "capital_return_pct": (adjusted_end - adjusted_start) / contributed_capital * 100.0 if contributed_capital else None,
            "adjusted_max_drawdown_usdt": adjusted_dd,
            "adjusted_max_drawdown_pct": adjusted_dd_pct,
        },
        "research": {
            "candidate": candidate["config"],
            "validation_net_pnl_usdt": validation["net_pnl_usdt"],
            "validation_mean_net_return_pct": validation["mean_net_return_pct"],
            "validation_profit_factor": validation["profit_factor"],
            "holdout_net_pnl_usdt": holdout["net_pnl_usdt"],
            "holdout_mean_net_return_pct": holdout["mean_net_return_pct"],
            "holdout_profit_factor": holdout["profit_factor"],
            "full_candidate_pnl_usdt": best_final_pnl,
            "full_candidate_equity_start_usdt": raw_start,
            "full_candidate_equity_end_usdt": raw_start + best_final_pnl,
            "baseline_full_pnl_usdt": points[-1]["research_baseline_pnl"],
            "candidate_index_change_pct": best_final_pnl / contributed_capital * 100.0 if contributed_capital else None,
            "capital_denominator_usdt": contributed_capital,
            "interpretation": "absolute fixed-notional replay anchored to live raw starting equity for the chart; no exact live fill, funding, or capital constraint model",
        },
        "margin": {
            "method": "live actual fills matched FIFO by symbol; research filled candidate entries at fixed notional; both converted to initial margin as notional / leverage",
            "entry_notional_usdt": entry_notional,
            "leverage": leverage,
            "live": {
                **margin_summary(live_margin_curve, start=collection_start, end=collection_end),
                **live_margin_load,
            },
            "research": {
                **margin_summary(research_margin_curve, start=collection_start, end=collection_end),
                **research_margin_load,
            },
        },
        "robustness": {
            "grid_rows": len(grid_rows),
            "grid_rows_with_both_periods": len(parsed_grid),
            "validation_and_holdout_both_positive": sum(
                row["validation_mean"] > 0 and row["holdout_mean"] > 0
                for row in parsed_grid
            ),
            "holdout_positive_rows": sum(row["holdout_mean"] > 0 for row in parsed_grid),
            "best_holdout_row": best_holdout,
            "conclusion": "no parameter combination is positive in both validation and holdout" if parsed_grid and not any(row["validation_mean"] > 0 and row["holdout_mean"] > 0 for row in parsed_grid) else "at least one combination is positive in both periods",
        },
        "chart": {
            "start": iso(collection_start),
            "mid": iso(collection_start + (collection_end - collection_start) / 2),
            "end": iso(collection_end),
            "train_end": optimization["splits"]["train"]["end"],
            "validation_end": optimization["splits"]["validation"]["end"],
            "points": points,
            "margin_points": margin_points,
        },
    }
    return report


def write_outputs(report: dict[str, Any], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    points = report["chart"]["points"]
    with (output_dir / "research_vs_live_curve.csv").open("w", encoding="utf-8", newline="") as handle:
        fields = list(points[0])
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(points)
    margin_points = report["chart"]["margin_points"]
    with (output_dir / "margin_usage_curve.csv").open("w", encoding="utf-8", newline="") as handle:
        fields = list(margin_points[0])
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(margin_points)
    (output_dir / "research_vs_live_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    live = report["live"]
    research = report["research"]
    margin = report["margin"]
    robustness = report["robustness"]
    best_holdout = robustness["best_holdout_row"]
    best_holdout_text = "无" if best_holdout is None else f"{best_holdout['holdout_mean']:+.4f}%（验证集 {best_holdout['validation_mean']:+.4f}%）"
    contributed_capital = research["capital_denominator_usdt"]
    markdown = f'''# 本地采集寻优与实盘权益对比

## 本地采集区间

- 作图起点：本地采集第一条有效状态 `{report["collection"]["start_utc"]}`；纵轴使用绝对 USDT
- 采集区间：`{report["collection"]["start_utc"]}` 至 `{report["collection"]["end_utc"]}`，共 {report["collection"]["duration_text"]}
- 本地数据：{report["source"]["research_files"]} 个 Parquet，{report["source"]["research_states"]:,} 条可用 15 秒状态
- 实盘基准：`live/primary` 账户实际 USDT 权益

## 寻优候选

- 参数：`{json.dumps(research["candidate"], ensure_ascii=False)}`
- 验证集：{research["validation_net_pnl_usdt"]:+.2f}U，平均净收益 {research["validation_mean_net_return_pct"]:+.4f}%，PF {research["validation_profit_factor"]:.3f}
- 留出集：{research["holdout_net_pnl_usdt"]:+.2f}U，平均净收益 {research["holdout_mean_net_return_pct"]:+.4f}%，PF {research["holdout_profit_factor"]:.3f}
- 全区间候选累计：{research["full_candidate_pnl_usdt"]:+.2f}U；网格基线累计：{research["baseline_full_pnl_usdt"]:+.2f}U
- 网格共 {robustness["grid_rows"]:,} 组；验证集和留出集同时为正：{robustness["validation_and_holdout_both_positive"]} 组；留出集为正：{robustness["holdout_positive_rows"]} 组
- 留出集单独最优的平均净收益：{best_holdout_text}
- 结论：{robustness["conclusion"]}

## 实盘走势

- 实盘原始权益：{live["raw_start_usdt"]:.2f}U → {live["raw_end_usdt"]:.2f}U，变化 {live["raw_change_usdt"]:+.2f}U（{live["raw_change_pct"]:+.2f}%）
- 重叠段原始最大回撤：{live["raw_max_drawdown_usdt"]:.2f}U（{live["raw_max_drawdown_pct"]:.2f}%）
- 按总投入资本 {contributed_capital:.0f}U 计算的资金调整回报：{live["adjusted_change_usdt"]:+.2f}U（{live["capital_return_pct"]:+.2f}%）

蓝线为 `account_balance_snapshots.csv.gz` 的真实实盘权益，橙线为“采集起点实盘权益 + 本地寻优绝对累计 PnL”：本例为 `{live["raw_start_usdt"]:.2f}U + {research["full_candidate_pnl_usdt"]:.2f}U = {research["full_candidate_equity_end_usdt"]:.2f}U`。橙线是固定 100U 仓位的本地回放，不是完整账户仿真；本地数据缺少完整历史 Top10 快照、真实成交、资金费和资金约束，因此不能直接解释成实盘可下单收益。

## 保证金占用

- 口径：初始保证金 = 名义仓位 ÷ 杠杆；固定设置为 {margin["entry_notional_usdt"]:.0f}U ÷ {margin["leverage"]:.0f}x = {margin["entry_notional_usdt"] / margin["leverage"]:.2f}U/笔
- 实盘估算：起点 {margin["live"]["start_usdt"]:.2f}U，峰值 {margin["live"]["peak_usdt"]:.2f}U，终点 {margin["live"]["end_usdt"]:.2f}U（作图区间内；持仓由全部导出成交历史重建）
- 寻优模拟：起点 {margin["research"]["start_usdt"]:.2f}U，峰值 {margin["research"]["peak_usdt"]:.2f}U，终点 {margin["research"]["end_usdt"]:.2f}U；成交开仓 {margin["research"]["filled_entries"]} 笔，已平仓 {margin["research"]["closed_entries"]} 笔
- 该图不是维持保证金、保证金率或完整账户风险敞口；实盘线由实际成交按币种 FIFO 匹配开平仓得到估算值。
'''
    (output_dir / "research_vs_live_report.md").write_text(markdown, encoding="utf-8")
    (output_dir / "research-vs-live-equity.html").write_text(build_html(report), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--optimization-report", type=Path, required=True)
    parser.add_argument("--equity-series", type=Path, required=True)
    parser.add_argument("--balance-snapshots", type=Path, required=True)
    parser.add_argument("--grid-results", type=Path, required=True)
    parser.add_argument("--live-report", type=Path, required=True)
    parser.add_argument(
        "--fill-events",
        type=Path,
        default=Path("server_exports/cml-live-current-20260905/account_fill_events.csv.gz"),
    )
    parser.add_argument(
        "--exchange-orders",
        type=Path,
        default=Path("server_exports/cml-live-current-20260905/exchange_orders.csv.gz"),
    )
    parser.add_argument("--best-candidate-events", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    report = build_report(args)
    write_outputs(report, args.output_dir)
    print(json.dumps({"overlap": report["overlap"], "live": report["live"], "research": report["research"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
