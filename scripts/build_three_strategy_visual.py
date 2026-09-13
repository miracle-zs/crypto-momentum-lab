#!/usr/bin/env python3
"""Build an absolute-equity and margin comparison for three strategy paths."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from build_research_vs_live_visual import (
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


def load_live_equity_points(
    balance_path: Path,
    live_report: dict[str, Any],
) -> list[tuple[float, float]]:
    rows = read_csv(balance_path)
    rows.sort(key=lambda row: parse_time(row["observed_at"]))
    cashflow_events = sorted(
        (
            parse_time(event["timestamp"]),
            number(str(event.get("wallet_delta_usdt"))),
        )
        for event in live_report.get("capital_flow", {}).get("events", [])
    )
    cumulative = 0.0
    event_index = 0
    points: list[tuple[float, float]] = []
    for row in rows:
        timestamp = parse_time(row["observed_at"])
        while event_index < len(cashflow_events) and cashflow_events[event_index][0] <= timestamp:
            cumulative += cashflow_events[event_index][1]
            event_index += 1
        equity = number(row.get("wallet_balance")) + number(row.get("unrealized_pnl"))
        points.append((timestamp.timestamp(), equity))
    return points


def load_pnl_points(path: Path) -> list[tuple[float, float]]:
    rows = read_csv(path)
    rows.sort(key=lambda row: parse_time(row["timestamp"]))
    return [
        (
            parse_time(row["timestamp"]).timestamp(),
            number(row.get("best_validation_cumulative_pnl_usdt")),
        )
        for row in rows
    ]


def strategy_summary(
    optimization: dict[str, Any],
    pnl_points: list[tuple[float, float]],
    *,
    label: str,
    cap_usdt: float | None,
) -> dict[str, Any]:
    best = optimization["best_validation"]
    full = best["metrics"]["full"]
    return {
        "label": label,
        "cap_initial_margin_usdt": cap_usdt,
        "config": best["config"],
        "validation": best["metrics"]["validation"],
        "holdout": best["metrics"]["holdout"],
        "full": full,
        "full_pnl_usdt": round(pnl_points[-1][1], 8) if pnl_points else 0.0,
        "full_closed": full.get("n_closed"),
        "full_open_at_data_end": full.get("n_open_at_data_end"),
    }


def build_html(report: dict[str, Any]) -> str:
    live = report["live"]
    unconstrained = report["strategies"]["unconstrained"]
    margin350 = report["strategies"]["margin350"]
    payload = json.dumps(report["chart"], ensure_ascii=False, separators=(",", ":"))
    template = '''<div id="cml-three-strategy-compare" aria-label="真实实盘与两种寻优策略的权益和保证金对比">
  <style>
    #cml-three-strategy-compare {
      color: var(--foreground);
      display: block;
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      font-size: 14px;
      line-height: 1.4;
      position: relative;
      width: 100%;
    }
    #cml-three-strategy-compare .title { font-size: 17px; font-weight: 500; margin: 0 0 3px; }
    #cml-three-strategy-compare .subtitle { color: var(--muted-foreground); margin: 0 0 10px; }
    #cml-three-strategy-compare .section-title { font-size: 14px; font-weight: 500; margin: 14px 0 3px; }
    #cml-three-strategy-compare .legend { align-items: center; display: flex; flex-wrap: wrap; gap: 4px 12px; margin: 5px 0 8px; }
    #cml-three-strategy-compare .series-toggle { background: transparent; border: 0; color: var(--foreground); cursor: pointer; font: inherit; padding: 2px 0; }
    #cml-three-strategy-compare .series-toggle[aria-pressed="false"] { color: var(--muted-foreground); }
    #cml-three-strategy-compare .swatch { display: inline-block; height: 3px; margin: 0 5px 3px 0; width: 18px; }
    #cml-three-strategy-compare svg { display: block; height: auto; overflow: visible; width: 100%; }
    #cml-three-strategy-compare text { fill: var(--foreground); font-size: 12px; }
    #cml-three-strategy-compare .muted { fill: var(--muted-foreground); }
    #cml-three-strategy-compare .frame { fill: none; stroke: var(--border); stroke-width: 1; }
    #cml-three-strategy-compare .grid-line { stroke: var(--border); stroke-dasharray: 2 3; opacity: .75; }
    #cml-three-strategy-compare .split-line { stroke: var(--muted-foreground); stroke-dasharray: 5 4; opacity: .8; }
    #cml-three-strategy-compare .hover-guide { stroke: var(--muted-foreground); stroke-dasharray: 3 3; opacity: .8; }
    #cml-three-strategy-compare .tooltip { background: var(--popover); border: 1px solid var(--border); border-radius: 5px; color: var(--popover-foreground); display: none; max-width: 300px; padding: 6px 8px; pointer-events: none; position: absolute; z-index: 3; }
    #cml-three-strategy-compare .note { color: var(--muted-foreground); margin: 7px 0 0; }
    @media (max-width: 460px) { #cml-three-strategy-compare .legend { gap: 2px 9px; } }
  </style>
  <div class="title">实盘 vs 两种寻优：绝对权益与保证金占用</div>
  <p class="subtitle">三条策略线使用同一采集区间和实盘起始权益；时间轴为 UTC。保证金面板为初始保证金估算。</p>
  <div class="legend" role="group" aria-label="策略曲线开关">
    <button type="button" class="series-toggle" data-series="live" aria-pressed="true"><span class="swatch" style="background:var(--viz-series-1)"></span>真实实盘</button>
    <button type="button" class="series-toggle" data-series="unconstrained" aria-pressed="true"><span class="swatch" style="background:var(--viz-series-2)"></span>寻优（无上限）</button>
    <button type="button" class="series-toggle" data-series="margin350" aria-pressed="true"><span class="swatch" style="background:var(--viz-series-3)"></span>寻优（保证金≤350U）</button>
  </div>
  <div class="section-title">账户权益（U）</div>
  <svg id="cml-three-equity-svg" viewBox="0 0 960 430" role="img" aria-label="真实实盘、无保证金上限寻优、保证金上限350U寻优的绝对账户权益曲线"><title>三策略绝对账户权益曲线</title><g id="cml-three-equity-chart"></g></svg>
  <div class="section-title">保证金占用（U）</div>
  <svg id="cml-three-margin-svg" viewBox="0 0 960 350" role="img" aria-label="真实实盘、无保证金上限寻优、保证金上限350U寻优的初始保证金占用曲线"><title>三策略初始保证金占用曲线</title><g id="cml-three-margin-chart"></g></svg>
  <div class="tooltip" id="cml-three-tooltip" role="tooltip"></div>
  <p class="note">悬停图表可查看同一时刻的三条曲线；点击图例可隐藏或恢复对应策略。</p>
  <script>
    (() => {
      const root = document.getElementById("cml-three-strategy-compare");
      const equityChart = document.getElementById("cml-three-equity-chart");
      const marginChart = document.getElementById("cml-three-margin-chart");
      const tooltip = document.getElementById("cml-three-tooltip");
      const data = __DATA__;
      const NS = "http://www.w3.org/2000/svg";
      const W = 960, equityH = 430, marginH = 350;
      const left = 72, right = 20, equityTop = 24, equityPlotH = 300, marginTop = 24, marginPlotH = 240;
      const x0 = left, x1 = W - right, equityY1 = equityTop + equityPlotH, marginY1 = marginTop + marginPlotH;
      const series = {
        live: {label:"真实实盘", color:"var(--viz-series-1)", equity:"live_equity_usdt", margin:"live_margin_usdt"},
        unconstrained: {label:"寻优（无上限）", color:"var(--viz-series-2)", equity:"unconstrained_equity_usdt", margin:"unconstrained_margin_usdt"},
        margin350: {label:"寻优（保证金≤350U）", color:"var(--viz-series-3)", equity:"margin350_equity_usdt", margin:"margin350_margin_usdt"}
      };
      const visible = {live:true, unconstrained:true, margin350:true};
      const add = (name, attrs, parent) => { const node = document.createElementNS(NS, name); Object.entries(attrs || {}).forEach(([key,value]) => node.setAttribute(key, String(value))); parent.appendChild(node); return node; };
      const text = (parent, x, y, value, attrs = {}) => { const node = add("text", {x, y, ...attrs}, parent); node.appendChild(document.createTextNode(value)); return node; };
      const firstTime = Date.parse(data.start), lastTime = Date.parse(data.end);
      const x = timestamp => x0 + (Date.parse(timestamp) - firstTime) / Math.max(1, lastTime - firstTime) * (x1 - x0);
      const finite = value => Number.isFinite(Number(value));
      const allValues = key => data.points.flatMap(row => Object.values(series).map(item => Number(row[item[key]]))).filter(finite);
      const makeScale = (values, floorAtZero) => { const low = Math.min(...values), high = Math.max(...values); const span = Math.max(1, high-low); const domainLow = floorAtZero ? Math.max(0, low-span*.05) : low-span*.08; return {domain:[domainLow, high+span*.08]}; };
      const equityScale = makeScale(allValues("equity"), false), marginScale = makeScale(allValues("margin"), true);
      const equityY = value => equityY1 - (Number(value)-equityScale.domain[0]) / (equityScale.domain[1]-equityScale.domain[0] || 1) * equityPlotH;
      const marginY = value => marginY1 - (Number(value)-marginScale.domain[0]) / (marginScale.domain[1]-marginScale.domain[0] || 1) * marginPlotH;
      const linePath = (key, yScale) => data.points.map((row,index) => `${index ? "L" : "M"}${x(row.timestamp)},${yScale(row[key])}`).join(" ");
      const stepPath = (key, yScale) => { let path = ""; data.points.forEach((row,index) => { const xx=x(row.timestamp), yy=yScale(row[key]); if (!index) path=`M${xx},${yy}`; else path += ` L${xx},${yScale(data.points[index-1][key])} L${xx},${yy}`; }); return path; };
      const interpolated = (key, timestamp) => { const times=data.points.map(row=>Date.parse(row.timestamp)); if (timestamp<=times[0]) return Number(data.points[0][key]); if (timestamp>=times[times.length-1]) return Number(data.points[times.length-1][key]); let rightIndex=1; while (rightIndex<times.length && times[rightIndex]<timestamp) rightIndex++; const leftIndex=rightIndex-1, ratio=(timestamp-times[leftIndex])/Math.max(1,times[rightIndex]-times[leftIndex]); return Number(data.points[leftIndex][key])+(Number(data.points[rightIndex][key])-Number(data.points[leftIndex][key]))*ratio; };
      const drawAxes = (parent, top, plotH, yScale, values, yLabel) => { add("rect", {x:x0,y:top,width:x1-x0,height:plotH,class:"frame","data-chart-frame":"true"}, parent); const domain=yScale.domain, ticks=Array.from({length:5},(_,index)=>domain[0]+(domain[1]-domain[0])*index/4); ticks.forEach(value=>{const yy=(parent===equityChart?equityY:marginY)(value); add("line",{x1:x0,x2:x1,y1:yy,y2:yy,class:"grid-line"},parent); text(parent,x0-8,yy+4,value.toFixed(0),{"text-anchor":"end"});}); text(parent,x0+3,top+15,yLabel,{class:"muted"}); [data.start,data.mid,data.end].forEach((timestamp,index)=>text(parent,x(timestamp),top+plotH+23,timestamp.replace("T"," ").replace("Z"," UTC"),{"text-anchor":index===0?"start":index===2?"end":"middle",class:"muted"})); };
      const split = (parent, timestamp, label, top, plotH) => { const xx=x(timestamp); add("line",{x1:xx,x2:xx,y1:top,y2:top+plotH,class:"split-line"},parent); text(parent,xx+4,top+14,label,{class:"muted"}); };
      drawAxes(equityChart,equityTop,equityPlotH,equityScale,allValues("equity"),"账户权益 / 模拟权益（U）");
      split(equityChart,data.train_end,"训练/验证",equityTop,equityPlotH); split(equityChart,data.validation_end,"验证/留出",equityTop,equityPlotH);
      drawAxes(marginChart,marginTop,marginPlotH,marginScale,allValues("margin"),"初始保证金占用（U）");
      const nodes = {equity:{},margin:{},equityMarkers:{},marginMarkers:{}};
      Object.entries(series).forEach(([name,item])=>{
        nodes.equity[name]=add("path",{d:linePath(item.equity,equityY),fill:"none",stroke:item.color,"stroke-width":name==="live"?2.5:2.2},equityChart);
        nodes.margin[name]=add("path",{d:stepPath(item.margin,marginY),fill:"none",stroke:item.color,"stroke-width":name==="live"?2.5:2.2},marginChart);
        nodes.equityMarkers[name]=add("circle",{r:4,fill:item.color,display:"none"},equityChart);
        nodes.marginMarkers[name]=add("circle",{r:4,fill:item.color,display:"none"},marginChart);
      });
      const equityGuide=add("line",{y1:equityTop,y2:equityY1,class:"hover-guide",display:"none"},equityChart);
      const marginGuide=add("line",{y1:marginTop,y2:marginY1,class:"hover-guide",display:"none"},marginChart);
      const equityOverlay=add("rect",{x:x0,y:equityTop,width:x1-x0,height:equityPlotH,fill:"transparent","data-chart-hit":"true","data-chart-hover-overlay":"cross-series"},equityChart);
      const marginOverlay=add("rect",{x:x0,y:marginTop,width:x1-x0,height:marginPlotH,fill:"transparent","data-chart-hit":"true","data-chart-hover-overlay":"cross-series"},marginChart);
      const updateHover=(event,overlay,guide,markers,yScale,key,kind)=>{const bounds=overlay.getBoundingClientRect(),ratio=Math.max(0,Math.min(1,(event.clientX-bounds.left)/bounds.width)),timestamp=firstTime+ratio*(lastTime-firstTime),xx=x(new Date(timestamp).toISOString()); guide.setAttribute("x1",xx); guide.setAttribute("x2",xx); guide.style.display="block"; const rows=[]; Object.entries(series).forEach(([name,item])=>{const value=interpolated(item[key],timestamp), marker=markers[name]; marker.setAttribute("cx",xx); marker.setAttribute("cy",yScale(value)); marker.style.display=visible[name]?"block":"none"; if(visible[name]) rows.push(`<div><span style="color:${item.color}">●</span> ${item.label}：${value.toFixed(2)}U</div>`);}); tooltip.innerHTML=`<strong>${new Date(timestamp).toISOString().replace("T"," ").replace(".000Z"," UTC")}</strong>${rows.join("")}`; const box=root.getBoundingClientRect(); tooltip.style.display="block"; tooltip.style.left=`${event.clientX-box.left+10}px`; tooltip.style.top=`${event.clientY-box.top+10}px`;};
      equityOverlay.addEventListener("pointermove",event=>updateHover(event,equityOverlay,equityGuide,nodes.equityMarkers,equityY,"equity","equity"));
      marginOverlay.addEventListener("pointermove",event=>updateHover(event,marginOverlay,marginGuide,nodes.marginMarkers,marginY,"margin","margin"));
      const clearHover=()=>{tooltip.style.display="none"; equityGuide.style.display="none"; marginGuide.style.display="none"; Object.values(nodes.equityMarkers).forEach(node=>node.style.display="none"); Object.values(nodes.marginMarkers).forEach(node=>node.style.display="none");};
      equityOverlay.addEventListener("pointerleave",clearHover); marginOverlay.addEventListener("pointerleave",clearHover);
      document.querySelectorAll("#cml-three-strategy-compare .series-toggle").forEach(button=>button.addEventListener("click",()=>{const name=button.dataset.series; visible[name]=!visible[name]; button.setAttribute("aria-pressed",String(visible[name])); nodes.equity[name].style.display=visible[name]?"block":"none"; nodes.margin[name].style.display=visible[name]?"block":"none";}));
    })();
  </script>
</div>
'''
    replacements = {
        "__DATA__": payload,
    }
    for key, value in replacements.items():
        template = template.replace(key, str(value))
    return template


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    old_optimization = json.loads(args.unconstrained_optimization_report.read_text(encoding="utf-8"))
    capped_optimization = json.loads(args.margin350_optimization_report.read_text(encoding="utf-8"))
    live_report = json.loads(args.live_report.read_text(encoding="utf-8"))
    old_start = parse_time(old_optimization["data_start"])
    old_end = parse_time(old_optimization["data_end"])
    capped_start = parse_time(capped_optimization["data_start"])
    capped_end = parse_time(capped_optimization["data_end"])
    if old_start != capped_start or old_end != capped_end:
        raise SystemExit("the two optimization windows must match")
    collection_start, collection_end = old_start, old_end
    live_balance_rows = read_csv(args.balance_snapshots)
    live_balance_rows.sort(key=lambda row: parse_time(row["observed_at"]))
    live_points = load_live_equity_points(args.balance_snapshots, live_report)
    if not live_points:
        raise SystemExit("live balance snapshots are required")
    live_start = parse_time(live_balance_rows[0]["observed_at"])
    live_end = parse_time(live_balance_rows[-1]["observed_at"])
    if live_start > collection_start or live_end < collection_end:
        raise SystemExit("live balance snapshots must cover the full comparison window")
    raw_start = interpolate(live_points, collection_start.timestamp())
    raw_end = interpolate(live_points, collection_end.timestamp())

    old_pnl_points = load_pnl_points(args.unconstrained_equity_series)
    capped_pnl_points = load_pnl_points(args.margin350_equity_series)
    if not old_pnl_points or not capped_pnl_points:
        raise SystemExit("both optimization equity series are required")
    leverage = max(number(str(capped_optimization["fixed_live_settings"].get("entry_leverage")), 5.0), 1e-12)
    entry_notional = max(number(str(capped_optimization["fixed_live_settings"].get("entry_notional_usdt")), 100.0), 0.0)
    cap_usdt = number(str(capped_optimization.get("margin_constraint", {}).get("max_initial_margin_usdt")), 350.0)
    live_margin_curve, live_margin_load = load_live_margin_events(
        args.fill_events,
        args.exchange_orders,
        leverage=leverage,
    )
    old_margin_curve, old_margin_load = load_simulated_margin_events(
        args.unconstrained_events,
        entry_notional=entry_notional,
        leverage=leverage,
    )
    capped_margin_curve, capped_margin_load = load_simulated_margin_events(
        args.margin350_events,
        entry_notional=entry_notional,
        leverage=leverage,
    )
    grid = sample_grid(collection_start, collection_end)
    chart_timestamps = {timestamp.timestamp() for timestamp in grid}
    for pnl_curve in (old_pnl_points, capped_pnl_points):
        chart_timestamps.update(
            timestamp
            for timestamp, _ in pnl_curve
            if collection_start.timestamp() <= timestamp <= collection_end.timestamp()
        )
    for margin_curve in (live_margin_curve, old_margin_curve, capped_margin_curve):
        chart_timestamps.update(
            timestamp
            for timestamp, _ in margin_curve
            if collection_start.timestamp() <= timestamp <= collection_end.timestamp()
        )
    chart_times = [datetime.fromtimestamp(timestamp, tz=UTC) for timestamp in sorted(chart_timestamps)]
    points: list[dict[str, Any]] = []
    for timestamp in chart_times:
        epoch = timestamp.timestamp()
        old_pnl = step_value(old_pnl_points, epoch)
        capped_pnl = step_value(capped_pnl_points, epoch)
        points.append(
            {
                "timestamp": iso(timestamp),
                "live_equity_usdt": round(interpolate(live_points, epoch), 8),
                "unconstrained_equity_usdt": round(raw_start + old_pnl, 8),
                "margin350_equity_usdt": round(raw_start + capped_pnl, 8),
                "unconstrained_pnl": round(old_pnl, 8),
                "margin350_pnl": round(capped_pnl, 8),
                "live_margin_usdt": round(step_value(live_margin_curve, epoch), 8),
                "unconstrained_margin_usdt": round(step_value(old_margin_curve, epoch), 8),
                "margin350_margin_usdt": round(step_value(capped_margin_curve, epoch), 8),
            }
        )
    live_margin = margin_summary(live_margin_curve, start=collection_start, end=collection_end)
    old_margin = margin_summary(old_margin_curve, start=collection_start, end=collection_end)
    capped_margin = margin_summary(capped_margin_curve, start=collection_start, end=collection_end)
    if capped_margin["peak_usdt"] > cap_usdt + 1e-6:
        raise SystemExit(
            f"margin cap violation: peak {capped_margin['peak_usdt']}U > {cap_usdt}U"
        )
    old_strategy = strategy_summary(
        old_optimization,
        old_pnl_points,
        label="寻优（无保证金上限）",
        cap_usdt=None,
    )
    capped_strategy = strategy_summary(
        capped_optimization,
        capped_pnl_points,
        label="寻优（保证金≤350U）",
        cap_usdt=cap_usdt,
    )
    old_strategy["margin_load"] = old_margin_load
    capped_strategy["margin_load"] = capped_margin_load
    return {
        "source": {
            "balance_snapshots": str(args.balance_snapshots),
            "fill_events": str(args.fill_events),
            "exchange_orders": str(args.exchange_orders),
            "unconstrained_optimization_report": str(args.unconstrained_optimization_report),
            "margin350_optimization_report": str(args.margin350_optimization_report),
            "unconstrained_events": str(args.unconstrained_events),
            "margin350_events": str(args.margin350_events),
        },
        "collection": {
            "start_utc": iso(collection_start),
            "end_utc": iso(collection_end),
            "duration_hours": (collection_end - collection_start).total_seconds() / 3600.0,
            "duration_text": f"{(collection_end - collection_start).total_seconds() / 3600.0:.1f} 小时",
        },
        "live": {
            "label": "真实实盘",
            "raw_start_usdt": raw_start,
            "raw_end_usdt": raw_end,
            "raw_change_usdt": raw_end - raw_start,
            "raw_change_pct": (raw_end / raw_start - 1.0) * 100.0 if raw_start else None,
            "margin": {**live_margin, **live_margin_load},
        },
        "strategies": {
            "unconstrained": old_strategy,
            "margin350": capped_strategy,
        },
        "margin": {
            "entry_notional_usdt": entry_notional,
            "leverage": leverage,
            "initial_margin_per_entry_usdt": entry_notional / leverage,
            "cap_initial_margin_usdt": cap_usdt,
            "live": {**live_margin, **live_margin_load},
            "unconstrained": {**old_margin, **old_margin_load},
            "margin350": {**capped_margin, **capped_margin_load},
        },
        "chart": {
            "start": iso(collection_start),
            "mid": iso(collection_start + (collection_end - collection_start) / 2),
            "end": iso(collection_end),
            "train_end": old_optimization["splits"]["train"]["end"],
            "validation_end": old_optimization["splits"]["validation"]["end"],
            "points": points,
        },
    }


def write_outputs(report: dict[str, Any], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    points = report["chart"]["points"]
    equity_fields = [
        "timestamp",
        "live_equity_usdt",
        "unconstrained_equity_usdt",
        "margin350_equity_usdt",
        "unconstrained_pnl",
        "margin350_pnl",
    ]
    margin_fields = [
        "timestamp",
        "live_margin_usdt",
        "unconstrained_margin_usdt",
        "margin350_margin_usdt",
    ]
    for filename, fields in (
        ("three_strategy_equity_curve.csv", equity_fields),
        ("three_strategy_margin_curve.csv", margin_fields),
    ):
        with (output_dir / filename).open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows({field: row[field] for field in fields} for row in points)
    (output_dir / "three_strategy_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    live = report["live"]
    old = report["strategies"]["unconstrained"]
    capped = report["strategies"]["margin350"]
    margin = report["margin"]
    markdown = f'''# 三策略权益与保证金对比

## 对比区间

- 采集区间：`{report["collection"]["start_utc"]}` 至 `{report["collection"]["end_utc"]}`，共 {report["collection"]["duration_text"]}
- 三条权益线均以采集起点实盘权益 {live["raw_start_usdt"]:.2f}U 为绝对金额起点
- 实盘基准：账户余额快照中的真实 `wallet_balance + unrealized_pnl`

## 三个策略

### 1. 真实实盘

- 权益：{live["raw_start_usdt"]:.2f}U → {live["raw_end_usdt"]:.2f}U，变化 {live["raw_change_usdt"]:+.2f}U（{live["raw_change_pct"]:+.2f}%）
- 保证金估算：起点 {margin["live"]["start_usdt"]:.2f}U，峰值 {margin["live"]["peak_usdt"]:.2f}U，终点 {margin["live"]["end_usdt"]:.2f}U

### 2. 原先寻优（无保证金上限）

- 参数：`{json.dumps(old["config"], ensure_ascii=False)}`
- 全区间模拟：{old["full_pnl_usdt"]:+.2f}U；验证集 {old["validation"]["net_pnl_usdt"]:+.2f}U；留出集 {old["holdout"]["net_pnl_usdt"]:+.2f}U
- 保证金估算：峰值 {margin["unconstrained"]["peak_usdt"]:.2f}U，终点 {margin["unconstrained"]["end_usdt"]:.2f}U

### 3. 新寻优（保证金≤350U）

- 参数：`{json.dumps(capped["config"], ensure_ascii=False)}`
- 全区间模拟：{capped["full_pnl_usdt"]:+.2f}U；验证集 {capped["validation"]["net_pnl_usdt"]:+.2f}U；留出集 {capped["holdout"]["net_pnl_usdt"]:+.2f}U
- 保证金估算：峰值 {margin["margin350"]["peak_usdt"]:.2f}U，终点 {margin["margin350"]["end_usdt"]:.2f}U；严格低于上限 {margin["cap_initial_margin_usdt"]:.2f}U

## 固定设置

- 两次寻优都只调整六个入场参数：`impulse_window_buckets`、`confirmation_buckets`、`min_return_pct`、`min_imbalance`、`min_intensity`、`cooldown_buckets`
- 其余设置与实盘保持：Top10、只做多、100U名义仓位、5倍杠杆、限价单 TTL 900 秒、15 分钟 K 线退出、B8 恢复逻辑、双边费率 0.05%
- 保证金口径：每笔初始保证金 = 100U ÷ 5 = 20U；350U 上限最多允许 17 笔满额仓位同时占用（340U）
- 退出回放使用本地 15 秒状态合成 15 分钟 K 线；实际实盘使用官方已收盘 K 线和真实成交，因此仍属于研究近似
'''
    (output_dir / "three_strategy_report.md").write_text(markdown, encoding="utf-8")
    (output_dir / "three-strategy-equity-margin.html").write_text(
        build_html(report),
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--unconstrained-optimization-report", type=Path, required=True)
    parser.add_argument("--unconstrained-equity-series", type=Path, required=True)
    parser.add_argument("--unconstrained-events", type=Path, required=True)
    parser.add_argument("--margin350-optimization-report", type=Path, required=True)
    parser.add_argument("--margin350-equity-series", type=Path, required=True)
    parser.add_argument("--margin350-events", type=Path, required=True)
    parser.add_argument("--balance-snapshots", type=Path, required=True)
    parser.add_argument("--fill-events", type=Path, required=True)
    parser.add_argument("--exchange-orders", type=Path, required=True)
    parser.add_argument("--live-report", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    report = build_report(args)
    write_outputs(report, args.output_dir)
    print(
        json.dumps(
            {
                "collection": report["collection"],
                "live": report["live"],
                "strategies": report["strategies"],
                "margin": report["margin"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
