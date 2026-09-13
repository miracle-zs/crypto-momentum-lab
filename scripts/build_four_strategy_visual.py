#!/usr/bin/env python3
"""Build a multi-strategy absolute-equity and initial-margin comparison."""

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


LIVE_BASELINE_CONFIG = {
    "impulse_window_buckets": 3,
    "confirmation_buckets": 1,
    "min_return_pct": 1.0,
    "min_imbalance": 0.4,
    "min_intensity": 2.0,
    "cooldown_buckets": 0,
}


def load_live_equity_points(
    balance_path: Path,
) -> list[tuple[float, float]]:
    rows = read_csv(balance_path)
    rows.sort(key=lambda row: parse_time(row["observed_at"]))
    return [
        (
            parse_time(row["observed_at"]).timestamp(),
            number(row.get("wallet_balance")) + number(row.get("unrealized_pnl")),
        )
        for row in rows
    ]


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
    metrics = best["metrics"]
    full = metrics["full"]
    drawdown_weight = optimization.get("drawdown_weight")
    selection_score = best.get("selection_score")
    if selection_score is None and drawdown_weight is not None:
        validation_pnl = metrics["validation"].get("net_pnl_usdt")
        validation_drawdown = metrics["validation"].get("max_drawdown_usdt")
        if validation_pnl is not None and validation_drawdown is not None:
            selection_score = round(
                float(validation_pnl) - float(drawdown_weight) * float(validation_drawdown),
                8,
            )
    return {
        "label": label,
        "cap_initial_margin_usdt": cap_usdt,
        "natural_initial_margin_peak_usdt": best.get("natural_initial_margin_peak_usdt"),
        "margin_constraint_feasible": best.get("margin_constraint_feasible"),
        "fixed_cooldown_buckets": optimization.get("fixed_cooldown_buckets"),
        "selection_objective": optimization.get("selection_objective"),
        "drawdown_weight": optimization.get("drawdown_weight"),
        "validation_selection_score": selection_score,
        "config": best["config"],
        "validation": metrics["validation"],
        "holdout": metrics["holdout"],
        "full": full,
        "full_pnl_usdt": round(pnl_points[-1][1], 8) if pnl_points else 0.0,
        "full_closed": full.get("n_closed"),
        "full_open_at_data_end": full.get("n_open_at_data_end"),
    }


def build_html(report: dict[str, Any]) -> str:
    chart_fields = (
        "timestamp",
        "live_equity_usdt",
        "unconstrained_equity_usdt",
        "margin350_equity_usdt",
        "margin350cd0_equity_usdt",
        "margin280_equity_usdt",
        "margin280cd0_equity_usdt",
        "drawdown_cd0_equity_usdt",
        "drawdown_free_equity_usdt",
        "live_margin_usdt",
        "unconstrained_margin_usdt",
        "margin350_margin_usdt",
        "margin350cd0_margin_usdt",
        "margin280_margin_usdt",
        "margin280cd0_margin_usdt",
        "drawdown_cd0_margin_usdt",
        "drawdown_free_margin_usdt",
    )
    compact_chart = {
        key: value
        for key, value in report["chart"].items()
        if key != "points" and key != "mid"
    }
    compact_chart["points"] = [
        {
            key: (
                value
                if key == "timestamp"
                else round(float(value), 4)
            )
            for key in chart_fields
            for value in [row[key]]
        }
        for row in report["chart"]["points"]
    ]
    payload = json.dumps(compact_chart, ensure_ascii=False, separators=(",", ":"))
    unconstrained_label = report["strategies"]["unconstrained"]["label"]
    margin350_label = report["strategies"]["margin350"]["label"]
    margin350cd0_label = report["strategies"]["margin350cd0"]["label"]
    margin280_label = report["strategies"]["margin280"]["label"]
    margin280cd0_label = report["strategies"]["margin280cd0"]["label"]
    drawdown_cd0_label = report["strategies"]["drawdown_cd0"]["label"]
    drawdown_free_label = report["strategies"]["drawdown_free"]["label"]
    drawdown_weight = report["strategies"]["drawdown_cd0"].get("drawdown_weight")
    if drawdown_weight is None:
        drawdown_weight = report["strategies"]["drawdown_free"].get("drawdown_weight")
    drawdown_weight_text = f"{float(drawdown_weight if drawdown_weight is not None else 0.10):g}"
    if report.get("evaluation_mode") == "frozen_parameters_extended_replay":
        evaluation_mode_text = (
            "A-G 参数冻结为上一轮各自选出的参数，本次只在最新采集数据上做扩展回放；"
            "F/G 的历史选择目标为验证集绝对净 PnL − 回撤权重×验证集最大回撤"
        )
    else:
        evaluation_mode_text = (
            "A-E 按既有验证集 PnL 目标，F/G 使用验证集绝对净 PnL − "
            "回撤权重×验证集最大回撤的单目标"
        )
    template = '''<div id="cml-eight-strategy-compare" aria-label="真实实盘与七种寻优策略的绝对权益和保证金对比">
  <style>
    #cml-eight-strategy-compare {
      color: var(--foreground);
      display: block;
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      font-size: 14px;
      line-height: 1.4;
      position: relative;
      width: 100%;
    }
    #cml-eight-strategy-compare .title { font-size: 17px; font-weight: 500; margin: 0 0 3px; }
    #cml-eight-strategy-compare .subtitle { color: var(--muted-foreground); margin: 0 0 10px; }
    #cml-eight-strategy-compare .section-title { font-size: 14px; font-weight: 500; margin: 14px 0 3px; }
    #cml-eight-strategy-compare .legend { align-items: center; display: flex; flex-wrap: wrap; gap: 4px 12px; margin: 5px 0 8px; }
    #cml-eight-strategy-compare .series-toggle { background: transparent; border: 0; color: var(--foreground); cursor: pointer; font: inherit; padding: 2px 0; }
    #cml-eight-strategy-compare .series-toggle[aria-pressed="false"] { color: var(--muted-foreground); }
    #cml-eight-strategy-compare .swatch { display: inline-block; height: 3px; margin: 0 5px 3px 0; width: 18px; }
    #cml-eight-strategy-compare svg { display: block; height: auto; overflow: visible; width: 100%; }
    #cml-eight-strategy-compare text { fill: var(--foreground); font-size: 12px; }
    #cml-eight-strategy-compare .muted { fill: var(--muted-foreground); }
    #cml-eight-strategy-compare .frame { fill: none; stroke: var(--border); stroke-width: 1; }
    #cml-eight-strategy-compare .grid-line { stroke: var(--border); stroke-dasharray: 2 3; opacity: .75; }
    #cml-eight-strategy-compare .split-line { stroke: var(--muted-foreground); stroke-dasharray: 5 4; opacity: .8; }
    #cml-eight-strategy-compare .hover-guide { stroke: var(--muted-foreground); stroke-dasharray: 3 3; opacity: .8; }
    #cml-eight-strategy-compare .tooltip { background: var(--popover); border: 1px solid var(--border); border-radius: 5px; color: var(--popover-foreground); display: none; max-width: 330px; padding: 6px 8px; pointer-events: none; position: absolute; z-index: 3; }
    #cml-eight-strategy-compare .note { color: var(--muted-foreground); margin: 7px 0 0; }
    @media (max-width: 460px) { #cml-eight-strategy-compare .legend { gap: 2px 9px; } }
  </style>
  <div class="title">实盘 vs 七种寻优：绝对权益与保证金占用</div>
  <p class="subtitle">八条策略线使用同一对比区间和采集起点实盘权益；时间轴为 UTC。__EVALUATION_MODE__；模拟策略为固定 100U 名义仓位、5 倍杠杆的绝对 PnL 回放。350U/280U 是参数组事前自然峰值约束，超限参数组整组淘汰，不在运行中拒绝入场。</p>
  <div class="legend" role="group" aria-label="策略曲线开关">
    <button type="button" class="series-toggle" data-series="live" aria-pressed="true"><span class="swatch" style="background:var(--viz-series-1)"></span>真实实盘</button>
    <button type="button" class="series-toggle" data-series="unconstrained" aria-pressed="true"><span class="swatch" style="background:var(--viz-series-2)"></span>__UNCONSTRAINED_LABEL__</button>
    <button type="button" class="series-toggle" data-series="margin350" aria-pressed="true"><span class="swatch" style="background:var(--viz-series-3)"></span>__MARGIN350_LABEL__</button>
    <button type="button" class="series-toggle" data-series="margin350cd0" aria-pressed="true"><span class="swatch" style="background:var(--viz-series-4)"></span>__MARGIN350CD0_LABEL__</button>
    <button type="button" class="series-toggle" data-series="margin280" aria-pressed="true"><span class="swatch" style="background:var(--viz-series-5)"></span>__MARGIN280_LABEL__</button>
    <button type="button" class="series-toggle" data-series="margin280cd0" aria-pressed="true"><span class="swatch" style="background:var(--viz-series-6)"></span>__MARGIN280CD0_LABEL__</button>
    <button type="button" class="series-toggle" data-series="drawdown_cd0" aria-pressed="true"><span class="swatch" style="background:var(--viz-series-1)"></span>__DRAWDOWN_CD0_LABEL__</button>
    <button type="button" class="series-toggle" data-series="drawdown_free" aria-pressed="true"><span class="swatch" style="background:var(--viz-series-2)"></span>__DRAWDOWN_FREE_LABEL__</button>
  </div>
  <div class="section-title">账户权益（U）</div>
  <svg id="cml-eight-equity-svg" viewBox="0 0 980 430" role="img" aria-label="真实实盘和七种寻优策略的绝对账户权益曲线"><title>八策略绝对账户权益曲线</title><g id="cml-eight-equity-chart"></g></svg>
  <div class="section-title">保证金占用（U）</div>
  <svg id="cml-eight-margin-svg" viewBox="0 0 980 350" role="img" aria-label="真实实盘和七种寻优策略的初始保证金占用曲线"><title>八策略初始保证金占用曲线</title><g id="cml-eight-margin-chart"></g></svg>
  <div class="tooltip" id="cml-eight-tooltip" role="tooltip"></div>
  <p class="note">悬停任一图表可查看同一时刻的八条曲线；点击图例可隐藏或恢复对应策略。F/G 用虚线区分；竖虚线为训练/验证和验证/留出分界；保证金上限仅用于参数组筛选。</p>
  <script>
    (() => {
      const root = document.getElementById("cml-eight-strategy-compare");
      const equityChart = document.getElementById("cml-eight-equity-chart");
      const marginChart = document.getElementById("cml-eight-margin-chart");
      const tooltip = document.getElementById("cml-eight-tooltip");
      const data = __DATA__;
      const NS = "http://www.w3.org/2000/svg";
      const W = 980, equityTop = 24, equityPlotH = 300, marginTop = 24, marginPlotH = 240;
      const left = 76, right = 20, x0 = left, x1 = W - right;
      const equityY1 = equityTop + equityPlotH, marginY1 = marginTop + marginPlotH;
      const series = {
        live: {label:"真实实盘", color:"var(--viz-series-1)", equity:"live_equity_usdt", margin:"live_margin_usdt"},
        unconstrained: {label:"__UNCONSTRAINED_LABEL__", color:"var(--viz-series-2)", equity:"unconstrained_equity_usdt", margin:"unconstrained_margin_usdt"},
        margin350: {label:"__MARGIN350_LABEL__", color:"var(--viz-series-3)", equity:"margin350_equity_usdt", margin:"margin350_margin_usdt"},
        margin350cd0: {label:"__MARGIN350CD0_LABEL__", color:"var(--viz-series-4)", equity:"margin350cd0_equity_usdt", margin:"margin350cd0_margin_usdt"},
        margin280: {label:"__MARGIN280_LABEL__", color:"var(--viz-series-5)", equity:"margin280_equity_usdt", margin:"margin280_margin_usdt"},
        margin280cd0: {label:"__MARGIN280CD0_LABEL__", color:"var(--viz-series-6)", equity:"margin280cd0_equity_usdt", margin:"margin280cd0_margin_usdt"},
        drawdown_cd0: {label:"__DRAWDOWN_CD0_LABEL__", color:"var(--viz-series-1)", dash:"7 4", equity:"drawdown_cd0_equity_usdt", margin:"drawdown_cd0_margin_usdt"},
        drawdown_free: {label:"__DRAWDOWN_FREE_LABEL__", color:"var(--viz-series-2)", dash:"2 3", equity:"drawdown_free_equity_usdt", margin:"drawdown_free_margin_usdt"}
      };
      const visible = {live:true, unconstrained:true, margin350:true, margin350cd0:true, margin280:true, margin280cd0:true, drawdown_cd0:true, drawdown_free:true};
      const add = (name, attrs, parent) => { const node = document.createElementNS(NS, name); Object.entries(attrs || {}).forEach(([key,value]) => node.setAttribute(key, String(value))); parent.appendChild(node); return node; };
      const text = (parent, x, y, value, attrs = {}) => { const node = add("text", {x, y, ...attrs}, parent); node.appendChild(document.createTextNode(value)); return node; };
      const firstTime = Date.parse(data.start), lastTime = Date.parse(data.end);
      const times = data.points.map(row => Date.parse(row.timestamp));
      const x = timestamp => x0 + (timestamp - firstTime) / Math.max(1, lastTime - firstTime) * (x1 - x0);
      const finite = value => Number.isFinite(Number(value));
      const allValues = key => data.points.flatMap(row => Object.values(series).map(item => Number(row[item[key]]))).filter(finite);
      const makeScale = (values, floorAtZero) => { const low = Math.min(...values), high = Math.max(...values); const span = Math.max(1, high-low); const pad = span*.08; const domainLow = floorAtZero ? Math.max(0, low-pad) : low-pad; return {domain:[domainLow, high+pad]}; };
      const equityScale = makeScale(allValues("equity"), false), marginScale = makeScale(allValues("margin"), true);
      const equityY = value => equityY1 - (Number(value)-equityScale.domain[0]) / (equityScale.domain[1]-equityScale.domain[0] || 1) * equityPlotH;
      const marginY = value => marginY1 - (Number(value)-marginScale.domain[0]) / (marginScale.domain[1]-marginScale.domain[0] || 1) * marginPlotH;
      const linePath = (key, yScale) => data.points.map((row,index) => `${index ? "L" : "M"}${x(Date.parse(row.timestamp))},${yScale(row[key])}`).join(" ");
      const stepPath = (key, yScale) => { let path = ""; data.points.forEach((row,index) => { const xx=x(Date.parse(row.timestamp)), yy=yScale(row[key]); if (!index) path=`M${xx},${yy}`; else path += ` L${xx},${yScale(data.points[index-1][key])} L${xx},${yy}`; }); return path; };
      const stepped = (key, timestamp) => { if (!data.points.length) return 0; let low=0, high=times.length; while (low<high) { const mid=Math.floor((low+high)/2); if (times[mid]<=timestamp) low=mid+1; else high=mid; } return Number(data.points[Math.max(0,low-1)][key]); };
      const interpolated = (key, timestamp) => { if (!data.points.length) return 0; if (timestamp<=times[0]) return Number(data.points[0][key]); if (timestamp>=times[times.length-1]) return Number(data.points[times.length-1][key]); let low=0, high=times.length; while (low<high) { const mid=Math.floor((low+high)/2); if (times[mid]<=timestamp) low=mid+1; else high=mid; } const rightIndex=low, leftIndex=rightIndex-1, ratio=(timestamp-times[leftIndex])/Math.max(1,times[rightIndex]-times[leftIndex]); return Number(data.points[leftIndex][key])+(Number(data.points[rightIndex][key])-Number(data.points[leftIndex][key]))*ratio; };
      const axisTime = timestamp => new Date(timestamp).toISOString().slice(5,16).replace("T", " ") + " UTC";
      const drawAxes = (parent, top, plotH, yScale, yLabel, isEquity) => { add("rect", {x:x0,y:top,width:x1-x0,height:plotH,class:"frame","data-chart-frame":"true"}, parent); const domain=yScale.domain, ticks=Array.from({length:5},(_,index)=>domain[0]+(domain[1]-domain[0])*index/4); ticks.forEach(value=>{const yy=(isEquity?equityY:marginY)(value); add("line",{x1:x0,x2:x1,y1:yy,y2:yy,class:"grid-line"},parent); text(parent,x0-8,yy+4,value.toFixed(0),{"text-anchor":"end"});}); text(parent,x0+3,top+15,yLabel,{class:"muted","data-axis":"y"}); [firstTime,firstTime+(lastTime-firstTime)/2,lastTime].forEach((timestamp,index)=>text(parent,x(timestamp),top+plotH+23,axisTime(timestamp),{"text-anchor":index===0?"start":index===2?"end":"middle",class:"muted","data-axis":"x"})); };
      const split = (parent, timestamp, label, top, plotH) => { const numeric=Date.parse(timestamp); if (!Number.isFinite(numeric) || numeric<firstTime || numeric>lastTime) return; const xx=x(numeric); add("line",{x1:xx,x2:xx,y1:top,y2:top+plotH,class:"split-line"},parent); text(parent,xx+4,top+14,label,{class:"muted"}); };
      drawAxes(equityChart,equityTop,equityPlotH,equityScale,"账户权益 / 模拟权益（U）",true);
      split(equityChart,data.train_end,"训练/验证",equityTop,equityPlotH); split(equityChart,data.validation_end,"验证/留出",equityTop,equityPlotH);
      drawAxes(marginChart,marginTop,marginPlotH,marginScale,"初始保证金占用（U）",false);
      const nodes = {equity:{},margin:{},equityMarkers:{},marginMarkers:{}};
      Object.entries(series).forEach(([name,item])=>{
        nodes.equity[name]=add("path",{d:linePath(item.equity,equityY),fill:"none",stroke:item.color,"stroke-width":name==="live"?2.5:2.2,"stroke-dasharray":item.dash || "none"},equityChart);
        nodes.margin[name]=add("path",{d:stepPath(item.margin,marginY),fill:"none",stroke:item.color,"stroke-width":name==="live"?2.5:2.2,"stroke-dasharray":item.dash || "none"},marginChart);
        nodes.equityMarkers[name]=add("circle",{r:4,fill:item.color,display:"none"},equityChart);
        nodes.marginMarkers[name]=add("circle",{r:4,fill:item.color,display:"none"},marginChart);
      });
      const equityGuide=add("line",{y1:equityTop,y2:equityY1,class:"hover-guide",display:"none"},equityChart);
      const marginGuide=add("line",{y1:marginTop,y2:marginY1,class:"hover-guide",display:"none"},marginChart);
      const equityOverlay=add("rect",{x:x0,y:equityTop,width:x1-x0,height:equityPlotH,fill:"transparent","data-chart-hit":"true","data-chart-hover-overlay":"cross-series"},equityChart);
      const marginOverlay=add("rect",{x:x0,y:marginTop,width:x1-x0,height:marginPlotH,fill:"transparent","data-chart-hit":"true","data-chart-hover-overlay":"cross-series"},marginChart);
      const updateHover=(event,overlay,guide,markers,yScale,key,kind)=>{const bounds=overlay.getBoundingClientRect(),ratio=Math.max(0,Math.min(1,(event.clientX-bounds.left)/Math.max(1,bounds.width))),timestamp=firstTime+ratio*(lastTime-firstTime),xx=x(timestamp); guide.setAttribute("x1",xx); guide.setAttribute("x2",xx); guide.style.display="block"; const rows=[]; Object.entries(series).forEach(([name,item])=>{const value=kind==="margin"?stepped(item[key],timestamp):interpolated(item[key],timestamp), marker=markers[name]; marker.setAttribute("cx",xx); marker.setAttribute("cy",yScale(value)); marker.style.display=visible[name]?"block":"none"; if(visible[name]) rows.push(`<div><span style="color:${item.color}">●</span> ${item.label}：${value.toFixed(2)}U</div>`);}); tooltip.innerHTML=`<strong>${new Date(timestamp).toISOString().replace("T"," ").replace(".000Z"," UTC")}</strong>${rows.join("")}`; const box=root.getBoundingClientRect(); tooltip.style.display="block"; tooltip.style.left=`${event.clientX-box.left+10}px`; tooltip.style.top=`${event.clientY-box.top+10}px`;};
      equityOverlay.addEventListener("pointermove",event=>updateHover(event,equityOverlay,equityGuide,nodes.equityMarkers,equityY,"equity","equity"));
      marginOverlay.addEventListener("pointermove",event=>updateHover(event,marginOverlay,marginGuide,nodes.marginMarkers,marginY,"margin","margin"));
      const clearHover=()=>{tooltip.style.display="none"; equityGuide.style.display="none"; marginGuide.style.display="none"; Object.values(nodes.equityMarkers).forEach(node=>node.style.display="none"); Object.values(nodes.marginMarkers).forEach(node=>node.style.display="none");};
      equityOverlay.addEventListener("pointerleave",clearHover); marginOverlay.addEventListener("pointerleave",clearHover);
      root.querySelectorAll(".series-toggle").forEach(button=>button.addEventListener("click",()=>{const name=button.dataset.series; visible[name]=!visible[name]; button.setAttribute("aria-pressed",String(visible[name])); nodes.equity[name].style.display=visible[name]?"block":"none"; nodes.margin[name].style.display=visible[name]?"block":"none";}));
    })();
  </script>
</div>
'''
    return (
        template
        .replace("__DATA__", payload)
        .replace("__UNCONSTRAINED_LABEL__", unconstrained_label)
        .replace("__MARGIN350_LABEL__", margin350_label)
        .replace("__MARGIN350CD0_LABEL__", margin350cd0_label)
        .replace("__MARGIN280_LABEL__", margin280_label)
        .replace("__MARGIN280CD0_LABEL__", margin280cd0_label)
        .replace("__DRAWDOWN_CD0_LABEL__", drawdown_cd0_label)
        .replace("__DRAWDOWN_FREE_LABEL__", drawdown_free_label)
        .replace("__DRAWDOWN_WEIGHT__", drawdown_weight_text)
        .replace("__EVALUATION_MODE__", evaluation_mode_text)
    )


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    unconstrained = json.loads(args.unconstrained_optimization_report.read_text(encoding="utf-8"))
    margin350 = json.loads(args.margin350_optimization_report.read_text(encoding="utf-8"))
    margin350cd0 = json.loads(args.margin350cd0_optimization_report.read_text(encoding="utf-8"))
    margin280 = json.loads(args.margin280_optimization_report.read_text(encoding="utf-8"))
    margin280cd0 = json.loads(args.margin280cd0_optimization_report.read_text(encoding="utf-8"))
    drawdown_cd0 = json.loads(args.drawdown_cd0_optimization_report.read_text(encoding="utf-8"))
    drawdown_free = json.loads(args.drawdown_free_optimization_report.read_text(encoding="utf-8"))
    live_report = json.loads(args.live_report.read_text(encoding="utf-8"))
    windows = [
        (parse_time(unconstrained["data_start"]), parse_time(unconstrained["data_end"])),
        (parse_time(margin350["data_start"]), parse_time(margin350["data_end"])),
        (parse_time(margin350cd0["data_start"]), parse_time(margin350cd0["data_end"])),
        (parse_time(margin280["data_start"]), parse_time(margin280["data_end"])),
        (parse_time(margin280cd0["data_start"]), parse_time(margin280cd0["data_end"])),
        (parse_time(drawdown_cd0["data_start"]), parse_time(drawdown_cd0["data_end"])),
        (parse_time(drawdown_free["data_start"]), parse_time(drawdown_free["data_end"])),
    ]
    if not all(window == windows[0] for window in windows[1:]):
        raise SystemExit("the seven optimization windows must match")
    reported_start, collection_end = windows[0]
    collection_start = (
        parse_time(args.comparison_start)
        if args.comparison_start
        else reported_start
    )
    if collection_start < reported_start or collection_start > collection_end:
        raise SystemExit("comparison start must be inside the optimization data window")

    live_points = load_live_equity_points(args.balance_snapshots)
    if not live_points:
        raise SystemExit("live balance snapshots are required")
    if live_points[0][0] > collection_start.timestamp() or live_points[-1][0] < collection_end.timestamp():
        raise SystemExit("live balance snapshots must cover the full comparison window")
    raw_start = interpolate(live_points, collection_start.timestamp())
    raw_end = interpolate(live_points, collection_end.timestamp())

    pnl_paths = {
        "unconstrained": load_pnl_points(args.unconstrained_equity_series),
        "margin350": load_pnl_points(args.margin350_equity_series),
        "margin350cd0": load_pnl_points(args.margin350cd0_equity_series),
        "margin280": load_pnl_points(args.margin280_equity_series),
        "margin280cd0": load_pnl_points(args.margin280cd0_equity_series),
        "drawdown_cd0": load_pnl_points(args.drawdown_cd0_equity_series),
        "drawdown_free": load_pnl_points(args.drawdown_free_equity_series),
    }
    if any(not points for points in pnl_paths.values()):
        raise SystemExit("all optimization equity series are required")

    settings = margin350cd0["fixed_live_settings"]
    leverage = max(number(str(settings.get("entry_leverage")), 5.0), 1e-12)
    entry_notional = max(number(str(settings.get("entry_notional_usdt")), 100.0), 0.0)
    cap_usdt = number(
        str(margin350cd0.get("margin_constraint", {}).get("max_initial_margin_usdt")),
        350.0,
    )
    cap280_usdt = number(
        str(margin280.get("margin_constraint", {}).get("max_initial_margin_usdt")),
        280.0,
    )
    cap280cd0_usdt = number(
        str(margin280cd0.get("margin_constraint", {}).get("max_initial_margin_usdt")),
        280.0,
    )
    drawdown_cap_cd0_usdt = number(
        str(drawdown_cd0.get("margin_constraint", {}).get("max_initial_margin_usdt")),
        280.0,
    )
    drawdown_cap_free_usdt = number(
        str(drawdown_free.get("margin_constraint", {}).get("max_initial_margin_usdt")),
        280.0,
    )
    margin_curves: dict[str, list[tuple[float, float]]] = {}
    margin_loads: dict[str, dict[str, Any]] = {}
    margin_curves["live"], margin_loads["live"] = load_live_margin_events(
        args.fill_events,
        args.exchange_orders,
        leverage=leverage,
    )
    event_paths = {
        "unconstrained": args.unconstrained_events,
        "margin350": args.margin350_events,
        "margin350cd0": args.margin350cd0_events,
        "margin280": args.margin280_events,
        "margin280cd0": args.margin280cd0_events,
        "drawdown_cd0": args.drawdown_cd0_events,
        "drawdown_free": args.drawdown_free_events,
    }
    for key, path in event_paths.items():
        margin_curves[key], margin_loads[key] = load_simulated_margin_events(
            path,
            entry_notional=entry_notional,
            leverage=leverage,
        )

    grid = sample_grid(collection_start, collection_end)
    chart_timestamps = {timestamp.timestamp() for timestamp in grid}
    for points in (*pnl_paths.values(), *margin_curves.values()):
        chart_timestamps.update(
            timestamp
            for timestamp, _ in points
            if collection_start.timestamp() <= timestamp <= collection_end.timestamp()
        )
    chart_times = [datetime.fromtimestamp(timestamp, tz=UTC) for timestamp in sorted(chart_timestamps)]
    points: list[dict[str, Any]] = []
    for timestamp in chart_times:
        epoch = timestamp.timestamp()
        unconstrained_pnl = step_value(pnl_paths["unconstrained"], epoch)
        margin350_pnl = step_value(pnl_paths["margin350"], epoch)
        margin350cd0_pnl = step_value(pnl_paths["margin350cd0"], epoch)
        margin280_pnl = step_value(pnl_paths["margin280"], epoch)
        margin280cd0_pnl = step_value(pnl_paths["margin280cd0"], epoch)
        drawdown_cd0_pnl = step_value(pnl_paths["drawdown_cd0"], epoch)
        drawdown_free_pnl = step_value(pnl_paths["drawdown_free"], epoch)
        points.append(
            {
                "timestamp": iso(timestamp),
                "live_equity_usdt": round(interpolate(live_points, epoch), 8),
                "unconstrained_equity_usdt": round(raw_start + unconstrained_pnl, 8),
                "margin350_equity_usdt": round(raw_start + margin350_pnl, 8),
                "margin350cd0_equity_usdt": round(raw_start + margin350cd0_pnl, 8),
                "margin280_equity_usdt": round(raw_start + margin280_pnl, 8),
                "margin280cd0_equity_usdt": round(raw_start + margin280cd0_pnl, 8),
                "drawdown_cd0_equity_usdt": round(raw_start + drawdown_cd0_pnl, 8),
                "drawdown_free_equity_usdt": round(raw_start + drawdown_free_pnl, 8),
                "unconstrained_pnl": round(unconstrained_pnl, 8),
                "margin350_pnl": round(margin350_pnl, 8),
                "margin350cd0_pnl": round(margin350cd0_pnl, 8),
                "margin280_pnl": round(margin280_pnl, 8),
                "margin280cd0_pnl": round(margin280cd0_pnl, 8),
                "drawdown_cd0_pnl": round(drawdown_cd0_pnl, 8),
                "drawdown_free_pnl": round(drawdown_free_pnl, 8),
                "live_margin_usdt": round(step_value(margin_curves["live"], epoch), 8),
                "unconstrained_margin_usdt": round(step_value(margin_curves["unconstrained"], epoch), 8),
                "margin350_margin_usdt": round(step_value(margin_curves["margin350"], epoch), 8),
                "margin350cd0_margin_usdt": round(step_value(margin_curves["margin350cd0"], epoch), 8),
                "margin280_margin_usdt": round(step_value(margin_curves["margin280"], epoch), 8),
                "margin280cd0_margin_usdt": round(step_value(margin_curves["margin280cd0"], epoch), 8),
                "drawdown_cd0_margin_usdt": round(step_value(margin_curves["drawdown_cd0"], epoch), 8),
                "drawdown_free_margin_usdt": round(step_value(margin_curves["drawdown_free"], epoch), 8),
            }
        )

    margin_summaries = {
        key: margin_summary(curve, start=collection_start, end=collection_end)
        for key, curve in margin_curves.items()
    }
    frozen_replay = all(
        item.get("evaluation_mode") == "frozen_parameters_extended_replay"
        for item in (
            unconstrained,
            margin350,
            margin350cd0,
            margin280,
            margin280cd0,
            drawdown_cd0,
            drawdown_free,
        )
    )
    for key, cap in (
        ("margin350", cap_usdt),
        ("margin350cd0", cap_usdt),
        ("margin280", cap280_usdt),
        ("margin280cd0", cap280cd0_usdt),
        ("drawdown_cd0", drawdown_cap_cd0_usdt),
        ("drawdown_free", drawdown_cap_free_usdt),
    ):
        if margin_summaries[key]["peak_usdt"] > cap + 1e-6:
            if frozen_replay:
                continue
            raise SystemExit(
                f"margin cap violation for {key}: peak {margin_summaries[key]['peak_usdt']}U > {cap}U"
            )

    strategies = {
        "unconstrained": strategy_summary(
            unconstrained,
            pnl_paths["unconstrained"],
            label="寻优 A（无保证金上限）",
            cap_usdt=None,
        ),
        "margin350": strategy_summary(
            margin350,
            pnl_paths["margin350"],
            label=(
                f"寻优 B（保证金≤{cap_usdt:.0f}U，自由cooldown→"
                f"{margin350['best_validation']['config']['cooldown_buckets']}，自然峰值约束）"
            ),
            cap_usdt=cap_usdt,
        ),
        "margin350cd0": strategy_summary(
            margin350cd0,
            pnl_paths["margin350cd0"],
            label=(
                f"寻优 C（保证金≤{cap_usdt:.0f}U，固定cooldown="
                f"{margin350cd0['best_validation']['config']['cooldown_buckets']}，自然峰值约束）"
            ),
            cap_usdt=cap_usdt,
        ),
        "margin280": strategy_summary(
            margin280,
            pnl_paths["margin280"],
            label=(
                f"寻优 D（保证金≤{cap280_usdt:.0f}U，自由cooldown→"
                f"{margin280['best_validation']['config']['cooldown_buckets']}，自然峰值约束）"
            ),
            cap_usdt=cap280_usdt,
        ),
        "margin280cd0": strategy_summary(
            margin280cd0,
            pnl_paths["margin280cd0"],
            label=(
                f"寻优 E（保证金≤{cap280cd0_usdt:.0f}U，固定cooldown="
                f"{margin280cd0['best_validation']['config']['cooldown_buckets']}，自然峰值约束）"
            ),
            cap_usdt=cap280cd0_usdt,
        ),
        "drawdown_cd0": strategy_summary(
            drawdown_cd0,
            pnl_paths["drawdown_cd0"],
            label=(
                f"寻优 F（保证金≤{drawdown_cap_cd0_usdt:.0f}U，固定cooldown=0；"
                "PnL优先、回撤次优）"
            ),
            cap_usdt=drawdown_cap_cd0_usdt,
        ),
        "drawdown_free": strategy_summary(
            drawdown_free,
            pnl_paths["drawdown_free"],
            label=(
                f"寻优 G（保证金≤{drawdown_cap_free_usdt:.0f}U，自由cooldown→"
                f"{drawdown_free['best_validation']['config']['cooldown_buckets']}；"
                "PnL优先、回撤次优）"
            ),
            cap_usdt=drawdown_cap_free_usdt,
        ),
    }
    for key, cap in (
        ("margin350", cap_usdt),
        ("margin350cd0", cap_usdt),
        ("margin280", cap280_usdt),
        ("margin280cd0", cap280cd0_usdt),
        ("drawdown_cd0", drawdown_cap_cd0_usdt),
        ("drawdown_free", drawdown_cap_free_usdt),
    ):
        peak = margin_summaries[key]["peak_usdt"]
        if peak > cap + 1e-6 and frozen_replay:
            strategies[key]["label"] += f"；验证峰值{peak:.0f}U>上限{cap:.0f}U"
    for key in strategies:
        strategies[key]["natural_initial_margin_peak_usdt"] = margin_summaries[key]["peak_usdt"]
        strategies[key]["margin_load"] = margin_loads[key]

    return {
        "evaluation_mode": (
            "frozen_parameters_extended_replay"
            if frozen_replay
            else "latest_window_optimization_replay"
        ),
        "source": {
            "balance_snapshots": str(args.balance_snapshots),
            "fill_events": str(args.fill_events),
            "exchange_orders": str(args.exchange_orders),
            "live_report": str(args.live_report),
            "unconstrained_optimization_report": str(args.unconstrained_optimization_report),
            "margin350_optimization_report": str(args.margin350_optimization_report),
            "margin350cd0_optimization_report": str(args.margin350cd0_optimization_report),
            "margin280_optimization_report": str(args.margin280_optimization_report),
            "margin280cd0_optimization_report": str(args.margin280cd0_optimization_report),
            "drawdown_cd0_optimization_report": str(args.drawdown_cd0_optimization_report),
            "drawdown_free_optimization_report": str(args.drawdown_free_optimization_report),
            "unconstrained_events": str(args.unconstrained_events),
            "margin350_events": str(args.margin350_events),
            "margin350cd0_events": str(args.margin350cd0_events),
            "margin280_events": str(args.margin280_events),
            "margin280cd0_events": str(args.margin280cd0_events),
            "drawdown_cd0_events": str(args.drawdown_cd0_events),
            "drawdown_free_events": str(args.drawdown_free_events),
        },
        "collection": {
            "start_utc": iso(collection_start),
            "end_utc": iso(collection_end),
            "duration_hours": (collection_end - collection_start).total_seconds() / 3600.0,
            "duration_text": f"{(collection_end - collection_start).total_seconds() / 3600.0:.1f} 小时",
        },
        "live": {
            "label": "真实实盘",
            "config": LIVE_BASELINE_CONFIG,
            "raw_start_usdt": raw_start,
            "raw_end_usdt": raw_end,
            "raw_change_usdt": raw_end - raw_start,
            "raw_change_pct": (raw_end / raw_start - 1.0) * 100.0 if raw_start else None,
            "margin": {**margin_summaries["live"], **margin_loads["live"]},
        },
        "strategies": strategies,
        "margin": {
            "entry_notional_usdt": entry_notional,
            "leverage": leverage,
            "initial_margin_per_entry_usdt": entry_notional / leverage,
            "cap_initial_margin_usdt": cap_usdt,
            "cap_margin280_initial_margin_usdt": cap280_usdt,
            "cap_margin280cd0_initial_margin_usdt": cap280cd0_usdt,
            "cap_drawdown_cd0_initial_margin_usdt": drawdown_cap_cd0_usdt,
            "cap_drawdown_free_initial_margin_usdt": drawdown_cap_free_usdt,
            **{
                key: {**margin_summaries[key], **margin_loads[key]}
                for key in margin_curves
            },
        },
        "chart": {
            "start": iso(collection_start),
            "mid": iso(collection_start + (collection_end - collection_start) / 2),
            "end": iso(collection_end),
            "train_end": unconstrained["splits"]["train"]["end"],
            "validation_end": unconstrained["splits"]["validation"]["end"],
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
        "margin350cd0_equity_usdt",
        "margin280_equity_usdt",
        "margin280cd0_equity_usdt",
        "drawdown_cd0_equity_usdt",
        "drawdown_free_equity_usdt",
        "unconstrained_pnl",
        "margin350_pnl",
        "margin350cd0_pnl",
        "margin280_pnl",
        "margin280cd0_pnl",
        "drawdown_cd0_pnl",
        "drawdown_free_pnl",
    ]
    margin_fields = [
        "timestamp",
        "live_margin_usdt",
        "unconstrained_margin_usdt",
        "margin350_margin_usdt",
        "margin350cd0_margin_usdt",
        "margin280_margin_usdt",
        "margin280cd0_margin_usdt",
        "drawdown_cd0_margin_usdt",
        "drawdown_free_margin_usdt",
    ]
    for filename, fields in (
        ("eight_strategy_equity_curve.csv", equity_fields),
        ("eight_strategy_margin_curve.csv", margin_fields),
    ):
        with (output_dir / filename).open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows({field: row[field] for field in fields} for row in points)
    (output_dir / "eight_strategy_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    live = report["live"]
    strategies = report["strategies"]
    margin = report["margin"]
    cap = margin["cap_initial_margin_usdt"]
    cap280 = margin["cap_margin280_initial_margin_usdt"]
    cap280cd0 = margin["cap_margin280cd0_initial_margin_usdt"]
    drawdown_cap_cd0 = margin["cap_drawdown_cd0_initial_margin_usdt"]
    drawdown_cap_free = margin["cap_drawdown_free_initial_margin_usdt"]
    drawdown_weight = strategies["drawdown_cd0"].get("drawdown_weight")
    if drawdown_weight is None:
        drawdown_weight = strategies["drawdown_free"].get("drawdown_weight")
    drawdown_weight = float(drawdown_weight if drawdown_weight is not None else 0.10)
    evaluation_mode = report.get("evaluation_mode")
    if evaluation_mode == "frozen_parameters_extended_replay":
        evaluation_mode_text = (
            "本次为最新数据扩展回放：A-G 参数均冻结为上一轮选定值，"
            "没有在新增数据上重新寻优"
        )
    else:
        evaluation_mode_text = "本次按各策略定义完成寻优后的全窗口回放"
    def cap_status(key: str, limit: float) -> str:
        peak = margin[key]["peak_usdt"]
        if peak <= limit + 1e-6:
            return f"自然峰值 {peak:.2f}U，符合 {limit:.2f}U 上限"
        return (
            f"验证扩展自然峰值 {peak:.2f}U，超出 {limit:.2f}U 上限；"
            "该参数在验证段不满足限额要求"
        )
    markdown = f'''# 八策略绝对权益与保证金对比

## 对比区间

- 采集区间：`{report["collection"]["start_utc"]}` 至 `{report["collection"]["end_utc"]}`，共 {report["collection"]["duration_text"]}
- 八条权益线均以采集起点实盘权益 {live["raw_start_usdt"]:.2f}U 为绝对金额起点
- 实盘基准：账户余额快照中的真实 `wallet_balance + unrealized_pnl`
- {evaluation_mode_text}

## 八个策略

### 1. 真实实盘

- 参数：`{json.dumps(live["config"], ensure_ascii=False)}`
- 权益：{live["raw_start_usdt"]:.2f}U → {live["raw_end_usdt"]:.2f}U，变化 {live["raw_change_usdt"]:+.2f}U（{live["raw_change_pct"]:+.2f}%）
- 保证金估算：起点 {margin["live"]["start_usdt"]:.2f}U，峰值 {margin["live"]["peak_usdt"]:.2f}U，终点 {margin["live"]["end_usdt"]:.2f}U

### 2. {strategies["unconstrained"]["label"]}

- 参数：`{json.dumps(strategies["unconstrained"]["config"], ensure_ascii=False)}`
- 全区间模拟：{strategies["unconstrained"]["full_pnl_usdt"]:+.2f}U；验证集 {strategies["unconstrained"]["validation"]["net_pnl_usdt"]:+.2f}U；留出集 {strategies["unconstrained"]["holdout"]["net_pnl_usdt"]:+.2f}U
- 保证金估算：峰值 {margin["unconstrained"]["peak_usdt"]:.2f}U，终点 {margin["unconstrained"]["end_usdt"]:.2f}U

### 3. {strategies["margin350"]["label"]}

- 参数：`{json.dumps(strategies["margin350"]["config"], ensure_ascii=False)}`
- 全区间模拟：{strategies["margin350"]["full_pnl_usdt"]:+.2f}U；验证集 {strategies["margin350"]["validation"]["net_pnl_usdt"]:+.2f}U；留出集 {strategies["margin350"]["holdout"]["net_pnl_usdt"]:+.2f}U
- 保证金估算：自然峰值 {margin["margin350"]["peak_usdt"]:.2f}U，终点 {margin["margin350"]["end_usdt"]:.2f}U；{cap_status("margin350", cap)}

### 4. {strategies["margin350cd0"]["label"]}

- 参数：`{json.dumps(strategies["margin350cd0"]["config"], ensure_ascii=False)}`
- 全区间模拟：{strategies["margin350cd0"]["full_pnl_usdt"]:+.2f}U；验证集 {strategies["margin350cd0"]["validation"]["net_pnl_usdt"]:+.2f}U；留出集 {strategies["margin350cd0"]["holdout"]["net_pnl_usdt"]:+.2f}U
- 保证金估算：自然峰值 {margin["margin350cd0"]["peak_usdt"]:.2f}U，终点 {margin["margin350cd0"]["end_usdt"]:.2f}U；{cap_status("margin350cd0", cap)}

### 5. {strategies["margin280"]["label"]}

- 参数：`{json.dumps(strategies["margin280"]["config"], ensure_ascii=False)}`
- 全区间模拟：{strategies["margin280"]["full_pnl_usdt"]:+.2f}U；验证集 {strategies["margin280"]["validation"]["net_pnl_usdt"]:+.2f}U；留出集 {strategies["margin280"]["holdout"]["net_pnl_usdt"]:+.2f}U
- 保证金估算：自然峰值 {margin["margin280"]["peak_usdt"]:.2f}U，终点 {margin["margin280"]["end_usdt"]:.2f}U；{cap_status("margin280", cap280)}

### 6. {strategies["margin280cd0"]["label"]}

- 参数：`{json.dumps(strategies["margin280cd0"]["config"], ensure_ascii=False)}`
- 全区间模拟：{strategies["margin280cd0"]["full_pnl_usdt"]:+.2f}U；验证集 {strategies["margin280cd0"]["validation"]["net_pnl_usdt"]:+.2f}U；留出集 {strategies["margin280cd0"]["holdout"]["net_pnl_usdt"]:+.2f}U
- 保证金估算：自然峰值 {margin["margin280cd0"]["peak_usdt"]:.2f}U，终点 {margin["margin280cd0"]["end_usdt"]:.2f}U；{cap_status("margin280cd0", cap280cd0)}

### 7. {strategies["drawdown_cd0"]["label"]}

- 参数：`{json.dumps(strategies["drawdown_cd0"]["config"], ensure_ascii=False)}`
- 单目标分数：验证集绝对净 PnL − {drawdown_weight:g} × 验证集最大回撤；分数 {strategies["drawdown_cd0"]["validation_selection_score"]:.2f}，验证集最大回撤 {strategies["drawdown_cd0"]["validation"]["max_drawdown_usdt"]:.2f}U
- 全区间模拟：{strategies["drawdown_cd0"]["full_pnl_usdt"]:+.2f}U；验证集 {strategies["drawdown_cd0"]["validation"]["net_pnl_usdt"]:+.2f}U；留出集 {strategies["drawdown_cd0"]["holdout"]["net_pnl_usdt"]:+.2f}U
- 保证金估算：自然峰值 {margin["drawdown_cd0"]["peak_usdt"]:.2f}U，终点 {margin["drawdown_cd0"]["end_usdt"]:.2f}U；{cap_status("drawdown_cd0", drawdown_cap_cd0)}

### 8. {strategies["drawdown_free"]["label"]}

- 参数：`{json.dumps(strategies["drawdown_free"]["config"], ensure_ascii=False)}`
- 单目标分数：验证集绝对净 PnL − {drawdown_weight:g} × 验证集最大回撤；分数 {strategies["drawdown_free"]["validation_selection_score"]:.2f}，验证集最大回撤 {strategies["drawdown_free"]["validation"]["max_drawdown_usdt"]:.2f}U
- 全区间模拟：{strategies["drawdown_free"]["full_pnl_usdt"]:+.2f}U；验证集 {strategies["drawdown_free"]["validation"]["net_pnl_usdt"]:+.2f}U；留出集 {strategies["drawdown_free"]["holdout"]["net_pnl_usdt"]:+.2f}U
- 保证金估算：自然峰值 {margin["drawdown_free"]["peak_usdt"]:.2f}U，终点 {margin["drawdown_free"]["end_usdt"]:.2f}U；{cap_status("drawdown_free", drawdown_cap_free)}

## 共同设置与口径

- 七次寻优只调整入场参数；Top10、只做多、100U 名义仓位、5倍杠杆、限价单 TTL 900 秒、15分钟 K线退出、B8 恢复逻辑、双边费率 0.05% 均保持实盘设置
- `cooldown=0` 只固定冷却参数为 0，其余五个入场参数重新寻优
- A-E 沿用既有验证集绝对净 PnL 目标；F/G 改用单目标 `验证集绝对净 PnL − {drawdown_weight:g} × 验证集最大回撤`（至少 10 笔已平仓交易）。因此 PnL 是主要贡献，PnL 接近时较小回撤会改善分数
- 保证金约束口径：每组参数完整回放，不做动态拒单；B/C 要求天然初始保证金峰值 ≤ {cap:.2f}U，D/E/F/G 要求 ≤ {cap280:.2f}U。每笔初始保证金 = 100U ÷ 5 = 20U，因此 D/E/F/G 最多自然出现 14 笔同时占用，峰值为 280U
- 退出回放使用本地15秒状态合成15分钟K线；实际实盘使用官方已收盘K线和真实成交，因此模拟仍是研究近似
'''
    (output_dir / "eight_strategy_report.md").write_text(markdown, encoding="utf-8")
    (output_dir / "eight-strategy-equity-margin.html").write_text(
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
    parser.add_argument("--margin350cd0-optimization-report", type=Path, required=True)
    parser.add_argument("--margin350cd0-equity-series", type=Path, required=True)
    parser.add_argument("--margin350cd0-events", type=Path, required=True)
    parser.add_argument("--margin280-optimization-report", type=Path, required=True)
    parser.add_argument("--margin280-equity-series", type=Path, required=True)
    parser.add_argument("--margin280-events", type=Path, required=True)
    parser.add_argument("--margin280cd0-optimization-report", type=Path, required=True)
    parser.add_argument("--margin280cd0-equity-series", type=Path, required=True)
    parser.add_argument("--margin280cd0-events", type=Path, required=True)
    parser.add_argument("--drawdown-cd0-optimization-report", type=Path, required=True)
    parser.add_argument("--drawdown-cd0-equity-series", type=Path, required=True)
    parser.add_argument("--drawdown-cd0-events", type=Path, required=True)
    parser.add_argument("--drawdown-free-optimization-report", type=Path, required=True)
    parser.add_argument("--drawdown-free-equity-series", type=Path, required=True)
    parser.add_argument("--drawdown-free-events", type=Path, required=True)
    parser.add_argument("--balance-snapshots", type=Path, required=True)
    parser.add_argument("--fill-events", type=Path, required=True)
    parser.add_argument("--exchange-orders", type=Path, required=True)
    parser.add_argument("--live-report", type=Path, required=True)
    parser.add_argument(
        "--comparison-start",
        help="optional comparison start; defaults to the optimization data start",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    report = build_report(args)
    write_outputs(report, args.output_dir)
    print(json.dumps({"collection": report["collection"], "live": report["live"], "strategies": report["strategies"], "margin": report["margin"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
