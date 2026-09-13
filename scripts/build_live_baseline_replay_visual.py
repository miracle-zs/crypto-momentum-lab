#!/usr/bin/env python3
"""Build a live-account versus fixed-live-parameter replay comparison."""

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


def load_live_equity_points(path: Path) -> list[tuple[float, float]]:
    rows = read_csv(path)
    rows.sort(key=lambda row: parse_time(row["observed_at"]))
    return [
        (
            parse_time(row["observed_at"]).timestamp(),
            number(row.get("wallet_balance")) + number(row.get("unrealized_pnl")),
        )
        for row in rows
    ]


def load_baseline_pnl_points(path: Path) -> list[tuple[float, float]]:
    rows = read_csv(path)
    rows.sort(key=lambda row: parse_time(row["timestamp"]))
    return [
        (
            parse_time(row["timestamp"]).timestamp(),
            number(row.get("baseline_cumulative_pnl_usdt")),
        )
        for row in rows
    ]


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    optimization = json.loads(args.optimization_report.read_text(encoding="utf-8"))
    start = parse_time(optimization["data_start"])
    proxy_start = parse_time(optimization["optimization_window"]["start"])
    end = parse_time(optimization["data_end"])
    live_points = load_live_equity_points(args.balance_snapshots)
    replay_points = load_baseline_pnl_points(args.equity_series)
    if not live_points or not replay_points:
        raise SystemExit("live balance and baseline replay series are required")
    if live_points[0][0] > start.timestamp() or live_points[-1][0] < end.timestamp():
        raise SystemExit("live balance snapshots must cover the replay window")

    live_start = interpolate(live_points, start.timestamp())
    live_proxy = interpolate(live_points, proxy_start.timestamp())
    live_end = interpolate(live_points, end.timestamp())
    replay_proxy_pnl = step_value(replay_points, proxy_start.timestamp())

    live_margin_curve, live_margin_load = load_live_margin_events(
        args.fill_events,
        args.exchange_orders,
        leverage=5.0,
    )
    replay_margin_curve, replay_margin_load = load_simulated_margin_events(
        args.baseline_events,
        entry_notional=100.0,
        leverage=5.0,
    )

    chart_timestamps = {timestamp.timestamp() for timestamp in sample_grid(start, end)}
    for curve in (replay_points, live_margin_curve, replay_margin_curve):
        chart_timestamps.update(
            timestamp
            for timestamp, _ in curve
            if start.timestamp() <= timestamp <= end.timestamp()
        )
    chart_times = [datetime.fromtimestamp(value, tz=UTC) for value in sorted(chart_timestamps)]
    points: list[dict[str, Any]] = []
    for timestamp in chart_times:
        epoch = timestamp.timestamp()
        replay_pnl = step_value(replay_points, epoch)
        points.append(
            {
                "timestamp": iso(timestamp),
                "live_equity_usdt": round(interpolate(live_points, epoch), 8),
                "replay_equity_usdt": round(live_start + replay_pnl, 8),
                "replay_rebased_equity_usdt": round(
                    live_proxy + replay_pnl - replay_proxy_pnl,
                    8,
                ),
                "live_margin_usdt": round(step_value(live_margin_curve, epoch), 8),
                "replay_margin_usdt": round(step_value(replay_margin_curve, epoch), 8),
            }
        )

    live_margin = margin_summary(live_margin_curve, start=start, end=end)
    replay_margin = margin_summary(replay_margin_curve, start=start, end=end)
    return {
        "source": {
            "optimization_report": str(args.optimization_report),
            "equity_series": str(args.equity_series),
            "baseline_events": str(args.baseline_events),
            "balance_snapshots": str(args.balance_snapshots),
            "fill_events": str(args.fill_events),
            "exchange_orders": str(args.exchange_orders),
        },
        "window": {
            "start_utc": iso(start),
            "proxy_start_utc": iso(proxy_start),
            "end_utc": iso(end),
            "duration_hours": (end - start).total_seconds() / 3600.0,
            "duration_text": f"{(end - start).total_seconds() / 3600.0:.1f} 小时",
        },
        "live": {
            "label": "真实实盘",
            "start_equity_usdt": live_start,
            "proxy_start_equity_usdt": live_proxy,
            "end_equity_usdt": live_end,
            "full_change_usdt": live_end - live_start,
            "before_proxy_change_usdt": live_proxy - live_start,
            "after_proxy_change_usdt": live_end - live_proxy,
            "margin": {**live_margin, **live_margin_load},
        },
        "replay": {
            "label": "实盘参数回放（采集起点锚定）",
            "config": optimization["baseline"]["config"],
            "start_equity_usdt": live_start,
            "end_equity_usdt": live_start + step_value(replay_points, end.timestamp()),
            "full_pnl_usdt": step_value(replay_points, end.timestamp()),
            "pnl_at_proxy_start_usdt": replay_proxy_pnl,
            "margin": {**replay_margin, **replay_margin_load},
        },
        "diagnosis": {
            "replay_full_minus_live_usdt": round(
                step_value(replay_points, end.timestamp()) - (live_end - live_start),
                8,
            ),
            "live_minus_replay_full_usdt": round(
                (live_end - live_start) - step_value(replay_points, end.timestamp()),
                8,
            ),
            "live_change_before_proxy_usdt": round(live_proxy - live_start, 8),
            "replay_change_before_proxy_usdt": round(replay_proxy_pnl, 8),
            "live_change_after_proxy_usdt": round(live_end - live_proxy, 8),
            "replay_change_after_proxy_usdt": round(
                step_value(replay_points, end.timestamp()) - replay_proxy_pnl,
                8,
            ),
            "fair_window_replay_minus_live_usdt": round(
                (step_value(replay_points, end.timestamp()) - replay_proxy_pnl)
                - (live_end - live_proxy),
                8,
            ),
            "top10_proxy_disabled_before_proxy_start": True,
        },
        "chart": {
            "start": iso(start),
            "mid": iso(start + (end - start) / 2),
            "end": iso(end),
            "proxy_start": iso(proxy_start),
            "points": points,
        },
    }


def build_html(report: dict[str, Any]) -> str:
    payload = json.dumps(report["chart"], ensure_ascii=False, separators=(",", ":"))
    template = '''<div id="cml-live-baseline-replay-compare" aria-label="真实实盘与实盘参数原样回放的权益和保证金对比">
  <style>
    #cml-live-baseline-replay-compare {
      color: var(--foreground);
      display: block;
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      font-size: 14px;
      line-height: 1.4;
      position: relative;
      width: 100%;
    }
    #cml-live-baseline-replay-compare .title { font-size: 17px; font-weight: 500; margin: 0 0 3px; }
    #cml-live-baseline-replay-compare .subtitle { color: var(--muted-foreground); margin: 0 0 10px; }
    #cml-live-baseline-replay-compare .section-title { font-size: 14px; font-weight: 500; margin: 14px 0 3px; }
    #cml-live-baseline-replay-compare .legend { align-items: center; display: flex; flex-wrap: wrap; gap: 4px 12px; margin: 5px 0 8px; }
    #cml-live-baseline-replay-compare .series-toggle { background: transparent; border: 0; color: var(--foreground); cursor: pointer; font: inherit; padding: 2px 0; }
    #cml-live-baseline-replay-compare .series-toggle[aria-pressed="false"] { color: var(--muted-foreground); }
    #cml-live-baseline-replay-compare .swatch { display: inline-block; height: 3px; margin: 0 5px 3px 0; width: 18px; }
    #cml-live-baseline-replay-compare svg { display: block; height: auto; overflow: visible; width: 100%; }
    #cml-live-baseline-replay-compare text { fill: var(--foreground); font-size: 12px; }
    #cml-live-baseline-replay-compare .muted { fill: var(--muted-foreground); }
    #cml-live-baseline-replay-compare .frame { fill: none; stroke: var(--border); stroke-width: 1; }
    #cml-live-baseline-replay-compare .grid-line { stroke: var(--border); stroke-dasharray: 2 3; opacity: .75; }
    #cml-live-baseline-replay-compare .split-line { stroke: var(--muted-foreground); stroke-dasharray: 5 4; opacity: .8; }
    #cml-live-baseline-replay-compare .hover-guide { stroke: var(--muted-foreground); stroke-dasharray: 3 3; opacity: .8; }
    #cml-live-baseline-replay-compare .tooltip { background: var(--popover); border: 1px solid var(--border); border-radius: 5px; color: var(--popover-foreground); display: none; max-width: 330px; padding: 6px 8px; pointer-events: none; position: absolute; z-index: 3; }
    #cml-live-baseline-replay-compare .note { color: var(--muted-foreground); margin: 7px 0 0; }
    @media (max-width: 460px) { #cml-live-baseline-replay-compare .legend { gap: 2px 9px; } }
  </style>
  <div class="title">真实实盘 vs 实盘参数原样回放</div>
  <p class="subtitle">回放参数固定为实盘基线；实线为采集起点锚定，虚线为同一回放从 2026-09-04 00:00 的有效段起点重锚。竖虚线左侧是 Top10 代理尚未生效的部分日。</p>
  <div class="legend" role="group" aria-label="权益曲线开关">
    <button type="button" class="series-toggle" data-series="live" data-chart="equity" aria-pressed="true"><span class="swatch" style="background:var(--viz-series-1)"></span>真实实盘</button>
    <button type="button" class="series-toggle" data-series="replay" data-chart="equity" aria-pressed="true"><span class="swatch" style="background:var(--viz-series-2)"></span>参数回放（起点锚定）</button>
    <button type="button" class="series-toggle" data-series="rebased" data-chart="equity" aria-pressed="true"><span class="swatch" style="background:var(--viz-series-3)"></span>参数回放（有效段重锚）</button>
  </div>
  <div class="section-title">账户权益（U）</div>
  <svg id="cml-baseline-replay-equity-svg" viewBox="0 0 980 430" role="img" aria-label="真实实盘、起点锚定回放、有效段重锚回放的绝对权益曲线"><title>实盘参数回放与真实实盘权益曲线</title><g id="cml-baseline-replay-equity-chart"></g></svg>
  <div class="legend" role="group" aria-label="保证金曲线开关">
    <button type="button" class="series-toggle" data-series="live" data-chart="margin" aria-pressed="true"><span class="swatch" style="background:var(--viz-series-1)"></span>真实实盘</button>
    <button type="button" class="series-toggle" data-series="replay" data-chart="margin" aria-pressed="true"><span class="swatch" style="background:var(--viz-series-2)"></span>参数回放</button>
  </div>
  <div class="section-title">保证金占用（U）</div>
  <svg id="cml-baseline-replay-margin-svg" viewBox="0 0 980 350" role="img" aria-label="真实实盘与实盘参数回放的初始保证金占用曲线"><title>实盘参数回放与真实实盘保证金曲线</title><g id="cml-baseline-replay-margin-chart"></g></svg>
  <div class="tooltip" id="cml-baseline-replay-tooltip" role="tooltip"></div>
  <p class="note">权益重锚线不是第三个策略，只是把同一回放的起点改为 2026-09-04 00:00 的真实权益，用来隔离前段 Top10 代理缺失造成的口径差异。</p>
  <script>
    (() => {
      const root = document.getElementById("cml-live-baseline-replay-compare");
      const equitySvg = document.getElementById("cml-baseline-replay-equity-svg");
      const equityChart = document.getElementById("cml-baseline-replay-equity-chart");
      const marginSvg = document.getElementById("cml-baseline-replay-margin-svg");
      const marginChart = document.getElementById("cml-baseline-replay-margin-chart");
      const tooltip = document.getElementById("cml-baseline-replay-tooltip");
      const data = __DATA__;
      const NS = "http://www.w3.org/2000/svg";
      const series = {
        live: {label:"真实实盘", color:"var(--viz-series-1)", equity:"live_equity_usdt", margin:"live_margin_usdt"},
        replay: {label:"参数回放（起点锚定）", color:"var(--viz-series-2)", equity:"replay_equity_usdt", margin:"replay_margin_usdt"},
        rebased: {label:"参数回放（有效段重锚）", color:"var(--viz-series-3)", equity:"replay_rebased_equity_usdt", margin:"replay_margin_usdt"}
      };
      const equityVisible = {live:true, replay:true, rebased:true};
      const marginVisible = {live:true, replay:true};
      let pinned = false;
      const add = (name, attrs, parent) => { const node = document.createElementNS(NS, name); Object.entries(attrs || {}).forEach(([key,value]) => node.setAttribute(key, String(value))); parent.appendChild(node); return node; };
      const text = (parent, x, y, value, attrs = {}) => { const node = add("text", {x, y, ...attrs}, parent); node.appendChild(document.createTextNode(value)); return node; };
      const finite = value => Number.isFinite(Number(value));
      const dataTimes = data.points.map(row => Date.parse(row.timestamp));
      const xValue = (timestamp, firstTime, lastTime, x0, x1) => x0 + (timestamp-firstTime) / Math.max(1,lastTime-firstTime) * (x1-x0);
      const axisTime = timestamp => new Date(timestamp).toISOString().slice(5,16).replace("T", " ") + " UTC";
      const makeScale = (values, floorAtZero) => { const low=Math.min(...values), high=Math.max(...values), span=Math.max(1,high-low), pad=span*.08; return {domain:[floorAtZero?Math.max(0,low-pad):low-pad,high+pad]}; };
      const clear = parent => { while (parent.firstChild) parent.removeChild(parent.firstChild); };
      const draw = (svg, chart, height, keys, visible, isEquity) => {
        const width = Math.max(320, svg.getBoundingClientRect().width || 980);
        svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
        clear(chart);
        const left = Math.max(64, Math.min(82, width*.11)), right = 14, top = 24, plotH = isEquity ? 300 : 240;
        const x0=left, x1=width-right, y1=top+plotH, firstTime=dataTimes[0], lastTime=dataTimes[dataTimes.length-1];
        const values=data.points.flatMap(row => keys.filter(name=>visible[name]).map(name => Number(row[series[name][isEquity?"equity":"margin"]]))).filter(finite);
        const yScale=makeScale(values,isEquity?false:true), y=value => y1-(Number(value)-yScale.domain[0])/(yScale.domain[1]-yScale.domain[0]||1)*plotH;
        const x=timestamp => xValue(timestamp,firstTime,lastTime,x0,x1);
        const yTicks=Array.from({length:5},(_,index)=>yScale.domain[0]+(yScale.domain[1]-yScale.domain[0])*index/4);
        add("rect",{x:x0,y:top,width:x1-x0,height:plotH,class:"frame","data-chart-frame":"true"},chart);
        yTicks.forEach(value=>{const yy=y(value); add("line",{x1:x0,x2:x1,y1:yy,y2:yy,class:"grid-line"},chart); text(chart,x0-8,yy+4,value.toFixed(0),{"text-anchor":"end"});});
        text(chart,x0+3,top+15,isEquity?"账户权益（U）":"初始保证金（U）",{class:"muted","data-axis":"y"});
        [firstTime,firstTime+(lastTime-firstTime)/2,lastTime].forEach((timestamp,index)=>text(chart,x(timestamp),top+plotH+23,axisTime(timestamp),{"text-anchor":index===0?"start":index===2?"end":"middle",class:"muted","data-axis":"x"}));
        const proxyTime=Date.parse(data.proxy_start); if (proxyTime>=firstTime && proxyTime<=lastTime) { const xx=x(proxyTime); add("line",{x1:xx,x2:xx,y1:top,y2:y1,class:"split-line"},chart); text(chart,xx+4,top+14,"Top10代理生效",{class:"muted"}); }
        const path = (key, stepped) => { let output=""; data.points.forEach((row,index)=>{const timestamp=Date.parse(row.timestamp),xx=x(timestamp),value=Number(row[series[key][isEquity?"equity":"margin"]]),yy=y(value); if (!index) output=`M${xx},${yy}`; else if (stepped) output+=` L${xx},${y(Number(data.points[index-1][series[key].margin]))} L${xx},${yy}`; else output+=` L${xx},${yy}`;}); return output; };
        const nodes={},markers={};
        keys.forEach((key,index)=>{nodes[key]=add("path",{d:path(key,!isEquity),fill:"none",stroke:series[key].color,"stroke-width":key==="live"?2.5:2.2,"stroke-dasharray":key==="rebased"?"6 4":""},chart); markers[key]=add("circle",{r:4,fill:series[key].color,display:"none","data-chart-hover-marker":key},chart);});
        const guide=add("line",{y1:top,y2:y1,class:"hover-guide",display:"none","data-chart-hover-guide":"true"},chart);
        const overlay=add("rect",{x:x0,y:top,width:x1-x0,height:plotH,fill:"transparent","data-chart-hit":"true","data-chart-hover-overlay":"cross-series"},chart);
        const getValue=(key,timestamp)=>{const rowKey=series[key][isEquity?"equity":"margin"]; if (!isEquity) {let low=0,high=dataTimes.length; while(low<high){const mid=Math.floor((low+high)/2); if(dataTimes[mid]<=timestamp) low=mid+1; else high=mid;} return Number(data.points[Math.max(0,low-1)][rowKey]);} if(timestamp<=dataTimes[0]) return Number(data.points[0][rowKey]); if(timestamp>=dataTimes[dataTimes.length-1]) return Number(data.points[dataTimes.length-1][rowKey]); let low=0,high=dataTimes.length; while(low<high){const mid=Math.floor((low+high)/2); if(dataTimes[mid]<=timestamp) low=mid+1; else high=mid;} const rightIndex=low,leftIndex=rightIndex-1,ratio=(timestamp-dataTimes[leftIndex])/Math.max(1,dataTimes[rightIndex]-dataTimes[leftIndex]); return Number(data.points[leftIndex][rowKey])+(Number(data.points[rightIndex][rowKey])-Number(data.points[leftIndex][rowKey]))*ratio;};
        const update=(event)=>{const bounds=overlay.getBoundingClientRect(),ratio=Math.max(0,Math.min(1,(event.clientX-bounds.left)/Math.max(1,bounds.width))),timestamp=firstTime+ratio*(lastTime-firstTime),xx=x(timestamp),rows=[]; guide.setAttribute("x1",xx);guide.setAttribute("x2",xx);guide.style.display="block";keys.forEach(key=>{const value=getValue(key,timestamp);markers[key].setAttribute("cx",xx);markers[key].setAttribute("cy",y(value));markers[key].style.display=visible[key]?"block":"none";if(visible[key]) rows.push(`<div><span style="color:${series[key].color}">●</span> ${series[key].label}：${value.toFixed(2)}U</div>`);});tooltip.innerHTML=`<strong>${new Date(timestamp).toISOString().replace("T"," ").replace(".000Z"," UTC")}</strong>${rows.join("")}`;const box=root.getBoundingClientRect();tooltip.style.display="block";tooltip.style.left=`${event.clientX-box.left+10}px`;tooltip.style.top=`${event.clientY-box.top+10}px`;};
        overlay.addEventListener("pointermove",update); overlay.addEventListener("pointerdown",event=>{pinned=true;update(event);}); overlay.addEventListener("pointerleave",()=>{if(!pinned){tooltip.style.display="none";guide.style.display="none";Object.values(markers).forEach(node=>node.style.display="none");}}); return {overlay,guide,markers};
      };
      let equityPlot, marginPlot;
      const redraw=()=>{pinned=false;tooltip.style.display="none";equityPlot=draw(equitySvg,equityChart,430,["live","replay","rebased"],equityVisible,true);marginPlot=draw(marginSvg,marginChart,350,["live","replay"],marginVisible,false);};
      redraw(); if (typeof ResizeObserver !== "undefined") new ResizeObserver(redraw).observe(root);
      root.addEventListener("pointerdown",event=>{if(!event.target.closest('[data-chart-hit="true"]')){pinned=false;tooltip.style.display="none";}});
      root.querySelectorAll(".series-toggle").forEach(button=>button.addEventListener("click",()=>{const name=button.dataset.series,chartName=button.dataset.chart,visible=chartName==="equity"?equityVisible:marginVisible;visible[name]=!visible[name];button.setAttribute("aria-pressed",String(visible[name]));redraw();}));
    })();
  </script>
</div>
'''
    return template.replace("__DATA__", payload)


def write_outputs(report: dict[str, Any], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    points = report["chart"]["points"]
    equity_fields = [
        "timestamp",
        "live_equity_usdt",
        "replay_equity_usdt",
        "replay_rebased_equity_usdt",
    ]
    margin_fields = ["timestamp", "live_margin_usdt", "replay_margin_usdt"]
    for filename, fields in (
        ("live_baseline_replay_equity_curve.csv", equity_fields),
        ("live_baseline_replay_margin_curve.csv", margin_fields),
    ):
        with (output_dir / filename).open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows({field: row[field] for field in fields} for row in points)
    (output_dir / "live_baseline_replay_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    live = report["live"]
    replay = report["replay"]
    diagnosis = report["diagnosis"]
    markdown = f'''# 实盘参数原样回放对比

## 结论

- 实盘权益：{live["start_equity_usdt"]:.2f}U → {live["end_equity_usdt"]:.2f}U，变化 {live["full_change_usdt"]:+.2f}U
- 原样回放：{replay["start_equity_usdt"]:.2f}U → {replay["end_equity_usdt"]:.2f}U，模拟 PnL {replay["full_pnl_usdt"]:+.2f}U
- 表面差额：实盘比回放多 {diagnosis["live_minus_replay_full_usdt"]:.2f}U

## 差额定位

- Top10 代理生效时间：`{report["window"]["proxy_start_utc"]}`
- 生效前：实盘权益变化 {diagnosis["live_change_before_proxy_usdt"]:+.2f}U；回放变化 {diagnosis["replay_change_before_proxy_usdt"]:+.2f}U（回放在该段按设计没有入场）
- 生效后：实盘变化 {diagnosis["live_change_after_proxy_usdt"]:+.2f}U；回放变化 {diagnosis["replay_change_after_proxy_usdt"]:+.2f}U；回放反而高 {diagnosis["fair_window_replay_minus_live_usdt"]:+.2f}U
- 因此主要问题是比较窗口包含了 Top10 代理不可用的前段部分日，不是把回放 PnL 加错；该段真实实盘已经上涨 {diagnosis["live_change_before_proxy_usdt"]:.2f}U，而回放只能从 `{report["window"]["proxy_start_utc"]}` 后开始。

## 原样参数

- `{json.dumps(replay["config"], ensure_ascii=False)}`
- 其余设置：Top10、只做多、100U名义仓位、5倍杠杆、限价 TTL 900 秒、15分钟K线退出、B8 恢复、双边费率 0.05%

## 保证金

- 实盘估算峰值：{live["margin"]["peak_usdt"]:.2f}U；回放峰值：{replay["margin"]["peak_usdt"]:.2f}U
'''
    (output_dir / "live_baseline_replay_report.md").write_text(markdown, encoding="utf-8")
    (output_dir / "live-baseline-replay-equity-margin.html").write_text(
        build_html(report),
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--optimization-report", type=Path, required=True)
    parser.add_argument("--equity-series", type=Path, required=True)
    parser.add_argument("--baseline-events", type=Path, required=True)
    parser.add_argument("--balance-snapshots", type=Path, required=True)
    parser.add_argument("--fill-events", type=Path, required=True)
    parser.add_argument("--exchange-orders", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    report = build_report(args)
    write_outputs(report, args.output_dir)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
