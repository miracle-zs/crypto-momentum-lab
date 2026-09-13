#!/usr/bin/env python3
"""Summarise the real live-primary equity baseline and build its visual.

The account balance snapshots are the source of truth for the baseline.  The
fill and order tables are used only for audit context and are intentionally not
stitched into a synthetic per-trade equity curve.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


def parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def read_csv(path: Path) -> list[dict[str, str]]:
    opener = gzip.open if path.suffix == ".gz" else Path.open
    with opener(path, "rt", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def number(value: str | None, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        parsed = float(value)
    except ValueError:
        return default
    return parsed if math.isfinite(parsed) else default


def boolean(value: str | None) -> bool:
    return str(value).strip().lower() in {"1", "true", "t", "yes"}


def fmt_time(value: datetime | None) -> str | None:
    return value.isoformat(timespec="seconds").replace("+00:00", "Z") if value else None


def max_drawdown(values: Iterable[float]) -> tuple[float, float, int, int]:
    peak = None
    peak_index = 0
    best_drawdown = 0.0
    best_peak_index = 0
    best_trough_index = 0
    for index, value in enumerate(values):
        if peak is None or value > peak:
            peak = value
            peak_index = index
        drawdown = peak - value
        if drawdown > best_drawdown:
            best_drawdown = drawdown
            best_peak_index = peak_index
            best_trough_index = index
    peak_value = 0.0
    return best_drawdown, peak_value, best_peak_index, best_trough_index


def sample_rows(rows: list[dict[str, Any]], limit: int = 900) -> list[dict[str, Any]]:
    if len(rows) <= limit:
        return rows
    step = (len(rows) - 1) / (limit - 1)
    indices = {round(index * step) for index in range(limit)}
    return [rows[index] for index in sorted(indices)]


def drawdown_stats(values: list[float]) -> tuple[float, float, int, int]:
    peak_value = None
    peak_index = 0
    best_drawdown = 0.0
    best_drawdown_pct = 0.0
    best_peak_index = 0
    best_trough_index = 0
    for index, value in enumerate(values):
        if peak_value is None or value > peak_value:
            peak_value = value
            peak_index = index
        drawdown = max(0.0, peak_value - value)
        drawdown_pct = drawdown / peak_value * 100.0 if peak_value else 0.0
        if drawdown > best_drawdown:
            best_drawdown = drawdown
            best_drawdown_pct = drawdown_pct
            best_peak_index = peak_index
            best_trough_index = index
    return best_drawdown, best_drawdown_pct, best_peak_index, best_trough_index


def percent(value: float | None) -> str:
    return "—" if value is None else f"{value:.2f}%"


def money(value: float | None) -> str:
    return "—" if value is None else f"{value:.2f} USDT"


def svg_visual(data: dict[str, Any]) -> str:
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    return f'''<div id="live-baseline-equity" aria-label="实盘账户权益基准曲线">
  <style>
    #live-baseline-equity {{
      --fg: light-dark(#17202a, #edf2f7);
      --muted: light-dark(#64748b, #aab6c3);
      --border: light-dark(#cbd5e1, #465568);
      --grid: light-dark(#e2e8f0, #334155);
      --blue: light-dark(#1769aa, #63b3ed);
      --green: light-dark(#2f855a, #68d391);
      --orange: light-dark(#c05621, #f6ad55);
      --red: light-dark(#b5482f, #f28f79);
      --card: light-dark(#f8fafc, #1c2733);
      color: var(--fg);
      display: block;
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      font-size: 12px;
      line-height: 1.4;
      width: 100%;
    }}
    #live-baseline-equity .title {{ font-size: 17px; font-weight: 700; margin: 0 0 3px; }}
    #live-baseline-equity .subtitle {{ color: var(--muted); margin: 0 0 12px; }}
    #live-baseline-equity .cards {{ display: grid; grid-template-columns: repeat(6, minmax(110px, 1fr)); gap: 7px; margin-bottom: 12px; }}
    #live-baseline-equity .card {{ background: var(--card); border: 1px solid var(--border); border-radius: 7px; padding: 8px 9px; }}
    #live-baseline-equity .label {{ color: var(--muted); font-size: 11px; }}
    #live-baseline-equity .value {{ font-size: 15px; font-weight: 700; margin-top: 2px; }}
    #live-baseline-equity .negative {{ color: var(--red); }}
    #live-baseline-equity svg {{ display: block; height: auto; overflow: visible; width: 100%; }}
    #live-baseline-equity text {{ fill: var(--fg); font-size: 11px; }}
    #live-baseline-equity .muted {{ fill: var(--muted); }}
    #live-baseline-equity .frame {{ fill: none; stroke: var(--border); stroke-width: 1; }}
    #live-baseline-equity .grid-line {{ stroke: var(--grid); stroke-dasharray: 2 3; opacity: .75; }}
    #live-baseline-equity .tooltip {{ background: light-dark(#fff, #1c2733); border: 1px solid var(--border); border-radius: 5px; color: var(--fg); display: none; max-width: 260px; padding: 6px 8px; pointer-events: none; position: absolute; z-index: 3; }}
    #live-baseline-equity .legend {{ color: var(--muted); display: flex; gap: 14px; margin: 5px 0 8px; }}
    #live-baseline-equity .swatch {{ display: inline-block; height: 3px; margin: 0 5px 3px 0; width: 16px; }}
    #live-baseline-equity .note {{ color: var(--muted); margin: 8px 0 0; }}
    @media (max-width: 660px) {{ #live-baseline-equity .cards {{ grid-template-columns: repeat(2, minmax(130px, 1fr)); }} }}
  </style>
  <div class="title">实盘账户权益基准：live-b1-long-100u-5x-v1</div>
  <p class="subtitle">数据源：live / primary 的 USDT 余额快照；权益 = wallet_balance + unrealized_pnl。时间为 UTC。</p>
  <div class="cards">
    <div class="card"><div class="label">起始权益</div><div class="value">{money(data["start_equity"])}</div></div>
    <div class="card"><div class="label">最新权益</div><div class="value">{money(data["end_equity"])}</div></div>
    <div class="card"><div class="label">原始净变化</div><div class="value {"negative" if data["equity_change"] < 0 else ""}">{money(data["equity_change"])}</div></div>
    <div class="card"><div class="label">推断资金流入</div><div class="value">{money(data["external_cashflow"])}</div></div>
    <div class="card"><div class="label">资金调整净变化</div><div class="value {"negative" if data["strategy_change"] < 0 else ""}">{money(data["strategy_change"])}</div></div>
    <div class="card"><div class="label">最大回撤</div><div class="value negative">{money(data["max_drawdown_usdt"])} / {percent(data["max_drawdown_pct"])}</div></div>
    <div class="card"><div class="label">观察时长</div><div class="value">{data["duration_text"]}</div></div>
  </div>
  <div class="legend"><span><span class="swatch" style="background:var(--blue)"></span>账户权益</span><span><span class="swatch" style="background:var(--orange)"></span>资金调整权益</span><span><span class="swatch" style="background:var(--green)"></span>钱包余额</span><span><span class="swatch" style="background:var(--red)"></span>回撤</span></div>
  <svg id="live-baseline-equity-svg" viewBox="0 0 960 590" role="img" aria-label="实盘账户权益和回撤曲线"><g id="live-baseline-equity-chart"></g></svg>
  <div class="tooltip" id="live-baseline-equity-tooltip" role="tooltip"></div>
  <p class="note">这条曲线是实际账户权益，不是把每个信号都按固定 100U 叠加的事件研究曲线；因此它反映真实资金占用、成交、退出和未实现盈亏。</p>
  <script>
    (() => {{
      const root = document.getElementById("live-baseline-equity");
      const svg = document.getElementById("live-baseline-equity-svg");
      const chart = document.getElementById("live-baseline-equity-chart");
      const tooltip = document.getElementById("live-baseline-equity-tooltip");
      const data = {payload};
      const NS = "http://www.w3.org/2000/svg";
      const W = 960, H = 590, left = 66, right = 20, top = 28, bottom = 48;
      const upperH = 338, lowerTop = top + upperH + 62, lowerH = 105;
      const x0 = left, x1 = W - right;
      const extent = (items, key) => [Math.min(...items.map(row => Number(row[key]))), Math.max(...items.map(row => Number(row[key])))];
      const pad = (domain, minimum) => {{ const span = Math.max(domain[1] - domain[0], minimum); const p = span * .10; return [domain[0] - p, domain[1] + p]; }};
      const scale = (domain, range) => value => range[0] + (Number(value) - domain[0]) / (domain[1] - domain[0] || 1) * (range[1] - range[0]);
      const add = (name, attrs, parent = chart) => {{ const node = document.createElementNS(NS, name); Object.entries(attrs || {{}}).forEach(([key, value]) => node.setAttribute(key, String(value))); parent.appendChild(node); return node; }};
      const addText = (parent, x, y, value, attrs = {{}}) => {{ const node = add("text", {{x, y, ...attrs}}, parent); node.appendChild(document.createTextNode(value)); return node; }};
      const fmt = (value, digits = 2) => Number(value).toFixed(digits);
      const ticks = (domain, count = 4) => Array.from({{length: count}}, (_, i) => domain[0] + (domain[1] - domain[0]) * i / (count - 1));
      const timeLabel = value => value.replace("T", " ").replace("Z", " UTC");
      const upper = data.curve;
      const equityValues = upper.flatMap(row => [Number(row.equity), Number(row.adjusted_equity)]);
      const equityDomain = pad([Math.min(...equityValues), Math.max(...equityValues)], 1);
      const cashDomain = pad(extent(upper, "wallet"), 1);
      const y = scale(equityDomain, [top + upperH, top]);
      const yCash = scale(cashDomain, [top + upperH, top]);
      const x = index => x0 + index / Math.max(1, upper.length - 1) * (x1 - x0);
      const ydd = scale([0, Math.max(1, data.max_drawdown_pct)], [lowerTop + lowerH, lowerTop]);
      const line = (key, fn) => upper.map((row, i) => `${{i ? "L" : "M"}}${{x(i)}},${{fn(row[key])}}`).join(" ");
      add("rect", {{x:x0, y:top, width:x1-x0, height:upperH, class:"frame"}});
      add("rect", {{x:x0, y:lowerTop, width:x1-x0, height:lowerH, class:"frame"}});
      ticks(equityDomain).forEach(value => {{ const yy = y(value); add("line", {{x1:x0, x2:x1, y1:yy, y2:yy, class:"grid-line"}}); addText(chart, x0-8, yy+4, fmt(value, 0), {{"text-anchor":"end"}}); }});
      ticks([0, Math.max(1, data.max_drawdown_pct)]).forEach(value => {{ const yy = ydd(value); add("line", {{x1:x0, x2:x1, y1:yy, y2:yy, class:"grid-line"}}); addText(chart, x0-8, yy+4, fmt(value, 1)+"%", {{"text-anchor":"end"}}); }});
      const xTick = [0, Math.floor((upper.length-1)/2), upper.length-1];
      xTick.forEach(index => addText(chart, x(index), lowerTop+lowerH+20, timeLabel(upper[index].timestamp), {{"text-anchor": index === 0 ? "start" : index === upper.length-1 ? "end" : "middle", class:"muted"}}));
      addText(chart, x0 + 3, top+15, "USDT", {{class:"muted"}});
      addText(chart, x0 + 3, lowerTop+15, "回撤", {{class:"muted"}});
      add("path", {{d: line("wallet", yCash), fill:"none", stroke:"var(--green)", "stroke-width":1.5, "stroke-dasharray":"4 3"}});
      add("path", {{d: line("equity", y), fill:"none", stroke:"var(--blue)", "stroke-width":2.4}});
      add("path", {{d: line("adjusted_equity", y), fill:"none", stroke:"var(--orange)", "stroke-width":2}});
      const ddPath = upper.map((row, i) => `${{i ? "L" : "M"}}${{x(i)}},${{ydd(row.drawdown_pct)}}`).join(" ");
      add("path", {{d: `${{ddPath}} L${{x(upper.length-1)}},${{ydd(0)}} L${{x(0)}},${{ydd(0)}} Z`, fill:"var(--red)", opacity:".24"}});
      add("path", {{d: ddPath, fill:"none", stroke:"var(--red)", "stroke-width":1.8}});
      const overlay = add("rect", {{x:x0, y:top, width:x1-x0, height:upperH, fill:"transparent"}});
      overlay.addEventListener("pointermove", event => {{ const bounds = overlay.getBoundingClientRect(); const index = Math.max(0, Math.min(upper.length-1, Math.round((event.clientX-bounds.left)/bounds.width*(upper.length-1)))); const row = upper[index]; tooltip.innerHTML = `<strong>${{timeLabel(row.timestamp)}}</strong><br>账户权益：${{fmt(row.equity, 2)}} USDT<br>资金调整权益：${{fmt(row.adjusted_equity, 2)}} USDT<br>钱包：${{fmt(row.wallet, 2)}} USDT<br>未实现盈亏：${{fmt(row.unrealized, 2)}} USDT<br>回撤：${{fmt(row.drawdown_pct, 2)}}%`; const rootBox = root.getBoundingClientRect(); tooltip.style.display = "block"; tooltip.style.left = `${{event.clientX-rootBox.left+10}}px`; tooltip.style.top = `${{event.clientY-rootBox.top+10}}px`; }});
      overlay.addEventListener("pointerleave", () => tooltip.style.display = "none");
    }})();
  </script>
</div>
'''


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    balance_rows = read_csv(args.input_dir / "account_balance_snapshots.csv.gz")
    balance_rows.sort(key=lambda row: parse_time(row["observed_at"]))
    if not balance_rows:
        raise SystemExit("no USDT balance snapshots found")

    curve: list[dict[str, Any]] = []
    peak = None
    peak_index = 0
    max_dd = 0.0
    max_dd_pct = 0.0
    max_dd_peak_index = 0
    max_dd_trough_index = 0
    for index, row in enumerate(balance_rows):
        wallet = number(row.get("wallet_balance"))
        unrealized = number(row.get("unrealized_pnl"))
        equity = wallet + unrealized
        if peak is None or equity > peak:
            peak = equity
            peak_index = index
        drawdown = max(0.0, peak - equity)
        drawdown_pct = drawdown / peak * 100.0 if peak else 0.0
        if drawdown > max_dd:
            max_dd = drawdown
            max_dd_pct = drawdown_pct
            max_dd_peak_index = peak_index
            max_dd_trough_index = index
        curve.append(
            {
                "timestamp": fmt_time(parse_time(row["observed_at"])),
                "equity": equity,
                "wallet": wallet,
                "unrealized": unrealized,
                "available": number(row.get("available_balance")),
                "drawdown_usdt": drawdown,
                "drawdown_pct": drawdown_pct,
            }
        )

    start = parse_time(balance_rows[0]["observed_at"])
    end = parse_time(balance_rows[-1]["observed_at"])
    duration_seconds = max(0.0, (end - start).total_seconds())
    duration_hours = duration_seconds / 3600.0
    duration_text = f"{duration_hours:.1f} 小时"
    start_equity = curve[0]["equity"]

    # No transfer ledger is present in this export.  A large discontinuity in
    # wallet_balance is therefore reported as an inferred cash-flow marker,
    # rather than being mistaken for strategy PnL.  The current data has one
    # unambiguous +200 USDT jump.
    cashflow_threshold = max(100.0, start_equity * 0.5)
    cashflow_events: list[dict[str, Any]] = []
    previous_wallet = curve[0]["wallet"]
    for index, item in enumerate(curve[1:], start=1):
        wallet_delta = item["wallet"] - previous_wallet
        if abs(wallet_delta) >= cashflow_threshold:
            cashflow_events.append(
                {
                    "timestamp": item["timestamp"],
                    "wallet_delta_usdt": wallet_delta,
                    "inference": "large wallet-balance discontinuity; transfer ledger unavailable",
                    "curve_index": index,
                }
            )
        previous_wallet = item["wallet"]
    event_by_index = {
        int(event["curve_index"]): float(event["wallet_delta_usdt"])
        for event in cashflow_events
    }
    cumulative_cashflow = 0.0
    for index, item in enumerate(curve):
        cashflow = event_by_index.get(index, 0.0)
        cumulative_cashflow += cashflow
        item["cashflow_usdt"] = cashflow
        item["cumulative_cashflow_usdt"] = cumulative_cashflow
        item["adjusted_equity"] = item["equity"] - cumulative_cashflow
        item["adjusted_pnl_usdt"] = item["adjusted_equity"] - start_equity
    external_cashflow = sum(event["wallet_delta_usdt"] for event in cashflow_events)
    contributed_capital = start_equity + external_cashflow
    end_equity = curve[-1]["equity"]
    equity_change = end_equity - start_equity
    equity_change_pct = equity_change / start_equity * 100.0 if start_equity else None
    strategy_change = curve[-1]["adjusted_equity"] - start_equity
    strategy_return_pct = strategy_change / contributed_capital * 100.0 if contributed_capital else None
    adjusted_dd, adjusted_dd_pct, adjusted_peak_index, adjusted_trough_index = drawdown_stats(
        [item["adjusted_equity"] for item in curve]
    )

    orders = read_csv(args.input_dir / "exchange_orders.csv.gz")
    orders_by_exchange_id = {
        row["exchange_order_id"]: row
        for row in orders
        if row.get("exchange_order_id")
    }
    fills = read_csv(args.input_dir / "account_fill_events.csv.gz")
    run_fills = [row for row in fills if row.get("order_id") in orders_by_exchange_id]
    fee_by_asset: defaultdict[str, float] = defaultdict(float)
    realized_pnl = 0.0
    for row in run_fills:
        realized_pnl += number(row.get("realized_pnl"))
        fee_by_asset[row.get("fee_asset") or "UNKNOWN"] += number(row.get("fee"))
    order_state_counts = Counter(row.get("state") or "UNKNOWN" for row in orders)
    order_side_counts = Counter(
        (row.get("side") or "UNKNOWN", "reduce" if boolean(row.get("reduce_only")) else "entry")
        for row in orders
    )
    entry_order_ids = {
        row.get("exchange_order_id")
        for row in orders
        if not boolean(row.get("reduce_only"))
    }
    exit_order_ids = {
        row.get("exchange_order_id")
        for row in orders
        if boolean(row.get("reduce_only"))
    }
    fill_class_counts = Counter(
        "entry" if row.get("order_id") in entry_order_ids else "exit"
        for row in run_fills
    )

    report: dict[str, Any] = {
        "benchmark": {
            "run_id": args.run_id,
            "environment": "live",
            "account_label": "primary",
            "strategy": "orderflow_impulse",
            "entry": "positive gainer Top10, long-only, EMA5/EMA10 filters disabled",
            "sizing": "100 USDT desired notional per entry, 5x leverage",
            "exit": "candle_15m, grace B8; live command also records TP 2% and SL 1%",
            "equity_definition": "USDT wallet_balance + unrealized_pnl from account_balance_snapshots",
            "capital_flow_handling": "large wallet_balance discontinuities are marked as inferred cash flows and removed from the adjusted strategy curve",
        },
        "period": {
            "start_utc": fmt_time(start),
            "end_utc": fmt_time(end),
            "duration_hours": duration_hours,
            "duration_text": duration_text,
            "balance_snapshot_count": len(curve),
        },
        "equity": {
            "start_usdt": start_equity,
            "end_usdt": end_equity,
            "change_usdt": equity_change,
            "change_pct": equity_change_pct,
            "min_usdt": min(item["equity"] for item in curve),
            "max_usdt": max(item["equity"] for item in curve),
            "max_drawdown_usdt": max_dd,
            "max_drawdown_pct": max_dd_pct,
            "max_drawdown_peak_at_utc": curve[max_dd_peak_index]["timestamp"],
            "max_drawdown_trough_at_utc": curve[max_dd_trough_index]["timestamp"],
            "inferred_cashflow_usdt": external_cashflow,
            "contributed_capital_usdt": contributed_capital,
            "capital_adjusted_end_usdt": curve[-1]["adjusted_equity"],
            "capital_adjusted_change_usdt": strategy_change,
            "capital_adjusted_return_on_contributed_capital_pct": strategy_return_pct,
            "capital_adjusted_max_drawdown_usdt": adjusted_dd,
            "capital_adjusted_max_drawdown_pct": adjusted_dd_pct,
            "capital_adjusted_max_drawdown_peak_at_utc": curve[adjusted_peak_index]["timestamp"],
            "capital_adjusted_max_drawdown_trough_at_utc": curve[adjusted_trough_index]["timestamp"],
        },
        "capital_flow": {
            "detection_threshold_usdt": cashflow_threshold,
            "events": cashflow_events,
            "interpretation": "inferred from balance discontinuity; verify against exchange transfer history if available",
        },
        "audit": {
            "run_orders": len(orders),
            "order_state_counts": dict(sorted(order_state_counts.items())),
            "order_side_type_counts": {
                f"{side}_{kind}": count
                for (side, kind), count in sorted(order_side_counts.items())
            },
            "account_fills_loaded": len(fills),
            "run_fills_joined_by_exchange_order_id": len(run_fills),
            "fill_class_counts": dict(sorted(fill_class_counts.items())),
            "exchange_realized_pnl_sum_usdt": realized_pnl,
            "fee_sum_by_asset": dict(sorted(fee_by_asset.items())),
            "fee_sum_note": "fee amounts are kept by fee_asset; they are not converted to USDT here",
        },
        "curve": sample_rows(curve),
    }
    return report


def write_outputs(report: dict[str, Any], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    full_curve_path = output_dir / "live_account_equity.csv"
    with full_curve_path.open("w", encoding="utf-8", newline="") as handle:
        fields = [
            "timestamp",
            "equity",
            "wallet",
            "unrealized",
            "available",
            "cashflow_usdt",
            "cumulative_cashflow_usdt",
            "adjusted_equity",
            "adjusted_pnl_usdt",
            "drawdown_usdt",
            "drawdown_pct",
        ]
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(report["curve"])
    (output_dir / "live_baseline_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    equity = report["equity"]
    audit = report["audit"]
    markdown = f'''# 实盘权益基准

## 口径

- 运行：`{report["benchmark"]["run_id"]}` / `live` / `primary`
- 策略：{report["benchmark"]["entry"]}
- 仓位：{report["benchmark"]["sizing"]}
- 退出：{report["benchmark"]["exit"]}
- 权益：{report["benchmark"]["equity_definition"]}
- 观察区间：`{report["period"]["start_utc"]}` 至 `{report["period"]["end_utc"]}`，共 {report["period"]["duration_text"]}

## 结果

| 指标 | 数值 |
| --- | ---: |
| 起始权益 | {money(equity["start_usdt"])} |
| 最新权益 | {money(equity["end_usdt"])} |
| 账户原始净变化 | {money(equity["change_usdt"])}（{percent(equity["change_pct"])}） |
| 推断资金流入/流出 | {money(equity["inferred_cashflow_usdt"])} |
| 资金调整后策略净变化 | {money(equity["capital_adjusted_change_usdt"])} |
| 按投入资金计算回报 | {percent(equity["capital_adjusted_return_on_contributed_capital_pct"])} |
| 最大回撤 | {money(equity["max_drawdown_usdt"])}（{percent(equity["max_drawdown_pct"])}） |
| 最大回撤区间 | `{equity["max_drawdown_peak_at_utc"]}` → `{equity["max_drawdown_trough_at_utc"]}` |
| 余额快照 | {report["period"]["balance_snapshot_count"]:,} |

## 订单成交审计

- 实盘运行订单：{audit["run_orders"]:,}
- 账户成交：{audit["account_fills_loaded"]:,} 条；按 exchange order id 关联到本运行：{audit["run_fills_joined_by_exchange_order_id"]:,} 条
- 交易所记录 realized PnL 合计：{money(audit["exchange_realized_pnl_sum_usdt"])}
- 手续费按币种保留：`{json.dumps(audit["fee_sum_by_asset"], ensure_ascii=False)}`

本次在钱包余额中发现一笔 `{money(equity["inferred_cashflow_usdt"])}` 的大额跳变（阈值 {money(report["capital_flow"]["detection_threshold_usdt"])}），按资金流入标记；数据库导出中没有独立 transfer ledger，因此这是基于余额断点的推断，正式财务核算应再用交易所资金流水核验。

手续费可能以非 USDT 资产记录，因此本报告不把不同币种的 fee 直接相加作为 USDT；账户权益曲线本身仍以余额快照为准。交易所 realized PnL 与余额变化的差异还可能包含资金费等非成交账项。
'''
    (output_dir / "live_baseline_report.md").write_text(markdown, encoding="utf-8")
    visual_data = {
        "curve": report["curve"],
        "max_drawdown_pct": equity["max_drawdown_pct"],
        "start_equity": equity["start_usdt"],
        "end_equity": equity["end_usdt"],
        "equity_change": equity["change_usdt"],
        "external_cashflow": equity["inferred_cashflow_usdt"],
        "strategy_change": equity["capital_adjusted_change_usdt"],
        "duration_text": report["period"]["duration_text"],
        "max_drawdown_usdt": equity["max_drawdown_usdt"],
    }
    (output_dir / "live-baseline-equity.html").write_text(
        svg_visual(visual_data), encoding="utf-8"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run-id", default="live-b1-long-100u-5x-v1")
    args = parser.parse_args()
    write_outputs(build_report(args), args.output_dir)
    report = json.loads((args.output_dir / "live_baseline_report.json").read_text(encoding="utf-8"))
    print(json.dumps({"period": report["period"], "equity": report["equity"], "audit": report["audit"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
