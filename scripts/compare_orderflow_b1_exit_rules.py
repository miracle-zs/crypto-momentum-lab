"""Compare the long-only B1 break-even exit with a +0.88% recovery limit.

The comparison is deliberately paired.  Both variants use the exact same
virtual B1 entry cohort and the same archived replay inputs; only the grace
limit differs:

* original B1: the first adverse long candle is ``close < open`` and the
  recovery limit is the entry price;
* target B1: the same adverse candle, but the recovery limit is entry * 1.0088.

The script writes CSV/JSON data plus three native-SVG charts and a small HTML
report.  It has no browser/CDN dependency, so opening the report from a local
file cannot fail with ``d3 is not defined``.
"""

# The generated SVG/HTML templates intentionally keep selected lines together.
# ruff: noqa: E501

from __future__ import annotations

import argparse
import csv
import html
import json
import math
import statistics
from collections import defaultdict
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

ORIGINAL = "original"
TARGET = "target"
VARIANTS = (ORIGINAL, TARGET)
LABELS = {
    ORIGINAL: "原始 B1 · 反向阴线后回收至开仓价",
    TARGET: "B1 +0.88% · 反向阴线后回收至开仓价+0.88%",
}
COLORS = {ORIGINAL: "#8fa8c7", TARGET: "#55d6be"}
LOCAL_TZ = ZoneInfo("Asia/Shanghai")


def parse_dt(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def fmt_dt(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def num(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def decimal_key(value: Any) -> str:
    return str(Decimal(str(value or "0")).normalize())


def position_key(row: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(row.get("symbol", "")),
        parse_dt(str(row.get("opened_at"))).isoformat(),
        decimal_key(row.get("entry_price")),
        decimal_key(row.get("quantity")),
    )


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object in {path}")
    return value


def replay_status(value: str | None) -> str:
    return value or "unresolved"


def make_rows(
    *,
    original_path: Path,
    target_path: Path,
    positions_path: Path,
    history_path: Path,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    original = {row["position_id"]: row for row in read_csv(original_path)}
    target = {row["position_id"]: row for row in read_csv(target_path)}
    positions = {
        row["position_id"]: row for row in read_csv(positions_path)
    }
    history = load_json(history_path)
    actual_closed = history.get("closed_trades") or []
    actual_by_key = {position_key(row): row for row in actual_closed}
    rows: list[dict[str, Any]] = []

    for position_id, source in original.items():
        target_source = target.get(position_id)
        position = positions.get(position_id)
        if target_source is None or position is None:
            continue
        key = position_key(position)
        actual = actual_by_key.get(key)
        # B0 and B1 use the same signal/entry cohort, but their position IDs
        # are account-local.  Matching on the entry tuple avoids conflating
        # the accounts while still anchoring the analysis to virtual B1 data.
        if actual is None:
            continue
        entry_price = num(position.get("entry_price")) or 0.0
        quantity = num(position.get("quantity")) or 0.0
        notional = num(position.get("entry_notional"))
        if notional is None:
            notional = entry_price * quantity
        rows.append(
            {
                "position_id": actual.get("position_id") or position_id,
                "replay_position_id": position_id,
                "symbol": position.get("symbol", ""),
                "side": position.get("side", ""),
                "opened_at": parse_dt(position["opened_at"]),
                "entry_price": entry_price,
                "quantity": quantity,
                "entry_notional": notional,
                "actual_b1_closed_at": parse_dt(actual["closed_at"]),
                "actual_b1_pnl": num(actual.get("realized_pnl")),
                "actual_b1_reason": actual.get("close_reason") or "",
                "original_status": replay_status(source.get("b1_status")),
                "original_reverse_at": (
                    parse_dt(source["b1_reverse_candle_end"])
                    if source.get("b1_reverse_candle_end")
                    else None
                ),
                "original_closed_at": (
                    parse_dt(source["b1_closed_at"])
                    if source.get("b1_closed_at")
                    else None
                ),
                "original_exit_price": num(source.get("b1_exit_price")),
                "original_target_price": num(source.get("b1_target_price")),
                "original_pnl": num(source.get("b1_pnl")),
                "original_fill_kind": source.get("b1_fill_kind") or "",
                "target_status": replay_status(target_source.get("b1_status")),
                "target_reverse_at": (
                    parse_dt(target_source["b1_reverse_candle_end"])
                    if target_source.get("b1_reverse_candle_end")
                    else None
                ),
                "target_closed_at": (
                    parse_dt(target_source["b1_closed_at"])
                    if target_source.get("b1_closed_at")
                    else None
                ),
                "target_exit_price": num(target_source.get("b1_exit_price")),
                "target_target_price": num(target_source.get("b1_target_price")),
                "target_pnl": num(target_source.get("b1_pnl")),
                "target_fill_kind": target_source.get("b1_fill_kind") or "",
            }
        )
    rows.sort(key=lambda row: (row["opened_at"], row["position_id"]))
    meta = {
        "actual_b1_closed_rows": len(actual_closed),
        "matched_replay_rows": len(rows),
        "unmatched_actual_rows": len(actual_closed) - len(rows),
        "history_first_opened_at": (
            min(parse_dt(row["opened_at"]) for row in actual_closed).isoformat()
            if actual_closed
            else None
        ),
        "history_last_opened_at": (
            max(parse_dt(row["opened_at"]) for row in actual_closed).isoformat()
            if actual_closed
            else None
        ),
    }
    return rows, meta


def closed_rows(rows: list[dict[str, Any]], variant: str) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if row.get(f"{variant}_pnl") is not None
        and row.get(f"{variant}_closed_at") is not None
    ]


def profit_factor(values: list[float]) -> float | None:
    gains = sum(value for value in values if value > 0)
    losses = -sum(value for value in values if value < 0)
    if losses == 0:
        return None if gains == 0 else float("inf")
    return gains / losses


def metric(rows: list[dict[str, Any]], variant: str) -> dict[str, Any]:
    closed = sorted(closed_rows(rows, variant), key=lambda row: row[f"{variant}_closed_at"])
    values = [float(row[f"{variant}_pnl"]) for row in closed]
    wins = [value for value in values if value > 0]
    losses = [value for value in values if value < 0]
    cumulative = 0.0
    peak = 0.0
    drawdown = 0.0
    for value in values:
        cumulative += value
        peak = max(peak, cumulative)
        drawdown = min(drawdown, cumulative - peak)
    return {
        "trades": len(values),
        "net_pnl": sum(values),
        "profit_factor": profit_factor(values),
        "win_rate": len(wins) / len(values) if values else None,
        "expectancy": statistics.mean(values) if values else None,
        "median_pnl": statistics.median(values) if values else None,
        "average_win": statistics.mean(wins) if wins else None,
        "average_loss": statistics.mean(losses) if losses else None,
        "max_drawdown": drawdown,
        "first_close_at": (
            closed[0][f"{variant}_closed_at"].isoformat() if closed else None
        ),
        "last_close_at": (
            closed[-1][f"{variant}_closed_at"].isoformat() if closed else None
        ),
    }


def build_equity(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    events: dict[str, dict[datetime, float]] = {}
    for variant in VARIANTS:
        grouped: dict[datetime, float] = defaultdict(float)
        for row in closed_rows(rows, variant):
            grouped[row[f"{variant}_closed_at"]] += float(row[f"{variant}_pnl"])
        events[variant] = dict(grouped)
    timestamps = sorted(set().union(*(set(item) for item in events.values())))
    cumulative = dict.fromkeys(VARIANTS, 0.0)
    peaks = dict.fromkeys(VARIANTS, 0.0)
    output: list[dict[str, Any]] = []
    for timestamp in timestamps:
        point: dict[str, Any] = {"timestamp": timestamp.isoformat()}
        for variant in VARIANTS:
            step = events[variant].get(timestamp, 0.0)
            cumulative[variant] += step
            peaks[variant] = max(peaks[variant], cumulative[variant])
            point[f"{variant}_step_pnl"] = step
            point[f"{variant}_cumulative_pnl"] = cumulative[variant]
            point[f"{variant}_drawdown"] = cumulative[variant] - peaks[variant]
        output.append(point)
    return output


def build_capital(rows: list[dict[str, Any]], timestamps: list[datetime]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for timestamp in timestamps:
        point: dict[str, Any] = {"timestamp": timestamp.isoformat()}
        for variant in VARIANTS:
            point[f"{variant}_capital_used"] = sum(
                float(row["entry_notional"])
                for row in rows
                if row["opened_at"] <= timestamp
                and (
                    row.get(f"{variant}_closed_at") is None
                    or timestamp < row[f"{variant}_closed_at"]
                )
            )
        output.append(point)
    return output


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    field: (
                        fmt_dt(value)
                        if isinstance(value, datetime)
                        else value
                    )
                    for field, value in row.items()
                    if field in fields
                }
            )


def write_trade_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = [
        "position_id",
        "replay_position_id",
        "symbol",
        "side",
        "opened_at",
        "entry_price",
        "quantity",
        "entry_notional",
        "actual_b1_closed_at",
        "actual_b1_pnl",
        "actual_b1_reason",
        "original_status",
        "original_reverse_at",
        "original_closed_at",
        "original_exit_price",
        "original_target_price",
        "original_pnl",
        "original_fill_kind",
        "target_status",
        "target_reverse_at",
        "target_closed_at",
        "target_exit_price",
        "target_target_price",
        "target_pnl",
        "target_fill_kind",
        "target_minus_original",
    ]
    serialized: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        if item.get("original_pnl") is not None and item.get("target_pnl") is not None:
            item["target_minus_original"] = item["target_pnl"] - item["original_pnl"]
        else:
            item["target_minus_original"] = None
        serialized.append(item)
    write_csv(path, serialized, fields)


def xml(value: Any) -> str:
    return html.escape(str(value), quote=True)


def nice(value: float) -> str:
    if abs(value) >= 1000:
        return f"{value:,.0f}"
    if abs(value) >= 100:
        return f"{value:,.0f}"
    return f"{value:,.1f}"


def svg_chart(
    *,
    rows: list[dict[str, Any]],
    metric_name: str,
    title: str,
    subtitle: str,
    metrics: dict[str, Any],
) -> str:
    width, height = 1440, 620
    left, right, top, bottom = 86, 34, 94, 68
    suffix = {
        "pnl": "cumulative_pnl",
        "drawdown": "drawdown",
        "capital": "capital_used",
    }[metric_name]
    parsed = [(parse_dt(str(row["timestamp"])), row) for row in rows]
    if not parsed:
        raise ValueError(f"no rows for {metric_name}")
    values = [
        float(row[f"{variant}_{suffix}"])
        for _, row in parsed
        for variant in VARIANTS
        if row.get(f"{variant}_{suffix}") not in (None, "")
    ]
    lo, hi = min(values), max(values)
    if metric_name == "capital":
        lo = min(0.0, lo)
    if lo == hi:
        lo, hi = lo - 1.0, hi + 1.0
    elif metric_name != "capital":
        pad = (hi - lo) * 0.08
        lo, hi = lo - pad, hi + pad
    start, end = parsed[0][0].timestamp(), parsed[-1][0].timestamp()
    if end <= start:
        end = start + 1
    plot_width, plot_height = width - left - right, height - top - bottom

    def x(value: float) -> float:
        return left + (value - start) / (end - start) * plot_width

    def y(value: float) -> float:
        return top + (1 - (value - lo) / (hi - lo)) * plot_height

    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" role="img" aria-label="{xml(title)}">',
        '<rect width="100%" height="100%" fill="#0d1119"/>',
        '<style>text{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}.axis{fill:#94a3b8;font-size:14px}.grid{stroke:#2a3545;stroke-width:1;stroke-dasharray:3 5}.line{fill:none;stroke-width:2.8;stroke-linecap:round;stroke-linejoin:round}.frame{fill:none;stroke:#3a475b}.zero{stroke:#94a3b8;stroke-width:1;stroke-dasharray:5 5;opacity:.65}.legend{font-size:14px;fill:#e6edf7}</style>',
        f'<text x="{left}" y="40" fill="#e6edf7" font-size="24" font-weight="600">{xml(title)}</text>',
        f'<text x="{left}" y="66" fill="#94a3b8" font-size="13">{xml(subtitle)}</text>',
    ]
    for index in range(7):
        value = lo + (hi - lo) * index / 6
        yy = y(value)
        lines.append(f'<line class="grid" x1="{left}" x2="{width-right}" y1="{yy:.2f}" y2="{yy:.2f}"/>')
        lines.append(f'<text class="axis" x="{left-12}" y="{yy+5:.2f}" text-anchor="end">{xml(nice(value))}</text>')
    if lo < 0 < hi:
        yy = y(0)
        lines.append(f'<line class="zero" x1="{left}" x2="{width-right}" y1="{yy:.2f}" y2="{yy:.2f}"/>')
    for index in range(7):
        stamp = start + (end - start) * index / 6
        label = datetime.fromtimestamp(stamp, tz=UTC).astimezone(LOCAL_TZ).strftime("%m-%d %H:%M")
        lines.append(f'<text class="axis" x="{x(stamp):.2f}" y="{height-bottom+28}" text-anchor="middle">{label}</text>')
    for variant in VARIANTS:
        points = [
            (x(timestamp.timestamp()), y(float(row[f"{variant}_{suffix}"])))
            for timestamp, row in parsed
            if row.get(f"{variant}_{suffix}") not in (None, "")
        ]
        if points:
            path = "M " + " L ".join(f"{px:.2f},{py:.2f}" for px, py in points)
            lines.append(f'<path class="line" stroke="{COLORS[variant]}" d="{path}"/>')
    lines.append(f'<rect class="frame" x="{left}" y="{top}" width="{plot_width}" height="{plot_height}"/>')
    legend_x, legend_y = left, height - 18
    for variant in VARIANTS:
        lines.append(f'<line x1="{legend_x}" x2="{legend_x+24}" y1="{legend_y-5}" y2="{legend_y-5}" stroke="{COLORS[variant]}" stroke-width="3"/>')
        lines.append(f'<text class="legend" x="{legend_x+31}" y="{legend_y}">{xml(LABELS[variant])}</text>')
        legend_x += 310
    metric_line = " · ".join(f"{LABELS[v]} {metrics[v]['net_pnl']:+.1f}" for v in VARIANTS)
    lines.append(f'<text x="{width-right}" y="40" fill="#94a3b8" font-size="12" text-anchor="end">{xml(metric_line)}</text>')
    lines.append("</svg>")
    return "\n".join(lines)


def money(value: Any) -> str:
    if value is None:
        return "—"
    return f"{float(value):+.2f}"


def html_report(
    *,
    metrics: dict[str, Any],
    equity_svg: str,
    drawdown_svg: str,
    capital_svg: str,
) -> str:
    cards = []
    for variant in VARIANTS:
        item = metrics[variant]
        cards.append(
            f'<div class="card"><div class="name" style="color:{COLORS[variant]}">{xml(LABELS[variant])}</div>'
            f'<div class="value">{money(item["net_pnl"])} USDT</div>'
            f'<div class="meta">{item["trades"]} 笔 · 胜率 {item["win_rate"]:.1%} · 最大回撤 {money(item["max_drawdown"])} USDT</div></div>'
        )
    coverage = metrics["data"]
    subtitle = (
        f'只做多 · 同一批 B1 入场 · 共同成熟 {coverage["common_matured_rows"]}/{coverage["matched_replay_rows"]} 笔 · '
        f'手续费按每边 {metrics["fee_rate"]:.2%} · 本地行情快照 {coverage["replay_snapshot_at_utc"]}'
    )
    return f'''<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>B1 原始 vs +0.88% 退出对比</title>
<style>
:root{{--bg:#0d1119;--panel:#151c28;--line:#3a475b;--text:#e6edf7;--muted:#94a3b8}}
*{{box-sizing:border-box}}body{{margin:0;padding:20px;background:var(--bg);color:var(--text);font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}}main{{max-width:1500px;margin:auto}}h1{{margin:0 0 4px;font-size:24px}}.subtitle{{margin:0 0 14px;color:var(--muted)}}.note{{padding:10px 12px;border:1px solid #53657d;border-radius:9px;background:#121b29;color:#cbd5e1;margin:0 0 14px}}.cards{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin-bottom:14px}}.card{{padding:12px;border:1px solid var(--line);border-radius:10px;background:var(--panel)}}.name{{font-size:13px}}.value{{font-size:22px;margin-top:4px}}.meta{{color:var(--muted);font-size:12px}}.panel{{padding:12px;border:1px solid var(--line);border-radius:10px;background:var(--panel);margin-bottom:14px;overflow:auto}}h2{{margin:0 0 8px;font-size:16px}}svg{{display:block;width:100%;min-width:760px;height:auto}}table{{border-collapse:collapse;width:100%;font-size:12px}}th,td{{padding:7px 8px;border-bottom:1px solid #263244;text-align:right}}th:first-child,td:first-child{{text-align:left}}th{{color:var(--muted);font-weight:500}}.positive{{color:#55d6be}}.negative{{color:#ff7b72}}@media(max-width:700px){{body{{padding:10px}}.cards{{grid-template-columns:1fr}}}}
</style></head><body><main>
<h1>B1 原始退出 vs 价格上涨 0.88% 退出</h1><p class="subtitle">{xml(subtitle)}</p>
<div class="note"><b>口径说明：</b>多头的原始反向 K 线是 <code>close &lt; open</code>；<code>close &gt; open</code> 是顺向阳线，不是原始 B1 的反向判定。两条线只改变宽限后的回收目标：开仓价 vs 开仓价 + 0.88%。服务器 B1 历史有 {coverage["actual_b1_closed_rows"]} 笔，本地行情快照能配对 {coverage["matched_replay_rows"]} 笔，剩余 {coverage["unmatched_actual_rows"]} 笔未纳入本次共同回放。</div>
<div class="cards">{"".join(cards)}</div>
<section class="panel"><h2>累计净 PnL（USDT）</h2>{equity_svg}</section>
<section class="panel"><h2>序列回撤（USDT）</h2>{drawdown_svg}</section>
<section class="panel"><h2>资金占用（USDT）</h2>{capital_svg}</section>
<section class="panel"><h2>指标对比</h2><table><thead><tr><th>策略</th><th>笔数</th><th>净 PnL</th><th>PF</th><th>胜率</th><th>期望/笔</th><th>最大回撤</th></tr></thead><tbody>
<tr><td>{xml(LABELS[ORIGINAL])}</td><td>{metrics[ORIGINAL]["trades"]}</td><td>{money(metrics[ORIGINAL]["net_pnl"])}</td><td>{metrics[ORIGINAL]["profit_factor"]:.3f}</td><td>{metrics[ORIGINAL]["win_rate"]:.1%}</td><td>{money(metrics[ORIGINAL]["expectancy"])}</td><td>{money(metrics[ORIGINAL]["max_drawdown"])}</td></tr>
<tr><td>{xml(LABELS[TARGET])}</td><td>{metrics[TARGET]["trades"]}</td><td>{money(metrics[TARGET]["net_pnl"])}</td><td>{metrics[TARGET]["profit_factor"]:.3f}</td><td>{metrics[TARGET]["win_rate"]:.1%}</td><td>{money(metrics[TARGET]["expectancy"])}</td><td>{money(metrics[TARGET]["max_drawdown"])}</td></tr>
</tbody></table></section>
</main></body></html>'''


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-trades", type=Path, required=True)
    parser.add_argument("--target-trades", type=Path, required=True)
    parser.add_argument("--positions", type=Path, required=True)
    parser.add_argument("--b1-history", type=Path, required=True)
    parser.add_argument("--replay-snapshot", required=True)
    parser.add_argument("--fee-rate", type=float, default=0.0004)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    rows, coverage = make_rows(
        original_path=args.original_trades,
        target_path=args.target_trades,
        positions_path=args.positions,
        history_path=args.b1_history,
    )
    common = [
        row
        for row in rows
        if all(row.get(f"{variant}_pnl") is not None for variant in VARIANTS)
    ]
    equity = build_equity(common)
    event_times = {
        parse_dt(str(point["timestamp"])) for point in equity
    }
    for row in common:
        event_times.add(row["opened_at"])
        for variant in VARIANTS:
            event_times.add(row[f"{variant}_closed_at"])
    capital = build_capital(common, sorted(event_times))
    metrics = {
        "fee_rate": args.fee_rate,
        "favorable_exit_pct": 0.0088,
        "data": {
            **coverage,
            "common_matured_rows": len(common),
            "replay_snapshot_at_utc": args.replay_snapshot,
            "original_replay": str(args.original_trades),
            "target_replay": str(args.target_trades),
            "b1_history": str(args.b1_history),
            "positions": str(args.positions),
        },
        "rules": {
            "side": "long",
            "adverse_candle": "close < open",
            "original_recovery": "entry price (0.00%)",
            "target_recovery": "entry price + 0.88%",
            "fee_model": "0.04% entry + 0.04% exit",
        },
    }
    for variant in VARIANTS:
        metrics[variant] = metric(common, variant)
    delta = metrics[TARGET]["net_pnl"] - metrics[ORIGINAL]["net_pnl"]
    deltas = [
        float(row[TARGET + "_pnl"]) - float(row[ORIGINAL + "_pnl"])
        for row in common
    ]
    metrics["target_minus_original"] = {
        "net_pnl_delta": delta,
        "mean_delta": statistics.mean(deltas) if deltas else None,
        "median_delta": statistics.median(deltas) if deltas else None,
        "target_better_rate": sum(value > 0 for value in deltas) / len(deltas) if deltas else None,
        "target_worse_rate": sum(value < 0 for value in deltas) / len(deltas) if deltas else None,
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    write_trade_csv(args.output_dir / "trade_comparison.csv", rows)
    equity_fields = [
        "timestamp",
        *(f"{variant}_{suffix}" for variant in VARIANTS for suffix in ("step_pnl", "cumulative_pnl", "drawdown")),
    ]
    write_csv(args.output_dir / "equity_series.csv", equity, equity_fields)
    capital_fields = ["timestamp", *(f"{variant}_capital_used" for variant in VARIANTS)]
    write_csv(args.output_dir / "capital_series.csv", capital, capital_fields)
    metrics["data"]["trade_csv"] = str(args.output_dir / "trade_comparison.csv")
    (args.output_dir / "metrics.json").write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    subtitle = (
        f"共同成熟 {len(common)}/{len(rows)} 笔 · 快照 {args.replay_snapshot} · "
        f"手续费每边 {args.fee_rate:.2%}"
    )
    equity_svg = svg_chart(rows=equity, metric_name="pnl", title="累计净 PnL", subtitle=subtitle, metrics=metrics)
    drawdown_svg = svg_chart(rows=equity, metric_name="drawdown", title="序列回撤", subtitle=subtitle, metrics=metrics)
    capital_svg = svg_chart(rows=capital, metric_name="capital", title="资金占用", subtitle=subtitle, metrics=metrics)
    (args.output_dir / "cumulative-net-pnl.svg").write_text(equity_svg, encoding="utf-8")
    (args.output_dir / "sequence-drawdown.svg").write_text(drawdown_svg, encoding="utf-8")
    (args.output_dir / "capital-usage.svg").write_text(capital_svg, encoding="utf-8")
    (args.output_dir / "report.html").write_text(
        html_report(metrics=metrics, equity_svg=equity_svg, drawdown_svg=drawdown_svg, capital_svg=capital_svg),
        encoding="utf-8",
    )
    print(json.dumps({"output_dir": str(args.output_dir), "matched": len(rows), "common": len(common), "metrics": {variant: metrics[variant] for variant in VARIANTS}, "delta": metrics["target_minus_original"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
