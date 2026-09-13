"""Analyze the paired entry-candle boundary replay for B1 and live.

The input is the compact JSON emitted by ``replay_candle_entry_boundary.py``.
This script keeps the raw per-position comparison in CSV and creates a native
SVG/HTML report without any browser/CDN dependency.
"""

# The report embeds compact SVG/HTML templates; selected long lines are kept
# together to make the generated document easier to audit.
# ruff: noqa: E501

from __future__ import annotations

import argparse
import csv
import html
import json
import math
import statistics
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

VARIANTS = ("current", "skip")
VARIANT_LABELS = {
    "current": "现行：入场所在15m K线可触发",
    "skip": "假设：从下一根完整15m K线开始",
}
COLORS = {"current": "#8fa8c7", "skip": "#55d6be"}


def parse_dt(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def finite(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def json_number(value: Any) -> float | None:
    result = finite(value)
    return round(result, 10) if result is not None else None


def profit_factor(values: list[float]) -> float | None:
    gains = sum(value for value in values if value > 0)
    losses = -sum(value for value in values if value < 0)
    if losses == 0:
        return None if gains == 0 else float("inf")
    return gains / losses


def equity_stats(rows: list[dict[str, Any]], variant: str) -> dict[str, Any]:
    closed = [
        row
        for row in rows
        if finite(row.get(f"{variant}_pnl")) is not None
        and row.get(f"{variant}_closed_at")
    ]
    closed.sort(key=lambda row: parse_dt(row[f"{variant}_closed_at"]))
    values = [float(row[f"{variant}_pnl"]) for row in closed]
    wins = [value for value in values if value > 0]
    losses = [value for value in values if value < 0]
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in values:
        cumulative += value
        peak = max(peak, cumulative)
        max_drawdown = min(max_drawdown, cumulative - peak)
    holding_minutes = [
        (
            parse_dt(row[f"{variant}_closed_at"])
            - parse_dt(row["opened_at"])
        ).total_seconds()
        / 60
        for row in closed
    ]
    usage_events: list[tuple[datetime, float]] = []
    for row in closed:
        notional = finite(row.get("entry_notional")) or 0.0
        usage_events.append((parse_dt(row["opened_at"]), notional))
        usage_events.append((parse_dt(row[f"{variant}_closed_at"]), -notional))
    usage_events.sort(key=lambda item: (item[0], item[1] < 0))
    open_notional = 0.0
    peak_open_notional = 0.0
    area = 0.0
    last_at = usage_events[0][0] if usage_events else None
    for at, change in usage_events:
        if last_at is not None:
            area += open_notional * (at - last_at).total_seconds()
        open_notional += change
        peak_open_notional = max(peak_open_notional, open_notional)
        last_at = at
    span_seconds = (
        (usage_events[-1][0] - usage_events[0][0]).total_seconds()
        if len(usage_events) > 1
        else 0.0
    )
    return {
        "trades": len(values),
        "unresolved": len(rows) - len(values),
        "net_pnl": sum(values),
        "profit_factor": profit_factor(values),
        "win_rate": len(wins) / len(values) if values else None,
        "expectancy": statistics.mean(values) if values else None,
        "median_pnl": statistics.median(values) if values else None,
        "average_win": statistics.mean(wins) if wins else None,
        "average_loss": statistics.mean(losses) if losses else None,
        "max_drawdown": max_drawdown,
        "average_holding_minutes": (
            statistics.mean(holding_minutes) if holding_minutes else None
        ),
        "median_holding_minutes": (
            statistics.median(holding_minutes) if holding_minutes else None
        ),
        "peak_open_notional": peak_open_notional,
        "time_weighted_average_open_notional": (
            area / span_seconds if span_seconds > 0 else None
        ),
        "first_closed_at": closed[0][f"{variant}_closed_at"] if closed else None,
        "last_closed_at": closed[-1][f"{variant}_closed_at"] if closed else None,
        "fill_kinds": {
            str(kind): count
            for kind, count in sorted(
                Counter(
                    row.get(f"{variant}_fill_kind") or "unresolved"
                    for row in rows
                ).items()
            )
        },
    }


def timeline(rows: list[dict[str, Any]], variant: str) -> list[dict[str, Any]]:
    events: dict[datetime, float] = defaultdict(float)
    for row in rows:
        value = finite(row.get(f"{variant}_pnl"))
        closed_at = row.get(f"{variant}_closed_at")
        if value is not None and closed_at:
            events[parse_dt(closed_at)] += value
    cumulative = 0.0
    peak = 0.0
    output = []
    for at in sorted(events):
        cumulative += events[at]
        peak = max(peak, cumulative)
        output.append(
            {
                "at": at.isoformat(),
                "step": events[at],
                "cumulative": cumulative,
                "drawdown": cumulative - peak,
            }
        )
    return output


def paired_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    paired = [
        row
        for row in rows
        if finite(row.get("current_pnl")) is not None
        and finite(row.get("skip_pnl")) is not None
    ]
    deltas = [float(row["skip_pnl"]) - float(row["current_pnl"]) for row in paired]
    positive_order = sorted(deltas, reverse=True)
    return {
        "paired_entries": len(paired),
        "current": equity_stats(paired, "current"),
        "skip": equity_stats(paired, "skip"),
        "delta_skip_minus_current": {
            "net_pnl": sum(deltas),
            "mean": statistics.mean(deltas) if deltas else None,
            "median": statistics.median(deltas) if deltas else None,
            "better_count": sum(value > 1e-9 for value in deltas),
            "worse_count": sum(value < -1e-9 for value in deltas),
            "same_count": sum(abs(value) <= 1e-9 for value in deltas),
            "changed_count": sum(abs(value) > 1e-9 for value in deltas),
            "best_delta": max(deltas) if deltas else None,
            "worst_delta": min(deltas) if deltas else None,
            "net_without_top_1_positive_delta": (
                sum(positive_order[1:]) if len(positive_order) > 1 else 0.0
            ),
            "net_without_top_3_positive_deltas": (
                sum(positive_order[3:]) if len(positive_order) > 3 else 0.0
            ),
        },
    }


def compact_trade(row: dict[str, Any]) -> dict[str, Any]:
    delta = (
        finite(row.get("skip_pnl")) - finite(row.get("current_pnl"))
        if finite(row.get("skip_pnl")) is not None
        and finite(row.get("current_pnl")) is not None
        else None
    )
    return {
        "account": row.get("account"),
        "position_id": row.get("position_id"),
        "symbol": row.get("symbol"),
        "opened_at": row.get("opened_at"),
        "entry_price": json_number(row.get("entry_price")),
        "quantity": json_number(row.get("quantity")),
        "entry_notional": json_number(row.get("entry_notional")),
        "actual_closed_at": row.get("actual_closed_at"),
        "actual_exit_price": json_number(row.get("actual_exit_price")),
        "actual_pnl": json_number(row.get("actual_pnl")),
        "actual_reason": row.get("actual_reason"),
        **{
            f"{variant}_{field}": (
                json_number(row.get(f"{variant}_{field}"))
                if field in {"pnl", "exit_price", "target_price"}
                else row.get(f"{variant}_{field}")
            )
            for variant in VARIANTS
            for field in (
                "status",
                "reverse_candle_end",
                "closed_at",
                "exit_price",
                "target_price",
                "pnl",
                "fill_kind",
            )
        },
        "delta_skip_minus_current": json_number(delta),
        "changed": abs(delta) > 1e-9 if delta is not None else False,
    }


def svg_line(
    series: dict[str, list[dict[str, Any]]],
    *,
    title: str,
    value_key: str,
    y_label: str,
) -> str:
    width, height = 960, 340
    left, right, top, bottom = 70, 24, 46, 48
    points = [point for values in series.values() for point in values]
    if not points:
        return f'<svg viewBox="0 0 {width} {height}"><text x="20" y="40">{html.escape(title)}：无数据</text></svg>'
    timestamps = [parse_dt(point["at"]).timestamp() for point in points]
    values = [float(point[value_key]) for point in points]
    min_x, max_x = min(timestamps), max(timestamps)
    min_y, max_y = min(0.0, min(values)), max(0.0, max(values))
    if max_x == min_x:
        max_x += 1
    if max_y == min_y:
        max_y += 1
    plot_w, plot_h = width - left - right, height - top - bottom

    def xy(point: dict[str, Any]) -> tuple[float, float]:
        x = left + (parse_dt(point["at"]).timestamp() - min_x) / (max_x - min_x) * plot_w
        y = top + (max_y - float(point[value_key])) / (max_y - min_y) * plot_h
        return x, y

    zero_y = top + (max_y / (max_y - min_y)) * plot_h
    parts = [
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="{html.escape(title)}">',
        f'<rect width="{width}" height="{height}" fill="#10151f" rx="14"/>',
        f'<text x="{left}" y="28" fill="#eef4ff" font-size="16" font-weight="700">{html.escape(title)}</text>',
        f'<line x1="{left}" y1="{zero_y:.1f}" x2="{width-right}" y2="{zero_y:.1f}" stroke="#536274" stroke-dasharray="4 4"/>',
        f'<text x="8" y="{top+5}" fill="#95a7bd" font-size="11">{html.escape(y_label)}</text>',
    ]
    for index, (variant, points_for_variant) in enumerate(series.items()):
        if not points_for_variant:
            continue
        coords = [xy(point) for point in points_for_variant]
        path = " ".join(
            ("M" if index_point == 0 else "L")
            + f" {x:.1f} {y:.1f}"
            for index_point, (x, y) in enumerate(coords)
        )
        parts.append(
            f'<path d="{path}" fill="none" stroke="{COLORS[variant]}" stroke-width="2.4"/> '
            f'<text x="{left + index*230}" y="{height-16}" fill="{COLORS[variant]}" font-size="12">● {html.escape(VARIANT_LABELS[variant])}</text>'
        )
    parts.append("</svg>")
    return "".join(parts)


def svg_delta(rows: list[dict[str, Any]], title: str) -> str:
    changed = [
        row
        for row in rows
        if finite(row.get("delta_skip_minus_current")) is not None
        and abs(float(row["delta_skip_minus_current"])) > 1e-9
    ]
    changed.sort(key=lambda row: float(row["delta_skip_minus_current"]))
    changed = changed[:12] + changed[-12:]
    width, row_h = 960, 25
    height = 70 + row_h * max(1, len(changed))
    if not changed:
        return f'<svg viewBox="0 0 {width} 100"><text x="20" y="40">{html.escape(title)}：无差异</text></svg>'
    max_abs = max(abs(float(row["delta_skip_minus_current"])) for row in changed) or 1.0
    center = 580
    scale = 300 / max_abs
    parts = [
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="{html.escape(title)}">',
        f'<rect width="{width}" height="{height}" fill="#10151f" rx="14"/>',
        f'<text x="24" y="30" fill="#eef4ff" font-size="16" font-weight="700">{html.escape(title)}</text>',
        f'<line x1="{center}" y1="48" x2="{center}" y2="{height-18}" stroke="#536274"/>',
    ]
    for index, row in enumerate(changed):
        y = 58 + index * row_h
        delta = float(row["delta_skip_minus_current"])
        bar_x = center if delta >= 0 else center + delta * scale
        bar_w = abs(delta) * scale
        color = COLORS["skip"] if delta >= 0 else "#ff7d8d"
        label = f"{row.get('account')} · {row.get('symbol')} · {row.get('opened_at', '')[:16]}"
        parts.append(
            f'<text x="24" y="{y+4}" fill="#b8c6d8" font-size="11">{html.escape(label)}</text>'
            f'<rect x="{bar_x:.1f}" y="{y-8}" width="{bar_w:.1f}" height="15" fill="{color}" rx="3"/>'
            f'<text x="900" y="{y+4}" fill="{color}" font-size="11" text-anchor="end">{delta:+.3f}</text>'
        )
    parts.append("</svg>")
    return "".join(parts)


def write_report(rows: list[dict[str, Any]], metadata: dict[str, Any], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    compact_rows = [compact_trade(row) for row in rows]
    compact_rows.sort(key=lambda row: (row["account"], row["opened_at"], row["position_id"]))
    csv_path = output_dir / "trade_comparison.csv"
    fields = list(compact_rows[0]) if compact_rows else ["account", "position_id"]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(compact_rows)

    metrics: dict[str, Any] = {
        "metadata": metadata,
        "accounts": {},
    }
    for account in ("b1", "live"):
        account_rows = [row for row in rows if row.get("account") == account]
        paired = [
            row
            for row in account_rows
            if finite(row.get("current_pnl")) is not None
            and finite(row.get("skip_pnl")) is not None
        ]
        metrics["accounts"][account] = {
            "all_positions": len(account_rows),
            "actual_closed": sum(row.get("actual_closed_at") is not None for row in account_rows),
            "actual_pnl_sum": sum(
                finite(row.get("actual_pnl")) or 0.0 for row in account_rows
            ),
            "current_all": equity_stats(account_rows, "current"),
            "skip_all": equity_stats(account_rows, "skip"),
            "paired": paired_summary(account_rows),
            "unresolved_symbols": sorted(
                {
                    str(row["symbol"])
                    for row in account_rows
                    if finite(row.get("current_pnl")) is None
                }
            ),
            "top_worse": sorted(
                [compact_trade(row) for row in paired],
                key=lambda row: row["delta_skip_minus_current"],
            )[:12],
            "top_better": sorted(
                [compact_trade(row) for row in paired],
                key=lambda row: row["delta_skip_minus_current"],
                reverse=True,
            )[:12],
            "timeline_current": timeline(paired, "current"),
            "timeline_skip": timeline(paired, "skip"),
        }
    (output_dir / "metrics.json").write_text(
        json.dumps(metrics, ensure_ascii=False, indent=2, allow_nan=False),
        encoding="utf-8",
    )

    sections = []
    for account, title in (("b1", "虚拟账户 B1"), ("live", "实盘账户")):
        account_metrics = metrics["accounts"][account]
        paired = account_metrics["paired"]
        current = paired["current"]
        skip = paired["skip"]
        delta = paired["delta_skip_minus_current"]
        paired_rows = [
            row
            for row in rows
            if row.get("account") == account
            and finite(row.get("current_pnl")) is not None
            and finite(row.get("skip_pnl")) is not None
        ]
        sections.append(
            f"<section><h2>{title}</h2>"
            f"<p class='muted'>全部记录 {account_metrics['all_positions']} 笔；可配对完整结果 {paired['paired_entries']} 笔；实际已平仓 {account_metrics['actual_closed']} 笔。</p>"
            "<table><thead><tr><th>指标</th><th>现行规则</th><th>下一根K线</th><th>变化</th></tr></thead><tbody>"
            f"<tr><td>模拟净PNL</td><td>{current['net_pnl']:+.3f}</td><td>{skip['net_pnl']:+.3f}</td><td class='{'good' if delta['net_pnl'] >= 0 else 'bad'}'>{delta['net_pnl']:+.3f}</td></tr>"
            f"<tr><td>剔除最大1笔正向差异</td><td colspan='2'>{delta['net_without_top_1_positive_delta']:+.3f}</td><td>检验单笔行情依赖</td></tr>"
            f"<tr><td>胜率</td><td>{current['win_rate']*100:.1f}%</td><td>{skip['win_rate']*100:.1f}%</td><td>{(skip['win_rate']-current['win_rate'])*100:+.1f}pp</td></tr>"
            f"<tr><td>Profit Factor</td><td>{current['profit_factor']:.3f}</td><td>{skip['profit_factor']:.3f}</td><td>{skip['profit_factor']-current['profit_factor']:+.3f}</td></tr>"
            f"<tr><td>序列最大回撤</td><td>{current['max_drawdown']:+.3f}</td><td>{skip['max_drawdown']:+.3f}</td><td>{skip['max_drawdown']-current['max_drawdown']:+.3f}</td></tr>"
            f"<tr><td>逐笔变化</td><td colspan='2'>改善 {delta['better_count']} · 变差 {delta['worse_count']} · 不变 {delta['same_count']}</td><td>{delta['changed_count']} 笔改变</td></tr>"
            "</tbody></table>"
            f"<div class='chart'>{svg_line({'current': account_metrics['timeline_current'], 'skip': account_metrics['timeline_skip']}, title='累计模拟净PNL · '+title, value_key='cumulative', y_label='USDT')}</div>"
            f"<div class='chart'>{svg_line({'current': account_metrics['timeline_current'], 'skip': account_metrics['timeline_skip']}, title='序列回撤 · '+title, value_key='drawdown', y_label='USDT')}</div>"
            f"<div class='chart'>{svg_delta([compact_trade(row) for row in paired_rows], '逐笔 ΔPNL（下一根K线 − 现行） · '+title)}</div>"
            f"<h3>差异最大的交易</h3>{trade_table(account_metrics['top_better'][:8], account_metrics['top_worse'][:8])}</section>"
        )
    report = """<!doctype html><html lang='zh-CN'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>入场K线边界回测</title><style>
body{margin:0;background:#0b1018;color:#e9f0fa;font:14px/1.55 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}main{max-width:1120px;margin:0 auto;padding:28px}h1{margin:0 0 8px;font-size:26px}h2{margin-top:36px;color:#55d6be}h3{color:#c8d5e6}.muted{color:#93a4b8}.chart{margin:18px 0;border:1px solid #263241;border-radius:14px;overflow:hidden}table{width:100%;border-collapse:collapse;background:#111925;border-radius:10px;overflow:hidden}th,td{padding:9px 10px;border-bottom:1px solid #263241;text-align:right}th:first-child,td:first-child{text-align:left}th{color:#9eb0c4;background:#182333}.good{color:#55d6be}.bad{color:#ff7d8d}.small{font-size:12px;color:#9eb0c4}a{color:#55d6be}.trade-table{font-size:12px}.trade-table td,.trade-table th{padding:6px}.trade-table td:nth-child(2),.trade-table th:nth-child(2){text-align:left}</style></head><body><main><h1>入场所在15m K线是否应跳过</h1><p class='muted'>现行：入场所在K线收阴即可触发；假设：从下一根完整15m K线开始检查。两种方案使用同一批真实开仓、同一手续费和同一服务器15秒买一价，唯一改变是反向K线的起点。</p>""" + "".join(sections) + f"<p class='small'>数据截止：{html.escape(str(metadata.get('candle_cutoff')))}；官方K线加载 {metadata.get('candle_symbols_loaded')}/{metadata.get('symbol_count')} 个品种；LONGXIAUSDT（本地名称“龙虾USDT”）未成功获取，相关交易列为 unresolved。完整逐笔数据见 <a href='trade_comparison.csv'>trade_comparison.csv</a>，指标见 <a href='metrics.json'>metrics.json</a>。</p></main></body></html>"
    (output_dir / "entry-candle-boundary-report.html").write_text(report, encoding="utf-8")


def trade_table(better: list[dict[str, Any]], worse: list[dict[str, Any]]) -> str:
    rows = [("改善", row) for row in better] + [("变差", row) for row in worse]
    body = []
    for label, row in rows:
        body.append(
            "<tr>"
            f"<td>{html.escape(label)}</td><td>{html.escape(str(row.get('account')))}</td>"
            f"<td>{html.escape(str(row.get('symbol')))}</td><td>{html.escape(str(row.get('opened_at'))[:19])}</td>"
            f"<td>{float(row.get('current_pnl') or 0):+.3f}</td><td>{float(row.get('skip_pnl') or 0):+.3f}</td>"
            f"<td>{float(row.get('delta_skip_minus_current') or 0):+.3f}</td>"
            "</tr>"
        )
    return "<table class='trade-table'><thead><tr><th>方向</th><th>账户</th><th>品种</th><th>开仓时间</th><th>现行PNL</th><th>下一根PNL</th><th>Δ</th></tr></thead><tbody>" + "".join(body) + "</tbody></table>"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=Path("/private/tmp/candle_entry_boundary_results.json"))
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    source = json.loads(args.input.read_text(encoding="utf-8"))
    rows = source.get("rows") or []
    metadata = {
        key: source.get(key)
        for key in (
            "generated_at",
            "candle_source",
            "candle_cutoff",
            "candle_count",
            "candle_symbols_loaded",
            "symbol_count",
            "candle_errors",
            "runtime_state_max",
            "runtime_quote_rows",
            "b1_position_rows",
            "live_entry_order_rows",
            "live_reconstructed_rows",
        )
    }
    write_report(rows, metadata, args.output_dir)


if __name__ == "__main__":
    main()
