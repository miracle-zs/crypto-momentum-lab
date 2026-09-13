"""Export the three orderflow grace comparison charts as standalone SVGs."""

# The SVG template intentionally keeps a few XML lines together for readability
# in the generated artifact.
# ruff: noqa: E501

from __future__ import annotations

import argparse
import csv
import html
import json
from datetime import datetime
from pathlib import Path
from typing import Any

VARIANTS = ("b0", "b1", "b2", "b3", "b4", "b8", "b16", "b96")
LABELS = {
    "b0": "B0",
    "b1": "B1",
    "b2": "B2",
    "b3": "B3",
    "b4": "B4",
    "b8": "B8",
    "b16": "B16",
    "b96": "B96",
}
COLORS = {
    "b0": "#8fa8c7",
    "b1": "#55d6be",
    "b2": "#ffbf69",
    "b3": "#f58cba",
    "b4": "#b8a1ff",
    "b8": "#ff7b72",
    "b16": "#6ea8fe",
    "b96": "#f2e66d",
}

WIDTH = 1440
HEIGHT = 620
LEFT = 86
RIGHT = 34
TOP = 94
BOTTOM = 68


def parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def nice_number(value: float) -> str:
    if abs(value) >= 1000:
        return f"{value:,.0f}"
    if abs(value) >= 100:
        return f"{value:,.0f}"
    return f"{value:,.1f}"


def xml_text(value: Any) -> str:
    return html.escape(str(value), quote=True)


def path_for(
    rows: list[dict[str, str]],
    variant: str,
    field_suffix: str,
    x,
    y,
) -> str:
    points = []
    field = f"{variant}_{field_suffix}"
    for row in rows:
        value = row.get(field)
        if value in (None, ""):
            continue
        points.append((x(parse_dt(row["timestamp"])), y(float(value))))
    if not points:
        return ""
    return "M " + " L ".join(f"{px:.2f},{py:.2f}" for px, py in points)


def render_chart(
    *,
    rows: list[dict[str, str]],
    metric: str,
    title: str,
    subtitle: str,
    output: Path,
    metrics: dict[str, Any],
) -> None:
    if not rows:
        raise ValueError("chart input is empty")
    times = [parse_dt(row["timestamp"]).timestamp() for row in rows]
    suffix = "capital_used" if metric == "capital" else (
        "cumulative_pnl" if metric == "pnl" else "drawdown"
    )
    values = [
        float(row[f"{variant}_{suffix}"])
        for row in rows
        for variant in VARIANTS
        if row.get(f"{variant}_{suffix}") not in (None, "")
    ]
    lo, hi = min(values), max(values)
    if metric == "capital":
        lo = min(0.0, lo)
    elif lo == hi:
        lo -= 1.0
        hi += 1.0
    else:
        pad = (hi - lo) * 0.08
        lo -= pad
        hi += pad
    x0, x1 = times[0], times[-1] if times[-1] > times[0] else times[0] + 1
    plot_width = WIDTH - LEFT - RIGHT
    plot_height = HEIGHT - TOP - BOTTOM

    def x(value: float) -> float:
        return LEFT + (value - x0) / (x1 - x0) * plot_width

    def y(value: float) -> float:
        return TOP + (1.0 - (value - lo) / (hi - lo)) * plot_height

    svg: list[str] = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {WIDTH} {HEIGHT}" role="img" aria-label="{xml_text(title)}">',
        '<rect width="100%" height="100%" fill="#0d1119"/>',
        '<style>text{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif} .axis{fill:#94a3b8;font-size:14px} .grid{stroke:#2a3545;stroke-width:1;stroke-dasharray:3 5} .line{fill:none;stroke-width:2.6;stroke-linecap:round;stroke-linejoin:round} .frame{fill:none;stroke:#3a475b;stroke-width:1} .zero{stroke:#94a3b8;stroke-width:1;stroke-dasharray:5 5;opacity:.65} .legend{font-size:14px;fill:#e6edf7}</style>',
        f'<text x="{LEFT}" y="40" fill="#e6edf7" font-size="24" font-weight="600">{xml_text(title)}</text>',
        f'<text x="{LEFT}" y="66" fill="#94a3b8" font-size="13">{xml_text(subtitle)}</text>',
    ]
    y_ticks = 6
    for index in range(y_ticks + 1):
        value = lo + (hi - lo) * index / y_ticks
        yy = y(value)
        svg.append(
            f'<line class="grid" x1="{LEFT}" x2="{WIDTH-RIGHT}" y1="{yy:.2f}" y2="{yy:.2f}"/>'
        )
        svg.append(
            f'<text class="axis" x="{LEFT-12}" y="{yy+5:.2f}" text-anchor="end">{xml_text(nice_number(value))}</text>'
        )
    if lo < 0 < hi:
        yy = y(0)
        svg.append(
            f'<line class="zero" x1="{LEFT}" x2="{WIDTH-RIGHT}" y1="{yy:.2f}" y2="{yy:.2f}"/>'
        )
    x_ticks = 7
    for index in range(x_ticks):
        timestamp = x0 + (x1 - x0) * index / (x_ticks - 1)
        xx = x(timestamp)
        label = datetime.fromtimestamp(timestamp).strftime("%m-%d %H:%M")
        svg.append(
            f'<text class="axis" x="{xx:.2f}" y="{HEIGHT-BOTTOM+28}" text-anchor="middle">{label}</text>'
        )
    for variant in VARIANTS:
        path = path_for(rows, variant, suffix, lambda value: x(value.timestamp()), y)
        if path:
            svg.append(f'<path class="line" stroke="{COLORS[variant]}" d="{path}"/>')
    svg.append(
        f'<rect class="frame" x="{LEFT}" y="{TOP}" width="{plot_width}" height="{plot_height}"/>'
    )
    legend_x = LEFT
    legend_y = HEIGHT - 18
    for variant in VARIANTS:
        svg.append(
            f'<line x1="{legend_x}" x2="{legend_x+24}" y1="{legend_y-5}" y2="{legend_y-5}" stroke="{COLORS[variant]}" stroke-width="3"/>'
        )
        svg.append(
            f'<text class="legend" x="{legend_x+31}" y="{legend_y}">{LABELS[variant]}</text>'
        )
        legend_x += 96
    # Keep the headline metrics visible even when the SVG is viewed alone.
    metric_line = " · ".join(
        f"{LABELS[variant]} {float(metrics[variant]['net_pnl']):+.1f}"
        for variant in VARIANTS
    )
    svg.append(
        f'<text x="{WIDTH-RIGHT}" y="40" fill="#94a3b8" font-size="12" text-anchor="end">{xml_text(metric_line)}</text>'
    )
    svg.append("</svg>")
    output.write_text("\n".join(svg), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--equity-series", type=Path, required=True)
    parser.add_argument("--capital-series", type=Path, required=True)
    parser.add_argument("--metrics", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    equity = read_csv(args.equity_series)
    capital = read_csv(args.capital_series)
    metrics = json.loads(args.metrics.read_text(encoding="utf-8"))
    args.output_dir.mkdir(parents=True, exist_ok=True)
    common_rows = metrics.get("common_matured_rows")
    manifest = metrics.get("data", {})
    side = manifest.get("side_filter") or "all"
    scope = {"long": "只做多", "short": "只做空", "all": "多空合计"}.get(
        side, side
    )
    snapshot = manifest.get("snapshot_at_utc") or "unknown"
    common = (
        f"{scope} · {common_rows:,} 笔共同成熟交易 · "
        f"每边手续费 {float(metrics.get('fee_rate') or 0):.2%} · 快照 {snapshot}"
    )
    outputs = {
        "cumulative-net-pnl.svg": (equity, "pnl", "累计净 PnL（USDT）"),
        "sequence-drawdown.svg": (equity, "drawdown", "序列回撤（USDT）"),
        "capital-usage.svg": (capital, "capital", "资金占用（USDT）"),
    }
    for filename, (rows, metric, title) in outputs.items():
        render_chart(
            rows=rows,
            metric=metric,
            title=title,
            subtitle=common,
            output=args.output_dir / filename,
            metrics=metrics,
        )
    print({"output_dir": str(args.output_dir), "charts": list(outputs)})


if __name__ == "__main__":
    main()
