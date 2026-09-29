"""Update the inline B0/B1/B2/B3/B4/B8/B16 visualization from a replay result."""

from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Any


def read_series(path: Path) -> list[dict[str, Any]]:
    fields = (
        "timestamp",
        "b0_cumulative_pnl",
        "b1_cumulative_pnl",
        "b2_cumulative_pnl",
        "b3_cumulative_pnl",
        "b4_cumulative_pnl",
        "b8_cumulative_pnl",
        "b16_cumulative_pnl",
        "b96_cumulative_pnl",
        "b0_drawdown",
        "b1_drawdown",
        "b2_drawdown",
        "b3_drawdown",
        "b4_drawdown",
        "b8_drawdown",
        "b16_drawdown",
        "b96_drawdown",
    )
    rows: list[dict[str, Any]] = []
    with path.open(newline="", encoding="utf-8") as handle:
        for source in csv.DictReader(handle):
            row: dict[str, Any] = {"timestamp": source["timestamp"]}
            for field in fields[1:]:
                row[field] = float(source[field])
            rows.append(row)
    return rows


def load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object in {path}")
    return value


def replace_once(text: str, pattern: str, replacement: str) -> str:
    updated, count = re.subn(pattern, replacement, text, count=1, flags=re.DOTALL)
    if count != 1:
        raise ValueError(f"template pattern did not match exactly once: {pattern}")
    return updated


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--template", type=Path, required=True)
    parser.add_argument("--series", type=Path, required=True)
    parser.add_argument("--metrics", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    html = args.template.read_text(encoding="utf-8")
    series = read_series(args.series)
    metrics = load_json(args.metrics)
    manifest = load_json(args.manifest)
    b0 = metrics["b0"]
    b1 = metrics["b1"]
    b2 = metrics["b2"]
    b3 = metrics["b3"]
    b4 = metrics["b4"]
    b8 = metrics["b8"]
    b16 = metrics["b16"]
    b96 = metrics["b96"]
    paired = metrics["paired_delta"]
    paired_b2 = metrics["paired_delta_b2"]
    paired_b3 = metrics["paired_delta_b3"]
    paired_b4 = metrics["paired_delta_b4"]
    paired_b8 = metrics["paired_delta_b8"]
    paired_b16 = metrics["paired_delta_b16"]
    paired_b96 = metrics["paired_delta_b96"]
    favorable_exit_pct = float(metrics.get("favorable_exit_pct") or 0.0)
    favorable_exit_label = (
        f"方向性盈利目标（开仓价±{favorable_exit_pct:.2%}）"
        if favorable_exit_pct > 0
        else "开仓价退出"
    )
    side_filter = metrics.get("data", {}).get("side_filter", "all")
    scope_label = {
        "all": "多空合计",
        "long": "只做多",
        "short": "只做空",
    }.get(side_filter, str(side_filter))
    snapshot = manifest.get("snapshot_at_utc") or "unknown"
    snapshot_display = (
        str(snapshot).replace("T", " ").replace("+00:00", " UTC").replace("Z", " UTC")
    )
    mature = int(metrics["common_matured_rows"])
    compact_series = [
        {
            key: round(value, 6) if isinstance(value, float) else value
            for key, value in row.items()
        }
        for row in series
    ]
    data_json = json.dumps(compact_series, ensure_ascii=False, separators=(",", ":"))
    html = replace_once(
        html,
        (
            r"const data = \[.*?\]\.map\(\(row\) => \(\{\s*\.\.\.row,\s*"
            r"t: new Date\(row\.timestamp\),\s*\}\)\);"
        ),
        "const data = "
        + data_json
        + ".map((row) => ({\n    ...row,\n    t: new Date(row.timestamp),\n  }));",
    )
    html = replace_once(
        html,
        r"B0 · 服务器实际（[^）]+）",
        f"B0 · 服务器实际（{float(b0['net_pnl']):.2f}）",
    )
    html = replace_once(
        html,
        r'<div class="viz-legend" aria-label="序列图例">.*?</div>',
        '<div class="viz-legend" aria-label="序列图例">\n'
        '    <button type="button" aria-pressed="true" data-series="b0">\n'
        '      <span class="swatch" style="--swatch: var(--viz-series-1)"></span>\n'
        f"      <span>B0 · 服务器实际（{float(b0['net_pnl']):.2f}）</span>\n"
        '    </button>\n'
        '    <button type="button" aria-pressed="true" data-series="b1">\n'
        '      <span class="swatch" style="--swatch: var(--viz-series-2)"></span>\n'
        f"      <span>B1 · 一根宽限（{float(b1['net_pnl']):.2f}）</span>\n"
        '    </button>\n'
        '    <button type="button" aria-pressed="true" data-series="b2">\n'
        '      <span class="swatch" style="--swatch: var(--viz-series-3)"></span>\n'
        f"      <span>B2 · 两根宽限（{float(b2['net_pnl']):.2f}）</span>\n"
        '    </button>\n'
        '    <button type="button" aria-pressed="true" data-series="b3">\n'
        '      <span class="swatch" style="--swatch: var(--viz-series-4)"></span>\n'
        f"      <span>B3 · 三根宽限（{float(b3['net_pnl']):.2f}）</span>\n"
        '    </button>\n'
        '    <button type="button" aria-pressed="true" data-series="b4">\n'
        '      <span class="swatch" style="--swatch: var(--viz-series-5)"></span>\n'
        f"      <span>B4 · 四根宽限（{float(b4['net_pnl']):.2f}）</span>\n"
        '    </button>\n'
        '    <button type="button" aria-pressed="true" data-series="b8">\n'
        '      <span class="swatch" style="--swatch: var(--viz-series-6)"></span>\n'
        f"      <span>B8 · 八根宽限（{float(b8['net_pnl']):.2f}）</span>\n"
        '    </button>\n'
        '    <button type="button" aria-pressed="true" data-series="b16">\n'
        '      <span class="swatch" style="--swatch: var(--viz-series-1)"></span>\n'
        f"      <span>B16 · 十六根宽限（{float(b16['net_pnl']):.2f}）</span>\n"
        '    </button>\n'
        '    <button type="button" aria-pressed="true" data-series="b96">\n'
        '      <span class="swatch" style="--swatch: var(--viz-series-2)"></span>\n'
        f"      <span>B96 · 九十六根宽限（{float(b96['net_pnl']):.2f}）</span>\n"
        '    </button>\n'
        '  </div>',
    )
    html = replace_once(
        html,
        r"B1 · 一根宽限（[^）]+）",
        f"B1 · 一根宽限（{float(b1['net_pnl']):.2f}）",
    )
    html = replace_once(
        html,
        r"const series = \{.*?\};",
        "const series = {\n"
        '    b0: { label: "B0", color: "var(--viz-series-1)" },\n'
        '    b1: { label: "B1", color: "var(--viz-series-2)" },\n'
        '    b2: { label: "B2", color: "var(--viz-series-3)" },\n'
        '    b3: { label: "B3", color: "var(--viz-series-4)" },\n'
        '    b4: { label: "B4", color: "var(--viz-series-5)" },\n'
        '    b8: { label: "B8", color: "var(--viz-series-6)" },\n'
        '    b16: { label: "B16", color: "var(--viz-series-1)", dash: "5 3" },\n'
        '    b96: { label: "B96", color: "var(--viz-series-2)", dash: "2 3" },\n'
        "  };",
    )
    html = replace_once(
        html,
        r'b1: "b1_cumulative_pnl"(?:, b2: "b2_cumulative_pnl")?'
        r'(?:, b3: "b3_cumulative_pnl")?'
        r'(?:, b4: "b4_cumulative_pnl")?'
        r'(?:, b8: "b8_cumulative_pnl")?'
        r'(?:, b16: "b16_cumulative_pnl")?',
        'b1: "b1_cumulative_pnl", b2: "b2_cumulative_pnl", '
        'b3: "b3_cumulative_pnl", b4: "b4_cumulative_pnl", '
        'b8: "b8_cumulative_pnl", b16: "b16_cumulative_pnl"',
        'b3: "b3_cumulative_pnl", b4: "b4_cumulative_pnl", '
        'b8: "b8_cumulative_pnl", b16: "b16_cumulative_pnl", '
        'b96: "b96_cumulative_pnl"',
    )
    html = replace_once(
        html,
        r'b1: "b1_drawdown"(?:, b2: "b2_drawdown")?'
        r'(?:, b3: "b3_drawdown")?'
        r'(?:, b4: "b4_drawdown")?'
        r'(?:, b8: "b8_drawdown")?'
        r'(?:, b16: "b16_drawdown")?',
        'b1: "b1_drawdown", b2: "b2_drawdown", '
        'b3: "b3_drawdown", b4: "b4_drawdown", '
        'b8: "b8_drawdown", b16: "b16_drawdown"',
        'b3: "b3_drawdown", b4: "b4_drawdown", '
        'b8: "b8_drawdown", b16: "b16_drawdown", '
        'b96: "b96_drawdown"',
    )
    html = replace_once(
        html,
        r"const visible = \{.*?\};",
        "const visible = { b0: true, b1: true, b2: true, b3: true, "
        "b4: true, b8: true, b16: true, b96: true };",
    )
    html = replace_once(
        html,
        r"const values = data\.flatMap\(\(row\) => \[.*?\]\);",
        "const values = data.flatMap((row) => ["
        "row[chart.b0], row[chart.b1], row[chart.b2], row[chart.b3], "
        "row[chart.b4], row[chart.b8], row[chart.b16], row[chart.b96], 0]);",
    )
    html = replace_once(
        html,
        r"<p>配对交易累计净 PnL（USDT）与回撤.*?</p>",
        "<p>配对交易累计净 PnL（USDT）与回撤｜范围："
        f"{scope_label}｜B1：反向 15m 后挂{favorable_exit_label}，"
        "下一根收线超时强平；B2：等两根 15m；B3：等三根 15m；B4：等四根 15m｜"
        "B8：等八根 15m｜"
        "B16：等十六根 15m｜B96：等九十六根 15m｜"
        f"快照 {snapshot_display} · {mature:,} 笔成熟配对"
        "</p>",
    )
    html = replace_once(
        html,
        r"<h2>Orderflow Impulse · .*? · 最新服务器快照</h2>",
        f"<h2>Orderflow Impulse · {scope_label} · "
        "B0 vs B1 vs B2 vs B3 vs B4 vs B8 vs B16 vs B96 · 最新服务器快照</h2>",
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(html, encoding="utf-8")
    print(
        json.dumps(
            {
                "output": str(args.output),
                "points": len(series),
                "b0_net_pnl": b0["net_pnl"],
                "b1_net_pnl": b1["net_pnl"],
                "b2_net_pnl": b2["net_pnl"],
                "b3_net_pnl": b3["net_pnl"],
                "b4_net_pnl": b4["net_pnl"],
                "b8_net_pnl": b8["net_pnl"],
                "b16_net_pnl": b16["net_pnl"],
                "b96_net_pnl": b96["net_pnl"],
                "paired_delta": paired["net_pnl_delta"],
                "paired_delta_b2": paired_b2["net_pnl_delta"],
                "paired_delta_b3": paired_b3["net_pnl_delta"],
                "paired_delta_b4": paired_b4["net_pnl_delta"],
                "paired_delta_b8": paired_b8["net_pnl_delta"],
                "paired_delta_b16": paired_b16["net_pnl_delta"],
                "paired_delta_b96": paired_b96["net_pnl_delta"],
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
