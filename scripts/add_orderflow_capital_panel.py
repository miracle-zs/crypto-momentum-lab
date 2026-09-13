"""Add a capital-usage subplot to an orderflow visualization fragment."""

from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Any

VARIANTS = ("b0", "b1", "b2", "b3", "b4", "b8", "b16", "b96")


def replace_once(text: str, pattern: str, replacement: str) -> str:
    updated, count = re.subn(pattern, replacement, text, count=1, flags=re.DOTALL)
    if count != 1:
        raise ValueError(f"template pattern did not match exactly once: {pattern}")
    return updated


def read_capital(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(newline="", encoding="utf-8") as handle:
        for source in csv.DictReader(handle):
            rows.append(
                {
                    "timestamp": source["timestamp"],
                    **{
                        f"{variant}_capital_used": round(
                            float(source[f"{variant}_capital_used"]), 6
                        )
                        for variant in VARIANTS
                    },
                }
            )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--capital-series", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    html = args.input.read_text(encoding="utf-8")
    capital = read_capital(args.capital_series)
    data_json = json.dumps(capital, ensure_ascii=False, separators=(",", ":"))

    panel = (
        '    <section class="plot-panel plot-panel-wide" aria-label="资金占用">\n'
        '      <svg class="plot" data-plot="capital" role="img" '
        'aria-label="B0、B1、B2、B3、B4、B8、B16 与 B96 的资金占用时序图"></svg>\n'
        "    </section>"
    )
    html = replace_once(
        html,
        r'(<section class="plot-panel" aria-label="序列回撤">.*?</section>)',
        r"\1\n" + panel,
    )
    html = replace_once(
        html,
        r"(#orderflow-b0-b1-replay \.plot-panel \{\s*min-width: 0;\s*\})",
        r"\1\n#orderflow-b0-b1-replay .plot-panel-wide {\n  grid-column: 1 / -1;\n}",
    )
    html = replace_once(
        html,
        r"(\n  const data = .*?\.map\(\(row\) => \(\{\s*\.\.\.row,\s*"
        r"t: new Date\(row\.timestamp\),\s*\}\)\);)",
        r"\1\n  const capitalData = "
        + data_json
        + ".map((row) => ({\n    ...row,\n    t: new Date(row.timestamp),\n  }));",
    )
    html = replace_once(
        html,
        r"const charts = \[.*?\n  \];",
        "const charts = [\n"
        '    { key: "pnl", title: "累计净 PnL（USDT）", b0: "b0_cumulative_pnl", b1: "b1_cumulative_pnl", b2: "b2_cumulative_pnl", b3: "b3_cumulative_pnl", b4: "b4_cumulative_pnl", b8: "b8_cumulative_pnl", b16: "b16_cumulative_pnl" },\n'
        '    { key: "drawdown", title: "序列回撤（USDT）", b0: "b0_drawdown", b1: "b1_drawdown", b2: "b2_drawdown", b3: "b3_drawdown", b4: "b4_drawdown", b8: "b8_drawdown", b16: "b16_drawdown" },\n'
        '    { key: "capital", title: "资金占用（USDT）", b0: "b0_capital_used", b1: "b1_capital_used", b2: "b2_capital_used", b3: "b3_capital_used", b4: "b4_capital_used", b8: "b8_capital_used", b16: "b16_capital_used", b96: "b96_capital_used" },\n'
        "  ];",
    )
    html = replace_once(
        html,
        r"  function valueAt\(time, key\) \{.*?\n  \}\n\n  function draw",
        "  function valueAt(time, key, source = data) {\n"
        "    if (!source.length) return 0;\n"
        "    const nearestIndex = Math.max(0, Math.min(source.length - 1, bisect(source, time)));\n"
        "    const leftIndex = source[nearestIndex].t <= time ? nearestIndex : Math.max(0, nearestIndex - 1);\n"
        "    const left = source[leftIndex];\n"
        "    const right = source[Math.min(source.length - 1, leftIndex + 1)];\n"
        "    if (!right || !left || right.t.getTime() === left.t.getTime()) return (right || left)[key];\n"
        "    const ratio = (time - left.t) / (right.t - left.t);\n"
        "    return left[key] + (right[key] - left[key]) * ratio;\n"
        "  }\n\n  function draw",
    )
    html = replace_once(
        html,
        r"    const rootGroup = d3\.select\(svg\).*?;\n    const x = d3\.scaleTime\(\)\.domain\(d3\.extent\(data, \(row\) => row\.t\)\)\.range\(\[0, innerWidth\]\);\n    const values = data\.flatMap",
        "    const rootGroup = d3.select(svg).append(\"g\").attr(\"transform\", `translate(${margin.left},${margin.top})`);\n"
        "    const chartData = chart.key === \"capital\" ? capitalData : data;\n"
        "    const x = d3.scaleTime().domain(d3.extent(chartData, (row) => row.t)).range([0, innerWidth]);\n"
        "    const values = chartData.flatMap",
    )
    html = replace_once(
        html,
        r"\.datum\(data\)\.attr\(\"class\", `series-line series-\$\{key\}`",
        ".datum(chartData).attr(\"class\", `series-line series-${key}`",
    )
    html = replace_once(
        html,
        r"    overlay\.on\(\"pointermove\", \(event\) => \{\n      const \[px\]",
        '    overlay.on("pointermove", (event) => {\n'
        '      const source = chart.key === "capital" ? capitalData : data;\n'
        "      const [px]",
    )
    html = html.replace(
        "const current = valueAt(time, chart[key]);",
        "const current = valueAt(time, chart[key], source);",
    )
    html = html.replace(
        "${valueAt(time, chart[key]).toFixed(2)} USDT",
        "${valueAt(time, chart[key], source).toFixed(2)} USDT",
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(html, encoding="utf-8")
    print(
        json.dumps(
            {"output": str(args.output), "capital_points": len(capital)},
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
