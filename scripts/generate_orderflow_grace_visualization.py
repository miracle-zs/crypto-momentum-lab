"""Generate a self-contained comparison page for orderflow grace exits.

The generated page deliberately uses browser-native SVG and JavaScript rather
than a CDN dependency.  It can therefore be opened from a local file without
the ``d3 is not defined`` failure that affected an earlier inline fragment.
"""

# The generated HTML/JavaScript intentionally keeps several template lines
# together so the embedded page remains easy to inspect as a standalone file.
# ruff: noqa: E501

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

VARIANTS = ("b0", "b1", "b2", "b3", "b4", "b8", "b16", "b96")
LABELS = {
    "b0": "B0 · 基准",
    "b1": "B1 · 1 根宽限",
    "b2": "B2 · 2 根宽限",
    "b3": "B3 · 3 根宽限",
    "b4": "B4 · 4 根宽限",
    "b8": "B8 · 8 根宽限",
    "b16": "B16 · 16 根宽限",
    "b96": "B96 · 96 根宽限",
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


def read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def numeric_rows(path: Path, fields: tuple[str, ...]) -> list[dict[str, Any]]:
    rows = []
    for source in read_csv(path):
        row: dict[str, Any] = {"timestamp": source["timestamp"]}
        for field in fields:
            value = source.get(field)
            row[field] = float(value) if value not in (None, "") else None
        rows.append(row)
    return rows


def load_metrics(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected an object in {path}")
    return value


def compact(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def metric_cards(metrics: dict[str, Any], capital: list[dict[str, Any]]) -> list[dict[str, Any]]:
    common = metrics.get("common_matured_rows")
    result = []
    for variant in VARIANTS:
        values = [
            float(row[f"{variant}_capital_used"])
            for row in capital
            if row.get(f"{variant}_capital_used") not in (None, "")
        ]
        result.append(
            {
                "id": variant,
                "label": LABELS[variant],
                "net": metrics.get(variant, {}).get("net_pnl"),
                "drawdown": metrics.get(variant, {}).get("max_drawdown"),
                "peakCapital": max(values, default=None),
                "mature": common,
            }
        )
    return result


def build_html(
    *,
    equity: list[dict[str, Any]],
    capital: list[dict[str, Any]],
    metrics: dict[str, Any],
    title: str,
) -> str:
    equity_fields = tuple(
        field
        for variant in VARIANTS
        for field in (f"{variant}_cumulative_pnl", f"{variant}_drawdown")
    )
    capital_fields = tuple(f"{variant}_capital_used" for variant in VARIANTS)
    equity_data = numeric_rows_from_dicts(equity, equity_fields)
    capital_data = numeric_rows_from_dicts(capital, capital_fields)
    cards = metric_cards(metrics, capital)
    manifest = metrics.get("data", {})
    snapshot = manifest.get("snapshot_at_utc") or "unknown"
    side = manifest.get("side_filter") or "all"
    scope = {"long": "只做多", "short": "只做空", "all": "多空合计"}.get(side, side)
    subtitle = (
        f"{scope} · 同一批共同成熟入场 · 手续费 {float(metrics.get('fee_rate') or 0):.4%} · "
        f"快照 {snapshot} · 序列点 {len(equity_data):,}"
    )
    data_json = compact({"equity": equity_data, "capital": capital_data})
    cards_json = compact(cards)
    labels_json = compact(LABELS)
    colors_json = compact(COLORS)
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>
:root {{ color-scheme: dark; --bg:#0d1119; --panel:#151c28; --panel2:#1b2534; --text:#e6edf7; --muted:#94a3b8; --grid:#2a3545; --line:#3a475b; }}
* {{ box-sizing:border-box; }} body {{ margin:0; padding:24px; background:var(--bg); color:var(--text); font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; }}
main {{ max-width:1500px; margin:0 auto; }} h1 {{ margin:0 0 4px; font-size:24px; }} .subtitle {{ color:var(--muted); margin:0 0 18px; }}
.legend {{ display:flex; flex-wrap:wrap; gap:7px 15px; margin:0 0 14px; }} .legend button {{ border:0; border-radius:7px; padding:5px 7px; color:var(--text); background:transparent; cursor:pointer; }}
.legend button[aria-pressed="false"] {{ opacity:.36; text-decoration:line-through; }} .swatch {{ display:inline-block; width:20px; height:3px; vertical-align:middle; margin-right:6px; background:var(--c); }}
.grid {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:14px; }} .panel {{ min-width:0; padding:14px; border:1px solid var(--line); border-radius:12px; background:var(--panel); }}
.panel.wide {{ grid-column:1/-1; }} h2 {{ font-size:15px; margin:0 0 8px; }} .chart {{ display:block; width:100%; height:315px; overflow:visible; }}
.axis text {{ fill:var(--muted); font-size:11px; }} .axis line,.axis path {{ stroke:var(--line); }} .grid-line {{ stroke:var(--grid); stroke-dasharray:2 4; }} .series {{ fill:none; stroke-width:2; stroke-linejoin:round; stroke-linecap:round; }}
.zero {{ stroke:var(--muted); stroke-dasharray:4 4; opacity:.6; }} .hint {{ color:var(--muted); font-size:12px; margin:8px 0 0; }}
.cards {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:8px; margin:0 0 14px; }} .card {{ padding:10px; border:1px solid var(--line); border-radius:9px; background:var(--panel); }} .card-name {{ color:var(--muted); font-size:12px; }} .card-value {{ font-size:18px; margin-top:3px; }} .card-meta {{ color:var(--muted); font-size:11px; margin-top:3px; }}
.tooltip {{ position:fixed; display:none; z-index:5; pointer-events:none; max-width:340px; padding:8px 10px; border:1px solid var(--line); border-radius:8px; background:var(--panel2); box-shadow:0 8px 24px #0008; font-size:12px; }} .tooltip.on {{ display:block; }} .tip-time {{ color:var(--muted); margin-bottom:4px; }}
@media (max-width:900px) {{ body {{ padding:12px; }} .grid {{ grid-template-columns:1fr; }} .cards {{ grid-template-columns:repeat(2,minmax(0,1fr)); }} .panel.wide {{ grid-column:auto; }} }}
</style>
</head>
<body>
<main>
  <h1>{title}</h1>
  <p class="subtitle">{subtitle}</p>
  <div class="cards" id="cards"></div>
  <div class="legend" id="legend" aria-label="策略图例"></div>
  <div class="grid">
    <section class="panel wide"><h2>累计净 PnL（USDT）</h2><svg class="chart" data-chart="pnl" role="img" aria-label="累计净 PnL"></svg><p class="hint">只统计 B0/B1/B2/B3/B4/B8/B16/B96 都已成熟的共同交易，避免快照尾部未成熟带来的偏差。</p></section>
    <section class="panel"><h2>序列回撤（USDT）</h2><svg class="chart" data-chart="drawdown" role="img" aria-label="序列回撤"></svg></section>
    <section class="panel"><h2>资金占用（USDT）</h2><svg class="chart" data-chart="capital" role="img" aria-label="资金占用"></svg></section>
  </div>
</main>
<div class="tooltip" id="tooltip"></div>
<script>
(() => {{
  const payload = {data_json};
  const labels = {labels_json};
  const colors = {colors_json};
  const cards = {cards_json};
  const variants = {compact(list(VARIANTS))};
  const active = new Set(variants);
  const parsed = Object.fromEntries(Object.entries(payload).map(([key, rows]) => [key, rows.map(row => ({{ ...row, t: Date.parse(row.timestamp) }}))]));
  const tooltip = document.getElementById('tooltip');
  const money = value => value == null || !Number.isFinite(value) ? '—' : `${{value >= 0 ? '+' : ''}}${{value.toFixed(2)}}`;
  const esc = value => String(value).replace(/[&<>"']/g, char => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[char]));
  const fmtTime = time => new Date(time).toLocaleString('zh-CN', {{hour12:false, month:'2-digit', day:'2-digit', hour:'2-digit', minute:'2-digit'}});
  const nearest = (rows, time) => rows.reduce((best, row) => Math.abs(row.t-time) < Math.abs(best.t-time) ? row : best, rows[0]);
  const linePath = (rows, key, x, y) => rows.filter(row => Number.isFinite(row[key])).map((row, i) => `${{i ? 'L' : 'M'}}${{x(row.t).toFixed(2)}} ${{y(row[key]).toFixed(2)}}`).join(' ');
  function niceDomain(values, isCapital) {{
    const finite = values.filter(Number.isFinite); if (!finite.length) return [0, 1];
    let lo = Math.min(...finite), hi = Math.max(...finite); if (lo === hi) {{ lo -= 1; hi += 1; }}
    if (isCapital) lo = Math.min(0, lo); else {{ const pad = (hi-lo)*.08; lo -= pad; hi += pad; }}
    return [lo, hi];
  }}
  function render(svg, chartKey) {{
    const rows = parsed[chartKey === 'capital' ? 'capital' : 'equity']; if (!rows.length) return;
    const rect = svg.getBoundingClientRect(), width = Math.max(360, rect.width || 700), height = 315;
    const margin = {{top:12,right:12,bottom:34,left:55}}, innerW = width-margin.left-margin.right, innerH = height-margin.top-margin.bottom;
    const keys = chartKey === 'capital' ? variants.map(v => `${{v}}_capital_used`) : variants.map(v => `${{v}}_${{chartKey === 'pnl' ? 'cumulative_pnl' : 'drawdown'}}`);
    const allValues = rows.flatMap(row => keys.map(key => row[key])); const [yMin,yMax] = niceDomain(allValues, chartKey === 'capital');
    const times = rows.map(row => row.t), tMin = Math.min(...times), tMax = Math.max(...times) || tMin + 1;
    const x = time => margin.left + ((time-tMin)/(tMax-tMin || 1))*innerW;
    const y = value => margin.top + (1-(value-yMin)/(yMax-yMin))*innerH;
    const yTicks = 5, xTicks = Math.min(6, Math.max(2, Math.floor(innerW/150)));
    let out = `<g class="grid">`;
    for (let i=0;i<=yTicks;i++) {{ const value=yMin+(yMax-yMin)*i/yTicks, yy=y(value); out += `<line class="grid-line" x1="${{margin.left}}" x2="${{width-margin.right}}" y1="${{yy}}" y2="${{yy}}"/><text class="axis" x="${{margin.left-8}}" y="${{yy+4}}" text-anchor="end">${{money(value)}}</text>`; }}
    for (let i=0;i<xTicks;i++) {{ const time=tMin+(tMax-tMin)*i/(xTicks-1), xx=x(time); out += `<text class="axis" x="${{xx}}" y="${{height-9}}" text-anchor="middle">${{esc(fmtTime(time))}}</text>`; }}
    out += `</g>`;
    if (yMin < 0 && yMax > 0) out += `<line class="zero" x1="${{margin.left}}" x2="${{width-margin.right}}" y1="${{y(0)}}" y2="${{y(0)}}"/>`;
    for (const variant of variants) if (active.has(variant)) out += `<path class="series" stroke="${{colors[variant]}}" d="${{linePath(rows, `${{variant}}_${{chartKey === 'pnl' ? 'cumulative_pnl' : chartKey === 'drawdown' ? 'drawdown' : 'capital_used'}}`, x, y)}}"/>`;
    out += `<rect x="${{margin.left}}" y="${{margin.top}}" width="${{innerW}}" height="${{innerH}}" fill="none" stroke="var(--line)"/>`;
    svg.setAttribute('viewBox', `0 0 ${{width}} ${{height}}`); svg.innerHTML = out;
    svg.onpointermove = event => {{ const box=svg.getBoundingClientRect(), time=tMin+((event.clientX-box.left)/box.width)*(tMax-tMin), row=nearest(rows,time); tooltip.innerHTML=`<div class="tip-time">${{esc(fmtTime(row.t))}}</div>`+variants.filter(v=>active.has(v)).map(v=>`<div><span style="color:${{colors[v]}}">●</span> ${{esc(labels[v])}}：${{money(row[`${{v}}_${{chartKey === 'pnl' ? 'cumulative_pnl' : chartKey === 'drawdown' ? 'drawdown' : 'capital_used'}}`])}} USDT</div>`).join(''); tooltip.classList.add('on'); tooltip.style.left=`${{Math.min(event.clientX+14,innerWidth-355)}}px`; tooltip.style.top=`${{Math.max(8,event.clientY-20)}}px`; }};
    svg.onpointerleave = () => tooltip.classList.remove('on');
  }}
  const legend = document.getElementById('legend'); variants.forEach(variant => {{ const button=document.createElement('button'); button.type='button'; button.setAttribute('aria-pressed','true'); button.innerHTML=`<span class="swatch" style="--c:${{colors[variant]}}"></span>${{labels[variant]}}`; button.onclick=()=>{{ active.has(variant) ? active.delete(variant) : active.add(variant); button.setAttribute('aria-pressed',active.has(variant)); document.querySelectorAll('.chart').forEach(svg=>render(svg,svg.dataset.chart)); }}; legend.appendChild(button); }});
  const cardRoot=document.getElementById('cards'); cards.forEach(card=>{{ const node=document.createElement('div'); node.className='card'; node.innerHTML=`<div class="card-name" style="color:${{colors[card.id]}}">${{esc(card.label)}}</div><div class="card-value">${{money(card.net)}} USDT</div><div class="card-meta">最大回撤 ${{money(card.drawdown)}} · 峰值占用 ${{money(card.peakCapital)}} · ${{card.mature ?? '—'}} 笔</div>`; cardRoot.appendChild(node); }});
  const redraw=()=>document.querySelectorAll('.chart').forEach(svg=>render(svg,svg.dataset.chart)); window.addEventListener('resize',redraw); redraw();
}})();
</script>
</body>
</html>
"""


def numeric_rows_from_dicts(rows: list[dict[str, Any]], fields: tuple[str, ...]) -> list[dict[str, Any]]:
    output = []
    for source in rows:
        row = {"timestamp": source["timestamp"]}
        for field in fields:
            value = source.get(field)
            row[field] = float(value) if value not in (None, "") else None
        output.append(row)
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--equity-series", type=Path, required=True)
    parser.add_argument("--capital-series", type=Path, required=True)
    parser.add_argument("--metrics", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--title", default="Orderflow Impulse · B0/B1/B2/B3/B4/B8/B16/B96")
    args = parser.parse_args()
    metrics = load_metrics(args.metrics)
    equity = read_csv(args.equity_series)
    capital = read_csv(args.capital_series)
    html = build_html(equity=equity, capital=capital, metrics=metrics, title=args.title)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(html, encoding="utf-8")
    print(json.dumps({"output": str(args.output), "equity_points": len(equity), "capital_points": len(capital)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
