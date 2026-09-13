#!/usr/bin/env python3
"""Build the self-contained inline visualization for the server replay report."""

from __future__ import annotations

import json
from pathlib import Path


def main() -> None:
    report_path = Path("server_exports/cml-gainer-top20-from-20260725/replay/live_gainer_top20_report.json")
    output_path = Path("server_exports/cml-gainer-top20-from-20260725/replay/live-gainer-top20-server-comparison.html")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    compact_data = json.dumps(
        {"curves": report["visual_curves"]},
        ensure_ascii=False,
        separators=(",", ":"),
    )
    html = f'''<div id="live-gainer-top20-server-comparison">
  <style>
    #live-gainer-top20-server-comparison {{
      --series-all-1: var(--viz-series-1);
      --series-top-1: var(--viz-series-2);
      --series-all-8: var(--viz-series-3);
      --series-top-8: var(--viz-series-4);
      position: relative;
      width: 100%;
      color: var(--foreground);
      font-family: var(--font-sans, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif);
      line-height: 1.4;
    }}

    #live-gainer-top20-server-comparison *,
    #live-gainer-top20-server-comparison *::before,
    #live-gainer-top20-server-comparison *::after {{ box-sizing: border-box; }}
    #live-gainer-top20-server-comparison .viz-title {{ margin: 0 0 5px; font-weight: 600; letter-spacing: -0.01em; }}
    #live-gainer-top20-server-comparison .viz-subtitle,
    #live-gainer-top20-server-comparison .viz-chart-subtitle {{ margin: 0 0 12px; color: var(--muted-foreground); font-size: 12px; }}
    #live-gainer-top20-server-comparison .viz-legend {{ display: flex; flex-wrap: wrap; gap: 5px 17px; margin: 0 0 14px; }}
    #live-gainer-top20-server-comparison .viz-legend button {{ display: inline-flex; align-items: center; gap: 7px; min-height: 30px; padding: 3px 0; border: 0; background: transparent; color: inherit; cursor: pointer; font: inherit; font-size: 12px; text-align: left; }}
    #live-gainer-top20-server-comparison .viz-legend button:focus-visible {{ outline: 2px solid var(--ring); outline-offset: 3px; border-radius: 3px; }}
    #live-gainer-top20-server-comparison .viz-legend button[aria-pressed="false"] {{ opacity: 0.4; }}
    #live-gainer-top20-server-comparison .viz-swatch {{ width: 19px; height: 3px; flex: 0 0 auto; border-radius: 2px; }}
    #live-gainer-top20-server-comparison .viz-legend button[data-series="all_live_grace_1"] .viz-swatch {{ background: var(--series-all-1); }}
    #live-gainer-top20-server-comparison .viz-legend button[data-series="gainer_top20_grace_1"] .viz-swatch {{ background: var(--series-top-1); }}
    #live-gainer-top20-server-comparison .viz-legend button[data-series="all_live_grace_8"] .viz-swatch {{ background: var(--series-all-8); }}
    #live-gainer-top20-server-comparison .viz-legend button[data-series="gainer_top20_grace_8"] .viz-swatch {{ background: var(--series-top-8); }}
    #live-gainer-top20-server-comparison .viz-chart {{ margin: 0 0 22px; }}
    #live-gainer-top20-server-comparison .viz-chart-title {{ margin: 0 0 5px; font-weight: 600; }}
    #live-gainer-top20-server-comparison .viz-plot {{ position: relative; width: 100%; min-height: 245px; touch-action: none; }}
    #live-gainer-top20-server-comparison .viz-plot svg {{ display: block; width: 100%; height: auto; min-height: 245px; overflow: visible; }}
    #live-gainer-top20-server-comparison .viz-frame,
    #live-gainer-top20-server-comparison .viz-axis path,
    #live-gainer-top20-server-comparison .viz-axis line {{ stroke: var(--border); fill: none; }}
    #live-gainer-top20-server-comparison .viz-grid-line {{ stroke: var(--border); stroke-dasharray: 2 4; opacity: 0.7; }}
    #live-gainer-top20-server-comparison .viz-axis text,
    #live-gainer-top20-server-comparison .viz-axis-title {{ fill: var(--foreground); font-size: 12px; }}
    #live-gainer-top20-server-comparison .viz-axis-title {{ font-weight: 600; }}
    #live-gainer-top20-server-comparison .viz-line {{ fill: none; stroke-width: 2; stroke-linejoin: round; stroke-linecap: round; pointer-events: none; }}
    #live-gainer-top20-server-comparison .viz-line[data-series="all_live_grace_1"] {{ stroke: var(--series-all-1); }}
    #live-gainer-top20-server-comparison .viz-line[data-series="gainer_top20_grace_1"] {{ stroke: var(--series-top-1); }}
    #live-gainer-top20-server-comparison .viz-line[data-series="all_live_grace_8"] {{ stroke: var(--series-all-8); stroke-dasharray: 6 4; }}
    #live-gainer-top20-server-comparison .viz-line[data-series="gainer_top20_grace_8"] {{ stroke: var(--series-top-8); stroke-dasharray: 6 4; }}
    #live-gainer-top20-server-comparison .viz-guide {{ stroke: var(--muted-foreground); stroke-dasharray: 3 3; opacity: 0.7; pointer-events: none; }}
    #live-gainer-top20-server-comparison .viz-marker {{ stroke: var(--background); stroke-width: 1.5; pointer-events: none; }}
    #live-gainer-top20-server-comparison .viz-marker[data-series="all_live_grace_1"] {{ fill: var(--series-all-1); }}
    #live-gainer-top20-server-comparison .viz-marker[data-series="gainer_top20_grace_1"] {{ fill: var(--series-top-1); }}
    #live-gainer-top20-server-comparison .viz-marker[data-series="all_live_grace_8"] {{ fill: var(--series-all-8); }}
    #live-gainer-top20-server-comparison .viz-marker[data-series="gainer_top20_grace_8"] {{ fill: var(--series-top-8); }}
    #live-gainer-top20-server-comparison .viz-hit {{ fill: transparent; pointer-events: all; cursor: crosshair; }}
    #live-gainer-top20-server-comparison .viz-tooltip {{ position: absolute; z-index: 3; min-width: 205px; max-width: min(330px, calc(100% - 16px)); padding: 8px 10px; border: 1px solid var(--border); border-radius: 4px; background: var(--popover); color: var(--popover-foreground); box-shadow: 0 3px 14px color-mix(in srgb, var(--foreground) 18%, transparent); font-size: 12px; pointer-events: none; }}
    #live-gainer-top20-server-comparison .viz-tooltip[hidden] {{ display: none; }}
    #live-gainer-top20-server-comparison .viz-tooltip-time {{ margin-bottom: 5px; color: var(--muted-foreground); font-variant-numeric: tabular-nums; }}
    #live-gainer-top20-server-comparison .viz-tooltip-row {{ display: flex; align-items: center; justify-content: space-between; gap: 12px; margin: 2px 0; font-variant-numeric: tabular-nums; }}
    #live-gainer-top20-server-comparison .viz-tooltip-key {{ display: inline-flex; align-items: center; gap: 6px; min-width: 0; }}
    #live-gainer-top20-server-comparison .viz-tooltip-dot {{ width: 7px; height: 7px; flex: 0 0 auto; border-radius: 50%; }}
    #live-gainer-top20-server-comparison .viz-tooltip-value {{ white-space: nowrap; }}
    @media (max-width: 520px) {{
      #live-gainer-top20-server-comparison .viz-plot,
      #live-gainer-top20-server-comparison .viz-plot svg {{ min-height: 230px; }}
      #live-gainer-top20-server-comparison .viz-axis text,
      #live-gainer-top20-server-comparison .viz-axis-title {{ font-size: 11px; }}
    }}
  </style>

  <h2 class="viz-title">服务器实盘回放：全部多单 vs 涨幅榜 Top20（自 2026-07-25 21:13 起）</h2>
  <p class="viz-subtitle">四条曲线使用共同的连续行情样本：全部实盘多单 558 笔、Top20 285 笔；实线=退出宽限 1 根 15m，虚线=8 根 15m。跨越超过 15 分钟服务器行情空档的交易已剔除，起点前无交易保持 1,000 USDT。</p>

  <div class="viz-legend" aria-label="曲线显示切换">
    <button type="button" data-series="all_live_grace_1" aria-pressed="true"><span class="viz-swatch" aria-hidden="true"></span><span>全部实盘多单 · 1根</span></button>
    <button type="button" data-series="gainer_top20_grace_1" aria-pressed="true"><span class="viz-swatch" aria-hidden="true"></span><span>涨幅前20 · 1根</span></button>
    <button type="button" data-series="all_live_grace_8" aria-pressed="true"><span class="viz-swatch" aria-hidden="true"></span><span>全部实盘多单 · 8根</span></button>
    <button type="button" data-series="gainer_top20_grace_8" aria-pressed="true"><span class="viz-swatch" aria-hidden="true"></span><span>涨幅前20 · 8根</span></button>
  </div>

  <section class="viz-chart" data-chart="equity" aria-labelledby="server-gainer-equity-title">
    <h3 class="viz-chart-title" id="server-gainer-equity-title">账户权益走势</h3>
    <p class="viz-chart-subtitle">起始权益统一为 1,000 USDT；包含已实现与数据截止时的未实现盈亏</p>
    <div class="viz-plot"><svg role="img" aria-label="四个服务器回放策略的账户权益走势"></svg></div>
  </section>

  <section class="viz-chart" data-chart="margin" aria-labelledby="server-gainer-margin-title">
    <h3 class="viz-chart-title" id="server-gainer-margin-title">资金占用走势</h3>
    <p class="viz-chart-subtitle">持仓名义价值除以 5 倍杠杆；未计手续费占用</p>
    <div class="viz-plot"><svg role="img" aria-label="四个服务器回放策略的保证金占用走势"></svg></div>
  </section>

  <div class="viz-tooltip tooltip" role="tooltip" aria-live="polite" hidden></div>

  <script src="https://cdn.jsdelivr.net/npm/d3@7.9.0/dist/d3.min.js"></script>
  <script>
    (() => {{
      const root = document.getElementById('live-gainer-top20-server-comparison');
      if (!root || !window.d3) return;
      const raw = {compact_data};
      const series = [
        {{ key: 'all_live_grace_1', label: '全部实盘多单 · 1根', color: 'var(--series-all-1)' }},
        {{ key: 'gainer_top20_grace_1', label: '涨幅前20 · 1根', color: 'var(--series-top-1)' }},
        {{ key: 'all_live_grace_8', label: '全部实盘多单 · 8根', color: 'var(--series-all-8)' }},
        {{ key: 'gainer_top20_grace_8', label: '涨幅前20 · 8根', color: 'var(--series-top-8)' }}
      ];
      const values = Object.fromEntries(series.map((s) => [s.key, raw.curves[s.key].map((r) => ({{
        epoch: +r.epoch,
        equity: +r.equity_usdt,
        margin: +r.margin_occupied_usdt,
        open: +r.open_positions
      }}))]));
      const visible = new Set(series.map((s) => s.key));
      const tooltip = root.querySelector('.viz-tooltip');
      const charts = new Map();
      const formatMoney = d3.format(',.2f');
      const formatTime = (date) => d3.utcFormat('%m-%d %H:%M')(new Date(date.getTime() + 8 * 60 * 60 * 1000));

      function interpolateAt(points, epoch, field) {{
        if (!points.length) return null;
        if (points.length === 1) return points[0][field];
        const nearest = d3.bisector((d) => d.epoch).center(points, epoch);
        let right = Math.max(1, Math.min(points.length - 1, nearest));
        if (epoch <= points[0].epoch) right = 1;
        if (epoch >= points[points.length - 1].epoch) right = points.length - 1;
        const left = right - 1;
        const a = points[left];
        const b = points[right];
        if (b.epoch === a.epoch) return a[field];
        const t = Math.max(0, Math.min(1, (epoch - a.epoch) / (b.epoch - a.epoch)));
        return a[field] + (b[field] - a[field]) * t;
      }}

      function tickCount(width) {{ return width < 500 ? 4 : width < 780 ? 6 : 8; }}

      function updateTooltip(chart, event) {{
        const [x] = d3.pointer(event, chart.overlay.node());
        const clampedX = Math.max(0, Math.min(chart.innerWidth, x));
        const epoch = chart.x.invert(clampedX).getTime() / 1000;
        const rootRect = root.getBoundingClientRect();
        const plotRect = chart.plot.node().getBoundingClientRect();
        const pageX = plotRect.left - rootRect.left + chart.margin.left + clampedX;
        const pageY = plotRect.top - rootRect.top + chart.margin.top;
        chart.guide.attr('x1', clampedX).attr('x2', clampedX).style('display', null);
        chart.markers.forEach((marker, key) => {{
          if (!visible.has(key)) {{ marker.style('display', 'none'); return; }}
          const value = interpolateAt(values[key], epoch, chart.field);
          marker.attr('cx', clampedX).attr('cy', chart.y(value)).style('display', null);
        }});
        const rows = series.filter((s) => visible.has(s.key)).map((s) => ({{ ...s, value: interpolateAt(values[s.key], epoch, chart.field) }}));
        tooltip.innerHTML = '<div class="viz-tooltip-time">' + formatTime(new Date(epoch * 1000)) + ' 北京时间</div>' + rows.map((row) =>
          '<div class="viz-tooltip-row"><span class="viz-tooltip-key"><span class="viz-tooltip-dot" style="background:' + row.color + '"></span><span>' + row.label + '</span></span><span class="viz-tooltip-value">' + formatMoney(row.value) + ' USDT</span></div>'
        ).join('');
        tooltip.hidden = false;
        const tooltipWidth = tooltip.offsetWidth || 220;
        const tooltipHeight = tooltip.offsetHeight || 100;
        const maxLeft = Math.max(8, root.clientWidth - tooltipWidth - 8);
        const left = Math.max(8, Math.min(maxLeft, pageX + 12));
        const top = Math.max(8, pageY + 12);
        tooltip.style.left = left + 'px';
        tooltip.style.top = Math.min(top, Math.max(8, root.scrollHeight - tooltipHeight - 8)) + 'px';
      }}

      function hideTooltip(chart) {{
        chart.guide.style('display', 'none');
        chart.markers.forEach((marker) => marker.style('display', 'none'));
        tooltip.innerHTML = '';
        tooltip.hidden = true;
      }}

      function renderChart(chart) {{
        const plot = chart.plot.node();
        const width = Math.max(280, plot.clientWidth || 640);
        const height = width < 500 ? 238 : 258;
        chart.innerWidth = width - chart.margin.left - chart.margin.right;
        chart.innerHeight = height - chart.margin.top - chart.margin.bottom;
        chart.svg.attr('viewBox', '0 0 ' + width + ' ' + height).attr('height', height);
        chart.svg.selectAll('*').remove();
        const allPoints = series.flatMap((s) => values[s.key]);
        chart.x = d3.scaleUtc().domain(d3.extent(allPoints, (d) => new Date(d.epoch * 1000))).range([0, chart.innerWidth]);
        const shownPoints = series.filter((s) => visible.has(s.key)).flatMap((s) => values[s.key]);
        const minValue = d3.min(shownPoints, (d) => d[chart.field]);
        const maxValue = d3.max(shownPoints, (d) => d[chart.field]);
        const baseline = chart.field === 'margin' ? 0 : minValue;
        const span = Math.max(1, maxValue - baseline);
        chart.y = d3.scaleLinear().domain([baseline, maxValue + span * 0.08]).nice().range([chart.innerHeight, 0]);
        const g = chart.svg.append('g').attr('transform', 'translate(' + chart.margin.left + ',' + chart.margin.top + ')');
        g.append('rect').attr('class', 'viz-frame').attr('data-chart-frame', '').attr('width', chart.innerWidth).attr('height', chart.innerHeight);
        const yTicks = chart.y.ticks(width < 500 ? 4 : 6);
        g.append('g').attr('class', 'viz-grid').selectAll('line').data(yTicks).join('line').attr('class', 'viz-grid-line').attr('x1', 0).attr('x2', chart.innerWidth).attr('y1', (d) => chart.y(d)).attr('y2', (d) => chart.y(d));
        const xTickTotal = tickCount(width);
        const xDomain = chart.x.domain();
        const xTickValues = d3.range(xTickTotal).map((i) => new Date(xDomain[0].getTime() + (xDomain[1].getTime() - xDomain[0].getTime()) * i / Math.max(1, xTickTotal - 1)));
        const xAxis = g.append('g').attr('class', 'viz-axis').attr('transform', 'translate(0,' + chart.innerHeight + ')').call(d3.axisBottom(chart.x).tickValues(xTickValues).tickFormat(formatTime));
        xAxis.selectAll('.tick').filter((d, i, nodes) => i === 0).select('text').attr('text-anchor', 'start');
        xAxis.selectAll('.tick').filter((d, i, nodes) => i === nodes.length - 1).select('text').attr('text-anchor', 'end');
        g.append('g').attr('class', 'viz-axis').call(d3.axisLeft(chart.y).ticks(width < 500 ? 4 : 6).tickFormat((d) => formatMoney(d)));
        const line = d3.line().defined((d) => Number.isFinite(d[chart.field])).x((d) => chart.x(new Date(d.epoch * 1000))).y((d) => chart.y(d[chart.field])).curve(d3.curveLinear);
        series.forEach((s) => {{
          if (!visible.has(s.key)) return;
          g.append('path').datum(values[s.key]).attr('class', 'viz-line').attr('data-series', s.key).attr('d', line);
        }});
        chart.guide = g.append('line').attr('class', 'viz-guide').attr('data-chart-hover-guide', '').attr('y1', 0).attr('y2', chart.innerHeight).style('display', 'none');
        chart.markers = new Map();
        series.forEach((s) => {{
          const marker = g.append('circle').attr('class', 'viz-marker').attr('data-chart-hover-marker', '').attr('data-series', s.key).attr('r', 4).style('display', 'none');
          chart.markers.set(s.key, marker);
        }});
        chart.svg.append('text').attr('class', 'viz-axis-title').attr('data-axis', 'y').attr('transform', 'translate(14,' + (chart.margin.top + chart.innerHeight / 2) + ') rotate(-90)').attr('text-anchor', 'middle').text(chart.yTitle);
        chart.svg.append('text').attr('class', 'viz-axis-title').attr('data-axis', 'x').attr('x', chart.margin.left + chart.innerWidth / 2).attr('y', height - 5).attr('text-anchor', 'middle').text('时间（北京时间）');
        chart.overlay = g.append('rect').attr('class', 'viz-hit').attr('width', chart.innerWidth).attr('height', chart.innerHeight)
          .attr('data-chart-hit', '').attr('data-chart-hover-overlay', 'cross-series')
          .on('pointerdown', (event) => {{ event.currentTarget.setPointerCapture?.(event.pointerId); updateTooltip(chart, event); }})
          .on('pointermove', (event) => updateTooltip(chart, event))
          .on('pointerleave pointercancel', () => hideTooltip(chart));
      }}

      root.querySelectorAll('.viz-chart').forEach((section) => {{
        const kind = section.dataset.chart;
        const plot = d3.select(section.querySelector('.viz-plot'));
        const chart = {{ field: kind === 'equity' ? 'equity' : 'margin', yTitle: kind === 'equity' ? '权益（USDT）' : '保证金占用（USDT）', plot, svg: plot.select('svg'), margin: {{ top: 10, right: 18, bottom: 43, left: 66 }} }};
        charts.set(kind, chart);
        renderChart(chart);
        new ResizeObserver(() => renderChart(chart)).observe(section.querySelector('.viz-plot'));
      }});
      root.querySelectorAll('.viz-legend button').forEach((button) => {{
        button.addEventListener('click', () => {{
          const key = button.dataset.series;
          if (visible.has(key)) {{ if (visible.size === 1) return; visible.delete(key); button.setAttribute('aria-pressed', 'false'); }}
          else {{ visible.add(key); button.setAttribute('aria-pressed', 'true'); }}
          charts.forEach((chart) => {{ hideTooltip(chart); renderChart(chart); }});
        }});
      }});
    }})();
  </script>
</div>
'''
    output_path.write_text(html, encoding="utf-8")
    print({"output": str(output_path), "bytes": output_path.stat().st_size})


if __name__ == "__main__":
    main()
