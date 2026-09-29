from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

# ruff: noqa: E501 - literal HTML/JavaScript stays readable in its native layout.

HIGHLIGHTS = {
    "C0_lw2_bw4_mv100bp_ai33_cd30s": "C0 baseline",
    "C2_lw4_bw4_mv50bp_ai33_o12_exbelow33_cd15m": "C2 near miss",
}


def read_points(path: Path) -> list[dict[str, object]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [
            {
                "id": row["candidate_id"],
                "family": row["family"],
                "trainNet": round(float(row["train_net_pnl"]), 4),
                "validationNet": round(float(row["validation_net_pnl"]), 4),
                "trainTail": round(
                    float(row["train_net_without_top_5_trades"]),
                    4,
                ),
                "validationTail": round(
                    float(row["validation_net_without_top_5_trades"]),
                    4,
                ),
                "highlight": HIGHLIGHTS.get(row["candidate_id"]),
            }
            for row in csv.DictReader(handle)
        ]


FRAGMENT = r"""<div id="liquidation-segment-robustness">
  <style>
    #liquidation-segment-robustness {
      position: relative;
      color: var(--foreground);
      width: 100%;
    }
    #liquidation-segment-robustness .lsr-title {
      margin: 0 0 8px;
    }
    #liquidation-segment-robustness .lsr-legend {
      display: flex;
      flex-wrap: wrap;
      gap: 6px 18px;
      margin: 0 0 10px;
    }
    #liquidation-segment-robustness .lsr-legend button {
      appearance: none;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 2px 0;
      border: 0;
      background: transparent;
      color: var(--foreground);
      font: inherit;
      cursor: pointer;
    }
    #liquidation-segment-robustness .lsr-legend button[aria-pressed="false"] {
      color: var(--muted-foreground);
      text-decoration: line-through;
    }
    #liquidation-segment-robustness .lsr-swatch {
      width: 10px;
      height: 10px;
      background: var(--series);
      transform: var(--shape);
    }
    #liquidation-segment-robustness .lsr-plots {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 18px;
    }
    #liquidation-segment-robustness .lsr-panel h3 {
      margin: 0 0 4px;
      font-weight: 500;
    }
    #liquidation-segment-robustness .lsr-plot {
      min-width: 0;
      width: 100%;
    }
    #liquidation-segment-robustness .lsr-plot svg {
      display: block;
      width: 100%;
      height: auto;
      overflow: visible;
    }
    #liquidation-segment-robustness .lsr-plot text {
      fill: var(--foreground);
      font-size: 12px;
      font-weight: 400;
    }
    #liquidation-segment-robustness .lsr-plot .lsr-muted {
      fill: var(--muted-foreground);
    }
    #liquidation-segment-robustness .lsr-tooltip {
      position: absolute;
      z-index: 5;
      display: none;
      max-width: 320px;
      padding: 8px 10px;
      border: 1px solid var(--border);
      border-radius: 6px;
      background: var(--popover);
      color: var(--popover-foreground);
      pointer-events: none;
    }
    #liquidation-segment-robustness .lsr-tooltip strong {
      display: block;
      margin-bottom: 3px;
      font-weight: 500;
      overflow-wrap: anywhere;
    }
    @media (max-width: 680px) {
      #liquidation-segment-robustness .lsr-plots {
        grid-template-columns: 1fr;
      }
    }
  </style>
  <h2 class="lsr-title">Liquidation 参数的分段稳健性</h2>
  <div class="lsr-legend" aria-label="策略入口族图例"></div>
  <div class="lsr-plots">
    <section class="lsr-panel">
      <h3>总净收益：277 组两段同时为正</h3>
      <div class="lsr-plot" data-plot="net"></div>
    </section>
    <section class="lsr-panel">
      <h3>各段剔除前 5 笔盈利后：0 组两段同时为正</h3>
      <div class="lsr-plot" data-plot="tail"></div>
    </section>
  </div>
  <div class="lsr-tooltip" role="tooltip"></div>
  <script src="https://cdn.jsdelivr.net/npm/d3@7.9.0/dist/d3.min.js"></script>
  <script>
    (() => {
      const root = document.getElementById("liquidation-segment-robustness");
      const data = __DATA__;
      const families = [
        { id: "C0", label: "C0 原顺势", color: "var(--viz-series-1)", symbol: d3.symbolCircle },
        { id: "C1", label: "C1 延迟顺势", color: "var(--viz-series-2)", symbol: d3.symbolSquare },
        { id: "C2", label: "C2 失败突破反向", color: "var(--viz-series-3)", symbol: d3.symbolTriangle }
      ];
      const familyById = new Map(families.map(d => [d.id, d]));
      const active = new Set(families.map(d => d.id));
      const tooltip = root.querySelector(".lsr-tooltip");
      const money = d3.format("+,.1f");

      const legend = d3.select(root.querySelector(".lsr-legend"));
      const buttons = legend.selectAll("button")
        .data(families)
        .join("button")
        .attr("type", "button")
        .attr("aria-pressed", "true")
        .on("click", function(event, family) {
          if (active.has(family.id) && active.size > 1) active.delete(family.id);
          else active.add(family.id);
          d3.select(this).attr("aria-pressed", active.has(family.id) ? "true" : "false");
          drawAll();
        });
      buttons.append("span")
        .attr("class", "lsr-swatch")
        .style("--series", d => d.color)
        .style("--shape", d => d.id === "C1" ? "rotate(0deg)" : d.id === "C2" ? "rotate(45deg)" : "rotate(0deg)")
        .style("border-radius", d => d.id === "C0" ? "50%" : "0");
      buttons.append("span").text(d => d.label);

      function paddedDomain(values) {
        const extent = d3.extent(values);
        let low = Math.min(extent[0], 0);
        let high = Math.max(extent[1], 0);
        const span = Math.max(high - low, 1);
        low -= span * 0.06;
        high += span * 0.06;
        return [low, high];
      }

      function showTooltip(event, point, xLabel, yLabel) {
        tooltip.innerHTML = `<strong>${point.id}</strong>${point.family} · ${xLabel}: ${money(point.x)} USDT<br>${yLabel}: ${money(point.y)} USDT`;
        tooltip.style.display = "block";
        const bounds = root.getBoundingClientRect();
        const tipWidth = tooltip.offsetWidth;
        const left = Math.min(event.clientX - bounds.left + 12, bounds.width - tipWidth - 4);
        tooltip.style.left = `${Math.max(4, left)}px`;
        tooltip.style.top = `${event.clientY - bounds.top + 12}px`;
      }

      function hideTooltip() {
        tooltip.style.display = "none";
      }

      function draw(container, config) {
        const visible = data
          .filter(d => active.has(d.family))
          .map(d => ({ ...d, x: d[config.x], y: d[config.y] }));
        const width = Math.max(320, Math.round(container.getBoundingClientRect().width));
        const height = width < 500 ? 390 : 430;
        const margin = { top: 26, right: 18, bottom: 66, left: width < 500 ? 72 : 82 };
        const innerWidth = width - margin.left - margin.right;
        const innerHeight = height - margin.top - margin.bottom;
        const x = d3.scaleLinear()
          .domain(paddedDomain(visible.map(d => d.x)))
          .range([margin.left, margin.left + innerWidth]);
        const y = d3.scaleLinear()
          .domain(paddedDomain(visible.map(d => d.y)))
          .nice()
          .range([margin.top + innerHeight, margin.top]);
        const svg = d3.select(container)
          .selectAll("svg")
          .data([null])
          .join("svg")
          .attr("viewBox", `0 0 ${width} ${height}`)
          .attr("role", "img")
          .attr("aria-label", config.aria);
        svg.selectAll("*").remove();
        svg.append("title").text(config.aria);
        svg.append("desc").text(config.desc);
        svg.append("rect")
          .attr("data-chart-frame", "")
          .attr("x", margin.left)
          .attr("y", margin.top)
          .attr("width", innerWidth)
          .attr("height", innerHeight)
          .attr("fill", "none")
          .attr("stroke", "var(--border)");
        svg.append("line")
          .attr("x1", x(0)).attr("x2", x(0))
          .attr("y1", margin.top).attr("y2", margin.top + innerHeight)
          .attr("stroke", "var(--border)");
        svg.append("line")
          .attr("x1", margin.left).attr("x2", margin.left + innerWidth)
          .attr("y1", y(0)).attr("y2", y(0))
          .attr("stroke", "var(--border)");
        const tickCount = width < 500 ? 4 : 6;
        svg.append("g")
          .attr("transform", `translate(0,${margin.top + innerHeight})`)
          .call(d3.axisBottom(x).ticks(tickCount).tickFormat(d3.format("~s")))
          .call(g => g.select(".domain").remove())
          .call(g => g.selectAll("line").attr("stroke", "var(--border)"))
          .call(g => g.selectAll("text").attr("fill", "var(--foreground)"));
        svg.append("g")
          .attr("transform", `translate(${margin.left},0)`)
          .call(d3.axisLeft(y).ticks(tickCount).tickFormat(d3.format("~s")))
          .call(g => g.select(".domain").remove())
          .call(g => g.selectAll("line").attr("stroke", "var(--border)"))
          .call(g => g.selectAll("text").attr("fill", "var(--foreground)"));
        svg.append("text")
          .attr("class", "axis-title")
          .attr("data-axis", "x")
          .attr("x", margin.left + innerWidth / 2)
          .attr("y", height - 16)
          .attr("text-anchor", "middle")
          .text(config.xLabel);
        svg.append("text")
          .attr("class", "axis-title")
          .attr("data-axis", "y")
          .attr("transform", `translate(18,${margin.top + innerHeight / 2}) rotate(-90)`)
          .attr("text-anchor", "middle")
          .text(config.yLabel);

        svg.append("g")
          .selectAll("path")
          .data(visible)
          .join("path")
          .attr("d", d => d3.symbol().type(familyById.get(d.family).symbol).size(d.highlight ? 74 : 24)())
          .attr("transform", d => `translate(${x(d.x)},${y(d.y)})`)
          .attr("fill", d => familyById.get(d.family).color)
          .attr("fill-opacity", d => d.highlight ? 1 : 0.55)
          .attr("stroke", d => d.highlight ? "var(--foreground)" : "none")
          .attr("stroke-width", d => d.highlight ? 1.6 : 0)
          .attr("pointer-events", "none");

        const positive = visible.filter(d => d.x > 0 && d.y > 0).length;
        svg.append("text")
          .attr("class", "lsr-muted")
          .attr("x", margin.left + innerWidth - 7)
          .attr("y", margin.top + 16)
          .attr("text-anchor", "end")
          .text(`右上象限 ${positive} / ${visible.length}`);

        const overlay = svg.append("rect")
          .attr("data-chart-hit", "")
          .attr("x", margin.left)
          .attr("y", margin.top)
          .attr("width", innerWidth)
          .attr("height", innerHeight)
          .attr("fill", "transparent")
          .style("cursor", "crosshair");
        const hover = svg.append("path")
          .attr("fill", "none")
          .attr("stroke", "var(--foreground)")
          .attr("stroke-width", 1.5)
          .style("display", "none")
          .attr("pointer-events", "none");
        overlay.on("pointermove", event => {
          const [mx, my] = d3.pointer(event, svg.node());
          let nearest = null;
          let nearestDistance = Infinity;
          for (const point of visible) {
            const distance = Math.hypot(x(point.x) - mx, y(point.y) - my);
            if (distance < nearestDistance) {
              nearest = point;
              nearestDistance = distance;
            }
          }
          if (!nearest || nearestDistance > 16) {
            hover.style("display", "none");
            hideTooltip();
            return;
          }
          hover
            .style("display", null)
            .attr("d", d3.symbol().type(familyById.get(nearest.family).symbol).size(150)())
            .attr("transform", `translate(${x(nearest.x)},${y(nearest.y)})`);
          showTooltip(event, nearest, "训练段", "验证段");
        }).on("pointerleave", () => {
          hover.style("display", "none");
          hideTooltip();
        });
      }

      function drawAll() {
        draw(root.querySelector('[data-plot="net"]'), {
          x: "trainNet",
          y: "validationNet",
          xLabel: "训练段净收益（USDT；单笔名义 100）",
          yLabel: "验证段净收益（USDT）",
          aria: "训练段与验证段总净收益散点图",
          desc: "每个点是一组参数。颜色和形状区分三类入口。"
        });
        draw(root.querySelector('[data-plot="tail"]'), {
          x: "trainTail",
          y: "validationTail",
          xLabel: "训练段剔除前 5 笔后净收益（USDT）",
          yLabel: "验证段剔除前 5 笔后净收益（USDT）",
          aria: "剔除每段前五笔盈利后的训练与验证收益散点图",
          desc: "没有参数位于训练和验证均为正的右上象限。"
        });
      }

      const observer = new ResizeObserver(() => requestAnimationFrame(drawAll));
      root.querySelectorAll(".lsr-plot").forEach(node => observer.observe(node));
      drawAll();
    })();
  </script>
</div>
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--metrics", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    payload = json.dumps(read_points(args.metrics), ensure_ascii=False, separators=(",", ":"))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(FRAGMENT.replace("__DATA__", payload), encoding="utf-8")
    print(args.output)


if __name__ == "__main__":
    main()
