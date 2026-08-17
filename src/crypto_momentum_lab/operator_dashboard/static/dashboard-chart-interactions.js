const WIRED_ROOTS = new WeakSet();

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), Math.max(min, max));
}

function elementTarget(target) {
  return target && typeof target.closest === "function" ? target : null;
}

function chartForTarget(target, root) {
  const element = elementTarget(target);
  if (!element) return null;
  const chart = element.closest("[data-chart-interactive]");
  return chart && root.contains(chart) ? chart : null;
}

function pointForTarget(target, chart) {
  const element = elementTarget(target);
  if (!element) return null;
  const point = element.closest("[data-chart-point]");
  return point && chart.contains(point) ? point : null;
}

function setCrosshair(chart, point) {
  const crosshair = chart.querySelector("[data-chart-crosshair]");
  if (!crosshair) return;
  const x = point?.dataset.chartX;
  if (!x) {
    crosshair.setAttribute("hidden", "");
    return;
  }
  crosshair.setAttribute("x1", x);
  crosshair.setAttribute("x2", x);
  crosshair.removeAttribute("hidden");
}

function tooltipPosition(chart, point, tooltip) {
  const svg = chart.querySelector("svg");
  if (!svg) return;
  const svgRect = svg.getBoundingClientRect();
  const chartRect = chart.getBoundingClientRect();
  const width = Number(chart.dataset.chartWidth) || 1000;
  const height = Number(chart.dataset.chartHeight) || 280;
  const scaleX = svgRect.width / width || 1;
  const scaleY = svgRect.height / height || 1;
  const pointX = (Number(point.dataset.chartX) || 0) * scaleX;
  const pointY = (Number(point.dataset.chartY) || 0) * scaleY;
  const chartWidth = chart.clientWidth || chartRect.width || svgRect.width;
  const chartHeight = chart.clientHeight || chartRect.height || svgRect.height;
  const x = svgRect.left - chartRect.left + pointX;
  const y = svgRect.top - chartRect.top + pointY - 8;
  const halfTooltip = Math.max(48, (tooltip.offsetWidth || 160) / 2);
  tooltip.style.left = `${clamp(x, halfTooltip + 6, chartWidth - halfTooltip - 6)}px`;
  tooltip.style.top = `${clamp(y, 28, chartHeight - 8)}px`;
}

export function showChartPoint(chart, point) {
  const tooltip = chart.querySelector("[data-chart-tooltip]");
  if (!tooltip || !point) return;
  tooltip.textContent = point.dataset.chartReadout || "暂无数据";
  tooltip.hidden = false;
  tooltipPosition(chart, point, tooltip);
  setCrosshair(chart, point);
  chart.dataset.chartPointIndex = String(
    [...chart.querySelectorAll("[data-chart-point]")].indexOf(point),
  );
}

export function hideChartPoint(chart) {
  const tooltip = chart.querySelector("[data-chart-tooltip]");
  if (tooltip) {
    tooltip.hidden = true;
    tooltip.textContent = "";
  }
  const crosshair = chart.querySelector("[data-chart-crosshair]");
  if (crosshair) crosshair.setAttribute("hidden", "");
}

function handlePointerOver(event, root) {
  const chart = chartForTarget(event.target, root);
  const point = chart && pointForTarget(event.target, chart);
  if (chart && point) showChartPoint(chart, point);
}

function handlePointerMove(event, root) {
  const chart = chartForTarget(event.target, root);
  const point = chart && pointForTarget(event.target, chart);
  if (chart && point) showChartPoint(chart, point);
}

function handlePointerOut(event, root) {
  const chart = chartForTarget(event.target, root);
  if (!chart || (event.relatedTarget && chart.contains(event.relatedTarget))) return;
  hideChartPoint(chart);
}

function handleFocusOut(event, root) {
  const chart = chartForTarget(event.target, root);
  if (!chart || (event.relatedTarget && chart.contains(event.relatedTarget))) return;
  hideChartPoint(chart);
}

function handleKeyDown(event, root) {
  const chart = chartForTarget(event.target, root);
  if (!chart) return;
  const points = [...chart.querySelectorAll("[data-chart-point]")];
  if (!points.length) return;
  const current = Number(chart.dataset.chartPointIndex);
  const index = Number.isInteger(current) && current >= 0 ? current : 0;
  let next = index;
  if (event.key === "ArrowRight") next = Math.min(index + 1, points.length - 1);
  else if (event.key === "ArrowLeft") next = Math.max(index - 1, 0);
  else if (event.key === "Home") next = 0;
  else if (event.key === "End") next = points.length - 1;
  else return;
  event.preventDefault();
  showChartPoint(chart, points[next]);
}

export function wireChartInteractions(root = document) {
  if (!root || typeof root.addEventListener !== "function" || WIRED_ROOTS.has(root)) return;
  WIRED_ROOTS.add(root);
  root.addEventListener("pointerover", (event) => handlePointerOver(event, root));
  root.addEventListener("pointermove", (event) => handlePointerMove(event, root));
  root.addEventListener("pointerout", (event) => handlePointerOut(event, root));
  root.addEventListener("focusout", (event) => handleFocusOut(event, root));
  root.addEventListener("keydown", (event) => handleKeyDown(event, root));
}
