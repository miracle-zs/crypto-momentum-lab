import {
  DISPLAY_TIME_ZONE_LABEL,
} from "./dashboard-config.js";
import {
  dateOnly,
  dayTime,
  esc,
  fullDateTime,
  money,
  signedMoney,
  timeOnly,
  yearMonth,
} from "./dashboard-formatters.js";

const CHART_PAYLOADS = new Map();
const CHART_INSTANCES = new Map();
const WIRED_ROOTS = new WeakSet();
const ROOT_LIFECYCLES = new WeakMap();

const FALLBACK_COLORS = {
  up: "#34d399",
  down: "#fb7185",
  brand: "#7aa2f7",
  faint: "#738099",
  surface: "#1d2431",
  line: "#273043",
  lineStrong: "#3a4960",
  text: "#e9edf4",
  muted: "#a5b0c3",
  live: "#67e8f9",
  seriesFixed: "#7aa2f7",
  seriesCandle: "#34d399",
  seriesAmber: "#f5b755",
  seriesViolet: "#c084fc",
  seriesCyan: "#22d3ee",
  seriesCoral: "#fb7185",
  seriesLime: "#a3e635",
  seriesSky: "#38bdf8",
};

export function registerChartPayload(id, payload) {
  CHART_PAYLOADS.set(String(id), payload);
  return String(id);
}

export function getChartPayload(id) {
  return CHART_PAYLOADS.get(String(id)) || null;
}

function cssToken(name, fallback) {
  if (typeof document === "undefined" || typeof getComputedStyle !== "function") return fallback;
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim() || fallback;
}

function chartColors() {
  return {
    up: cssToken("--up", FALLBACK_COLORS.up),
    down: cssToken("--down", FALLBACK_COLORS.down),
    brand: cssToken("--brand", FALLBACK_COLORS.brand),
    faint: cssToken("--faint", FALLBACK_COLORS.faint),
    surface: cssToken("--surface-3", FALLBACK_COLORS.surface),
    line: cssToken("--line", FALLBACK_COLORS.line),
    lineStrong: cssToken("--line-strong", FALLBACK_COLORS.lineStrong),
    text: cssToken("--text", FALLBACK_COLORS.text),
    muted: cssToken("--muted", FALLBACK_COLORS.muted),
    live: cssToken("--series-live", FALLBACK_COLORS.live),
    seriesFixed: cssToken("--series-fixed", FALLBACK_COLORS.seriesFixed),
    seriesCandle: cssToken("--series-candle", FALLBACK_COLORS.seriesCandle),
    seriesAmber: cssToken("--series-amber", FALLBACK_COLORS.seriesAmber),
    seriesViolet: cssToken("--series-violet", FALLBACK_COLORS.seriesViolet),
    seriesCyan: cssToken("--series-cyan", FALLBACK_COLORS.seriesCyan),
    seriesCoral: cssToken("--series-coral", FALLBACK_COLORS.seriesCoral),
    seriesLime: cssToken("--series-lime", FALLBACK_COLORS.seriesLime),
    seriesSky: cssToken("--series-sky", FALLBACK_COLORS.seriesSky),
  };
}

function resolveSeriesColor(color, colors) {
  const token = String(color || "").match(/^var\((--[^)]+)\)$/)?.[1];
  if (!token) return color || colors.muted;
  return cssToken(token, {
    "--series-fixed": colors.seriesFixed,
    "--series-candle": colors.seriesCandle,
    "--series-live": colors.live,
    "--series-amber": colors.seriesAmber,
    "--series-violet": colors.seriesViolet,
    "--series-cyan": colors.seriesCyan,
    "--series-coral": colors.seriesCoral,
    "--series-lime": colors.seriesLime,
    "--series-sky": colors.seriesSky,
  }[token] || colors.muted);
}

function axisTime(value, payload) {
  const span = Math.max(
    0,
    Number(payload.domainEnd ?? payload.endAt)
      - Number(payload.domainStart ?? payload.startAt),
  );
  if (span <= 2 * 24 * 60 * 60 * 1000) return timeOnly(Number(value));
  if (span <= 45 * 24 * 60 * 60 * 1000) return dateOnly(Number(value));
  return yearMonth(Number(value));
}

function baseAxis(colors) {
  return {
    axisLine: { lineStyle: { color: colors.lineStrong } },
    axisTick: { show: false },
    axisLabel: {
      color: colors.faint,
      fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace",
      fontSize: 10,
      hideOverlap: true,
    },
  };
}

function baseTooltip(colors) {
  return {
    trigger: "axis",
    className: "echarts-tooltip",
    axisPointer: {
      type: "cross",
      lineStyle: { color: colors.brand, type: "dashed", width: 1 },
      crossStyle: { color: colors.brand, type: "dashed", width: 1 },
    },
    backgroundColor: colors.surface,
    borderColor: colors.lineStrong,
    borderWidth: 1,
    padding: [8, 10],
    textStyle: {
      color: colors.text,
      fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace",
      fontSize: 10,
    },
    extraCssText: "box-shadow:0 7px 22px rgba(0,0,0,.28);",
  };
}

function baseOption(payload, colors) {
  return {
    animation: false,
    aria: {
      enabled: true,
      description: payload.title,
    },
    grid: {
      left: 68,
      right: 26,
      top: 16,
      bottom: 40,
      containLabel: true,
    },
    xAxis: {
      ...baseAxis(colors),
      type: "time",
      min: payload.domainStart ?? payload.startAt,
      max: payload.domainEnd ?? payload.endAt,
      axisLabel: {
        ...baseAxis(colors).axisLabel,
        formatter: (value) => axisTime(value, payload),
      },
      splitLine: { show: false },
    },
    yAxis: {
      ...baseAxis(colors),
      type: "value",
      min: payload.min,
      max: payload.max,
      splitNumber: 4,
      axisLabel: {
        ...baseAxis(colors).axisLabel,
        formatter: (value) => money(value),
      },
      splitLine: {
        show: true,
        lineStyle: { color: colors.line, type: "dashed", opacity: 0.8 },
      },
    },
    tooltip: baseTooltip(colors),
  };
}

function tooltipTime(params, payload) {
  const first = Array.isArray(params) ? params[0] : params;
  const timestamp = first?.axisValue ?? first?.value?.[0];
  if (!Number.isFinite(Number(timestamp))) return "暂无时间";
  const span = Math.max(
    0,
    Number(payload.domainEnd ?? payload.endAt)
      - Number(payload.domainStart ?? payload.startAt),
  );
  const formatted = span > 180 * 24 * 60 * 60 * 1000
    ? fullDateTime(Number(timestamp))
    : dayTime(Number(timestamp));
  return `${formatted} ${DISPLAY_TIME_ZONE_LABEL}`;
}

function tooltipValue(param) {
  const value = Array.isArray(param?.value) ? param.value[1] : param?.value;
  return Number(value);
}

function equityOption(payload, colors) {
  const lineColor = payload.lastUp ? colors.up : colors.down;
  const option = baseOption(payload, colors);
  option.tooltip.formatter = (params) => {
    const first = Array.isArray(params) ? params[0] : params;
    return `<div>${esc(payload.title)}</div><div>${esc(tooltipTime(params, payload))}</div><b style="color:${esc(lineColor)}">${esc(money(tooltipValue(first)))}</b>`;
  };
  option.series = [{
    name: payload.title,
    type: "line",
    data: payload.points.map((point) => [point.atMs, point.equity]),
    showSymbol: false,
    symbol: "circle",
    symbolSize: 7,
    smooth: false,
    lineStyle: { color: lineColor, width: 2 },
    itemStyle: { color: lineColor },
    areaStyle: { color: lineColor, opacity: 0.16 },
    emphasis: { focus: "series", scale: true },
    markLine: {
      silent: true,
      symbol: ["none", "none"],
      label: { show: false },
      lineStyle: { color: colors.faint, type: "dashed", width: 1, opacity: 0.7 },
      data: [{ yAxis: payload.baseline }],
    },
  }];
  return option;
}

function comparisonOption(payload, colors) {
  const option = baseOption(payload, colors);
  option.grid.left = 62;
  option.grid.right = 18;
  option.grid.bottom = 40;
  option.tooltip.formatter = (params) => {
    const entries = Array.isArray(params) ? params : [params];
    const rows = entries.map((param) => {
      const value = tooltipValue(param);
      const color = param.color || colors.muted;
      return `<div><span style="color:${esc(color)}">●</span> ${esc(param.seriesName)} <b>${esc(signedMoney(value))}</b></div>`;
    }).join("");
    return `<div>${esc(tooltipTime(params, payload))}</div>${rows}`;
  };
  option.series = payload.series.map((series, index) => {
    const seriesColor = resolveSeriesColor(series.color, colors);
    return ({
    name: series.label,
    type: "line",
    data: payload.points.map((point, pointIndex) => [point.at, series.values[pointIndex]]),
    showSymbol: false,
    symbol: "circle",
    symbolSize: series.isLive ? 7 : 6,
    smooth: false,
    lineStyle: { color: seriesColor, width: series.isLive ? 2.5 : 1.8 },
    itemStyle: { color: seriesColor },
    emphasis: { focus: "series", scale: true },
    ...(index === 0 ? {
      markLine: {
        silent: true,
        symbol: ["none", "none"],
        label: { show: false },
        lineStyle: { color: colors.faint, type: "dashed", width: 1, opacity: 0.7 },
        data: [{ yAxis: 0 }],
      },
    } : {}),
    });
  });
  return option;
}

export function buildChartOption(payload, colors = chartColors()) {
  return payload?.kind === "comparison"
    ? comparisonOption(payload, colors)
    : equityOption(payload, colors);
}

function rootContains(root, element) {
  if (!root || !element) return false;
  return typeof root.contains === "function" ? root.contains(element) : false;
}

function documentFor(root) {
  return root?.nodeType === 9 ? root : root?.ownerDocument || document;
}

function echartForDocument(doc) {
  return doc?.defaultView?.echarts || globalThis.echarts || null;
}

function chartShells(root) {
  const shells = [];
  if (root?.matches?.("[data-echart-chart]")) shells.push(root);
  if (root?.querySelectorAll) shells.push(...root.querySelectorAll("[data-echart-chart]"));
  return shells;
}

function disposeChart(shell) {
  const chart = CHART_INSTANCES.get(shell);
  if (!chart) return;
  chart.dispose();
  CHART_INSTANCES.delete(shell);
  shell.removeAttribute("data-echart-mounted");
}

function mountChart(shell, doc) {
  if (CHART_INSTANCES.has(shell)) return;
  const chartLibrary = echartForDocument(doc);
  const surface = shell.querySelector(".echart-surface");
  const payload = getChartPayload(shell.dataset.echartId);
  if (!chartLibrary || !surface || !payload) {
    shell.dataset.echartState = "waiting";
    return;
  }
  const chart = chartLibrary.init(surface, null, { renderer: "svg" });
  chart.setOption(buildChartOption(payload), { notMerge: true, lazyUpdate: false });
  CHART_INSTANCES.set(shell, chart);
  shell.dataset.echartMounted = "true";
  shell.removeAttribute("data-echart-state");
}

function showChartPoint(shell, index) {
  const chart = CHART_INSTANCES.get(shell);
  const payload = getChartPayload(shell.dataset.echartId);
  if (!chart || !payload?.points?.length) return;
  const dataIndex = Math.min(Math.max(index, 0), payload.points.length - 1);
  chart.dispatchAction({ type: "showTip", seriesIndex: 0, dataIndex });
  shell.dataset.echartPointIndex = String(dataIndex);
}

function hideChartPoint(shell) {
  const chart = CHART_INSTANCES.get(shell);
  if (chart) chart.dispatchAction({ type: "hideTip" });
}

function wireKeyboard(root) {
  root.addEventListener("keydown", (event) => {
    const shell = event.target?.closest?.("[data-echart-chart]");
    if (!shell || !rootContains(root, shell)) return;
    if (event.key === "Escape") {
      hideChartPoint(shell);
      return;
    }
    const payload = getChartPayload(shell.dataset.echartId);
    const pointCount = payload?.points?.length || 0;
    if (!pointCount) return;
    const current = Number(shell.dataset.echartPointIndex);
    const index = Number.isInteger(current) && current >= 0 ? current : 0;
    let next = index;
    if (event.key === "ArrowRight") next = Math.min(index + 1, pointCount - 1);
    else if (event.key === "ArrowLeft") next = Math.max(index - 1, 0);
    else if (event.key === "Home") next = 0;
    else if (event.key === "End") next = pointCount - 1;
    else return;
    event.preventDefault();
    showChartPoint(shell, next);
  });

  root.addEventListener("focusout", (event) => {
    const shell = event.target?.closest?.("[data-echart-chart]");
    if (!shell || (event.relatedTarget && shell.contains(event.relatedTarget))) return;
    hideChartPoint(shell);
  });
}

export function wireEcharts(root = document) {
  if (!root || typeof root.addEventListener !== "function" || WIRED_ROOTS.has(root)) return;
  WIRED_ROOTS.add(root);
  const doc = documentFor(root);
  const lifecycle = {
    observer: null,
    resize: () => {
      CHART_INSTANCES.forEach((chart) => chart.resize());
    },
  };
  ROOT_LIFECYCLES.set(root, lifecycle);
  wireKeyboard(root);

  const scan = () => {
    chartShells(root).forEach((shell) => mountChart(shell, doc));
    CHART_INSTANCES.forEach((_chart, shell) => {
      if (!rootContains(root, shell)) disposeChart(shell);
    });
  };
  scan();

  const observationTarget = root.nodeType === 9 ? root.body : root;
  if (typeof MutationObserver === "function" && observationTarget) {
    lifecycle.observer = new MutationObserver(scan);
    lifecycle.observer.observe(observationTarget, { childList: true, subtree: true });
  }
  const view = doc?.defaultView;
  view?.addEventListener("resize", lifecycle.resize, { passive: true });
}
