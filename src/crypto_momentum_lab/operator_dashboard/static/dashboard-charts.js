import {
  DAILY_EQUITY_ANCHOR_HOUR,
  DEFAULT_EQUITY_BUCKET_SECONDS,
  DISPLAY_TIME_ZONE,
} from "./dashboard-config.js";
import {
  asNumber,
  esc,
  money,
  signedMoney,
  signedPercent,
} from "./dashboard-formatters.js";
import { registerChartPayload } from "./dashboard-chart-engine.js";
import { emptyBox } from "./dashboard-ui.js";

function equityValues(rows) {
  return (rows || [])
    .map((row) => asNumber(typeof row === "object" && row !== null ? row.equity : row))
    .filter((value) => value != null);
}

export function maxDrawdown(rows) {
  const values = equityValues(rows);
  if (!values.length) return null;
  let peak = values[0];
  let drawdown = 0;
  for (const value of values) {
    peak = Math.max(peak, value);
    drawdown = Math.min(drawdown, value - peak);
  }
  return drawdown;
}

export function equityWindowMetrics(rows) {
  const values = equityValues(rows);
  const baseline = values[0] ?? null;
  const latest = values.at(-1) ?? null;
  return {
    baseline,
    latest,
    delta: baseline == null || latest == null ? null : latest - baseline,
    maxDrawdown: maxDrawdown(values),
  };
}

const DAILY_ANCHOR_FORMATTER = new Intl.DateTimeFormat("en-CA", {
  timeZone: DISPLAY_TIME_ZONE,
  calendar: "gregory",
  numberingSystem: "latn",
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hourCycle: "h23",
});

function dailyEquityAnchor(referenceMs) {
  const parts = Object.fromEntries(
    DAILY_ANCHOR_FORMATTER.formatToParts(new Date(referenceMs))
      .filter((part) => part.type !== "literal")
      .map((part) => [part.type, Number(part.value)]),
  );
  const observedWallMs = Date.UTC(
    parts.year,
    parts.month - 1,
    parts.day,
    parts.hour,
    parts.minute,
    parts.second,
  );
  const referenceSecondMs = Math.floor(referenceMs / 1000) * 1000;
  const offsetMs = observedWallMs - referenceSecondMs;
  const anchorWallMs = Date.UTC(
    parts.year,
    parts.month - 1,
    parts.day,
    DAILY_EQUITY_ANCHOR_HOUR,
  );
  return anchorWallMs - offsetMs;
}

function dailyAnchorAtOrAfter(referenceMs) {
  const anchorAt = dailyEquityAnchor(referenceMs);
  return anchorAt >= referenceMs
    ? anchorAt
    : dailyEquityAnchor(referenceMs + 24 * 60 * 60 * 1000);
}

export function accountWindowDelta(account) {
  const values = (account.equity_curve || [])
    .map((row) => asNumber(row.equity))
    .filter((value) => value != null);
  if (values.length < 2) return null;
  return values.at(-1) - values[0];
}

export function equityChart(rows, chartId = "eq", windowStart = null, windowEnd = null) {
  const points = (rows || [])
    .map((row) => ({
      at: row.observed_at,
      atMs: new Date(row.observed_at).getTime(),
      equity: asNumber(row.equity),
    }))
    .filter((point) => point.equity != null && Number.isFinite(point.atMs));
  if (points.length < 2) return emptyBox("等待权益快照", "图表按 UTC 6 分钟桶采样");
  const values = points.map((point) => point.equity);
  let min = Math.min(...values);
  let max = Math.max(...values);
  const span = Math.max(max - min, 0.01);
  min -= span * 0.08;
  max += span * 0.08;
  const requestedStart = windowStart ? new Date(windowStart).getTime() : Number.NaN;
  const requestedEnd = windowEnd ? new Date(windowEnd).getTime() : Number.NaN;
  const domainStart = Number.isFinite(requestedStart) ? requestedStart : points[0].atMs;
  const domainEnd = Number.isFinite(requestedEnd) && requestedEnd > domainStart
    ? requestedEnd : points.at(-1).atMs;
  const lastValue = values.at(-1);
  const lastUp = lastValue >= values[0];
  const windowMetrics = equityWindowMetrics(values);
  const changeClass = windowMetrics.delta == null || windowMetrics.delta === 0
    ? ""
    : windowMetrics.delta > 0 ? "pos" : "neg";
  const drawdownClass = windowMetrics.maxDrawdown < 0 ? "neg" : "";
  const chartMetrics = `<div class="chart-metrics" aria-label="权益窗口指标">
    <span><small>窗口基线</small><b class="num">${esc(money(windowMetrics.baseline))}</b></span>
    <span><small>窗口变化</small><b class="num ${changeClass}">${esc(signedMoney(windowMetrics.delta))}</b></span>
    <span><small>最大回撤</small><b class="num ${drawdownClass}">${esc(signedMoney(windowMetrics.maxDrawdown))}</b></span>
  </div>`;
  registerChartPayload(chartId, {
    kind: "equity",
    title: "账户权益",
    points,
    domainStart,
    domainEnd,
    min,
    max,
    baseline: windowMetrics.baseline,
    delta: windowMetrics.delta,
    maxDrawdown: windowMetrics.maxDrawdown,
    lastUp,
  });
  return `<div class="equity-chart echart-shell" data-echart-chart data-echart-kind="equity" data-echart-id="${esc(chartId)}" tabindex="0" role="group" aria-label="账户权益曲线；使用左右方向键查看数据点">
    ${chartMetrics}
    <div class="echart-surface" aria-hidden="true"></div>
  </div>`;
}

const LIVE_ACCOUNT_METRIC_COLORS = [
  "var(--series-cyan)",
  "var(--series-lime)",
  "var(--series-amber)",
  "var(--series-violet)",
];

function liveAccountMetricBucketMap(
  account,
  metricKey,
  intervalSeconds,
  bucketOriginMs = Number.NaN,
) {
  const intervalMs = Math.max(1, intervalSeconds) * 1000;
  const buckets = new Map();
  for (const row of account.metrics_curve || []) {
    const observedAt = new Date(row.observed_at || "").getTime();
    const value = asNumber(row[metricKey]);
    if (!Number.isFinite(observedAt) || value == null) continue;
    const bucket = Number.isFinite(bucketOriginMs)
      ? bucketOriginMs
        + Math.floor((observedAt - bucketOriginMs) / intervalMs) * intervalMs
      : Math.floor(observedAt / intervalMs) * intervalMs;
    buckets.set(bucket, value);
  }
  return buckets;
}

function metricValueFormat(metricKey) {
  return metricKey.endsWith("_ratio")
    ? metricKey === "margin_occupancy_ratio" ? "percent" : "signed-percent"
    : "signed-money";
}

export function liveAccountMetricModel(
  accounts,
  metricKey,
  intervalSeconds = DEFAULT_EQUITY_BUCKET_SECONDS,
  windowStart = null,
  windowEnd = null,
) {
  const normalizedAccounts = (accounts || []).filter(
    (account) => Array.isArray(account.metrics_curve),
  );
  if (!normalizedAccounts.length) return null;
  const requestedStart = windowStart
    ? new Date(windowStart).getTime()
    : Number.NaN;
  const requestedEnd = windowEnd
    ? new Date(windowEnd).getTime()
    : Number.NaN;
  const dailyAnchor = Number.isFinite(requestedStart)
    ? dailyAnchorAtOrAfter(requestedStart)
    : Number.NaN;
  const anchoredWindow = Number.isFinite(dailyAnchor)
    && (!Number.isFinite(requestedEnd) || dailyAnchor <= requestedEnd);
  const chartStart = anchoredWindow ? dailyAnchor : requestedStart;
  const equityMetric = [
    "equity", "equity_change_ratio", "drawdown", "drawdown_ratio",
  ].includes(metricKey);
  const maps = normalizedAccounts.map((account) => (
    liveAccountMetricBucketMap(
      account, equityMetric ? "equity" : metricKey, intervalSeconds, chartStart,
    )
  ));
  const buckets = [...new Set(maps.flatMap((map) => [...map.keys()]))]
    .sort((left, right) => left - right)
    .filter((bucket) => (
      (!Number.isFinite(chartStart) || bucket >= chartStart)
      && (!Number.isFinite(requestedEnd) || bucket <= requestedEnd)
    ))
    .slice(-241);
  const valueCount = maps.reduce(
    (total, map) => total + buckets.filter((bucket) => map.has(bucket)).length,
    0,
  );
  if (buckets.length < 2 || valueCount < 2) return null;
  const series = normalizedAccounts.map((account, index) => {
    const values = buckets.map((bucket) => maps[index].get(bucket) ?? null);
    // Derive all equity metrics after selecting the displayed window.
    // The API ratios and peaks may include points before the daily anchor.
    const baseline = equityMetric ? values.find((value) => value != null) : null;
    let peak = null;
    const metricValues = equityMetric ? values.map((value) => {
      if (value == null || baseline == null) return null;
      peak = peak == null ? value : Math.max(peak, value);
      switch (metricKey) {
        case "equity": return value - baseline;
        case "equity_change_ratio":
          return baseline === 0 ? null : (value - baseline) / baseline;
        case "drawdown": return value - peak;
        case "drawdown_ratio":
          return peak === 0 ? null : (value - peak) / peak;
      }
    }) : values;
    return {
      account,
      label: account.account_label || `账户 ${index + 1}`,
      color: LIVE_ACCOUNT_METRIC_COLORS[index % LIVE_ACCOUNT_METRIC_COLORS.length],
      values: metricValues,
      isLive: true,
    };
  });
  const values = series.flatMap((seriesItem) => (
    seriesItem.values.filter((value) => value != null)
  ));
  const valueFormat = metricValueFormat(metricKey);
  const includeZero = valueFormat !== "money";
  const nonNegativeMetric = metricKey === "margin_used"
    || metricKey === "margin_occupancy_ratio";
  let min = includeZero ? Math.min(0, ...values) : Math.min(...values);
  let max = includeZero ? Math.max(0, ...values) : Math.max(...values);
  if (min === max) {
    const padding = Math.max(Math.abs(min) * 0.08, 0.01);
    min = nonNegativeMetric ? 0 : min - padding;
    max += padding;
  } else {
    const padding = Math.max((max - min) * 0.08, 0.01);
    min = nonNegativeMetric ? 0 : min - padding;
    max += padding;
  }
  return {
    metricKey,
    valueFormat,
    series,
    points: buckets.map((bucket, index) => ({
      at: bucket,
      values: series.map((seriesItem) => seriesItem.values[index]),
    })),
    domainStart: Number.isFinite(chartStart) ? chartStart : buckets[0],
    domainEnd: Number.isFinite(requestedEnd) && requestedEnd > chartStart
      ? requestedEnd
      : buckets.at(-1),
    anchorAt: anchoredWindow ? dailyAnchor : null,
    anchorMode: anchoredWindow ? "daily-anchor" : "window-start-fallback",
    min,
    max,
    intervalSeconds,
  };
}

export function liveAccountMetricChart(
  accounts,
  metricKey,
  chartId,
  title,
  ariaLabel,
  intervalSeconds = DEFAULT_EQUITY_BUCKET_SECONDS,
  windowStart = null,
  windowEnd = null,
) {
  const model = liveAccountMetricModel(
    accounts,
    metricKey,
    intervalSeconds,
    windowStart,
    windowEnd,
  );
  if (!model) {
    // Keep the slot tall so a sparse/empty refresh cannot collapse the page
    // and clamp scrollTop (field jump 2474→153).
    return `<div class="live-metric-chart" style="min-height:220px">${emptyBox("等待实盘时序数据", "至少需要两个有效采样点")}</div>`;
  }
  registerChartPayload(chartId, {
    kind: "metric-comparison",
    title,
    valueFormat: model.valueFormat,
    domainStart: model.domainStart,
    domainEnd: model.domainEnd,
    min: model.min,
    max: model.max,
    anchorAt: model.anchorAt,
    anchorMode: model.anchorMode,
    points: model.points,
    series: model.series.map((series) => ({
      label: series.label,
      color: series.color,
      values: series.values,
      isLive: true,
    })),
  });
  const legend = model.series.map((series) => (
    `<span class="live-metric-legend-item"><i style="--series-color:${esc(series.color)}"></i>${esc(series.label)}</span>`
  )).join("");
  return `<div class="live-metric-chart echart-shell" data-echart-chart data-echart-kind="metric-comparison" data-echart-group="live-account-fleet" data-echart-id="${esc(chartId)}" tabindex="0" role="group" aria-label="${esc(ariaLabel)}；使用左右方向键查看数据点">
    <div class="live-metric-legend">${legend}</div>
    <div class="echart-surface" aria-hidden="true"></div>
  </div>`;
}

export function returnBar(value, maxAbs) {
  const parsed = asNumber(value);
  if (parsed == null) return '<span class="num">—</span>';
  const direction = parsed >= 0 ? "pos" : "neg";
  const widthPct = Math.min(100, (Math.abs(parsed) / Math.max(maxAbs, 0.0001)) * 100);
  return `<div class="ret"><span class="num ${direction}">${esc(signedPercent(parsed))}</span><span class="ret-track"><i class="${direction}" style="width:${widthPct.toFixed(1)}%"></i></span></div>`;
}
