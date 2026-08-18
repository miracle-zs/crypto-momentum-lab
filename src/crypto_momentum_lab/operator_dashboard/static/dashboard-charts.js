import {
  COMPARISON_ANCHOR_HOUR,
  COMPARISON_SERIES_COLORS,
  DEFAULT_EQUITY_BUCKET_SECONDS,
  DISPLAY_TIME_ZONE,
  STRATEGY_ORDER,
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

function equityBucketMap(account) {
  const intervalSeconds = asNumber(account.equity_sample_interval_seconds) || DEFAULT_EQUITY_BUCKET_SECONDS;
  const intervalMs = intervalSeconds * 1000;
  const buckets = new Map();
  for (const row of account.equity_curve || []) {
    const observedAt = new Date(row.observed_at).getTime();
    const equity = asNumber(row.equity);
    if (!Number.isFinite(observedAt) || equity == null) continue;
    const bucket = Math.floor(observedAt / intervalMs) * intervalMs;
    buckets.set(bucket, equity);
  }
  return { intervalSeconds, buckets };
}

function comparisonSeriesClass(account) {
  if (account.source === "live") return "live";
  return account.exit_mode === "candle_15m" ? "candle" : "variant";
}

function comparisonSeriesColor(account, index) {
  if (account.source === "live") return "var(--series-live)";
  return COMPARISON_SERIES_COLORS[index % COMPARISON_SERIES_COLORS.length];
}

function comparisonSeriesLabel(account, index, accounts) {
  if (account.source === "live") return account.exit_label || "实盘 B1";
  const base = account.exit_label || "15M 收线退出";
  const duplicateCount = accounts.filter((candidate) => (
    candidate.exit_label || "15M 收线退出"
  ) === base).length;
  if (duplicateCount < 2) return base;
  const accountNumber = String(account.run_id || "").match(/^paper-account-(\d+)/)?.[1];
  return accountNumber ? `${base} · 账户 ${accountNumber}` : `${base} · 版本 ${index + 1}`;
}

export function comparisonSeriesStyle(series) {
  return `style="--series-color:${series.color}"`;
}

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

const COMPARISON_TIME_FORMATTER = new Intl.DateTimeFormat("en-CA", {
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

function comparisonReferenceMs(accounts) {
  const windowEnds = accounts
    .map((account) => new Date(account.equity_window_end || "").getTime())
    .filter((value) => Number.isFinite(value));
  if (windowEnds.length) return Math.max(...windowEnds);
  const curveEnds = accounts.flatMap((account) => (account.equity_curve || [])
    .map((row) => new Date(row.observed_at || "").getTime())
    .filter((value) => Number.isFinite(value)));
  return curveEnds.length ? Math.max(...curveEnds) : Date.now();
}

function comparisonDailyAnchor(referenceMs) {
  const parts = Object.fromEntries(
    COMPARISON_TIME_FORMATTER.formatToParts(new Date(referenceMs))
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
    COMPARISON_ANCHOR_HOUR,
  );
  return anchorWallMs - offsetMs;
}

function comparisonStart(commonBuckets, anchorAt) {
  const anchoredBucket = commonBuckets.find((bucket) => bucket >= anchorAt);
  if (anchoredBucket != null) {
    return {
      at: anchoredBucket,
      mode: anchoredBucket === anchorAt ? "daily-anchor" : "after-anchor",
    };
  }
  return {
    at: commonBuckets[0],
    mode: "common-start-fallback",
  };
}

function strategyEquityModel(strategyName, accounts) {
  if (accounts.length < 2) return null;
  const maps = accounts.map(equityBucketMap);
  const commonBuckets = [...maps[0].buckets.keys()]
    .filter((bucket) => maps.every((map) => map.buckets.has(bucket)))
    .sort((left, right) => left - right);
  if (commonBuckets.length < 2) return null;

  const anchorAt = comparisonDailyAnchor(comparisonReferenceMs(accounts));
  const start = comparisonStart(commonBuckets, anchorAt);
  const comparisonBuckets = commonBuckets.filter((bucket) => bucket >= start.at);
  if (comparisonBuckets.length < 2) return null;
  const series = accounts.map((account, index) => {
    const map = maps[index];
    const base = map.buckets.get(start.at);
    const values = comparisonBuckets.map((bucket) => map.buckets.get(bucket) - base);
    return {
      account,
      label: comparisonSeriesLabel(account, index, accounts),
      colorClass: comparisonSeriesClass(account),
      color: comparisonSeriesColor(account, index),
      values,
      delta: values.at(-1),
    };
  });
  const points = comparisonBuckets.map((bucket, index) => ({
    at: bucket,
    values: series.map((item) => item.values[index]),
  }));
  const domainValues = [0, ...series.flatMap((item) => item.values)];
  let min = Math.min(...domainValues);
  let max = Math.max(...domainValues);
  if (min === max) {
    min -= 0.01;
    max += 0.01;
  } else {
    const padding = Math.max((max - min) * 0.08, 0.01);
    min -= padding;
    max += padding;
  }
  return {
    strategyName,
    accounts,
    series,
    points,
    startAt: start.at,
    endAt: comparisonBuckets.at(-1),
    anchorAt,
    anchorMode: start.mode,
    min,
    max,
    intervalSeconds: Math.max(...maps.map((map) => map.intervalSeconds)),
  };
}

export function buildStrategyEquityModels(accounts) {
  return STRATEGY_ORDER.flatMap((strategyName) => {
    const strategyAccounts = accounts
      .filter((account) => account.strategy_name === strategyName)
      .sort((left, right) => {
        const leftLive = left.source === "live" ? 1 : 0;
        const rightLive = right.source === "live" ? 1 : 0;
        return leftLive - rightLive || String(left.run_id).localeCompare(String(right.run_id));
      });
    const model = strategyEquityModel(strategyName, strategyAccounts);
    return model ? [model] : [];
  });
}

export function standaloneSparkline(rows) {
  const values = (rows || []).map((row) => asNumber(row.equity)).filter((value) => value != null);
  if (values.length < 2) return '<div class="spark spark-empty">—</div>';
  const width = 150;
  const height = 40;
  let min = Math.min(...values);
  let max = Math.max(...values);
  if (min === max) { min -= 1; max += 1; }
  const stepX = width / (values.length - 1);
  const y = (value) => 3 + ((max - value) / (max - min)) * (height - 6);
  const line = values.map((value, i) => `${(i * stepX).toFixed(1)},${y(value).toFixed(1)}`).join(" ");
  const dir = values.at(-1) >= values[0] ? "pos" : "neg";
  return `<svg class="spark ${dir}" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-hidden="true"><polyline points="${line}"/></svg>`;
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

export function strategyEquityChart(model) {
  const chartId = `comparison-${String(model.strategyName || "strategy")
    .replace(/[^A-Za-z0-9_-]+/g, "-")}`;
  registerChartPayload(chartId, {
    kind: "comparison",
    title: `${model.strategyName} 各退出方式同期权益对比`,
    strategyName: model.strategyName,
    startAt: model.startAt,
    endAt: model.endAt,
    anchorAt: model.anchorAt,
    anchorMode: model.anchorMode,
    min: model.min,
    max: model.max,
    points: model.points,
    series: model.series.map((series) => ({
      label: series.label,
      color: series.color,
      colorClass: series.colorClass,
      values: series.values,
      delta: series.delta,
      isLive: series.account?.source === "live",
    })),
  });
  return `<div class="pair-chart echart-shell" data-echart-chart data-echart-kind="comparison" data-echart-id="${esc(chartId)}" tabindex="0" role="group" aria-label="${esc(model.strategyName)} 各退出方式同期权益对比；使用左右方向键查看数据点">
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
