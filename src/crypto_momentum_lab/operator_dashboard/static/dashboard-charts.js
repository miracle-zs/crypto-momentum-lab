import {
  COMPARISON_SERIES_COLORS,
  DEFAULT_EQUITY_BUCKET_SECONDS,
  DISPLAY_TIME_ZONE_LABEL,
  STRATEGY_ORDER,
} from "./dashboard-config.js";
import {
  asNumber,
  dayTime,
  esc,
  money,
  signedMoney,
  signedPercent,
  timeOnly,
} from "./dashboard-formatters.js";
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

function chartPointTitle(at, value, label = "权益") {
  return `<title>${esc(`${label} · ${dayTime(at)} ${DISPLAY_TIME_ZONE_LABEL} · ${money(value)}`)}</title>`;
}

function strategyEquityModel(strategyName, accounts) {
  if (accounts.length < 2) return null;
  const maps = accounts.map(equityBucketMap);
  const commonBuckets = [...maps[0].buckets.keys()]
    .filter((bucket) => maps.every((map) => map.buckets.has(bucket)))
    .sort((left, right) => left - right);
  if (commonBuckets.length < 2) return null;

  const firstBucket = commonBuckets[0];
  const series = accounts.map((account, index) => {
    const map = maps[index];
    const base = map.buckets.get(firstBucket);
    const values = commonBuckets.map((bucket) => map.buckets.get(bucket) - base);
    return {
      account,
      label: comparisonSeriesLabel(account, index, accounts),
      colorClass: comparisonSeriesClass(account),
      color: comparisonSeriesColor(account, index),
      values,
      delta: values.at(-1),
    };
  });
  const points = commonBuckets.map((bucket, index) => ({
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
    startAt: commonBuckets[0],
    endAt: commonBuckets.at(-1),
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
  const width = 1000;
  const height = 280;
  const padL = 68;
  const padR = 26;
  const padT = 16;
  const padB = 32;
  const requestedStart = windowStart ? new Date(windowStart).getTime() : Number.NaN;
  const requestedEnd = windowEnd ? new Date(windowEnd).getTime() : Number.NaN;
  const domainStart = Number.isFinite(requestedStart) ? requestedStart : points[0].atMs;
  const domainEnd = Number.isFinite(requestedEnd) && requestedEnd > domainStart
    ? requestedEnd : points.at(-1).atMs;
  const timeSpan = Math.max(domainEnd - domainStart, 1);
  const x = (atMs) => padL + ((atMs - domainStart) / timeSpan) * (width - padL - padR);
  const y = (value) => padT + ((max - value) / (max - min)) * (height - padT - padB);
  const lastValue = values.at(-1);
  const lastUp = lastValue >= values[0];
  const line = points
    .map((point) => `${x(point.atMs).toFixed(1)},${y(point.equity).toFixed(1)}`)
    .join(" ");
  const area = `${x(points[0].atMs).toFixed(1)},${(height - padB).toFixed(1)} ${line} ` +
    `${x(points.at(-1).atMs).toFixed(1)},${(height - padB).toFixed(1)}`;
  const pointMarkers = points.map((point) => `<circle
    cx="${x(point.atMs).toFixed(1)}" cy="${y(point.equity).toFixed(1)}" r="7"
    class="chart-point ${lastUp ? "pos" : "neg"}" aria-hidden="true">
    ${chartPointTitle(point.at, point.equity)}
  </circle>`).join("");
  const gridSteps = 4;
  const grid = Array.from({ length: gridSteps + 1 }, (_, i) => {
    const value = max - ((max - min) / gridSteps) * i;
    const gy = y(value).toFixed(1);
    return `<line x1="${padL}" y1="${gy}" x2="${width - padR}" y2="${gy}" class="chart-grid"/>` +
      `<text x="${padL - 10}" y="${gy}" class="chart-label" text-anchor="end" dominant-baseline="middle">${esc(money(value))}</text>`;
  }).join("");
  const baseline = 1000 >= min && 1000 <= max
    ? `<line x1="${padL}" y1="${y(1000).toFixed(1)}" x2="${width - padR}" y2="${y(1000).toFixed(1)}" class="chart-baseline"/>`
    : "";
  const axisTimes = [domainStart, domainStart + timeSpan / 2, domainEnd];
  const timeAxis = axisTimes.map((atMs, i) =>
    `<text x="${x(atMs).toFixed(1)}" y="${height - 8}" class="chart-label" text-anchor="${i === 0 ? "start" : i === 2 ? "end" : "middle"}">${esc(i === 1 ? timeOnly(atMs) : dayTime(atMs))}${i === 2 ? ` ${DISPLAY_TIME_ZONE_LABEL}` : ""}</text>`).join("");
  return `<div class="equity-chart"><svg viewBox="0 0 ${width} ${height}" role="img" aria-label="账户权益曲线">
    <defs><linearGradient id="${esc(chartId)}" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0" stop-color="${lastUp ? "var(--up)" : "var(--down)"}" stop-opacity=".22"/>
      <stop offset="1" stop-color="${lastUp ? "var(--up)" : "var(--down)"}" stop-opacity="0"/>
    </linearGradient></defs>
    ${grid}${baseline}
    <polygon points="${area}" fill="url(#${esc(chartId)})"/>
    <polyline points="${line}" class="equity-line ${lastUp ? "pos" : "neg"}"/>
    ${pointMarkers}
    <circle cx="${x(points.at(-1).atMs).toFixed(1)}" cy="${y(lastValue).toFixed(1)}" r="4" class="equity-dot ${lastUp ? "pos" : "neg"}"/>
    ${timeAxis}
  </svg></div>`;
}

export function strategyEquityChart(model) {
  const width = 600;
  const height = 230;
  const padL = 58;
  const padR = 18;
  const padT = 14;
  const padB = 30;
  const timeSpan = Math.max(model.endAt - model.startAt, 1);
  const x = (point) => padL + ((point.at - model.startAt) / timeSpan) * (width - padL - padR);
  const y = (value) => padT + ((model.max - value) / (model.max - model.min)) * (height - padT - padB);
  const line = (series) => model.points
    .map((point, index) => `${x(point).toFixed(1)},${y(series.values[index]).toFixed(1)}`)
    .join(" ");
  const grid = Array.from({ length: 5 }, (_, index) => {
    const value = model.max - ((model.max - model.min) / 4) * index;
    const gy = y(value).toFixed(1);
    return `<line x1="${padL}" y1="${gy}" x2="${width - padR}" y2="${gy}" class="chart-grid"/>` +
      `<text x="${padL - 8}" y="${gy}" class="chart-label" text-anchor="end" dominant-baseline="middle">${esc(signedMoney(value))}</text>`;
  }).join("");
  const axisTimes = [model.startAt, model.startAt + timeSpan / 2, model.endAt];
  const timeAxis = axisTimes.map((atMs, index) =>
    `<text x="${(padL + (index / 2) * (width - padL - padR)).toFixed(1)}" y="${height - 8}" class="chart-label"
      text-anchor="${index === 0 ? "start" : index === 2 ? "end" : "middle"}">${esc(timeOnly(atMs))}${index === 2 ? ` ${DISPLAY_TIME_ZONE_LABEL}` : ""}</text>`).join("");
  const lines = model.series.map((series) =>
    `<polyline points="${line(series)}" class="pair-line ${series.colorClass}" ${comparisonSeriesStyle(series)}/>`).join("");
  const pointMarkers = model.series.flatMap((series) => model.points.map((point, index) =>
    `<circle cx="${x(point).toFixed(1)}" cy="${y(series.values[index]).toFixed(1)}" r="6"
      class="pair-point ${series.colorClass}" ${comparisonSeriesStyle(series)} aria-hidden="true">
      ${chartPointTitle(point.at, series.values[index], series.label)}
    </circle>`)).join("");
  const dots = model.series.map((series) =>
    `<circle cx="${x(model.points.at(-1)).toFixed(1)}" cy="${y(series.delta).toFixed(1)}" r="3" class="pair-dot ${series.colorClass}" ${comparisonSeriesStyle(series)}/>`).join("");
  return `<div class="pair-chart"><svg viewBox="0 0 ${width} ${height}" role="img"
    aria-label="${esc(model.strategyName)} 各退出方式同期权益对比">
    ${grid}
    <line x1="${padL}" y1="${y(0).toFixed(1)}" x2="${width - padR}" y2="${y(0).toFixed(1)}" class="chart-baseline"/>
    ${lines}
    ${pointMarkers}
    ${dots}
    ${timeAxis}
  </svg></div>`;
}

export function returnBar(value, maxAbs) {
  const parsed = asNumber(value);
  if (parsed == null) return '<span class="num">—</span>';
  const direction = parsed >= 0 ? "pos" : "neg";
  const widthPct = Math.min(100, (Math.abs(parsed) / Math.max(maxAbs, 0.0001)) * 100);
  return `<div class="ret"><span class="num ${direction}">${esc(signedPercent(parsed))}</span><span class="ret-track"><i class="${direction}" style="width:${widthPct.toFixed(1)}%"></i></span></div>`;
}
