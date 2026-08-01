const SECTIONS = ["overview", "strategy", "universe", "risk", "account", "reports"];
const POLL_MS = 5000;
const DEFAULT_EQUITY_BUCKET_SECONDS = 6 * 60;
const STRATEGY_ORDER = [
  "compression_breakout",
  "orderflow_impulse",
  "liquidation_cascade",
];
let selectedPaperAccount = 0;
let lastPollAt = null;
let pollInFlight = false;
const paperHistoryByRun = new Map();

const esc = (value) => String(value ?? "—").replace(/[&<>'"]/g, (char) => ({
  "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;"
}[char]));

const statusSlug = (status) => String(status || "UNKNOWN").trim().replace(/[^A-Za-z]+/g, "-").toUpperCase();
const statusClass = (status) => `status-${statusSlug(status)}`;

/* ---------- formatting ---------- */

const asNumber = (value) => {
  if (value == null || value === "") return null;
  const parsed = Number(value);
  return Number.isNaN(parsed) ? null : parsed;
};

const num = (value, digits = 2) => {
  const parsed = asNumber(value);
  return parsed == null ? "—" : parsed.toLocaleString("en-US", {
    minimumFractionDigits: digits, maximumFractionDigits: digits,
  });
};

const price = (value) => {
  const parsed = asNumber(value);
  if (parsed == null) return value == null ? "—" : String(value);
  const magnitude = Math.abs(parsed);
  const digits = magnitude >= 1000 ? 2 : magnitude >= 10 ? 3 : magnitude >= 0.1 ? 4 : 6;
  return num(parsed, digits);
};

const money = (value) => {
  const parsed = asNumber(value);
  return parsed == null ? "—" : `${parsed < 0 ? "−" : ""}$${num(Math.abs(parsed))}`;
};

const signedMoney = (value) => {
  const parsed = asNumber(value);
  return parsed == null ? "—" : `${parsed > 0 ? "+" : parsed < 0 ? "−" : ""}$${num(Math.abs(parsed))}`;
};

const percent = (value, digits = 2) => {
  const parsed = asNumber(value);
  return parsed == null ? "—" : `${(parsed * 100).toFixed(digits)}%`;
};

const signedPercent = (value, digits = 2) => {
  const parsed = asNumber(value);
  return parsed == null ? "—" : `${parsed > 0 ? "+" : ""}${(parsed * 100).toFixed(digits)}%`;
};

const pnlClass = (value) => {
  const parsed = asNumber(value);
  return parsed == null || parsed === 0 ? "" : parsed > 0 ? "pos" : "neg";
};

const timeOnly = (value) => {
  if (!value) return "—";
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? String(value) : parsed.toISOString().slice(11, 19);
};

const dayTime = (value) => {
  if (!value) return "—";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return String(value);
  return `${parsed.toISOString().slice(5, 10)} ${parsed.toISOString().slice(11, 19)}`;
};

const elapsedTime = (start, end) => {
  const startAt = new Date(start).getTime();
  const endAt = new Date(end).getTime();
  if (!Number.isFinite(startAt) || !Number.isFinite(endAt) || endAt < startAt) return "—";
  const minutes = Math.max(0, Math.round((endAt - startAt) / 60000));
  const hours = Math.floor(minutes / 60);
  const remainder = minutes % 60;
  if (hours === 0) return `${remainder} 分钟`;
  return remainder ? `${hours} 小时 ${remainder} 分` : `${hours} 小时`;
};

const relAge = (seconds) => {
  const parsed = asNumber(seconds);
  if (parsed == null) return "未知";
  if (parsed < 60) return `${Math.round(parsed)} 秒前`;
  if (parsed < 3600) return `${Math.floor(parsed / 60)} 分前`;
  if (parsed < 86400) return `${Math.floor(parsed / 3600)} 小时前`;
  return `${Math.floor(parsed / 86400)} 天前`;
};

const relToNow = (value) => {
  if (!value) return "未知";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return "未知";
  const delta = (Date.now() - parsed.getTime()) / 1000;
  return delta >= 0 ? relAge(delta) : `${Math.round(-delta / 60)} 分后`;
};

const shortHash = (value) => {
  const hash = String(value || "").trim();
  return hash.length > 8 ? `${hash.slice(0, 8)}…` : hash || "—";
};

/* ---------- building blocks ---------- */

const pill = (status) => `<span class="pill ${statusClass(status)}"><i></i>${esc(status || "UNKNOWN")}</span>`;

const sideTag = (side) => side === "long"
  ? '<span class="side-tag long">多</span>'
  : '<span class="side-tag short">空</span>';

const signalEvidence = (row) => {
  const features = row.features || {};
  const referencePrices = row.reference_prices || {};
  const parts = [];
  const add = (label, value) => {
    if (value == null || value === "") return;
    parts.push(`<span class="signal-evidence-item"><b>${esc(label)}</b>${esc(value)}</span>`);
  };
  const moneyValue = (value) => money(value);
  const percentValue = (value, digits = 2) => percent(value, digits);

  if (features.liquidation_notional != null) {
    add("清算", `${moneyValue(features.liquidation_notional)} / ${num(features.liquidation_count, 0)} 笔`);
  }
  if (features.range_width_pct != null) {
    add("压缩区间", percentValue(features.range_width_pct, 2));
  }
  if (features.impulse_return_pct != null) {
    add("冲击收益", percentValue(features.impulse_return_pct, 2));
  }
  if (features.notional_intensity != null) {
    add("成交强度", `${num(features.notional_intensity, 2)}x`);
  }
  if (features.breakout_distance_pct != null) {
    add("突破距离", percentValue(features.breakout_distance_pct, 2));
  }
  if (features.aggressive_imbalance != null) {
    add("主动不平衡", percentValue(features.aggressive_imbalance, 1));
  }
  const tradeNotional = features.trade_notional ?? features.impulse_trade_notional ?? features.cluster_trade_notional;
  if (tradeNotional != null) add("成交额", moneyValue(tradeNotional));
  if (features.aggressive_buy_notional != null) {
    add("主动买", moneyValue(features.aggressive_buy_notional));
  }
  if (features.aggressive_sell_notional != null) {
    add("主动卖", moneyValue(features.aggressive_sell_notional));
  }
  if (features.breakout_price != null) add("触发价", price(features.breakout_price));
  else if (referencePrices.breakout_level != null) add("触发价", price(referencePrices.breakout_level));
  if (!parts.length) return "—";
  return `<div class="signal-evidence">${parts.join("")}</div>`;
};

const tile = (label, value, sub = "", cls = "") =>
  `<div class="tile"><label>${esc(label)}</label><strong class="${cls}">${esc(value)}</strong><small>${esc(sub)}</small></div>`;

const emptyBox = (text = "暂无数据", hint = "") =>
  `<div class="empty"><span>${esc(text)}</span>${hint ? `<small>${esc(hint)}</small>` : ""}</div>`;

const blockTitle = (title, eyebrow, aside = "") =>
  `<div class="block-title"><div><b>${esc(title)}</b><small>${esc(eyebrow)}</small></div>${aside ? `<span>${aside}</span>` : ""}</div>`;

function dataTable(columns, rows, options = {}) {
  if (!rows?.length) return emptyBox(options.emptyText || "暂无数据");
  const head = columns.map((column) =>
    `<th class="${column.align === "right" ? "ta-r" : ""}">${esc(column.label)}</th>`).join("");
  const body = rows.map((row) => `<tr>${columns.map((column) => {
    const raw = column.value ? column.value(row) : row[column.key];
    const classes = [
      column.align === "right" ? "ta-r num" : "",
      typeof column.cls === "function" ? column.cls(row) : column.cls || "",
    ].filter(Boolean).join(" ");
    return `<td class="${classes}">${column.html ? raw : esc(raw)}</td>`;
  }).join("")}</tr>`).join("");
  return `<div class="table-scroll${options.tall ? " tall" : ""}"><table class="data-table"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`;
}

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

function pairedEquityModel(strategyName, fixedAccount, candleAccount) {
  const fixed = equityBucketMap(fixedAccount);
  const candle = equityBucketMap(candleAccount);
  const commonBuckets = [...fixed.buckets.keys()]
    .filter((bucket) => candle.buckets.has(bucket))
    .sort((left, right) => left - right);
  if (commonBuckets.length < 2) return null;

  const firstBucket = commonBuckets[0];
  const fixedBase = fixed.buckets.get(firstBucket);
  const candleBase = candle.buckets.get(firstBucket);
  const points = commonBuckets.map((bucket) => ({
    at: bucket,
    fixed: fixed.buckets.get(bucket) - fixedBase,
    candle: candle.buckets.get(bucket) - candleBase,
  }));
  const domainValues = [0, ...points.flatMap((point) => [point.fixed, point.candle])];
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
    fixedAccount,
    candleAccount,
    points,
    startAt: commonBuckets[0],
    endAt: commonBuckets.at(-1),
    min,
    max,
    fixedDelta: points.at(-1).fixed,
    candleDelta: points.at(-1).candle,
    intervalSeconds: Math.max(fixed.intervalSeconds, candle.intervalSeconds),
  };
}

function buildPairedEquityModels(accounts) {
  const pairs = new Map();
  for (const account of accounts) {
    if (!account.strategy_name) continue;
    const pair = pairs.get(account.strategy_name) || {};
    if (account.exit_mode === "candle_15m") pair.candle = account;
    else pair.fixed = account;
    pairs.set(account.strategy_name, pair);
  }
  return STRATEGY_ORDER.flatMap((strategyName) => {
    const pair = pairs.get(strategyName);
    if (!pair?.fixed || !pair?.candle) return [];
    const model = pairedEquityModel(strategyName, pair.fixed, pair.candle);
    return model ? [model] : [];
  });
}

/* ---------- charts ---------- */

function standaloneSparkline(rows) {
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

function comparisonSparkline(account, model) {
  if (!model) return standaloneSparkline(account.equity_curve);
  const seriesKey = account.exit_mode === "candle_15m" ? "candle" : "fixed";
  const points = model.points;
  const width = 150;
  const height = 40;
  const timeSpan = Math.max(model.endAt - model.startAt, 1);
  const x = (point) => ((point.at - model.startAt) / timeSpan) * width;
  const y = (value) => 3 + ((model.max - value) / (model.max - model.min)) * (height - 6);
  const line = points
    .map((point) => `${x(point).toFixed(1)},${y(point[seriesKey]).toFixed(1)}`)
    .join(" ");
  return `<svg class="spark ${seriesKey}" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none"
    role="img" aria-label="${seriesKey === "fixed" ? "固定止盈止损" : "15 分钟收线退出"}同期权益走势">
    <line x1="0" y1="${y(0).toFixed(1)}" x2="${width}" y2="${y(0).toFixed(1)}" class="spark-zero"/>
    <polyline points="${line}"/>
  </svg>`;
}

function equityChart(rows, chartId = "eq", windowStart = null, windowEnd = null) {
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
  const line = points
    .map((point) => `${x(point.atMs).toFixed(1)},${y(point.equity).toFixed(1)}`)
    .join(" ");
  const area = `${x(points[0].atMs).toFixed(1)},${(height - padB).toFixed(1)} ${line} ` +
    `${x(points.at(-1).atMs).toFixed(1)},${(height - padB).toFixed(1)}`;
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
  const lastValue = values.at(-1);
  const lastUp = lastValue >= values[0];
  const axisTimes = [domainStart, domainStart + timeSpan / 2, domainEnd];
  const timeAxis = axisTimes.map((atMs, i) =>
    `<text x="${x(atMs).toFixed(1)}" y="${height - 8}" class="chart-label" text-anchor="${i === 0 ? "start" : i === 2 ? "end" : "middle"}">${esc(i === 1 ? timeOnly(atMs) : dayTime(atMs))}${i === 2 ? " UTC" : ""}</text>`).join("");
  return `<div class="equity-chart"><svg viewBox="0 0 ${width} ${height}" role="img" aria-label="账户权益曲线">
    <defs><linearGradient id="${esc(chartId)}" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0" stop-color="${lastUp ? "var(--up)" : "var(--down)"}" stop-opacity=".22"/>
      <stop offset="1" stop-color="${lastUp ? "var(--up)" : "var(--down)"}" stop-opacity="0"/>
    </linearGradient></defs>
    ${grid}${baseline}
    <polygon points="${area}" fill="url(#${esc(chartId)})"/>
    <polyline points="${line}" class="equity-line ${lastUp ? "pos" : "neg"}"/>
    <circle cx="${x(points.at(-1).atMs).toFixed(1)}" cy="${y(lastValue).toFixed(1)}" r="4" class="equity-dot ${lastUp ? "pos" : "neg"}"/>
    ${timeAxis}
  </svg></div>`;
}

function pairedEquityChart(model) {
  const width = 600;
  const height = 230;
  const padL = 58;
  const padR = 18;
  const padT = 14;
  const padB = 30;
  const timeSpan = Math.max(model.endAt - model.startAt, 1);
  const x = (point) => padL + ((point.at - model.startAt) / timeSpan) * (width - padL - padR);
  const y = (value) => padT + ((model.max - value) / (model.max - model.min)) * (height - padT - padB);
  const line = (key) => model.points
    .map((point) => `${x(point).toFixed(1)},${y(point[key]).toFixed(1)}`)
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
      text-anchor="${index === 0 ? "start" : index === 2 ? "end" : "middle"}">${esc(timeOnly(atMs))}${index === 2 ? " UTC" : ""}</text>`).join("");
  const last = model.points.at(-1);
  return `<div class="pair-chart"><svg viewBox="0 0 ${width} ${height}" role="img"
    aria-label="${esc(model.strategyName)} 固定止盈止损与 15 分钟收线退出同期权益对比">
    ${grid}
    <line x1="${padL}" y1="${y(0).toFixed(1)}" x2="${width - padR}" y2="${y(0).toFixed(1)}" class="chart-baseline"/>
    <polyline points="${line("fixed")}" class="pair-line fixed"/>
    <polyline points="${line("candle")}" class="pair-line candle"/>
    <circle cx="${x(last).toFixed(1)}" cy="${y(last.fixed).toFixed(1)}" r="3" class="pair-dot fixed"/>
    <circle cx="${x(last).toFixed(1)}" cy="${y(last.candle).toFixed(1)}" r="3" class="pair-dot candle"/>
    ${timeAxis}
  </svg></div>`;
}

function returnBar(value, maxAbs) {
  const parsed = asNumber(value);
  if (parsed == null) return '<span class="num">—</span>';
  const direction = parsed >= 0 ? "pos" : "neg";
  const widthPct = Math.min(100, (Math.abs(parsed) / Math.max(maxAbs, 0.0001)) * 100);
  return `<div class="ret"><span class="num ${direction}">${esc(signedPercent(parsed))}</span><span class="ret-track"><i class="${direction}" style="width:${widthPct.toFixed(1)}%"></i></span></div>`;
}

/* ---------- section renderers ---------- */

function renderOverview(data) {
  const lease = data.active_lease;
  const services = data.services || [];
  const haltCount = data.active_halt_count || 0;
  const tiles = `<div class="tile-grid">
    ${tile("数据库", data.database_status || "UNKNOWN", "PostgreSQL 只读连接", statusSlug(data.database_status) === "READY" ? "pos" : "warn")}
    ${tile("活跃停机", haltCount, haltCount ? "入场信号已被阻断" : "无全局停机", haltCount ? "neg" : "")}
    ${tile("交易租约", lease?.strategy_name || "无租约", lease ? `持有者 ${lease.owner || "未知"}` : "当前无进程持有交易权", "txt")}
    ${tile("租约到期", lease?.expires_at ? relToNow(lease.expires_at) : "—", lease?.expires_at ? dayTime(lease.expires_at) + " UTC" : "")}
  </div>`;
  const serviceRows = services.map((service) => {
    const age = service.age_seconds;
    const freshness = age == null ? 0 : Math.max(6, 100 - Math.min(100, (age / 120) * 100));
    return `<div class="service-row">
      <b>${esc(service.name)}</b>
      <span class="service-meter"><i class="${statusClass(service.status)}" style="width:${freshness.toFixed(0)}%"></i></span>
      <span class="service-age num">${age == null ? "未知" : relAge(age)}</span>
      ${pill(service.status)}
    </div>`;
  }).join("");
  const body = `${tiles}
    ${blockTitle("服务心跳", "SERVICE HEARTBEATS", `<span class="num muted">${services.length} 个进程</span>`)}
    <div class="service-list">${serviceRows || emptyBox("尚未观察到任何服务心跳")}</div>`;
  return [haltCount ? "HALTED" : data.database_status, body];
}

function pairedComparisonPanel(model) {
  const spread = model.candleDelta - model.fixedDelta;
  const bucketMinutes = Math.round(model.intervalSeconds / 60);
  return `<article class="pair-panel">
    <div class="pair-head">
      <div><strong>${esc(model.strategyName)}</strong>
        <small>${esc(dayTime(model.startAt))} → ${esc(dayTime(model.endAt))} UTC · ${model.points.length} 个共同桶</small></div>
      <div class="pair-spread"><span>退出差值</span><b class="num ${pnlClass(spread)}">${esc(signedMoney(spread))}</b></div>
    </div>
    <div class="pair-legend">
      <span class="fixed"><i></i>固定 TP / SL <b class="num ${pnlClass(model.fixedDelta)}">${esc(signedMoney(model.fixedDelta))}</b></span>
      <span class="candle"><i></i>15M 收线退出 <b class="num ${pnlClass(model.candleDelta)}">${esc(signedMoney(model.candleDelta))}</b></span>
    </div>
    ${pairedEquityChart(model)}
    <footer><span>共同起点归零 · ${bucketMinutes} 分钟 UTC 采样</span><b>${esc(elapsedTime(model.startAt, model.endAt))}</b></footer>
  </article>`;
}

function accountCard(account, index, pairModel) {
  const summary = account.portfolio_summary || {};
  const equity = asNumber(summary.equity);
  const returnSinceStart = equity == null ? null : equity / 1000 - 1;
  const isCandle = account.exit_mode === "candle_15m";
  const matchedDelta = pairModel
    ? (isCandle ? pairModel.candleDelta : pairModel.fixedDelta)
    : null;
  const active = index === selectedPaperAccount;
  const exitMode = isCandle ? "15M 收线退出" : "固定 TP / SL";
  return `<button class="acct-card${active ? " is-active" : ""}" type="button" role="tab"
    aria-selected="${active}" data-account-index="${index}">
    <div class="acct-top"><span>账户 0${index + 1}</span><span class="acct-mode">${esc(exitMode)}</span>${pill(account.status)}</div>
    <b class="acct-name">${esc(account.strategy_name || "未启动")}</b>
    <div class="acct-equity"><strong class="num">${esc(money(summary.equity))}</strong>
      <span class="acct-total"><small>累计收益</small><em class="num ${pnlClass(returnSinceStart)}">${esc(signedPercent(returnSinceStart))}</em></span></div>
    <div class="acct-window"><span>${pairModel ? `配对同期 ${elapsedTime(pairModel.startAt, pairModel.endAt)}` : "等待配对同期"}</span>
      <b class="num ${pnlClass(matchedDelta)}">${esc(signedMoney(matchedDelta))}</b></div>
    ${comparisonSparkline(account, pairModel)}
    <small>${summary.open_position_count || 0} 持仓 · ${summary.closed_trade_count || 0} 已平仓 · 胜率 ${esc(percent(summary.win_rate, 0))}</small>
  </button>`;
}

function accountDetail(account, index) {
  const summary = account.portfolio_summary || {};
  const positions = (account.open_positions || []).map((row) => ({
    ...row,
    upnl_pct: asNumber(row.unrealized_pnl) != null && asNumber(row.entry_notional)
      ? asNumber(row.unrealized_pnl) / asNumber(row.entry_notional) : null,
  }));
  const equityDelta = asNumber(summary.unrealized_pnl);
  const sampleMinutes = Math.round(
    (asNumber(account.equity_sample_interval_seconds) || DEFAULT_EQUITY_BUCKET_SECONDS) / 60,
  );
  const chartWindow = `${dayTime(account.equity_window_start)} → ${dayTime(account.equity_window_end)} UTC`;
  const configHash = account.config_hash || "—";
  const historyLoaded = account.history_loaded === true;
  const historyAction = `<span class="history-actions">
    <span class="num muted">${historyLoaded ? "全部" : "最近"} ${(account.closed_trades || []).length} / 共 ${summary.closed_trade_count || 0}</span>
    <button class="history-button" type="button" data-load-paper-history>${historyLoaded ? "刷新全部历史" : "查看全部历史"}</button>
  </span>`;
  const detailMeta = `<div class="detail-meta">
    <span>RUN <b class="num">${esc(account.run_id || "—")}</b></span>
    <span>CONFIG <b class="num" title="${esc(configHash)}" aria-label="完整配置哈希 ${esc(configHash)}">${esc(shortHash(configHash))}</b></span>
    <span>检查点 <b class="num">${esc(relToNow(account.checkpoint_at))}</b></span>
  </div>`;
  const kpis = `<div class="tile-grid kpi-grid">
    ${tile("账户权益", money(summary.equity), "余额 + 未实现盈亏", "hero")}
    ${tile("可用余额", money(summary.balance), "初始资金 $1,000.00")}
    ${tile("已实现盈亏", signedMoney(summary.realized_pnl), `${summary.closed_trade_count || 0} 笔已平仓`, pnlClass(summary.realized_pnl))}
    ${tile("未实现盈亏", signedMoney(summary.unrealized_pnl), `${summary.open_position_count || 0} 个持仓`, pnlClass(summary.unrealized_pnl))}
    ${tile("胜率", percent(summary.win_rate), `累计手续费 ${money(summary.total_fees)}`)}
  </div>`;
  const chartBlock = `<div class="block">
    ${blockTitle("资产权益走势", `ROLLING 24H · ${sampleMinutes} MIN UTC BUCKETS`,
      `<strong class="num ${pnlClass(equityDelta)}">${esc(money(summary.equity))}</strong>`)}
    <div class="chart-context"><span>${esc(chartWindow)}</span><b class="num">${(account.equity_curve || []).length} / 240 桶</b></div>
    ${equityChart(
      account.equity_curve,
      `eq-fill-${index}`,
      account.equity_window_start,
      account.equity_window_end,
    )}
  </div>`;
  const positionsTable = dataTable([
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "方向", value: (row) => sideTag(row.side), html: true },
    { label: "开仓价", value: (row) => price(row.entry_price), align: "right" },
    { label: "标记价", value: (row) => price(row.last_mark_price), align: "right" },
    { label: "名义价值", value: (row) => money(row.entry_notional), align: "right" },
    { label: "浮动盈亏", value: (row) => signedMoney(row.unrealized_pnl), align: "right", cls: (row) => pnlClass(row.unrealized_pnl) },
    { label: "收益率", value: (row) => signedPercent(row.upnl_pct), align: "right", cls: (row) => pnlClass(row.upnl_pct) },
  ], positions, { emptyText: "当前无持仓" });
  const closedTable = dataTable([
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "方向", value: (row) => sideTag(row.side), html: true },
    { label: "开仓价", value: (row) => price(row.entry_price), align: "right" },
    { label: "平仓价", value: (row) => price(row.exit_price), align: "right" },
    { label: "净盈亏", value: (row) => signedMoney(row.realized_pnl), align: "right", cls: (row) => pnlClass(row.realized_pnl) },
    { label: "收益率", value: (row) => signedPercent(row.return_pct), align: "right", cls: (row) => pnlClass(row.return_pct) },
    { label: "平仓原因", key: "close_reason", cls: "muted" },
  ], account.closed_trades, { emptyText: "尚无已平仓交易" });
  const eventsTable = dataTable([
    { label: "时间", value: (row) => dayTime(row.occurred_at), align: "right", cls: "muted" },
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "动作", value: (row) => `<span class="event-tag ${String(row.event || "").startsWith("OPEN") ? "open" : "close"}">${esc(row.label)}</span>`, html: true },
    { label: "订单方向", key: "order_action", cls: (row) => row.order_action === "BUY" ? "pos" : "neg" },
    { label: "价格", value: (row) => price(row.price), align: "right" },
    { label: "数量", value: (row) => num(row.quantity, 3), align: "right" },
    { label: "盈亏", value: (row) => row.pnl == null ? "—" : signedMoney(row.pnl), align: "right", cls: (row) => pnlClass(row.pnl) },
    { label: "原因", key: "reason", cls: "muted" },
  ], account.trade_events, { emptyText: "尚无开平仓流水", tall: true });
  const signalsTable = dataTable([
    { label: "时间", value: (row) => dayTime(row.detected_at), align: "right", cls: "muted" },
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "方向", value: (row) => sideTag(row.side), html: true },
    { label: "原因", key: "reason", cls: "muted" },
    { label: "买入名义", value: (row) => money(row.requested_notional), align: "right" },
    { label: "触发依据", value: signalEvidence, html: true, cls: "signal-evidence-cell" },
  ], account.latest_signals, { emptyText: "尚无策略信号" });
  return `<div class="paper-account-detail" role="tabpanel">
    ${detailMeta}
    ${kpis}
    ${chartBlock}
    <div class="block-split">
      <div class="block">${blockTitle("当前持仓", "OPEN POSITIONS", `<strong class="num">${positions.length}</strong>`)}${positionsTable}</div>
      <div class="block">${blockTitle("已平仓交易", historyLoaded ? "CLOSED TRADES · FULL HISTORY" : "CLOSED TRADES · LATEST 30", historyAction)}${closedTable}</div>
    </div>
    <div class="block">${blockTitle("开平仓流水", "POSITION LIFECYCLE · BUY / SELL WITH CONTEXT")}${eventsTable}</div>
    <details class="block secondary">
      <summary>${blockTitle("策略信号", "RAW SIGNALS · LATEST 20")}</summary>
      ${signalsTable}
    </details>
  </div>`;
}

function renderStrategy(data) {
  const accounts = data.accounts || [];
  if (!accounts.length) return [data.status, emptyBox("等待模拟账户启动", "compression_breakout · orderflow_impulse · liquidation_cascade")];
  selectedPaperAccount = Math.min(selectedPaperAccount, accounts.length - 1);
  const pairModels = buildPairedEquityModels(accounts);
  const pairByStrategy = new Map(pairModels.map((model) => [model.strategyName, model]));
  const cards = `<div class="acct-cards" role="tablist" aria-label="模拟盘策略账户">${accounts
    .map((account, index) => accountCard(account, index, pairByStrategy.get(account.strategy_name)))
    .join("")}</div>`;
  const comparisons = pairModels.length
    ? `<div class="block pair-section">
      ${blockTitle("同期退出方式对比", "PAIR-MATCHED EQUITY · COMMON START · SHARED AXES",
        '<span class="muted">15M 收线 − 固定 TP / SL</span>')}
      <div class="pair-grid">${pairModels.map(pairedComparisonPanel).join("")}</div>
    </div>`
    : "";
  const selectedAccount = withPaperHistory(accounts[selectedPaperAccount]);
  return [data.status, cards + comparisons + accountDetail(selectedAccount, selectedPaperAccount)];
}

function renderUniverse(data) {
  const maxAbs = Math.max(
    ...[...(data.gainers || []), ...(data.losers || [])]
      .map((row) => Math.abs(asNumber(row.utc_day_return) ?? 0)),
    0.0001,
  );
  const universeTable = (rows) => dataTable([
    { label: "#", key: "rank", align: "right", cls: "muted" },
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "现价", value: (row) => price(row.current_price), align: "right" },
    { label: "UTC 日内涨跌", value: (row) => returnBar(row.utc_day_return, maxAbs), html: true },
  ], rows, { emptyText: "暂无数据" });
  const monitored = data.monitored_symbols || [];
  const statusBySymbol = new Map(monitored.map((row) => [row.symbol, row]));
  const targetCount = monitored.filter((row) => row.status === "target").length;
  const retainedRows = monitored.filter((row) => row.status === "retained");
  const forcedRows = monitored.filter((row) => row.status === "forced");
  const sideLabel = (side) => side === "gainer" ? "涨幅" : side === "loser" ? "跌幅" : "保护";
  const targetChips = (rows, side) => rows
    .filter((row) => (statusBySymbol.get(row.symbol)?.status || "target") === "target")
    .map((row) => `<span class="chip ${side === "gainer" ? "pos" : "neg"}">
      <span class="chip-main"><b>${esc(row.symbol)}</b><small>#${esc(row.rank)}</small></span>
      <strong class="chip-return">${esc(signedPercent(row.utc_day_return, 1))}</strong>
    </span>`)
    .join("");
  const secondaryChips = (rows) => rows
    .map((row) => `<span class="chip retained ${row.side === "gainer" ? "pos" : "neg"}">
      <span class="chip-main"><b>${esc(row.symbol)}</b><small>${esc(row.status === "forced" ? "持仓保护" : "保留")}</small></span>
      <strong class="chip-side">${esc(sideLabel(row.side))}</strong>
    </span>`)
    .join("");
  const targetGroups = `<div class="monitor-target-grid">
    <div class="monitor-group">
      ${blockTitle("目标池 · 涨幅 Top 20", "TARGET GAINERS")}
      <div class="chip-grid">${targetChips(data.gainers || [], "gainer") || emptyBox("暂无涨幅目标")}</div>
    </div>
    <div class="monitor-group">
      ${blockTitle("目标池 · 跌幅 Top 20", "TARGET LOSERS")}
      <div class="chip-grid">${targetChips(data.losers || [], "loser") || emptyBox("暂无跌幅目标")}</div>
    </div>
  </div>`;
  const secondaryRows = [...retainedRows, ...forcedRows];
  const secondarySection = secondaryRows.length
    ? `<div class="monitor-secondary">
      ${blockTitle("保留与持仓保护", "RETAINED / POSITION PROTECTION", `<span class="num muted">${retainedRows.length} 保留 · ${forcedRows.length} 保护</span>`)}
      <div class="chip-grid">${secondaryChips(secondaryRows)}</div>
    </div>`
    : "";
  const summary = `<span class="monitor-summary"><b>${targetCount}</b> 目标 · <b>${retainedRows.length}</b> 保留 · <b>${forcedRows.length}</b> 保护</span>`;
  const body = `<div class="detail-meta"><span>快照时间 <b class="num">${esc(dayTime(data.observed_at))} UTC</b></span><span>${esc(relToNow(data.observed_at))}</span></div>
    <div class="block-split">
      <div class="block">${blockTitle("涨幅榜 Top 20", "TOP GAINERS · UTC DAY")}${universeTable(data.gainers)}</div>
      <div class="block">${blockTitle("跌幅榜 Top 20", "TOP LOSERS · UTC DAY")}${universeTable(data.losers)}</div>
    </div>
    <div class="block">${blockTitle(`监控池 ${monitored.length}`, "MONITORED CANDIDATES", summary)}
      ${targetGroups}${secondarySection}</div>`;
  return [data.status, body];
}

function renderRisk(data) {
  const halts = data.active_halts?.length
    ? data.active_halts.map((halt) => `<div class="alert-box"><strong>HALT</strong><div>${esc(halt.reason)}<small>${esc(dayTime(halt.created_at))} UTC</small></div></div>`).join("")
    : `<div class="ok-box"><i></i>无活跃停机 · 风控闸门畅通</div>`;
  const ambiguousTable = dataTable([
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "客户端订单号", key: "client_order_id", cls: "num cut" },
    { label: "方向", key: "side", cls: (row) => row.side === "BUY" ? "pos" : "neg" },
    { label: "状态", value: (row) => pill(row.state), html: true },
    { label: "更新时间", value: (row) => dayTime(row.updated_at), align: "right", cls: "muted" },
  ], data.ambiguous_orders, { emptyText: "无未决订单" });
  const decisionsTable = dataTable([
    { label: "候选单", key: "candidate_id", cls: "num cut" },
    { label: "决策", value: (row) => `<span class="decision ${row.decision === "approved" ? "ok" : "no"}">${row.decision === "approved" ? "通过" : "拒绝"}</span>`, html: true },
    { label: "原因", key: "reason", cls: "muted" },
    { label: "时间", value: (row) => dayTime(row.evaluated_at), align: "right", cls: "muted" },
  ], data.latest_risk_decisions, { emptyText: "暂无风控决策流水", tall: true });
  const body = `<div class="block">${blockTitle("活跃停机", "ACTIVE HALTS")}${halts}</div>
    <div class="block-split">
      <div class="block">${blockTitle("未决订单", "AMBIGUOUS / UNRESOLVED", `<strong class="num">${(data.ambiguous_orders || []).length}</strong>`)}${ambiguousTable}</div>
      <div class="block">${blockTitle("风控决策", "RISK DECISIONS · LATEST 30")}${decisionsTable}</div>
    </div>`;
  return [data.status, body];
}

function renderAccount(data) {
  const balancesTable = dataTable([
    { label: "资产", key: "asset", cls: "sym" },
    { label: "钱包余额", value: (row) => num(row.wallet_balance, 4), align: "right" },
    { label: "可用余额", value: (row) => num(row.available_balance, 4), align: "right" },
    { label: "未实现盈亏", value: (row) => signedMoney(row.unrealized_pnl), align: "right", cls: (row) => pnlClass(row.unrealized_pnl) },
  ], data.balances, { emptyText: "尚无余额快照" });
  const positionsTable = dataTable([
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "持仓量", value: (row) => num(row.position_amt, 4), align: "right", cls: (row) => pnlClass(row.position_amt) },
    { label: "名义价值", value: (row) => money(row.notional), align: "right" },
    { label: "未实现盈亏", value: (row) => signedMoney(row.unrealized_pnl), align: "right", cls: (row) => pnlClass(row.unrealized_pnl) },
  ], data.positions, { emptyText: "交易所无持仓" });
  const ordersTable = dataTable([
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "客户端订单号", key: "client_order_id", cls: "num cut" },
    { label: "方向", key: "side", cls: (row) => row.side === "BUY" ? "pos" : "neg" },
    { label: "状态", value: (row) => pill(row.status), html: true },
    { label: "只减仓", value: (row) => row.reduce_only ? "是" : "否", cls: "muted" },
  ], data.open_orders, { emptyText: "无挂单" });
  const body = `<div class="detail-meta"><span>同步时间 <b class="num">${esc(dayTime(data.observed_at))} UTC</b></span><span>${esc(relToNow(data.observed_at))}</span></div>
    <div class="block-split">
      <div class="block">${blockTitle("资产余额", "BALANCES")}${balancesTable}</div>
      <div class="block">${blockTitle("交易所持仓", "EXCHANGE POSITIONS")}${positionsTable}</div>
    </div>
    <div class="block">${blockTitle("当前挂单", "OPEN ORDERS", `<strong class="num">${(data.open_orders || []).length}</strong>`)}${ordersTable}</div>`;
  return [data.status, body];
}

function renderReports(data) {
  const paperTable = dataTable([
    { label: "运行 ID", key: "run_id", cls: "num cut" },
    { label: "策略", key: "strategy_name", cls: "sym" },
    { label: "信号数", key: "signal_count", align: "right" },
    { label: "成交数", key: "fill_count", align: "right" },
    { label: "创建时间", value: (row) => dayTime(row.created_at), align: "right", cls: "muted" },
  ], data.paper_runs, { emptyText: "尚无纸面运行记录" });
  const shadowTable = dataTable([
    { label: "运行 ID", key: "run_id", cls: "num cut" },
    { label: "策略", key: "strategy_name", cls: "sym" },
    { label: "状态", value: (row) => pill(row.state), html: true },
    { label: "开始时间", value: (row) => dayTime(row.started_at), align: "right", cls: "muted" },
  ], data.shadow_sessions, { emptyText: "尚无影子会话" });
  const liveTable = dataTable([
    { label: "会话 ID", key: "session_id", cls: "num cut" },
    { label: "状态", value: (row) => pill(row.state), html: true },
    { label: "时间", value: (row) => dayTime(row.occurred_at), align: "right", cls: "muted" },
  ], data.live_sessions, { emptyText: "尚无实盘状态迁移" });
  const body = `<div class="block-split">
      <div class="block">${blockTitle("影子会话", "SHADOW SESSIONS")}${shadowTable}</div>
      <div class="block">${blockTitle("实盘状态迁移", "LIVE TRANSITIONS")}${liveTable}</div>
    </div>
    <div class="block">${blockTitle("纸面运行", "PAPER RUNS · LATEST 10")}${paperTable}</div>`;
  return [data.status, body];
}

/* ---------- polling engine ---------- */

const renderers = {
  overview: renderOverview,
  strategy: renderStrategy,
  universe: renderUniverse,
  risk: renderRisk,
  account: renderAccount,
  reports: renderReports,
};

function setSectionStatus(id, status) {
  const section = document.getElementById(id);
  const badge = section.querySelector(".section-state");
  badge.className = `section-state ${statusClass(status)}`;
  badge.textContent = status || "UNKNOWN";
  const dot = document.querySelector(`[data-nav-dot="${id}"]`);
  if (dot) dot.className = `nav-dot ${statusClass(status)}`;
}

function updateGlobalMode(data) {
  const live = data.services?.find((service) => service.status === "LIVE");
  const halted = (data.active_halt_count || 0) > 0;
  const mode = halted ? "HALTED" : live ? "LIVE" : "SHADOW";
  const stamp = document.getElementById("global-mode");
  stamp.className = `mode-badge ${statusClass(mode)}`;
  stamp.textContent = mode;
}

function wirePaperAccountTabs(body, data) {
  body.querySelectorAll("[data-account-index]").forEach((tab) => {
    tab.addEventListener("click", () => {
      const next = Number(tab.dataset.accountIndex);
      const account = withPaperHistory(data.accounts?.[next]);
      if (!Number.isInteger(next) || !account || next === selectedPaperAccount) return;
      selectedPaperAccount = next;
      body.querySelectorAll("[data-account-index]").forEach((candidate) => {
        const isSelected = candidate === tab;
        candidate.classList.toggle("is-active", isSelected);
        candidate.setAttribute("aria-selected", String(isSelected));
      });
      const detail = body.querySelector(".paper-account-detail");
      if (detail) detail.outerHTML = accountDetail(account, next);
      wirePaperHistoryButton(body, account, next);
    });
  });
  const account = withPaperHistory(data.accounts?.[selectedPaperAccount]);
  if (account) wirePaperHistoryButton(body, account, selectedPaperAccount);
}

function withPaperHistory(account) {
  if (!account?.run_id) return account;
  const history = paperHistoryByRun.get(account.run_id);
  if (!history) return account;
  return {
    ...account,
    closed_trades: history.closed_trades,
    trade_events: history.trade_events,
    history_loaded: true,
  };
}

function wirePaperHistoryButton(body, account, index) {
  const button = body.querySelector("[data-load-paper-history]");
  if (!button || !account?.run_id) return;
  button.addEventListener("click", () => loadPaperAccountHistory(body, account, index));
}

async function loadPaperAccountHistory(body, account, index) {
  const button = body.querySelector("[data-load-paper-history]");
  if (button) {
    button.disabled = true;
    button.textContent = "加载中…";
  }
  try {
    const runId = encodeURIComponent(account.run_id);
    const response = await fetch(`api/paper-accounts/${runId}/history`, {
      headers: { "Accept": "application/json" },
    });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const history = await response.json();
    paperHistoryByRun.set(account.run_id, history);
    const merged = withPaperHistory(account);
    const detail = body.querySelector(".paper-account-detail");
    if (detail) detail.outerHTML = accountDetail(merged, index);
    wirePaperHistoryButton(body, merged, index);
  } catch (error) {
    if (button) {
      button.disabled = false;
      button.textContent = `加载失败 · ${error.message}`;
    }
  }
}

function captureScrollState(body) {
  return {
    pageX: window.scrollX,
    pageY: window.scrollY,
    containers: Array.from(body.querySelectorAll(".table-scroll")).map((container) => ({
      left: container.scrollLeft,
      top: container.scrollTop,
    })),
  };
}

function restoreScrollState(body, state) {
  const root = document.documentElement;
  const previousBehavior = root.style.scrollBehavior;
  root.style.scrollBehavior = "auto";
  window.scrollTo(state.pageX, state.pageY);
  root.style.scrollBehavior = previousBehavior;

  body.querySelectorAll(".table-scroll").forEach((container, index) => {
    const saved = state.containers[index];
    if (!saved) return;
    container.scrollLeft = saved.left;
    container.scrollTop = saved.top;
  });
}

async function refreshSection(id) {
  const section = document.getElementById(id);
  try {
    const response = await fetch(section.dataset.endpoint, { headers: { "Accept": "application/json" } });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    const [status, html] = renderers[id](data);
    setSectionStatus(id, status);
    const body = section.querySelector(".panel-body");
    const scrollState = captureScrollState(body);
    body.innerHTML = html;
    restoreScrollState(body, scrollState);
    body.classList.remove("loading");
    body.removeAttribute("aria-busy");
    if (id === "strategy") wirePaperAccountTabs(body, data);
    if (id === "overview") updateGlobalMode(data);
  } catch (error) {
    setSectionStatus(id, "UNKNOWN");
    const body = section.querySelector(".panel-body");
    const scrollState = captureScrollState(body);
    body.classList.remove("loading");
    body.innerHTML = emptyBox("接口不可达", `${section.dataset.endpoint} · ${error.message}`);
    restoreScrollState(body, scrollState);
  }
}

async function poll() {
  if (pollInFlight) return;
  pollInFlight = true;
  try {
    await Promise.allSettled(SECTIONS.map(refreshSection));
    lastPollAt = new Date();
    const pollbar = document.getElementById("pollbar");
    pollbar.classList.remove("run");
    void pollbar.offsetWidth;
    pollbar.classList.add("run");
    renderPollState();
    updateSpy();
  } finally {
    pollInFlight = false;
  }
}

function renderPollState() {
  const label = document.getElementById("last-cycle");
  if (!lastPollAt) return;
  const delta = Math.max(0, Math.round((Date.now() - lastPollAt.getTime()) / 1000));
  label.textContent = `上次轮询 ${lastPollAt.toISOString().slice(11, 19)} UTC · ${delta}s 前`;
}

function tick() {
  document.getElementById("utc-clock").textContent = new Date().toISOString().slice(11, 19);
  renderPollState();
}

/* ---------- sidebar scroll spy ---------- */

const navLinks = new Map(
  Array.from(document.querySelectorAll(".nav a")).map((link) => [link.dataset.nav, link]),
);

const spyCards = Array.from(document.querySelectorAll("main .card"));
let spyQueued = false;

function updateSpy() {
  spyQueued = false;
  const refLine = window.innerHeight * 0.32;
  let currentId = spyCards[0]?.id;
  for (const card of spyCards) {
    if (card.getBoundingClientRect().top <= refLine) currentId = card.id;
  }
  navLinks.forEach((link, id) => link.classList.toggle("active", id === currentId));
}

window.addEventListener("scroll", () => {
  if (!spyQueued) {
    spyQueued = true;
    requestAnimationFrame(updateSpy);
  }
}, { passive: true });
window.addEventListener("resize", updateSpy, { passive: true });
updateSpy();

tick();
poll();
setInterval(tick, 1000);
setInterval(poll, POLL_MS);
