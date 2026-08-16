const SECTIONS = ["overview", "risk", "account", "strategy", "universe", "reports"];
const POLL_MS = 5000;
const DEFAULT_EQUITY_BUCKET_SECONDS = 6 * 60;
const STRATEGY_ORDER = [
  "compression_breakout",
  "orderflow_impulse",
  "liquidation_cascade",
];
const COMPARISON_SERIES_COLORS = [
  "var(--series-fixed)",
  "var(--series-candle)",
  "var(--series-amber)",
  "var(--series-violet)",
  "var(--series-cyan)",
  "var(--series-coral)",
  "var(--series-lime)",
  "var(--series-sky)",
];
let selectedPaperAccount = 0;
let pollInFlight = false;
let latestLiveService = null;
let latestLiveMode = "UNKNOWN";
const paperHistoryByRun = new Map();
const paperDetailsByRun = new Map();
const paperDetailRequests = new Map();
const paperEquityByRun = new Map();
let paperEquityRequest = null;
let latestPaperAccounts = [];
const latestSectionData = new Map();

const esc = (value) => String(value ?? "—").replace(/[&<>'"]/g, (char) => ({
  "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;"
}[char]));

const statusSlug = (status) => String(status || "UNKNOWN").trim().replace(/[^A-Za-z]+/g, "-").toUpperCase();
const statusClass = (status) => `status-${statusSlug(status)}`;
const normalizedStatus = (status) => String(status || "").trim().toUpperCase();
const UNCERTAIN_STATUSES = new Set(["UNKNOWN", "STALE", "DOWN", "NO DATA", "NO-DATA"]);
const SAFETY_SECTIONS = new Set(["overview", "risk", "account", "strategy", "universe"]);

/* ---------- formatting ---------- */

const DISPLAY_TIME_ZONE = "Asia/Shanghai";
const DISPLAY_TIME_ZONE_LABEL = "UTC+8";
const DISPLAY_TIME_FORMATTER = new Intl.DateTimeFormat("en-CA", {
  timeZone: DISPLAY_TIME_ZONE,
  calendar: "gregory",
  numberingSystem: "latn",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hourCycle: "h23",
});

const displayTimeParts = (value) => {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return null;
  return Object.fromEntries(
    DISPLAY_TIME_FORMATTER.formatToParts(parsed)
      .filter((part) => part.type !== "literal")
      .map((part) => [part.type, part.value]),
  );
};

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
  const parts = displayTimeParts(value);
  return parts ? `${parts.hour}:${parts.minute}:${parts.second}` : String(value);
};

const dayTime = (value) => {
  if (!value) return "—";
  const parts = displayTimeParts(value);
  return parts
    ? `${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:${parts.second}`
    : String(value);
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

const liveHeartbeatAge = (service) => {
  if (!service) return null;
  const observedAt = new Date(service.observed_at || "").getTime();
  if (Number.isFinite(observedAt)) {
    return Math.max(0, (Date.now() - observedAt) / 1000);
  }
  return asNumber(service.age_seconds);
};

const liveHeartbeatStatus = (age) => {
  if (age == null) return "UNKNOWN";
  return age <= 120 ? "FRESH" : "STALE";
};

function renderLiveRuntime() {
  const stamp = document.getElementById("global-mode");
  const heartbeat = document.getElementById("last-cycle");
  const heartbeatRow = heartbeat?.closest(".poll-state");
  if (!stamp || !heartbeat) return;

  const mode = latestLiveMode || "UNKNOWN";
  const startedAt = latestLiveService?.details?.started_at;
  let duration = "等待数据";
  if (mode === "LIVE") {
    duration = startedAt
      ? `已运行 ${elapsedTime(startedAt, new Date())}`
      : "运行时间未知";
  } else if (mode === "HALTED") {
    duration = "已停止";
  } else if (mode === "SHADOW") {
    duration = "未启用";
  }
  stamp.className = `mode-badge runtime-line ${statusClass(mode)}`;
  stamp.textContent = `实盘状态：${mode} · ${duration}`;

  const age = liveHeartbeatAge(latestLiveService);
  const freshness = liveHeartbeatStatus(age);
  heartbeat.textContent = age == null
    ? "实盘心跳：暂无数据 · UNKNOWN"
    : `实盘心跳：${relAge(age)} · ${freshness}`;
  if (heartbeatRow) heartbeatRow.className = `poll-state ${statusClass(freshness)}`;
}

const hasUncertainStatus = (status) => UNCERTAIN_STATUSES.has(normalizedStatus(status));

function updateGlobalState(id, data) {
  latestSectionData.set(id, data);
  renderGlobalReadiness();
}

function globalReadinessModel() {
  const overview = latestSectionData.get("overview");
  const risk = latestSectionData.get("risk");
  const account = latestSectionData.get("account");
  const snapshots = [...latestSectionData.values()].filter(Boolean);
  if (!snapshots.length) {
    return {
      status: "UNKNOWN",
      detail: "等待关键服务数据",
      uncertain: "—",
      halts: "—",
      ambiguous: "—",
      reconciliation: "—",
    };
  }

  const services = overview?.services || [];
  const uncertainSections = [...latestSectionData.entries()]
    .filter(([id, data]) => SAFETY_SECTIONS.has(id) && hasUncertainStatus(data.status))
    .length;
  const uncertainServices = services.filter((service) => hasUncertainStatus(service.status)).length;
  const uncertain = uncertainSections + uncertainServices;
  const activeHalts = Math.max(
    asNumber(overview?.active_halt_count) || 0,
    risk?.active_halts?.length || 0,
  );
  const ambiguous = risk?.ambiguous_orders?.length || 0;
  const mismatch = asNumber(account?.reconciliation?.mismatch_count);
  const accountStatus = normalizedStatus(account?.status);
  let reconciliation = "—";
  if (account) {
    reconciliation = hasUncertainStatus(accountStatus)
      ? "UNKNOWN"
      : mismatch != null && mismatch > 0
        ? `${mismatch} 差异`
        : String(account.reconciliation?.status || "READY").toUpperCase();
  }

  let status = "READY";
  let detail = "关键读数正常";
  if (!overview) {
    status = "UNKNOWN";
    detail = "等待系统总览数据";
  } else if (activeHalts > 0) {
    status = "HALTED";
    detail = "存在活跃停机 · 新入场已被阻断";
  } else if (ambiguous > 0) {
    status = "DEGRADED";
    detail = "存在未决订单 · 需要交易所对账";
  } else if (mismatch != null && mismatch > 0) {
    status = "DEGRADED";
    detail = "账户对账存在差异 · 暂不视为安全";
  } else if (uncertain > 0) {
    status = "UNKNOWN";
    detail = `${uncertain} 个关键读数需要确认`;
  } else if (latestLiveMode === "LIVE") {
    detail = "实盘链路运行中 · 关键读数正常";
  } else if (latestLiveMode === "SHADOW") {
    detail = "影子路径运行中 · 关键读数正常";
  } else {
    detail = "无实盘会话 · 只读安全";
  }

  return {
    status,
    detail,
    uncertain: String(uncertain),
    halts: String(activeHalts),
    ambiguous: String(ambiguous),
    reconciliation,
  };
}

function renderGlobalReadiness() {
  const strip = document.getElementById("readiness-strip");
  if (!strip) return;
  const model = globalReadinessModel();
  strip.className = `readiness-strip ${statusClass(model.status)}`;
  const values = {
    "global-readiness": model.status,
    "global-readiness-detail": model.detail,
    "global-uncertain": model.uncertain,
    "global-halts": model.halts,
    "global-ambiguous": model.ambiguous,
    "global-reconciliation": model.reconciliation,
  };
  Object.entries(values).forEach(([id, value]) => {
    const element = document.getElementById(id);
    if (element) element.textContent = value;
  });
}

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
  const stateKey = options.stateKey ? ` data-state-key="${esc(options.stateKey)}"` : "";
  return `<div class="table-scroll${options.tall ? " tall" : ""}"${stateKey}><table class="data-table"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`;
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

function comparisonSeriesClass(account, index) {
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

function comparisonSeriesStyle(series) {
  return `style="--series-color:${series.color}"`;
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
      colorClass: comparisonSeriesClass(account, index),
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

function buildStrategyEquityModels(accounts) {
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

function accountWindowDelta(account) {
  const values = (account.equity_curve || [])
    .map((row) => asNumber(row.equity))
    .filter((value) => value != null);
  if (values.length < 2) return null;
  return values.at(-1) - values[0];
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
    `<text x="${x(atMs).toFixed(1)}" y="${height - 8}" class="chart-label" text-anchor="${i === 0 ? "start" : i === 2 ? "end" : "middle"}">${esc(i === 1 ? timeOnly(atMs) : dayTime(atMs))}${i === 2 ? ` ${DISPLAY_TIME_ZONE_LABEL}` : ""}</text>`).join("");
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

function strategyEquityChart(model) {
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
  const dots = model.series.map((series) =>
    `<circle cx="${x(model.points.at(-1)).toFixed(1)}" cy="${y(series.delta).toFixed(1)}" r="3" class="pair-dot ${series.colorClass}" ${comparisonSeriesStyle(series)}/>`).join("");
  return `<div class="pair-chart"><svg viewBox="0 0 ${width} ${height}" role="img"
    aria-label="${esc(model.strategyName)} 各退出方式同期权益对比">
    ${grid}
    <line x1="${padL}" y1="${y(0).toFixed(1)}" x2="${width - padR}" y2="${y(0).toFixed(1)}" class="chart-baseline"/>
    ${lines}
    ${dots}
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
    ${tile("租约到期", lease?.expires_at ? relToNow(lease.expires_at) : "—", lease?.expires_at ? `${dayTime(lease.expires_at)} ${DISPLAY_TIME_ZONE_LABEL}` : "")}
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
  const bucketMinutes = Math.round(model.intervalSeconds / 60);
  const legend = model.series.map((series) =>
    `<span class="${series.colorClass}" ${comparisonSeriesStyle(series)}><i></i><em>${esc(series.label)}</em> <b class="num ${pnlClass(series.delta)}">${esc(signedMoney(series.delta))}</b></span>`).join("");
  return `<article class="pair-panel">
    <div class="pair-head">
      <div><strong>${esc(model.strategyName)}</strong>
        <small>${esc(dayTime(model.startAt))} → ${esc(dayTime(model.endAt))} ${DISPLAY_TIME_ZONE_LABEL} · ${model.points.length} 个共同桶</small></div>
      <div class="pair-spread"><span>退出版本</span><b class="num">${model.series.length} 个</b></div>
    </div>
    <div class="pair-legend">${legend}</div>
    ${strategyEquityChart(model)}
    <footer><span>共同起点归零 · ${bucketMinutes} 分钟 UTC 采样</span><b>${esc(elapsedTime(model.startAt, model.endAt))}</b></footer>
  </article>`;
}

function accountCard(account, index) {
  const summary = account.portfolio_summary || {};
  const equity = asNumber(summary.equity);
  const returnSinceStart = equity == null ? null : equity / 1000 - 1;
  const isCandle = account.exit_mode === "candle_15m";
  const hasEquity = Array.isArray(account.equity_curve);
  const windowDelta = hasEquity ? accountWindowDelta(account) : null;
  const active = index === selectedPaperAccount;
  const exitMode = account.exit_label || (isCandle ? "15M 收线退出" : "退出方式未标注");
  const accountNumber = String(index + 1).padStart(2, "0");
  const sparkline = hasEquity
    ? standaloneSparkline(account.equity_curve)
    : '<div class="spark spark-empty">详情按需加载</div>';
  return `<button class="acct-card${active ? " is-active" : ""}" type="button" role="tab"
    aria-selected="${active}" data-account-index="${index}">
    <div class="acct-top"><span>账户 ${accountNumber}</span><span class="acct-mode">${esc(exitMode)}</span>${pill(account.status)}</div>
    <b class="acct-name">${esc(account.strategy_name || "未启动")}</b>
    <div class="acct-equity"><strong class="num">${esc(money(summary.equity))}</strong>
      <span class="acct-total"><small>累计收益</small><em class="num ${pnlClass(returnSinceStart)}">${esc(signedPercent(returnSinceStart))}</em></span></div>
    <div class="acct-window"><span>滚动 24H 权益变化</span>
      <b class="num ${pnlClass(windowDelta)}">${esc(hasEquity ? signedMoney(windowDelta) : "详情加载后显示")}</b></div>
    ${sparkline}
    <small>${summary.open_position_count || 0} 持仓 · ${summary.closed_trade_count || 0} 已平仓 · 胜率 ${esc(percent(summary.win_rate, 0))}</small>
  </button>`;
}

function strategyAccountColumn(accounts, strategyName, index) {
  const entries = accounts
    .map((account, accountIndex) => ({ account, accountIndex }))
    .filter(({ account }) => account.strategy_name === strategyName);
  if (!entries.length) return "";
  return `<section class="acct-strategy-column" aria-label="${esc(strategyName)} 模拟账户">
    <header class="acct-strategy-head">
      <div><span>策略 0${index + 1}</span><strong>${esc(strategyName)}</strong></div>
      <small>${entries.length} 个账户</small>
    </header>
    <div class="acct-strategy-cards" role="tablist" aria-label="${esc(strategyName)}退出版本">${entries
      .map(({ account, accountIndex }) => accountCard(account, accountIndex))
      .join("")}</div>
  </section>`;
}

function withPaperEquity(account) {
  if (!account?.run_id) return account;
  const equity = paperEquityByRun.get(account.run_id);
  return equity ? { ...account, ...equity } : account;
}

function paperCards(accounts) {
  return `<div class="acct-cards" role="tablist" aria-label="模拟盘策略账户">${STRATEGY_ORDER
    .map((strategyName, index) => strategyAccountColumn(accounts, strategyName, index))
    .join("")}</div>`;
}

function paperComparisonBlock(accounts) {
  const paperAccounts = accounts.map(withPaperEquity);
  const liveAccounts = [...paperEquityByRun.values()].filter(
    (account) => account.source === "live",
  );
  const comparisonModels = buildStrategyEquityModels([
    ...paperAccounts,
    ...liveAccounts,
  ]);
  const content = comparisonModels.length
    ? `<div class="pair-grid">${comparisonModels.map(pairedComparisonPanel).join("")}</div>`
    : emptyBox("同期权益曲线加载中", "账户摘要已就绪，曲线在后台批量加载");
  return `<div class="block pair-section" data-paper-comparison>
    ${blockTitle("同期退出方式对比", "STRATEGY EXIT EQUITY · COMMON START · SHARED AXES",
      '<span class="muted">模拟盘版本 + 实盘 B1 · 同期叠加</span>')}
    ${content}
  </div>`;
}

function paperDetailPlaceholder(account, index, message = "账户详情按需加载") {
  return `<div class="paper-account-detail" data-run-id="${esc(account?.run_id || "")}" role="tabpanel">
    <div class="lazy-detail">
      <strong>${esc(message)}</strong>
      <small>首屏只查询账户摘要；曲线、持仓、交易和信号将在选中后加载。</small>
      <button class="history-button" type="button" data-load-paper-detail>加载账户详情</button>
    </div>
  </div>`;
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
  const chartWindow = `${dayTime(account.equity_window_start)} → ${dayTime(account.equity_window_end)} ${DISPLAY_TIME_ZONE_LABEL}`;
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
  ], positions, { emptyText: "当前无持仓", stateKey: "paper-open-positions" });
  const closedTable = dataTable([
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "方向", value: (row) => sideTag(row.side), html: true },
    { label: "开仓价", value: (row) => price(row.entry_price), align: "right" },
    { label: "平仓价", value: (row) => price(row.exit_price), align: "right" },
    { label: "净盈亏", value: (row) => signedMoney(row.realized_pnl), align: "right", cls: (row) => pnlClass(row.realized_pnl) },
    { label: "收益率", value: (row) => signedPercent(row.return_pct), align: "right", cls: (row) => pnlClass(row.return_pct) },
    { label: "平仓原因", key: "close_reason", cls: "muted" },
  ], account.closed_trades, { emptyText: "尚无已平仓交易", stateKey: "paper-closed-trades" });
  const eventsTable = dataTable([
    { label: "时间", value: (row) => dayTime(row.occurred_at), align: "right", cls: "muted" },
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "动作", value: (row) => `<span class="event-tag ${String(row.event || "").startsWith("OPEN") ? "open" : "close"}">${esc(row.label)}</span>`, html: true },
    { label: "订单方向", key: "order_action", cls: (row) => row.order_action === "BUY" ? "pos" : "neg" },
    { label: "价格", value: (row) => price(row.price), align: "right" },
    { label: "数量", value: (row) => num(row.quantity, 3), align: "right" },
    { label: "盈亏", value: (row) => row.pnl == null ? "—" : signedMoney(row.pnl), align: "right", cls: (row) => pnlClass(row.pnl) },
    { label: "原因", key: "reason", cls: "muted" },
  ], account.trade_events, { emptyText: "尚无开平仓流水", tall: true, stateKey: "paper-trade-events" });
  const signalsTable = dataTable([
    { label: "时间", value: (row) => dayTime(row.detected_at), align: "right", cls: "muted" },
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "方向", value: (row) => sideTag(row.side), html: true },
    { label: "原因", key: "reason", cls: "muted" },
    { label: "买入名义", value: (row) => money(row.requested_notional), align: "right" },
    { label: "触发依据", value: signalEvidence, html: true, cls: "signal-evidence-cell" },
  ], account.latest_signals, { emptyText: "尚无策略信号", stateKey: "paper-strategy-signals" });
  return `<div class="paper-account-detail" data-run-id="${esc(account.run_id || "")}" role="tabpanel">
    ${detailMeta}
    ${kpis}
    ${chartBlock}
    <div class="block-split">
      <div class="block">${blockTitle("当前持仓", "OPEN POSITIONS", `<strong class="num">${positions.length}</strong>`)}${positionsTable}</div>
      <div class="block">${blockTitle("已平仓交易", historyLoaded ? "CLOSED TRADES · FULL HISTORY" : "CLOSED TRADES · LATEST 30", historyAction)}${closedTable}</div>
    </div>
    <div class="block">${blockTitle("开平仓流水", "POSITION LIFECYCLE · BUY / SELL WITH CONTEXT")}${eventsTable}</div>
    <details class="block secondary" data-state-key="paper-strategy-signals">
      <summary>${blockTitle("策略信号", "RAW SIGNALS · LATEST 20")}</summary>
      ${signalsTable}
    </details>
  </div>`;
}

function renderStrategy(data) {
  const accounts = (data.accounts || []).filter((account) => account.exit_mode !== "fixed");
  if (!accounts.length) return [data.status, emptyBox("等待模拟账户启动", "compression_breakout · orderflow_impulse · liquidation_cascade")];
  latestPaperAccounts = accounts;
  selectedPaperAccount = Math.min(selectedPaperAccount, accounts.length - 1);
  const cards = paperCards(accounts.map(withPaperEquity));
  const selectedSummary = accounts[selectedPaperAccount];
  const selectedAccount = withPaperHistory({
    ...selectedSummary,
    ...paperDetailsByRun.get(selectedSummary.run_id),
  });
  const detail = paperDetailsByRun.has(selectedSummary.run_id)
    ? accountDetail(selectedAccount, selectedPaperAccount)
    : paperDetailPlaceholder(selectedSummary, selectedPaperAccount);
  return [data.status, cards + paperComparisonBlock(accounts) + detail];
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
  const body = `<div class="detail-meta"><span>快照时间 <b class="num">${esc(dayTime(data.observed_at))} ${DISPLAY_TIME_ZONE_LABEL}</b></span><span>${esc(relToNow(data.observed_at))}</span></div>
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
    ? data.active_halts.map((halt) => `<div class="alert-box"><strong>HALT</strong><div>${esc(halt.reason)}<small>${esc(dayTime(halt.created_at))} ${DISPLAY_TIME_ZONE_LABEL}</small></div></div>`).join("")
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
  const summary = data.summary || {};
  const config = data.account_config || {};
  const reconciliation = data.reconciliation || {};
  const permission = (value) => value == null ? "未知" : value ? "是 · API 已授权" : "否 · API 返回";
  const permissionClass = (value) => value == null ? "muted" : value ? "pos" : "warn";
  const modeLabel = (value, yesLabel, noLabel) => value == null ? "—" : value ? yesLabel : noLabel;
  const reconciliationLabel = (value) => ({
    ready: "已完成",
    halted: "已中止",
    degraded: "降级",
  }[String(value || "").toLowerCase()] || value || "—");
  const strategy = (value) => value
    ? `<span class="account-strategy">${esc(value)}</span>`
    : `<span class="muted">未关联</span>`;
  const hero = `<div class="account-hero">
    <div>
      <div class="account-eyebrow">${esc(String(data.environment || "LIVE").toUpperCase())} · EXECUTION ACCOUNT</div>
      <h3>${esc(data.account_label || "交易所账户")}</h3>
      <p>账户状态由 execution-account 同步；实盘订单由 live-strategy 执行并按客户端订单号回链。</p>
    </div>
    <div class="account-hero-meta">
      ${pill(data.status)}
      <span>同步 <b class="num">${esc(dayTime(data.observed_at))}</b></span>
      <small>${esc(relToNow(data.observed_at))}</small>
    </div>
  </div>`;
  const kpis = `<div class="tile-grid account-kpi-grid">
    ${tile("USDT 钱包余额", money(summary.usdt_wallet_balance), "账户钱包余额", "hero")}
    ${tile("USDT 可用余额", money(summary.usdt_available_balance), "可用于开仓/保证金")}
    ${tile("总未实现盈亏", signedMoney(summary.total_unrealized_pnl), `${summary.position_count || 0} 个交易所持仓`, pnlClass(summary.total_unrealized_pnl))}
    ${tile("持仓名义价值", money(summary.gross_position_notional), "当前交易所总暴露")}
    ${tile("挂单 / 最近成交", `${summary.open_order_count ?? 0} / ${summary.recent_trade_count ?? summary.recent_fill_count ?? 0}`, "当前挂单 / 最近 20 笔订单")}
  </div>`;
  const accountFacts = `<div class="account-facts">
    <div><span>实盘下单通道</span><b class="pos">live-strategy</b></div>
    <div><span>账户 API 交易权限</span><b class="${permissionClass(config.can_trade)}" title="Binance 账户快照 canTrade 字段">${esc(permission(config.can_trade))}</b></div>
    <div><span>持仓模式</span><b>${esc(modeLabel(config.hedge_mode, "Hedge · 双向", "One-way · 单向"))}</b></div>
    <div><span>保证金模式</span><b>${esc(modeLabel(config.multi_assets_mode, "Multi-Assets · 多资产", "Single-Asset · 单资产"))}</b></div>
    <div><span>手续费等级</span><b>${esc(config.fee_tier == null ? "—" : `VIP ${config.fee_tier}`)}</b></div>
    <div><span>对账状态</span><b>${esc(reconciliationLabel(reconciliation.status))}</b></div>
    <div><span>对账差异项</span><b class="${asNumber(reconciliation.mismatch_count) > 0 ? "neg" : "pos"}">${esc(reconciliation.mismatch_count == null ? "—" : `${reconciliation.mismatch_count} 项`)}</b></div>
    <div><span>对账快照 资产 / 持仓</span><b>${esc(`${reconciliation.balance_count ?? "—"} / ${reconciliation.position_count ?? "—"}`)}</b></div>
    <div><span>对账快照 挂单 / 成交</span><b>${esc(`${reconciliation.open_order_count ?? "—"} / ${reconciliation.fill_count ?? "—"}`)}</b></div>
  </div>
  <p class="account-facts-note"><b>怎么读：</b>账户 API 交易权限只表示 Binance 账户快照的 <code>canTrade</code>；实盘是否提交订单由 <code>live-strategy</code> 的下单开关与风控闸门决定。对账是把余额、持仓、挂单和成交快照写入数据库并检查差异，<code>已完成 / 0 项</code> 表示本次对账成功且没有发现不一致。</p>`;
  const usdtBalances = (data.balances || []).filter((row) => String(row.asset || "").toUpperCase() === "USDT");
  const balancesTable = dataTable([
    { label: "资产", key: "asset", cls: "sym" },
    { label: "钱包余额", value: (row) => num(row.wallet_balance, 4), align: "right" },
    { label: "可用余额", value: (row) => num(row.available_balance, 4), align: "right" },
    { label: "未实现盈亏", value: (row) => signedMoney(row.unrealized_pnl), align: "right", cls: (row) => pnlClass(row.unrealized_pnl) },
  ], usdtBalances, { emptyText: "尚无 USDT 余额快照", tall: true });
  const positionRows = (data.positions || []).map((row) => ({
    ...row,
    roi: asNumber(row.entry_notional) ? asNumber(row.unrealized_pnl) / asNumber(row.entry_notional) : null,
  }));
  const positionsTable = dataTable([
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "方向", value: (row) => pill(row.position_side || "BOTH"), html: true },
    { label: "策略", value: (row) => strategy(row.strategy_name), html: true },
    { label: "持仓量", value: (row) => num(row.position_amt, 4), align: "right" },
    { label: "开仓 / 标记", value: (row) => `${price(row.entry_price)} / ${price(row.mark_price)}`, align: "right" },
    { label: "杠杆", value: (row) => row.leverage ? `${esc(row.leverage)}x` : "—", align: "right", cls: "muted" },
    { label: "保证金", value: (row) => row.margin_type || "—", cls: "muted" },
    { label: "名义价值", value: (row) => money(row.notional), align: "right" },
    { label: "未实现盈亏", value: (row) => signedMoney(row.unrealized_pnl), align: "right", cls: (row) => pnlClass(row.unrealized_pnl) },
    { label: "ROI", value: (row) => signedPercent(row.roi), align: "right", cls: (row) => pnlClass(row.roi) },
  ], positionRows, { emptyText: "交易所无持仓", tall: true });
  const ordersTable = dataTable([
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "策略", value: (row) => strategy(row.strategy_name), html: true },
    { label: "方向", key: "side", cls: (row) => row.side === "BUY" ? "pos" : "neg" },
    { label: "类型 / 价格", value: (row) => `${row.order_type || "—"} / ${price(row.price)}`, align: "right" },
    { label: "数量", value: (row) => `${num(row.executed_quantity, 4)} / ${num(row.original_quantity, 4)}`, align: "right" },
    { label: "状态", value: (row) => pill(row.status), html: true },
    { label: "只减仓", value: (row) => row.reduce_only ? "是" : "否", cls: "muted" },
    { label: "更新时间", value: (row) => dayTime(row.observed_at), align: "right", cls: "muted" },
  ], data.open_orders, { emptyText: "无挂单", tall: true });
  const fillsTable = dataTable([
    { label: "时间", value: (row) => dayTime(row.trade_at), align: "right", cls: "muted" },
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "订单", value: (row) => shortHash(row.order_id), cls: "num cut" },
    { label: "策略", value: (row) => strategy(row.strategy_name), html: true },
    { label: "方向", key: "side", cls: (row) => row.side === "BUY" ? "pos" : "neg" },
    { label: "均价", value: (row) => price(row.price), align: "right" },
    { label: "数量", value: (row) => num(row.quantity, 4), align: "right" },
    { label: "成交片数", value: (row) => `${row.fill_count || 1} 片`, align: "right", cls: "muted" },
    { label: "已实现盈亏", value: (row) => signedMoney(row.realized_pnl), align: "right", cls: (row) => pnlClass(row.realized_pnl) },
    { label: "手续费", value: (row) => `${num(row.fee, 4)} ${row.fee_asset || ""}`, align: "right" },
  ], data.fills, { emptyText: "尚无成交记录", tall: true });
  const body = `<div class="detail-meta"><span>同步时间 <b class="num">${esc(dayTime(data.observed_at))} ${DISPLAY_TIME_ZONE_LABEL}</b></span><span>${esc(relToNow(data.observed_at))}</span></div>
    ${hero}${kpis}
    <div class="block-split account-overview-grid">
      <div class="block">${blockTitle("账户权限与对账", "EXECUTION CHANNEL / RECONCILIATION")}${accountFacts}</div>
      <div class="block">${blockTitle("USDT 资产余额", "USDT BALANCE · ACCOUNT COLLATERAL")}${balancesTable}</div>
    </div>
    <div class="block">${blockTitle("交易所持仓", "EXCHANGE POSITIONS · STRATEGY ATTRIBUTION", `<strong class="num">${(data.positions || []).length}</strong>`)}${positionsTable}</div>
    <div class="block">${blockTitle("当前挂单", "OPEN ORDERS · EXCHANGE SOURCE OF TRUTH", `<strong class="num">${(data.open_orders || []).length}</strong>`)}${ordersTable}</div>
    <div class="block">${blockTitle("最近成交订单", "RECENT TRADES · ONE ORDER PER ROW", `<strong class="num">${(data.fills || []).length}</strong>`)}${fillsTable}</div>`;
  return [data.status, body];
}

function renderReports(data) {
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
    </div>`;
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
  const live = data.services?.find((service) => service.name === "live-rollout");
  const halted = (data.active_halt_count || 0) > 0;
  const mode = halted
    ? "HALTED"
    : live?.status === "LIVE"
      ? "LIVE"
      : live?.status === "HALTED"
        ? "HALTED"
        : live?.status === "SHADOW"
          ? "SHADOW"
          : "UNKNOWN";
  latestLiveService = live || null;
  latestLiveMode = mode;
  renderLiveRuntime();
  renderGlobalReadiness();
}

function wirePaperAccountTabs(body, data) {
  body.querySelectorAll("[data-account-index]").forEach((tab) => {
    tab.addEventListener("click", () => {
      const next = Number(tab.dataset.accountIndex);
      const account = data.accounts?.[next];
      if (!Number.isInteger(next) || !account || next === selectedPaperAccount) return;
      selectedPaperAccount = next;
      body.querySelectorAll("[data-account-index]").forEach((candidate) => {
        const isSelected = candidate === tab;
        candidate.classList.toggle("is-active", isSelected);
        candidate.setAttribute("aria-selected", String(isSelected));
      });
      mountPaperDetail(body, account, next);
      void loadPaperAccountDetail(body, account, next);
    });
  });
  const account = data.accounts?.[selectedPaperAccount];
  if (account) {
    const mounted = body.querySelector(".paper-account-detail");
    if (mounted?.dataset.runId !== account.run_id) {
      mountPaperDetail(body, account, selectedPaperAccount);
    } else {
      wirePaperDetailControls(body, account, selectedPaperAccount);
    }
    void loadPaperAccountDetail(body, account, selectedPaperAccount);
  }
  void loadPaperEquityComparison(body);
}

function withPaperDetail(account) {
  if (!account?.run_id) return account;
  const detail = paperDetailsByRun.get(account.run_id);
  return detail ? { ...account, ...detail } : account;
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

function currentPaperAccountIs(account) {
  return latestPaperAccounts[selectedPaperAccount]?.run_id === account?.run_id;
}

function replacePaperDetail(body, html, preserveState = true) {
  const detail = body.querySelector(".paper-account-detail");
  if (!detail) return null;
  const scrollState = preserveState ? captureScrollState(body) : null;
  detail.outerHTML = html;
  if (scrollState) restoreScrollState(body, scrollState);
  return body.querySelector(".paper-account-detail");
}

function mountPaperDetail(body, account, index) {
  const detail = body.querySelector(".paper-account-detail");
  if (!detail) return;
  const merged = withPaperHistory(withPaperDetail(account));
  const html = paperDetailsByRun.has(account.run_id)
    ? accountDetail(merged, index)
    : paperDetailPlaceholder(account, index);
  replacePaperDetail(body, html, detail.dataset.runId === account.run_id);
  wirePaperDetailControls(body, account, index);
}

function wirePaperDetailControls(body, account, index) {
  wirePaperDetailButton(body, account, index);
  if (paperDetailsByRun.has(account.run_id)) {
    wirePaperHistoryButton(body, withPaperHistory(withPaperDetail(account)), index);
  }
}

function wirePaperDetailButton(body, account, index) {
  const button = body.querySelector("[data-load-paper-detail]");
  if (!button || !account?.run_id || button.dataset.wired === "true") return;
  button.dataset.wired = "true";
  button.addEventListener("click", () => void loadPaperAccountDetail(body, account, index));
}

async function loadPaperAccountDetail(body, account, index) {
  if (!account?.run_id) return;
  const existingRequest = paperDetailRequests.get(account.run_id);
  if (existingRequest) return existingRequest;
  const button = body.querySelector("[data-load-paper-detail]");
  if (button) {
    button.disabled = true;
    button.textContent = "加载中…";
  }
  const request = (async () => {
    try {
      const runId = encodeURIComponent(account.run_id);
      const response = await fetch(`api/paper-accounts/${runId}`, {
        headers: { "Accept": "application/json" },
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const detail = await response.json();
      paperDetailsByRun.set(account.run_id, detail);
      if (currentPaperAccountIs(account)) {
        mountPaperDetail(body, account, index);
      }
    } catch (error) {
      if (currentPaperAccountIs(account)) {
        replacePaperDetail(body, paperDetailPlaceholder(account, index, `详情加载失败 · ${error.message}`));
        wirePaperDetailButton(body, account, index);
      }
    } finally {
      paperDetailRequests.delete(account.run_id);
    }
  })();
  paperDetailRequests.set(account.run_id, request);
  return request;
}

async function loadPaperEquityComparison(body) {
  if (paperEquityRequest) return paperEquityRequest;
  paperEquityRequest = (async () => {
    try {
      const response = await fetch("api/paper-accounts/equity", {
        headers: { "Accept": "application/json" },
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json();
      paperEquityByRun.clear();
      for (const account of data.accounts || []) paperEquityByRun.set(account.run_id, account);
      const comparison = body.querySelector("[data-paper-comparison]");
      if (comparison) comparison.outerHTML = paperComparisonBlock(latestPaperAccounts);
      const cards = body.querySelector(".acct-cards");
      if (cards) {
        cards.outerHTML = paperCards(latestPaperAccounts.map(withPaperEquity));
        wirePaperAccountTabs(body, { accounts: latestPaperAccounts });
      }
    } catch (error) {
      const comparison = body.querySelector("[data-paper-comparison]");
      if (comparison) comparison.outerHTML = `<div class="block pair-section" data-paper-comparison>${emptyBox("权益曲线加载失败", error.message)}</div>`;
    } finally {
      paperEquityRequest = null;
    }
  })();
  return paperEquityRequest;
}

function wirePaperHistoryButton(body, account, index) {
  const button = body.querySelector("[data-load-paper-history]");
  if (!button || !account?.run_id || button.dataset.wired === "true") return;
  button.dataset.wired = "true";
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
    const merged = withPaperHistory(withPaperDetail(account));
    replacePaperDetail(body, accountDetail(merged, index));
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
      key: container.dataset.stateKey || null,
      left: container.scrollLeft,
      top: container.scrollTop,
    })),
    disclosures: Array.from(body.querySelectorAll("details")).map((details) => ({
      key: details.dataset.stateKey || null,
      open: details.open,
    })),
  };
}

function restoreScrollState(body, state) {
  const disclosureStates = new Map(
    state.disclosures.filter((saved) => saved.key).map((saved) => [saved.key, saved]),
  );
  body.querySelectorAll("details").forEach((details, index) => {
    const saved = details.dataset.stateKey
      ? disclosureStates.get(details.dataset.stateKey)
      : state.disclosures[index];
    if (!saved) return;
    details.open = saved.open;
  });

  const containerStates = new Map(
    state.containers.filter((saved) => saved.key).map((saved) => [saved.key, saved]),
  );
  body.querySelectorAll(".table-scroll").forEach((container, index) => {
    const saved = container.dataset.stateKey
      ? containerStates.get(container.dataset.stateKey)
      : state.containers[index];
    if (!saved) return;
    container.scrollLeft = saved.left;
    container.scrollTop = saved.top;
  });

  const root = document.documentElement;
  const previousBehavior = root.style.scrollBehavior;
  root.style.scrollBehavior = "auto";
  window.scrollTo(state.pageX, state.pageY);
  root.style.scrollBehavior = previousBehavior;
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
    updateGlobalState(id, data);
  } catch (error) {
    setSectionStatus(id, "UNKNOWN");
    const body = section.querySelector(".panel-body");
    const scrollState = captureScrollState(body);
    body.classList.remove("loading");
    body.innerHTML = emptyBox("接口不可达", `${section.dataset.endpoint} · ${error.message}`);
    restoreScrollState(body, scrollState);
    updateGlobalState(id, { status: "UNKNOWN", error: true });
  }
}

async function poll() {
  if (pollInFlight) return;
  pollInFlight = true;
  try {
    await Promise.allSettled(SECTIONS.map(refreshSection));
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
  renderLiveRuntime();
}

function tick() {
  document.getElementById("utc-clock").textContent = timeOnly(new Date());
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
