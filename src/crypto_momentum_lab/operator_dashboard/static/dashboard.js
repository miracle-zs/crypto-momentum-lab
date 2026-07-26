const SECTIONS = ["overview", "strategy", "universe", "risk", "account", "reports"];
const POLL_MS = 2000;
let selectedPaperAccount = 0;
let lastPollAt = null;

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

/* ---------- building blocks ---------- */

const pill = (status) => `<span class="pill ${statusClass(status)}"><i></i>${esc(status || "UNKNOWN")}</span>`;

const sideTag = (side) => side === "long"
  ? '<span class="side-tag long">多</span>'
  : '<span class="side-tag short">空</span>';

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

/* ---------- charts ---------- */

function sparkline(rows) {
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

function equityChart(rows, chartId = "eq") {
  const points = (rows || [])
    .map((row) => ({ at: row.observed_at, equity: asNumber(row.equity) }))
    .filter((point) => point.equity != null);
  if (points.length < 2) return emptyBox("等待第一条权益快照", "策略进程每 15 秒写入一次快照");
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
  const x = (index) => padL + (index / (points.length - 1)) * (width - padL - padR);
  const y = (value) => padT + ((max - value) / (max - min)) * (height - padT - padB);
  const line = values.map((value, index) => `${x(index).toFixed(1)},${y(value).toFixed(1)}`).join(" ");
  const area = `${padL},${(height - padB).toFixed(1)} ${line} ${(width - padR).toFixed(1)},${(height - padB).toFixed(1)}`;
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
  const timeAxis = [0, Math.floor((points.length - 1) / 2), points.length - 1].map((index, i) =>
    `<text x="${x(index).toFixed(1)}" y="${height - 8}" class="chart-label" text-anchor="${i === 0 ? "start" : i === 2 ? "end" : "middle"}">${esc(timeOnly(points[index].at))}${i === 2 ? " UTC" : ""}</text>`).join("");
  return `<div class="equity-chart"><svg viewBox="0 0 ${width} ${height}" role="img" aria-label="账户权益曲线">
    <defs><linearGradient id="${esc(chartId)}" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0" stop-color="${lastUp ? "var(--up)" : "var(--down)"}" stop-opacity=".22"/>
      <stop offset="1" stop-color="${lastUp ? "var(--up)" : "var(--down)"}" stop-opacity="0"/>
    </linearGradient></defs>
    ${grid}${baseline}
    <polygon points="${area}" fill="url(#${esc(chartId)})"/>
    <polyline points="${line}" class="equity-line ${lastUp ? "pos" : "neg"}"/>
    <circle cx="${x(values.length - 1).toFixed(1)}" cy="${y(lastValue).toFixed(1)}" r="4" class="equity-dot ${lastUp ? "pos" : "neg"}"/>
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

function accountCard(account, index) {
  const summary = account.portfolio_summary || {};
  const equity = asNumber(summary.equity);
  const returnSinceStart = equity == null ? null : equity / 1000 - 1;
  const active = index === selectedPaperAccount;
  return `<button class="acct-card${active ? " is-active" : ""}" type="button" role="tab"
    aria-selected="${active}" data-account-index="${index}">
    <div class="acct-top"><span>账户 0${index + 1}</span>${pill(account.status)}</div>
    <b class="acct-name">${esc(account.strategy_name || "未启动")}</b>
    <div class="acct-equity"><strong class="num">${esc(money(summary.equity))}</strong>
      <em class="num ${pnlClass(returnSinceStart)}">${esc(signedPercent(returnSinceStart))}</em></div>
    ${sparkline(account.equity_curve)}
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
  const detailMeta = `<div class="detail-meta">
    <span>RUN <b class="num">${esc(account.run_id || "—")}</b></span>
    <span>CONFIG <b class="num">${esc(account.config_hash || "—")}</b></span>
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
    ${blockTitle("资产权益走势", "ACCOUNT EQUITY · LAST 240 SNAPSHOTS",
      `<strong class="num ${pnlClass(equityDelta)}">${esc(money(summary.equity))}</strong>`)}
    ${equityChart(account.equity_curve, `eq-fill-${index}`)}
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
  ], account.latest_signals, { emptyText: "尚无策略信号" });
  return `<div class="paper-account-detail" role="tabpanel">
    ${detailMeta}
    ${kpis}
    ${chartBlock}
    <div class="block-split">
      <div class="block">${blockTitle("当前持仓", "OPEN POSITIONS", `<strong class="num">${positions.length}</strong>`)}${positionsTable}</div>
      <div class="block">${blockTitle("已平仓交易", "CLOSED TRADES", `<strong class="num">${(account.closed_trades || []).length}</strong>`)}${closedTable}</div>
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
  if (!accounts.length) return [data.status, emptyBox("等待三个模拟账户启动", "compression_breakout · orderflow_impulse · liquidation_cascade")];
  selectedPaperAccount = Math.min(selectedPaperAccount, accounts.length - 1);
  const cards = `<div class="acct-cards" role="tablist" aria-label="模拟盘策略账户">${accounts.map(accountCard).join("")}</div>`;
  return [data.status, cards + accountDetail(accounts[selectedPaperAccount], selectedPaperAccount)];
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
  const chips = (data.monitored_symbols || []).map((row) =>
    `<span class="chip ${row.side === "gainer" ? "pos" : "neg"}"><b>${esc(row.symbol)}</b><small>${row.side === "gainer" ? "涨幅榜" : "跌幅榜"}</small></span>`).join("");
  const body = `<div class="detail-meta"><span>快照时间 <b class="num">${esc(dayTime(data.observed_at))} UTC</b></span><span>${esc(relToNow(data.observed_at))}</span></div>
    <div class="block-split">
      <div class="block">${blockTitle("涨幅榜 Top 20", "TOP GAINERS · UTC DAY")}${universeTable(data.gainers)}</div>
      <div class="block">${blockTitle("跌幅榜 Top 20", "TOP LOSERS · UTC DAY")}${universeTable(data.losers)}</div>
    </div>
    <div class="block">${blockTitle("监控池 40", "MONITORED CANDIDATES", `<span class="num muted">${(data.monitored_symbols || []).length} 个标的</span>`)}
      ${chips ? `<div class="chip-grid">${chips}</div>` : emptyBox("监控池为空")}</div>`;
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

async function refreshSection(id) {
  const section = document.getElementById(id);
  try {
    const response = await fetch(section.dataset.endpoint, { headers: { "Accept": "application/json" } });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    const [status, html] = renderers[id](data);
    setSectionStatus(id, status);
    const body = section.querySelector(".panel-body");
    body.innerHTML = html;
    body.classList.remove("loading");
    body.removeAttribute("aria-busy");
    if (id === "strategy") {
      body.querySelectorAll("[data-account-index]").forEach((tab) => {
        tab.addEventListener("click", () => {
          const next = Number(tab.dataset.accountIndex);
          if (next !== selectedPaperAccount) {
            selectedPaperAccount = next;
            refreshSection("strategy");
          }
        });
      });
    }
    if (id === "overview") updateGlobalMode(data);
  } catch (error) {
    setSectionStatus(id, "UNKNOWN");
    const body = section.querySelector(".panel-body");
    body.classList.remove("loading");
    body.innerHTML = emptyBox("接口不可达", `${section.dataset.endpoint} · ${error.message}`);
  }
}

async function poll() {
  await Promise.allSettled(SECTIONS.map(refreshSection));
  lastPollAt = new Date();
  const pollbar = document.getElementById("pollbar");
  pollbar.classList.remove("run");
  void pollbar.offsetWidth;
  pollbar.classList.add("run");
  renderPollState();
  updateSpy();
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
