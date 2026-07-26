const sections = ["overview", "risk", "universe", "strategy", "account", "reports"];

const esc = (value) => String(value ?? "NO DATA").replace(/[&<>'"]/g, (char) => ({
  "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;"
}[char]));

const statusClass = (status) => `status-${String(status || "UNKNOWN").replaceAll("_", " ")}`;

function setStatus(section, status) {
  const badge = section.querySelector(".section-state");
  badge.className = `section-state ${statusClass(status)}`;
  badge.textContent = status || "UNKNOWN";
}

function metric(label, value, detail = "") {
  return `<div class="metric"><label>${esc(label)}</label><strong>${esc(value)}</strong><small>${esc(detail)}</small></div>`;
}

function empty(label = "NO DATA") { return `<div class="empty">${esc(label)}</div>`; }

function table(rows, columns) {
  if (!rows?.length) return empty();
  return `<table class="data-table"><thead><tr>${columns.map(([label]) => `<th>${esc(label)}</th>`).join("")}</tr></thead><tbody>${rows.map((row) => `<tr>${columns.map(([, key, klass]) => `<td class="${klass ? (row[klass] || klass) : ""}">${esc(row[key])}</td>`).join("")}</tr>`).join("")}</tbody></table>`;
}

function symbolGrid(rows) {
  if (!rows?.length) return empty();
  return `<div class="symbol-grid">${rows.map((row) => `<span class="symbol-pill ${row.side === "gainer" ? "positive" : "negative"}"><b>${esc(row.symbol)}</b><small>${esc(row.side)}</small></span>`).join("")}</div>`;
}

const number = (value, digits = 2) => value == null ? "NO DATA" : Number(value).toFixed(digits);
const money = (value) => value == null ? "NO DATA" : `${Number(value) >= 0 ? "" : "−"}$${Math.abs(Number(value)).toFixed(2)}`;
const percent = (value) => value == null ? "NO DATA" : `${(Number(value) * 100).toFixed(2)}%`;
const pnlClass = (value) => Number(value) > 0 ? "positive" : Number(value) < 0 ? "negative" : "";

function equityChart(rows, chartId = "equity-fill") {
  if (!rows?.length) return empty("等待第一条权益快照");
  const values = rows.map((row) => Number(row.equity));
  let min = Math.min(...values);
  let max = Math.max(...values);
  if (min === max) { min -= 1; max += 1; }
  const width = 1000;
  const height = 250;
  const pad = 34;
  const x = (index) => pad + (index / Math.max(rows.length - 1, 1)) * (width - pad * 2);
  const y = (value) => pad + ((max - value) / (max - min)) * (height - pad * 2);
  const line = values.map((value, index) => `${x(index)},${y(value)}`).join(" ");
  const area = `${pad},${height - pad} ${line} ${width - pad},${height - pad}`;
  const first = new Date(rows[0].observed_at).toISOString().slice(11, 16);
  const last = new Date(rows.at(-1).observed_at).toISOString().slice(11, 16);
  return `<div class="equity-chart"><svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Paper account equity curve"><defs><linearGradient id="${chartId}" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#4cc9c0" stop-opacity=".34"/><stop offset="1" stop-color="#4cc9c0" stop-opacity="0"/></linearGradient></defs><line x1="${pad}" y1="${pad}" x2="${width - pad}" y2="${pad}" class="chart-grid"/><line x1="${pad}" y1="${height - pad}" x2="${width - pad}" y2="${height - pad}" class="chart-grid"/><polygon points="${area}" fill="url(#${chartId})"/><polyline points="${line}" class="equity-line"/><text x="${pad}" y="18">${esc(money(max))}</text><text x="${pad}" y="${height - 8}">${esc(money(min))}</text><text x="${pad}" y="${height - 8}" text-anchor="start">${esc(first)}</text><text x="${width - pad}" y="${height - 8}" text-anchor="end">${esc(last)} UTC</text></svg></div>`;
}

function renderOverview(data) {
  const lease = data.active_lease;
  const services = data.services || [];
  const body = `<div class="metric-grid">
    ${metric("Database", data.database_status)}
    ${metric("Active Halts", data.active_halt_count, data.active_halt_count ? "ENTRY BLOCKED" : "No global halt")}
    ${metric("Lease Strategy", lease?.strategy_name || "NO LEASE", lease?.owner || "UNKNOWN OWNER")}
    ${metric("Lease Expiry", lease?.expires_at || "UNKNOWN")}
  </div><div class="service-list">${services.map((service) => `<div class="service-row"><b>${esc(service.name)}</b><span class="${statusClass(service.status)}">${esc(service.status)}</span><span class="age">${service.age_seconds == null ? "UNKNOWN AGE" : `${Math.round(service.age_seconds)}s`}</span></div>`).join("")}</div>`;
  return [data.active_halt_count ? "HALTED" : data.database_status, body];
}

function renderUniverse(data) {
  const rows = (items) => items.map((item) => ({...item, utc_day_return: item.utc_day_return == null ? "NO DATA" : `${(Number(item.utc_day_return) * 100).toFixed(2)}%`}));
  const body = `<div class="split"><div><p class="subhead">Top 20 / Gainers</p>${table(rows(data.gainers), [["#", "rank"], ["Symbol", "symbol"], ["UTC Return", "utc_day_return", "positive"]])}</div><div><p class="subhead">Top 20 / Losers</p>${table(rows(data.losers), [["#", "rank"], ["Symbol", "symbol"], ["UTC Return", "utc_day_return", "negative"]])}</div></div><p class="subhead" style="margin-top:24px">Monitored 40 / 当前候选池</p>${symbolGrid(data.monitored_symbols)}`;
  return [data.status, body];
}

function strategyAccount(data, index) {
  const summary = data.portfolio_summary || {};
  const positions = (data.open_positions || []).map((row) => ({...row, side_label: row.side === "long" ? "多" : "空", entry_price: number(row.entry_price, 6), last_mark_price: number(row.last_mark_price, 6), entry_notional: money(row.entry_notional), unrealized_pnl: money(row.unrealized_pnl), return_pct: percent(Number(row.unrealized_pnl) / Number(row.entry_notional)), pnl_class: pnlClass(row.unrealized_pnl)}));
  const closed = (data.closed_trades || []).map((row) => ({...row, side_label: row.side === "long" ? "多" : "空", entry_price: number(row.entry_price, 6), exit_price: number(row.exit_price, 6), realized_pnl: money(row.realized_pnl), return_pct: percent(row.return_pct), pnl_class: pnlClass(row.realized_pnl)}));
  const events = (data.trade_events || []).map((row) => ({...row, price: number(row.price, 6), pnl: row.pnl == null ? "—" : money(row.pnl), event_class: row.event.startsWith("OPEN") ? "event-open" : "event-close", pnl_class: pnlClass(row.pnl)}));
  const winRate = summary.win_rate == null ? "—" : percent(summary.win_rate);
  return `<article class="paper-account"><div class="strategy-meta"><span>ACCOUNT 0${index + 1}</span><span>${esc(data.strategy_name)}</span><span>${esc(data.run_id)}</span><span>CHECKPOINT ${esc(data.checkpoint_at)}</span></div><div class="portfolio-metrics">${metric("账户权益 / Equity", money(summary.equity), "余额 + 未实现盈亏")}${metric("可用余额 / Balance", money(summary.balance), "初始 1,000 USDT")}${metric("已实现盈亏", money(summary.realized_pnl), `${summary.closed_trade_count || 0} 笔已平仓`)}${metric("未实现盈亏", money(summary.unrealized_pnl), `${summary.open_position_count || 0} 个持仓`)}${metric("胜率", winRate, `手续费 ${money(summary.total_fees)}`)}</div><div class="ledger-section chart-section"><div class="section-title"><div><b>资产权益走势</b><span>ACCOUNT EQUITY · LAST 240 SNAPSHOTS</span></div><strong class="${pnlClass(Number(summary.equity) - Number(summary.balance))}">${money(summary.equity)}</strong></div>${equityChart(data.equity_curve, `equity-fill-${index}`)}</div><div class="ledger-split"><div class="ledger-section"><div class="section-title"><div><b>当前持仓</b><span>OPEN POSITIONS</span></div><strong>${positions.length}</strong></div>${table(positions, [["币种", "symbol"], ["方向", "side_label"], ["开仓价", "entry_price"], ["标记价", "last_mark_price"], ["名义价值", "entry_notional"], ["浮动盈亏", "unrealized_pnl", "pnl_class"], ["收益率", "return_pct", "pnl_class"]])}</div><div class="ledger-section"><div class="section-title"><div><b>已平仓交易</b><span>CLOSED TRADES</span></div><strong>${closed.length}</strong></div>${table(closed, [["币种", "symbol"], ["方向", "side_label"], ["开仓价", "entry_price"], ["平仓价", "exit_price"], ["净盈亏", "realized_pnl", "pnl_class"], ["收益率", "return_pct", "pnl_class"], ["平仓原因", "close_reason"]])}</div></div><div class="ledger-section"><div class="section-title"><div><b>开平仓流水</b><span>POSITION LIFECYCLE · BUY/SELL WITH CONTEXT</span></div></div>${table(events, [["时间", "occurred_at"], ["币种", "symbol"], ["动作", "label", "event_class"], ["订单方向", "order_action"], ["价格", "price"], ["数量", "quantity"], ["盈亏", "pnl", "pnl_class"], ["原因", "reason"]])}</div><div class="ledger-section secondary-ledger"><div class="section-title"><div><b>策略信号</b><span>RAW SIGNALS</span></div></div>${table(data.latest_signals, [["时间", "detected_at"], ["币种", "symbol"], ["方向", "side"], ["原因", "reason"]])}</div></article>`;
}

function renderStrategy(data) {
  const accounts = data.accounts || [];
  if (!accounts.length) return [data.status, empty("等待三个虚拟账户启动")];
  const overview = `<div class="paper-account-grid">${accounts.map((account, index) => {
    const summary = account.portfolio_summary || {};
    return `<div class="paper-account-card"><span>ACCOUNT 0${index + 1}</span><b>${esc(account.strategy_name)}</b><strong>${money(summary.equity)}</strong><small>${summary.open_position_count || 0} 持仓 · ${summary.closed_trade_count || 0} 已平仓</small></div>`;
  }).join("")}</div>`;
  return [data.status, `${overview}<div class="paper-account-stack">${accounts.map(strategyAccount).join("")}</div>`];
}

function renderAccount(data) {
  const body = `<div class="split"><div><p class="subhead">Balances</p>${table(data.balances, [["Asset", "asset"], ["Wallet", "wallet_balance"], ["Available", "available_balance"], ["uPnL", "unrealized_pnl"]])}</div><div><p class="subhead">Positions</p>${table(data.positions, [["Symbol", "symbol"], ["Amount", "position_amt"], ["Notional", "notional"], ["uPnL", "unrealized_pnl"]])}</div></div><p class="subhead" style="margin-top:22px">Open Orders</p>${table(data.open_orders, [["Symbol", "symbol"], ["Client ID", "client_order_id", "mono-cut"], ["Side", "side"], ["State", "status"], ["Reduce", "reduce_only"]])}`;
  return [data.status, body];
}

function renderRisk(data) {
  const halts = data.active_halts?.length ? data.active_halts.map((halt) => `<div class="alert-box"><strong>HALT</strong> ${esc(halt.reason)}<br><small>${esc(halt.created_at)}</small></div>`).join("") : empty("NO ACTIVE HALTS");
  const ambiguous = table(data.ambiguous_orders, [["Symbol", "symbol"], ["Client ID", "client_order_id", "mono-cut"], ["State", "state", "warning"], ["Updated", "updated_at"]]);
  const decisions = table(data.latest_risk_decisions, [["Candidate", "candidate_id", "mono-cut"], ["Decision", "decision"], ["Reason", "reason"], ["Time", "evaluated_at"]]);
  return [data.status, `<p class="subhead">Active Halts</p>${halts}<p class="subhead" style="margin-top:20px">Ambiguous / Unresolved</p>${ambiguous}<p class="subhead" style="margin-top:20px">Risk Decisions</p>${decisions}`];
}

function renderReports(data) {
  const body = `<div class="split"><div><p class="subhead">Shadow Sessions</p>${table(data.shadow_sessions, [["Run", "run_id", "mono-cut"], ["Strategy", "strategy_name"], ["State", "state"], ["Started", "started_at"]])}</div><div><p class="subhead">Live Transitions</p>${table(data.live_sessions, [["Session", "session_id", "mono-cut"], ["State", "state"], ["Time", "occurred_at"]])}</div></div><p class="subhead" style="margin-top:22px">Paper Runs</p>${table(data.paper_runs, [["Run", "run_id", "mono-cut"], ["Strategy", "strategy_name"], ["Signals", "signal_count"], ["Fills", "fill_count"], ["Created", "created_at"]])}`;
  return [data.status, body];
}

const renderers = { overview: renderOverview, risk: renderRisk, universe: renderUniverse, strategy: renderStrategy, account: renderAccount, reports: renderReports };

async function refreshSection(id) {
  const section = document.getElementById(id);
  try {
    const response = await fetch(section.dataset.endpoint, {headers: {"Accept": "application/json"}});
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    const [status, html] = renderers[id](data);
    setStatus(section, status);
    const body = section.querySelector(".panel-body");
    body.innerHTML = html;
    body.classList.remove("loading");
    section.classList.remove("updated");
    void section.offsetWidth;
    section.classList.add("updated");
    if (id === "overview") {
      const live = data.services?.find((item) => item.status === "LIVE");
      const halted = data.active_halt_count > 0;
      const mode = halted ? "HALTED" : live ? "LIVE" : "SHADOW";
      const stamp = document.getElementById("global-mode");
      stamp.className = `mode-stamp ${statusClass(mode)}`;
      stamp.textContent = mode;
    }
  } catch (error) {
    setStatus(section, "UNKNOWN");
    section.querySelector(".panel-body").innerHTML = `<div class="empty">UNKNOWN / ${esc(error.message)}</div>`;
  }
}

async function poll() {
  await Promise.allSettled(sections.map(refreshSection));
  document.getElementById("last-cycle").textContent = `LAST POLL: ${new Date().toISOString()}`;
}

function tick() { document.getElementById("utc-clock").textContent = new Date().toISOString().slice(11, 19); }
tick(); poll();
setInterval(tick, 1000);
setInterval(poll, 5000);
