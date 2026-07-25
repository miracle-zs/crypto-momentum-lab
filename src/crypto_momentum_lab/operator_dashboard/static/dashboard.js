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
  return `<table class="data-table"><thead><tr>${columns.map(([label]) => `<th>${esc(label)}</th>`).join("")}</tr></thead><tbody>${rows.map((row) => `<tr>${columns.map(([, key, klass]) => `<td class="${klass || ""}">${esc(row[key])}</td>`).join("")}</tr>`).join("")}</tbody></table>`;
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
  const body = `<div class="split"><div><p class="subhead">Top 20 / Gainers</p>${table(rows(data.gainers), [["#", "rank"], ["Symbol", "symbol"], ["UTC Return", "utc_day_return", "positive"]])}</div><div><p class="subhead">Top 20 / Losers</p>${table(rows(data.losers), [["#", "rank"], ["Symbol", "symbol"], ["UTC Return", "utc_day_return", "negative"]])}</div></div><p class="subhead" style="margin-top:24px">Monitored 40</p>${table(data.monitored_symbols, [["Symbol", "symbol"], ["Pool State", "status"], ["Side", "side"]])}`;
  return [data.status, body];
}

function renderStrategy(data) {
  const body = `<div class="metric-grid">${metric("Strategy", data.strategy_name)}${metric("Run ID", data.run_id)}${metric("Config Hash", data.config_hash, "immutable")}${metric("Checkpoint", data.checkpoint_at)}</div><p class="subhead" style="margin-top:22px">Latest Signals</p>${table(data.latest_signals, [["Time", "detected_at"], ["Symbol", "symbol"], ["Side", "side"], ["Reason", "reason"]])}<p class="subhead" style="margin-top:22px">Virtual Buy / Sell</p>${table(data.latest_paper_fills, [["Time", "filled_at"], ["Symbol", "symbol"], ["Action", "action"], ["Side", "side"], ["Status", "status"], ["Price", "fill_price"], ["Quantity", "quantity"], ["Notional", "filled_notional"], ["Fee", "fee"]])}<p class="subhead" style="margin-top:22px">Rejections</p><pre>${esc(JSON.stringify(data.rejection_summary || {}, null, 2))}</pre>`;
  return [data.status, body];
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
