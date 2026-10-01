import {
  asNumber,
  computeRoi,
  dayTime,
  esc,
  fullDateTime,
  hasUncertainStatus,
  money,
  num,
  pnlClass,
  price,
  relToNow,
  shortHash,
  signedMoney,
  signedPercent,
} from "../../dashboard-formatters.js";
import {
  accountWindowDelta,
  equityChart,
} from "../../dashboard-charts.js";
import {
  blockTitle,
  dataTable,
  disclosure,
  pill,
  tile,
} from "../../dashboard-ui.js";
import {
  ACCOUNT_EQUITY_RANGES,
  DEFAULT_EQUITY_BUCKET_SECONDS,
  DISPLAY_TIME_ZONE_LABEL,
  equityRangeControls,
  equitySampleLabel,
} from "./constants.js";
import { renderLiveSignalsContent } from "./signals.js";

function renderAccountHero(data, syncStatus, freshnessSeconds) {
  const accountHeroDescription = syncStatus === "ready" && freshnessSeconds != null && freshnessSeconds <= 120
    ? "只读同步正常 · 实盘订单由 live-strategy 执行管控"
    : syncStatus === "ready" && freshnessSeconds != null
      ? "只读同步数据延迟 · 实盘订单由 live-strategy 执行管控，请检查同步服务"
      : syncStatus === "halted"
        ? "只读同步已停止 · 实盘订单由 live-strategy 执行管控，请检查同步服务"
        : "只读同步状态待确认 · 实盘订单由 live-strategy 执行管控";

  return `<div class="account-hero">
    <div>
      <div class="account-eyebrow">${esc(String(data.environment || "LIVE").toUpperCase())} · EXECUTION ACCOUNT</div>
      <h3>${esc(data.account_label || "交易所账户")}</h3>
      <p>${esc(accountHeroDescription)}</p>
    </div>
    <div class="account-hero-meta">
      <div class="account-hero-status"><small>同步状态</small>${pill(data.status)}</div>
      <span>同步 <b class="num">${esc(dayTime(data.observed_at))}</b></span>
      <small>${esc(relToNow(data.observed_at))}</small>
    </div>
  </div>`;
}

function renderAccountStateGrid(syncState, configState, executionState, reconciliationState, freshnessState) {
  const stateCard = (label, state) => `<div class="account-state-card ${state.className}">
    <span>${esc(label)}</span>
    <strong>${esc(state.label)}</strong>
    <small>${esc(state.detail)}</small>
  </div>`;

  return `<div class="account-state-grid" aria-label="实盘账户状态">
    ${stateCard("同步服务", syncState)}
    ${stateCard("账户配置", configState)}
    ${stateCard("实盘执行", executionState)}
    ${stateCard("对账状态", reconciliationState)}
    ${stateCard("数据新鲜度", freshnessState)}
  </div>`;
}

function renderAccountKpiGrid(summary) {
  return `<div class="tile-grid account-kpi-grid">
    ${tile("USDT 钱包余额", money(summary.usdt_wallet_balance), "账户钱包余额", "hero")}
    ${tile("USDT 可用余额", money(summary.usdt_available_balance), `交易所快照 · ${summary.available_balance_observed_at ? esc(fullDateTime(summary.available_balance_observed_at)) : "时间未知"}`)}
    ${tile("总未实现盈亏", signedMoney(summary.total_unrealized_pnl), `${summary.position_count || 0} 个交易所持仓`, pnlClass(summary.total_unrealized_pnl))}
    ${tile("持仓名义价值", money(summary.gross_position_notional), "当前交易所总暴露")}
    ${tile("挂单 / 最近成交", `${summary.open_order_count ?? 0} / ${summary.recent_trade_count ?? summary.recent_fill_count ?? 0}`, "当前挂单 / 最近 20 笔订单")}
  </div>`;
}

function renderAccountEquityBlock(data, accountEquity, selectedEquityRange, accountSample, accountEquityDelta, latestAccountEquity) {
  const equityDataStart = accountEquity[0]?.observed_at;
  const requestedStartMs = new Date(data.equity_window_start || "").getTime();
  const dataStartMs = new Date(equityDataStart || "").getTime();
  const bucketMs = (asNumber(data.equity_sample_interval_seconds) || DEFAULT_EQUITY_BUCKET_SECONDS) * 1000;
  const hasPartialHistory = Number.isFinite(requestedStartMs)
    && Number.isFinite(dataStartMs)
    && dataStartMs - requestedStartMs > bucketMs * 2;
  const equityCoverage = hasPartialHistory
    ? `<p class="equity-coverage-note">可用历史始于 <b class="num">${esc(fullDateTime(equityDataStart))} ${DISPLAY_TIME_ZONE_LABEL}</b>（随实盘运行持续沉淀）。</p>`
    : "";
  const equityValue = `<span class="account-equity-value"><small>${esc(selectedEquityRange.shortLabel)} 期末权益</small><strong class="num ${pnlClass(accountEquityDelta)}">${esc(money(latestAccountEquity))}</strong></span>`;

  return `<div class="block account-equity-block" data-equity-range="${selectedEquityRange.key}">
    ${blockTitle("实盘账户权益", `ROLLING ${selectedEquityRange.shortLabel} · ${accountSample} BUCKETS`, `${equityRangeControls(selectedEquityRange.key)}${equityValue}`)}
    <div class="chart-context"><span>${esc(`${selectedEquityRange.key === "1y" ? fullDateTime(data.equity_window_start) : dayTime(data.equity_window_start)} → ${selectedEquityRange.key === "1y" ? fullDateTime(data.equity_window_end) : dayTime(data.equity_window_end)} ${DISPLAY_TIME_ZONE_LABEL}`)}</span><b class="num">${accountEquity.length} 个采样点</b></div>
    ${equityCoverage}
    ${equityChart(accountEquity, "live-account-equity", data.equity_window_start, data.equity_window_end)}
  </div>`;
}

function renderAccountFacts(config, reconciliation, mismatchCount, modeLabel, reconciliationLabel) {
  return `<div class="account-facts">
    <div><span>实盘下单通道</span><b class="pos">live-strategy</b></div>
    <div><span>同步服务模式</span><b class="muted">只读同步 · 不下单</b></div>
    <div><span>持仓模式</span><b>${esc(modeLabel(config.hedge_mode, "Hedge · 双向", "One-way · 单向"))}</b></div>
    <div><span>保证金模式</span><b>${esc(modeLabel(config.multi_assets_mode, "Multi-Assets · 多资产", "Single-Asset · 单资产"))}</b></div>
    <div><span>手续费等级</span><b>${esc(config.fee_tier == null ? "—" : `VIP ${config.fee_tier}`)}</b></div>
    <div><span>对账状态</span><b>${esc(reconciliationLabel(reconciliation.status))}</b></div>
    <div><span>对账差异项</span><b class="${mismatchCount > 0 ? "neg" : "pos"}">${esc(reconciliation.mismatch_count == null ? "—" : `${reconciliation.mismatch_count} 项`)}</b></div>
    <div><span>对账快照 资产 / 持仓</span><b>${esc(`${reconciliation.balance_count ?? "—"} / ${reconciliation.position_count ?? "—"}`)}</b></div>
    <div><span>对账快照 挂单 / 成交</span><b>${esc(`${reconciliation.open_order_count ?? "—"} / ${reconciliation.fill_count ?? "—"}`)}</b></div>
  </div>
  <div class="account-facts-note"><b>怎么读</b>：<code>只读同步</code>不会下单；实盘订单由 <code>live-strategy</code> 与风控闸门共同决定。<code>对账一致 / 0 项</code>表示本次快照未发现差异。</div>`;
}

function renderBalancesTable(usdtBalances) {
  return dataTable([
    { label: "资产", key: "asset", cls: "sym" },
    { label: "钱包余额", value: (row) => num(row.wallet_balance, 4), align: "right" },
    { label: "可用余额（快照）", value: (row) => num(row.available_balance, 4), align: "right" },
    { label: "未实现盈亏", value: (row) => signedMoney(row.unrealized_pnl), align: "right", cls: (row) => pnlClass(row.unrealized_pnl) },
  ], usdtBalances, { emptyText: "尚无 USDT 余额快照", tall: true, rowKey: (row) => row.asset });
}

function renderPositionsTable(positions, strategy) {
  const positionRows = (positions || []).map((row) => ({
    ...row,
    roi: computeRoi(row.unrealized_pnl, row.entry_notional),
  }));
  return dataTable([
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
  ], positionRows, {
    emptyText: "交易所无持仓",
    tall: true,
    rowKey: (row) => `${row.symbol || ""}:${row.position_side || "BOTH"}`,
  });
}

function renderOrdersTable(openOrders, strategy) {
  return dataTable([
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "策略", value: (row) => strategy(row.strategy_name), html: true },
    { label: "方向", key: "side", cls: (row) => row.side === "BUY" ? "pos" : "neg" },
    { label: "类型 / 价格", value: (row) => `${row.order_type || "—"} / ${price(row.price)}`, align: "right" },
    { label: "数量", value: (row) => `${num(row.executed_quantity, 4)} / ${num(row.original_quantity, 4)}`, align: "right" },
    { label: "状态", value: (row) => pill(row.status), html: true },
    { label: "只减仓", value: (row) => row.reduce_only ? "是" : "否", cls: "muted" },
    { label: "更新时间", value: (row) => dayTime(row.observed_at), align: "right", cls: "muted" },
  ], openOrders, {
    emptyText: "无挂单",
    tall: true,
    rowKey: (row) => row.order_id || row.exchange_order_id || row.client_order_id
      || `${row.symbol || ""}:${row.side || ""}:${row.order_type || ""}:${row.price || ""}`,
  });
}

function renderFillsTable(fills, strategy) {
  return dataTable([
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
    { label: "平仓原因", value: (row) => row.reduce_only ? (row.close_reason || "原因未记录") : "开仓", cls: "muted" },
  ], fills, {
    emptyText: "尚无成交记录",
    tall: true,
    rowKey: (row) => row.trade_id || row.fill_id || row.id
      || `${row.order_id || ""}:${row.trade_at || row.observed_at || ""}`,
  });
}

export function renderAccount(data) {
  const summary = data.summary || {};
  const config = data.account_config || {};
  const reconciliation = data.reconciliation || {};
  const accountEquity = data.equity_curve || [];
  const selectedEquityRange = ACCOUNT_EQUITY_RANGES.find(
    (option) => option.key === data.equity_range,
  ) || ACCOUNT_EQUITY_RANGES[0];
  const accountSample = equitySampleLabel(data.equity_sample_interval_seconds);
  const accountEquityDelta = accountWindowDelta({ equity_curve: accountEquity });
  const latestAccountEquity = accountEquity.at(-1)?.equity;
  const normalized = (value) => String(value || "").trim().toLowerCase();
  const modeLabel = (value, yesLabel, noLabel) => value == null ? "—" : value ? yesLabel : noLabel;
  const reconciliationLabel = (value) => ({
    ready: "已完成",
    halted: "已中止",
    degraded: "降级",
  }[String(value || "").toLowerCase()] || value || "—");
  const mismatchCount = asNumber(reconciliation.mismatch_count);
  const syncStatus = normalized(data.status);
  const syncState = syncStatus === "ready"
    ? { className: "status-READY", label: "同步正常", detail: "execution-account · 只读同步" }
    : syncStatus === "halted"
      ? { className: "status-HALTED", label: "同步已停止", detail: "execution-account · 需要检查" }
      : { className: "status-UNKNOWN", label: "等待同步", detail: "execution-account · 暂无可靠状态" };
  const configState = {
    className: Object.keys(config).length > 0 ? "status-READY" : "status-UNKNOWN",
    label: Object.keys(config).length > 0 ? "已同步" : "等待同步",
    detail: "Binance V3 账户配置快照",
  };
  const reconciliationState = mismatchCount != null && mismatchCount > 0
    ? { className: "status-ATTENTION", label: `${mismatchCount} 项差异`, detail: "余额、持仓或订单快照需要核对" }
    : normalized(reconciliation.status) === "ready"
      ? { className: "status-READY", label: "对账一致", detail: "快照已完成 · 0 项差异" }
      : { className: "status-UNKNOWN", label: reconciliationLabel(reconciliation.status), detail: "等待本次对账结果" };
  const observedAtMs = new Date(data.observed_at || "").getTime();
  const freshnessSeconds = Number.isFinite(observedAtMs)
    ? Math.max(0, (Date.now() - observedAtMs) / 1000)
    : null;
  const freshnessState = freshnessSeconds == null
    ? { className: "status-UNKNOWN", label: "未知", detail: "没有可用同步时间" }
    : freshnessSeconds <= 120
      ? { className: "status-FRESH", label: "数据新鲜", detail: `${relToNow(data.observed_at)} · 最近一次同步` }
      : { className: "status-STALE", label: "数据过期", detail: `${relToNow(data.observed_at)} · 请检查同步服务` };
  const executionState = {
    className: "status-SHADOW",
    label: "live-strategy",
    detail: "实盘下单通道 · 状态见全局实盘状态与风控",
  };
  const strategy = (value) => value
    ? `<span class="account-strategy">${esc(value)}</span>`
    : `<span class="muted">未关联</span>`;

  const hero = renderAccountHero(data, syncStatus, freshnessSeconds);
  const stateGrid = renderAccountStateGrid(syncState, configState, executionState, reconciliationState, freshnessState);
  const kpis = renderAccountKpiGrid(summary);
  const equityChartBlock = renderAccountEquityBlock(
    data,
    accountEquity,
    selectedEquityRange,
    accountSample,
    accountEquityDelta,
    latestAccountEquity,
  );
  const accountFacts = renderAccountFacts(config, reconciliation, mismatchCount, modeLabel, reconciliationLabel);
  const usdtBalances = (data.balances || []).filter((row) => String(row.asset || "").toUpperCase() === "USDT");
  const balancesTable = renderBalancesTable(usdtBalances);
  const positions = data.positions || [];
  const positionsTable = renderPositionsTable(positions, strategy);
  const openOrders = data.open_orders || [];
  const ordersTable = renderOrdersTable(openOrders, strategy);
  const fills = data.fills || [];
  const fillsTable = renderFillsTable(fills, strategy);
  const liveSignals = data.live_signals || [];
  const liveSignalContent = renderLiveSignalsContent(liveSignals);

  const accountNeedsReview = mismatchCount > 0
    || hasUncertainStatus(syncStatus)
    || hasUncertainStatus(normalized(reconciliation.status))
    || !data.observed_at;

  const body = `<div class="detail-meta"><span>同步时间 <b class="num">${esc(dayTime(data.observed_at))} ${DISPLAY_TIME_ZONE_LABEL}</b></span><span>${esc(relToNow(data.observed_at))}</span><small class="muted" data-refresh-state hidden></small></div>
    ${hero}${stateGrid}${kpis}${equityChartBlock}
    ${disclosure("实盘策略信号", "LIVE SIGNALS · NON-BLOCKING OBSERVATION · LATEST 30", liveSignalContent,
      `<strong class="num">${liveSignals.length}</strong>`, { open: liveSignals.length > 0, stateKey: "live-strategy-signals" })}
    ${disclosure("账户配置与对账", "EXECUTION CHANNEL / RECONCILIATION", accountFacts, "", { open: accountNeedsReview, stateKey: "account-reconciliation" })}
    ${disclosure("USDT 资产余额", "USDT BALANCE · ACCOUNT COLLATERAL", balancesTable, `<strong class="num">${usdtBalances.length}</strong>`, { open: usdtBalances.length > 0, stateKey: "account-balances" })}
    ${disclosure("交易所持仓", "EXCHANGE POSITIONS · STRATEGY ATTRIBUTION", positionsTable, `<strong class="num">${positions.length}</strong>`, { open: positions.length > 0, stateKey: "account-positions" })}
    ${disclosure("当前挂单", "OPEN ORDERS · EXCHANGE SOURCE OF TRUTH", ordersTable, `<strong class="num">${openOrders.length}</strong>`, { open: openOrders.length > 0, stateKey: "account-open-orders" })}
    ${disclosure("最近成交订单", "RECENT TRADES · ONE ORDER PER ROW", fillsTable, `<strong class="num">${fills.length}</strong>`, { stateKey: "account-fills" })}`;
  return [data.status, body];
}
