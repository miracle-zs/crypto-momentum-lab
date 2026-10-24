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
  enumLabel,
  MARGIN_TYPE_LABELS,
  ORDER_SIDE_LABELS,
  ORDER_STATUS_LABELS,
  ORDER_TYPE_LABELS,
  pill,
  POSITION_SIDE_LABELS,
  tile,
} from "../../dashboard-ui.js";
import {
  DEFAULT_EQUITY_BUCKET_SECONDS,
  DISPLAY_TIME_ZONE_LABEL,
  equitySampleLabel,
} from "./constants.js";
import { renderLiveSignalsContent } from "./signals.js";

const SIZING_LABELS = {
  fixed: "固定金额（单利）",
  daily: "每日复利（北京时间 08:00 换日）",
  per_trade: "逐笔实时复利",
  flat_reset: "全账户空仓重置复利",
};

function renderAccountSizing(config) {
  const sizing = config.entry_sizing;
  const fraction = asNumber(sizing?.equity_fraction);
  const fractionLabel = sizing?.mode !== "fixed" && fraction != null
    ? `<span>权益占比 <b class="num">${esc(num(fraction * 100, 2))}%</b></span>`
    : "";
  return `<div class="account-sizing-rule">
    <span>资金模式 <strong>${esc(SIZING_LABELS[sizing?.mode] || "待确认")}</strong></span>
    ${fractionLabel}
    <span>最近单笔目标 <b class="num">${esc(money(sizing?.target_notional))}</b></span>
  </div>`;
}

function renderAccountKpiGrid(summary) {
  return `<div class="tile-grid account-kpi-grid">
    ${tile("USDT 钱包余额", money(summary.usdt_wallet_balance), "账户钱包余额", "hero")}
    ${tile("USDT 可用余额", money(summary.usdt_available_balance), `更新于 ${summary.available_balance_observed_at ? esc(dayTime(summary.available_balance_observed_at)) : "时间未知"}`)}
    ${tile("持仓浮动盈亏（估算）", signedMoney(summary.total_unrealized_pnl), `${summary.position_count || 0} 个策略持仓批次`, pnlClass(summary.total_unrealized_pnl))}
    ${tile("持仓名义价值", money(summary.gross_position_notional), "按近期行情估算的策略持仓暴露")}
  </div>`;
}

function renderAccountEquityBlock(data, accountEquity, accountSample, accountEquityDelta, latestAccountEquity) {
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
  const equityValue = `<span class="account-equity-value"><small>最新采样权益</small><strong class="num ${pnlClass(accountEquityDelta)}">${esc(money(latestAccountEquity))}</strong></span>`;

  return `<div class="block account-equity-block">
    ${blockTitle("实盘账户权益", `ROLLING 24H · ${accountSample} BUCKETS`, equityValue)}
    <p class="account-history-note muted">最近 24 小时权益，曲线只展示已有采样。</p>
    <div class="chart-context"><span>${esc(`${dayTime(data.equity_window_start)} → ${dayTime(data.equity_window_end)} ${DISPLAY_TIME_ZONE_LABEL}`)}</span><b class="num">${accountEquity.length} 个采样点</b></div>
    ${equityCoverage}
    ${equityChart(accountEquity, "live-account-equity", data.equity_window_start, data.equity_window_end)}
  </div>`;
}

function renderAccountFacts(config, startupBaseline, modeLabel) {
  const sizing = config.entry_sizing;
  const fraction = asNumber(sizing?.equity_fraction);
  const fractionLabel = sizing?.mode === "fixed"
    ? "固定名义金额"
    : fraction == null ? "—" : `${num(fraction * 100, 4)}% · 每笔名义金额占比`;
  const baselineMatched = String(startupBaseline.status || "").toLowerCase() === "matched";
  const baselineTime = startupBaseline.observed_at
    ? fullDateTime(startupBaseline.observed_at)
    : "—";
  return `<div class="account-facts">
    <div><span>实盘下单通道</span><b class="pos">NextGen 账户进程</b></div>
    <div><span>同步服务模式</span><b class="muted">账户私有 WebSocket</b></div>
    <div><span>资金模式</span><b>${esc(SIZING_LABELS[sizing?.mode] || "—")}</b></div>
    <div><span>开仓金额规则</span><b>${esc(fractionLabel)}</b></div>
    <div><span>最近计算的单笔名义金额</span><b class="num">${esc(money(sizing?.target_notional))}</b></div>
    <div><span>定额基准权益</span><b class="num">${esc(money(sizing?.basis_equity))}</b></div>
    <div><span>定额基准时间</span><b class="num">${esc(sizing?.sampled_at ? fullDateTime(sizing.sampled_at) : "—")}</b></div>
    <div><span>持仓模式</span><b>${esc(modeLabel(config.hedge_mode, "Hedge · 双向", "One-way · 单向"))}</b></div>
    <div><span>保证金模式</span><b>${esc(modeLabel(config.multi_assets_mode, "Multi-Assets · 多资产", "Single-Asset · 单资产"))}</b></div>
    <div><span>手续费等级</span><b>${esc(config.fee_tier == null ? "—" : `VIP ${config.fee_tier}`)}</b></div>
    <div><span>启动持仓 / 挂单检查</span><b class="${baselineMatched ? "pos" : "muted"}">${baselineMatched ? "一致" : "未提供"}</b></div>
    <div><span>启动时持仓 / 挂单数</span><b>${esc(`${startupBaseline.position_count ?? "—"} / ${startupBaseline.open_order_count ?? "—"}`)}</b></div>
    <div><span>启动基线时间</span><b class="num">${esc(baselineTime)}</b></div>
  </div>
  <div class="account-facts-note">启动时用 REST 快照核对持仓和挂单；运行期间由私有 WebSocket 更新账户，不做周期性 REST 对账。</div>`;
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
    { label: "方向", value: (row) => pill(row.position_side || "BOTH", POSITION_SIDE_LABELS), html: true },
    { label: "策略", value: (row) => strategy(row.strategy_name), html: true },
    { label: "持仓量", value: (row) => num(row.position_amt, 4), align: "right" },
    { label: "开仓 / 标记", value: (row) => `${price(row.entry_price)} / ${price(row.mark_price)}`, align: "right" },
    { label: "策略杠杆", value: (row) => row.leverage ? `${esc(row.leverage)}x` : "—", align: "right", cls: "muted" },
    { label: "保证金配置", value: (row) => enumLabel(row.margin_type, MARGIN_TYPE_LABELS), cls: "muted" },
    { label: "名义价值（估算）", value: (row) => money(row.notional), align: "right" },
    { label: "未实现盈亏（估算）", value: (row) => signedMoney(row.unrealized_pnl), align: "right", cls: (row) => pnlClass(row.unrealized_pnl) },
    { label: "ROI（估算）", value: (row) => signedPercent(row.roi), align: "right", cls: (row) => pnlClass(row.roi) },
  ], positionRows, {
    emptyText: "本地策略账本无持仓",
    tall: true,
    rowKey: (row) => `${row.symbol || ""}:${row.position_side || "BOTH"}`,
  });
}

function renderExchangePositionsTable(positions, observedAt) {
  const snapshotLabel = observedAt
    ? `交易所持仓状态更新 ${fullDateTime(observedAt)} · 启动 REST 基线，运行期间由私有 WebSocket 更新`
    : "暂无交易所持仓快照；当前进程状态尚未提供此字段";
  const table = dataTable([
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "方向", value: (row) => pill(row.direction || row.position_side || "BOTH", POSITION_SIDE_LABELS), html: true },
    { label: "持仓量", value: (row) => num(row.position_amt, 4), align: "right" },
    { label: "开仓均价", value: (row) => price(row.entry_price), align: "right" },
    { label: "标记价格", value: (row) => price(row.mark_price), align: "right" },
    { label: "名义价值（估算）", value: (row) => money(row.notional), align: "right" },
    { label: "未实现盈亏（估算）", value: (row) => signedMoney(row.unrealized_pnl), align: "right", cls: (row) => pnlClass(row.unrealized_pnl) },
  ], positions, {
    emptyText: observedAt ? "交易所当前无持仓" : "交易所持仓数据尚未接入",
    tall: true,
    rowKey: (row) => `${row.symbol || ""}:${row.position_side || "BOTH"}`,
  });
  return `<p class="muted">${esc(snapshotLabel)}</p>${table}`;
}

function renderOrdersTable(openOrders, strategy) {
  return dataTable([
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "策略", value: (row) => strategy(row.strategy_name), html: true },
    { label: "方向", value: (row) => enumLabel(row.side, ORDER_SIDE_LABELS), cls: (row) => row.side === "BUY" ? "pos" : "neg" },
    { label: "类型 / 价格", value: (row) => `${enumLabel(row.order_type, ORDER_TYPE_LABELS)} / ${price(row.price)}`, align: "right" },
    { label: "数量", value: (row) => `${num(row.executed_quantity, 4)} / ${num(row.original_quantity, 4)}`, align: "right" },
    { label: "状态", value: (row) => pill(row.status, ORDER_STATUS_LABELS), html: true },
    { label: "开 / 平仓", value: (row) => row.is_exit ? "平仓" : "开仓", cls: "muted" },
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
    { label: "成交 / 订单更新时间", value: (row) => row.aggregation === "order" ? `${dayTime(row.order_updated_at)}（订单更新）` : dayTime(row.trade_at), align: "right", cls: "muted" },
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "订单", value: (row) => shortHash(row.order_id), cls: "num cut" },
    { label: "策略", value: (row) => strategy(row.strategy_name), html: true },
    { label: "方向", value: (row) => enumLabel(row.side, ORDER_SIDE_LABELS), cls: (row) => row.side === "BUY" ? "pos" : "neg" },
    { label: "均价", value: (row) => price(row.price), align: "right" },
    { label: "数量", value: (row) => num(row.quantity, 4), align: "right" },
    { label: "已实现盈亏", value: (row) => signedMoney(row.realized_pnl), align: "right", cls: (row) => pnlClass(row.realized_pnl) },
    { label: "手续费", value: (row) => row.fees ? Object.entries(row.fees).map(([asset, amount]) => `${num(amount, 4)} ${asset}`).join(" / ") : `${num(row.fee, 4)} ${row.fee_asset || ""}`, align: "right" },
    { label: "退出路径", value: (row) => row.is_exit ? (row.close_path || "退出路径未记录") : "开仓", cls: "muted" },
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
  const startupBaseline = data.startup_baseline || {};
  const accountEquity = data.equity_curve || [];
  const accountSample = equitySampleLabel(data.equity_sample_interval_seconds);
  const accountEquityDelta = accountWindowDelta({ equity_curve: accountEquity });
  const latestAccountEquity = accountEquity.at(-1)?.equity;
  const normalized = (value) => String(value || "").trim().toLowerCase();
  const modeLabel = (value, yesLabel, noLabel) => value == null ? "—" : value ? yesLabel : noLabel;
  const syncStatus = normalized(data.status);
  const runtimeAccount = (data.available_accounts || []).find(
    (account) => account.account_label === data.account_label,
  );
  const runtimeReadiness = normalized(runtimeAccount?.readiness);
  const runtimeTradeability = runtimeAccount?.runtime_tradeability;
  const accountConfigAvailable = config.hedge_mode != null
    && config.multi_assets_mode != null
    && config.fee_tier != null;
  const baselineMatched = normalized(startupBaseline.status) === "matched";
  const observedAtMs = new Date(data.observed_at || "").getTime();
  const freshnessSeconds = Number.isFinite(observedAtMs)
    ? Math.max(0, (Date.now() - observedAtMs) / 1000)
    : null;
  const executionDetail = runtimeTradeability
    ? `开仓${runtimeTradeability.entry_gate_open ? "可用" : "暂停"} · 退出${runtimeTradeability.exit_gate_open ? "可用" : "暂停"}`
    : "当前账户没有交易通道状态";
  const executionState = runtimeReadiness === "ready"
    ? { className: "status-READY", label: "进程正常", detail: executionDetail }
    : runtimeReadiness === "halted"
      ? { className: "status-HALTED", label: "已停止", detail: executionDetail }
      : runtimeReadiness === "degraded"
        ? { className: "status-ATTENTION", label: "状态降级", detail: executionDetail }
        : runtimeReadiness === "stale"
          ? { className: "status-STALE", label: "数据过期", detail: executionDetail }
          : { className: "status-UNKNOWN", label: "待确认", detail: executionDetail };
  const strategy = (value) => value
    ? `<span class="account-strategy">${esc(value)}</span>`
    : `<span class="muted">未关联</span>`;

  const warnings = [
    baselineMatched ? null : "启动基线待确认",
    accountConfigAvailable ? null : "账户配置待确认",
    freshnessSeconds == null ? "同步时间待确认" : freshnessSeconds > 180 ? "数据已过期" : null,
  ].filter(Boolean);
  const executionLine = `<div class="account-execution-line ${executionState.className}"><strong>${esc(executionState.label)}</strong><span>${esc(executionState.detail)}</span>${warnings.map((warning) => `<span class="warn">${esc(warning)}</span>`).join("")}</div>`;
  const kpis = renderAccountKpiGrid(summary);
  const equityChartBlock = renderAccountEquityBlock(
    data,
    accountEquity,
    accountSample,
    accountEquityDelta,
    latestAccountEquity,
  );
  const accountFacts = renderAccountFacts(config, startupBaseline, modeLabel);
  const usdtBalances = (data.balances || []).filter((row) => String(row.asset || "").toUpperCase() === "USDT");
  const balancesTable = renderBalancesTable(usdtBalances);
  const positions = data.positions || [];
  const positionsTable = renderPositionsTable(positions, strategy);
  const exchangePositions = data.exchange_positions || [];
  const exchangePositionsTable = renderExchangePositionsTable(
    exchangePositions,
    data.exchange_positions_observed_at,
  );
  const openOrders = data.open_orders || [];
  const ordersTable = renderOrdersTable(openOrders, strategy);
  const fills = data.fills || [];
  const fillsTable = renderFillsTable(fills, strategy);
  const liveSignals = data.live_signals || [];
  const liveSignalContent = renderLiveSignalsContent(liveSignals);

  const accountNeedsReview = !baselineMatched
    || !accountConfigAvailable
    || hasUncertainStatus(syncStatus)
    || !data.observed_at;

  const body = `<small class="account-refresh-note muted" data-refresh-state hidden></small>
    ${renderAccountSizing(config)}${executionLine}${kpis}
    ${disclosure("交易所实际持仓", "EXCHANGE POSITIONS · PRIVATE USER DATA STREAM", exchangePositionsTable, `<strong class="num">${exchangePositions.length}</strong>`, { open: exchangePositions.length > 0, stateKey: "exchange-positions" })}
    ${disclosure("当前挂单", "OPEN ORDERS · PRIVATE USER DATA STREAM", ordersTable, `<strong class="num">${openOrders.length}</strong>`, { open: openOrders.length > 0, stateKey: "account-open-orders" })}
    ${equityChartBlock}
    ${disclosure("最近成交订单", "RECENT TRADES · ONE ORDER PER ROW", fillsTable, `<strong class="num">${fills.length}</strong>`, { stateKey: "account-fills" })}
    ${disclosure("实盘策略信号", "LIVE SIGNALS · LATEST 30", liveSignalContent, `<strong class="num">${liveSignals.length}</strong>`, { stateKey: "live-strategy-signals" })}
    ${disclosure("策略归因持仓", "LOCAL STRATEGY POSITION BATCHES", positionsTable, `<strong class="num">${positions.length}</strong>`, { stateKey: "account-positions" })}
    ${disclosure("账户配置与启动基线", "ACCOUNT CONFIGURATION / STARTUP BASELINE", accountFacts, "", { open: accountNeedsReview, stateKey: "account-settings" })}
    ${disclosure("USDT 资产余额", "USDT BALANCE · ACCOUNT COLLATERAL", balancesTable, `<strong class="num">${usdtBalances.length}</strong>`, { stateKey: "account-balances" })}`;
  return [data.status, body];
}
