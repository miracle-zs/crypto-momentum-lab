import {
  DEFAULT_EQUITY_BUCKET_SECONDS,
  DISPLAY_TIME_ZONE_LABEL,
} from "../dashboard-config.js";
import {
  asNumber,
  dayTime,
  esc,
  money,
  num,
  pnlClass,
  price,
  relToNow,
  shortHash,
  signedMoney,
  signedPercent,
} from "../dashboard-formatters.js";
import { accountWindowDelta, equityChart } from "../dashboard-charts.js";
import { blockTitle, dataTable, pill, tile } from "../dashboard-ui.js";

export function renderAccount(data) {
  const summary = data.summary || {};
  const config = data.account_config || {};
  const reconciliation = data.reconciliation || {};
  const accountEquity = data.equity_curve || [];
  const accountSampleMinutes = Math.round(
    (asNumber(data.equity_sample_interval_seconds) || DEFAULT_EQUITY_BUCKET_SECONDS) / 60,
  );
  const accountEquityDelta = accountWindowDelta({ equity_curve: accountEquity });
  const latestAccountEquity = accountEquity.at(-1)?.equity;
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
  const equityChartBlock = `<div class="block account-equity-block">
    ${blockTitle("实盘账户权益", `LIVE USDT ACCOUNT EQUITY · ROLLING 24H · ${accountSampleMinutes} MIN BUCKETS`, `<strong class="num ${pnlClass(accountEquityDelta)}">${esc(money(latestAccountEquity))}</strong>`)}
    <div class="chart-context"><span>${esc(`${dayTime(data.equity_window_start)} → ${dayTime(data.equity_window_end)} ${DISPLAY_TIME_ZONE_LABEL}`)}</span><b class="num">${accountEquity.length} / 240 桶</b></div>
    ${equityChart(accountEquity, "live-account-equity", data.equity_window_start, data.equity_window_end)}
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
    { label: "平仓原因", value: (row) => row.reduce_only ? (row.close_reason || "原因未记录") : "开仓", cls: "muted" },
  ], data.fills, { emptyText: "尚无成交记录", tall: true });
  const body = `<div class="detail-meta"><span>同步时间 <b class="num">${esc(dayTime(data.observed_at))} ${DISPLAY_TIME_ZONE_LABEL}</b></span><span>${esc(relToNow(data.observed_at))}</span></div>
    ${hero}${kpis}${equityChartBlock}
    <div class="block-split account-overview-grid">
      <div class="block">${blockTitle("账户权限与对账", "EXECUTION CHANNEL / RECONCILIATION")}${accountFacts}</div>
      <div class="block">${blockTitle("USDT 资产余额", "USDT BALANCE · ACCOUNT COLLATERAL")}${balancesTable}</div>
    </div>
    <div class="block">${blockTitle("交易所持仓", "EXCHANGE POSITIONS · STRATEGY ATTRIBUTION", `<strong class="num">${(data.positions || []).length}</strong>`)}${positionsTable}</div>
    <div class="block">${blockTitle("当前挂单", "OPEN ORDERS · EXCHANGE SOURCE OF TRUTH", `<strong class="num">${(data.open_orders || []).length}</strong>`)}${ordersTable}</div>
    <div class="block">${blockTitle("最近成交订单", "RECENT TRADES · ONE ORDER PER ROW", `<strong class="num">${(data.fills || []).length}</strong>`)}${fillsTable}</div>`;
  return [data.status, body];
}
