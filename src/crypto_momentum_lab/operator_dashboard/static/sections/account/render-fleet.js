import {
  asNumber,
  dayTime,
  esc,
  money,
  pnlClass,
  relToNow,
  signedMoney,
} from "../../dashboard-formatters.js";
import { emptyBox, pill, tile } from "../../dashboard-ui.js";
import { K } from "../../ui/css-keys.js";
import { DISPLAY_TIME_ZONE_LABEL } from "./constants.js";
import { renderAccount } from "./render-detail.js";
import {
  normalizeSelectedAccount,
  selectedEquityRangeKey,
} from "./state.js";

export function liveAccountStatusLabel(status) {
  const normalized = String(status || "UNKNOWN").toUpperCase();
  return normalized === "READY"
    ? "正常"
    : normalized === "HALTED"
      ? "已停止"
      : "待确认";
}

export function liveAccountStatusClass(status) {
  const normalized = String(status || "UNKNOWN").toUpperCase();
  return normalized === "READY"
    ? "status-READY"
    : normalized === "HALTED"
      ? "status-HALTED"
      : "status-UNKNOWN";
}

export function liveStrategyStateLabel(state, leaseExpiresAt) {
  const normalized = String(state || "").toLowerCase();
  if (!normalized && leaseExpiresAt) return "租约有效";
  return {
    active: "运行中",
    running: "运行中",
    draining: "排空中",
    halted: "已停止",
  }[normalized] || state || "状态未知";
}

export function accountFleetMetric(accounts, key) {
  return accounts.reduce(
    (total, account) => total + (asNumber(account.summary?.[key]) || 0),
    0,
  );
}

export function liveAccountCard(account, index, selectedLabel) {
  const summary = account.summary || {};
  const selected = account.account_label === selectedLabel;
  const statusClass = liveAccountStatusClass(account.status);
  const readiness = account.readiness || "待确认";
  const strategy = account.strategy_name || "未关联策略";
  const strategyState = liveStrategyStateLabel(
    account.strategy_state,
    account.lease_expires_at,
  );
  const lease = account.lease_expires_at
    ? `租约至 ${dayTime(account.lease_expires_at)}`
    : "无有效租约";
  const financialSnapshot = Object.keys(summary).length > 0;
  const reconciliation = account.reconciliation || {};
  const mismatchCount = asNumber(reconciliation.mismatch_count);
  const reconciliationLabel = mismatchCount != null && mismatchCount > 0
    ? `${mismatchCount} 项差异`
    : String(reconciliation.status || "").toUpperCase() === "READY"
      ? "对账一致"
      : "对账待确认";
  const secondary = financialSnapshot
    ? `<span class="live-account-card-kpis">
      <span><small>USDT 钱包</small><b class="num">${esc(money(summary.usdt_wallet_balance))}</b></span>
      <span><small>可用余额（快照）</small><b class="num">${esc(money(summary.usdt_available_balance))}</b></span>
      <span><small>未实现盈亏</small><b class="num ${pnlClass(summary.total_unrealized_pnl)}">${esc(signedMoney(summary.total_unrealized_pnl))}</b></span>
      <span><small>名义价值</small><b class="num">${esc(money(summary.gross_position_notional))}</b></span>
    </span>`
    : `<span class="live-account-card-state-detail"><span>${esc(strategy)} · ${esc(strategyState)}</span><span>${esc(readiness)} · ${esc(lease)}</span></span>`;
  const footer = financialSnapshot
    ? `${summary.position_count ?? 0} 个持仓 · ${summary.open_order_count ?? 0} 个挂单`
    : "进入账户详情";
  const cardState = financialSnapshot ? reconciliationLabel : readiness;
  return `<button type="button" class="live-account-card${selected ? " is-selected" : ""}" ${K.liveAccountLabel}="${esc(account.account_label || "")}" role="tab" id="live-account-tab-${index}" aria-selected="${selected ? "true" : "false"}" aria-controls="live-account-detail" tabindex="${selected ? "0" : "-1"}">
    <span class="live-account-card-head">
      <span>
        <span class="live-account-card-kicker">LIVE ${String(index + 1).padStart(2, "0")} · ${esc(String(account.environment || "LIVE").toUpperCase())}</span>
        <strong>${esc(account.account_label || "交易所账户")}</strong>
      </span>
      <span class="live-account-card-status ${statusClass}">${esc(liveAccountStatusLabel(account.status))}</span>
    </span>
    <span class="live-account-card-state"><span>同步 <b>${esc(relToNow(account.observed_at))}</b></span><span>${esc(cardState)}</span></span>
    ${secondary}
    <span class="live-account-card-footer">${esc(footer)}<span aria-hidden="true">→</span></span>
  </button>`;
}

export function liveAccountSummary(accounts, overallStatus) {
  const readyCount = accounts.filter((account) => String(account.status).toUpperCase() === "READY").length;
  const haltedCount = accounts.filter((account) => String(account.status).toUpperCase() === "HALTED").length;
  const reviewCount = accounts.length - readyCount - haltedCount;
  const financialSnapshots = accounts.some((account) => account.summary);
  const tiles = [
    tile("实盘账户", `${accounts.length} 个`, "execution-account 独立状态"),
    tile("正常账户", `${readyCount} 个`, "可继续观察"),
    tile("停止账户", `${haltedCount} 个`, "需要检查"),
    tile("待确认", `${reviewCount} 个`, "缺少可靠状态"),
  ];
  if (financialSnapshots) {
    tiles.push(
      tile("USDT 钱包合计", money(accountFleetMetric(accounts, "usdt_wallet_balance")), "账户快照合计", "hero"),
      tile("总未实现盈亏", signedMoney(accountFleetMetric(accounts, "total_unrealized_pnl")), "账户群当前浮动盈亏", pnlClass(accountFleetMetric(accounts, "total_unrealized_pnl"))),
    );
  }
  return `<div class="live-account-fleet-summary">
    <div class="live-account-fleet-title">
      <div>
        <span class="section-kicker">LIVE FLEET</span>
        <h3>${esc(accounts.length === 4 ? "实盘账户矩阵 · 四账户实盘总览" : `${accounts.length} 个实盘账户总览`)}</h3>
        <p>共享同一 market-data 接入 · 独立执行与对账快照</p>
      </div>
      <div class="live-account-fleet-status"><small>集群状态</small>${pill(overallStatus)}<span>${readyCount} 正常 · ${haltedCount} 停止 · ${reviewCount} 待确认</span></div>
    </div>
    <div class="tile-grid live-account-fleet-kpis">${tiles.join("")}</div>
  </div>`;
}

export function renderLiveAccounts(state, data) {
  const sourceAccounts = Array.isArray(data?.accounts) ? data.accounts : [];
  const accounts = sourceAccounts.length
    ? sourceAccounts
    : (data?.account_label || data?.summary ? [data] : []);
  if (!accounts.length) {
    return [data?.status || "UNKNOWN", `<div class="live-account-empty">${emptyBox("等待实盘账户同步", "尚未发现 live execution-account；写入状态后会显示四账户矩阵。")}</div>`];
  }
  const selectedLabel = normalizeSelectedAccount(state, accounts, data?.selected_account_label);
  const selectedAccount = accounts.find(
    (account) => account.account_label === selectedLabel,
  ) || accounts[0];
  const readyCount = accounts.filter((account) => String(account.status).toUpperCase() === "READY").length;
  const haltedCount = accounts.filter((account) => String(account.status).toUpperCase() === "HALTED").length;
  const reviewCount = accounts.length - readyCount - haltedCount;
  const overallStatus = data?.status || (haltedCount ? "HALTED" : reviewCount ? "UNKNOWN" : "READY");
  const hasFullSnapshot = Boolean(selectedAccount.summary || selectedAccount.balances);
  const selectedEquityRange = selectedEquityRangeKey(state, selectedAccount);
  const selectedDetail = hasFullSnapshot
    ? renderAccount({ ...selectedAccount, equity_range: selectedEquityRange })[1]
    : `<div class="lazy-detail"><strong>账户详情加载中…</strong><small>${esc(selectedAccount.account_label || "交易所账户")} · 正在读取余额、持仓、挂单和权益。</small></div>`;
  const cards = `<div class="live-account-grid" role="tablist" aria-label="实盘账户选择">${accounts.map((account, index) => liveAccountCard(account, index, selectedLabel)).join("")}</div>`;
  const renderedAccount = hasFullSnapshot ? ` data-rendered-account="${esc(selectedAccount.account_label || "")}" data-rendered-range="${selectedEquityRange}" data-requested-range="${selectedEquityRange}"` : "";
  const detail = `<div id="live-account-detail" class="live-account-detail" ${K.liveAccountDetail} role="tabpanel" aria-labelledby="live-account-tab-${accounts.indexOf(selectedAccount)}" data-account-label="${esc(selectedAccount.account_label || "")}"${renderedAccount}>
    <div class="live-account-detail-head">
      <div><span class="section-kicker">SELECTED ACCOUNT</span><h3 ${K.selectedAccountLabel}>${esc(selectedAccount.account_label || "交易所账户")}</h3><small class="muted">切换卡片查看快照与权益曲线</small></div>
      <div class="live-account-detail-status">${pill(selectedAccount.status)}<span>${esc(dayTime(selectedAccount.observed_at))} ${DISPLAY_TIME_ZONE_LABEL}</span><small class="muted" ${K.accountLoadState} hidden></small></div>
    </div>
    <div ${K.liveAccountDetailContent}>${selectedDetail}</div>
  </div>`;
  const metrics = `<div ${K.liveAccountMetrics} aria-live="polite">${emptyBox("加载四账户时序", "正在读取权益、保证金和回撤历史")}</div>`;
  return [overallStatus, `<div class="live-account-fleet" data-live-account-directory>${liveAccountSummary(accounts, overallStatus)}${cards}${metrics}${detail}</div>`];
}
