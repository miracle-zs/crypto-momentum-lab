import {
  dayTime,
  esc,
  relToNow,
} from "../../dashboard-formatters.js";
import { emptyBox, pill, tile } from "../../dashboard-ui.js";
import { K } from "../../ui/css-keys.js";
import { DISPLAY_TIME_ZONE_LABEL } from "./constants.js";
import { normalizeSelectedAccount } from "./state.js";

export function liveAccountStatusLabel(status) {
  const normalized = String(status || "UNKNOWN").toUpperCase();
  return normalized === "READY"
    ? "正常"
    : normalized === "HALTED"
      ? "已停止"
      : normalized === "DEGRADED"
        ? "降级"
        : normalized === "STALE"
          ? "数据过期"
          : "待确认";
}

export function liveAccountStatusClass(status) {
  const normalized = String(status || "UNKNOWN").toUpperCase();
  return normalized === "READY"
    ? "status-READY"
    : normalized === "HALTED"
      ? "status-HALTED"
      : normalized === "DEGRADED"
        ? "status-ATTENTION"
        : normalized === "STALE"
          ? "status-STALE"
          : "status-UNKNOWN";
}

export function liveStrategyStateLabel(state) {
  const normalized = String(state || "").toLowerCase();
  return {
    active: "运行中",
    running: "运行中",
    draining: "排空中",
    halted: "已停止",
  }[normalized] || state || "状态未知";
}

function liveAccountCard(account, index, selectedLabel) {
  const selected = account.account_label === selectedLabel;
  const statusClass = liveAccountStatusClass(account.status);
  const readiness = liveAccountStatusLabel(account.readiness);
  const strategy = account.strategy_name || "未关联策略";
  const strategyState = liveStrategyStateLabel(account.strategy_state);
  const baseline = String(account.startup_baseline?.status || "").toUpperCase();
  const baselineLabel = baseline === "MATCHED" ? "启动基线一致" : "启动基线未提供";
  return `<button type="button" class="live-account-card${selected ? " is-selected" : ""}" ${K.liveAccountLabel}="${esc(account.account_label || "")}" role="tab" id="live-account-tab-${index}" aria-selected="${selected ? "true" : "false"}" aria-controls="live-account-detail" tabindex="${selected ? "0" : "-1"}">
    <span class="live-account-card-head">
      <span>
        <span class="live-account-card-kicker">LIVE ${String(index + 1).padStart(2, "0")} · ${esc(String(account.environment || "LIVE").toUpperCase())}</span>
        <strong>${esc(account.account_label || "交易所账户")}</strong>
      </span>
      <span class="live-account-card-status ${statusClass}">${esc(liveAccountStatusLabel(account.status))}</span>
    </span>
    <span class="live-account-card-state"><span>同步 <b>${esc(relToNow(account.observed_at))}</b></span><span>${esc(readiness)}</span></span>
    <span class="live-account-card-state-detail"><span>${esc(strategy)} · ${esc(strategyState)}</span><span>${esc(baselineLabel)}</span></span>
    <span class="live-account-card-footer">进入账户详情<span aria-hidden="true">→</span></span>
  </button>`;
}

function liveAccountSummary(accounts, overallStatus) {
  const readyCount = accounts.filter((account) => String(account.status).toUpperCase() === "READY").length;
  const haltedCount = accounts.filter((account) => String(account.status).toUpperCase() === "HALTED").length;
  const reviewCount = accounts.length - readyCount - haltedCount;
  const tiles = [
    tile("实盘账户", `${accounts.length} 个`, "独立账户状态"),
    tile("正常账户", `${readyCount} 个`, "可继续观察"),
    tile("停止账户", `${haltedCount} 个`, "需要检查"),
    tile("需检查", `${reviewCount} 个`, "降级、过期或未知"),
  ];
  return `<div class="live-account-fleet-summary">
    <div class="live-account-fleet-title">
      <div>
        <span class="section-kicker">LIVE FLEET</span>
        <h3>${esc(accounts.length === 4 ? "实盘账户矩阵 · 四账户实盘总览" : `${accounts.length} 个实盘账户总览`)}</h3>
        <p>共享行情接入 · 各账户独立执行</p>
      </div>
      <div class="live-account-fleet-status"><small>集群状态</small>${pill(overallStatus)}<span>${readyCount} 正常 · ${haltedCount} 停止 · ${reviewCount} 需检查</span></div>
    </div>
    <div class="tile-grid live-account-fleet-kpis">${tiles.join("")}</div>
  </div>`;
}

export function renderLiveAccounts(state, data) {
  const sourceAccounts = Array.isArray(data?.accounts) ? data.accounts : [];
  const accounts = sourceAccounts;
  if (!accounts.length) {
    return [data?.status || "UNKNOWN", `<div class="live-account-empty">${emptyBox("等待实盘账户状态", "账户状态写入后会显示实盘账户矩阵。")}</div>`];
  }
  const selectedLabel = normalizeSelectedAccount(state, accounts, data?.selected_account_label);
  const selectedAccount = accounts.find(
    (account) => account.account_label === selectedLabel,
  ) || accounts[0];
  const readyCount = accounts.filter((account) => String(account.status).toUpperCase() === "READY").length;
  const haltedCount = accounts.filter((account) => String(account.status).toUpperCase() === "HALTED").length;
  const reviewCount = accounts.length - readyCount - haltedCount;
  const overallStatus = data?.status || (haltedCount ? "HALTED" : reviewCount ? "UNKNOWN" : "READY");
  const selectedDetail = `<div class="lazy-detail"><strong>账户详情加载中…</strong><small>${esc(selectedAccount.account_label || "交易所账户")} · 正在读取余额、持仓、挂单和权益。</small></div>`;
  const cards = `<div class="live-account-grid" role="tablist" aria-label="实盘账户选择">${accounts.map((account, index) => liveAccountCard(account, index, selectedLabel)).join("")}</div>`;
  const detail = `<div id="live-account-detail" class="live-account-detail" ${K.liveAccountDetail} role="tabpanel" aria-labelledby="live-account-tab-${accounts.indexOf(selectedAccount)}" data-account-label="${esc(selectedAccount.account_label || "")}">
    <div class="live-account-detail-head">
      <div><span class="section-kicker">SELECTED ACCOUNT</span><h3 ${K.selectedAccountLabel}>${esc(selectedAccount.account_label || "交易所账户")}</h3><small class="muted">切换卡片查看快照与权益曲线</small></div>
      <div class="live-account-detail-status">${pill(selectedAccount.status)}<span>${esc(dayTime(selectedAccount.observed_at))} ${DISPLAY_TIME_ZONE_LABEL}</span><small class="muted" ${K.accountLoadState} hidden></small></div>
    </div>
    <div ${K.liveAccountDetailContent}>${selectedDetail}</div>
  </div>`;
  const metrics = `<div ${K.liveAccountMetrics} aria-live="polite">${emptyBox("加载四账户时序", "正在读取权益、保证金和回撤历史")}</div>`;
  return [overallStatus, `<div class="live-account-fleet" data-live-account-directory>${liveAccountSummary(accounts, overallStatus)}${cards}${detail}${metrics}</div>`];
}
