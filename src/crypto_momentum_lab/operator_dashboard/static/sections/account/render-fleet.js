import {
  dayTime,
  esc,
  relToNow,
} from "../../dashboard-formatters.js";
import { emptyBox, pill } from "../../dashboard-ui.js";
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

export function liveAccountGateSummary(account) {
  const gates = account.runtime_tradeability;
  if (!gates) return "执行状态待确认";
  return `<span>开仓 <b>${gates.entry_gate_open ? "可用" : "暂停"}</b></span><span>退出 <b>${gates.exit_gate_open ? "可用" : "暂停"}</b></span>`;
}

export function liveAccountObservationSummary(account) {
  const baselineMatched = String(account.startup_baseline?.status || "").toUpperCase() === "MATCHED";
  return `<span>同步 ${esc(relToNow(account.observed_at))}</span>${baselineMatched ? "" : '<span class="warn">启动基线待确认</span>'}`;
}

function liveAccountCard(account, index, selectedLabel) {
  const selected = account.account_label === selectedLabel;
  const statusClass = liveAccountStatusClass(account.status);
  return `<button type="button" class="live-account-card${selected ? " is-selected" : ""}" ${K.liveAccountLabel}="${esc(account.account_label || "")}" role="tab" id="live-account-tab-${index}" aria-selected="${selected ? "true" : "false"}" aria-controls="live-account-detail" tabindex="${selected ? "0" : "-1"}">
    <span class="live-account-card-head">
      <span>
        <strong>${esc(account.account_label || "交易所账户")}</strong>
      </span>
      <span class="live-account-card-status ${statusClass}">${esc(liveAccountStatusLabel(account.status))}</span>
    </span>
    <span class="live-account-card-state">${liveAccountGateSummary(account)}</span>
    <span class="live-account-card-state-detail">${liveAccountObservationSummary(account)}</span>
  </button>`;
}

function liveAccountSummary(accounts, overallStatus) {
  const readyCount = accounts.filter((account) => String(account.status).toUpperCase() === "READY").length;
  const haltedCount = accounts.filter((account) => String(account.status).toUpperCase() === "HALTED").length;
  const reviewCount = accounts.length - readyCount - haltedCount;
  return `<div class="live-account-fleet-summary">
    <span>${accounts.length} 个账户</span>
    <div class="live-account-fleet-status">${pill(overallStatus)}<span>${readyCount} 正常 · ${haltedCount} 停止 · ${reviewCount} 需检查</span></div>
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
      <h3 ${K.selectedAccountLabel}>${esc(selectedAccount.account_label || "交易所账户")}</h3>
      <div class="live-account-detail-status">${pill(selectedAccount.status)}<span>${esc(dayTime(selectedAccount.observed_at))} ${DISPLAY_TIME_ZONE_LABEL}</span><small class="muted" ${K.accountLoadState} hidden></small></div>
    </div>
    <div ${K.liveAccountDetailContent}>${selectedDetail}</div>
  </div>`;
  const metrics = `<div ${K.liveAccountMetrics} aria-live="polite">${emptyBox("加载集群时序数据", "正在读取权益、保证金和回撤历史")}</div>`;
  return [overallStatus, `<div class="live-account-fleet" data-live-account-directory>${liveAccountSummary(accounts, overallStatus)}${cards}${detail}${metrics}</div>`];
}
