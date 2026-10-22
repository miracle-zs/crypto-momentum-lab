import {
  esc,
  relToNow,
} from "../../dashboard-formatters.js";
import { pill, tile } from "../../dashboard-ui.js";
import { K, sel } from "../../ui/css-keys.js";
import { createAccountSectionState } from "./state.js";
import {
  defaultAccountRequestJson,
  loadLiveAccountDetail,
  loadLiveAccountMetrics,
} from "./loaders.js";
import {
  liveAccountStatusLabel,
  liveAccountStatusClass,
  liveStrategyStateLabel,
  renderLiveAccounts as renderLiveAccountsInternal,
} from "./render-fleet.js";

/** Account section state shared by rendering and event handlers. */
const state = createAccountSectionState();

export function renderLiveAccounts(data) {
  return renderLiveAccountsInternal(state, data);
}

export function wireLiveAccounts(root, data, { requestJson = defaultAccountRequestJson } = {}) {
  const accounts = Array.isArray(data?.accounts) ? data.accounts : [];
  root.__liveAccountData = accounts;
  root.__requestJson = requestJson;
  root.querySelectorAll(sel.accountCards()).forEach((button) => {
    if (button.dataset.liveAccountWired === "true") return;
    button.dataset.liveAccountWired = "true";
    button.addEventListener("click", () => {
      const accountLabel = button.dataset.liveAccountLabel;
      if (accountLabel) {
        void loadLiveAccountDetail(
          state,
          root,
          accountLabel,
          requestJson,
        );
      }
    });
    button.addEventListener("keydown", (event) => {
      const buttons = [...root.querySelectorAll(sel.accountCards())];
      const current = buttons.indexOf(button);
      if (current < 0) return;
      let next = null;
      if (event.key === "ArrowRight" || event.key === "ArrowDown") next = (current + 1) % buttons.length;
      if (event.key === "ArrowLeft" || event.key === "ArrowUp") next = (current - 1 + buttons.length) % buttons.length;
      if (event.key === "Home") next = 0;
      if (event.key === "End") next = buttons.length - 1;
      if (next == null) return;
      buttons[next].focus({ preventScroll: true });
      buttons[next].click();
      event.preventDefault();
    });
  });
  const selected = accounts.find(
    (account) => account.account_label === state.selectedLiveAccount,
  ) || accounts[0];
  if (selected) {
    void loadLiveAccountDetail(
      state,
      root,
      selected.account_label,
      requestJson,
    );
  }
  void loadLiveAccountMetrics(state, root, requestJson);
}

export async function updateLiveAccountsDynamic(root, data) {
  if (!root || !data) return;
  const accounts = Array.isArray(data?.accounts) ? data.accounts : [];
  if (!accounts.length) return;
  root.__liveAccountData = accounts;

  accounts.forEach((account) => {
    const card = root.querySelector(sel.accountCard(account.account_label));
    if (!card) return;
    const statusEl = card.querySelector(`.${K.liveAccountCardStatus}`);
    if (statusEl) {
      statusEl.className = `live-account-card-status ${liveAccountStatusClass(account.status)}`;
      statusEl.textContent = liveAccountStatusLabel(account.status);
    }
    const readiness = liveAccountStatusLabel(account.readiness);
    const baseline = String(account.startup_baseline?.status || "").toUpperCase();
    const baselineLabel = baseline === "MATCHED" ? "启动基线一致" : "启动基线未提供";

    const stateEl = card.querySelector(`.${K.liveAccountCardState}`);
    if (stateEl) {
      stateEl.innerHTML = `<span>同步 <b>${esc(relToNow(account.observed_at))}</b></span><span>${esc(readiness)}</span>`;
    }

    const stateDetailEl = card.querySelector(`.${K.liveAccountCardStateDetail}`);
    if (stateDetailEl) {
      const strategy = account.strategy_name || "未关联策略";
      const strategyState = liveStrategyStateLabel(account.strategy_state);
      stateDetailEl.innerHTML = `<span>${esc(strategy)} · ${esc(strategyState)}</span><span>${esc(baselineLabel)}</span>`;
    }

    const footerEl = card.querySelector(`.${K.liveAccountCardFooter}`);
    if (footerEl) {
      footerEl.innerHTML = `进入账户详情<span aria-hidden="true">→</span>`;
    }
  });

  const readyCount = accounts.filter((account) => String(account.status).toUpperCase() === "READY").length;
  const haltedCount = accounts.filter((account) => String(account.status).toUpperCase() === "HALTED").length;
  const reviewCount = accounts.length - readyCount - haltedCount;
  const overallStatus = data?.status || (haltedCount ? "HALTED" : reviewCount ? "UNKNOWN" : "READY");

  const fleetStatusEl = root.querySelector(`.${K.liveAccountFleetStatus}`);
  if (fleetStatusEl) {
    fleetStatusEl.innerHTML = `<small>集群状态</small>${pill(overallStatus)}<span>${readyCount} 正常 · ${haltedCount} 停止 · ${reviewCount} 需检查</span>`;
  }
  const fleetKpisEl = root.querySelector(`.${K.liveAccountFleetKpis}`);
  if (fleetKpisEl) {
    const tiles = [
      tile("实盘账户", `${accounts.length} 个`, "NextGen 账户进程状态"),
      tile("正常账户", `${readyCount} 个`, "可继续观察"),
      tile("停止账户", `${haltedCount} 个`, "需要检查"),
      tile("需检查", `${reviewCount} 个`, "降级、过期或未知"),
    ];
    fleetKpisEl.innerHTML = tiles.join("");
  }

  const isSectionHidden = root?.hidden === true || Boolean(root?.closest?.("[hidden]")) || root?.isConnected === false;
  if (isSectionHidden) {
    return;
  }

  const slot = root.querySelector(sel.liveAccountDetail());
  const requestJson = root.__requestJson || defaultAccountRequestJson;
  const currentDetailLabel = slot?.dataset.accountLabel
    || slot?.dataset.renderedAccount
    || state.selectedLiveAccount;
  const selectedAccount = accounts.find((account) => account.account_label === currentDetailLabel);
  const detailRefresh = selectedAccount
    ? loadLiveAccountDetail(
      state,
      root,
      currentDetailLabel,
      requestJson,
    )
    : Promise.resolve();
  // These regions are stacked: fleet metrics sit above the selected-account
  // detail.  Each patch captures/restores page reading position, so concurrent
  // writes can replay stale offsets. Keep one DOM mutation transaction active
  // at a time; the poller awaits this whole function before its final guard.
  await detailRefresh;
  await loadLiveAccountMetrics(state, root, requestJson);
}

export {
  renderAccount,
} from "./render-detail.js";
export {
  renderLiveAccountMetrics,
} from "./render-metrics.js";
