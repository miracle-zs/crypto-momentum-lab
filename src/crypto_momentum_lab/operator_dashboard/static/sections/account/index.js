import { pill } from "../../dashboard-ui.js";
import { K, sel } from "../../ui/css-keys.js";
import {
  defaultAccountRequestJson,
  loadLiveAccountDetail,
  loadLiveAccountMetrics,
} from "./loaders.js";
import {
  liveAccountStatusLabel,
  liveAccountStatusClass,
  liveAccountGateSummary,
  liveAccountObservationSummary,
  renderLiveAccounts as renderLiveAccountsInternal,
} from "./render-fleet.js";

/** Account section state shared by rendering and event handlers. */
const state = {
  selectedLiveAccount: "primary",
  liveAccountDetailRequest: 0,
  liveAccountMetricsRequest: 0,
};

export function renderLiveAccounts(data) {
  return renderLiveAccountsInternal(state, data);
}

export function selectLiveAccount(root, accountLabel) {
  if (root?.__liveAccountData?.length
      && !root.__liveAccountData.some((account) => account.account_label === accountLabel)) return;
  state.selectedLiveAccount = accountLabel;
  if (root?.__liveAccountData?.some((account) => account.account_label === accountLabel)) {
    return loadLiveAccountDetail(state, root, accountLabel, root.__requestJson || defaultAccountRequestJson);
  }
}

export function wireLiveAccounts(root, data, { requestJson = defaultAccountRequestJson } = {}) {
  const accounts = data.accounts;
  root.__liveAccountData = accounts;
  root.__requestJson = requestJson;
  root.querySelectorAll(sel.accountCards()).forEach((button) => {
    if (button.dataset.liveAccountWired === "true") return;
    button.dataset.liveAccountWired = "true";
    button.addEventListener("click", () => {
      const accountLabel = button.dataset.liveAccountLabel;
      if (accountLabel) {
        if (typeof window !== "undefined") {
          window.history.replaceState(null, "", `#account?account=${encodeURIComponent(accountLabel)}`);
        }
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
  );
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

function setHtml(element, html) {
  if (typeof element.setHTMLUnsafe === "function") {
    element.setHTMLUnsafe(html);
  } else {
    element.innerHTML = html;
  }
}

export async function updateLiveAccountsDynamic(root, data) {
  if (!root) return;
  const accounts = data.accounts;
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
    const stateEl = card.querySelector(`.${K.liveAccountCardState}`);
    if (stateEl) {
      setHtml(stateEl, liveAccountGateSummary(account));
    }

    const stateDetailEl = card.querySelector(`.${K.liveAccountCardStateDetail}`);
    if (stateDetailEl) {
      setHtml(stateDetailEl, liveAccountObservationSummary(account));
    }
  });

  const readyCount = accounts.filter((account) => String(account.status).toUpperCase() === "READY").length;
  const haltedCount = accounts.filter((account) => String(account.status).toUpperCase() === "HALTED").length;
  const reviewCount = accounts.length - readyCount - haltedCount;
  const overallStatus = data.status;

  const fleetStatusEl = root.querySelector(`.${K.liveAccountFleetStatus}`);
  if (fleetStatusEl) {
    setHtml(fleetStatusEl, `${pill(overallStatus)}<span>${readyCount} 正常 · ${haltedCount} 停止 · ${reviewCount} 需检查</span>`);
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
  // Each patch captures/restores page reading position, so concurrent
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
