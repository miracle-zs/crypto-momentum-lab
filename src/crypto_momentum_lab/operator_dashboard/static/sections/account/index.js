import {
  asNumber,
  dayTime,
  esc,
  money,
  pnlClass,
  relToNow,
  signedMoney,
} from "../../dashboard-formatters.js";
import { pill, tile } from "../../dashboard-ui.js";
import { K, sel } from "../../ui/css-keys.js";
import {
  accountDetailRange,
  createAccountSectionState,
} from "./state.js";
import {
  defaultAccountRequestJson,
  loadLiveAccountDetail,
  loadLiveAccountMetrics,
  selectedAccountDetailRange,
  wireAccountEquityRanges,
} from "./loaders.js";
import {
  accountFleetMetric,
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
          accountDetailRange(state, accountLabel),
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
  if (selected && !(selected.summary || selected.balances)) {
    void loadLiveAccountDetail(
      state,
      root,
      selected.account_label,
      requestJson,
      accountDetailRange(state, selected.account_label),
    );
  } else if (selected) {
    const slot = root.querySelector(sel.liveAccountDetail());
    if (slot) {
      wireAccountEquityRanges(slot, (nextRange) => loadLiveAccountDetail(state, root, selected.account_label, requestJson, nextRange));
    }
  }
  void loadLiveAccountMetrics(state, root, requestJson, state.selectedLiveAccountMetricsRange);
}

export function updateLiveAccountsDynamic(root, data) {
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
    const summary = account.summary || {};
    const financialSnapshot = Object.keys(summary).length > 0;
    const reconciliation = account.reconciliation || {};
    const mismatchCount = asNumber(reconciliation.mismatch_count);
    const reconciliationLabel = mismatchCount != null && mismatchCount > 0
      ? `${mismatchCount} 项差异`
      : String(reconciliation.status || "").toUpperCase() === "READY"
        ? "对账一致"
        : (account.reconciliation_status ? `对账 ${account.reconciliation_status}` : "对账待确认");
    const readiness = account.readiness || "就绪未知";
    const cardState = financialSnapshot ? reconciliationLabel : readiness;

    const stateEl = card.querySelector(`.${K.liveAccountCardState}`);
    if (stateEl) {
      stateEl.innerHTML = `<span>同步 <b>${esc(relToNow(account.observed_at))}</b></span><span>${esc(cardState)}</span>`;
    }

    const kpisEl = card.querySelector(`.${K.liveAccountCardKpis}`);
    const stateDetailEl = card.querySelector(`.${K.liveAccountCardStateDetail}`);
    if (financialSnapshot) {
      const kpisHtml = `<span><small>USDT 钱包</small><b class="num">${esc(money(summary.usdt_wallet_balance))}</b></span>` +
        `<span><small>可用余额（快照）</small><b class="num">${esc(money(summary.usdt_available_balance))}</b></span>` +
        `<span><small>未实现盈亏</small><b class="num ${pnlClass(summary.total_unrealized_pnl)}">${esc(signedMoney(summary.total_unrealized_pnl))}</b></span>` +
        `<span><small>名义价值</small><b class="num">${esc(money(summary.gross_position_notional))}</b></span>`;
      if (kpisEl) {
        kpisEl.innerHTML = kpisHtml;
      } else if (stateDetailEl) {
        stateDetailEl.className = "live-account-card-kpis";
        stateDetailEl.innerHTML = kpisHtml;
      }
    } else {
      const strategy = account.strategy_name || "未关联策略";
      const strategyState = liveStrategyStateLabel(account.strategy_state, account.lease_expires_at);
      const lease = account.lease_expires_at ? `租约至 ${dayTime(account.lease_expires_at)}` : "无有效租约";
      const stateDetailHtml = `<span>${esc(strategy)} · ${esc(strategyState)}</span><span>${esc(readiness)} · ${esc(lease)}</span>`;
      if (stateDetailEl) {
        stateDetailEl.innerHTML = stateDetailHtml;
      } else if (kpisEl) {
        kpisEl.className = "live-account-card-state-detail";
        kpisEl.innerHTML = stateDetailHtml;
      }
    }

    const footerEl = card.querySelector(`.${K.liveAccountCardFooter}`);
    if (footerEl) {
      const footerText = financialSnapshot
        ? `${summary.position_count ?? 0} 个持仓 · ${summary.open_order_count ?? 0} 个挂单`
        : "进入账户详情";
      footerEl.innerHTML = `${esc(footerText)}<span aria-hidden="true">→</span>`;
    }
  });

  const readyCount = accounts.filter((account) => String(account.status).toUpperCase() === "READY").length;
  const haltedCount = accounts.filter((account) => String(account.status).toUpperCase() === "HALTED").length;
  const reviewCount = accounts.length - readyCount - haltedCount;
  const overallStatus = data?.status || (haltedCount ? "HALTED" : reviewCount ? "UNKNOWN" : "READY");

  const fleetStatusEl = root.querySelector(`.${K.liveAccountFleetStatus}`);
  if (fleetStatusEl) {
    fleetStatusEl.innerHTML = `<small>集群状态</small>${pill(overallStatus)}<span>${readyCount} 正常 · ${haltedCount} 停止 · ${reviewCount} 待确认</span>`;
  }
  const fleetKpisEl = root.querySelector(`.${K.liveAccountFleetKpis}`);
  if (fleetKpisEl) {
    const tiles = [
      tile("实盘账户", `${accounts.length} 个`, "execution-account 独立状态"),
      tile("正常账户", `${readyCount} 个`, "可继续观察"),
      tile("停止账户", `${haltedCount} 个`, "需要检查"),
      tile("待确认", `${reviewCount} 个`, "缺少可靠状态"),
    ];
    if (accounts.some((account) => account.summary)) {
      tiles.push(
        tile("USDT 钱包合计", money(accountFleetMetric(accounts, "usdt_wallet_balance")), "账户快照合计", "hero"),
        tile("总未实现盈亏", signedMoney(accountFleetMetric(accounts, "total_unrealized_pnl")), "账户群当前浮动盈亏", pnlClass(accountFleetMetric(accounts, "total_unrealized_pnl"))),
      );
    }
    fleetKpisEl.innerHTML = tiles.join("");
  }

  const isSectionHidden = root?.hidden === true || Boolean(root?.closest?.("[hidden]")) || root?.isConnected === false;
  if (isSectionHidden) {
    return Promise.resolve();
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
      selectedAccountDetailRange(slot),
      { forceFetch: true },
    )
    : Promise.resolve();
  return Promise.all([
    detailRefresh,
    loadLiveAccountMetrics(state, root, requestJson, state.selectedLiveAccountMetricsRange),
  ]);
}

export {
  renderAccount,
} from "./render-detail.js";
export {
  renderLiveAccountMetrics,
} from "./render-metrics.js";
export {
  wireAccountEquityRanges,
} from "./loaders.js";
