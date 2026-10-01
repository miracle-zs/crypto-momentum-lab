import {
  patchChildrenFromHtml,
  replaceChildrenFromHtml,
} from "../../dashboard-dom.js";
import { refreshEcharts } from "../../dashboard-chart-engine.js";
import { dayTime, esc } from "../../dashboard-formatters.js";
import { emptyBox, pill } from "../../dashboard-ui.js";
import { K, sel } from "../../ui/css-keys.js";
import { DISPLAY_TIME_ZONE_LABEL } from "./constants.js";
import { renderAccount } from "./render-detail.js";
import { renderLiveAccountMetrics } from "./render-metrics.js";
import {
  accountDetailRange,
  rememberAccountDetailRange,
} from "./state.js";

export async function defaultAccountRequestJson(url, { timeoutMs = 15000 } = {}) {
  const signal = typeof AbortSignal !== "undefined" && typeof AbortSignal.timeout === "function"
    ? AbortSignal.timeout(timeoutMs)
    : undefined;
  const response = await fetch(url, {
    headers: { "Accept": "application/json" },
    signal,
  });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
}

const liveAccountDetailInFlight = new Map();
const liveAccountMetricsInFlight = new Map();


export function markRefreshStale(root, message) {
  const state = root.querySelector(sel.refreshState());
  if (!state) return;
  state.hidden = false;
  state.textContent = message;
  state.title = message;
  root.dataset.refreshState = "stale";
}

export function detailContentSlot(slot) {
  return slot.querySelector(sel.liveAccountDetailContent()) || slot;
}

export function selectedAccountDetailRange(slot) {
  return slot?.dataset.requestedRange
    || slot?.querySelector(sel.accountEquityRangePressed())?.dataset.accountEquityRange
    || slot?.dataset.renderedRange
    || "24h";
}

export function setLiveAccountDetailHeader(slot, accountLabel, data = {}, loadingMessage = "") {
  const label = slot.querySelector(sel.selectedAccountLabel());
  if (label) label.textContent = accountLabel || "交易所账户";
  const status = slot.querySelector(`.${K.liveAccountDetailStatus}`);
  if (status && (data.status || data.observed_at)) {
    status.innerHTML = `${pill(data.status || "UNKNOWN")}<span>${esc(dayTime(data.observed_at))} ${DISPLAY_TIME_ZONE_LABEL}</span><small class="muted" ${K.accountLoadState}${loadingMessage ? "" : " hidden"}>${esc(loadingMessage)}</small>`;
  } else {
    const loadState = slot.querySelector(sel.accountLoadState());
    if (loadState) {
      loadState.textContent = loadingMessage;
      loadState.hidden = !loadingMessage;
    }
  }
}

export function setLiveAccountTabState(root, accountLabel) {
  root.querySelectorAll(sel.accountCards()).forEach((button) => {
    const active = button.dataset.liveAccountLabel === accountLabel;
    button.classList.toggle("is-selected", active);
    button.classList.toggle("is-active", active);
    button.setAttribute("aria-selected", String(active));
    button.setAttribute("tabindex", active ? "0" : "-1");
  });
}

export function wireAccountEquityRanges(root, onSelect) {
  root.querySelectorAll(sel.accountEquityRange()).forEach((button) => {
    if (button.dataset.accountEquityRangeWired === "true") return;
    button.dataset.accountEquityRangeWired = "true";
    button.addEventListener("click", async () => {
      if (button.getAttribute("aria-pressed") === "true") return;
      const range = button.dataset.accountEquityRange;
      if (!range) return;
      const slot = root.matches?.(`[${K.liveAccountDetail}]`)
        ? root
        : root.closest?.(`[${K.liveAccountDetail}]`);
      if (slot) slot.dataset.requestedRange = range;
      const controls = root.querySelectorAll(sel.accountEquityRange());
      controls.forEach((control) => {
        control.disabled = true;
        control.setAttribute("aria-pressed", String(control === button));
      });
      root.querySelector(`.${K.accountEquityBlock}`)?.classList.add("is-range-loading");
      try {
        await onSelect(range);
      } finally {
        if (button.isConnected) {
          controls.forEach((control) => { control.disabled = false; });
          root.querySelector(`.${K.accountEquityBlock}`)?.classList.remove("is-range-loading");
        }
      }
    });
  });
}

export function wireLiveAccountMetricsRanges(root, onSelect, state) {
  root.querySelectorAll(sel.liveMetricsRange()).forEach((button) => {
    if (button.dataset.liveMetricsRangeWired === "true") return;
    button.dataset.liveMetricsRangeWired = "true";
    button.addEventListener("click", async () => {
      if (button.getAttribute("aria-pressed") === "true") return;
      const range = button.dataset.liveAccountMetricsRange;
      if (!range) return;
      const controls = root.querySelectorAll(sel.liveMetricsRange());
      controls.forEach((control) => {
        control.disabled = true;
        control.setAttribute("aria-pressed", String(control === button));
      });
      root.querySelector(`.${K.liveAccountMetricsBlock}`)?.classList.add("is-range-loading");
      try {
        state.selectedLiveAccountMetricsRange = range;
        await onSelect(range);
      } finally {
        if (root.isConnected) {
          root.querySelectorAll(sel.liveMetricsRange()).forEach((control) => { control.disabled = false; });
          root.querySelector(`.${K.liveAccountMetricsBlock}`)?.classList.remove("is-range-loading");
        }
      }
    });
  });
}

export async function loadLiveAccountDetail(
  state,
  root,
  accountLabel,
  requestJson,
  equityRange = "24h",
  { forceFetch = false } = {},
) {
  const slot = root.querySelector(sel.liveAccountDetail());
  if (!slot) return;
  const contentSlot = detailContentSlot(slot);
  const selectedCard = [...root.querySelectorAll(sel.accountCards())]
    .find((button) => button.dataset.liveAccountLabel === accountLabel);
  const accountData = selectedCard ? (root.__liveAccountData || []).find(
    (account) => account.account_label === accountLabel,
  ) : null;
  const requestId = ++state.liveAccountDetailRequest;
  state.selectedLiveAccount = accountLabel;
  rememberAccountDetailRange(state, accountLabel, equityRange);
  setLiveAccountTabState(root, accountLabel);
  slot.dataset.accountLabel = accountLabel;
  slot.dataset.requestedRange = equityRange;
  const renderedAccount = slot.dataset.renderedAccount;
  const isSameAccount = renderedAccount === accountLabel;
  const hasLastGood = Boolean(renderedAccount && contentSlot.children.length);
  setLiveAccountDetailHeader(
    slot,
    accountLabel,
    accountData || {},
    !isSameAccount && hasLastGood ? `正在读取 ${accountLabel} 快照；保留上次内容` : "",
  );

  if (!forceFetch && (accountData?.summary || accountData?.balances)) {
    const [status, html] = renderAccount({ ...accountData, equity_range: equityRange });
    if (isSameAccount) patchChildrenFromHtml(contentSlot, html);
    else replaceChildrenFromHtml(contentSlot, html);
    slot.dataset.accountStatus = status;
    slot.dataset.renderedAccount = accountLabel;
    slot.dataset.renderedRange = equityRange;
    slot.dataset.requestedRange = equityRange;
    setLiveAccountDetailHeader(slot, accountLabel, accountData);
    wireAccountEquityRanges(contentSlot, (nextRange) => loadLiveAccountDetail(state, root, accountLabel, requestJson, nextRange));
    refreshEcharts(contentSlot);
    slot.removeAttribute("aria-busy");
    return;
  }
  if (!hasLastGood && !contentSlot.children.length) {
    replaceChildrenFromHtml(
      contentSlot,
      `<div class="lazy-detail"><strong>账户详情加载中…</strong><small>${esc(accountLabel)} · 正在读取最新快照</small></div>`,
    );
  }
  slot.setAttribute("aria-busy", "true");
  try {
    const fetchKey = `${accountLabel}:${equityRange}`;
    let detailPromise = liveAccountDetailInFlight.get(fetchKey);
    if (!detailPromise) {
      const query = new URLSearchParams({
        account_label: accountLabel,
        equity_range: equityRange,
      });
      detailPromise = Promise.resolve(requestJson(`api/account?${query.toString()}`))
        .finally(() => {
          liveAccountDetailInFlight.delete(fetchKey);
        });
      liveAccountDetailInFlight.set(fetchKey, detailPromise);
    }
    const detail = await detailPromise;
    if (requestId !== state.liveAccountDetailRequest || !slot.isConnected) return;
    const [status, html] = renderAccount(detail);
    if (slot.dataset.renderedAccount === accountLabel) patchChildrenFromHtml(contentSlot, html);
    else replaceChildrenFromHtml(contentSlot, html);
    slot.dataset.accountStatus = status;
    slot.dataset.renderedAccount = accountLabel;
    slot.dataset.renderedRange = equityRange;
    slot.dataset.requestedRange = equityRange;
    setLiveAccountDetailHeader(slot, accountLabel, detail);
    wireAccountEquityRanges(contentSlot, (nextRange) => loadLiveAccountDetail(state, root, accountLabel, requestJson, nextRange));
    refreshEcharts(contentSlot);
  } catch (error) {
    if (requestId !== state.liveAccountDetailRequest || !slot.isConnected) return;
    if (hasLastGood) {
      markRefreshStale(contentSlot, `${accountLabel} 刷新失败，保留上次成功详情：${error.message}`);
      setLiveAccountDetailHeader(
        slot,
        accountLabel,
        accountData || {},
        isSameAccount ? "刷新失败，仍显示上次成功详情" : `${accountLabel} 加载失败，仍显示上一账户详情`,
      );
    } else {
      replaceChildrenFromHtml(contentSlot, emptyBox("账户详情加载失败", `${accountLabel} · ${error.message}`));
    }
  } finally {
    if (requestId === state.liveAccountDetailRequest && slot.isConnected) {
      slot.removeAttribute("aria-busy");
    }
  }
}

export async function loadLiveAccountMetrics(state, root, requestJson, equityRange) {
  const slot = root.querySelector(sel.liveAccountMetrics());
  if (!slot) return;
  const requestId = ++state.liveAccountMetricsRequest;
  state.selectedLiveAccountMetricsRange = equityRange;
  slot.dataset.requestedRange = equityRange;
  const hasLastGood = Boolean(slot.dataset.renderedRange && slot.children.length);
  if (!hasLastGood && !slot.children.length) {
    replaceChildrenFromHtml(
      slot,
      `<div class="live-account-metrics-loading">${emptyBox("加载四账户时序", "正在读取权益、保证金和回撤历史")}</div>`,
    );
  }
  slot.setAttribute("aria-busy", "true");
  try {
    const fetchKey = String(equityRange);
    let metricsPromise = liveAccountMetricsInFlight.get(fetchKey);
    if (!metricsPromise) {
      const query = new URLSearchParams({ equity_range: equityRange });
      metricsPromise = Promise.resolve(requestJson(`api/live-account-metrics?${query.toString()}`))
        .finally(() => {
          liveAccountMetricsInFlight.delete(fetchKey);
        });
      liveAccountMetricsInFlight.set(fetchKey, metricsPromise);
    }
    const data = await metricsPromise;
    if (requestId !== state.liveAccountMetricsRequest || !slot.isConnected) return;
    const html = renderLiveAccountMetrics(data);
    if (hasLastGood) patchChildrenFromHtml(slot, html);
    else replaceChildrenFromHtml(slot, html);
    slot.dataset.renderedRange = equityRange;
    slot.dataset.requestedRange = equityRange;
    wireLiveAccountMetricsRanges(slot, (nextRange) => loadLiveAccountMetrics(state, root, requestJson, nextRange), state);
    refreshEcharts(slot);
  } catch (error) {
    if (requestId !== state.liveAccountMetricsRequest || !slot.isConnected) return;
    if (hasLastGood) {
      markRefreshStale(slot, `${equityRange} 时序刷新失败，保留上次成功图表：${error.message}`);
    } else {
      replaceChildrenFromHtml(slot, emptyBox("账户时序加载失败", `${equityRange} · ${error.message}`));
    }
  } finally {
    if (requestId === state.liveAccountMetricsRequest && slot.isConnected) {
      slot.removeAttribute("aria-busy");
    }
  }
}
