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
let liveAccountMetricsInFlight;

/**
 * A cached API response can be polled again before its source has changed.
 * Keep the already-rendered, scroll-heavy region intact in that case instead
 * of reconciling the same DOM tree and asking ECharts to scan its surfaces.
 */
export function renderedPayloadMatches(
  slot,
  payload,
  { accountLabel = null } = {},
) {
  if (!slot || slot.__cmlRenderedPayload === undefined) return false;
  if (accountLabel != null && slot.dataset.renderedAccount !== accountLabel) return false;
  return slot.__cmlRenderedPayload === JSON.stringify(payload);
}

function rememberRenderedPayload(slot, payload) {
  slot.__cmlRenderedPayload = JSON.stringify(payload);
}

function markRefreshStale(root, message) {
  const state = root.querySelector(sel.refreshState());
  if (!state) return;
  state.hidden = false;
  state.textContent = message;
  state.title = message;
  root.dataset.refreshState = "stale";
}

function detailContentSlot(slot) {
  return slot.querySelector(sel.liveAccountDetailContent()) || slot;
}

function setLiveAccountDetailHeader(slot, accountLabel, data = {}, loadingMessage = "") {
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

function setLiveAccountTabState(root, accountLabel) {
  root.querySelectorAll(sel.accountCards()).forEach((button) => {
    const active = button.dataset.liveAccountLabel === accountLabel;
    button.classList.toggle("is-selected", active);
    button.setAttribute("aria-selected", String(active));
    button.setAttribute("tabindex", active ? "0" : "-1");
  });
}

export async function loadLiveAccountDetail(
  state,
  root,
  accountLabel,
  requestJson,
) {
  const slot = root.querySelector(sel.liveAccountDetail());
  if (!slot) return;
  const contentSlot = detailContentSlot(slot);
  const selectedCard = [...root.querySelectorAll(sel.accountCards())]
    .find((button) => button.dataset.liveAccountLabel === accountLabel);
  if (selectedCard?.id) slot.setAttribute("aria-labelledby", selectedCard.id);
  const accountData = selectedCard ? (root.__liveAccountData || []).find(
    (account) => account.account_label === accountLabel,
  ) : null;
  const requestId = ++state.liveAccountDetailRequest;
  state.selectedLiveAccount = accountLabel;
  setLiveAccountTabState(root, accountLabel);
  slot.dataset.accountLabel = accountLabel;
  const renderedAccount = slot.dataset.renderedAccount;
  const isSameAccount = renderedAccount === accountLabel;
  const hasLastGood = Boolean(renderedAccount && contentSlot.children.length);
  setLiveAccountDetailHeader(
    slot,
    accountLabel,
    accountData || {},
    !isSameAccount && hasLastGood ? `正在读取 ${accountLabel} 快照；保留上次内容` : "",
  );

  if (!hasLastGood && !contentSlot.children.length) {
    replaceChildrenFromHtml(
      contentSlot,
      `<div class="lazy-detail"><strong>账户详情加载中…</strong><small>${esc(accountLabel)} · 正在读取最新快照</small></div>`,
    );
  }
  slot.setAttribute("aria-busy", "true");
  try {
    const fetchKey = accountLabel;
    let detailPromise = liveAccountDetailInFlight.get(fetchKey);
    if (!detailPromise) {
      const query = new URLSearchParams({ account_label: accountLabel });
      detailPromise = Promise.resolve(requestJson(`api/account?${query.toString()}`))
        .finally(() => {
          liveAccountDetailInFlight.delete(fetchKey);
        });
      liveAccountDetailInFlight.set(fetchKey, detailPromise);
    }
    const detail = await detailPromise;
    if (requestId !== state.liveAccountDetailRequest || !slot.isConnected) return;
    if (renderedPayloadMatches(slot, detail, { accountLabel })) return;
    const [status, html] = renderAccount(detail);
    if (slot.dataset.renderedAccount === accountLabel) patchChildrenFromHtml(contentSlot, html);
    else replaceChildrenFromHtml(contentSlot, html);
    slot.dataset.accountStatus = status;
    slot.dataset.renderedAccount = accountLabel;
    rememberRenderedPayload(slot, detail);
    setLiveAccountDetailHeader(slot, accountLabel, detail);
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

export async function loadLiveAccountMetrics(state, root, requestJson) {
  const slot = root.querySelector(sel.liveAccountMetrics());
  if (!slot) return;
  const requestId = ++state.liveAccountMetricsRequest;
  const hasLastGood = slot.__cmlRenderedPayload !== undefined && slot.children.length > 0;
  if (!hasLastGood && !slot.children.length) {
    replaceChildrenFromHtml(
      slot,
      `<div class="live-account-metrics-loading">${emptyBox("加载四账户时序", "正在读取权益、保证金和回撤历史")}</div>`,
    );
  }
  slot.setAttribute("aria-busy", "true");
  try {
    let metricsPromise = liveAccountMetricsInFlight;
    if (!metricsPromise) {
      metricsPromise = Promise.resolve(requestJson("api/live-account-metrics"))
        .finally(() => {
          liveAccountMetricsInFlight = undefined;
        });
      liveAccountMetricsInFlight = metricsPromise;
    }
    const data = await metricsPromise;
    if (requestId !== state.liveAccountMetricsRequest || !slot.isConnected) return;
    if (renderedPayloadMatches(slot, data)) return;
    const html = renderLiveAccountMetrics(data);
    if (hasLastGood) patchChildrenFromHtml(slot, html);
    else replaceChildrenFromHtml(slot, html);
    rememberRenderedPayload(slot, data);
    refreshEcharts(slot);
  } catch (error) {
    if (requestId !== state.liveAccountMetricsRequest || !slot.isConnected) return;
    if (hasLastGood) {
      markRefreshStale(slot, `资金时序刷新失败，保留上次成功图表：${error.message}`);
    } else {
      replaceChildrenFromHtml(slot, emptyBox("账户时序加载失败", error.message));
    }
  } finally {
    if (requestId === state.liveAccountMetricsRequest && slot.isConnected) {
      slot.removeAttribute("aria-busy");
    }
  }
}
