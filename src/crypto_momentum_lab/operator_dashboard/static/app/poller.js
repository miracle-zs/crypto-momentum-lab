import { POLL_MS, SECTION_POLL_MS } from "../dashboard-config.js";
import { statusClass } from "../dashboard-formatters.js";
import { createScrollGuard, patchChildrenFromHtml } from "../dashboard-dom.js";
import { sectionRenderKey as buildSectionRenderKey } from "../dashboard-rendering.js";
import { emptyBox } from "../dashboard-ui.js";
import {
  SECTION_FETCH_TIMEOUT_MS,
  expireSectionReading,
  forcedSectionRefreshes,
  getLiveMode,
  lastSectionPollAt,
  latestSectionData,
  sectionInFlight,
  sectionRenderKeys,
  setLiveMode,
  setLiveService,
} from "./section-state.js";
import { markSectionError, renderGlobalReadiness, updateGlobalState } from "./readiness.js";
import { renderLiveRuntime } from "./runtime-badge.js";

function fetchTimeoutSignal(timeoutMs) {
  if (typeof AbortSignal !== "undefined" && typeof AbortSignal.timeout === "function") {
    return AbortSignal.timeout(timeoutMs);
  }
  const controller = new AbortController();
  setTimeout(() => controller.abort(), timeoutMs);
  return controller.signal;
}

function resolveApiUrl(endpoint) {
  try {
    const url = new URL(endpoint, window.location.href);
    url.username = "";
    url.password = "";
    return url.pathname + url.search + url.hash;
  } catch {
    return endpoint;
  }
}

export function setSectionStatus(id, status, cacheStatus = null) {
  const section = document.getElementById(id);
  if (!section) return;
  const badge = section.querySelector(".section-state");
  const isStale = cacheStatus === "STALE";
  const displayStatus = isStale ? "STALE" : (status || "UNKNOWN");
  if (badge) {
    badge.className = `section-state ${statusClass(displayStatus)}`;
    badge.textContent = isStale ? `${status || "READY"} (STALE)` : (status || "UNKNOWN");
    if (isStale) {
      badge.setAttribute("title", "响应来源于过期的后台缓存 (X-Cache-Status: STALE)");
    } else {
      badge.removeAttribute("title");
    }
  }
  const dot = document.querySelector(`[data-nav-dot="${id}"]`);
  if (dot) dot.className = `nav-dot ${statusClass(displayStatus)}`;
}

export function createPoller({ renderers, onAfterRender }) {
  const strategyHooks = renderers.__strategyHooks || null;

  function updateGlobalMode(data) {
    const live = data.services?.find((service) => service.name === "live-rollout");
    const halted = (data.active_halt_count || 0) > 0;
    const mode = halted
      ? "HALTED"
      : live?.status === "LIVE"
        ? "LIVE"
        : live?.status === "HALTED"
          ? "HALTED"
          : live?.status === "SHADOW"
            ? "SHADOW"
            : "UNKNOWN";
    setLiveService(live || null);
    if (getLiveMode() === "LIVE" && mode !== "LIVE") {
      ["risk", "account"].forEach((id) => expireSectionReading(id));
    }
    setLiveMode(mode);
    renderLiveRuntime();
    renderGlobalReadiness();
  }

  async function refreshSection(id) {
    if (sectionInFlight.has(id)) return;
    sectionInFlight.add(id);
    const section = document.getElementById(id);
    if (!section) {
      sectionInFlight.delete(id);
      return;
    }
    const endpoint = section.dataset.endpoint;
    if (!endpoint) {
      sectionInFlight.delete(id);
      return;
    }
    const requestUrl = resolveApiUrl(endpoint);
    try {
      const response = await fetch(requestUrl, {
        headers: { "Accept": "application/json" },
        signal: fetchTimeoutSignal(SECTION_FETCH_TIMEOUT_MS),
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const cacheHeader =
        response.headers.get("x-cache-status") ||
        response.headers.get("X-Cache-Status");
      const cacheStatus = cacheHeader ? cacheHeader.toUpperCase() : null;
      const data = await response.json();
      if (data && typeof data === "object") {
        data._cache_status = cacheStatus;
      }
      if (endpoint !== section.dataset.endpoint) return;
      const body = section.querySelector(".panel-body");
      const selectedMarketView = id === "universe"
        ? body?.querySelector("[data-market-view].is-active")?.dataset.marketView
        : null;
      const renderKey = buildSectionRenderKey(id, data);
      const shouldRender = sectionRenderKeys.get(id) !== renderKey;
      if (shouldRender) {
        const renderer = renderers[id];
        if (typeof renderer !== "function") {
          throw new Error(`未定义分区渲染器: ${id}`);
        }
        const [status, html] = renderer(data);
        setSectionStatus(id, status, cacheStatus);
        patchChildrenFromHtml(body, html);
        sectionRenderKeys.set(id, renderKey);
        if (id === "universe") onAfterRender?.wireMarketViews?.(body, selectedMarketView);
        onAfterRender?.wireTableFilters?.(body);
      } else {
        const currentBadgeText = section?.querySelector(".section-state")?.textContent || "";
        const baseStatus = currentBadgeText.replace(" (STALE)", "").trim();
        setSectionStatus(id, baseStatus, cacheStatus);
      }
      body?.classList.remove("loading");
      body?.removeAttribute("aria-busy");
      if (id === "strategy" && strategyHooks) {
        if (shouldRender) strategyHooks.wire(body, data);
        else strategyHooks.refresh(body, data);
      }
      if (id === "account" && onAfterRender?.account) {
        if (shouldRender) onAfterRender.account.wire(body, data);
        else onAfterRender.account.updateDynamic(body, data);
      }
      if (id === "overview") updateGlobalMode(data);
      updateGlobalState(id, data);
    } catch (error) {
      console.error(`[FlightDeck] 刷新分区 ${id} 失败:`, error);
      const body = section.querySelector(".panel-body");
      if (endpoint !== section.dataset.endpoint) return;
      const timedOut = error?.name === "TimeoutError" || error?.name === "AbortError";
      const reason = timedOut ? `请求超时（>${SECTION_FETCH_TIMEOUT_MS / 1000}s）` : error.message;
      const errorHtml = emptyBox("数据加载未完成", `${endpoint} · ${reason}`);
      const errorKey = `error:${endpoint}:${reason}`;
      const hadPriorSuccess = sectionRenderKeys.has(id) && !sectionRenderKeys.get(id).startsWith("error:");
      if (!hadPriorSuccess) {
        setSectionStatus(id, "UNKNOWN");
        if (sectionRenderKeys.get(id) !== errorKey) {
          patchChildrenFromHtml(body, errorHtml);
          sectionRenderKeys.set(id, errorKey);
        }
      } else {
        setSectionStatus(id, "STALE");
      }
      body?.classList.remove("loading");
      if (latestSectionData.has(id)) {
        markSectionError(id, reason);
      } else {
        updateGlobalState(id, { status: "UNKNOWN", error: true });
      }
    } finally {
      sectionInFlight.delete(id);
    }
  }

  async function poll(force = false) {
    if (document.hidden && !force) return;
    const now = Date.now();
    const activeView = document.body.dataset.activeView || "overview";
    const visibleSections = new Set(["overview", activeView]);
    if (getLiveMode() === "LIVE") {
      visibleSections.add("risk");
      visibleSections.add("account");
    }
    forcedSectionRefreshes.forEach((id) => visibleSections.add(id));
    const allSectionIds = Array.from(new Set([
      ...Object.keys(renderers).filter((id) => id !== "__strategyHooks" && document.getElementById(id)?.dataset?.endpoint),
    ]));
    const dueSections = allSectionIds.filter((id) => {
      if (!visibleSections.has(id)) return false;
      if (sectionInFlight.has(id)) return false;
      if (force) return true;
      const lastPolledAt = lastSectionPollAt.get(id);
      const interval = SECTION_POLL_MS[id] || POLL_MS;
      return lastPolledAt == null || now - lastPolledAt >= interval;
    });
    if (!dueSections.length) return;
    dueSections.forEach((id) => lastSectionPollAt.set(id, now));
    const pollbar = document.getElementById("pollbar");
    if (pollbar) {
      pollbar.classList.remove("run");
      requestAnimationFrame(() => {
        pollbar.classList.add("run");
      });
    }
    const refreshBtn = document.getElementById("manual-refresh-btn");
    if (refreshBtn) refreshBtn.classList.add("is-refreshing");
    const scrollGuard = typeof window !== "undefined" ? createScrollGuard() : null;
    await Promise.allSettled(dueSections.map(refreshSection));
    if (refreshBtn) refreshBtn.classList.remove("is-refreshing");
    onAfterRender?.pollState?.();
    if (scrollGuard) {
      scrollGuard.restore();
      requestAnimationFrame(() => {
        scrollGuard.restore();
        requestAnimationFrame(() => scrollGuard.restore());
      });
    }
  }

  function maybeRefresh(id) {
    const section = document.getElementById(id);
    const isRefreshable = Boolean(section?.dataset?.endpoint && renderers[id]);
    if (!isRefreshable) return;
    const body = section.querySelector(".panel-body");
    const lastPolledAt = lastSectionPollAt.get(id);
    const interval = SECTION_POLL_MS[id] || POLL_MS;
    const isDue = lastPolledAt == null || Date.now() - lastPolledAt >= interval;
    const isLoading = body?.classList.contains("loading");
    if ((isLoading || isDue) && !sectionInFlight.has(id)) {
      lastSectionPollAt.set(id, Date.now());
      void refreshSection(id);
    }
  }

  return { poll, refreshSection, maybeRefresh, updateGlobalMode };
}
