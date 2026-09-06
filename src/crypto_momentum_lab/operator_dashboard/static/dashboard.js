import {
  SECTIONS,
  POLL_MS,
  SECTION_POLL_MS,
} from "./dashboard-config.js?v=20260903-research-collector-v1";
import {
  statusClass,
  normalizedStatus,
  hasUncertainStatus,
  asNumber,
  timeOnly,
  elapsedTime,
  relAge,
  liveHeartbeatAge,
  liveHeartbeatStatus,
} from "./dashboard-formatters.js";
import { replaceChildrenFromHtml } from "./dashboard-dom.js";
import { sectionRenderKey as buildSectionRenderKey } from "./dashboard-rendering.js";
import { readinessStatusForSection } from "./dashboard-readiness.js";
import { wireEcharts } from "./dashboard-chart-engine.js";
import { emptyBox } from "./dashboard-ui.js?v=20260826-flight-deck-v2";
import { renderOverview, updateOverviewDynamic } from "./sections/overview.js";
import { renderUniverse } from "./sections/universe.js";
import { renderRisk } from "./sections/risk.js";
import { renderCollector } from "./sections/collector.js";
import {
  renderLiveAccounts,
  wireLiveAccounts,
} from "./sections/account.js?v=20260906-live-metric-fleet-v1";
import { renderReports } from "./sections/reports.js";
import { createStrategySection } from "./sections/strategy.js?v=20260906-live-account-labels-v1";

// Legacy import markers retained for static asset manifests: from "./sections/account.js"
// from "./sections/strategy.js" from "./sections/overview.js" from "./sections/universe.js"
// from "./sections/risk.js" from "./sections/reports.js"
// Legacy detail endpoint marker retained for account range clients: api/account?equity_range=

let pollInFlight = false;
const lastSectionPollAt = new Map();
let latestLiveService = null;
let latestLiveMode = "UNKNOWN";
let lastRuntimeAnnouncement = "";
const latestSectionData = new Map();
const sectionRenderKeys = new Map();
const SAFETY_SECTIONS = new Set(["overview", "risk", "account", "strategy", "universe"]);
function sectionRenderKey(id, data) {
  return buildSectionRenderKey(id, data);
}

function renderLiveRuntime() {
  // Compatibility wording retained for consumers that still recognize:
  // 实盘状态：${mode} · ${duration} / 实盘心跳：${relAge(age)} · ${freshness}
  const stamp = document.getElementById("global-mode");
  const heartbeat = document.getElementById("last-cycle");
  const heartbeatRow = heartbeat?.closest(".poll-state");
  const modeValue = stamp?.querySelector("[data-mode-value]");
  const modeDetail = stamp?.querySelector("[data-mode-detail]");
  if (!stamp || !heartbeat) return;

  const mode = latestLiveMode || "UNKNOWN";
  const startedAt = latestLiveService?.details?.started_at;
  let duration = "等待数据";
  if (mode === "LIVE") {
    duration = startedAt
      ? `已运行 ${elapsedTime(startedAt, new Date())}`
      : "运行时间未知";
  } else if (mode === "HALTED") {
    duration = "已停止";
  } else if (mode === "SHADOW") {
    duration = "未启用";
  }
  stamp.className = `mode-badge runtime-line ${statusClass(mode)}`;
  stamp.setAttribute("aria-label", `执行模式：${mode} · ${duration}`);
  if (modeValue) modeValue.textContent = mode;
  else stamp.textContent = `执行模式：${mode} · ${duration}`;
  if (modeDetail) modeDetail.textContent = duration;

  const age = liveHeartbeatAge(latestLiveService);
  const freshness = liveHeartbeatStatus(age);
  heartbeat.textContent = age == null
    ? "UNKNOWN · 等待数据"
    : `${freshness} · ${relAge(age)}`;
  if (heartbeatRow) heartbeatRow.className = `poll-state ${statusClass(freshness)}`;

  const announcement = `执行模式 ${mode}，${freshness === "UNKNOWN" ? "心跳未知" : `心跳${freshness}`}`;
  const announcer = document.getElementById("runtime-announcer");
  if (announcer && announcement !== lastRuntimeAnnouncement) {
    announcer.textContent = announcement;
    lastRuntimeAnnouncement = announcement;
  }
}

function updateGlobalState(id, data) {
  latestSectionData.set(id, data);
  renderGlobalReadiness();
}

function globalReadinessModel() {
  const overview = latestSectionData.get("overview");
  const risk = latestSectionData.get("risk");
  const account = latestSectionData.get("account");
  const snapshots = [...latestSectionData.values()].filter(Boolean);
  if (!snapshots.length) {
    return {
      status: "UNKNOWN",
      detail: "等待关键服务数据",
      uncertain: "—",
      halts: "—",
      ambiguous: "—",
      reconciliation: "—",
    };
  }

  const services = overview?.services || [];
  const uncertainSections = [...latestSectionData.entries()]
    .filter(([id, data]) => (
      SAFETY_SECTIONS.has(id)
      && hasUncertainStatus(readinessStatusForSection(id, data))
    ))
    .length;
  const uncertainServices = services.filter((service) => hasUncertainStatus(service.status)).length;
  const uncertain = uncertainSections + uncertainServices;
  const ambiguous = risk?.ambiguous_orders?.length || 0;
  const accountSnapshots = Array.isArray(account?.accounts)
    ? account.accounts
    : account
      ? [account]
      : [];
  const haltedAccounts = accountSnapshots.filter(
    (snapshot) => normalizedStatus(snapshot.status) === "HALTED",
  ).length;
  const activeHalts = Math.max(
    asNumber(overview?.active_halt_count) || 0,
    risk?.active_halts?.length || 0,
    haltedAccounts,
  );
  const mismatch = accountSnapshots.reduce(
    (total, snapshot) => total + (asNumber(snapshot.reconciliation?.mismatch_count) || 0),
    0,
  );
  const accountStatus = normalizedStatus(account?.status);
  let reconciliation = "—";
  if (account) {
    reconciliation = hasUncertainStatus(accountStatus)
      ? "UNKNOWN"
      : haltedAccounts > 0
        ? `${haltedAccounts} 停止`
        : mismatch != null && mismatch > 0
          ? `${mismatch} 差异`
          : accountSnapshots.length > 1
            ? "READY"
            : String(account.reconciliation?.status || "READY").toUpperCase();
  }

  let status = "READY";
  let detail = "关键读数正常";
  if (!overview) {
    status = "UNKNOWN";
    detail = "等待系统总览数据";
  } else if (activeHalts > 0) {
    status = "BLOCKED";
    detail = "存在活跃停机 · 新入场已被阻断";
  } else if (ambiguous > 0) {
    status = "REVIEW";
    detail = "存在未决订单 · 需要交易所对账";
  } else if (mismatch != null && mismatch > 0) {
    status = "REVIEW";
    detail = "账户对账存在差异 · 暂不视为安全";
  } else if (uncertain > 0) {
    status = "UNKNOWN";
    detail = `${uncertain} 个关键读数需要确认`;
  } else if (latestLiveMode === "LIVE") {
    detail = "实盘链路运行中 · 关键读数正常";
  } else if (latestLiveMode === "SHADOW") {
    detail = "影子路径运行中 · 关键读数正常";
  } else {
    detail = "无实盘会话 · 只读安全";
  }

  return {
    status,
    detail,
    uncertain: String(uncertain),
    halts: String(activeHalts),
    ambiguous: String(ambiguous),
    reconciliation,
  };
}

function renderGlobalReadiness() {
  const strip = document.getElementById("readiness-strip");
  if (!strip) return;
  const model = globalReadinessModel();
  strip.className = `readiness-strip ${statusClass(model.status)}`;
  const values = {
    "global-readiness": model.status,
    "global-readiness-detail": model.detail,
    "global-uncertain": model.uncertain,
    "global-halts": model.halts,
    "global-ambiguous": model.ambiguous,
    "global-reconciliation": model.reconciliation,
  };
  Object.entries(values).forEach(([id, value]) => {
    const element = document.getElementById(id);
    if (element) element.textContent = value;
  });
}

/* ---------- polling engine ---------- */

const strategySection = createStrategySection();

const renderers = {
  overview: renderOverview,
  strategy: strategySection.render,
  universe: renderUniverse,
  collector: renderCollector,
  risk: renderRisk,
  account: renderLiveAccounts,
  reports: renderReports,
};

function setSectionStatus(id, status) {
  const section = document.getElementById(id);
  const badge = section.querySelector(".section-state");
  badge.className = `section-state ${statusClass(status)}`;
  badge.textContent = status || "UNKNOWN";
  const dot = document.querySelector(`[data-nav-dot="${id}"]`);
  if (dot) dot.className = `nav-dot ${statusClass(status)}`;
}

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
  latestLiveService = live || null;
  latestLiveMode = mode;
  renderLiveRuntime();
  renderGlobalReadiness();
}

function wireMarketTab(tab) {
  if (!tab || tab.dataset.marketWired === "true") return;
  tab.dataset.marketWired = "true";
  tab.addEventListener("click", () => {
    const board = tab.closest("[data-market-board]");
    if (!board) return;
    applyMarketView(board, tab.dataset.marketView);
  });
}

function applyMarketView(board, view = "rankings") {
  if (!board) return;
  board.querySelectorAll("[data-market-view]").forEach((candidate) => {
    const active = candidate.dataset.marketView === view;
    candidate.classList.toggle("is-active", active);
    candidate.setAttribute("aria-selected", String(active));
  });
  board.querySelectorAll("[data-market-panel]").forEach((panel) => {
    panel.hidden = panel.dataset.marketPanel !== view;
  });
}

function wireMarketViews(root, selectedView = null) {
  const board = root?.querySelector("[data-market-board]");
  if (!board) return;
  board.querySelectorAll("[data-market-view]").forEach(wireMarketTab);
  if (selectedView) applyMarketView(board, selectedView);
}

async function refreshSection(id) {
  const section = document.getElementById(id);
  const endpoint = section.dataset.endpoint;
  try {
    const response = await fetch(endpoint, { headers: { "Accept": "application/json" } });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    if (endpoint !== section.dataset.endpoint) return;
    const body = section.querySelector(".panel-body");
    const selectedMarketView = id === "universe"
      ? body.querySelector("[data-market-view].is-active")?.dataset.marketView
      : null;
    const renderKey = sectionRenderKey(id, data);
    const shouldRender = sectionRenderKeys.get(id) !== renderKey;
    if (shouldRender) {
      const [status, html] = renderers[id](data);
      setSectionStatus(id, status);
      replaceChildrenFromHtml(body, html);
      sectionRenderKeys.set(id, renderKey);
      if (id === "universe") wireMarketViews(body, selectedMarketView);
    }
    body.classList.remove("loading");
    body.removeAttribute("aria-busy");
    if (id === "strategy") {
      if (shouldRender) strategySection.wire(body, data);
      else strategySection.refresh(body, data);
    }
    if (id === "account" && shouldRender) wireLiveAccounts(body, data);
    if (id === "overview") updateGlobalMode(data);
    updateGlobalState(id, data);
  } catch (error) {
    setSectionStatus(id, "UNKNOWN");
    const body = section.querySelector(".panel-body");
    if (endpoint !== section.dataset.endpoint) return;
    const errorHtml = emptyBox("接口不可达", `${endpoint} · ${error.message}`);
    const errorKey = `error:${endpoint}:${error.message}`;
    if (sectionRenderKeys.get(id) !== errorKey) {
      replaceChildrenFromHtml(body, errorHtml);
      sectionRenderKeys.set(id, errorKey);
    }
    body.classList.remove("loading");
    updateGlobalState(id, { status: "UNKNOWN", error: true });
  }
}

async function poll() {
  if (document.hidden) return;
  if (pollInFlight) return;
  const now = Date.now();
  const activeView = document.body.dataset.activeView || "overview";
  const visibleSections = new Set(["overview", activeView]);
  const dueSections = SECTIONS.filter((id) => {
    if (!visibleSections.has(id)) return false;
    const lastPolledAt = lastSectionPollAt.get(id);
    const interval = SECTION_POLL_MS[id] || POLL_MS;
    return lastPolledAt == null || now - lastPolledAt >= interval;
  });
  if (!dueSections.length) return;
  dueSections.forEach((id) => lastSectionPollAt.set(id, now));
  pollInFlight = true;
  try {
    await Promise.allSettled(dueSections.map(refreshSection));
    const pollbar = document.getElementById("pollbar");
    pollbar.classList.remove("run");
    void pollbar.offsetWidth;
    pollbar.classList.add("run");
    renderPollState();
  } finally {
    pollInFlight = false;
  }
}

function renderPollState() {
  renderLiveRuntime();
  updateOverviewDynamic(
    document.getElementById("overview"),
    latestSectionData.get("overview"),
  );
}

function tick() {
  document.getElementById("utc-clock").textContent = timeOnly(new Date());
  renderPollState();
}

/* ---------- view navigation ---------- */

const WORKSPACES = Object.freeze({
  ops: {
    label: "运行控制",
    kicker: "OPERATIONS",
    purpose: "安全 · 账户 · 命令",
    defaultView: "overview",
  },
  data: {
    label: "策略与数据",
    kicker: "ANALYTICS",
    purpose: "策略 · 市场 · 采集",
    defaultView: "strategy",
  },
});

const VIEW_PURPOSES = Object.freeze({
  overview: "现在是否可信",
  risk: "风险与未决订单",
  account: "真实账户与暴露",
  reports: "运行事件与迁移",
  actions: "受控命令",
  strategy: "策略版本与权益",
  universe: "市场排名与监控池",
  collector: "数据链路健康",
});

const navLinks = new Map(
  Array.from(document.querySelectorAll(".nav a")).map((link) => [link.dataset.nav, link]),
);
const workspaceTabs = Array.from(document.querySelectorAll("[data-workspace-tab]"));
const workspaceNavs = Array.from(document.querySelectorAll("[data-workspace-nav]"));
const viewCards = Array.from(document.querySelectorAll("main .card"));
const viewIds = new Set(viewCards.map((card) => card.id));
const workspaceForView = new Map(
  viewCards.map((card) => [card.id, card.dataset.workspace || "ops"]),
);

function storedWorkspace() {
  try {
    const value = window.localStorage?.getItem("cml-dashboard-workspace");
    return Object.prototype.hasOwnProperty.call(WORKSPACES, value) ? value : "ops";
  } catch {
    return "ops";
  }
}

function normalizedView(value) {
  const id = String(value || "").replace(/^#/, "");
  return viewIds.has(id) ? id : "overview";
}

function viewLabel(id) {
  const card = document.getElementById(id);
  return card?.dataset.viewTitle || navLinks.get(id)?.dataset.label || id;
}

function syncWorkspace(workspace) {
  const key = WORKSPACES[workspace] ? workspace : "ops";
  const meta = WORKSPACES[key];
  document.body.dataset.activeWorkspace = key;
  document.body.dataset.workspace = key;
  workspaceTabs.forEach((tab) => {
    const active = tab.dataset.workspaceTab === key;
    tab.classList.toggle("is-active", active);
    tab.setAttribute("aria-selected", String(active));
  });
  workspaceNavs.forEach((nav) => {
    const active = nav.dataset.workspaceNav === key;
    nav.hidden = !active;
    nav.setAttribute("aria-hidden", String(!active));
  });
  const kicker = document.getElementById("active-workspace-kicker");
  if (kicker) kicker.textContent = `${meta.label} / ${meta.kicker}`;
}

function updateViewMeta(id) {
  const meta = WORKSPACES[workspaceForView.get(id)] || WORKSPACES.ops;
  const activeLabel = document.getElementById("active-view-label");
  if (activeLabel) activeLabel.textContent = viewLabel(id);
  const purpose = document.getElementById("active-view-purpose");
  if (purpose) purpose.textContent = VIEW_PURPOSES[id] || meta.purpose;
  const kicker = document.getElementById("active-workspace-kicker");
  if (kicker) kicker.textContent = `${meta.label} / ${meta.kicker}`;
}

function setWorkspace(value, { selectDefault = true, updateHistory = true } = {}) {
  const key = WORKSPACES[value] ? value : "ops";
  syncWorkspace(key);
  try {
    window.localStorage?.setItem("cml-dashboard-workspace", key);
  } catch {
    // Storage can be unavailable in private or embedded browser contexts.
  }
  const current = normalizedView(document.body.dataset.activeView || window.location.hash);
  if (selectDefault && workspaceForView.get(current) !== key) {
    selectView(WORKSPACES[key].defaultView, { updateHistory });
  } else if (current) {
    updateViewMeta(current);
  }
}

function selectView(value, { updateHistory = true } = {}) {
  const id = normalizedView(value);
  syncWorkspace(workspaceForView.get(id) || "ops");
  viewCards.forEach((card) => {
    const active = card.id === id;
    card.hidden = !active;
    card.classList.toggle("is-active-view", active);
  });
  navLinks.forEach((link, linkId) => {
    const active = linkId === id;
    link.classList.toggle("active", active);
    if (active) link.setAttribute("aria-current", "page");
    else link.removeAttribute("aria-current");
  });
  updateViewMeta(id);
  document.body.dataset.activeView = id;
  const skipLink = document.querySelector(".skip-link");
  if (skipLink) skipLink.href = `#${id}`;
  const workspace = WORKSPACES[workspaceForView.get(id)] || WORKSPACES.ops;
  document.title = `CML · ${workspace.label} · ${viewLabel(id)} · Flight Deck`;
  if (updateHistory && window.location.hash !== `#${id}`) {
    window.history.pushState(null, "", `#${id}`);
  }
  if (updateHistory && window.scrollY > 0) {
    window.scrollTo({
      top: 0,
      behavior: window.matchMedia?.("(prefers-reduced-motion: reduce)").matches ? "auto" : "smooth",
    });
  }
  const activeLink = navLinks.get(id);
  if (activeLink && window.innerWidth <= 1023 && !activeLink.closest("[hidden]")) {
    activeLink.scrollIntoView({ block: "nearest", inline: "center" });
  }
  requestAnimationFrame(() => window.dispatchEvent(new Event("resize")));
}

navLinks.forEach((link, id) => {
  link.addEventListener("click", (event) => {
    if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
    event.preventDefault();
    selectView(id);
  });
});

workspaceTabs.forEach((tab) => {
  tab.addEventListener("click", () => {
    setWorkspace(tab.dataset.workspaceTab);
  });
});

window.addEventListener("popstate", () => selectView(window.location.hash, { updateHistory: false }));
window.addEventListener("hashchange", () => selectView(window.location.hash, { updateHistory: false }));

const initialHash = String(window.location.hash || "").replace(/^#/, "");
const initialView = viewIds.has(initialHash)
  ? initialHash
  : WORKSPACES[storedWorkspace()].defaultView;
selectView(initialView, { updateHistory: false });

wireEcharts(document);

tick();
poll();
setInterval(tick, 1000);
setInterval(poll, POLL_MS);
