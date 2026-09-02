import {
  SECTIONS,
  POLL_MS,
  SECTION_POLL_MS,
} from "./dashboard-config.js?v=20260830-poll-budget-v1";
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
import { readinessStatusForSection } from "./dashboard-readiness.js";
import { wireEcharts } from "./dashboard-chart-engine.js";
import { emptyBox } from "./dashboard-ui.js?v=20260826-flight-deck-v2";
import { renderOverview, updateOverviewDynamic } from "./sections/overview.js";
import { renderUniverse } from "./sections/universe.js";
import { renderRisk } from "./sections/risk.js";
import {
  renderAccount,
  wireAccountEquityRanges,
} from "./sections/account.js?v=20260902-live-rank-null-fix-v1";
import { renderReports } from "./sections/reports.js";
import { createStrategySection } from "./sections/strategy.js?v=20260826-flight-deck-v2";

// Legacy import markers retained for static asset manifests: from "./sections/account.js"
// from "./sections/strategy.js" from "./sections/overview.js" from "./sections/universe.js"
// from "./sections/risk.js" from "./sections/reports.js"

let pollInFlight = false;
const lastSectionPollAt = new Map();
let latestLiveService = null;
let latestLiveMode = "UNKNOWN";
let lastRuntimeAnnouncement = "";
const latestSectionData = new Map();
const sectionRenderKeys = new Map();
const SAFETY_SECTIONS = new Set(["overview", "risk", "account", "strategy", "universe"]);
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
  const activeHalts = Math.max(
    asNumber(overview?.active_halt_count) || 0,
    risk?.active_halts?.length || 0,
  );
  const ambiguous = risk?.ambiguous_orders?.length || 0;
  const mismatch = asNumber(account?.reconciliation?.mismatch_count);
  const accountStatus = normalizedStatus(account?.status);
  let reconciliation = "—";
  if (account) {
    reconciliation = hasUncertainStatus(accountStatus)
      ? "UNKNOWN"
      : mismatch != null && mismatch > 0
        ? `${mismatch} 差异`
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
  risk: renderRisk,
  account: renderAccount,
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

function sectionRenderKey(id, data) {
  if (id !== "overview") return JSON.stringify(data);
  const snapshot = JSON.parse(JSON.stringify(data));
  delete snapshot.generated_at;
  for (const service of snapshot.services || []) {
    delete service.age_seconds;
    delete service.observed_at;
  }
  return JSON.stringify(snapshot);
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
    const renderKey = sectionRenderKey(id, data);
    const shouldRender = sectionRenderKeys.get(id) !== renderKey;
    if (shouldRender) {
      const [status, html] = renderers[id](data);
      setSectionStatus(id, status);
      replaceChildrenFromHtml(body, html);
      sectionRenderKeys.set(id, renderKey);
    }
    body.classList.remove("loading");
    body.removeAttribute("aria-busy");
    if (id === "strategy" && shouldRender) strategySection.wire(body, data);
    if (id === "account" && shouldRender) {
      wireAccountEquityRanges(body, async (equityRange) => {
        section.dataset.endpoint = `api/account?equity_range=${encodeURIComponent(equityRange)}`;
        sectionRenderKeys.delete(id);
        body.setAttribute("aria-busy", "true");
        await refreshSection(id);
      });
    }
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

const navLinks = new Map(
  Array.from(document.querySelectorAll(".nav a")).map((link) => [link.dataset.nav, link]),
);
const viewCards = Array.from(document.querySelectorAll("main .card"));
const viewIds = new Set(viewCards.map((card) => card.id));

function normalizedView(value) {
  const id = String(value || "").replace(/^#/, "");
  return viewIds.has(id) ? id : "overview";
}

function viewLabel(id) {
  const card = document.getElementById(id);
  return card?.dataset.viewTitle || navLinks.get(id)?.dataset.label || id;
}

function selectView(value, { updateHistory = true } = {}) {
  const id = normalizedView(value);
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
  const activeLabel = document.getElementById("active-view-label");
  if (activeLabel) activeLabel.textContent = viewLabel(id);
  document.body.dataset.activeView = id;
  document.title = `CML · ${viewLabel(id)} · Flight Deck`;
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
  if (activeLink && window.innerWidth <= 1023) {
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

window.addEventListener("popstate", () => selectView(window.location.hash, { updateHistory: false }));
window.addEventListener("hashchange", () => selectView(window.location.hash, { updateHistory: false }));

selectView(window.location.hash, { updateHistory: false });

wireEcharts(document);

tick();
poll();
setInterval(tick, 1000);
setInterval(poll, POLL_MS);
