import { POLL_MS } from "./dashboard-config.js";
import { timeOnly } from "./dashboard-formatters.js";
import { refreshChartColors, wireEcharts } from "./dashboard-chart-engine.js";
import { renderOverview, updateOverviewDynamic } from "./sections/overview.js";
import { renderUniverse } from "./sections/universe.js";
import { renderRisk } from "./sections/risk.js";
import { renderCollector } from "./sections/collector.js";
import {
  renderLiveAccounts,
  selectLiveAccount,
  wireLiveAccounts,
  updateLiveAccountsDynamic,
} from "./sections/account.js";
import { renderPerformance } from "./sections/performance.js";
import { latestSectionData } from "./app/section-state.js";
import { renderLiveRuntime } from "./app/runtime-badge.js";
import { createPoller } from "./app/poller.js";
import { createShell } from "./app/shell.js";
import {
  wireClickToCopy,
  wireGlobalShortcuts,
  wireMarketViews,
  wireTableFilters,
  wireTableSorting,
} from "./app/wire-widgets.js";
import { installJumpProbe } from "./app/jump-probe.js";

const renderers = {
  overview: renderOverview,
  universe: renderUniverse,
  collector: renderCollector,
  risk: renderRisk,
  account: renderLiveAccounts,
  performance: renderPerformance,
};

function renderPollState() {
  renderLiveRuntime();
  updateOverviewDynamic(
    document.getElementById("overview"),
    latestSectionData.get("overview"),
  );
}

function tick() {
  const clock = document.getElementById("utc-clock");
  if (clock) clock.textContent = timeOnly(new Date());
  renderPollState();
}

const poller = createPoller({
  renderers,
  onAfterRender: {
    wireMarketViews,
    wireTableFilters,
    wireClickToCopy,
    wireTableSorting,
    account: {
      wire: wireLiveAccounts,
      updateDynamic: updateLiveAccountsDynamic,
    },
    pollState: renderPollState,
  },
});

const shell = createShell({
  onNavigate: (id) => {
    if (id === "account") {
      const label = new URLSearchParams(window.location.hash.split("?")[1]).get("account");
      if (label) {
        void selectLiveAccount(document.getElementById("account")?.querySelector(".panel-body"), label);
      }
    }
    poller.maybeRefresh(id);
  },
});

const refreshBtn = document.getElementById("manual-refresh-btn");
if (refreshBtn) {
  refreshBtn.addEventListener("click", () => {
    poller.poll(true);
  });
}

function initPalette() {
  const btn = document.getElementById("palette-toggle-btn");
  const label = document.getElementById("palette-label");
  if (!btn) return;
  let stored = "intl";
  try { stored = window.localStorage?.getItem("cml-palette") || "intl"; } catch {}
  const apply = (palette) => {
    document.documentElement.dataset.colorPalette = palette;
    if (label) {
      label.textContent = palette === "cn" ? "国内配色 (红涨绿跌)" : "国际配色 (绿涨红跌)";
    }
    refreshChartColors();
  };
  apply(stored);
  btn.addEventListener("click", () => {
    const current = document.documentElement.dataset.colorPalette === "cn" ? "cn" : "intl";
    const next = current === "cn" ? "intl" : "cn";
    try { window.localStorage?.setItem("cml-palette", next); } catch {}
    apply(next);
  });
}

initPalette();
shell.selectView(shell.initialView(), { updateHistory: false });
installJumpProbe();
wireEcharts(document);
wireGlobalShortcuts(shell, poller);
tick();
poller.poll();
setInterval(tick, 1000);
setInterval(poller.poll, POLL_MS);

document.addEventListener("visibilitychange", () => {
  if (!document.hidden) {
    tick();
    void poller.poll(true);
  }
});
