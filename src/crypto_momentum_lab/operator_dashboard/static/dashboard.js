import { POLL_MS } from "./dashboard-config.js";
import { timeOnly } from "./dashboard-formatters.js";
import { wireEcharts } from "./dashboard-chart-engine.js";
import { renderOverview, updateOverviewDynamic } from "./sections/overview.js";
import { renderUniverse } from "./sections/universe.js";
import { renderRisk } from "./sections/risk.js";
import { renderCollector } from "./sections/collector.js";
import {
  renderLiveAccounts,
  wireLiveAccounts,
  updateLiveAccountsDynamic,
} from "./sections/account.js";
import { renderReports } from "./sections/reports.js";
import { renderPerformance } from "./sections/performance.js";
import { createStrategySection } from "./sections/strategy.js";
import { latestSectionData } from "./app/section-state.js";
import { renderLiveRuntime } from "./app/runtime-badge.js";
import { createPoller } from "./app/poller.js";
import { createShell } from "./app/shell.js";
import { wireMarketViews, wireTableFilters } from "./app/wire-widgets.js";

const strategySection = createStrategySection();

const renderers = {
  overview: renderOverview,
  strategy: strategySection.render,
  universe: renderUniverse,
  collector: renderCollector,
  risk: renderRisk,
  account: renderLiveAccounts,
  reports: renderReports,
  performance: renderPerformance,
  __strategyHooks: strategySection,
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
    account: {
      wire: wireLiveAccounts,
      updateDynamic: updateLiveAccountsDynamic,
    },
    pollState: renderPollState,
  },
});

const shell = createShell({
  onNavigate: (id) => poller.maybeRefresh(id),
});

shell.selectView(shell.initialView(), { updateHistory: false });
wireEcharts(document);
tick();
poller.poll();
setInterval(tick, 1000);
setInterval(poller.poll, POLL_MS);
