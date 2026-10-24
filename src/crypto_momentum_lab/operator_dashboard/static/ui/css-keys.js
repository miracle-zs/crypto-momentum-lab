/**
 * Shared DOM keys for render templates and querySelector wiring.
 * Sections must not invent private class-name selectors; extend this table.
 */

export const K = {
  // structural
  sectionState: "section-state",
  panelBody: "panel-body",
  searchBox: "search-box",
  searchClear: "search-clear",
  filterTable: "data-filter-table",
  tableScroll: "table-scroll",
  echartChart: "data-echart-chart",
  echartSurface: "echart-surface",
  modeValue: "data-mode-value",
  modeDetail: "data-mode-detail",
  navDot: "data-nav-dot",
  marketBoard: "data-market-board",
  marketView: "data-market-view",
  marketPanel: "data-market-panel",
  marketViewActive: "is-active",
  workspaceTab: "data-workspace-tab",
  workspaceNav: "data-workspace-nav",
  skipLink: "skip-link",
  // account
  liveAccountMetrics: "data-live-account-metrics",
  liveAccountLabel: "data-live-account-label",
  liveAccountDetail: "data-live-account-detail",
  liveAccountDetailContent: "data-live-account-detail-content",
  selectedAccountLabel: "data-selected-account-label",
  liveAccountDetailStatus: "live-account-detail-status",
  accountLoadState: "data-account-load-state",
  refreshState: "data-refresh-state",
  liveAccountCardStatus: "live-account-card-status",
  liveAccountCardState: "live-account-card-state",
  liveAccountCardStateDetail: "live-account-card-state-detail",
  liveAccountFleetStatus: "live-account-fleet-status",
  // overview
  serviceAge: "data-service-age",
  serviceMeter: "data-service-meter",
};

/** CSS.escape is not always present in test runtimes. */
function cssEscape(value) {
  return String(value).replace(/["\\]/g, "\\$&");
}

export const sel = {
  sectionState: () => `.${K.sectionState}`,
  panelBody: () => `.${K.panelBody}`,
  tableScroll: () => `.${K.tableScroll}`,
  echartSurface: () => `.${K.echartSurface}`,
  echartCharts: () => `[${K.echartChart}]`,
  accountCard: (label) => `[${K.liveAccountLabel}="${cssEscape(label)}"]`,
  accountCards: () => `[${K.liveAccountLabel}]`,
  liveAccountMetrics: () => `[${K.liveAccountMetrics}]`,
  liveAccountDetail: () => `[${K.liveAccountDetail}]`,
  liveAccountDetailContent: () => `[${K.liveAccountDetailContent}]`,
  selectedAccountLabel: () => `[${K.selectedAccountLabel}]`,
  accountLoadState: () => `[${K.accountLoadState}]`,
  refreshState: () => `[${K.refreshState}]`,
};
