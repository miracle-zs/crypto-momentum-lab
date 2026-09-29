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
  accountEquityBlock: "account-equity-block",
  accountEquityRange: "data-account-equity-range",
  liveAccountMetricsRange: "data-live-account-metrics-range",
  liveAccountMetricsBlock: "live-account-metrics-block",
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
  liveAccountCardKpis: "live-account-card-kpis",
  liveAccountCardStateDetail: "live-account-card-state-detail",
  liveAccountCardFooter: "live-account-card-footer",
  liveAccountFleetStatus: "live-account-fleet-status",
  liveAccountFleetKpis: "live-account-fleet-kpis",
  // strategy
  acctCards: "acct-cards",
  paperAccountDetail: "paper-account-detail",
  accountIndex: "data-account-index",
  loadPaperDetail: "data-load-paper-detail",
  loadPaperHistory: "data-load-paper-history",
  paperComparison: "data-paper-comparison",
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
  accountEquityRange: () => `[${K.accountEquityRange}]`,
  accountEquityRangePressed: () => `[${K.accountEquityRange}][aria-pressed='true']`,
  liveMetricsRange: () => `[${K.liveAccountMetricsRange}]`,
  liveAccountMetrics: () => `[${K.liveAccountMetrics}]`,
  liveAccountDetail: () => `[${K.liveAccountDetail}]`,
  liveAccountDetailContent: () => `[${K.liveAccountDetailContent}]`,
  selectedAccountLabel: () => `[${K.selectedAccountLabel}]`,
  accountLoadState: () => `[${K.accountLoadState}]`,
  refreshState: () => `[${K.refreshState}]`,
  paperDetail: () => `.${K.paperAccountDetail}`,
  acctCards: () => `.${K.acctCards}`,
  paperComparison: () => `[${K.paperComparison}]`,
  accountIndex: () => `[${K.accountIndex}]`,
  loadPaperDetail: () => `[${K.loadPaperDetail}]`,
  loadPaperHistory: () => `[${K.loadPaperHistory}]`,
};
