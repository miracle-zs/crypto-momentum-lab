/**
 * Shared account DOM keys for render templates and querySelector wiring.
 */

export const K = {
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
};

/** CSS.escape is not always present in test runtimes. */
function cssEscape(value) {
  return String(value).replace(/["\\]/g, "\\$&");
}

export const sel = {
  accountCard: (label) => `[${K.liveAccountLabel}="${cssEscape(label)}"]`,
  accountCards: () => `[${K.liveAccountLabel}]`,
  liveAccountMetrics: () => `[${K.liveAccountMetrics}]`,
  liveAccountDetail: () => `[${K.liveAccountDetail}]`,
  liveAccountDetailContent: () => `[${K.liveAccountDetailContent}]`,
  selectedAccountLabel: () => `[${K.selectedAccountLabel}]`,
  accountLoadState: () => `[${K.accountLoadState}]`,
  refreshState: () => `[${K.refreshState}]`,
};
