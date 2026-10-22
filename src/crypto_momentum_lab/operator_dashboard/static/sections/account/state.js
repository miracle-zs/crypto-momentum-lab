/**
 * Shared mutable selection for the account section.
 * Keep this a singleton factory so import-graph changes cannot fork state.
 */

export function createAccountSectionState() {
  return {
    selectedLiveAccount: "primary",
    liveAccountDetailRequest: 0,
    liveAccountMetricsRequest: 0,
  };
}

export function normalizeSelectedAccount(state, accounts, requestedLabel) {
  if (requestedLabel && accounts.some((account) => account.account_label === requestedLabel)) {
    state.selectedLiveAccount = requestedLabel;
  }
  if (!accounts.some((account) => account.account_label === state.selectedLiveAccount)) {
    state.selectedLiveAccount = accounts[0]?.account_label || "primary";
  }
  return state.selectedLiveAccount;
}
