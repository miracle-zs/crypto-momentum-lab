/**
 * Shared mutable selection for the account section.
 * Keep this a singleton factory so import-graph changes cannot fork state.
 */

import { ACCOUNT_EQUITY_RANGES } from "./constants.js";

export function createAccountSectionState() {
  return {
    selectedLiveAccount: "primary",
    liveAccountDetailRequest: 0,
    liveAccountDetailRanges: new Map(),
    selectedLiveAccountMetricsRange: "24h",
    liveAccountMetricsRequest: 0,
  };
}

export function accountDetailRange(state, accountLabel) {
  return state.liveAccountDetailRanges.get(accountLabel) || "24h";
}

export function rememberAccountDetailRange(state, accountLabel, equityRange) {
  state.liveAccountDetailRanges.set(accountLabel, equityRange);
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

export function selectedEquityRangeKey(state, selectedAccount) {
  return state.liveAccountDetailRanges.get(selectedAccount.account_label)
    || ACCOUNT_EQUITY_RANGES.find((option) => option.key === selectedAccount.equity_range)?.key
    || "24h";
}
