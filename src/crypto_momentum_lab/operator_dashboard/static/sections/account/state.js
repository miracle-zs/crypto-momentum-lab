export function normalizeSelectedAccount(state, accounts, requestedLabel) {
  if (requestedLabel && accounts.some((account) => account.account_label === requestedLabel)) {
    state.selectedLiveAccount = requestedLabel;
  }
  if (!accounts.some((account) => account.account_label === state.selectedLiveAccount)) {
    state.selectedLiveAccount = accounts[0]?.account_label || "primary";
  }
  return state.selectedLiveAccount;
}
