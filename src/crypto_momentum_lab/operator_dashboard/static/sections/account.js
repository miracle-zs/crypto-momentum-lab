/**
 * Compatibility facade for the account section.
 * Implementation lives in sections/account/ (render / loaders / state).
 */

export {
  renderAccount,
  renderLiveAccountMetrics,
  renderLiveAccounts,
  renderLiveAccountsSection,
  updateLiveAccountsDynamic,
  wireAccountEquityRanges,
  wireLiveAccounts,
} from "./account/index.js";
