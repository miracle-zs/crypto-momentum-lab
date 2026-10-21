export const SECTIONS = ["overview", "risk", "account", "universe", "collector", "reports", "performance"];
export const POLL_MS = 15 * 1000;
export const SECTION_POLL_MS = Object.freeze({
  overview: POLL_MS,
  risk: POLL_MS,
  account: POLL_MS,
  performance: POLL_MS,
  universe: 30 * 1000,
  collector: 30 * 1000,
  reports: 30 * 1000,
});
export const DEFAULT_EQUITY_BUCKET_SECONDS = 6 * 60;
export const DAILY_EQUITY_ANCHOR_HOUR = 8;

export const DISPLAY_TIME_ZONE = "Asia/Shanghai";
export const DISPLAY_TIME_ZONE_LABEL = "UTC+8";
