export const SECTIONS = ["overview", "risk", "account", "strategy", "universe", "reports"];
export const POLL_MS = 5000;
export const SECTION_POLL_MS = Object.freeze({
  overview: POLL_MS,
  risk: POLL_MS,
  account: POLL_MS,
  strategy: 15 * 1000,
  universe: 15 * 1000,
  reports: 30 * 1000,
});
export const DEFAULT_EQUITY_BUCKET_SECONDS = 6 * 60;
export const COMPARISON_ANCHOR_HOUR = 8;
export const PAPER_DETAIL_CACHE_MS = 30 * 1000;
export const PAPER_EQUITY_CACHE_MS = 30 * 1000;

export const STRATEGY_ORDER = [
  "compression_breakout",
  "orderflow_impulse",
  "liquidation_cascade",
];

export const COMPARISON_SERIES_COLORS = [
  "var(--series-fixed)",
  "var(--series-candle)",
  "var(--series-amber)",
  "var(--series-violet)",
  "var(--series-cyan)",
  "var(--series-coral)",
  "var(--series-lime)",
  "var(--series-sky)",
];

export const DISPLAY_TIME_ZONE = "Asia/Shanghai";
export const DISPLAY_TIME_ZONE_LABEL = "UTC+8";
