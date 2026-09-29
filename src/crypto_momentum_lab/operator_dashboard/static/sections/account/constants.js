import {
  COMPARISON_ANCHOR_HOUR,
  DEFAULT_EQUITY_BUCKET_SECONDS,
  DISPLAY_TIME_ZONE_LABEL,
} from "../../dashboard-config.js";
import { asNumber, dayTime, fullDateTime } from "../../dashboard-formatters.js";

export { DEFAULT_EQUITY_BUCKET_SECONDS, DISPLAY_TIME_ZONE_LABEL };

export const ACCOUNT_EQUITY_RANGES = [
  { key: "24h", label: "24小时", shortLabel: "24H" },
  { key: "7d", label: "1周", shortLabel: "7D" },
  { key: "30d", label: "1月", shortLabel: "30D" },
  { key: "1y", label: "1年", shortLabel: "1Y" },
];

export const LIVE_ACCOUNT_METRIC_DEFINITIONS = [
  { key: "equity", title: "资金权益金额变化", subtitle: "USDT · 各账户首个权益点归零" },
  { key: "equity_change_ratio", title: "资金权益比例变化", subtitle: "首个可用权益点 = 0%" },
  { key: "margin_used", title: "保证金占用金额对比", subtitle: "USDT · 交易所初始保证金（含挂单）" },
  { key: "margin_occupancy_ratio", title: "保证金占用比例对比", subtitle: "保证金占用 / 账户权益" },
  { key: "drawdown", title: "回撤金额对比", subtitle: "USDT · 窗口内峰值到当前，负值表示回撤" },
  { key: "drawdown_ratio", title: "回撤比例对比", subtitle: "窗口内峰值到当前，负值表示回撤" },
];

export function equitySampleLabel(seconds) {
  const value = asNumber(seconds) || DEFAULT_EQUITY_BUCKET_SECONDS;
  if (value < 60 * 60) return `${Math.round(value / 60)} MIN`;
  if (value < 24 * 60 * 60) return `${Math.round(value / 3600)} HOUR`;
  return `${Math.round(value / 86400)} DAY`;
}

export function windowRangeText(data, selectedRange) {
  if (!(data?.equity_window_start && data?.equity_window_end)) return "等待时间窗口";
  return `${selectedRange.key === "1y" ? fullDateTime(data.equity_window_start) : dayTime(data.equity_window_start)} → ${selectedRange.key === "1y" ? fullDateTime(data.equity_window_end) : dayTime(data.equity_window_end)} ${DISPLAY_TIME_ZONE_LABEL}`;
}

export function anchorLabel() {
  return `${String(COMPARISON_ANCHOR_HOUR).padStart(2, "0")}:00 ${DISPLAY_TIME_ZONE_LABEL}`;
}

export function equityRangeControls(selectedRange) {
  return `<span class="account-equity-actions">
    <span class="equity-range-switch" role="group" aria-label="实盘账户权益时间范围">
      ${ACCOUNT_EQUITY_RANGES.map((option) => `<button type="button" data-account-equity-range="${option.key}" aria-pressed="${option.key === selectedRange ? "true" : "false"}" title="查看最近${option.label}的账户权益">${option.label}</button>`).join("")}
    </span>
  </span>`;
}

export function liveMetricsRangeControls(selectedRange) {
  return `<span class="account-equity-actions">
    <span class="equity-range-switch" role="group" aria-label="四账户时序时间范围">
      ${ACCOUNT_EQUITY_RANGES.map((option) => `<button type="button" data-live-account-metrics-range="${option.key}" aria-pressed="${option.key === selectedRange ? "true" : "false"}" title="查看最近${option.label}的四账户时序">${option.label}</button>`).join("")}
    </span>
  </span>`;
}
