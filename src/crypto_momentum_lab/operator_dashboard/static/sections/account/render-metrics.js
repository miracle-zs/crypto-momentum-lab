import {
  asNumber,
  dayTime,
  esc,
  fullDateTime,
} from "../../dashboard-formatters.js";
import { liveAccountMetricChart } from "../../dashboard-charts.js";
import { blockTitle } from "../../dashboard-ui.js";
import {
  ACCOUNT_EQUITY_RANGES,
  COMPARISON_ANCHOR_HOUR,
  DEFAULT_EQUITY_BUCKET_SECONDS,
  DISPLAY_TIME_ZONE_LABEL,
  LIVE_ACCOUNT_METRIC_DEFINITIONS,
  anchorLabel,
  equitySampleLabel,
  liveMetricsRangeControls,
  windowRangeText,
} from "./constants.js";

function performanceRows(accounts) {
  return accounts
    .filter((acc) => acc.performance)
    .map((acc) => {
      const p = acc.performance;
      const twrPct = p.twr != null ? `${(Number(p.twr) * 100).toFixed(2)}%` : "--";
      const pnl = p.cash_flow_adjusted_pnl != null ? `${Number(p.cash_flow_adjusted_pnl).toFixed(2)} USDT` : "--";
      const dietz = p.modified_dietz != null ? `${(Number(p.modified_dietz) * 100).toFixed(2)}%` : "--";
      const mwr = p.mwr != null ? `${(Number(p.mwr) * 100).toFixed(2)}%` : "--";
      const certBadge = p.is_certified
        ? `<span class="badge badge-success" title="${esc(p.cash_flow_coverage_proof)}">已认证 (${p.cash_flow_corrections_count} 笔资金流)</span>`
        : `<span class="badge badge-warning" title="${esc(p.cash_flow_coverage_proof)}">未认证 (${esc(p.coverage_status)})</span>`;
      return `
        <div class="account-performance-card" style="display:flex; justify-content:space-between; align-items:center; padding:10px 14px; background:var(--card-bg, #1a1e24); border-radius:6px; margin-bottom:8px; border:1px solid var(--border-color, #2d333b); font-size:13px;">
          <div><strong style="color:var(--text-bright, #fff); font-size:14px;">${esc(acc.account_label)}</strong> <span style="margin-left:8px;">${certBadge}</span></div>
          <div style="display:flex; gap:16px;">
            <span>净收益: <strong style="color:var(--text-bright, #fff);">${pnl}</strong></span>
            <span>TWR: <strong style="color:var(--text-bright, #fff);">${twrPct}</strong></span>
            <span>Dietz: <strong>${dietz}</strong></span>
            <span>MWR: <strong>${mwr}</strong></span>
          </div>
        </div>`;
    }).join("");
}

export function renderLiveAccountMetrics(data) {
  const accounts = Array.isArray(data?.accounts) ? data.accounts : [];
  const selectedRange = ACCOUNT_EQUITY_RANGES.find(
    (option) => option.key === data?.equity_range,
  ) || ACCOUNT_EQUITY_RANGES[0];
  const interval = asNumber(data?.equity_sample_interval_seconds)
    || DEFAULT_EQUITY_BUCKET_SECONDS;
  const charts = LIVE_ACCOUNT_METRIC_DEFINITIONS.map((definition) => `
    <article class="live-metric-card">
      <div class="live-metric-card-head">
        <div><span class="section-kicker">LIVE ACCOUNT FLEET</span><h4>${esc(definition.title)}</h4><p>${esc(definition.subtitle)}</p></div>
      </div>
      ${liveAccountMetricChart(
        accounts,
        definition.key,
        `live-account-metric-${definition.key}`,
        definition.title,
        `${definition.title}，四个实盘账户对比`,
        interval,
        data?.equity_window_start,
        data?.equity_window_end,
      )}
    </article>`).join("");
  const windowText = windowRangeText(data, selectedRange);
  const dailyAnchor = anchorLabel();
  const perfRows = performanceRows(accounts);
  const perfSection = perfRows
    ? `<div class="live-metrics-performance-section" style="margin-bottom:16px;">
        <div style="font-size:12px; font-weight:600; text-transform:uppercase; letter-spacing:0.5px; color:var(--text-muted, #8b949e); margin-bottom:8px;">各账户真实绩效与资金流认证 (Audited Performance)</div>
        ${perfRows}
       </div>`
    : "";

  return `<div class="block live-account-metrics-block" data-live-account-metrics-selected="${selectedRange.key}">
    ${blockTitle("四账户资金与风险时序", `DAILY ${dailyAnchor} · ${selectedRange.shortLabel} · ${equitySampleLabel(interval)} BUCKETS`, liveMetricsRangeControls(selectedRange.key))}
    <div class="live-metrics-context"><span>${esc(windowText)}</span><span>${accounts.length} 个账户 · ${interval >= 86400 ? `${Math.round(interval / 86400)} 天` : `${Math.round(interval / 60)} 分钟`}采样</span><small class="muted" data-refresh-state hidden></small></div>
    <div class="live-metrics-note">每日 ${esc(dailyAnchor)} 起算 · 首点归零 (0 USDT / 0%) · 交易所初始保证金 · 相对窗口峰值回撤</div>
    ${perfSection}
    <div class="live-metrics-grid">${charts}</div>
  </div>`;
}

export { COMPARISON_ANCHOR_HOUR, DISPLAY_TIME_ZONE_LABEL, dayTime, fullDateTime };
