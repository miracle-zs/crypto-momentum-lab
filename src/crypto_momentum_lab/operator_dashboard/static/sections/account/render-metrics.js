import {
  asNumber,
  esc,
} from "../../dashboard-formatters.js";
import { liveAccountMetricChart } from "../../dashboard-charts.js";
import { blockTitle } from "../../dashboard-ui.js";
import {
  DEFAULT_EQUITY_BUCKET_SECONDS,
  LIVE_ACCOUNT_METRIC_DEFINITIONS,
  anchorLabel,
  equitySampleLabel,
  windowRangeText,
} from "./constants.js";

export function renderLiveAccountMetrics(data) {
  const accounts = Array.isArray(data?.accounts) ? data.accounts : [];
  const interval = asNumber(data?.equity_sample_interval_seconds)
    || DEFAULT_EQUITY_BUCKET_SECONDS;
  const charts = LIVE_ACCOUNT_METRIC_DEFINITIONS.map((definition) => `
    <article class="live-metric-card" data-metric-category="${esc(definition.category || "equity")}">
      <div class="live-metric-card-head">
        <div><span class="section-kicker">LIVE ACCOUNT FLEET</span><h4>${esc(definition.title)}</h4><p>${esc(definition.subtitle)}</p></div>
      </div>
      ${liveAccountMetricChart(
        accounts,
        definition.key,
        `live-account-metric-${definition.key}`,
        definition.title,
        `${definition.title}，实盘账户多维对比`,
        interval,
        data?.equity_window_start,
        data?.equity_window_end,
      )}
    </article>`).join("");
  const windowText = windowRangeText(data);
  const dailyAnchor = anchorLabel();
  const filterTabs = `<div class="view-tabs metric-filter-tabs" role="tablist" aria-label="时序指标分类过滤">
    <button type="button" class="view-tab is-active" data-metric-filter="all" aria-selected="true">全部 (6)</button>
    <button type="button" class="view-tab" data-metric-filter="equity" aria-selected="false">资金权益 (2)</button>
    <button type="button" class="view-tab" data-metric-filter="margin" aria-selected="false">保证金 (2)</button>
    <button type="button" class="view-tab" data-metric-filter="drawdown" aria-selected="false">回撤风险 (2)</button>
  </div>`;
  return `<div class="block live-account-metrics-block">
    ${blockTitle("集群资金与风险时序", `每日 ${dailyAnchor} 起 · ${equitySampleLabel(interval)} 采样`)}
    <div class="live-metrics-context"><span>${esc(windowText)}</span><span>${accounts.length} 个账户 · ${interval >= 86400 ? `${Math.round(interval / 86400)} 天` : `${Math.round(interval / 60)} 分钟`}采样</span><small class="muted" data-refresh-state hidden></small></div>
    <div class="live-metrics-note">每日 ${esc(dailyAnchor)} 起算 · 首点归零 (0 USDT / 0%) · 交易所初始保证金 · 相对窗口峰值回撤</div>
    ${filterTabs}
    <div class="live-metrics-grid">${charts}</div>
  </div>`;
}
