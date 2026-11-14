import { DISPLAY_TIME_ZONE_LABEL } from "../dashboard-config.js";
import {
  asNumber,
  dayTime,
  esc,
  price,
  relToNow,
} from "../dashboard-formatters.js";
import { blockTitle, dataTable, searchBox } from "../dashboard-ui.js";
import { returnBar } from "../dashboard-charts.js";

export function renderUniverse(data) {
  const labels = { target: "目标", extended: "扩展监控", retained: "保留", forced: "持仓保护" };
  const monitored = data.monitored_symbols.toSorted((left, right) => (
    (left.rank ?? Infinity) - (right.rank ?? Infinity)
    || String(left.symbol || "").localeCompare(String(right.symbol || ""))
  ));
  const maxAbs = Math.max(
    ...monitored.map((row) => Math.abs(asNumber(row.utc_day_return) ?? 0)),
    0.0001,
  );
  const table = dataTable([
    { label: "排名", value: (row) => row.rank ?? "—", align: "right", cls: "muted" },
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "现价", value: (row) => price(row.current_price), align: "right" },
    { label: "UTC 日内涨跌", value: (row) => returnBar(row.utc_day_return, maxAbs), html: true },
    {
      label: "监控状态",
      value: (row) => {
        const status = Object.hasOwn(labels, row.status) ? row.status : "unknown";
        return `<span class="membership-status ${status}" data-membership-status="${status}">${esc(labels[status] || "未知")}</span>`;
      },
      html: true,
    },
  ], monitored, { emptyText: "暂无监控币种", tall: monitored.length > 24, rowKey: (row) => row.symbol });
  const counts = Object.entries(labels).map(([status, label]) => {
    const count = monitored.filter((row) => row.status === status).length;
    return `<span><b>${count}</b> ${label}</span>`;
  }).join("");
  const controls = `<div class="market-controls">
    <label>状态 <select data-market-status aria-label="筛选监控状态">
      <option value="all">全部状态</option>
      ${Object.entries(labels).map(([status, label]) => `<option value="${status}">${label}</option>`).join("")}
    </select></label>
    ${searchBox("搜索币种", "[data-market-table]")}
  </div>`;
  const body = `<div class="detail-meta"><span>快照时间 <b class="num">${esc(dayTime(data.observed_at))} ${DISPLAY_TIME_ZONE_LABEL}</b></span><span>${esc(relToNow(data.observed_at))}</span></div>
    <div class="market-board" data-market-table>
      ${blockTitle(`监控池 ${monitored.length}`, "", controls)}
      <div class="monitor-summary">${counts}</div>
      ${table}
    </div>`;
  return [data.status, body];
}
