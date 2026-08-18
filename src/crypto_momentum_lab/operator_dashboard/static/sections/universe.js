import { DISPLAY_TIME_ZONE_LABEL } from "../dashboard-config.js";
import {
  asNumber,
  dayTime,
  esc,
  price,
  relToNow,
} from "../dashboard-formatters.js";
import { blockTitle, dataTable, emptyBox } from "../dashboard-ui.js";
import { returnBar } from "../dashboard-charts.js";

export function renderUniverse(data) {
  const statusOrder = { target: 0, retained: 1, forced: 2 };
  const sideOrder = { gainer: 0, loser: 1 };
  const monitored = [...(data.monitored_symbols || [])].sort((left, right) => (
    (statusOrder[left.status] ?? 99) - (statusOrder[right.status] ?? 99)
    || (sideOrder[left.side] ?? 99) - (sideOrder[right.side] ?? 99)
    || (left.rank ?? 999) - (right.rank ?? 999)
    || String(left.symbol || "").localeCompare(String(right.symbol || ""))
  ));
  const statusBySymbol = new Map(monitored.map((row) => [row.symbol, row]));
  const membershipLabels = {
    target: "目标",
    retained: "保留",
    forced: "持仓保护",
  };
  const membershipBadge = (status) => {
    const className = Object.prototype.hasOwnProperty.call(membershipLabels, status)
      ? status
      : "unknown";
    return `<span class="membership-status ${className}">${esc(membershipLabels[status] || "未监控")}</span>`;
  };
  const sideLabel = (side) => side === "gainer" ? "涨幅" : side === "loser" ? "跌幅" : "—";
  const maxAbs = Math.max(
    [...(data.gainers || []), ...(data.losers || []), ...monitored]
      .map((row) => Math.abs(asNumber(row.utc_day_return) ?? 0)),
    0.0001,
  );
  const universeTable = (rows) => dataTable([
    { label: "#", key: "rank", align: "right", cls: "muted" },
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "现价", value: (row) => price(row.current_price), align: "right" },
    { label: "UTC 日内涨跌", value: (row) => returnBar(row.utc_day_return, maxAbs), html: true },
    {
      label: "监控状态",
      value: (row) => membershipBadge(statusBySymbol.get(row.symbol)?.status),
      html: true,
    },
  ], rows, { emptyText: "暂无数据" });
  const targetCount = monitored.filter((row) => row.status === "target").length;
  const retainedRows = monitored.filter((row) => row.status === "retained");
  const forcedRows = monitored.filter((row) => row.status === "forced");
  const monitoringAdditions = monitored.filter((row) => row.status !== "target");
  const summary = `<span class="monitor-summary"><b>${targetCount}</b> 目标 · <b>${retainedRows.length}</b> 保留 · <b>${forcedRows.length}</b> 保护 · <b>${monitored.length}</b> 总计</span>`;
  const monitoringTable = dataTable([
    { label: "#", value: (row) => row.rank ?? "—", align: "right", cls: "muted" },
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "方向", value: (row) => sideLabel(row.side) },
    { label: "现价", value: (row) => price(row.current_price), align: "right" },
    { label: "UTC 日内涨跌", value: (row) => returnBar(row.utc_day_return, maxAbs), html: true },
    { label: "监控状态", value: (row) => membershipBadge(row.status), html: true },
  ], monitoringAdditions, { emptyText: "无补充监控成员", tall: monitoringAdditions.length > 24 });
  const monitoringNote = `<div class="monitor-note">榜单中的目标标的已计入监控池；此处只展示未出现在涨幅榜/跌幅榜中的保留和持仓保护成员。</div>`;
  const body = `<div class="detail-meta"><span>快照时间 <b class="num">${esc(dayTime(data.observed_at))} ${DISPLAY_TIME_ZONE_LABEL}</b></span><span>${esc(relToNow(data.observed_at))}</span></div>
    <div class="block-split">
      <div class="block">${blockTitle("涨幅榜 Top 20", "TOP GAINERS · UTC DAY")}${universeTable(data.gainers)}</div>
      <div class="block">${blockTitle("跌幅榜 Top 20", "TOP LOSERS · UTC DAY")}${universeTable(data.losers)}</div>
    </div>
    <div class="block">${blockTitle(`监控池 ${monitored.length}`, "MONITORED UNIVERSE", summary)}${monitoringNote}${blockTitle(`补充监控 ${monitoringAdditions.length}`, "MONITORING ADDITIONS")}${monitoringTable}</div>`;
  return [data.status, body];
}
