import { DISPLAY_TIME_ZONE_LABEL } from "../dashboard-config.js";
import {
  asNumber,
  dayTime,
  esc,
  price,
  relToNow,
  signedPercent,
} from "../dashboard-formatters.js";
import { blockTitle, dataTable, emptyBox } from "../dashboard-ui.js";
import { returnBar } from "../dashboard-charts.js";

export function renderUniverse(data) {
  const maxAbs = Math.max(
    ...[...(data.gainers || []), ...(data.losers || [])]
      .map((row) => Math.abs(asNumber(row.utc_day_return) ?? 0)),
    0.0001,
  );
  const universeTable = (rows) => dataTable([
    { label: "#", key: "rank", align: "right", cls: "muted" },
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "现价", value: (row) => price(row.current_price), align: "right" },
    { label: "UTC 日内涨跌", value: (row) => returnBar(row.utc_day_return, maxAbs), html: true },
  ], rows, { emptyText: "暂无数据" });
  const monitored = data.monitored_symbols || [];
  const statusBySymbol = new Map(monitored.map((row) => [row.symbol, row]));
  const targetCount = monitored.filter((row) => row.status === "target").length;
  const retainedRows = monitored.filter((row) => row.status === "retained");
  const forcedRows = monitored.filter((row) => row.status === "forced");
  const sideLabel = (side) => side === "gainer" ? "涨幅" : side === "loser" ? "跌幅" : "保护";
  const targetChips = (rows, side) => rows
    .filter((row) => (statusBySymbol.get(row.symbol)?.status || "target") === "target")
    .map((row) => `<span class="chip ${side === "gainer" ? "pos" : "neg"}">
      <span class="chip-main"><b>${esc(row.symbol)}</b><small>#${esc(row.rank)}</small></span>
      <strong class="chip-return">${esc(signedPercent(row.utc_day_return, 1))}</strong>
    </span>`)
    .join("");
  const secondaryChips = (rows) => rows
    .map((row) => `<span class="chip retained ${row.side === "gainer" ? "pos" : "neg"}">
      <span class="chip-main"><b>${esc(row.symbol)}</b><small>${esc(row.status === "forced" ? "持仓保护" : "保留")}</small></span>
      <strong class="chip-side">${esc(sideLabel(row.side))}</strong>
    </span>`)
    .join("");
  const targetGroups = `<div class="monitor-target-grid">
    <div class="monitor-group">
      ${blockTitle("目标池 · 涨幅 Top 20", "TARGET GAINERS")}
      <div class="chip-grid">${targetChips(data.gainers || [], "gainer") || emptyBox("暂无涨幅目标")}</div>
    </div>
    <div class="monitor-group">
      ${blockTitle("目标池 · 跌幅 Top 20", "TARGET LOSERS")}
      <div class="chip-grid">${targetChips(data.losers || [], "loser") || emptyBox("暂无跌幅目标")}</div>
    </div>
  </div>`;
  const secondaryRows = [...retainedRows, ...forcedRows];
  const secondarySection = secondaryRows.length
    ? `<div class="monitor-secondary">
      ${blockTitle("保留与持仓保护", "RETAINED / POSITION PROTECTION", `<span class="num muted">${retainedRows.length} 保留 · ${forcedRows.length} 保护</span>`)}
      <div class="chip-grid">${secondaryChips(secondaryRows)}</div>
    </div>`
    : "";
  const summary = `<span class="monitor-summary"><b>${targetCount}</b> 目标 · <b>${retainedRows.length}</b> 保留 · <b>${forcedRows.length}</b> 保护</span>`;
  const body = `<div class="detail-meta"><span>快照时间 <b class="num">${esc(dayTime(data.observed_at))} ${DISPLAY_TIME_ZONE_LABEL}</b></span><span>${esc(relToNow(data.observed_at))}</span></div>
    <div class="block-split">
      <div class="block">${blockTitle("涨幅榜 Top 20", "TOP GAINERS · UTC DAY")}${universeTable(data.gainers)}</div>
      <div class="block">${blockTitle("跌幅榜 Top 20", "TOP LOSERS · UTC DAY")}${universeTable(data.losers)}</div>
    </div>
    <div class="block">${blockTitle(`监控池 ${monitored.length}`, "MONITORED CANDIDATES", summary)}
      ${targetGroups}${secondarySection}</div>`;
  return [data.status, body];
}
