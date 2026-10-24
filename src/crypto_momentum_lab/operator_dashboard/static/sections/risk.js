import { DISPLAY_TIME_ZONE_LABEL } from "../dashboard-config.js";
import { dayTime, esc } from "../dashboard-formatters.js";
import { blockTitle, dataTable, pill } from "../dashboard-ui.js";

export function renderRisk(data) {
  const activeHalts = data.active_halts || [];
  const ambiguousOrders = data.ambiguous_orders || [];
  const pendingOrders = data.pending_orders || [];
  const metaStrip = `<div class="risk-meta-strip" data-state-key="risk-meta-strip"><span>四账户订单汇总</span><span class="chip-age">更新于 ${esc(dayTime(data.observed_at))} ${DISPLAY_TIME_ZONE_LABEL}</span></div>`;
  const halts = activeHalts.length
    ? activeHalts.map((halt) => `<div class="alert-box" data-state-key="halt-${esc(halt.account_label)}-${esc(halt.reason)}"><strong>${esc(halt.account_label)}</strong><div>${esc(halt.reason)}<small>${esc(dayTime(halt.created_at))} ${DISPLAY_TIME_ZONE_LABEL}</small></div></div>`).join("")
    : `<div class="ok-box" data-state-key="risk-halts-ok"><i></i>无活跃停机</div>`;
  const orderColumns = [
    { label: "账户", key: "account_label" },
    { label: "币种", key: "symbol", cls: "sym notranslate" },
    { label: "客户端订单号", key: "client_order_id", cls: "num cut notranslate" },
    { label: "方向", key: "side", cls: (row) => row.side === "BUY" ? "pos" : "neg" },
    { label: "状态", value: (row) => pill(row.status), html: true },
    { label: "更新时间", value: (row) => dayTime(row.observed_at), align: "right", cls: "muted" },
  ];
  const rowKey = (row) => `${row.account_label}:${row.client_order_id}`;
  const pendingTable = dataTable(orderColumns, pendingOrders, {
    emptyText: "无待完成订单",
    stateKey: "risk-pending-orders",
    rowKey,
  });
  const ambiguousBlock = ambiguousOrders.length
    ? `<div class="block risk-ambiguous" data-state-key="risk-ambiguous-block">
        ${blockTitle("未决订单", "需核实交易所订单状态", `<strong class="num">${ambiguousOrders.length}</strong>`)}
        ${dataTable(orderColumns, ambiguousOrders, { stateKey: "risk-ambiguous-orders", rowKey })}
      </div>`
    : `<div class="risk-empty-note" data-state-key="risk-ambiguous-note"><span>未决订单</span><strong class="num">0</strong><small>无状态待确认的订单</small></div>`;
  const body = `${metaStrip}
    <div class="block risk-halts" data-state-key="risk-halts-block">${blockTitle("活跃停机", "", `<strong class="num">${activeHalts.length}</strong>`)}${halts}</div>
    ${ambiguousBlock}
    <div class="block risk-pending" data-state-key="risk-pending-block">
      ${blockTitle("待完成订单", "已提交、挂单或部分成交", `<strong class="num">${pendingOrders.length}</strong>`)}
      ${pendingTable}
    </div>`;
  return [activeHalts.length ? "HALTED" : data.status, body];
}
