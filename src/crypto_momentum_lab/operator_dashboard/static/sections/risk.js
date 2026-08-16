import { dayTime, esc } from "../dashboard-formatters.js";
import { blockTitle, dataTable, pill } from "../dashboard-ui.js";

export function renderRisk(data) {
  const halts = data.active_halts?.length
    ? data.active_halts.map((halt) => `<div class="alert-box"><strong>HALT</strong><div>${esc(halt.reason)}<small>${esc(dayTime(halt.created_at))} ${DISPLAY_TIME_ZONE_LABEL}</small></div></div>`).join("")
    : `<div class="ok-box"><i></i>无活跃停机 · 风控闸门畅通</div>`;
  const ambiguousTable = dataTable([
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "客户端订单号", key: "client_order_id", cls: "num cut" },
    { label: "方向", key: "side", cls: (row) => row.side === "BUY" ? "pos" : "neg" },
    { label: "状态", value: (row) => pill(row.state), html: true },
    { label: "更新时间", value: (row) => dayTime(row.updated_at), align: "right", cls: "muted" },
  ], data.ambiguous_orders, { emptyText: "无未决订单" });
  const decisionsTable = dataTable([
    { label: "候选单", key: "candidate_id", cls: "num cut" },
    { label: "决策", value: (row) => `<span class="decision ${row.decision === "approved" ? "ok" : "no"}">${row.decision === "approved" ? "通过" : "拒绝"}</span>`, html: true },
    { label: "原因", key: "reason", cls: "muted" },
    { label: "时间", value: (row) => dayTime(row.evaluated_at), align: "right", cls: "muted" },
  ], data.latest_risk_decisions, { emptyText: "暂无风控决策流水", tall: true });
  const body = `<div class="block">${blockTitle("活跃停机", "ACTIVE HALTS")}${halts}</div>
    <div class="block-split">
      <div class="block">${blockTitle("未决订单", "AMBIGUOUS / UNRESOLVED", `<strong class="num">${(data.ambiguous_orders || []).length}</strong>`)}${ambiguousTable}</div>
      <div class="block">${blockTitle("风控决策", "RISK DECISIONS · LATEST 30")}${decisionsTable}</div>
    </div>`;
  return [data.status, body];
}
