import { DISPLAY_TIME_ZONE_LABEL } from "../dashboard-config.js";
import { dayTime, esc, relAge, relToNow, statusSlug } from "../dashboard-formatters.js";
import { blockTitle, dataTable, pill } from "../dashboard-ui.js";

export function renderRisk(data) {
  const ambiguousOrders = data.ambiguous_orders || [];
  const pendingOrders = data.pending_orders || [];
  const sourceStatus = data.source_status || (data.status === "HALTED" ? "HALTED" : "LIVE");
  const age = data.data_age_seconds;
  const observedAt = data.observed_at;
  const ageLabel = age != null ? relAge(age) : (observedAt ? relToNow(observedAt) : "实时");
  const requiredSymbols = data.required_symbols || [];
  const missingSymbols = data.missing_symbols || [];
  const coverageScope = data.coverage_scope || (requiredSymbols.length ? `${requiredSymbols.length - missingSymbols.length}/${requiredSymbols.length} 覆盖` : "全品种");
  const coverageChip = `<span class="chip-coverage">覆盖范围: ${esc(coverageScope)}</span>`;
  const metaStrip = `<div class="risk-meta-strip"><span class="chip chip-source">${esc(sourceStatus)}</span><span class="chip-age">数据年龄: ${esc(ageLabel)}</span>${coverageChip}</div>`;
  const missingAlert = missingSymbols.length
    ? `<div class="alert-box alert-missing-symbols"><strong>行情缺失</strong><div>必需品种未覆盖 (${missingSymbols.length}): <code>${esc(missingSymbols.join(", "))}</code></div></div>`
    : "";
  const coverageBar = requiredSymbols.length
    ? `<div class="risk-coverage-bar"><span class="coverage-tag">必需品种 (${requiredSymbols.length}):</span><div class="coverage-symbols">${requiredSymbols.map(s => `<span class="symbol-tag${missingSymbols.includes(s) ? " missing" : ""}">${esc(s)}</span>`).join("")}</div></div>`
    : "";
  const halts = data.active_halts?.length
    ? data.active_halts.map((halt) => `<div class="alert-box"><strong>HALT</strong><div>${esc(halt.reason)}<small>${esc(dayTime(halt.created_at))} ${DISPLAY_TIME_ZONE_LABEL}</small></div></div>`).join("")
    : `<div class="ok-box"><i></i>风控畅通 · 0 活跃停机</div>`;
  const orderColumns = [
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "客户端订单号", key: "client_order_id", cls: "num cut" },
    { label: "方向", key: "side", cls: (row) => row.side === "BUY" ? "pos" : "neg" },
    { label: "状态", value: (row) => pill(row.state), html: true },
    { label: "更新时间", value: (row) => dayTime(row.updated_at), align: "right", cls: "muted" },
  ];
  const pendingTable = dataTable(
    orderColumns,
    pendingOrders,
    { emptyText: "无待完成订单" },
  );
  const ambiguousTable = dataTable(
    orderColumns,
    ambiguousOrders,
    { emptyText: "无不确定订单" },
  );
  const decisionsTable = dataTable([
    { label: "候选单", key: "candidate_id", cls: "num cut" },
    { label: "决策", value: (row) => `<span class="decision ${row.decision === "approved" ? "ok" : "no"}">${row.decision === "approved" ? "通过" : "拒绝"}</span>`, html: true },
    { label: "原因", key: "reason", cls: "muted" },
    { label: "时间", value: (row) => dayTime(row.evaluated_at), align: "right", cls: "muted" },
  ], data.latest_risk_decisions, { emptyText: "暂无风控决策流水", tall: true });
  const ambiguousSummary = ambiguousOrders.length
    ? ""
    : `<div class="risk-empty-note"><span>不确定订单</span><strong class="num">0</strong><small>无不确定订单</small></div>`;
  const ambiguousBlock = ambiguousOrders.length
    ? `<div class="block risk-ambiguous">${blockTitle("不确定订单", "AMBIGUOUS / UNRESOLVED", `<strong class="num">${ambiguousOrders.length}</strong>`)}${ambiguousTable}</div>`
    : "";
  const body = `${metaStrip}${missingAlert}${coverageBar}<div class="risk-priority-grid">
      <div class="block risk-halts">${blockTitle("活跃停机", "ACTIVE HALTS")}${halts}</div>
      <div class="risk-decision-callout">
        <span class="callout-tag">处置顺序</span>
        <strong>阻断 → 未决 → 待完成</strong>
        <p>先完成交易所对账，再决定恢复执行或人工处理。</p>
      </div>
    </div>
    ${ambiguousSummary}
    <div class="block-split risk-order-grid${ambiguousOrders.length ? "" : " risk-order-grid-single"}">
      ${ambiguousBlock}
      <div class="block risk-pending">${blockTitle("待完成订单", "RESTING / PARTIALLY FILLED", `<strong class="num">${pendingOrders.length}</strong>`)}${pendingTable}</div>
    </div>
    <div class="block risk-decisions">${blockTitle("风控决策流水", "LATEST 30")}${decisionsTable}</div>`;
  return [data.status, body];
}
