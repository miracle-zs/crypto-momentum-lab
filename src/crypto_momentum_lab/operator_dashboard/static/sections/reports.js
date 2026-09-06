import { dayTime, esc } from "../dashboard-formatters.js";
import { blockTitle, dataTable, pill } from "../dashboard-ui.js";

export function renderReports(data) {
  const timelineEvents = [
    ...(data.shadow_sessions || []).map((row) => ({
      kind: "shadow",
      label: "影子会话",
      id: row.run_id,
      title: row.strategy_name || "未命名策略",
      state: row.state,
      at: row.started_at,
    })),
    ...(data.live_sessions || []).map((row) => ({
      kind: "live",
      label: "实盘迁移",
      id: row.session_id,
      title: "live-strategy",
      state: row.state,
      at: row.occurred_at,
    })),
  ].sort((left, right) => (
    (Date.parse(right.at || "") || 0) - (Date.parse(left.at || "") || 0)
  ));
  const timeline = timelineEvents.length
    ? `<ol class="ledger-timeline" aria-label="最近运行事件">${timelineEvents.map((event) => `<li class="ledger-event ${event.kind}">
        <span class="ledger-event-marker" aria-hidden="true"></span>
        <div class="ledger-event-main"><div><b>${esc(event.label)}</b><span>${esc(event.title)}</span></div><small class="num">${esc(event.id || "—")}</small></div>
        <div class="ledger-event-meta"><span>${esc(dayTime(event.at))}</span>${pill(event.state)}</div>
      </li>`).join("")}</ol>`
    : `<div class="empty ledger-empty"><span>尚无运行事件</span><small>影子会话或实盘状态迁移到达后会出现在这里</small></div>`;
  const shadowTable = dataTable([
    { label: "运行 ID", key: "run_id", cls: "num cut" },
    { label: "策略", key: "strategy_name", cls: "sym" },
    { label: "状态", value: (row) => pill(row.state), html: true },
    { label: "开始时间", value: (row) => dayTime(row.started_at), align: "right", cls: "muted" },
  ], data.shadow_sessions, { emptyText: "尚无影子会话" });
  const liveTable = dataTable([
    { label: "会话 ID", key: "session_id", cls: "num cut" },
    { label: "状态", value: (row) => pill(row.state), html: true },
    { label: "时间", value: (row) => dayTime(row.occurred_at), align: "right", cls: "muted" },
  ], data.live_sessions, { emptyText: "尚无实盘状态迁移" });
  const body = `<div class="ledger-overview">
      ${blockTitle("最近运行事件", "UNIFIED EVENT TIMELINE", `<strong class="num">${timelineEvents.length}</strong>`)}
      ${timeline}
    </div>
    <div class="block-split ledger-tables">
      <div class="block">${blockTitle("影子会话", "SHADOW SESSIONS")}${shadowTable}</div>
      <div class="block">${blockTitle("实盘状态迁移", "LIVE TRANSITIONS")}${liveTable}</div>
    </div>`;
  return [data.status, body];
}
