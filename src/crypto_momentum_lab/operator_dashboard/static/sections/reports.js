import { dayTime } from "../dashboard-formatters.js";
import { blockTitle, dataTable, pill } from "../dashboard-ui.js";

export function renderReports(data) {
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
  const body = `<div class="block-split">
      <div class="block">${blockTitle("影子会话", "SHADOW SESSIONS")}${shadowTable}</div>
      <div class="block">${blockTitle("实盘状态迁移", "LIVE TRANSITIONS")}${liveTable}</div>
    </div>`;
  return [data.status, body];
}
