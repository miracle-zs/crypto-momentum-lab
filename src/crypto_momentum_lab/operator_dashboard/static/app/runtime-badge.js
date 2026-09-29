import {
  elapsedTime,
  liveHeartbeatAge,
  liveHeartbeatStatus,
  relAge,
  statusClass,
} from "../dashboard-formatters.js";
import { getLiveMode, getLiveService } from "./section-state.js";

let lastRuntimeAnnouncement = "";

export function renderLiveRuntime() {
  const stamp = document.getElementById("global-mode");
  const heartbeat = document.getElementById("last-cycle");
  const heartbeatRow = heartbeat?.closest(".poll-state");
  const modeValue = stamp?.querySelector("[data-mode-value]");
  const modeDetail = stamp?.querySelector("[data-mode-detail]");
  if (!stamp || !heartbeat) return;

  const latestLiveService = getLiveService();
  const mode = getLiveMode() || "UNKNOWN";
  const startedAt = latestLiveService?.details?.started_at;
  let duration = "等待数据";
  if (mode === "LIVE") {
    duration = startedAt
      ? `已运行 ${elapsedTime(startedAt, new Date())}`
      : "运行时间未知";
  } else if (mode === "HALTED") {
    duration = "已停止";
  } else if (mode === "SHADOW") {
    duration = "未启用";
  }
  stamp.className = `mode-badge runtime-line ${statusClass(mode)}`;
  stamp.setAttribute("aria-label", `执行模式：${mode} · ${duration}`);
  if (modeValue) modeValue.textContent = mode;
  else stamp.textContent = `执行模式：${mode} · ${duration}`;
  if (modeDetail) modeDetail.textContent = duration;

  const age = liveHeartbeatAge(latestLiveService);
  const freshness = liveHeartbeatStatus(age);
  heartbeat.textContent = age == null
    ? "UNKNOWN · 等待数据"
    : `${freshness} · ${relAge(age)}`;
  if (heartbeatRow) heartbeatRow.className = `poll-state ${statusClass(freshness)}`;

  const announcement = `执行模式 ${mode}，${freshness === "UNKNOWN" ? "心跳未知" : `心跳${freshness}`}`;
  const announcer = document.getElementById("runtime-announcer");
  if (announcer && announcement !== lastRuntimeAnnouncement) {
    announcer.textContent = announcement;
    lastRuntimeAnnouncement = announcement;
  }
}
