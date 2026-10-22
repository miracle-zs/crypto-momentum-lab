import { DISPLAY_TIME_ZONE_LABEL } from "../dashboard-config.js";
import {
  dayTime,
  esc,
  statusClass,
  normalizedStatus,
} from "../dashboard-formatters.js";
import { blockTitle, tile } from "../dashboard-ui.js";

const GIB = 1024 ** 3;
const MIB = 1024 ** 2;

function bytes(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) return "—";
  if (parsed >= GIB) return `${(parsed / GIB).toFixed(2)} GiB`;
  if (parsed >= MIB) return `${(parsed / MIB).toFixed(1)} MiB`;
  if (parsed >= 1024) return `${(parsed / 1024).toFixed(1)} KiB`;
  return `${Math.round(parsed)} B`;
}

function meterCard(label, valText, percent, note, tone = "good") {
  const pct = Number.isFinite(percent) ? Math.max(0, Math.min(100, percent)) : 0;
  return `<div class="performance-meter-card">
    <div class="performance-meter-head"><span>${esc(label)}</span><b>${esc(valText)}</b></div>
    <div class="performance-progress ${tone}"><i style="width:${pct.toFixed(1)}%"></i></div>
    <small>${esc(note)}</small>
  </div>`;
}

const DECISION_PIPELINE_STAGES = new Set([
  "market_state_received->context_ready",
  "context_ready->candidate_accepted",
  "candidate_accepted->risk_approved",
  "candidate_accepted->intent_saved",
  "risk_approved->intent_saved",
  "intent_saved->submitting",
  "submitting->exchange_request_started",
  "exchange_request_started->exchange_response_received",
  "signal_recorded->candidate_accepted",
]);

export function renderPerformance(data) {
  const status = normalizedStatus(data?.status || "UNKNOWN") || "UNKNOWN";

  const decisionSlo = data?.decision_slo || {};
  const marketData = data?.market_data || {};
  const hostResources = data?.host_resources || {};

  // 1. Top KPI Tiles
  const marketDelay = marketData.market_delay_ms != null
    ? `${Number(marketData.market_delay_ms).toFixed(0)} ms`
    : "—";
  const marketSub = marketData.latency_source === "strategy_receive"
    ? `收桶至策略接收 · 采样 ${dayTime(marketData.latency_observed_at)}`
    : "暂无当前接收样本";
  const marketTone = marketData.market_delay_ms != null && marketData.market_delay_ms < 1000
    ? "pos"
    : marketData.market_delay_ms != null && marketData.market_delay_ms < 3000
      ? "warn"
      : "neut";

  const memAvail = bytes(hostResources.mem_available_bytes);
  const swapUsed = bytes(hostResources.swap_used_bytes);
  const memSub = hostResources.mem_usage_percent != null
    ? `内存已用 ${hostResources.mem_usage_percent}% · Swap ${swapUsed}`
    : `Swap 已用 ${swapUsed}`;

  // Decision SLO summary (Decision & Order Placement Pipeline)
  let maxDecisionP95 = null;
  const rawPhaseLatencies = decisionSlo.phase_latency && typeof decisionSlo.phase_latency === "object"
    ? decisionSlo.phase_latency
    : {};
  const phaseLatencies = Object.entries(rawPhaseLatencies).filter(
    ([_, v]) => v != null && typeof v === "object",
  );
  if (phaseLatencies.length) {
    const decisionP95s = phaseLatencies
      .filter(([transition]) => DECISION_PIPELINE_STAGES.has(transition))
      .map(([_, v]) => Number(v.p95_ms))
      .filter((n) => Number.isFinite(n));
    if (decisionP95s.length) {
      maxDecisionP95 = Math.max(...decisionP95s);
    }
  }
  const decisionP95Text = maxDecisionP95 != null
    ? `${maxDecisionP95.toFixed(1)} ms`
    : "未接入";
  const decisionSub = maxDecisionP95 != null
    ? `核心决策与报单链路 · 窗口 ${decisionSlo.window || "6h"}`
    : "尚未采集决策链路耗时";
  const decisionTone = maxDecisionP95 != null
    ? (maxDecisionP95 < 500 ? "pos" : maxDecisionP95 < 1500 ? "warn" : "neg")
    : "neut";

  const kpis = `<div class="kpi-grid performance-kpi-grid">
    ${tile("决策链路最高 P95", decisionP95Text, decisionSub, decisionTone)}
    ${tile("行情端到端时延", marketDelay, marketSub, marketTone)}
    ${tile("宿主机可用内存", memAvail, memSub, "pos")}
  </div>`;

  // 2. Host & Database Resource Meters
  const loadText = hostResources.cpu_load_1m != null
    ? `${Number(hostResources.cpu_load_1m).toFixed(2)} (1m) · ${Number(hostResources.cpu_load_5m).toFixed(2)} (5m)`
    : "—";
  const cpuTone = hostResources.cpu_load_1m != null && hostResources.cpu_load_1m > 1.8 ? "warning" : "good";
  const cpuMeter = meterCard("CPU 负载 (LoadAvg)", loadText, (hostResources.cpu_load_1m || 0) * 50, "双核系统负载指标", cpuTone);

  const memText = hostResources.mem_total_bytes
    ? `${bytes(hostResources.mem_used_bytes)} / ${bytes(hostResources.mem_total_bytes)} (${hostResources.mem_usage_percent || 0}%)`
    : "—";
  const memTone = (hostResources.mem_usage_percent || 0) > 85 ? "danger" : (hostResources.mem_usage_percent || 0) > 75 ? "warning" : "good";
  const memMeter = meterCard("物理内存 (RAM)", memText, hostResources.mem_usage_percent || 0, `剩余可用 ${bytes(hostResources.mem_available_bytes)}`, memTone);

  const swapText = hostResources.swap_total_bytes
    ? `${bytes(hostResources.swap_used_bytes)} / ${bytes(hostResources.swap_total_bytes)} (${hostResources.swap_usage_percent || 0}%)`
    : "—";
  const swapTone = (hostResources.swap_usage_percent || 0) > 50 ? "warning" : "good";
  const swapMeter = meterCard("Swap 虚拟内存", swapText, hostResources.swap_usage_percent || 0, "物理内存挤压与换出观测", swapTone);

  const pgActive = hostResources.postgres_active_connections != null ? hostResources.postgres_active_connections : "—";
  const pgIdle = hostResources.postgres_idle_connections != null ? hostResources.postgres_idle_connections : "—";
  const pgSize = bytes(hostResources.postgres_database_size_bytes);
  const pgNote = `活跃 ${pgActive} · 空闲 ${pgIdle} · 库大小 ${pgSize}`;
  const pgMeter = meterCard("PostgreSQL 连接与体积", `${pgActive} 活跃 / ${pgIdle} 空闲`, Math.min(100, (Number(pgActive) || 0) * 10), pgNote, "good");

  const body = `
    ${kpis}
    <p class="muted">决策耗时暂无采样时会显示为未接入。</p>
    <div class="block" style="margin-top: 14px;">
      ${blockTitle("系统基础设施稳态", "HOST & DATABASE RESOURCES")}
      <div class="performance-meter-grid">
        ${cpuMeter}
        ${memMeter}
        ${swapMeter}
        ${pgMeter}
      </div>
    </div>
  `;

  return [status, body];
}
