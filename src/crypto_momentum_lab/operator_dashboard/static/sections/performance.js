import { DISPLAY_TIME_ZONE_LABEL } from "../dashboard-config.js";
import {
  dayTime,
  esc,
  normalizedStatus,
} from "../dashboard-formatters.js";
import { blockTitle, tile } from "../dashboard-ui.js";

const GIB = 1024 ** 3;
const MIB = 1024 ** 2;

function bytes(value) {
  if (value == null || value === "") return "—";
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

export function renderPerformance(data) {
  const status = normalizedStatus(data?.status || "UNKNOWN") || "UNKNOWN";

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
    : "暂无主机资源采样";

  const kpis = `<div class="kpi-grid performance-kpi-grid">
    ${tile("行情端到端时延", marketDelay, marketSub, marketTone)}
    ${tile("宿主机可用内存", memAvail, memSub, "pos")}
  </div>`;

  // 2. Host & Database Resource Meters
  const loadText = hostResources.cpu_load_1m != null
    ? `${Number(hostResources.cpu_load_1m).toFixed(2)} (1m) · ${hostResources.cpu_load_5m == null ? "—" : Number(hostResources.cpu_load_5m).toFixed(2)} (5m)`
    : "—";
  const cpuTone = hostResources.cpu_load_1m != null && hostResources.cpu_load_1m > 1.8 ? "warning" : "good";
  const cpuMeter = meterCard("CPU 负载 (LoadAvg)", loadText, (hostResources.cpu_load_1m || 0) * 50, "系统 1m / 5m 平均负载指标 (LoadAvg)", cpuTone);

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
  const pgNote = `活跃连接 ${pgActive} · 空闲连接 ${pgIdle} · 数据库体积 ${pgSize}`;
  const pgMeter = meterCard("PostgreSQL 连接与体积", `${pgActive} 活跃 / ${pgIdle} 空闲`, Math.min(100, (Number(pgActive) || 0) * 10), pgNote, "good");

  const body = `
    ${kpis}
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
