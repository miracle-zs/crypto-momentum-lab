import { DISPLAY_TIME_ZONE_LABEL } from "../dashboard-config.js";
import {
  dayTime,
  esc,
  num,
  relAge,
  statusClass,
  normalizedStatus,
} from "../dashboard-formatters.js";
import { blockTitle, dataTable, pill, tile } from "../dashboard-ui.js";

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

const PHASE_LABELS = {
  "primary": { label: "主策略 (primary)", phase: "0s", phaseClass: "phase-0", targetSec: ":00" },
  "account-2": { label: "账户 2 (account-2)", phase: "15s", phaseClass: "phase-15", targetSec: ":15" },
  "account-3": { label: "账户 3 (account-3)", phase: "30s", phaseClass: "phase-30", targetSec: ":30" },
  "account-4": { label: "账户 4 (account-4)", phase: "45s", phaseClass: "phase-45", targetSec: ":45" },
};

const STAGE_TRANSLATIONS = {
  "candidate_accepted->risk_approved": "01. 风控审批 (Risk Gate)",
  "risk_approved->intent_saved": "02. 意图持久化 (Intent Save)",
  "intent_saved->submitting": "03. 下单调度 (Dispatch)",
  "submitting->exchange_request_started": "04. 网络请求发起 (Request)",
  "exchange_request_started->exchange_response_received": "05. 交易所应答 (Exchange ACK)",
  "exchange_response_received->exchange_filled": "06. 成交回报确认 (Fill Match)",
};

export function renderPerformance(data) {
  const status = normalizedStatus(data?.status || "UNKNOWN") || "UNKNOWN";

  const decisionSlo = data?.decision_slo || {};
  const persistence = data?.persistence || {};
  const marketData = data?.market_data || {};
  const hostResources = data?.host_resources || {};

  // 1. Top KPI Tiles
  const p95Checkpoint = persistence.p95_total_ms != null
    ? `${Number(persistence.p95_total_ms).toFixed(1)} ms`
    : "—";
  const checkpointSub = persistence.sample_count
    ? `${num(persistence.sample_count, 0)} 次样本 · 4账户错峰`
    : "暂无落盘样本";
  const checkpointTone = persistence.p95_total_ms != null && persistence.p95_total_ms < 250
    ? "pos"
    : persistence.p95_total_ms != null && persistence.p95_total_ms < 500
      ? "warn"
      : "neut";

  const marketDelay = marketData.market_delay_ms != null
    ? `${Number(marketData.market_delay_ms).toFixed(0)} ms`
    : "—";
  const marketSub = marketData.realtime_closure_delay_seconds != null
    ? `闭桶水位 ${Number(marketData.realtime_closure_delay_seconds * 1000).toFixed(0)}ms · 完整100%`
    : "行情接收中";
  const marketTone = marketData.market_delay_ms != null && marketData.market_delay_ms < 600
    ? "pos"
    : "warn";

  const memAvail = bytes(hostResources.mem_available_bytes);
  const swapUsed = bytes(hostResources.swap_used_bytes);
  const memSub = hostResources.mem_usage_percent != null
    ? `内存已用 ${hostResources.mem_usage_percent}% · Swap ${swapUsed}`
    : `Swap 已用 ${swapUsed}`;

  // Decision SLO summary
  let maxDecisionP95 = null;
  const phaseLatencies = Object.entries(decisionSlo.phase_latency || {});
  if (phaseLatencies.length) {
    const p95s = phaseLatencies.map(([_, v]) => Number(v.p95_ms) || 0);
    maxDecisionP95 = Math.max(...p95s);
  }
  const decisionP95Text = maxDecisionP95 != null
    ? `${maxDecisionP95.toFixed(1)} ms`
    : (decisionSlo.persisted_event_count ? "达标" : "无新交易");
  const decisionSub = `${num(decisionSlo.persisted_event_count || 0, 0)} 事件 · 窗口 ${decisionSlo.window || "24h"}`;

  const kpis = `<div class="kpi-grid performance-kpi-grid">
    ${tile("决策链路最高 P95", decisionP95Text, decisionSub, maxDecisionP95 != null && maxDecisionP95 < 50 ? "pos" : "neut")}
    ${tile("Checkpoint P95 耗时", p95Checkpoint, checkpointSub, checkpointTone)}
    ${tile("行情端到端时延", marketDelay, marketSub, marketTone)}
    ${tile("宿主机可用内存", memAvail, memSub, "pos")}
  </div>`;

  // 2. Staggered Checkpoints Grid (4 Accounts)
  const accountLatest = persistence.account_latest_checkpoints || {};
  const staggerCards = ["primary", "account-2", "account-3", "account-4"].map((accKey) => {
    const meta = PHASE_LABELS[accKey] || { label: accKey, phase: "0s", phaseClass: "phase-0", targetSec: ":00" };
    const latest = accountLatest[accKey];
    if (!latest) {
      return `<div class="performance-stagger-card">
        <div class="performance-stagger-head">
          <strong>${esc(meta.label)}</strong>
          <span class="performance-phase-pill ${meta.phaseClass}">相位 ${meta.phase} (${meta.targetSec})</span>
        </div>
        <div class="empty" style="padding: 12px 0;"><span>等待周期触发</span><small>分配在 ${meta.targetSec} 秒批次落盘</small></div>
      </div>`;
    }
    const age = (Date.now() - new Date(latest.occurred_at).getTime()) / 1000;
    const totalMs = latest.total_ms != null ? `${Number(latest.total_ms).toFixed(1)} ms` : "—";
    const loopLag = latest.event_loop_lag_ms != null ? `${Number(latest.event_loop_lag_ms).toFixed(1)}ms` : "—";
    const acquireMs = latest.pool_acquire_ms != null ? `${Number(latest.pool_acquire_ms).toFixed(1)}ms` : "—";
    const sqlMs = latest.sql_execute_ms != null ? `${Number(latest.sql_execute_ms).toFixed(1)}ms` : "—";
    const connType = latest.is_new_connection ? "新建物理连接" : "复用连接池";

    return `<div class="performance-stagger-card active-phase">
      <div class="performance-stagger-head">
        <strong>${esc(meta.label)}</strong>
        <span class="performance-phase-pill ${meta.phaseClass}">相位 ${meta.phase} (${meta.targetSec})</span>
      </div>
      <div style="display:flex; justify-content:space-between; align-items:baseline; margin-top:4px;">
        <span class="num" style="font-size:20px; font-weight:700; color:var(--text);">${totalMs}</span>
        <small style="color:var(--muted); font-family:var(--mono);">${relAge(age)} (${dayTime(latest.occurred_at)})</small>
      </div>
      <div style="display:grid; grid-template-columns: repeat(3, 1fr); gap:6px; font-size:11px; margin-top:4px; padding-top:6px; border-top:1px solid var(--line);">
        <div><small style="color:var(--faint); display:block;">事件循环</small><b class="num">${loopLag}</b></div>
        <div><small style="color:var(--faint); display:block;">连接检出</small><b class="num">${acquireMs}</b></div>
        <div><small style="color:var(--faint); display:block;">SQL 执行</small><b class="num">${sqlMs}</b></div>
      </div>
      <small style="color:var(--muted); font-size:10px; margin-top:2px;">连接模式: ${connType}</small>
    </div>`;
  }).join("");

  const staggerSection = `<div class="block">
    ${blockTitle("多账户 Checkpoint 物理时钟相位错峰", "60S PERIOD / 15S PHASE RING", "<span class='sub'>0s · 15s · 30s · 45s</span>")}
    <div class="performance-stagger-grid">${staggerCards}</div>
  </div>`;

  // 3. Decision Path SLO breakdown
  const sloRows = phaseLatencies.map(([transition, item]) => {
    const stageTitle = STAGE_TRANSLATIONS[transition] || transition;
    return {
      transition,
      stageTitle,
      sample_count: item.sample_count,
      p50_ms: item.p50_ms,
      p95_ms: item.p95_ms,
      max_ms: item.max_ms,
    };
  });

  const sloTable = dataTable([
    { label: "决策与执行阶段", key: "stageTitle" },
    { label: "样本数", value: (row) => num(row.sample_count, 0), align: "right", cls: "num" },
    { label: "P50 时延", value: (row) => `${Number(row.p50_ms).toFixed(2)} ms`, align: "right", cls: "num" },
    { label: "P95 时延", value: (row) => `${Number(row.p95_ms).toFixed(2)} ms`, align: "right", cls: "num pos" },
    { label: "Max 峰值", value: (row) => `${Number(row.max_ms).toFixed(2)} ms`, align: "right", cls: "num muted" },
  ], sloRows, { emptyText: "暂无决策时延样本 (窗口内无触发订单或全流程处于冷态)" });

  // 4. Checkpoint Recent History Table
  const recentCheckpoints = persistence.recent_checkpoints || [];
  const ckptTable = dataTable([
    { label: "账户", value: (row) => esc(row.account_label || row.run_id), cls: "sym" },
    { label: "相位", value: (row) => `${row.phase_seconds || 0}s`, align: "right", cls: "num" },
    { label: "落盘时间", value: (row) => dayTime(row.occurred_at), align: "right", cls: "muted" },
    { label: "事件循环", value: (row) => row.event_loop_lag_ms != null ? `${row.event_loop_lag_ms} ms` : "—", align: "right", cls: "num" },
    { label: "连接获取", value: (row) => row.pool_acquire_ms != null ? `${row.pool_acquire_ms} ms` : "—", align: "right", cls: "num" },
    { label: "SQL 耗时", value: (row) => row.sql_execute_ms != null ? `${row.sql_execute_ms} ms` : "—", align: "right", cls: "num" },
    { label: "总耗时", value: (row) => row.total_ms != null ? `${row.total_ms} ms` : "—", align: "right", cls: "num pos" },
    { label: "连接复用", value: (row) => row.is_new_connection ? pill("NEW") : pill("READY"), align: "center", html: true },
  ], recentCheckpoints, { emptyText: "暂无近期落盘记录", tall: true });

  // 5. Host & Database Resource Meters
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
    ${staggerSection}
    <div class="block-split" style="margin-top: 14px;">
      <div class="block">
        ${blockTitle("交易决策链路 SLO 细分", "DECISION & EXECUTION TIMINGS", `<strong class="num">${sloRows.length} 阶段</strong>`)}
        ${sloTable}
      </div>
      <div class="block">
        ${blockTitle("近期 Checkpoint 耗时流水", "PERSISTENCE SAMPLES", `<strong class="num">${recentCheckpoints.length}</strong>`)}
        ${ckptTable}
      </div>
    </div>
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
