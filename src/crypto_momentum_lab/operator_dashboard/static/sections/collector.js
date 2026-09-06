import {
  dayTime,
  esc,
  num,
  relAge,
  statusClass,
  normalizedStatus,
} from "../dashboard-formatters.js";
import { blockTitle, dataTable, tile } from "../dashboard-ui.js";

const GIB = 1024 ** 3;
const MIB = 1024 ** 2;

function bytes(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return "—";
  if (parsed >= GIB) return `${(parsed / GIB).toFixed(2)} GiB`;
  if (parsed >= MIB) return `${(parsed / MIB).toFixed(1)} MiB`;
  if (parsed >= 1024) return `${(parsed / 1024).toFixed(1)} KiB`;
  return `${Math.round(parsed)} B`;
}

function percentOf(value, limit) {
  const parsed = Number(value);
  const ceiling = Number(limit);
  if (!Number.isFinite(parsed) || !Number.isFinite(ceiling) || ceiling <= 0) return 0;
  return Math.max(0, Math.min(100, (parsed / ceiling) * 100));
}

function statusTone(status) {
  return ["FRESH", "READY", "ACTIVE"].includes(normalizedStatus(status))
    ? "pos"
    : ["STALE", "DEGRADED"].includes(normalizedStatus(status))
      ? "warn"
      : "neg";
}

function readableTime(value) {
  return value ? dayTime(value) : "—";
}

function capacityMeter(label, value, limit, note, accent = "brand") {
  const width = percentOf(value, limit);
  return `<div class="collector-meter-card">
    <div class="collector-meter-head"><span>${label}</span><b class="num">${bytes(value)} / ${bytes(limit)}</b></div>
    <div class="collector-progress ${accent}"><i style="width:${width.toFixed(1)}%"></i></div>
    <small>${note}</small>
  </div>`;
}

function freeSpaceMeter(data) {
  const free = Number(data.disk_free_bytes);
  const pauseLine = Number(data.disk_pause_free_bytes);
  const warningLine = Number(data.disk_warning_free_bytes);
  const width = Number.isFinite(free) && free >= 0 && pauseLine > 0
    ? Math.min(100, (free / (free + pauseLine)) * 100)
    : 0;
  const accent = free <= pauseLine ? "danger" : free < warningLine ? "warning" : "good";
  return `<div class="collector-meter-card">
    <div class="collector-meter-head"><span>整机剩余空间</span><b class="num">${bytes(free)} 可用</b></div>
    <div class="collector-progress ${accent}"><i style="width:${width.toFixed(1)}%"></i></div>
    <small>告警线 ${bytes(warningLine)} · 暂停线 ${bytes(pauseLine)}</small>
  </div>`;
}

function alertPanel(data) {
  const alerts = Array.isArray(data.alerts) ? data.alerts : [];
  if (!alerts.length) {
    return `<div class="ok-box collector-ok"><i></i><span>checkpoint 新鲜，窗口连续，当前没有容量或积压告警。</span></div>`;
  }
  return `<div class="alert-box collector-alert">
    <strong>REVIEW</strong>
    <div>${alerts.map((alert) => `<small>${esc(alert)}</small>`).join("")}</div>
  </div>`;
}

export function renderCollector(data) {
  const status = normalizedStatus(data?.status || "UNKNOWN") || "UNKNOWN";
  const tone = statusTone(status);
  const lastSequence = data?.last_sequence == null ? "—" : num(data.last_sequence, 0);
  const checkpointAge = data?.checkpoint_age_seconds == null
    ? "未知"
    : relAge(data.checkpoint_age_seconds);
  const latestWrittenAge = data?.parquet_latest_age_seconds == null
    ? "未知"
    : relAge(data.parquet_latest_age_seconds);
  const windows = Array.isArray(data?.recent_windows) ? data.recent_windows : [];
  const recentWindowTable = dataTable([
    {
      label: "窗口起点",
      value: (row) => readableTime(row.window_start),
      align: "right",
      cls: "num",
    },
    {
      label: "写入时间",
      value: (row) => readableTime(row.written_at),
      align: "right",
      cls: "muted",
    },
    {
      label: "文件大小",
      value: (row) => bytes(row.size_bytes),
      align: "right",
      cls: "num",
    },
  ], windows, {
    emptyText: "尚未发现 Parquet 窗口",
    stateKey: "collector-recent-windows",
  });
  const streamId = data?.stream_id ? `${String(data.stream_id).slice(0, 12)}…` : "—";
  const parquetRange = data?.parquet_first_window_start && data?.parquet_latest_window_start
    ? `${readableTime(data.parquet_first_window_start)} → ${readableTime(data.parquet_latest_window_start)}`
    : "—";
  const hero = `<div class="collector-hero ${statusClass(status)}">
    <div class="collector-hero-copy">
      <span class="collector-kicker">RESEARCH CAPTURE · CANONICAL 15S STATE</span>
      <h3>Top${escNumber(data?.top_count)} 数据采集</h3>
      <p>只读观察 research-data：15 秒状态持续写入，15 分钟窗口原子封存。</p>
    </div>
    <div class="collector-state-rail">
      <span class="collector-state-dot"></span>
      <strong>${status}</strong>
      <small>${esc(data?.status_detail || "等待状态")}</small>
    </div>
  </div>`;
  const metrics = `<div class="tile-grid collector-kpi-grid">
    ${tile("采集状态", status, data?.status_detail || "等待数据", tone)}
    ${tile("最近 checkpoint", checkpointAge, readableTime(data?.checkpoint_at), tone)}
    ${tile("最后状态", readableTime(data?.last_bucket_start), data?.last_symbol || "暂无标的", "txt")}
    ${tile("窗口数量", `${num(data?.parquet_file_count, 0)} 个`, data?.parquet_gap_count ? `缺口 ${num(data.parquet_gap_count, 0)} 个` : "连续性未发现缺口", data?.parquet_gap_count ? "warn" : "pos")}
    ${tile("spool 积压", `${num(data?.pending_spool_files, 0)} 个`, bytes(data?.pending_spool_bytes), data?.pending_spool_files ? "warn" : "pos")}
    ${tile("最新文件", latestWrittenAge, `${num(data?.parquet_file_count, 0)} 个 Parquet`, "num")}
  </div>`;
  const storage = `<div class="collector-meter-grid">
    ${capacityMeter("采集卷占用", data?.collector_bytes, data?.collector_hard_limit_bytes, `软上限 ${bytes(data?.collector_soft_limit_bytes)} · 硬上限 ${bytes(data?.collector_hard_limit_bytes)}`, "brand")}
    ${freeSpaceMeter(data || {})}
  </div>`;
  const facts = `<div class="collector-facts">
    <div><small>环境</small><b>${esc(data?.environment || "—")}</b></div>
    <div><small>选择范围</small><b>涨幅 Top${escNumber(data?.top_count)}</b></div>
    <div><small>封存窗口</small><b>${num((Number(data?.parquet_window_seconds) || 0) / 60, 0)} 分钟</b></div>
    <div><small>迟到容忍</small><b>${num(data?.late_tolerance_seconds, 0)} 秒</b></div>
    <div><small>Hub sequence</small><b class="num">${lastSequence}</b></div>
    <div><small>stream</small><b class="num" title="${esc(data?.stream_id || "暂无 stream")}">${esc(streamId)}</b></div>
    <div><small>窗口覆盖</small><b title="${esc(parquetRange)}">${esc(parquetRange)}</b></div>
    <div><small>容量状态</small><b>${esc(data?.capacity_state || "unknown")}</b></div>
  </div>`;
  const body = `${hero}
    ${metrics}
    <div class="block">${alertPanel(data)}</div>
    <div class="block">${blockTitle("容量保护", "CAPACITY GUARD", `<span class="num muted">${bytes(data?.collector_bytes)} / ${bytes(data?.collector_hard_limit_bytes)}</span>`)}${storage}</div>
    <div class="block-split">
      <div class="block">${blockTitle("最近封存窗口", "LATEST PARQUET WINDOWS", `<span class="num muted">${latestWrittenAge}</span>`)}${recentWindowTable}</div>
      <div class="block">${blockTitle("采集契约", "COLLECTOR CONTRACT", "READ ONLY")}${facts}</div>
    </div>`;
  return [status, body];
}

function escNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? String(Math.round(parsed)) : "—";
}
