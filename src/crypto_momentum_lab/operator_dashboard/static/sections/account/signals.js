import {
  asNumber,
  dayTime,
  esc,
  money,
  num,
  shortHash,
} from "../../dashboard-formatters.js";
import { dataTable, sideTag, signalEvidence } from "../../dashboard-ui.js";

const LIVE_SIGNAL_KIND_LABELS = {
  strategy_signal: { label: "策略信号", className: "signal" },
  candidate: { label: "开仓候选", className: "candidate" },
  reduce_only_candidate: { label: "退出候选", className: "reduce" },
};

function liveSignalKind(row) {
  return LIVE_SIGNAL_KIND_LABELS[row.signal_kind]
    || { label: row.signal_kind || "未知", className: "unknown" };
}

function liveSignalKindCell(row) {
  const kind = liveSignalKind(row);
  return `<span class="live-signal-kind ${kind.className}">${esc(kind.label)}</span>`;
}

function liveSignalFilterSummary(row) {
  const context = row.filter_context || {};
  const parts = [];
  if (typeof context.entry_enabled === "boolean") {
    parts.push(context.entry_enabled ? "入场开启" : "入场关闭");
  }
  if (context.entry_long_only === true) parts.push("仅多头");
  if (context.require_price_above_ema5 === true) parts.push("价格 > EMA5");
  if (context.require_price_above_ema10 === true) parts.push("价格 > EMA10");
  if (context.candidate_execution_path === "reduce_only_exit") {
    parts.push("只减仓退出");
  }
  if (Array.isArray(context.gate_reasons)) {
    parts.push(...context.gate_reasons.filter(Boolean).map((value) => `门控：${value}`));
  }
  const candidateResults = context.candidate_filter_results;
  if (candidateResults && typeof candidateResults === "object") {
    Object.values(candidateResults)
      .filter((result) => result && result.passed === false && result.rejection_reason)
      .forEach((result) => parts.push(`拒绝：${result.rejection_reason}`));
  }
  if (!parts.length) return "—";
  return `<div class="live-signal-filters">${parts
    .map((part) => `<span class="live-signal-filter">${esc(part)}</span>`)
    .join("")}</div>`;
}

function liveSignalRanking(row) {
  const universe = row.filter_context?.universe;
  if (!universe || typeof universe !== "object" || !universe.symbol) {
    return "—";
  }
  const badges = [];
  const rankBadge = (rank, label, className) => {
    const parsedRank = asNumber(rank);
    if (parsedRank == null || !Number.isFinite(parsedRank)) return;
    badges.push(
      `<span class="live-signal-rank-badge ${className}">${esc(label)}第${esc(num(parsedRank, 0))}名</span>`,
    );
  };
  rankBadge(universe.gainer_rank, "涨幅榜", "gainer");
  rankBadge(universe.loser_rank, "跌幅榜", "loser");
  if (!badges.length) {
    return '<span class="live-signal-rank-badge outside">未进涨/跌榜 Top100</span>';
  }
  return `<div class="live-signal-rank">${badges.join("")}</div>`;
}

function liveSignalVolume(row) {
  if (row.quote_volume_24h == null) return "—";
  const asset = row.quote_volume_24h_quote_asset || "USDT";
  return `${money(row.quote_volume_24h)} ${asset}`;
}

function liveSignalRecordLag(row) {
  const detectedAt = Date.parse(row.detected_at || "");
  const recordedAt = Date.parse(row.recorded_at || "");
  if (!Number.isFinite(detectedAt) || !Number.isFinite(recordedAt)) return "—";
  return `${num(Math.max(0, recordedAt - detectedAt) / 1000, 1)}s`;
}

function liveSignalMeta(signals) {
  const newest = signals[0];
  if (!newest) {
    return `<p class="live-signal-note">实盘信号记录尚未到达，或当前账户还没有可展示的信号。</p>`;
  }
  return `<div class="live-signal-note">
    <span>异步观测，不阻塞下单链路</span>
    <span>策略 <b>${esc(newest.strategy_name || "—")}</b> · ${esc(newest.strategy_version || "—")}</span>
    <span>配置 <b class="num" title="${esc(newest.config_hash || "—")}">${esc(shortHash(newest.config_hash))}</b></span>
    <span>代码 <b class="num" title="${esc(newest.code_commit || "—")}">${esc(shortHash(newest.code_commit))}</b></span>
  </div>`;
}

export function renderLiveSignalsContent(liveSignals) {
  const liveSignalsTable = dataTable([
    { label: "触发时间", value: (row) => dayTime(row.detected_at), align: "right", cls: "muted" },
    { label: "类型", value: liveSignalKindCell, html: true },
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "方向", value: (row) => sideTag(row.side), html: true },
    { label: "24H 成交额", value: liveSignalVolume, align: "right" },
    { label: "信号时排名", value: liveSignalRanking, html: true, cls: "live-signal-rank-cell" },
    { label: "触发依据", value: signalEvidence, html: true, cls: "signal-evidence-cell" },
    { label: "过滤 / 门控", value: liveSignalFilterSummary, html: true, cls: "live-signal-filter-cell" },
    { label: "记录延迟", value: liveSignalRecordLag, align: "right", cls: "muted" },
  ], liveSignals, { emptyText: "尚无实盘策略信号", tall: true, stateKey: "live-strategy-signals-table" });

  return `<div class="live-signal-log">${liveSignalMeta(liveSignals)}${liveSignalsTable}</div>`;
}
