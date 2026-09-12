import {
  replaceChildrenFromHtml,
} from "../dashboard-dom.js";
import {
  COMPARISON_ANCHOR_HOUR,
  DEFAULT_EQUITY_BUCKET_SECONDS,
  DISPLAY_TIME_ZONE_LABEL,
} from "../dashboard-config.js";
import {
  asNumber,
  dayTime,
  esc,
  fullDateTime,
  hasUncertainStatus,
  money,
  num,
  pnlClass,
  price,
  relToNow,
  shortHash,
  signedMoney,
  signedPercent,
} from "../dashboard-formatters.js";
import {
  accountWindowDelta,
  equityChart,
  liveAccountMetricChart,
} from "../dashboard-charts.js";
import {
  blockTitle,
  dataTable,
  disclosure,
  emptyBox,
  pill,
  sideTag,
  signalEvidence,
  tile,
} from "../dashboard-ui.js?v=20260826-flight-deck-v2";

const ACCOUNT_EQUITY_RANGES = [
  { key: "24h", label: "24小时", shortLabel: "24H" },
  { key: "7d", label: "1周", shortLabel: "7D" },
  { key: "30d", label: "1月", shortLabel: "30D" },
  { key: "1y", label: "1年", shortLabel: "1Y" },
];

const LIVE_ACCOUNT_METRIC_DEFINITIONS = [
  {
    key: "equity",
    title: "资金权益金额变化",
    subtitle: "USDT · 各账户首个权益点归零",
  },
  {
    key: "equity_change_ratio",
    title: "资金权益比例变化",
    subtitle: "首个可用权益点 = 0%",
  },
  {
    key: "margin_used",
    title: "保证金占用金额对比",
    subtitle: "USDT · 交易所初始保证金（含挂单）",
  },
  {
    key: "margin_occupancy_ratio",
    title: "保证金占用比例对比",
    subtitle: "保证金占用 / 账户权益",
  },
  {
    key: "drawdown",
    title: "回撤金额对比",
    subtitle: "USDT · 窗口内峰值到当前，负值表示回撤",
  },
  {
    key: "drawdown_ratio",
    title: "回撤比例对比",
    subtitle: "窗口内峰值到当前，负值表示回撤",
  },
];

function liveMetricsRangeControls(selectedRange) {
  return `<span class="account-equity-actions">
    <span class="equity-range-switch" role="group" aria-label="四账户时序时间范围">
      ${ACCOUNT_EQUITY_RANGES.map((option) => `<button type="button" data-live-account-metrics-range="${option.key}" aria-pressed="${option.key === selectedRange ? "true" : "false"}" title="查看最近${option.label}的四账户时序">${option.label}</button>`).join("")}
    </span>
  </span>`;
}

export function renderLiveAccountMetrics(data) {
  const accounts = Array.isArray(data?.accounts) ? data.accounts : [];
  const selectedRange = ACCOUNT_EQUITY_RANGES.find(
    (option) => option.key === data?.equity_range,
  ) || ACCOUNT_EQUITY_RANGES[0];
  const interval = asNumber(data?.equity_sample_interval_seconds)
    || DEFAULT_EQUITY_BUCKET_SECONDS;
  const charts = LIVE_ACCOUNT_METRIC_DEFINITIONS.map((definition) => `
    <article class="live-metric-card">
      <div class="live-metric-card-head">
        <div><span class="section-kicker">LIVE ACCOUNT FLEET</span><h4>${esc(definition.title)}</h4><p>${esc(definition.subtitle)}</p></div>
      </div>
      ${liveAccountMetricChart(
        accounts,
        definition.key,
        `live-account-metric-${definition.key}`,
        definition.title,
        `${definition.title}，四个实盘账户对比`,
        interval,
        data?.equity_window_start,
        data?.equity_window_end,
      )}
    </article>`).join("");
  const windowText = data?.equity_window_start && data?.equity_window_end
    ? `${selectedRange.key === "1y" ? fullDateTime(data.equity_window_start) : dayTime(data.equity_window_start)} → ${selectedRange.key === "1y" ? fullDateTime(data.equity_window_end) : dayTime(data.equity_window_end)} ${DISPLAY_TIME_ZONE_LABEL}`
    : "等待时间窗口";
  const anchorLabel = `${String(COMPARISON_ANCHOR_HOUR).padStart(2, "0")}:00 ${DISPLAY_TIME_ZONE_LABEL}`;
  return `<div class="block live-account-metrics-block" data-live-account-metrics-selected="${selectedRange.key}">
    ${blockTitle("四账户资金与风险时序", `DAILY ${anchorLabel} · ${selectedRange.shortLabel} · ${equitySampleLabel(interval)} BUCKETS`, liveMetricsRangeControls(selectedRange.key))}
    <div class="live-metrics-context"><span>${esc(windowText)}</span><span>${accounts.length} 个账户 · ${interval >= 86400 ? `${Math.round(interval / 86400)} 天` : `${Math.round(interval / 60)} 分钟`}采样</span></div>
    <div class="live-metrics-note">每日 ${esc(anchorLabel)} 起算 · 首点归零 (0 USDT / 0%) · 交易所初始保证金 · 相对窗口峰值回撤</div>
    <div class="live-metrics-grid">${charts}</div>
  </div>`;
}

function equitySampleLabel(seconds) {
  const value = asNumber(seconds) || DEFAULT_EQUITY_BUCKET_SECONDS;
  if (value < 60 * 60) return `${Math.round(value / 60)} MIN`;
  if (value < 24 * 60 * 60) return `${Math.round(value / 3600)} HOUR`;
  return `${Math.round(value / 86400)} DAY`;
}

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
    parts.push(
      ...context.gate_reasons.filter(Boolean).map((value) => `门控：${value}`),
    );
  }
  const candidateResults = context.candidate_filter_results;
  if (candidateResults && typeof candidateResults === "object") {
    Object.values(candidateResults)
      .filter(
        (result) => result && result.passed === false && result.rejection_reason,
      )
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

function equityRangeControls(selectedRange) {
  return `<span class="account-equity-actions">
    <span class="equity-range-switch" role="group" aria-label="实盘账户权益时间范围">
      ${ACCOUNT_EQUITY_RANGES.map((option) => `<button type="button" data-account-equity-range="${option.key}" aria-pressed="${option.key === selectedRange ? "true" : "false"}" title="查看最近${option.label}的账户权益">${option.label}</button>`).join("")}
    </span>
  </span>`;
}

export function wireAccountEquityRanges(root, onSelect) {
  root.querySelectorAll("[data-account-equity-range]").forEach((button) => {
    button.addEventListener("click", async () => {
      if (button.getAttribute("aria-pressed") === "true") return;
      const range = button.dataset.accountEquityRange;
      if (!range) return;
      const controls = root.querySelectorAll("[data-account-equity-range]");
      controls.forEach((control) => { control.disabled = true; });
      root.querySelector(".account-equity-block")?.classList.add("is-range-loading");
      try {
        await onSelect(range);
      } finally {
        if (button.isConnected) {
          controls.forEach((control) => { control.disabled = false; });
          root.querySelector(".account-equity-block")?.classList.remove("is-range-loading");
        }
      }
    });
  });
}

export function renderAccount(data) {
  const summary = data.summary || {};
  const config = data.account_config || {};
  const reconciliation = data.reconciliation || {};
  const accountEquity = data.equity_curve || [];
  const selectedEquityRange = ACCOUNT_EQUITY_RANGES.find(
    (option) => option.key === data.equity_range,
  ) || ACCOUNT_EQUITY_RANGES[0];
  const accountSample = equitySampleLabel(data.equity_sample_interval_seconds);
  const accountEquityDelta = accountWindowDelta({ equity_curve: accountEquity });
  const latestAccountEquity = accountEquity.at(-1)?.equity;
  const normalized = (value) => String(value || "").trim().toLowerCase();
  const modeLabel = (value, yesLabel, noLabel) => value == null ? "—" : value ? yesLabel : noLabel;
  const reconciliationLabel = (value) => ({
    ready: "已完成",
    halted: "已中止",
    degraded: "降级",
  }[String(value || "").toLowerCase()] || value || "—");
  const mismatchCount = asNumber(reconciliation.mismatch_count);
  const syncStatus = normalized(data.status);
  const syncState = syncStatus === "ready"
    ? { className: "status-READY", label: "同步正常", detail: "execution-account · 只读同步" }
    : syncStatus === "halted"
      ? { className: "status-HALTED", label: "同步已停止", detail: "execution-account · 需要检查" }
      : { className: "status-UNKNOWN", label: "等待同步", detail: "execution-account · 暂无可靠状态" };
  const configState = {
    className: Object.keys(config).length > 0 ? "status-READY" : "status-UNKNOWN",
    label: Object.keys(config).length > 0 ? "已同步" : "等待同步",
    detail: "Binance V3 账户配置快照",
  };
  const reconciliationState = mismatchCount != null && mismatchCount > 0
    ? { className: "status-ATTENTION", label: `${mismatchCount} 项差异`, detail: "余额、持仓或订单快照需要核对" }
    : normalized(reconciliation.status) === "ready"
      ? { className: "status-READY", label: "对账一致", detail: "快照已完成 · 0 项差异" }
      : { className: "status-UNKNOWN", label: reconciliationLabel(reconciliation.status), detail: "等待本次对账结果" };
  const observedAtMs = new Date(data.observed_at || "").getTime();
  const freshnessSeconds = Number.isFinite(observedAtMs)
    ? Math.max(0, (Date.now() - observedAtMs) / 1000)
    : null;
  const freshnessState = freshnessSeconds == null
    ? { className: "status-UNKNOWN", label: "未知", detail: "没有可用同步时间" }
    : freshnessSeconds <= 120
      ? { className: "status-FRESH", label: "数据新鲜", detail: `${relToNow(data.observed_at)} · 最近一次同步` }
      : { className: "status-STALE", label: "数据过期", detail: `${relToNow(data.observed_at)} · 请检查同步服务` };
  const accountHeroDescription = syncStatus === "ready" && freshnessSeconds != null && freshnessSeconds <= 120
    ? "只读同步正常 · 实盘订单由 live-strategy 执行管控"
    : syncStatus === "ready" && freshnessSeconds != null
      ? "只读同步数据延迟 · 实盘订单由 live-strategy 执行管控，请检查同步服务"
      : syncStatus === "halted"
        ? "只读同步已停止 · 实盘订单由 live-strategy 执行管控，请检查同步服务"
        : "只读同步状态待确认 · 实盘订单由 live-strategy 执行管控";
  const executionState = {
    className: "status-SHADOW",
    label: "live-strategy",
    detail: "实盘下单通道 · 状态见全局实盘状态与风控",
  };
  const stateCard = (label, state) => `<div class="account-state-card ${state.className}">
    <span>${esc(label)}</span>
    <strong>${esc(state.label)}</strong>
    <small>${esc(state.detail)}</small>
  </div>`;
  const strategy = (value) => value
    ? `<span class="account-strategy">${esc(value)}</span>`
    : `<span class="muted">未关联</span>`;
  const hero = `<div class="account-hero">
      <div>
      <div class="account-eyebrow">${esc(String(data.environment || "LIVE").toUpperCase())} · EXECUTION ACCOUNT</div>
      <h3>${esc(data.account_label || "交易所账户")}</h3>
      <p>${esc(accountHeroDescription)}</p>
    </div>
    <div class="account-hero-meta">
      <div class="account-hero-status"><small>同步状态</small>${pill(data.status)}</div>
      <span>同步 <b class="num">${esc(dayTime(data.observed_at))}</b></span>
      <small>${esc(relToNow(data.observed_at))}</small>
    </div>
  </div>`;
  const stateGrid = `<div class="account-state-grid" aria-label="实盘账户状态">
    ${stateCard("同步服务", syncState)}
    ${stateCard("账户配置", configState)}
    ${stateCard("实盘执行", executionState)}
    ${stateCard("对账状态", reconciliationState)}
    ${stateCard("数据新鲜度", freshnessState)}
  </div>`;
  const kpis = `<div class="tile-grid account-kpi-grid">
    ${tile("USDT 钱包余额", money(summary.usdt_wallet_balance), "账户钱包余额", "hero")}
    ${tile("USDT 可用余额", money(summary.usdt_available_balance), "可用于开仓/保证金")}
    ${tile("总未实现盈亏", signedMoney(summary.total_unrealized_pnl), `${summary.position_count || 0} 个交易所持仓`, pnlClass(summary.total_unrealized_pnl))}
    ${tile("持仓名义价值", money(summary.gross_position_notional), "当前交易所总暴露")}
    ${tile("挂单 / 最近成交", `${summary.open_order_count ?? 0} / ${summary.recent_trade_count ?? summary.recent_fill_count ?? 0}`, "当前挂单 / 最近 20 笔订单")}
  </div>`;
  const equityDataStart = accountEquity[0]?.observed_at;
  const requestedStartMs = new Date(data.equity_window_start || "").getTime();
  const dataStartMs = new Date(equityDataStart || "").getTime();
  const bucketMs = (asNumber(data.equity_sample_interval_seconds) || DEFAULT_EQUITY_BUCKET_SECONDS) * 1000;
  const hasPartialHistory = Number.isFinite(requestedStartMs)
    && Number.isFinite(dataStartMs)
    && dataStartMs - requestedStartMs > bucketMs * 2;
  const equityCoverage = hasPartialHistory
    ? `<p class="equity-coverage-note">可用历史始于 <b class="num">${esc(fullDateTime(equityDataStart))} ${DISPLAY_TIME_ZONE_LABEL}</b>（随实盘运行持续沉淀）。</p>`
    : "";
  const equityValue = `<span class="account-equity-value"><small>${esc(selectedEquityRange.shortLabel)} 期末权益</small><strong class="num ${pnlClass(accountEquityDelta)}">${esc(money(latestAccountEquity))}</strong></span>`;
  const equityChartBlock = `<div class="block account-equity-block" data-equity-range="${selectedEquityRange.key}">
    ${blockTitle("实盘账户权益", `ROLLING ${selectedEquityRange.shortLabel} · ${accountSample} BUCKETS`, `${equityRangeControls(selectedEquityRange.key)}${equityValue}`)}
    <div class="chart-context"><span>${esc(`${selectedEquityRange.key === "1y" ? fullDateTime(data.equity_window_start) : dayTime(data.equity_window_start)} → ${selectedEquityRange.key === "1y" ? fullDateTime(data.equity_window_end) : dayTime(data.equity_window_end)} ${DISPLAY_TIME_ZONE_LABEL}`)}</span><b class="num">${accountEquity.length} 个采样点</b></div>
    ${equityCoverage}
    ${equityChart(accountEquity, "live-account-equity", data.equity_window_start, data.equity_window_end)}
  </div>`;
  const accountFacts = `<div class="account-facts">
    <div><span>实盘下单通道</span><b class="pos">live-strategy</b></div>
    <div><span>同步服务模式</span><b class="muted">只读同步 · 不下单</b></div>
    <div><span>持仓模式</span><b>${esc(modeLabel(config.hedge_mode, "Hedge · 双向", "One-way · 单向"))}</b></div>
    <div><span>保证金模式</span><b>${esc(modeLabel(config.multi_assets_mode, "Multi-Assets · 多资产", "Single-Asset · 单资产"))}</b></div>
    <div><span>手续费等级</span><b>${esc(config.fee_tier == null ? "—" : `VIP ${config.fee_tier}`)}</b></div>
    <div><span>对账状态</span><b>${esc(reconciliationLabel(reconciliation.status))}</b></div>
    <div><span>对账差异项</span><b class="${mismatchCount > 0 ? "neg" : "pos"}">${esc(reconciliation.mismatch_count == null ? "—" : `${reconciliation.mismatch_count} 项`)}</b></div>
    <div><span>对账快照 资产 / 持仓</span><b>${esc(`${reconciliation.balance_count ?? "—"} / ${reconciliation.position_count ?? "—"}`)}</b></div>
    <div><span>对账快照 挂单 / 成交</span><b>${esc(`${reconciliation.open_order_count ?? "—"} / ${reconciliation.fill_count ?? "—"}`)}</b></div>
  </div>
  <div class="account-facts-note"><b>怎么读</b>：<code>只读同步</code>不会下单；实盘订单由 <code>live-strategy</code> 与风控闸门共同决定。<code>对账一致 / 0 项</code>表示本次快照未发现差异。</div>`;
  const usdtBalances = (data.balances || []).filter((row) => String(row.asset || "").toUpperCase() === "USDT");
  const balancesTable = dataTable([
    { label: "资产", key: "asset", cls: "sym" },
    { label: "钱包余额", value: (row) => num(row.wallet_balance, 4), align: "right" },
    { label: "可用余额", value: (row) => num(row.available_balance, 4), align: "right" },
    { label: "未实现盈亏", value: (row) => signedMoney(row.unrealized_pnl), align: "right", cls: (row) => pnlClass(row.unrealized_pnl) },
  ], usdtBalances, { emptyText: "尚无 USDT 余额快照", tall: true });
  const positionRows = (data.positions || []).map((row) => ({
    ...row,
    roi: asNumber(row.entry_notional) ? asNumber(row.unrealized_pnl) / asNumber(row.entry_notional) : null,
  }));
  const positionsTable = dataTable([
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "方向", value: (row) => pill(row.position_side || "BOTH"), html: true },
    { label: "策略", value: (row) => strategy(row.strategy_name), html: true },
    { label: "持仓量", value: (row) => num(row.position_amt, 4), align: "right" },
    { label: "开仓 / 标记", value: (row) => `${price(row.entry_price)} / ${price(row.mark_price)}`, align: "right" },
    { label: "杠杆", value: (row) => row.leverage ? `${esc(row.leverage)}x` : "—", align: "right", cls: "muted" },
    { label: "保证金", value: (row) => row.margin_type || "—", cls: "muted" },
    { label: "名义价值", value: (row) => money(row.notional), align: "right" },
    { label: "未实现盈亏", value: (row) => signedMoney(row.unrealized_pnl), align: "right", cls: (row) => pnlClass(row.unrealized_pnl) },
    { label: "ROI", value: (row) => signedPercent(row.roi), align: "right", cls: (row) => pnlClass(row.roi) },
  ], positionRows, { emptyText: "交易所无持仓", tall: true });
  const ordersTable = dataTable([
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "策略", value: (row) => strategy(row.strategy_name), html: true },
    { label: "方向", key: "side", cls: (row) => row.side === "BUY" ? "pos" : "neg" },
    { label: "类型 / 价格", value: (row) => `${row.order_type || "—"} / ${price(row.price)}`, align: "right" },
    { label: "数量", value: (row) => `${num(row.executed_quantity, 4)} / ${num(row.original_quantity, 4)}`, align: "right" },
    { label: "状态", value: (row) => pill(row.status), html: true },
    { label: "只减仓", value: (row) => row.reduce_only ? "是" : "否", cls: "muted" },
    { label: "更新时间", value: (row) => dayTime(row.observed_at), align: "right", cls: "muted" },
  ], data.open_orders, { emptyText: "无挂单", tall: true });
  const fillsTable = dataTable([
    { label: "时间", value: (row) => dayTime(row.trade_at), align: "right", cls: "muted" },
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "订单", value: (row) => shortHash(row.order_id), cls: "num cut" },
    { label: "策略", value: (row) => strategy(row.strategy_name), html: true },
    { label: "方向", key: "side", cls: (row) => row.side === "BUY" ? "pos" : "neg" },
    { label: "均价", value: (row) => price(row.price), align: "right" },
    { label: "数量", value: (row) => num(row.quantity, 4), align: "right" },
    { label: "成交片数", value: (row) => `${row.fill_count || 1} 片`, align: "right", cls: "muted" },
    { label: "已实现盈亏", value: (row) => signedMoney(row.realized_pnl), align: "right", cls: (row) => pnlClass(row.realized_pnl) },
    { label: "手续费", value: (row) => `${num(row.fee, 4)} ${row.fee_asset || ""}`, align: "right" },
    { label: "平仓原因", value: (row) => row.reduce_only ? (row.close_reason || "原因未记录") : "开仓", cls: "muted" },
  ], data.fills, { emptyText: "尚无成交记录", tall: true });
  const liveSignals = data.live_signals || [];
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
  const liveSignalContent = `<div class="live-signal-log">${liveSignalMeta(liveSignals)}${liveSignalsTable}</div>`;
  const accountNeedsReview = mismatchCount > 0
    || hasUncertainStatus(syncStatus)
    || hasUncertainStatus(normalized(reconciliation.status))
    || !data.observed_at;
  const positions = data.positions || [];
  const openOrders = data.open_orders || [];
  const fills = data.fills || [];
  const body = `<div class="detail-meta"><span>同步时间 <b class="num">${esc(dayTime(data.observed_at))} ${DISPLAY_TIME_ZONE_LABEL}</b></span><span>${esc(relToNow(data.observed_at))}</span></div>
    ${hero}${stateGrid}${kpis}${equityChartBlock}
    ${disclosure("实盘策略信号", "LIVE SIGNALS · NON-BLOCKING OBSERVATION · LATEST 30", liveSignalContent,
      `<strong class="num">${liveSignals.length}</strong>`, { open: liveSignals.length > 0, stateKey: "live-strategy-signals" })}
    ${disclosure("账户配置与对账", "EXECUTION CHANNEL / RECONCILIATION", accountFacts, "", { open: accountNeedsReview, stateKey: "account-reconciliation" })}
    ${disclosure("USDT 资产余额", "USDT BALANCE · ACCOUNT COLLATERAL", balancesTable, `<strong class="num">${usdtBalances.length}</strong>`, { open: usdtBalances.length > 0, stateKey: "account-balances" })}
    ${disclosure("交易所持仓", "EXCHANGE POSITIONS · STRATEGY ATTRIBUTION", positionsTable, `<strong class="num">${positions.length}</strong>`, { open: positions.length > 0, stateKey: "account-positions" })}
    ${disclosure("当前挂单", "OPEN ORDERS · EXCHANGE SOURCE OF TRUTH", ordersTable, `<strong class="num">${openOrders.length}</strong>`, { open: openOrders.length > 0, stateKey: "account-open-orders" })}
    ${disclosure("最近成交订单", "RECENT TRADES · ONE ORDER PER ROW", fillsTable, `<strong class="num">${fills.length}</strong>`, { stateKey: "account-fills" })}`;
  return [data.status, body];
}

let selectedLiveAccount = "primary";
let liveAccountDetailRequest = 0;
let selectedLiveAccountMetricsRange = "24h";
let liveAccountMetricsRequest = 0;

async function defaultAccountRequestJson(url) {
  const response = await fetch(url, {
    headers: { "Accept": "application/json" },
  });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
}

function liveAccountStatusLabel(status) {
  const normalized = String(status || "UNKNOWN").toUpperCase();
  return normalized === "READY"
    ? "正常"
    : normalized === "HALTED"
      ? "已停止"
      : "待确认";
}

function liveAccountStatusClass(status) {
  const normalized = String(status || "UNKNOWN").toUpperCase();
  return normalized === "READY"
    ? "status-READY"
    : normalized === "HALTED"
      ? "status-HALTED"
      : "status-UNKNOWN";
}

function liveStrategyStateLabel(state, leaseExpiresAt) {
  const normalized = String(state || "").toLowerCase();
  if (!normalized && leaseExpiresAt) return "租约有效";
  return {
    active: "运行中",
    running: "运行中",
    draining: "排空中",
    halted: "已停止",
  }[normalized] || state || "状态未知";
}

function accountFleetMetric(accounts, key) {
  return accounts.reduce(
    (total, account) => total + (asNumber(account.summary?.[key]) || 0),
    0,
  );
}

function liveAccountCard(account, index, selectedLabel) {
  const summary = account.summary || {};
  const selected = account.account_label === selectedLabel;
  const statusClass = liveAccountStatusClass(account.status);
  const readiness = account.readiness || "待确认";
  const strategy = account.strategy_name || "未关联策略";
  const strategyState = liveStrategyStateLabel(
    account.strategy_state,
    account.lease_expires_at,
  );
  const lease = account.lease_expires_at
    ? `租约至 ${dayTime(account.lease_expires_at)}`
    : "无有效租约";
  const financialSnapshot = Object.keys(summary).length > 0;
  const reconciliation = account.reconciliation || {};
  const mismatchCount = asNumber(reconciliation.mismatch_count);
  const reconciliationLabel = mismatchCount != null && mismatchCount > 0
    ? `${mismatchCount} 项差异`
    : String(reconciliation.status || "").toUpperCase() === "READY"
      ? "对账一致"
      : "对账待确认";
  const secondary = financialSnapshot
    ? `<span class="live-account-card-kpis">
      <span><small>USDT 钱包</small><b class="num">${esc(money(summary.usdt_wallet_balance))}</b></span>
      <span><small>可用余额</small><b class="num">${esc(money(summary.usdt_available_balance))}</b></span>
      <span><small>未实现盈亏</small><b class="num ${pnlClass(summary.total_unrealized_pnl)}">${esc(signedMoney(summary.total_unrealized_pnl))}</b></span>
      <span><small>名义价值</small><b class="num">${esc(money(summary.gross_position_notional))}</b></span>
    </span>`
    : `<span class="live-account-card-state-detail"><span>${esc(strategy)} · ${esc(strategyState)}</span><span>${esc(readiness)} · ${esc(lease)}</span></span>`;
  const footer = financialSnapshot
    ? `${summary.position_count ?? 0} 个持仓 · ${summary.open_order_count ?? 0} 个挂单`
    : "进入账户详情";
  const cardState = financialSnapshot ? reconciliationLabel : readiness;
  return `<button type="button" class="live-account-card${selected ? " is-selected" : ""}" data-live-account-label="${esc(account.account_label || "")}" role="tab" id="live-account-tab-${index}" aria-selected="${selected ? "true" : "false"}" aria-controls="live-account-detail" tabindex="${selected ? "0" : "-1"}">
    <span class="live-account-card-head">
      <span>
        <span class="live-account-card-kicker">LIVE ${String(index + 1).padStart(2, "0")} · ${esc(String(account.environment || "LIVE").toUpperCase())}</span>
        <strong>${esc(account.account_label || "交易所账户")}</strong>
      </span>
      <span class="live-account-card-status ${statusClass}">${esc(liveAccountStatusLabel(account.status))}</span>
    </span>
    <span class="live-account-card-state"><span>同步 <b>${esc(relToNow(account.observed_at))}</b></span><span>${esc(cardState)}</span></span>
    ${secondary}
    <span class="live-account-card-footer">${esc(footer)}<span aria-hidden="true">→</span></span>
  </button>`;
}

function liveAccountSummary(accounts, overallStatus) {
  const readyCount = accounts.filter((account) => String(account.status).toUpperCase() === "READY").length;
  const haltedCount = accounts.filter((account) => String(account.status).toUpperCase() === "HALTED").length;
  const reviewCount = accounts.length - readyCount - haltedCount;
  const financialSnapshots = accounts.some((account) => account.summary);
  const tiles = [
    tile("实盘账户", `${accounts.length} 个`, "execution-account 独立状态"),
    tile("正常账户", `${readyCount} 个`, "可继续观察"),
    tile("停止账户", `${haltedCount} 个`, "需要检查"),
    tile("待确认", `${reviewCount} 个`, "缺少可靠状态"),
  ];
  if (financialSnapshots) {
    tiles.push(
      tile("USDT 钱包合计", money(accountFleetMetric(accounts, "usdt_wallet_balance")), "账户快照合计", "hero"),
      tile("总未实现盈亏", signedMoney(accountFleetMetric(accounts, "total_unrealized_pnl")), "账户群当前浮动盈亏", pnlClass(accountFleetMetric(accounts, "total_unrealized_pnl"))),
    );
  }
  return `<div class="live-account-fleet-summary">
    <div class="live-account-fleet-title">
      <div>
        <span class="section-kicker">LIVE FLEET</span>
        <h3>${esc(accounts.length === 4 ? "实盘账户矩阵 · 四账户实盘总览" : `${accounts.length} 个实盘账户总览`)}</h3>
        <p>共享同一 market-data 接入 · 独立执行与对账快照</p>
      </div>
      <div class="live-account-fleet-status"><small>集群状态</small>${pill(overallStatus)}<span>${readyCount} 正常 · ${haltedCount} 停止 · ${reviewCount} 待确认</span></div>
    </div>
    <div class="tile-grid live-account-fleet-kpis">${tiles.join("")}</div>
  </div>`;
}

export function renderLiveAccounts(data) {
  const sourceAccounts = Array.isArray(data?.accounts) ? data.accounts : [];
  const accounts = sourceAccounts.length
    ? sourceAccounts
    : (data?.account_label || data?.summary ? [data] : []);
  if (!accounts.length) {
    return [data?.status || "UNKNOWN", `<div class="live-account-empty">${emptyBox("等待实盘账户同步", "尚未发现 live execution-account；写入状态后会显示四账户矩阵。")}</div>`];
  }
  const requestedLabel = data?.selected_account_label;
  if (requestedLabel && accounts.some((account) => account.account_label === requestedLabel)) {
    selectedLiveAccount = requestedLabel;
  }
  if (!accounts.some((account) => account.account_label === selectedLiveAccount)) {
    selectedLiveAccount = accounts[0].account_label;
  }
  const selectedAccount = accounts.find(
    (account) => account.account_label === selectedLiveAccount,
  ) || accounts[0];
  const readyCount = accounts.filter((account) => String(account.status).toUpperCase() === "READY").length;
  const haltedCount = accounts.filter((account) => String(account.status).toUpperCase() === "HALTED").length;
  const reviewCount = accounts.length - readyCount - haltedCount;
  const overallStatus = data?.status || (haltedCount ? "HALTED" : reviewCount ? "UNKNOWN" : "READY");
  const hasFullSnapshot = Boolean(selectedAccount.summary || selectedAccount.balances);
  const selectedDetail = hasFullSnapshot
    ? renderAccount(selectedAccount)[1]
    : `<div class="lazy-detail"><strong>账户详情加载中…</strong><small>${esc(selectedAccount.account_label || "交易所账户")} · 正在读取余额、持仓、挂单和权益。</small></div>`;
  const cards = `<div class="live-account-grid" role="tablist" aria-label="实盘账户选择">${accounts.map((account, index) => liveAccountCard(account, index, selectedLiveAccount)).join("")}</div>`;
  const detail = `<div id="live-account-detail" class="live-account-detail" data-live-account-detail role="tabpanel" aria-labelledby="live-account-tab-${accounts.indexOf(selectedAccount)}" data-account-label="${esc(selectedAccount.account_label || "")}">
    <div class="live-account-detail-head">
      <div><span class="section-kicker">SELECTED ACCOUNT</span><h3>${esc(selectedAccount.account_label || "交易所账户")}</h3><small class="muted">切换卡片查看快照与权益曲线</small></div>
      <div class="live-account-detail-status">${pill(selectedAccount.status)}<span>${esc(dayTime(selectedAccount.observed_at))} ${DISPLAY_TIME_ZONE_LABEL}</span></div>
    </div>
    ${selectedDetail}
  </div>`;
  const metrics = `<div data-live-account-metrics aria-live="polite">${emptyBox("加载四账户时序", "正在读取权益、保证金和回撤历史")}</div>`;
  return [overallStatus, `<div class="live-account-fleet" data-live-account-directory>${liveAccountSummary(accounts, overallStatus)}${cards}${metrics}${detail}</div>`];
}

function setLiveAccountTabState(root, accountLabel) {
  root.querySelectorAll("[data-live-account-label]").forEach((button) => {
    const active = button.dataset.liveAccountLabel === accountLabel;
    button.classList.toggle("is-selected", active);
    button.classList.toggle("is-active", active);
    button.setAttribute("aria-selected", String(active));
    button.setAttribute("tabindex", active ? "0" : "-1");
  });
}

async function loadLiveAccountDetail(root, accountLabel, requestJson, equityRange = "24h") {
  const slot = root.querySelector("[data-live-account-detail]");
  if (!slot) return;
  const selectedCard = [...root.querySelectorAll("[data-live-account-label]")]
    .find((button) => button.dataset.liveAccountLabel === accountLabel);
  const accountData = selectedCard ? (root.__liveAccountData || []).find(
    (account) => account.account_label === accountLabel,
  ) : null;
  const requestId = ++liveAccountDetailRequest;
  selectedLiveAccount = accountLabel;
  setLiveAccountTabState(root, accountLabel);
  slot.dataset.accountLabel = accountLabel;
  slot.setAttribute("aria-busy", "true");
  if (accountData?.summary || accountData?.balances) {
    const [status, html] = renderAccount({ ...accountData, equity_range: equityRange });
    replaceChildrenFromHtml(slot, html);
    slot.dataset.accountStatus = status;
    wireAccountEquityRanges(slot, (nextRange) => loadLiveAccountDetail(root, accountLabel, requestJson, nextRange));
    slot.removeAttribute("aria-busy");
    return;
  }
  replaceChildrenFromHtml(
    slot,
    `<div class="lazy-detail"><strong>账户详情加载中…</strong><small>${esc(accountLabel)} · 正在读取最新快照</small></div>`,
  );
  try {
    const query = new URLSearchParams({
      account_label: accountLabel,
      equity_range: equityRange,
    });
    const detail = await requestJson(`api/account?${query.toString()}`);
    if (requestId !== liveAccountDetailRequest || !slot.isConnected) return;
    const [status, html] = renderAccount(detail);
    replaceChildrenFromHtml(slot, html);
    slot.dataset.accountStatus = status;
    wireAccountEquityRanges(slot, (nextRange) => loadLiveAccountDetail(root, accountLabel, requestJson, nextRange));
  } catch (error) {
    if (requestId !== liveAccountDetailRequest || !slot.isConnected) return;
    replaceChildrenFromHtml(slot, emptyBox("账户详情加载失败", `${accountLabel} · ${error.message}`));
  } finally {
    slot.removeAttribute("aria-busy");
  }
}

function wireLiveAccountMetricsRanges(root, onSelect) {
  root.querySelectorAll("[data-live-account-metrics-range]").forEach((button) => {
    if (button.dataset.liveMetricsRangeWired === "true") return;
    button.dataset.liveMetricsRangeWired = "true";
    button.addEventListener("click", async () => {
      if (button.getAttribute("aria-pressed") === "true") return;
      const range = button.dataset.liveAccountMetricsRange;
      if (!range) return;
      const controls = root.querySelectorAll("[data-live-account-metrics-range]");
      controls.forEach((control) => { control.disabled = true; });
      root.querySelector(".live-account-metrics-block")?.classList.add("is-range-loading");
      try {
        selectedLiveAccountMetricsRange = range;
        await onSelect(range);
      } finally {
        if (root.isConnected) {
          root.querySelectorAll("[data-live-account-metrics-range]").forEach((control) => { control.disabled = false; });
          root.querySelector(".live-account-metrics-block")?.classList.remove("is-range-loading");
        }
      }
    });
  });
}

async function loadLiveAccountMetrics(root, requestJson, equityRange) {
  const slot = root.querySelector("[data-live-account-metrics]");
  if (!slot) return;
  const requestId = ++liveAccountMetricsRequest;
  selectedLiveAccountMetricsRange = equityRange;
  slot.setAttribute("aria-busy", "true");
  replaceChildrenFromHtml(
    slot,
    `<div class="live-account-metrics-loading">${emptyBox("加载四账户时序", "正在读取权益、保证金和回撤历史")}</div>`,
  );
  try {
    const query = new URLSearchParams({ equity_range: equityRange });
    const data = await requestJson(`api/live-account-metrics?${query.toString()}`);
    if (requestId !== liveAccountMetricsRequest || !slot.isConnected) return;
    replaceChildrenFromHtml(slot, renderLiveAccountMetrics(data));
    wireLiveAccountMetricsRanges(slot, (nextRange) => loadLiveAccountMetrics(root, requestJson, nextRange));
  } catch (error) {
    if (requestId !== liveAccountMetricsRequest || !slot.isConnected) return;
    replaceChildrenFromHtml(
      slot,
      emptyBox("账户时序加载失败", `${equityRange} · ${error.message}`),
    );
  } finally {
    if (requestId === liveAccountMetricsRequest && slot.isConnected) {
      slot.removeAttribute("aria-busy");
    }
  }
}

export function wireLiveAccounts(root, data, { requestJson = defaultAccountRequestJson } = {}) {
  const accounts = Array.isArray(data?.accounts) ? data.accounts : [];
  root.__liveAccountData = accounts;
  root.querySelectorAll("[data-live-account-label]").forEach((button) => {
    if (button.dataset.liveAccountWired === "true") return;
    button.dataset.liveAccountWired = "true";
    button.addEventListener("click", () => {
      const accountLabel = button.dataset.liveAccountLabel;
      if (accountLabel) void loadLiveAccountDetail(root, accountLabel, requestJson);
    });
    button.addEventListener("keydown", (event) => {
      const buttons = [...root.querySelectorAll("[data-live-account-label]")];
      const current = buttons.indexOf(button);
      if (current < 0) return;
      let next = null;
      if (event.key === "ArrowRight" || event.key === "ArrowDown") next = (current + 1) % buttons.length;
      if (event.key === "ArrowLeft" || event.key === "ArrowUp") next = (current - 1 + buttons.length) % buttons.length;
      if (event.key === "Home") next = 0;
      if (event.key === "End") next = buttons.length - 1;
      if (next == null) return;
      buttons[next].focus();
      buttons[next].click();
      event.preventDefault();
    });
  });
  const selected = accounts.find(
    (account) => account.account_label === selectedLiveAccount,
  ) || accounts[0];
  if (selected && !(selected.summary || selected.balances)) {
    void loadLiveAccountDetail(root, selected.account_label, requestJson);
  } else if (selected) {
    const slot = root.querySelector("[data-live-account-detail]");
    if (slot) wireAccountEquityRanges(slot, (nextRange) => loadLiveAccountDetail(root, selected.account_label, requestJson, nextRange));
  }
  void loadLiveAccountMetrics(root, requestJson, selectedLiveAccountMetricsRange);
}
