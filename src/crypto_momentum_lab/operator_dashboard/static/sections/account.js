import {
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
import { accountWindowDelta, equityChart } from "../dashboard-charts.js";
import {
  blockTitle,
  dataTable,
  disclosure,
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
    if (!Number.isFinite(Number(rank))) return;
    badges.push(
      `<span class="live-signal-rank-badge ${className}">${esc(label)}第${esc(num(rank, 0))}名</span>`,
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
  const permission = (value) => value == null
    ? "未知"
    : value ? "可交易" : "交易所快照：否";
  const permissionDetail = (value) => value == null
    ? "等待 Binance canTrade 快照"
    : value ? "Binance canTrade = true"
      : "仅代表账户快照字段，不代表页面只读";
  const permissionClass = (value) => value == null
    ? "status-UNKNOWN"
    : value ? "status-READY" : "status-ATTENTION";
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
  const permissionState = {
    className: permissionClass(config.can_trade),
    label: permission(config.can_trade),
    detail: permissionDetail(config.can_trade),
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
      <p>execution-account 负责只读同步，不代表账户不可交易；实盘订单由 live-strategy 执行并按客户端订单号回链。</p>
    </div>
    <div class="account-hero-meta">
      <div class="account-hero-status"><small>同步状态</small>${pill(data.status)}</div>
      <span>同步 <b class="num">${esc(dayTime(data.observed_at))}</b></span>
      <small>${esc(relToNow(data.observed_at))}</small>
    </div>
  </div>`;
  const stateGrid = `<div class="account-state-grid" aria-label="实盘账户状态">
    ${stateCard("同步服务", syncState)}
    ${stateCard("交易所权限", permissionState)}
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
    ? `<p class="equity-coverage-note">可用历史始于 <b class="num">${esc(fullDateTime(equityDataStart))} ${DISPLAY_TIME_ZONE_LABEL}</b>；更长区间会随实盘运行逐步积累。</p>`
    : "";
  const equityValue = `<span class="account-equity-value"><small>${esc(selectedEquityRange.shortLabel)} 期末权益</small><strong class="num ${pnlClass(accountEquityDelta)}">${esc(money(latestAccountEquity))}</strong></span>`;
  const equityChartBlock = `<div class="block account-equity-block" data-equity-range="${selectedEquityRange.key}">
    ${blockTitle("实盘账户权益", `LIVE USDT ACCOUNT EQUITY · ROLLING ${selectedEquityRange.shortLabel} · ${accountSample} BUCKETS`, `${equityRangeControls(selectedEquityRange.key)}${equityValue}`)}
    <div class="chart-context"><span>${esc(`${selectedEquityRange.key === "1y" ? fullDateTime(data.equity_window_start) : dayTime(data.equity_window_start)} → ${selectedEquityRange.key === "1y" ? fullDateTime(data.equity_window_end) : dayTime(data.equity_window_end)} ${DISPLAY_TIME_ZONE_LABEL}`)}</span><b class="num">${accountEquity.length} 个采样点</b></div>
    ${equityCoverage}
    ${equityChart(accountEquity, "live-account-equity", data.equity_window_start, data.equity_window_end)}
  </div>`;
  const accountFacts = `<div class="account-facts">
    <div><span>实盘下单通道</span><b class="pos">live-strategy</b></div>
    <div><span>账户 API 交易权限快照</span><b class="${permissionClass(config.can_trade)}" title="Binance 账户快照 canTrade 字段">${esc(permission(config.can_trade))}</b></div>
    <div><span>同步服务模式</span><b class="muted">只读同步 · 不下单</b></div>
    <div><span>持仓模式</span><b>${esc(modeLabel(config.hedge_mode, "Hedge · 双向", "One-way · 单向"))}</b></div>
    <div><span>保证金模式</span><b>${esc(modeLabel(config.multi_assets_mode, "Multi-Assets · 多资产", "Single-Asset · 单资产"))}</b></div>
    <div><span>手续费等级</span><b>${esc(config.fee_tier == null ? "—" : `VIP ${config.fee_tier}`)}</b></div>
    <div><span>对账状态</span><b>${esc(reconciliationLabel(reconciliation.status))}</b></div>
    <div><span>对账差异项</span><b class="${mismatchCount > 0 ? "neg" : "pos"}">${esc(reconciliation.mismatch_count == null ? "—" : `${reconciliation.mismatch_count} 项`)}</b></div>
    <div><span>对账快照 资产 / 持仓</span><b>${esc(`${reconciliation.balance_count ?? "—"} / ${reconciliation.position_count ?? "—"}`)}</b></div>
    <div><span>对账快照 挂单 / 成交</span><b>${esc(`${reconciliation.open_order_count ?? "—"} / ${reconciliation.fill_count ?? "—"}`)}</b></div>
  </div>
  <p class="account-facts-note"><b>怎么读：</b><code>只读同步</code>描述的是 execution-account 服务本身不会下单，不是交易所账户权限。账户 API 交易权限只显示 Binance 快照中的 <code>canTrade</code>；实盘是否提交订单由 <code>live-strategy</code> 的下单开关与风控闸门决定。对账会把余额、持仓、挂单和成交快照写入数据库并检查差异，<code>对账一致 / 0 项</code> 表示本次快照没有发现不一致。</p>`;
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
    ${disclosure("账户权限与对账", "EXECUTION CHANNEL / RECONCILIATION", accountFacts, "", { open: accountNeedsReview, stateKey: "account-reconciliation" })}
    ${disclosure("USDT 资产余额", "USDT BALANCE · ACCOUNT COLLATERAL", balancesTable, `<strong class="num">${usdtBalances.length}</strong>`, { open: usdtBalances.length > 0, stateKey: "account-balances" })}
    ${disclosure("交易所持仓", "EXCHANGE POSITIONS · STRATEGY ATTRIBUTION", positionsTable, `<strong class="num">${positions.length}</strong>`, { open: positions.length > 0, stateKey: "account-positions" })}
    ${disclosure("当前挂单", "OPEN ORDERS · EXCHANGE SOURCE OF TRUTH", ordersTable, `<strong class="num">${openOrders.length}</strong>`, { open: openOrders.length > 0, stateKey: "account-open-orders" })}
    ${disclosure("最近成交订单", "RECENT TRADES · ONE ORDER PER ROW", fillsTable, `<strong class="num">${fills.length}</strong>`, { stateKey: "account-fills" })}`;
  return [data.status, body];
}
