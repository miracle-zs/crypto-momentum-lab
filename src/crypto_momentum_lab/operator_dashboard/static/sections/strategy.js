import {
  DEFAULT_EQUITY_BUCKET_SECONDS,
  DISPLAY_TIME_ZONE_LABEL,
  PAPER_DETAIL_CACHE_MS,
  PAPER_EQUITY_CACHE_MS,
  STRATEGY_ORDER,
} from "../dashboard-config.js";
import {
  asNumber,
  dayTime,
  elapsedTime,
  esc,
  money,
  num,
  percent,
  pnlClass,
  price,
  relToNow,
  shortHash,
  signedMoney,
  signedPercent,
} from "../dashboard-formatters.js";
import {
  captureViewState,
  replaceElementFromHtml,
  restoreViewState,
} from "../dashboard-dom.js";
import {
  accountWindowDelta,
  buildStrategyEquityModels,
  comparisonSeriesStyle,
  equityChart,
  standaloneSparkline,
  strategyEquityChart,
} from "../dashboard-charts.js";
import {
  blockTitle,
  dataTable,
  emptyBox,
  pill,
  sideTag,
  signalEvidence,
  tile,
} from "../dashboard-ui.js";

async function defaultRequestJson(url) {
  const response = await fetch(url, {
    headers: { "Accept": "application/json" },
  });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
}

export function createStrategySection({ requestJson = defaultRequestJson } = {}) {
  let selectedPaperAccount = 0;
  const paperHistoryByRun = new Map();
  const paperDetailsByRun = new Map();
  const paperDetailLoadedAt = new Map();
  const paperDetailRequests = new Map();
  const paperEquityByRun = new Map();
  let paperEquityRequest = null;
  let paperEquityLoadedAt = 0;
  let paperEquityCacheKey = "";
  let latestPaperAccounts = [];

  function visiblePaperAccounts(data) {
    return (data.accounts || []).filter((account) => account.exit_mode !== "fixed");
  }

function pairedComparisonPanel(model) {
  const bucketMinutes = Math.round(model.intervalSeconds / 60);
  const legend = model.series.map((series) =>
    `<span class="${series.colorClass}" ${comparisonSeriesStyle(series)}><i></i><em>${esc(series.label)}</em> <b class="num ${pnlClass(series.delta)}">${esc(signedMoney(series.delta))}</b></span>`).join("");
  return `<article class="pair-panel">
    <div class="pair-head">
      <div><strong>${esc(model.strategyName)}</strong>
        <small>${esc(dayTime(model.startAt))} → ${esc(dayTime(model.endAt))} ${DISPLAY_TIME_ZONE_LABEL} · ${model.points.length} 个共同桶</small></div>
      <div class="pair-spread"><span>退出版本</span><b class="num">${model.series.length} 个</b></div>
    </div>
    <div class="pair-legend">${legend}</div>
    ${strategyEquityChart(model)}
    <footer><span>共同起点归零 · ${bucketMinutes} 分钟 UTC 采样</span><b>${esc(elapsedTime(model.startAt, model.endAt))}</b></footer>
  </article>`;
}

const paperAccountTabId = (index) => `paper-account-tab-${index}`;
const paperAccountPanelId = () => "paper-account-panel";

function accountCard(account, index) {
  const summary = account.portfolio_summary || {};
  const equity = asNumber(summary.equity);
  const returnSinceStart = equity == null ? null : equity / 1000 - 1;
  const isCandle = account.exit_mode === "candle_15m";
  const hasEquity = Array.isArray(account.equity_curve);
  const windowDelta = hasEquity ? accountWindowDelta(account) : null;
  const active = index === selectedPaperAccount;
  const exitMode = account.exit_label || (isCandle ? "15M 收线退出" : "退出方式未标注");
  const accountNumber = String(index + 1).padStart(2, "0");
  const sparkline = hasEquity
    ? standaloneSparkline(account.equity_curve)
    : '<div class="spark spark-empty">详情按需加载</div>';
  return `<button class="acct-card${active ? " is-active" : ""}" type="button" role="tab"
    id="${paperAccountTabId(index)}" aria-controls="${paperAccountPanelId()}"
    aria-selected="${active}" tabindex="${active ? "0" : "-1"}" data-account-index="${index}">
    <div class="acct-top"><span>账户 ${accountNumber}</span><span class="acct-mode">${esc(exitMode)}</span>${pill(account.status)}</div>
    <b class="acct-name">${esc(account.strategy_name || "未启动")}</b>
    <div class="acct-equity"><strong class="num">${esc(money(summary.equity))}</strong>
      <span class="acct-total"><small>累计收益</small><em class="num ${pnlClass(returnSinceStart)}">${esc(signedPercent(returnSinceStart))}</em></span></div>
    <div class="acct-window"><span>滚动 24H 权益变化</span>
      <b class="num ${pnlClass(windowDelta)}">${esc(hasEquity ? signedMoney(windowDelta) : "详情加载后显示")}</b></div>
    ${sparkline}
    <small>${summary.open_position_count || 0} 持仓 · ${summary.closed_trade_count || 0} 已平仓 · 胜率 ${esc(percent(summary.win_rate, 0))}</small>
  </button>`;
}

function strategyAccountColumn(accounts, strategyName, index) {
  const entries = accounts
    .map((account, accountIndex) => ({ account, accountIndex }))
    .filter(({ account }) => account.strategy_name === strategyName);
  if (!entries.length) return "";
  return `<section class="acct-strategy-column" aria-label="${esc(strategyName)} 模拟账户">
    <header class="acct-strategy-head">
      <div><span>策略 0${index + 1}</span><strong>${esc(strategyName)}</strong></div>
      <small>${entries.length} 个账户</small>
    </header>
    <div class="acct-strategy-cards" role="group" aria-label="${esc(strategyName)}退出版本">${entries
      .map(({ account, accountIndex }) => accountCard(account, accountIndex))
      .join("")}</div>
  </section>`;
}

function withPaperEquity(account) {
  if (!account?.run_id) return account;
  const equity = paperEquityByRun.get(account.run_id);
  return equity ? { ...account, ...equity } : account;
}

function paperCards(accounts) {
  return `<div class="acct-cards" role="tablist" aria-orientation="horizontal" aria-label="模拟盘策略账户">${STRATEGY_ORDER
    .map((strategyName, index) => strategyAccountColumn(accounts, strategyName, index))
    .join("")}</div>`;
}

function paperComparisonBlock(accounts) {
  const paperAccounts = accounts.map(withPaperEquity);
  const liveAccounts = [...paperEquityByRun.values()].filter(
    (account) => account.source === "live",
  );
  const comparisonModels = buildStrategyEquityModels([
    ...paperAccounts,
    ...liveAccounts,
  ]);
  const content = comparisonModels.length
    ? `<div class="pair-grid">${comparisonModels.map(pairedComparisonPanel).join("")}</div>`
    : emptyBox("同期权益曲线加载中", "账户摘要已就绪，曲线在后台批量加载");
  return `<div class="block pair-section" data-paper-comparison>
    ${blockTitle("同期退出方式对比", "STRATEGY EXIT EQUITY · COMMON START · SHARED AXES",
      '<span class="muted">模拟盘版本 + 实盘 B1 · 同期叠加</span>')}
    ${content}
  </div>`;
}

function paperDetailPlaceholder(account, index, message = "账户详情按需加载") {
  return `<div class="paper-account-detail" id="${paperAccountPanelId()}" data-run-id="${esc(account?.run_id || "")}" role="tabpanel" aria-labelledby="${paperAccountTabId(index)}">
    <div class="lazy-detail">
      <strong>${esc(message)}</strong>
      <small>首屏只查询账户摘要；曲线、持仓、交易和信号将在选中后加载。</small>
      <button class="history-button" type="button" data-load-paper-detail>加载账户详情</button>
    </div>
  </div>`;
}

function accountDetail(account, index) {
  const summary = account.portfolio_summary || {};
  const positions = (account.open_positions || []).map((row) => ({
    ...row,
    upnl_pct: asNumber(row.unrealized_pnl) != null && asNumber(row.entry_notional)
      ? asNumber(row.unrealized_pnl) / asNumber(row.entry_notional) : null,
  }));
  const equityDelta = asNumber(summary.unrealized_pnl);
  const sampleMinutes = Math.round(
    (asNumber(account.equity_sample_interval_seconds) || DEFAULT_EQUITY_BUCKET_SECONDS) / 60,
  );
  const chartWindow = `${dayTime(account.equity_window_start)} → ${dayTime(account.equity_window_end)} ${DISPLAY_TIME_ZONE_LABEL}`;
  const configHash = account.config_hash || "—";
  const historyLoaded = account.history_loaded === true;
  const historyAction = `<span class="history-actions">
    <span class="num muted">${historyLoaded ? "全部" : "最近"} ${(account.closed_trades || []).length} / 共 ${summary.closed_trade_count || 0}</span>
    <button class="history-button" type="button" data-load-paper-history>${historyLoaded ? "刷新全部历史" : "查看全部历史"}</button>
  </span>`;
  const detailMeta = `<div class="detail-meta">
    <span>RUN <b class="num">${esc(account.run_id || "—")}</b></span>
    <span>CONFIG <b class="num" title="${esc(configHash)}" aria-label="完整配置哈希 ${esc(configHash)}">${esc(shortHash(configHash))}</b></span>
    <span>检查点 <b class="num">${esc(relToNow(account.checkpoint_at))}</b></span>
  </div>`;
  const kpis = `<div class="tile-grid kpi-grid">
    ${tile("账户权益", money(summary.equity), "余额 + 未实现盈亏", "hero")}
    ${tile("可用余额", money(summary.balance), "初始资金 $1,000.00")}
    ${tile("已实现盈亏", signedMoney(summary.realized_pnl), `${summary.closed_trade_count || 0} 笔已平仓`, pnlClass(summary.realized_pnl))}
    ${tile("未实现盈亏", signedMoney(summary.unrealized_pnl), `${summary.open_position_count || 0} 个持仓`, pnlClass(summary.unrealized_pnl))}
    ${tile("胜率", percent(summary.win_rate), `累计手续费 ${money(summary.total_fees)}`)}
  </div>`;
  const chartBlock = `<div class="block">
    ${blockTitle("资产权益走势", `ROLLING 24H · ${sampleMinutes} MIN UTC BUCKETS`,
      `<strong class="num ${pnlClass(equityDelta)}">${esc(money(summary.equity))}</strong>`)}
    <div class="chart-context"><span>${esc(chartWindow)}</span><b class="num">${(account.equity_curve || []).length} / 240 桶</b></div>
    ${equityChart(
      account.equity_curve,
      `eq-fill-${index}`,
      account.equity_window_start,
      account.equity_window_end,
    )}
  </div>`;
  const positionsTable = dataTable([
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "方向", value: (row) => sideTag(row.side), html: true },
    { label: "开仓价", value: (row) => price(row.entry_price), align: "right" },
    { label: "标记价", value: (row) => price(row.last_mark_price), align: "right" },
    { label: "名义价值", value: (row) => money(row.entry_notional), align: "right" },
    { label: "浮动盈亏", value: (row) => signedMoney(row.unrealized_pnl), align: "right", cls: (row) => pnlClass(row.unrealized_pnl) },
    { label: "收益率", value: (row) => signedPercent(row.upnl_pct), align: "right", cls: (row) => pnlClass(row.upnl_pct) },
  ], positions, { emptyText: "当前无持仓", stateKey: "paper-open-positions" });
  const closedTable = dataTable([
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "方向", value: (row) => sideTag(row.side), html: true },
    { label: "开仓价", value: (row) => price(row.entry_price), align: "right" },
    { label: "平仓价", value: (row) => price(row.exit_price), align: "right" },
    { label: "净盈亏", value: (row) => signedMoney(row.realized_pnl), align: "right", cls: (row) => pnlClass(row.realized_pnl) },
    { label: "收益率", value: (row) => signedPercent(row.return_pct), align: "right", cls: (row) => pnlClass(row.return_pct) },
    { label: "平仓原因", key: "close_reason", cls: "muted" },
  ], account.closed_trades, { emptyText: "尚无已平仓交易", stateKey: "paper-closed-trades" });
  const eventsTable = dataTable([
    { label: "时间", value: (row) => dayTime(row.occurred_at), align: "right", cls: "muted" },
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "动作", value: (row) => `<span class="event-tag ${String(row.event || "").startsWith("OPEN") ? "open" : "close"}">${esc(row.label)}</span>`, html: true },
    { label: "订单方向", key: "order_action", cls: (row) => row.order_action === "BUY" ? "pos" : "neg" },
    { label: "价格", value: (row) => price(row.price), align: "right" },
    { label: "数量", value: (row) => num(row.quantity, 3), align: "right" },
    { label: "盈亏", value: (row) => row.pnl == null ? "—" : signedMoney(row.pnl), align: "right", cls: (row) => pnlClass(row.pnl) },
    { label: "原因", key: "reason", cls: "muted" },
  ], account.trade_events, { emptyText: "尚无开平仓流水", tall: true, stateKey: "paper-trade-events" });
  const signalsTable = dataTable([
    { label: "时间", value: (row) => dayTime(row.detected_at), align: "right", cls: "muted" },
    { label: "币种", key: "symbol", cls: "sym" },
    { label: "方向", value: (row) => sideTag(row.side), html: true },
    { label: "原因", key: "reason", cls: "muted" },
    { label: "买入名义", value: (row) => money(row.requested_notional), align: "right" },
    { label: "触发依据", value: signalEvidence, html: true, cls: "signal-evidence-cell" },
  ], account.latest_signals, { emptyText: "尚无策略信号", stateKey: "paper-strategy-signals" });
  return `<div class="paper-account-detail" id="${paperAccountPanelId()}" data-run-id="${esc(account.run_id || "")}" role="tabpanel" aria-labelledby="${paperAccountTabId(index)}">
    ${detailMeta}
    ${kpis}
    ${chartBlock}
    <div class="block-split">
      <div class="block">${blockTitle("当前持仓", "OPEN POSITIONS", `<strong class="num">${positions.length}</strong>`)}${positionsTable}</div>
      <div class="block">${blockTitle("已平仓交易", historyLoaded ? "CLOSED TRADES · FULL HISTORY" : "CLOSED TRADES · LATEST 30", historyAction)}${closedTable}</div>
    </div>
    <div class="block">${blockTitle("开平仓流水", "POSITION LIFECYCLE · BUY / SELL WITH CONTEXT")}${eventsTable}</div>
    <details class="block secondary" data-state-key="paper-strategy-signals">
      <summary>${blockTitle("策略信号", "RAW SIGNALS · LATEST 20")}</summary>
      ${signalsTable}
    </details>
  </div>`;
}

function renderStrategy(data) {
  const accounts = visiblePaperAccounts(data);
  latestPaperAccounts = accounts;
  if (!accounts.length) return [data.status, emptyBox("等待模拟账户启动", "compression_breakout · orderflow_impulse · liquidation_cascade")];
  selectedPaperAccount = Math.min(selectedPaperAccount, accounts.length - 1);
  const cards = paperCards(accounts.map(withPaperEquity));
  const selectedSummary = accounts[selectedPaperAccount];
  const selectedAccount = withPaperHistory({
    ...selectedSummary,
    ...paperDetailsByRun.get(selectedSummary.run_id),
  });
  const detail = paperDetailsByRun.has(selectedSummary.run_id)
    ? accountDetail(selectedAccount, selectedPaperAccount)
    : paperDetailPlaceholder(selectedSummary, selectedPaperAccount);
  return [data.status, cards + paperComparisonBlock(accounts) + detail];
}

function setPaperAccountTabState(body, selectedIndex) {
  body.querySelectorAll("[data-account-index]").forEach((candidate) => {
    const isSelected = Number(candidate.dataset.accountIndex) === selectedIndex;
    candidate.classList.toggle("is-active", isSelected);
    candidate.setAttribute("aria-selected", String(isSelected));
    candidate.setAttribute("tabindex", isSelected ? "0" : "-1");
  });
}

function selectPaperAccount(body, accounts, next) {
  const account = accounts?.[next];
  if (!Number.isInteger(next) || !account || next === selectedPaperAccount) return;
  selectedPaperAccount = next;
  setPaperAccountTabState(body, next);
  mountPaperDetail(body, account, next);
  void loadPaperAccountDetail(body, account, next);
}

function wirePaperAccountTabs(body, data) {
  const accounts = visiblePaperAccounts(data);
  latestPaperAccounts = accounts;
  const tabs = () => Array.from(body.querySelectorAll("[data-account-index]"));
  body.querySelectorAll("[data-account-index]").forEach((tab) => {
    tab.addEventListener("click", () => {
      selectPaperAccount(body, accounts, Number(tab.dataset.accountIndex));
    });
    tab.addEventListener("keydown", (event) => {
      const currentPosition = tabs().indexOf(tab);
      if (currentPosition < 0) return;
      let nextPosition = null;
      if (event.key === "ArrowRight" || event.key === "ArrowDown") {
        nextPosition = (currentPosition + 1) % tabs().length;
      } else if (event.key === "ArrowLeft" || event.key === "ArrowUp") {
        nextPosition = (currentPosition - 1 + tabs().length) % tabs().length;
      } else if (event.key === "Home") {
        nextPosition = 0;
      } else if (event.key === "End") {
        nextPosition = tabs().length - 1;
      } else if (event.key === "Enter" || event.key === " ") {
        selectPaperAccount(body, accounts, Number(tab.dataset.accountIndex));
        event.preventDefault();
        return;
      }
      if (nextPosition == null) return;
      const nextTab = tabs()[nextPosition];
      nextTab.focus();
      selectPaperAccount(body, accounts, Number(nextTab.dataset.accountIndex));
      event.preventDefault();
    });
  });
  setPaperAccountTabState(body, selectedPaperAccount);
  const account = accounts[selectedPaperAccount];
  if (account) {
    const mounted = body.querySelector(".paper-account-detail");
    if (mounted?.dataset.runId !== account.run_id) {
      mountPaperDetail(body, account, selectedPaperAccount);
    } else {
      wirePaperDetailControls(body, account, selectedPaperAccount);
    }
    void loadPaperAccountDetail(body, account, selectedPaperAccount);
  }
  void loadPaperEquityComparison(body);
}

function withPaperDetail(account) {
  if (!account?.run_id) return account;
  const detail = paperDetailsByRun.get(account.run_id);
  return detail ? { ...account, ...detail } : account;
}

function withPaperHistory(account) {
  if (!account?.run_id) return account;
  const history = paperHistoryByRun.get(account.run_id);
  if (!history) return account;
  return {
    ...account,
    closed_trades: history.closed_trades,
    trade_events: history.trade_events,
    history_loaded: true,
  };
}

function currentPaperAccountIs(account) {
  return latestPaperAccounts[selectedPaperAccount]?.run_id === account?.run_id;
}

function replacePaperDetail(body, html, preserveState = true) {
  const detail = body.querySelector(".paper-account-detail");
  if (!detail) return null;
  const viewState = preserveState ? captureViewState(body) : null;
  replaceElementFromHtml(detail, html);
  if (viewState) restoreViewState(body, viewState);
  return body.querySelector(".paper-account-detail");
}

function mountPaperDetail(body, account, index) {
  const detail = body.querySelector(".paper-account-detail");
  if (!detail) return;
  const merged = withPaperHistory(withPaperDetail(account));
  const html = paperDetailsByRun.has(account.run_id)
    ? accountDetail(merged, index)
    : paperDetailPlaceholder(account, index);
  replacePaperDetail(body, html, detail.dataset.runId === account.run_id);
  wirePaperDetailControls(body, account, index);
}

function wirePaperDetailControls(body, account, index) {
  wirePaperDetailButton(body, account, index);
  if (paperDetailsByRun.has(account.run_id)) {
    wirePaperHistoryButton(body, withPaperHistory(withPaperDetail(account)), index);
  }
}

function wirePaperDetailButton(body, account, index) {
  const button = body.querySelector("[data-load-paper-detail]");
  if (!button || !account?.run_id || button.dataset.wired === "true") return;
  button.dataset.wired = "true";
  button.addEventListener("click", () => void loadPaperAccountDetail(body, account, index));
}

async function loadPaperAccountDetail(body, account, index) {
  if (!account?.run_id) return;
  const loadedAt = paperDetailLoadedAt.get(account.run_id) || 0;
  if (paperDetailsByRun.has(account.run_id) && Date.now() - loadedAt < PAPER_DETAIL_CACHE_MS) {
    return paperDetailsByRun.get(account.run_id);
  }
  const existingRequest = paperDetailRequests.get(account.run_id);
  if (existingRequest) return existingRequest;
  const button = body.querySelector("[data-load-paper-detail]");
  if (button) {
    button.disabled = true;
    button.textContent = "加载中…";
  }
  const request = (async () => {
    try {
      const runId = encodeURIComponent(account.run_id);
      const detail = await requestJson(`api/paper-accounts/${runId}`);
      paperDetailsByRun.set(account.run_id, detail);
      paperDetailLoadedAt.set(account.run_id, Date.now());
      if (currentPaperAccountIs(account)) {
        mountPaperDetail(body, account, index);
      }
    } catch (error) {
      if (currentPaperAccountIs(account)) {
        replacePaperDetail(body, paperDetailPlaceholder(account, index, `详情加载失败 · ${error.message}`));
        wirePaperDetailButton(body, account, index);
      }
    } finally {
      paperDetailRequests.delete(account.run_id);
    }
  })();
  paperDetailRequests.set(account.run_id, request);
  return request;
}

async function loadPaperEquityComparison(body) {
  if (paperEquityRequest) return paperEquityRequest;
  const cacheKey = latestPaperAccounts
    .map((account) => account.run_id)
    .filter(Boolean)
    .sort()
    .join("|");
  if (
    cacheKey === paperEquityCacheKey
    && paperEquityLoadedAt
    && Date.now() - paperEquityLoadedAt < PAPER_EQUITY_CACHE_MS
  ) {
    return null;
  }
  paperEquityRequest = (async () => {
    try {
      const data = await requestJson("api/paper-accounts/equity");
      paperEquityByRun.clear();
      for (const account of data.accounts || []) paperEquityByRun.set(account.run_id, account);
      paperEquityCacheKey = cacheKey;
      paperEquityLoadedAt = Date.now();
      const comparison = body.querySelector("[data-paper-comparison]");
      if (comparison) replaceElementFromHtml(comparison, paperComparisonBlock(latestPaperAccounts));
      const cards = body.querySelector(".acct-cards");
      if (cards) {
        replaceElementFromHtml(cards, paperCards(latestPaperAccounts.map(withPaperEquity)));
        wirePaperAccountTabs(body, { accounts: latestPaperAccounts });
      }
    } catch (error) {
      const comparison = body.querySelector("[data-paper-comparison]");
      if (comparison) {
        replaceElementFromHtml(
          comparison,
          `<div class="block pair-section" data-paper-comparison>${emptyBox("权益曲线加载失败", error.message)}</div>`,
        );
      }
    } finally {
      paperEquityRequest = null;
    }
  })();
  return paperEquityRequest;
}

function wirePaperHistoryButton(body, account, index) {
  const button = body.querySelector("[data-load-paper-history]");
  if (!button || !account?.run_id || button.dataset.wired === "true") return;
  button.dataset.wired = "true";
  button.addEventListener("click", () => loadPaperAccountHistory(body, account, index));
}

async function loadPaperAccountHistory(body, account, index) {
  const button = body.querySelector("[data-load-paper-history]");
  if (button) {
    button.disabled = true;
    button.textContent = "加载中…";
  }
  try {
    const runId = encodeURIComponent(account.run_id);
    const history = await requestJson(`api/paper-accounts/${runId}/history`);
    paperHistoryByRun.set(account.run_id, history);
    const merged = withPaperHistory(withPaperDetail(account));
    replacePaperDetail(body, accountDetail(merged, index));
    wirePaperHistoryButton(body, merged, index);
  } catch (error) {
    if (button) {
      button.disabled = false;
      button.textContent = `加载失败 · ${error.message}`;
    }
  }
}

  return {
    render: renderStrategy,
    wire: wirePaperAccountTabs,
  };
}
