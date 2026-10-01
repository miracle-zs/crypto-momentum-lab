import assert from "node:assert/strict";
import test from "node:test";

import {
  asNumber,
  computeReturnRate,
  computeRoi,
  esc,
  money,
  statusClass,
} from "../../src/crypto_momentum_lab/operator_dashboard/static/dashboard-formatters.js";
import {
  buildLatestStartEquityModels,
  buildStrategyEquityModels,
  equityChart,
  equityWindowMetrics,
  latestStartEquityChart,
  liveAccountMetricChart,
  liveAccountMetricModel,
  maxDrawdown,
  standaloneSparkline,
  strategyEquityChart,
} from "../../src/crypto_momentum_lab/operator_dashboard/static/dashboard-charts.js";
import {
  buildChartOption,
  chartPayloadSignature,
  getChartPayload,
} from "../../src/crypto_momentum_lab/operator_dashboard/static/dashboard-chart-engine.js";
import { readinessStatusForSection } from "../../src/crypto_momentum_lab/operator_dashboard/static/dashboard-readiness.js";
import { renderOverview } from "../../src/crypto_momentum_lab/operator_dashboard/static/sections/overview.js";
import { renderRisk } from "../../src/crypto_momentum_lab/operator_dashboard/static/sections/risk.js";
import { renderAccount } from "../../src/crypto_momentum_lab/operator_dashboard/static/sections/account.js";
import { renderCollector } from "../../src/crypto_momentum_lab/operator_dashboard/static/sections/collector.js";
import {
  DEFAULT_INITIAL_CAPITAL,
  computePaperAccountReturn,
  computeUnrealizedPnlRatio,
  createStrategySection,
} from "../../src/crypto_momentum_lab/operator_dashboard/static/sections/strategy.js";
import { renderUniverse } from "../../src/crypto_momentum_lab/operator_dashboard/static/sections/universe.js";
import { renderPerformance } from "../../src/crypto_momentum_lab/operator_dashboard/static/sections/performance.js";
import {
  POLL_MS,
  SECTION_POLL_MS,
} from "../../src/crypto_momentum_lab/operator_dashboard/static/dashboard-config.js";
import { sectionRenderKey } from "../../src/crypto_momentum_lab/operator_dashboard/static/dashboard-rendering.js";
import { captureViewState, restoreViewState, replaceChildrenFromHtml, createScrollGuard, isUserScrolling } from "../../src/crypto_momentum_lab/operator_dashboard/static/dashboard-dom.js";

test("dashboard polling keeps safety sections fresh and backs off cold sections", () => {
  assert.equal(POLL_MS, 15000);
  assert.deepEqual(SECTION_POLL_MS, {
    overview: 15000,
    risk: 15000,
    account: 15000,
    performance: 15000,
    strategy: 30000,
    universe: 30000,
    collector: 30000,
    reports: 30000,
  });
});

test("heartbeat-only account updates do not rebuild the active section", () => {
  const first = {
    status: "READY",
    accounts: [{
      account_label: "primary",
      status: "READY",
      observed_at: "2026-09-06T11:00:00Z",
      lease_expires_at: "2026-09-06T11:02:00Z",
    }],
  };
  const second = {
    ...first,
    accounts: [{
      ...first.accounts[0],
      observed_at: "2026-09-06T11:00:15Z",
      lease_expires_at: "2026-09-06T11:02:15Z",
    }],
  };

  assert.equal(sectionRenderKey("account", first), sectionRenderKey("account", second));
});

test("syncing/ready_readonly flips do not rebuild the account section", () => {
  const base = {
    status: "DEGRADED",
    accounts: [{
      account_label: "primary",
      status: "DEGRADED",
      readiness: "syncing",
      strategy_state: null,
      strategy_name: "orderflow_impulse",
      environment: "live",
    }],
  };
  const flipped = {
    status: "READY",
    accounts: [{
      account_label: "primary",
      status: "READY",
      readiness: "ready_readonly",
      strategy_state: "active",
      strategy_name: "orderflow_impulse",
      environment: "live",
    }],
  };
  assert.equal(sectionRenderKey("account", base), sectionRenderKey("account", flipped));
});

test("overview account heartbeats do not rebuild the section", () => {
  const first = {
    status: "READY",
    services: [{ name: "market-data", status: "READY", age_seconds: 5, observed_at: "2026-09-27T05:00:00Z" }],
    account_statuses: [{ account_label: "primary", observed_at: "2026-09-27T05:00:00Z", lease_expires_at: "2026-10-01T00:00:00Z", status: "READY" }],
  };
  const second = {
    ...first,
    services: [{ name: "market-data", status: "READY", age_seconds: 20, observed_at: "2026-09-27T05:00:15Z" }],
    account_statuses: [{ account_label: "primary", observed_at: "2026-09-27T05:00:15Z", lease_expires_at: "2026-10-01T00:00:00Z", status: "READY" }],
  };

  assert.equal(sectionRenderKey("overview", first), sectionRenderKey("overview", second));
});

test("risk heartbeat updates do not rebuild the section", () => {
  const first = {
    status: "READY",
    data_age_seconds: 2.5,
    observed_at: "2026-09-27T05:00:00Z",
    active_halts: [],
  };
  const second = {
    ...first,
    data_age_seconds: 17.5,
    observed_at: "2026-09-27T05:00:15Z",
  };

  assert.equal(sectionRenderKey("risk", first), sectionRenderKey("risk", second));
});

test("collector heartbeat updates do not rebuild the section", () => {
  const first = {
    status: "READY",
    checkpoint_age_seconds: 10,
    parquet_latest_age_seconds: 15,
    pending_spool_oldest_age_seconds: 2,
    stream_id: "research",
  };
  const second = {
    ...first,
    checkpoint_age_seconds: 25,
    parquet_latest_age_seconds: 30,
    pending_spool_oldest_age_seconds: 17,
  };

  assert.equal(sectionRenderKey("collector", first), sectionRenderKey("collector", second));
});

test("strategy account heartbeats refresh cards without rebuilding the page", () => {
  const first = {
    status: "READY",
    accounts: [{
      run_id: "paper-account-1",
      checkpoint_at: "2026-09-06T11:00:00Z",
      portfolio_summary: { equity: "1000" },
    }],
  };
  const second = {
    ...first,
    accounts: [{
      ...first.accounts[0],
      checkpoint_at: "2026-09-06T11:00:30Z",
      portfolio_summary: { equity: "1001" },
    }],
  };

  assert.equal(sectionRenderKey("strategy", first), sectionRenderKey("strategy", second));
});

test("operator formatters keep status and money output stable", () => {
  assert.equal(esc('<live status="READY">'), "&lt;live status=&quot;READY&quot;&gt;");
  assert.equal(statusClass("live"), "status-LIVE");
  assert.equal(money(12.5), "$12.50");
  assert.equal(asNumber("12.5"), 12.5);
  assert.equal(asNumber(0), 0);
  assert.equal(asNumber("0"), 0);
  assert.equal(asNumber(null), null);
  assert.equal(asNumber(undefined), null);
  assert.equal(asNumber(""), null);
  assert.equal(asNumber("   "), null);
  assert.equal(asNumber(true), null);
  assert.equal(asNumber(false), null);
  assert.equal(asNumber(Infinity), null);
  assert.equal(asNumber(-Infinity), null);
  assert.equal(asNumber(NaN), null);
  assert.equal(asNumber("invalid"), null);

  assert.equal(computeReturnRate(1100, 1000), 0.1);
  assert.equal(computeReturnRate(900, 1000), -0.1);
  assert.equal(computeReturnRate(null, 1000), null);
  assert.equal(computeReturnRate(1000, 0), null);
  assert.equal(computeReturnRate(1000, -100), null);
  assert.equal(computeReturnRate(1000, null), null);

  assert.equal(computeRoi(50, 500), 0.1);
  assert.equal(computeRoi(-50, 500), -0.1);
  assert.equal(computeRoi(null, 500), null);
  assert.equal(computeRoi(50, 0), null);
  assert.equal(computeRoi(50, null), null);
  assert.equal(computeRoi(50, "0"), null);
});

test("global readiness uses database status for the overview section", () => {
  assert.equal(
    readinessStatusForSection("overview", { database_status: "READY" }),
    "READY",
  );
  assert.equal(
    readinessStatusForSection("risk", { status: "HALTED" }),
    "HALTED",
  );
  assert.equal(
    readinessStatusForSection("overview", { database_status: "READY", _cache_status: "STALE" }),
    "STALE",
  );
  assert.equal(
    readinessStatusForSection("risk", { status: "READY", _cache_status: "STALE" }),
    "STALE",
  );
});

test("strategy equity models align paper and live B1 on common buckets", () => {
  const equityCurve = (values) => values.map((equity, index) => ({
    observed_at: `2026-08-16T00:${String(index * 6).padStart(2, "0")}:00Z`,
    equity,
  }));
  const accounts = [
    {
      run_id: "paper-account-1",
      strategy_name: "orderflow_impulse",
      source: "paper",
      exit_label: "固定 TP / SL",
      equity_curve: equityCurve([1000, 1002, 1005]),
    },
    {
      run_id: "live-primary",
      strategy_name: "orderflow_impulse",
      source: "live",
      exit_label: "实盘 B1",
      equity_curve: equityCurve([1000, 999, 1003]),
    },
  ];

  const [model] = buildStrategyEquityModels(accounts);
  assert.equal(model.strategyName, "orderflow_impulse");
  assert.deepEqual(model.series.map((series) => series.label), ["固定 TP / SL", "实盘 B1"]);
  assert.deepEqual(model.series.map((series) => series.displayLabel), ["账户 1 · 固定 TP/SL", "实盘基准"]);
  assert.deepEqual(model.series.map((series) => series.values), [[0, 2, 5], [0, -1, 3]]);
  assert.equal(model.series[1].colorClass, "live");
  assert.equal(model.anchorAt, Date.parse("2026-08-16T00:00:00Z"));
  assert.equal(model.startAt, Date.parse("2026-08-16T00:00:00Z"));
  assert.equal(model.anchorMode, "daily-anchor");
});

test("strategy comparison labels duplicate live accounts by account label", () => {
  const equityCurve = (values) => values.map((equity, index) => ({
    observed_at: `2026-08-16T00:${String(index * 6).padStart(2, "0")}:00Z`,
    equity,
  }));
  const accounts = ["primary", "account-2", "account-3", "account-4"].map(
    (account_label, index) => ({
      run_id: `live-${account_label}`,
      account_label,
      strategy_name: "orderflow_impulse",
      source: "live",
      exit_label: "实盘 Top10 · B8",
      equity_curve: equityCurve([1000 + index, 1001 + index, 1002 + index]),
    }),
  );

  const [model] = buildStrategyEquityModels(accounts);
  assert.deepEqual(
    model.series.map((series) => series.label),
    [
      "实盘 Top10 · B8 · primary",
      "实盘 Top10 · B8 · account-2",
      "实盘 Top10 · B8 · account-3",
      "实盘 Top10 · B8 · account-4",
    ],
  );
  assert.deepEqual(
    model.series.map((series) => series.displayLabel),
    ["实盘 · primary", "实盘 · account-2", "实盘 · account-3", "实盘 · account-4"],
  );
});

test("strategy equity comparison starts at the first common bucket after 08:00", () => {
  const equityCurve = (values) => values.map((equity, index) => ({
    observed_at: `2026-08-16T00:${String((index + 1) * 6).padStart(2, "0")}:00Z`,
    equity,
  }));
  const [model] = buildStrategyEquityModels([
    {
      run_id: "paper-account-1",
      strategy_name: "orderflow_impulse",
      source: "paper",
      exit_label: "15M 收线退出",
      equity_curve: equityCurve([1000, 1002, 1005]),
    },
    {
      run_id: "live-primary",
      strategy_name: "orderflow_impulse",
      source: "live",
      exit_label: "实盘 B1",
      equity_curve: equityCurve([1000, 999, 1003]),
    },
  ]);

  assert.equal(model.anchorAt, Date.parse("2026-08-16T00:00:00Z"));
  assert.equal(model.startAt, Date.parse("2026-08-16T00:06:00Z"));
  assert.equal(model.anchorMode, "after-anchor");
  assert.deepEqual(model.series.map((series) => series.values), [[0, 2, 5], [0, -1, 3]]);
});

test("strategy equity comparison keeps the current synchronized cohort", () => {
  const equityCurve = (startHour, values) => values.map((equity, index) => ({
    observed_at: `2026-08-16T${String(startHour).padStart(2, "0")}:${String(index * 6).padStart(2, "0")}:00Z`,
    equity,
  }));
  const [model] = buildStrategyEquityModels([
    {
      run_id: "paper-account-old-1",
      strategy_name: "orderflow_impulse",
      source: "paper",
      exit_label: "旧版本 A",
      equity_curve: equityCurve(0, [1000, 1001, 1002]),
    },
    {
      run_id: "paper-account-old-2",
      strategy_name: "orderflow_impulse",
      source: "paper",
      exit_label: "旧版本 B",
      equity_curve: equityCurve(0, [1000, 999, 1001]),
    },
    {
      run_id: "paper-account-current-1",
      strategy_name: "orderflow_impulse",
      source: "paper",
      exit_label: "当前版本 A",
      equity_curve: equityCurve(3, [1000, 1002, 1004]),
    },
    {
      run_id: "paper-account-current-2",
      strategy_name: "orderflow_impulse",
      source: "paper",
      exit_label: "当前版本 B",
      equity_curve: equityCurve(3, [1000, 1001, 1003]),
    },
  ]);

  assert.deepEqual(
    model.accounts.map((account) => account.run_id),
    ["paper-account-current-1", "paper-account-current-2"],
  );
  assert.equal(model.omittedAccounts.length, 2);
  assert.equal(model.points.length, 3);
});

test("latest-start equity models expose cash-flow-adjusted amount deltas", () => {
  const commonCurve = (deltas) => deltas.map((delta, index) => ({
    observed_at: new Date(Date.parse("2026-08-21T02:45:00Z") + index * 900000).toISOString(),
    equity: String(1000 + delta),
    delta: String(delta),
    return_pct: String(delta / 10),
  }));
  const meta = {
    common_equity_start_at: "2026-08-21T02:45:00Z",
    common_equity_end_at: "2026-08-21T03:15:00Z",
    common_equity_sample_interval_seconds: 900,
    common_equity_note: "实盘已扣除 200 USDT 充值。",
  };
  const [model] = buildLatestStartEquityModels([
    {
      run_id: "paper-account-15",
      strategy_name: "orderflow_impulse",
      source: "paper",
      exit_label: "15M 收线退出",
      common_equity_curve: commonCurve([0, 2, 5]),
    },
    {
      run_id: "live-primary-b1",
      strategy_name: "orderflow_impulse",
      source: "live",
      exit_label: "实盘 B1",
      common_equity_curve: commonCurve([0, -1, 3]),
    },
  ], meta);

  assert.equal(model.startAt, Date.parse("2026-08-21T02:45:00Z"));
  assert.deepEqual(model.series.map((series) => series.values), [[0, 2, 5], [0, -1, 3]]);
  assert.equal(model.series[1].delta, 3);
  const chartHtml = latestStartEquityChart(model);
  const payload = getChartPayload("latest-start-orderflow_impulse");
  assert.match(chartHtml, /data-echart-id="latest-start-orderflow_impulse"/);
  assert.equal(payload.kind, "comparison");
  assert.deepEqual(payload.series.map((series) => series.label), ["15M 收线退出", "实盘 B1"]);
});

test("sparklines render an empty state without a browser DOM", () => {
  assert.equal(standaloneSparkline([]), '<div class="spark spark-empty">—</div>');
  assert.match(
    standaloneSparkline([{ equity: 1000 }, { equity: 1001 }]),
    /<svg class="spark pos"/,
  );
});

test("overview renderer exposes local heartbeat update hooks", () => {
  const [status, html] = renderOverview({
    database_status: "READY",
    active_halt_count: 0,
    active_lease: null,
    services: [{
      name: "live-rollout",
      status: "LIVE",
      age_seconds: 5,
      observed_at: "2026-08-16T00:00:00Z",
    }],
  });
  assert.equal(status, "READY");
  assert.match(html, /data-service-age="live-rollout"/);
  assert.match(html, /data-service-meter="live-rollout"/);
});

test("collector renderer exposes freshness, continuity, and capacity evidence", () => {
  const [status, html] = renderCollector({
    status: "FRESH",
    status_detail: "checkpoint 与 Parquet 窗口持续更新",
    environment: "research",
    top_count: 30,
    checkpoint_at: "2026-09-03T15:16:00Z",
    checkpoint_age_seconds: 12,
    last_bucket_start: "2026-09-03T15:14:45Z",
    last_sequence: 1814,
    last_symbol: "龙虾USDT",
    stream_id: "stream-id",
    collector_bytes: 4 * 1024 * 1024,
    collector_soft_limit_bytes: 6 * 1024 ** 3,
    collector_hard_limit_bytes: 8 * 1024 ** 3,
    disk_free_bytes: 43 * 1024 ** 3,
    disk_warning_free_bytes: 15 * 1024 ** 3,
    disk_pause_free_bytes: 10 * 1024 ** 3,
    pending_spool_files: 0,
    pending_spool_bytes: 0,
    pending_spool_overdue_files: 0,
    parquet_file_count: 32,
    parquet_first_window_start: "2026-09-03T07:15:00Z",
    parquet_latest_window_start: "2026-09-03T15:00:00Z",
    parquet_latest_written_at: "2026-09-03T15:15:46Z",
    parquet_latest_age_seconds: 14,
    parquet_window_seconds: 900,
    parquet_gap_count: 0,
    late_tolerance_seconds: 30,
    max_spool_bytes: 1024 ** 3,
    capacity_state: "healthy",
    alerts: [],
    recent_windows: [{
      window_start: "2026-09-03T15:00:00Z",
      written_at: "2026-09-03T15:15:46Z",
      size_bytes: 143740,
    }],
  });

  assert.equal(status, "FRESH");
  assert.match(html, /Top30 数据采集/);
  assert.match(html, /连续性未发现缺口/);
  assert.match(html, /容量保护/);
  assert.match(html, /最近封存窗口/);
  assert.match(html, /龙虾USDT/);
  assert.match(html, /collector-recent-windows/);
});

test("collector renderer distinguishes normal spool from overdue spool", () => {
  const [freshStatus, freshHtml] = renderCollector({
    status: "FRESH",
    status_detail: "当前 15 分钟窗口写入中，待封存 3 个",
    pending_spool_files: 3,
    pending_spool_bytes: 2048,
    pending_spool_overdue_files: 0,
    alerts: [],
  });
  assert.equal(freshStatus, "FRESH");
  assert.match(freshHtml, /spool 待封存/);
  assert.match(freshHtml, /当前 15 分钟窗口写入中/);
  assert.doesNotMatch(freshHtml, /spool 超时积压/);
  assert.match(freshHtml, /没有容量或超时积压告警/);

  const [degradedStatus, degradedHtml] = renderCollector({
    status: "DEGRADED",
    status_detail: "spool 超时待处理 2 个（当前共 5 个）",
    pending_spool_files: 5,
    pending_spool_bytes: 4096,
    pending_spool_overdue_files: 2,
    alerts: ["spool 有 2 个文件超时未落盘"],
  });
  assert.equal(degradedStatus, "DEGRADED");
  assert.match(degradedHtml, /spool 超时积压/);
  assert.match(degradedHtml, /2 个已超过封存时限/);
  assert.match(degradedHtml, /REVIEW/);
});

test("universe renderer includes relative snapshot age", () => {
  const [status, html] = renderUniverse({
    status: "READY",
    observed_at: "2026-08-18T04:51:00Z",
    gainers: [],
    losers: [],
    monitored_symbols: [],
  });
  assert.equal(status, "READY");
  assert.match(html, /快照时间/);
});

test("universe renderer keeps ranking and monitoring views distinct", () => {
  const [status, html] = renderUniverse({
    status: "FRESH",
    observed_at: "2026-08-18T04:51:00Z",
    gainers: [{
      symbol: "AAAUSDT",
      rank: 1,
      current_price: "10",
      utc_day_return: "0.12",
    }],
    losers: [{
      symbol: "BBBUSD",
      rank: 1,
      current_price: "3",
      utc_day_return: "-0.08",
    }],
    monitored_symbols: [
      {
        symbol: "AAAUSDT",
        status: "target",
        side: "gainer",
        rank: 1,
        current_price: "10",
        utc_day_return: "0.12",
      },
      {
        symbol: "CCCUSDT",
        status: "retained",
        side: "loser",
        rank: 25,
        current_price: "2",
        utc_day_return: "-0.02",
      },
    ],
  });
  assert.equal(status, "FRESH");
  assert.match(html, /监控池 2/);
  assert.match(html, /补充监控 1/);
  assert.match(html, /MONITORING ADDITIONS/);
  assert.match(html, /涨幅榜中的目标标的已计入监控池/);
  assert.match(html, /监控状态/);
  assert.match(html, /保留/);
  assert.doesNotMatch(html, /跌幅榜 Top 20/);
  assert.equal(html.match(/AAAUSDT/g)?.length, 1);
  assert.equal(html.match(/CCCUSDT/g)?.length, 1);
  assert.doesNotMatch(html, /目标池 · 涨幅 Top 20/);
  assert.doesNotMatch(html, /目标池 · 跌幅 Top 20/);
  assert.doesNotMatch(html, /class="chip/);
});

test("risk renderer separates confirmed pending orders from uncertain orders", () => {
  const [status, html] = renderRisk({
    status: "READY",
    active_halts: [],
    latest_risk_decisions: [],
    pending_orders: [{
      symbol: "龙虾USDT",
      client_order_id: "cml-order",
      side: "SELL",
      state: "acknowledged",
      updated_at: "2026-08-16T16:33:04Z",
    }],
    ambiguous_orders: [],
  });
  assert.equal(status, "READY");
  assert.match(html, /待完成订单/);
  assert.match(html, /RESTING \/ PARTIALLY FILLED/);
  assert.match(html, /龙虾USDT/);
  assert.match(html, /无不确定订单/);
  assert.match(html, /risk-order-grid-single/);
  assert.match(html, /先完成交易所对账，再决定恢复执行或人工处理/);
  assert.match(html, /chip-source/);
  assert.match(html, /数据年龄/);
  assert.match(html, /chip-coverage/);
  assert.match(html, /覆盖范围/);
});

test("risk renderer displays required symbols, missing symbols, and coverage alert", () => {
  const [status, html] = renderRisk({
    status: "STALE",
    active_halts: [],
    latest_risk_decisions: [],
    pending_orders: [],
    ambiguous_orders: [],
    required_symbols: ["BTCUSDT", "ETHUSDT"],
    missing_symbols: ["ETHUSDT"],
    coverage_scope: "1/2 covered",
  });
  assert.equal(status, "STALE");
  assert.match(html, /必需品种未覆盖/);
  assert.match(html, /alert-missing-symbols/);
  assert.match(html, /ETHUSDT/);
  assert.match(html, /1\/2 covered/);
  assert.match(html, /risk-coverage-bar/);
  assert.doesNotMatch(html, /alert-coverage-error/);
  assert.doesNotMatch(html, /覆盖查询异常/);
});

test("risk renderer distinguishes UNKNOWN/QUERY_ERROR coverage error from real market missing alert", () => {
  const [status, html] = renderRisk({
    status: "UNKNOWN",
    source_status: "QUERY_ERROR",
    coverage_scope: "QUERY_ERROR",
    coverage_error: "UNIVERSE_QUERY_FAILED (ref: cov_3f8a12bc)",
    coverage_error_code: "UNIVERSE_QUERY_FAILED",
    coverage_trace_id: "cov_3f8a12bc",
    required_symbols: [],
    missing_symbols: [],
    active_halts: [],
    latest_risk_decisions: [],
    pending_orders: [],
    ambiguous_orders: [],
  });
  assert.equal(status, "UNKNOWN");
  assert.match(html, /alert-coverage-error/);
  assert.match(html, /覆盖查询异常 \(QUERY_ERROR\)/);
  assert.match(html, /UNIVERSE_QUERY_FAILED \(ref: cov_3f8a12bc\)/);
  assert.doesNotMatch(html, /alert-missing-symbols/);
  assert.doesNotMatch(html, /行情缺失/);
});

test("strategy section owns paper-account rendering state", () => {
  const strategy = createStrategySection({
    requestJson: async () => ({}),
  });
  const [status, html] = strategy.render({ status: "NO_DATA", accounts: [] });
  assert.equal(status, "NO_DATA");
  assert.match(html, /等待模拟账户启动/);
});

test("strategy section hides fixed TP/SL accounts consistently", () => {
  const strategy = createStrategySection({
    requestJson: async () => ({}),
  });
  const [status, html] = strategy.render({
    status: "READY",
    accounts: [
      {
        run_id: "fixed-account",
        strategy_name: "orderflow_impulse",
        exit_mode: "fixed",
        exit_label: "固定 TP / SL",
      },
      {
        run_id: "candle-account",
        strategy_name: "orderflow_impulse",
        exit_mode: "candle_15m",
        exit_label: "15M 收线退出",
        portfolio_summary: {},
      },
    ],
  });
  assert.equal(status, "READY");
  assert.doesNotMatch(html, /固定 TP \/ SL/);
  assert.match(html, /15M 收线退出/);
});

test("strategy return and unrealized pnl calculations defend against bad inputs", () => {
  assert.equal(DEFAULT_INITIAL_CAPITAL, 1000);
  assert.equal(computePaperAccountReturn("1100"), 0.1);
  assert.equal(computePaperAccountReturn("900"), -0.1);
  assert.equal(computePaperAccountReturn(null), null);
  assert.equal(computePaperAccountReturn(undefined), null);
  assert.equal(computePaperAccountReturn("invalid"), null);

  assert.equal(computeUnrealizedPnlRatio("10", "100"), 0.1);
  assert.equal(computeUnrealizedPnlRatio("-5", "100"), -0.05);
  assert.equal(computeUnrealizedPnlRatio("10", "0"), null);
  assert.equal(computeUnrealizedPnlRatio("10", null), null);
  assert.equal(computeUnrealizedPnlRatio(null, "100"), null);
});

test("chartPayloadSignature detects series.values and point mutations without object coercion", () => {
  const initialComparison = {
    kind: "comparison",
    title: "Strategy Comparison",
    points: [{ at: 1000 }, { at: 2000 }],
    series: [
      { label: "Account 1", color: "#34d399", values: [0, 10] },
      { label: "Account 2", color: "#7aa2f7", values: [0, -5] },
    ],
  };

  const sig1 = chartPayloadSignature(initialComparison);
  assert.ok(sig1.length > 0);
  assert.doesNotMatch(sig1, /\[object Object\]/);

  // Reproduction case: values change from [0, 10] to [0, 99]
  const updatedComparison = {
    ...initialComparison,
    series: [
      { label: "Account 1", color: "#34d399", values: [0, 99] },
      { label: "Account 2", color: "#7aa2f7", values: [0, -5] },
    ],
  };
  const sig2 = chartPayloadSignature(updatedComparison);
  assert.notEqual(sig1, sig2, "Signature must differ when series.values change from [0, 10] to [0, 99]");

  // Identical values must produce identical signature
  const identicalComparison = {
    ...initialComparison,
    series: [
      { label: "Account 1", color: "#34d399", values: [0, 10] },
      { label: "Account 2", color: "#7aa2f7", values: [0, -5] },
    ],
  };
  assert.equal(sig1, chartPayloadSignature(identicalComparison));

  // Equity kind: point mutations must change signature and never produce [object Object]
  const initialEquity = {
    kind: "equity",
    title: "Account Equity",
    points: [
      { atMs: 1000, equity: 1000 },
      { atMs: 2000, equity: 1010 },
    ],
  };
  const eqSig1 = chartPayloadSignature(initialEquity);
  assert.doesNotMatch(eqSig1, /\[object Object\]/);

  const updatedEquity = {
    ...initialEquity,
    points: [
      { atMs: 1000, equity: 1000 },
      { atMs: 2000, equity: 1050 },
    ],
  };
  const eqSig2 = chartPayloadSignature(updatedEquity);
  assert.notEqual(eqSig1, eqSig2, "Signature must differ when equity point value changes");
});

test("strategy section defends against race conditions and unmounted nodes during history fetch", async () => {
  let resolveHistory0;
  const history0Promise = new Promise((resolve) => {
    resolveHistory0 = resolve;
  });

  const requestedUrls = [];
  const strategy = createStrategySection({
    requestJson: async (url) => {
      requestedUrls.push(url);
      if (url.includes("acc-0/history")) {
        return history0Promise;
      }
      return { closed_trades: [], trade_events: [] };
    },
  });

  const data = {
    status: "READY",
    accounts: [
      {
        run_id: "acc-0",
        strategy_name: "orderflow_impulse",
        exit_mode: "candle_15m",
        portfolio_summary: { equity: 1000 },
      },
      {
        run_id: "acc-1",
        strategy_name: "orderflow_impulse",
        exit_mode: "candle_15m",
        portfolio_summary: { equity: 2000 },
      },
    ],
  };

  strategy.render(data);
  assert.equal(strategy.currentAccountIs(data.accounts[0]), true);
  assert.equal(strategy.currentAccountIs(data.accounts[1]), false);

  let replaceCalled = false;
  const mockButton = { disabled: false, textContent: "" };
  const unmountedBody = {
    isConnected: false,
    querySelector: (sel) => {
      if (sel.includes("data-load-paper-history")) return mockButton;
      if (sel.includes("paper-account-panel") || sel.includes("paper-account-detail")) {
        return {
          replaceWith: () => { replaceCalled = true; },
        };
      }
      return null;
    },
  };

  // Launch history request on unmounted body
  const pendingHistory = strategy.loadHistory(unmountedBody, data.accounts[0], 0);
  assert.equal(mockButton.disabled, true);
  assert.equal(mockButton.textContent, "加载中…");

  // In-flight request deduplication: calling again does not fire another HTTP request
  const dupHistory = strategy.loadHistory(unmountedBody, data.accounts[0], 0);
  assert.equal(requestedUrls.filter((u) => u.includes("acc-0/history")).length, 1);

  // Resolve network response
  resolveHistory0({
    history_complete: true,
    closed_trades: [{ symbol: "BTCUSDT", realized_pnl: 100 }],
    trade_events: [],
  });

  await pendingHistory;

  // Unmounted body must not have replaced DOM
  assert.equal(replaceCalled, false);

  // Cross-account protection: when active account is acc-0, acc-1 request must not touch DOM
  let crossAccountReplaced = false;
  const connectedBody = {
    isConnected: true,
    querySelector: (sel) => {
      if (sel.includes("paper-account-panel") || sel.includes("paper-account-detail")) {
        return {
          replaceWith: () => { crossAccountReplaced = true; },
        };
      }
      return null;
    },
  };

  const acc1Promise = strategy.loadHistory(connectedBody, data.accounts[1], 1);
  await acc1Promise;
  assert.equal(crossAccountReplaced, false);
});

test("equity charts register an ECharts payload with native metrics", () => {
  const rows = [
    { observed_at: "2026-08-16T00:00:00Z", equity: 1000 },
    { observed_at: "2026-08-16T00:06:00Z", equity: 1002 },
  ];
  const html = equityChart(rows, "test-equity");
  const payload = getChartPayload("test-equity");
  assert.match(html, /data-echart-chart/);
  assert.match(html, /data-echart-kind="equity"/);
  assert.match(html, /data-echart-id="test-equity"/);
  assert.match(html, /窗口基线/);
  assert.match(html, /窗口变化/);
  assert.match(html, /最大回撤/);
  assert.equal(payload.kind, "equity");
  assert.equal(payload.points.length, 2);
  assert.equal(payload.baseline, 1000);
  assert.equal(payload.delta, 2);
  assert.equal(payload.maxDrawdown, 0);
  const option = buildChartOption(payload, {
    up: "#34d399",
    down: "#fb7185",
    brand: "#7aa2f7",
    faint: "#738099",
    surface: "#1d2431",
    line: "#273043",
    lineStrong: "#3a4960",
    text: "#e9edf4",
    muted: "#a5b0c3",
    live: "#67e8f9",
  });
  assert.equal(option.xAxis.type, "time");
  assert.equal(option.series[0].type, "line");
  assert.equal(option.series[0].data.length, 2);
  assert.deepEqual(equityWindowMetrics(rows), {
    baseline: 1000,
    latest: 1002,
    delta: 2,
    maxDrawdown: 0,
  });
  assert.equal(maxDrawdown([{ equity: 1000 }, { equity: 992 }, { equity: 995 }]), -8);

  const [model] = buildStrategyEquityModels([
    {
      run_id: "paper-account-1",
      strategy_name: "orderflow_impulse",
      source: "paper",
      exit_mode: "candle_15m",
      exit_label: "15M 收线退出",
      equity_curve: rows,
    },
    {
      run_id: "live-primary",
      strategy_name: "orderflow_impulse",
      source: "live",
      exit_label: "实盘 B1",
      equity_curve: [{ ...rows[0], equity: 1000 }, { ...rows[1], equity: 999 }],
    },
  ]);
  const comparisonHtml = strategyEquityChart(model);
  const comparisonPayload = getChartPayload("comparison-orderflow_impulse");
  assert.match(comparisonHtml, /data-echart-chart/);
  assert.match(comparisonHtml, /data-echart-kind="comparison"/);
  assert.match(comparisonHtml, /data-echart-id="comparison-orderflow_impulse"/);
  assert.deepEqual(comparisonPayload.series.map((series) => series.label), ["15M 收线退出", "实盘 B1"]);
  assert.equal(comparisonPayload.series[1].isLive, true);
  assert.equal(comparisonPayload.points.length, 2);
});

test("live account metric charts compare all four accounts with unit-aware axes", () => {
  const labels = ["primary", "account-2", "account-3", "account-4"];
  const accounts = labels.map((account_label, index) => ({
    account_label,
    metrics_curve: [
      {
        observed_at: "2026-08-16T00:00:00Z",
        equity: String(1000 + index * 10),
        equity_change_ratio: "0",
        margin_used: String(100 + index),
        margin_occupancy_ratio: "0.1",
        drawdown: "0",
        drawdown_ratio: "0",
      },
      {
        observed_at: "2026-08-16T00:06:00Z",
        equity: String(1010 + index * 10),
        equity_change_ratio: "0.01",
        margin_used: String(120 + index),
        margin_occupancy_ratio: "0.12",
        drawdown: "-2",
        drawdown_ratio: "-0.002",
      },
    ],
  }));
  const model = liveAccountMetricModel(
    accounts,
    "margin_occupancy_ratio",
    360,
    "2026-08-16T00:00:00Z",
    "2026-08-16T00:06:00Z",
  );
  assert.equal(model.series.length, 4);
  assert.equal(model.points.length, 2);
  assert.equal(model.valueFormat, "percent");
  const marginModel = liveAccountMetricModel(accounts, "margin_used", 360);
  assert.equal(marginModel.min, 0);
  const occupancyModel = liveAccountMetricModel(
    accounts,
    "margin_occupancy_ratio",
    360,
  );
  assert.equal(occupancyModel.min, 0);
  const html = liveAccountMetricChart(
    accounts,
    "drawdown_ratio",
    "live-account-metric-drawdown-ratio-test",
    "回撤比例对比",
    "回撤比例对比，四个实盘账户",
    360,
  );
  const payload = getChartPayload("live-account-metric-drawdown-ratio-test");
  assert.match(html, /data-echart-kind="metric-comparison"/);
  assert.equal(payload.series.length, 4);
  assert.equal(payload.valueFormat, "signed-percent");
  const option = buildChartOption(payload);
  assert.equal(option.yAxis.axisLabel.formatter(0.01), "+1.00%");
  assert.equal(option.series.length, 4);
});

test("live account equity amount changes align each account to its first point", () => {
  const model = liveAccountMetricModel(
    [
      {
        account_label: "primary",
        metrics_curve: [
          {
            observed_at: "2026-08-16T00:00:00Z",
            equity: "1000",
          },
          {
            observed_at: "2026-08-16T00:06:00Z",
            equity: "1010",
          },
        ],
      },
      {
        account_label: "account-2",
        metrics_curve: [
          {
            observed_at: "2026-08-16T00:00:00Z",
            equity: "2000",
          },
          {
            observed_at: "2026-08-16T00:06:00Z",
            equity: "1980",
          },
        ],
      },
    ],
    "equity",
    360,
    "2026-08-16T00:00:00Z",
    "2026-08-16T00:06:00Z",
  );

  assert.equal(model.valueFormat, "signed-money");
  assert.deepEqual(
    model.series.map((series) => series.values),
    [[0, 10], [0, -20]],
  );
  assert.equal(model.points[0].values.every((value) => value === 0), true);
});

test("live account metric charts start at the first daily 08:00 UTC+8 bucket", () => {
  const accounts = ["primary", "account-2"].map((accountLabel, index) => ({
    account_label: accountLabel,
    metrics_curve: [
      {
        observed_at: "2026-08-15T01:06:00Z",
        equity: String(900 + index),
      },
      {
        observed_at: "2026-08-16T00:00:00Z",
        equity: String(1000 + index),
      },
      {
        observed_at: "2026-08-16T00:06:00Z",
        equity: String(1010 + index),
      },
    ],
  }));

  const model = liveAccountMetricModel(
    accounts,
    "equity",
    6 * 60,
    "2026-08-15T01:00:00Z",
    "2026-08-16T01:00:00Z",
  );

  assert.equal(model.domainStart, Date.parse("2026-08-16T00:00:00Z"));
  assert.equal(model.anchorAt, Date.parse("2026-08-16T00:00:00Z"));
  assert.equal(model.anchorMode, "daily-anchor");
  assert.equal(model.points[0].at, Date.parse("2026-08-16T00:00:00Z"));
  assert.deepEqual(model.series.map((series) => series.values), [
    [0, 10],
    [0, 10],
  ]);
});

test("live account renderer separates sync service from account configuration", () => {
  const [status, html] = renderAccount({
    status: "READY",
    observed_at: new Date().toISOString(),
    environment: "live",
    account_label: "primary",
    account_config: { hedge_mode: false, multi_assets_mode: false, fee_tier: 0 },
    reconciliation: {
      status: "ready",
      mismatch_count: 0,
      balance_count: 1,
      position_count: 0,
      open_order_count: 0,
      fill_count: 0,
    },
    summary: {
      usdt_wallet_balance: "282.28",
      usdt_available_balance: "257.84",
      total_unrealized_pnl: "0",
      gross_position_notional: "0",
      position_count: 0,
      open_order_count: 0,
      recent_trade_count: 0,
    },
    balances: [{ asset: "USDT", wallet_balance: "282.28", available_balance: "257.84", unrealized_pnl: "0" }],
    equity_curve: [
      { observed_at: "2026-08-16T00:00:00Z", equity: "280" },
      { observed_at: "2026-08-16T00:06:00Z", equity: "282" },
    ],
    equity_window_start: "2026-08-15T00:00:00Z",
    equity_window_end: "2026-08-16T00:00:00Z",
    equity_sample_interval_seconds: 360,
    positions: [],
    open_orders: [],
    fills: [],
  });
  assert.equal(status, "READY");
  assert.match(html, /execution-account · 只读同步/);
  assert.match(html, /账户配置/);
  assert.match(html, /Binance V3 账户配置快照/);
  assert.match(html, /live-strategy/);
  assert.match(html, /对账一致/);
  assert.match(html, /数据新鲜度/);
  assert.doesNotMatch(html, /READ-ONLY ACCOUNT SYNC/);
});

test("performance renderer exposes decision SLO, checkpoint phase, and host metrics", () => {
  const [status, html] = renderPerformance({
    status: "READY",
    decision_slo: {
      persisted_event_count: 142,
      window: "24h",
      phase_latency: {
        "candidate_accepted->risk_approved": {
          sample_count: 142,
          p50_ms: 1.2,
          p95_ms: 3.4,
          max_ms: 5.0,
        },
      },
    },
    persistence: {
      status: "READY",
      p50_total_ms: 4.2,
      p95_total_ms: 11.8,
      max_total_ms: 18.0,
      sample_count: 40,
      stagger_slots: [
        {
          account_id: "primary",
          phase_seconds: 0,
          target_second: ":00",
          status: "READY",
          last_checkpoint_at: "2026-09-18T00:00:00Z",
          avg_total_ms: 4.2,
          p95_total_ms: 12.0,
        },
      ],
      recent_checkpoints: [
        {
          account_id: "primary",
          run_id: "primary",
          phase_seconds: 0,
          occurred_at: "2026-09-18T00:00:00Z",
          prepare_ms: 0.1,
          event_loop_lag_ms: 0.2,
          pool_acquire_ms: 0.8,
          sql_execute_ms: 3.5,
          total_ms: 4.5,
          is_new_connection: false,
        },
      ],
    },
    market_data: {
      status: "READY",
      market_delay_ms: 45.0,
      realtime_closure_delay_seconds: 0.4,
      last_bucket_end: "2026-09-18T00:00:00Z",
      dropped_batches_1h: 0,
      missing_rows_1h: 0,
    },
    host_resources: {
      status: "READY",
      cpu_load_1m: 0.45,
      cpu_load_5m: 0.52,
      cpu_load_15m: 0.48,
      mem_total_bytes: 8589934592,
      mem_used_bytes: 4294967296,
      mem_available_bytes: 4294967296,
      mem_usage_percent: 50.0,
      swap_total_bytes: 2147483648,
      swap_used_bytes: 0,
      swap_usage_percent: 0.0,
      postgres_active_connections: 5,
      postgres_idle_connections: 12,
      postgres_database_size_bytes: 159383552,
    },
  });

  assert.equal(status, "READY");
  assert.match(html, /决策链路最高 P95/);
  assert.match(html, /Checkpoint P95 耗时/);
  assert.match(html, /多账户 Checkpoint 物理时钟相位错峰/);
  assert.match(html, /闭桶水位 400ms/);
  assert.match(html, /primary/);
  assert.match(html, /152.0 MiB/);
});

test("account equity and pnl ticks do not change structural render key", () => {
  const first = {
    status: "READY",
    accounts: [{
      account_label: "primary",
      status: "READY",
      total_equity: "1000.00",
      unrealized_pnl: "10.00",
      observed_at: "2026-09-27T08:00:00Z",
    }],
  };
  const second = {
    status: "READY",
    accounts: [{
      account_label: "primary",
      status: "READY",
      total_equity: "1005.50",
      unrealized_pnl: "15.50",
      observed_at: "2026-09-27T08:00:15Z",
    }],
  };
  assert.equal(sectionRenderKey("account", first), sectionRenderKey("account", second));
});

test("restoreViewState keeps the current mock scroll when no semantic anchor survives", () => {
  const mockDoc = {
    scrollingElement: { scrollLeft: 0, scrollTop: 250, scrollHeight: 2000 },
    documentElement: { style: {} },
    body: { scrollLeft: 0, scrollTop: 250 },
    defaultView: {
      scrollX: 0,
      scrollY: 250,
      innerHeight: 800,
      scrollTo(opts) {
        if (typeof opts === "object") {
          mockDoc.scrollingElement.scrollTop = opts.top;
        }
      },
    },
    querySelectorAll: () => [],
  };
  const root = {
    ownerDocument: mockDoc,
    querySelectorAll: () => [],
  };

  const state = captureViewState(root);
  assert.equal(state.pageY, 250);
  assert.ok(state.capturedAt > 0);

  // Without a semantic anchor, restoration must not replay a stale scrollY.
  mockDoc.scrollingElement.scrollTop = 123;
  mockDoc.defaultView.scrollY = 123;
  restoreViewState(root, state);
  assert.equal(mockDoc.scrollingElement.scrollTop, 123);
});

test("replaceChildrenFromHtml restores minHeight on independent roots across rAF", () => {
  const rafCallbacks = [];
  const mockDoc = {
    createElement(tag) {
      if (tag === "template") {
        return {
          set innerHTML(val) { this._html = val; },
          get content() { return { childNodes: [] }; },
        };
      }
      return {};
    },
    scrollingElement: { scrollLeft: 0, scrollTop: 0 },
    documentElement: { style: {} },
    body: { scrollLeft: 0, scrollTop: 0 },
    defaultView: {
      scrollX: 0,
      scrollY: 0,
      innerHeight: 800,
      requestAnimationFrame(cb) {
        rafCallbacks.push(cb);
      },
    },
    querySelectorAll: () => [],
  };

  const rootA = {
    ownerDocument: mockDoc,
    style: { minHeight: "10px" },
    offsetHeight: 500,
    replaceChildren() {},
    querySelectorAll: () => [],
  };

  const rootB = {
    ownerDocument: mockDoc,
    style: { minHeight: "20px" },
    offsetHeight: 300,
    replaceChildren() {},
    querySelectorAll: () => [],
  };

  replaceChildrenFromHtml(rootA, "<div>A</div>");
  assert.equal(rootA.style.minHeight, "500px");

  replaceChildrenFromHtml(rootB, "<div>B</div>");
  assert.equal(rootB.style.minHeight, "300px");

  // Fire rAF callbacks in sequence
  while (rafCallbacks.length > 0) {
    const cb = rafCallbacks.shift();
    cb();
  }

  // Both should have their respective previous minHeights restored!
  assert.equal(rootA.style.minHeight, "10px");
  assert.equal(rootB.style.minHeight, "20px");
});

test("restoreViewState recovers saved pageY when browser collapsed scroll to 0", () => {
  const mockDoc = {
    scrollingElement: { scrollLeft: 0, scrollTop: 0, scrollHeight: 2000 },
    documentElement: { style: {} },
    body: { scrollLeft: 0, scrollTop: 0 },
    defaultView: {
      scrollX: 0,
      scrollY: 0,
      innerHeight: 800,
      scrollTo(opts) {
        if (typeof opts === "object") {
          mockDoc.scrollingElement.scrollTop = opts.top;
        }
      },
    },
    querySelectorAll: () => [],
  };
  const root = {
    ownerDocument: mockDoc,
    querySelectorAll: () => [],
  };

  const state = {
    pageX: 0,
    pageY: 600,
    anchor: {
      selector: "#overview",
      topOffset: 0,
    },
  };
  mockDoc.querySelector = (sel) => {
    if (sel === "#overview") {
      return {
        offsetParent: mockDoc.body,
        getBoundingClientRect: () => ({ top: 0, width: 100, height: 100 }),
      };
    }
    return null;
  };

  restoreViewState(root, state);
  assert.equal(mockDoc.scrollingElement.scrollTop, 600);
});

test("restoreViewState defends against anchor diff collapsing scroll to top when user is scrolled down", () => {
  const mockDoc = {
    scrollingElement: { scrollLeft: 0, scrollTop: 0, scrollHeight: 2500 },
    documentElement: { style: {} },
    body: { scrollLeft: 0, scrollTop: 0 },
    defaultView: {
      scrollX: 0,
      scrollY: 0,
      innerHeight: 800,
      scrollTo(opts) {
        if (typeof opts === "object") {
          mockDoc.scrollingElement.scrollTop = opts.top;
        }
      },
    },
    querySelectorAll: () => [],
  };
  const root = {
    ownerDocument: mockDoc,
    querySelectorAll: () => [],
  };

  const state = {
    pageX: 0,
    pageY: 850,
    anchor: {
      stateKey: "near-top-element",
      topOffset: 5,
    },
  };
  mockDoc.querySelector = (sel) => {
    if (sel === '[data-state-key="near-top-element"]') {
      return {
        offsetParent: mockDoc.body,
        getBoundingClientRect: () => ({ top: 20, width: 100, height: 40 }),
      };
    }
    return null;
  };

  restoreViewState(root, state);
  // candidateY was 0 + 15 = 15 <= 20, defense correctly kept state.pageY (850)!
  assert.equal(mockDoc.scrollingElement.scrollTop, 850);

  // Now simulate an anchor that collapsed towards 0 (candidateY <= 20)
  mockDoc.scrollingElement.scrollTop = 0;
  mockDoc.querySelector = (sel) => {
    if (sel === '[data-state-key="near-top-element"]') {
      return {
        offsetParent: mockDoc.body,
        getBoundingClientRect: () => ({ top: 5, width: 100, height: 40 }),
      };
    }
    return null;
  };
  state.anchor.topOffset = 840; // diff = -835 -> candidateY = 15 <= 20
  restoreViewState(root, state);
  assert.equal(mockDoc.scrollingElement.scrollTop, 850);
});

test("captureViewState ignores static buttons and cards for focusIdentity", () => {
  const button = {
    tagName: "BUTTON",
    dataset: { liveAccountLabel: "primary", accountIndex: "0" },
    id: "account-btn",
  };
  const mockDoc = {
    activeElement: button,
    scrollingElement: { scrollLeft: 0, scrollTop: 100 },
    defaultView: { scrollX: 0, scrollY: 100, innerHeight: 800 },
    querySelectorAll: () => [],
  };
  const root = {
    ownerDocument: mockDoc,
    contains: (el) => el === button,
    querySelectorAll: () => [],
  };

  const state = captureViewState(root);
  assert.equal(state.focusIdentity, null);
});

function mockScrollDoc({ scrollY = 0, scrollHeight = 2500, innerHeight = 800 } = {}) {
  const mockDoc = {
    scrollingElement: { scrollLeft: 0, scrollTop: scrollY, scrollHeight },
    documentElement: { style: {} },
    body: { scrollLeft: 0, scrollTop: scrollY },
    defaultView: {
      scrollX: 0,
      scrollY,
      innerHeight,
      scrollTo(opts) {
        const top = typeof opts === "object" ? opts.top : arguments[1];
        mockDoc.scrollingElement.scrollTop = top;
        mockDoc.defaultView.scrollY = top;
      },
    },
    querySelectorAll: () => [],
  };
  return mockDoc;
}

test("createScrollGuard does not drag the page forward when scroll moved up", () => {
  // 800 → 400 can be the user scrolling up or native anchoring after content
  // above shrank. Snapping back to 800 is the "jump forward" bug.
  const previousWindow = globalThis.window;
  const mockDoc = mockScrollDoc({ scrollY: 800, scrollHeight: 3000 });
  globalThis.window = mockDoc.defaultView;
  globalThis.window.document = mockDoc;
  try {
    const guard = createScrollGuard();
    mockDoc.scrollingElement.scrollTop = 400;
    mockDoc.defaultView.scrollY = 400;
    const restored = guard.restore();
    assert.equal(restored, 400);
    assert.equal(mockDoc.scrollingElement.scrollTop, 400);
  } finally {
    if (previousWindow === undefined) delete globalThis.window;
    else globalThis.window = previousWindow;
  }
});

test("createScrollGuard restores only when the page collapsed to the top", () => {
  const previousWindow = globalThis.window;
  const mockDoc = mockScrollDoc({ scrollY: 800, scrollHeight: 3000 });
  globalThis.window = mockDoc.defaultView;
  globalThis.window.document = mockDoc;
  try {
    const guard = createScrollGuard();
    // Simulate DOM churn collapsing the document and clamping scroll to top.
    mockDoc.scrollingElement.scrollTop = 0;
    mockDoc.defaultView.scrollY = 0;
    const restored = guard.restore();
    assert.equal(restored, 800);
    assert.equal(mockDoc.scrollingElement.scrollTop, 800);
  } finally {
    if (previousWindow === undefined) delete globalThis.window;
    else globalThis.window = previousWindow;
  }
});

test("createScrollGuard can force-restore even while a scroll gesture is active", () => {
  const previousWindow = globalThis.window;
  const mockDoc = mockScrollDoc({ scrollY: 800, scrollHeight: 3000 });
  globalThis.window = mockDoc.defaultView;
  globalThis.window.document = mockDoc;
  try {
    const guard = createScrollGuard();
    mockDoc.scrollingElement.scrollTop = 0;
    mockDoc.defaultView.scrollY = 0;
    guard.restore({ force: true });
    assert.equal(mockDoc.scrollingElement.scrollTop, 800);
    // Idle module state: no scroll gesture has been recorded in this process.
    assert.equal(isUserScrolling(Date.now() + 60_000), false);
  } finally {
    if (previousWindow === undefined) delete globalThis.window;
    else globalThis.window = previousWindow;
  }
});



