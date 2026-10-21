import assert from "node:assert/strict";
import test from "node:test";

import {
  asNumber,
  computeRoi,
  esc,
  money,
  statusClass,
} from "../../src/crypto_momentum_lab/operator_dashboard/static/dashboard-formatters.js";
import {
  equityChart,
  equityWindowMetrics,
  liveAccountMetricChart,
  liveAccountMetricModel,
  maxDrawdown,
  standaloneSparkline,
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
import { renderUniverse } from "../../src/crypto_momentum_lab/operator_dashboard/static/sections/universe.js";
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
    }],
  };
  const second = {
    ...first,
    accounts: [{
      ...first.accounts[0],
      observed_at: "2026-09-06T11:00:15Z",
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

test("account cache freshness changes do not rebuild the account section", () => {
  const base = {
    status: "READY",
    _cache_status: "STALE",
    accounts: [{
      account_label: "primary",
      environment: "live",
      strategy_name: "orderflow_impulse",
    }],
  };
  const refreshed = { ...base, _cache_status: "HIT" };

  assert.equal(sectionRenderKey("account", base), sectionRenderKey("account", refreshed));
});

test("overview account heartbeats do not rebuild the section", () => {
  const first = {
    status: "READY",
    services: [{ name: "market-data", status: "READY", age_seconds: 5, observed_at: "2026-09-27T05:00:00Z" }],
    account_statuses: [{ account_label: "primary", observed_at: "2026-09-27T05:00:00Z", status: "READY" }],
  };
  const second = {
    ...first,
    services: [{ name: "market-data", status: "READY", age_seconds: 20, observed_at: "2026-09-27T05:00:15Z" }],
    account_statuses: [{ account_label: "primary", observed_at: "2026-09-27T05:00:15Z", status: "READY" }],
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
    services: [{
      name: "strategy-runner",
      status: "LIVE",
      age_seconds: 5,
      observed_at: "2026-08-16T00:00:00Z",
    }],
  });
  assert.equal(status, "READY");
  assert.match(html, /data-service-age="strategy-runner"/);
  assert.match(html, /data-service-meter="strategy-runner"/);
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

test("chartPayloadSignature detects series.values and point mutations without object coercion", () => {
  const initialComparison = {
    kind: "metric-comparison",
    title: "Live Account Metric Comparison",
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


