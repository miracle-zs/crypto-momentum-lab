import assert from "node:assert/strict";
import test from "node:test";

import {
  esc,
  money,
  statusClass,
} from "../../src/crypto_momentum_lab/operator_dashboard/static/dashboard-formatters.js";
import {
  buildStrategyEquityModels,
  equityChart,
  equityWindowMetrics,
  maxDrawdown,
  standaloneSparkline,
  strategyEquityChart,
} from "../../src/crypto_momentum_lab/operator_dashboard/static/dashboard-charts.js";
import {
  buildChartOption,
  getChartPayload,
} from "../../src/crypto_momentum_lab/operator_dashboard/static/dashboard-chart-engine.js";
import { readinessStatusForSection } from "../../src/crypto_momentum_lab/operator_dashboard/static/dashboard-readiness.js";
import { renderOverview } from "../../src/crypto_momentum_lab/operator_dashboard/static/sections/overview.js";
import { renderRisk } from "../../src/crypto_momentum_lab/operator_dashboard/static/sections/risk.js";
import { renderAccount } from "../../src/crypto_momentum_lab/operator_dashboard/static/sections/account.js";
import { createStrategySection } from "../../src/crypto_momentum_lab/operator_dashboard/static/sections/strategy.js";
import { renderUniverse } from "../../src/crypto_momentum_lab/operator_dashboard/static/sections/universe.js";

test("operator formatters keep status and money output stable", () => {
  assert.equal(esc('<live status="READY">'), "&lt;live status=&quot;READY&quot;&gt;");
  assert.equal(statusClass("live"), "status-LIVE");
  assert.equal(money(12.5), "$12.50");
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
  assert.deepEqual(model.series.map((series) => series.values), [[0, 2, 5], [0, -1, 3]]);
  assert.equal(model.series[1].colorClass, "live");
  assert.equal(model.anchorAt, Date.parse("2026-08-16T00:00:00Z"));
  assert.equal(model.startAt, Date.parse("2026-08-16T00:00:00Z"));
  assert.equal(model.anchorMode, "daily-anchor");
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

test("live account renderer separates sync service from account permission", () => {
  const [status, html] = renderAccount({
    status: "READY",
    observed_at: new Date().toISOString(),
    environment: "live",
    account_label: "primary",
    account_config: { can_trade: true, hedge_mode: false, multi_assets_mode: false, fee_tier: 0 },
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
  assert.match(html, /不代表账户不可交易/);
  assert.match(html, /交易所权限/);
  assert.match(html, /可交易/);
  assert.match(html, /live-strategy/);
  assert.match(html, /对账一致/);
  assert.match(html, /数据新鲜度/);
  assert.doesNotMatch(html, /READ-ONLY ACCOUNT SYNC/);
});
