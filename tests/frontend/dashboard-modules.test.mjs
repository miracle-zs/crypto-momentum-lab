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
  standaloneSparkline,
  strategyEquityChart,
} from "../../src/crypto_momentum_lab/operator_dashboard/static/dashboard-charts.js";
import { renderOverview } from "../../src/crypto_momentum_lab/operator_dashboard/static/sections/overview.js";
import { renderRisk } from "../../src/crypto_momentum_lab/operator_dashboard/static/sections/risk.js";
import { createStrategySection } from "../../src/crypto_momentum_lab/operator_dashboard/static/sections/strategy.js";

test("operator formatters keep status and money output stable", () => {
  assert.equal(esc('<live status="READY">'), "&lt;live status=&quot;READY&quot;&gt;");
  assert.equal(statusClass("live"), "status-LIVE");
  assert.equal(money(12.5), "$12.50");
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

test("equity charts expose native point readouts", () => {
  const rows = [
    { observed_at: "2026-08-16T00:00:00Z", equity: 1000 },
    { observed_at: "2026-08-16T00:06:00Z", equity: 1002 },
  ];
  assert.match(equityChart(rows), /class="chart-point pos"/);
  assert.match(equityChart(rows), /权益 · 08-16 08:00:00 UTC\+8/);

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
  assert.match(strategyEquityChart(model), /class="pair-point candle"/);
  assert.match(strategyEquityChart(model), /class="pair-point live"/);
});
