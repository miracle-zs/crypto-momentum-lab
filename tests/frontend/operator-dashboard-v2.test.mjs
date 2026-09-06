import assert from "node:assert/strict";
import test from "node:test";

import {
  renderAccount,
  renderLiveAccountMetrics,
  renderLiveAccounts,
} from "../../src/crypto_momentum_lab/operator_dashboard/static/sections/account.js";
import { renderOverview } from "../../src/crypto_momentum_lab/operator_dashboard/static/sections/overview.js";
import { disclosure } from "../../src/crypto_momentum_lab/operator_dashboard/static/dashboard-ui.js";

const populatedAccount = {
  status: "READY",
  observed_at: "2026-08-26T08:00:00Z",
  environment: "live",
  account_label: "primary",
  account_config: { hedge_mode: false, multi_assets_mode: false, fee_tier: 0 },
  reconciliation: {
    status: "ready",
    mismatch_count: 0,
    balance_count: 1,
    position_count: 1,
    open_order_count: 1,
    fill_count: 1,
  },
  summary: {
    usdt_wallet_balance: "1250",
    usdt_available_balance: "980",
    total_unrealized_pnl: "12.5",
    gross_position_notional: "250",
    position_count: 1,
    open_order_count: 1,
    recent_trade_count: 1,
  },
  balances: [{ asset: "USDT", wallet_balance: "1250", available_balance: "980", unrealized_pnl: "12.5" }],
  positions: [{ symbol: "BTCUSDT", position_side: "LONG", strategy_name: "compression_breakout", position_amt: "0.01", entry_price: "60000", mark_price: "60125", leverage: 3, margin_type: "isolated", notional: "250", unrealized_pnl: "12.5" }],
  open_orders: [{ symbol: "BTCUSDT", strategy_name: "compression_breakout", side: "SELL", order_type: "LIMIT", price: "61000", executed_quantity: "0", original_quantity: "0.01", status: "ACKNOWLEDGED", reduce_only: true, observed_at: "2026-08-26T08:00:00Z" }],
  fills: [{ trade_at: "2026-08-26T07:55:00Z", symbol: "BTCUSDT", order_id: "order-123456789", strategy_name: "compression_breakout", side: "BUY", price: "60000", quantity: "0.01", fill_count: 1, realized_pnl: "0", fee: "0.24", fee_asset: "USDT", reduce_only: false }],
  live_signals: [{
    signal_id: "signal-1",
    strategy_name: "orderflow_impulse",
    strategy_version: "v0",
    config_hash: "config-hash",
    code_commit: "commit-hash",
    signal_kind: "strategy_signal",
    symbol: "BTCUSDT",
    side: "long",
    detected_at: "2026-08-26T07:54:30Z",
    recorded_at: "2026-08-26T07:54:31Z",
    reason: "orderflow_impulse",
    quote_volume_24h: "1234567",
    quote_volume_24h_quote_asset: "USDT",
    features: { impulse_return_pct: "0.01", notional_intensity: "2" },
    reference_prices: { midpoint: "60000" },
    filter_context: { entry_enabled: true, entry_long_only: true },
  }],
  equity_curve: [
    { observed_at: "2026-08-26T07:54:00Z", equity: "1237.5" },
    { observed_at: "2026-08-26T08:00:00Z", equity: "1250" },
  ],
  equity_window_start: "2026-08-25T08:00:00Z",
  equity_window_end: "2026-08-26T08:00:00Z",
  equity_sample_interval_seconds: 360,
};

test("account v2 keeps populated evidence behind stable disclosures", () => {
  const [status, html] = renderAccount(populatedAccount);

  assert.equal(status, "READY");
  assert.equal((html.match(/class="block secondary disclosure"/g) || []).length, 6);
  assert.match(html, /实盘策略信号/);
  assert.match(html, /24H 成交额/);
  assert.match(html, /过滤 \/ 门控/);
  assert.match(html, /data-state-key="live-strategy-signals" open/);
  assert.match(html, /data-state-key="account-balances"/);
  assert.match(html, /data-state-key="account-positions"/);
  assert.match(html, /data-state-key="account-open-orders"/);
  assert.match(html, /BTCUSDT/);
  assert.match(html, /USDT 钱包余额/);
  assert.doesNotMatch(html, /data-state-key="account-fills"[^>]* open/);
});

test("account v2 opens the reconciliation evidence when the posture needs review", () => {
  const [status, html] = renderAccount({
    ...populatedAccount,
    status: "UNKNOWN",
    reconciliation: { ...populatedAccount.reconciliation, status: "degraded", mismatch_count: 2 },
  });

  assert.equal(status, "UNKNOWN");
  assert.match(html, /data-state-key="account-reconciliation" open/);
});

test("live account fleet keeps four accounts visible and selects one detail", () => {
  const accounts = ["primary", "account-2", "account-3", "account-4"].map((label, index) => ({
    ...populatedAccount,
    account_label: label,
    summary: {
      ...populatedAccount.summary,
      usdt_wallet_balance: String(1000 + index * 100),
      usdt_available_balance: String(800 + index * 100),
    },
  }));
  const [status, html] = renderLiveAccounts({
    status: "READY",
    observed_at: populatedAccount.observed_at,
    accounts,
    account_count: 4,
    selected_account_label: "account-3",
  });

  assert.equal(status, "READY");
  assert.equal((html.match(/data-live-account-label=/g) || []).length, 4);
  assert.match(html, /四账户实盘总览/);
  assert.match(html, /实盘账户.*4 个/);
  assert.match(html, /class="live-account-card is-selected"[^>]*data-live-account-label="account-3"/);
  assert.match(html, /<h3>account-3<\/h3>/);
  assert.match(html, /USDT 钱包合计/);
});

test("live account fleet renders six comparable metric charts", () => {
  const labels = ["primary", "account-2", "account-3", "account-4"];
  const accounts = labels.map((account_label, index) => ({
    account_label,
    status: "READY",
    metrics_curve: [
      {
        observed_at: "2026-08-26T07:54:00Z",
        equity: String(1000 + index * 25),
        equity_change_ratio: "0",
        margin_used: String(100 + index * 5),
        margin_occupancy_ratio: "0.1",
        drawdown: "0",
        drawdown_ratio: "0",
      },
      {
        observed_at: "2026-08-26T08:00:00Z",
        equity: String(1010 + index * 25),
        equity_change_ratio: "0.01",
        margin_used: String(120 + index * 5),
        margin_occupancy_ratio: "0.12",
        drawdown: "-3",
        drawdown_ratio: "-0.003",
      },
    ],
  }));
  const html = renderLiveAccountMetrics({
    status: "READY",
    equity_range: "24h",
    equity_window_start: "2026-08-26T07:00:00Z",
    equity_window_end: "2026-08-26T08:00:00Z",
    equity_sample_interval_seconds: 360,
    accounts,
  });

  assert.match(html, /四账户资金与风险时序/);
  assert.equal((html.match(/class="live-metric-card"/g) || []).length, 6);
  assert.equal((html.match(/data-echart-kind="metric-comparison"/g) || []).length, 6);
  assert.match(html, /资金权益比例变化/);
  assert.match(html, /保证金占用比例对比/);
  assert.match(html, /回撤比例对比/);
});

test("overview surfaces the four live account statuses", () => {
  const [, html] = renderOverview({
    database_status: "READY",
    active_halt_count: 0,
    active_lease: null,
    services: [],
    account_statuses: ["primary", "account-2", "account-3", "account-4"].map((account_label) => ({
      account_label,
      status: "READY",
      readiness: "ready_readonly",
      strategy_name: "orderflow_impulse",
      strategy_state: "running",
      observed_at: "2026-09-06T08:00:00Z",
    })),
  });

  assert.match(html, /四账户实盘状态/);
  assert.equal((html.match(/class="overview-account-status-card"/g) || []).length, 4);
  assert.match(html, /href="#account"/);
});

test("disclosure helper preserves the default collapsed state", () => {
  assert.match(disclosure("证据", "EVIDENCE", "内容", "", { stateKey: "example" }), /data-state-key="example"/);
  assert.doesNotMatch(disclosure("证据", "EVIDENCE", "内容"), / open/);
  assert.match(disclosure("证据", "EVIDENCE", "内容", "", { open: true }), / open/);
});
