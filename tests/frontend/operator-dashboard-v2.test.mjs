import assert from "node:assert/strict";
import test from "node:test";

import { renderAccount } from "../../src/crypto_momentum_lab/operator_dashboard/static/sections/account.js";
import { disclosure } from "../../src/crypto_momentum_lab/operator_dashboard/static/dashboard-ui.js";

const populatedAccount = {
  status: "READY",
  observed_at: "2026-08-26T08:00:00Z",
  environment: "live",
  account_label: "primary",
  account_config: { can_trade: true, hedge_mode: false, multi_assets_mode: false, fee_tier: 0 },
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
  assert.equal((html.match(/class="block secondary disclosure"/g) || []).length, 5);
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

test("disclosure helper preserves the default collapsed state", () => {
  assert.match(disclosure("证据", "EVIDENCE", "内容", "", { stateKey: "example" }), /data-state-key="example"/);
  assert.doesNotMatch(disclosure("证据", "EVIDENCE", "内容"), / open/);
  assert.match(disclosure("证据", "EVIDENCE", "内容", "", { open: true }), / open/);
});
