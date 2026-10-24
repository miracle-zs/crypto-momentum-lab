import test from "node:test";
import assert from "node:assert/strict";
import { liveModeForService } from "../../src/crypto_momentum_lab/operator_dashboard/static/app/runtime-badge.js";
import { globalReadinessModel } from "../../src/crypto_momentum_lab/operator_dashboard/static/app/readiness.js";
import { latestSectionData, latestSectionUpdatedAt, markSectionStored, setLiveMode } from "../../src/crypto_momentum_lab/operator_dashboard/static/app/section-state.js";
import { renderAccount } from "../../src/crypto_momentum_lab/operator_dashboard/static/sections/account/render-detail.js";

test("fresh heartbeat cannot hide degraded NextGen trading", () => {
  const service = (status, freshness = "FRESH") => ({ status: freshness, details: { engine: "nextgen", status } });
  assert.equal(liveModeForService(service("READY"), 0), "LIVE");
  assert.equal(liveModeForService(service("DEGRADED"), 0), "DEGRADED");
  assert.equal(liveModeForService(service("HALTED"), 0), "HALTED");
  assert.equal(liveModeForService(service("READY", "STALE"), 0), "UNKNOWN");
  assert.equal(liveModeForService(service("READY"), 1), "HALTED");
});

test("account readiness reflects current runtime evidence", () => {
  try {
    markSectionStored("overview", { database_status: "READY", services: [] });
    markSectionStored("risk", { status: "READY", active_halts: [], ambiguous_orders: [] });
    markSectionStored("account", { status: "READY", accounts: [
      { account_label: "primary", status: "READY", readiness: "READY" },
      { account_label: "account-2", status: "DEGRADED", readiness: "DEGRADED" },
    ] });
    setLiveMode("LIVE");
    assert.equal(globalReadinessModel().accountsReady, "1/2");
    setLiveMode("DEGRADED");
    assert.equal(globalReadinessModel().status, "DEGRADED");
  } finally {
    latestSectionData.clear();
    latestSectionUpdatedAt.clear();
    setLiveMode("UNKNOWN");
  }
});

test("NextGen closing fill renders as an exit even without exchange reduceOnly", () => {
  const [, html] = renderAccount({
    status: "READY", observed_at: new Date().toISOString(),
    account_label: "primary", environment: "live", account_config: {engine: "nextgen"},
    summary: {}, positions: [], open_orders: [], balances: [], equity_curve: [],
    fills: [{symbol: "MANTRAUSDT", side: "SELL", is_exit: true, quantity: "1", price: "1"}],
  });
  assert.match(html, /退出路径未记录/);
});
