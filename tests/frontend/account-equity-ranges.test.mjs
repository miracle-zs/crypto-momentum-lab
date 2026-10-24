import assert from "node:assert/strict";
import test from "node:test";

import {
  buildChartOption,
  getChartPayload,
} from "../../src/crypto_momentum_lab/operator_dashboard/static/dashboard-chart-engine.js";
import { createPoller } from "../../src/crypto_momentum_lab/operator_dashboard/static/app/poller.js";
import { sectionRenderKey } from "../../src/crypto_momentum_lab/operator_dashboard/static/dashboard-rendering.js";
import {
  latestSectionData,
  latestSectionError,
  latestSectionUpdatedAt,
  sectionInFlight,
  sectionRenderKeys,
} from "../../src/crypto_momentum_lab/operator_dashboard/static/app/section-state.js";
import {
  renderAccount,
  renderLiveAccounts,
  updateLiveAccountsDynamic,
} from "../../src/crypto_momentum_lab/operator_dashboard/static/sections/account.js";
import {
  renderedPayloadMatches,
} from "../../src/crypto_momentum_lab/operator_dashboard/static/sections/account/loaders.js";


test("account payload cache matches the rendered account and exact payload", () => {
  const slot = {
    __cmlRenderedPayload: JSON.stringify({ value: 1 }),
    dataset: { renderedAccount: "primary" },
  };
  assert.equal(renderedPayloadMatches(slot, { value: 1 }, { accountLabel: "primary" }), true);
  assert.equal(renderedPayloadMatches(slot, { value: 2 }, { accountLabel: "primary" }), false);
  assert.equal(renderedPayloadMatches(slot, { value: 1 }, { accountLabel: "account-2" }), false);
  assert.equal(renderedPayloadMatches({ dataset: {} }, { value: 1 }), false);
  assert.equal(renderedPayloadMatches(slot, { value: 1 }), true);
});


test("live account renderer shows the online 24h window without archived range controls", () => {
  const [, html] = renderAccount({
    status: "READY",
    observed_at: "2026-08-24T00:00:00Z",
    equity_window_start: "2026-08-23T00:00:00Z",
    equity_window_end: "2026-08-24T00:00:00Z",
    equity_sample_interval_seconds: 360,
    equity_curve: [
      { observed_at: "2026-08-23T23:54:00Z", equity: "280" },
      { observed_at: "2026-08-24T00:00:00Z", equity: "282" },
    ],
    balances: [], positions: [], open_orders: [], fills: [],
  });

  assert.doesNotMatch(html, /data-account-equity-range/);
  assert.match(html, /ROLLING 24H/);
  assert.match(html, /最近 24 小时权益，曲线只展示已有采样/);
  const payload = getChartPayload("live-account-equity");
  const option = buildChartOption(payload);
  assert.equal(
    option.xAxis.axisLabel.formatter(Date.parse("2026-08-24T00:00:00Z")),
    "08:00:00",
  );
});

test("live account directory renders every account against shared market data", () => {
  const [status, html] = renderLiveAccounts({
    status: "READY",
    accounts: ["primary", "account-2", "account-3", "account-4"].map(
      (account_label) => ({
        account_label,
        environment: "live",
        status: "READY",
        readiness: "ready_readonly",
        strategy_name: "orderflow_impulse",
        strategy_state: "running",
      }),
    ),
  });

  assert.equal(status, "READY");
  assert.match(html, /4 个账户/);
  assert.match(html, /4 正常 · 0 停止 · 0 需检查/);
  for (const accountLabel of ["primary", "account-2", "account-3", "account-4"]) {
    assert.match(html, new RegExp(`>${accountLabel}<`));
  }
  assert.match(html, /data-live-account-detail/);
});

test("live account cards show current execution gates without lease status", () => {
  const [, html] = renderLiveAccounts({
    status: "READY",
    accounts: [
      {
        account_label: "primary",
        environment: "live",
        status: "READY",
        readiness: "ready_readonly",
        strategy_name: "orderflow_impulse",
        strategy_state: null,
        runtime_tradeability: { entry_gate_open: false, exit_gate_open: true },
      },
    ],
  });

  assert.match(html, /开仓 <b>暂停<\/b>/);
  assert.match(html, /退出 <b>可用<\/b>/);
  assert.doesNotMatch(html, /未关联策略/);
  assert.doesNotMatch(html, /租约/);
});

test("updateLiveAccountsDynamic updates execution cards and fleet status in-place", async () => {
  const cardElements = {
    status: { className: "", textContent: "" },
    state: { innerHTML: "" },
    detail: { innerHTML: "" },
  };
  const fleetElements = {
    status: { innerHTML: "" },
  };
  const mockCard = {
    querySelector(selector) {
      if (selector === ".live-account-card-status") return cardElements.status;
      if (selector === ".live-account-card-state") return cardElements.state;
      if (selector === ".live-account-card-state-detail") return cardElements.detail;
      return null;
    },
  };
  const mockRoot = {
    querySelector(selector) {
      if (selector === '[data-live-account-label="primary"]') return mockCard;
      if (selector === ".live-account-fleet-status") return fleetElements.status;
      return null;
    },
  };

  await updateLiveAccountsDynamic(mockRoot, {
    status: "READY",
    accounts: [
      {
        account_label: "primary",
        status: "READY",
        strategy_name: "orderflow_impulse",
        strategy_state: "running",
        startup_baseline: { status: "MATCHED" },
        runtime_tradeability: { entry_gate_open: true, exit_gate_open: true },
        observed_at: "2026-09-27T10:00:00Z",
        summary: {
          usdt_wallet_balance: "1500.50",
          usdt_available_balance: "1200.00",
          total_unrealized_pnl: "45.20",
          gross_position_notional: "3000.00",
          position_count: 2,
          open_order_count: 1,
        },
      },
    ],
  });

  assert.equal(cardElements.status.className, "live-account-card-status status-READY");
  assert.equal(cardElements.status.textContent, "正常");
  assert.match(cardElements.state.innerHTML, /开仓 <b>可用<\/b>/);
  assert.match(cardElements.state.innerHTML, /退出 <b>可用<\/b>/);
  assert.match(cardElements.detail.innerHTML, /同步/);
  assert.doesNotMatch(cardElements.detail.innerHTML, /启动基线待确认/);
  assert.match(fleetElements.status.innerHTML, /1 正常 · 0 停止 · 0 需检查/);
});

test("same-structure live-account polls refresh selected details and fleet metrics without dropping last-good DOM", async () => {
  const requestedUrls = [];
  const makeSlot = (dataset) => ({
    dataset,
    children: [{}],
    isConnected: true,
    style: { minHeight: "" },
    offsetHeight: 200,
    replacements: 0,
    ownerDocument: {
      scrollingElement: { scrollLeft: 0, scrollTop: 0 },
      documentElement: { style: {} },
      body: { scrollLeft: 0, scrollTop: 0 },
      querySelectorAll: () => [],
    },
    querySelectorAll: () => [],
    querySelector: () => null,
    replaceChildren() { this.replacements += 1; },
    setAttribute() {},
    removeAttribute() {},
  });
  const detailSlot = makeSlot({ accountLabel: "primary", renderedAccount: "primary" });
  const metricsSlot = makeSlot({});
  metricsSlot.__cmlRenderedPayload = JSON.stringify({ status: "READY", accounts: [] });
  const card = {
    dataset: { liveAccountLabel: "primary" },
    classList: { toggle() {} },
    querySelector: () => null,
    setAttribute() {},
  };
  const fleetStatus = { innerHTML: "" };
  const mockRoot = {
    querySelector(selector) {
      if (selector === '[data-live-account-label="primary"]') return card;
      if (selector === ".live-account-fleet-status") return fleetStatus;
      if (selector === "[data-live-account-detail]") return detailSlot;
      if (selector === "[data-live-account-metrics]") return metricsSlot;
      return null;
    },
    querySelectorAll(selector) {
      return selector === "[data-live-account-label]" ? [card] : [];
    },
    __requestJson: async (url) => {
      requestedUrls.push(url);
      throw new Error("offline during refresh");
    },
  };

  await updateLiveAccountsDynamic(mockRoot, {
    status: "READY",
    accounts: [{
      account_label: "primary",
      status: "READY",
      readiness: "ready_readonly",
      observed_at: "2026-09-27T10:00:00Z",
    }],
  });

  assert.deepEqual(requestedUrls, [
    "api/account?account_label=primary",
    "api/live-account-metrics",
  ]);
  assert.equal(detailSlot.replacements, 0, "a transient detail error must retain the last successful detail DOM");
  assert.equal(metricsSlot.replacements, 0, "a transient metrics error must retain the last successful charts");
});

test("live account detail settles before fleet metrics starts a second scroll-sensitive patch", async () => {
  const requestedUrls = [];
  let releaseDetail;
  const makeSlot = (dataset) => ({
    dataset,
    children: [{}],
    isConnected: true,
    style: { minHeight: "" },
    offsetHeight: 200,
    ownerDocument: {
      scrollingElement: { scrollLeft: 0, scrollTop: 0 },
      documentElement: { style: {} },
      body: { scrollLeft: 0, scrollTop: 0 },
      querySelectorAll: () => [],
    },
    querySelectorAll: () => [],
    querySelector: () => null,
    replaceChildren() {},
    setAttribute() {},
    removeAttribute() {},
  });
  const detailSlot = makeSlot({ accountLabel: "primary", renderedAccount: "primary" });
  const metricsSlot = makeSlot({});
  metricsSlot.__cmlRenderedPayload = JSON.stringify({ status: "READY", accounts: [] });
  const card = {
    dataset: { liveAccountLabel: "primary" },
    classList: { toggle() {} },
    querySelector: () => null,
    setAttribute() {},
  };
  const mockRoot = {
    querySelector(selector) {
      if (selector === '[data-live-account-label="primary"]') return card;
      if (selector === ".live-account-fleet-status") return { innerHTML: "" };
      if (selector === "[data-live-account-detail]") return detailSlot;
      if (selector === "[data-live-account-metrics]") return metricsSlot;
      return null;
    },
    querySelectorAll(selector) {
      return selector === "[data-live-account-label]" ? [card] : [];
    },
    __requestJson(url) {
      requestedUrls.push(url);
      if (url.startsWith("api/account?")) {
        return new Promise((resolve) => { releaseDetail = resolve; })
          .then(() => { throw new Error("synthetic detail refresh failure"); });
      }
      throw new Error("synthetic metrics refresh failure");
    },
  };

  const refresh = updateLiveAccountsDynamic(mockRoot, {
    status: "READY",
    accounts: [{ account_label: "primary", status: "READY" }],
  });
  while (!releaseDetail) {
    await new Promise((resolve) => setImmediate(resolve));
  }
  assert.deepEqual(requestedUrls, ["api/account?account_label=primary"]);

  releaseDetail();
  await refresh;
  assert.deepEqual(requestedUrls, [
    "api/account?account_label=primary",
    "api/live-account-metrics",
  ]);
});

test("poller waits for account dynamic refresh before ending the poll transaction", async () => {
  const data = {
    status: "READY",
    _cache_status: null,
    accounts: [{
      account_label: "primary",
      environment: "live",
      strategy_name: "orderflow_impulse",
    }],
  };
  const previousDocument = globalThis.document;
  const previousWindow = globalThis.window;
  let dynamicStarted = false;
  let releaseDynamicRefresh;
  const dynamicRefresh = new Promise((resolve) => {
    releaseDynamicRefresh = resolve;
  });
  const body = {
    classList: { remove() {} },
    removeAttribute() {},
    querySelector: () => null,
  };
  const section = {
    dataset: { endpoint: "api/live-accounts" },
    querySelector: (selector) => (selector === ".panel-body" ? body : null),
  };
  globalThis.window = { location: { href: "http://dashboard.test/" } };
  globalThis.document = {
    body: { dataset: {} },
    getElementById: (id) => (id === "account" ? section : null),
    querySelector: () => null,
  };
  sectionInFlight.clear();
  sectionRenderKeys.clear();
  latestSectionData.clear();
  latestSectionError.clear();
  latestSectionUpdatedAt.clear();
  sectionRenderKeys.set("account", sectionRenderKey("account", data));
  const previousFetch = globalThis.fetch;
  globalThis.fetch = async () => ({
    ok: true,
    headers: { get: () => null },
    json: async () => data,
  });

  try {
    const poller = createPoller({
      renderers: { account: () => ["READY", ""] },
      onAfterRender: {
        account: {
          updateDynamic: () => {
            dynamicStarted = true;
            return dynamicRefresh;
          },
        },
      },
    });
    let completed = false;
    const refresh = poller.refreshSection("account").then(() => {
      completed = true;
    });
    while (!dynamicStarted) {
      await new Promise((resolve) => setImmediate(resolve));
    }

    assert.equal(dynamicStarted, true);
    assert.equal(completed, false, "poll transaction ended before account DOM refresh");
    releaseDynamicRefresh();
    await refresh;
    assert.equal(completed, true);
  } finally {
    if (previousDocument === undefined) delete globalThis.document;
    else globalThis.document = previousDocument;
    if (previousWindow === undefined) delete globalThis.window;
    else globalThis.window = previousWindow;
    if (previousFetch === undefined) delete globalThis.fetch;
    else globalThis.fetch = previousFetch;
    sectionInFlight.clear();
    sectionRenderKeys.clear();
    latestSectionData.clear();
    latestSectionError.clear();
    latestSectionUpdatedAt.clear();
  }
});
