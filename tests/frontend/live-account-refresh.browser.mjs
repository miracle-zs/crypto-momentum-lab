import {
  captureViewState,
  replaceChildrenFromHtml,
} from "../../src/crypto_momentum_lab/operator_dashboard/static/dashboard-dom.js";
import { wireEcharts } from "../../src/crypto_momentum_lab/operator_dashboard/static/dashboard-chart-engine.js";
import {
  renderLiveAccounts,
  updateLiveAccountsDynamic,
  wireLiveAccounts,
} from "../../src/crypto_momentum_lab/operator_dashboard/static/sections/account.js";

const output = document.querySelector("#browser-results");
const result = (label, passed, detail = "") => {
  const item = document.createElement("li");
  item.className = passed ? "pass" : "fail";
  item.textContent = `${passed ? "PASS" : "FAIL"} · ${label}${detail ? ` — ${detail}` : ""}`;
  output.append(item);
};

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

async function waitFor(predicate, message, timeoutMs = 5000) {
  const start = performance.now();
  while (!predicate()) {
    if (performance.now() - start > timeoutMs) throw new Error(message);
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

const baseAt = Date.parse("2026-09-27T03:00:00Z");
const observedAt = (minute) => new Date(baseAt + minute * 60_000).toISOString();
let version = 1;
let offline = false;
const requests = [];

function accountSnapshot(range, snapshotVersion) {
  return {
    account_label: "primary",
    environment: "live",
    status: "READY",
    observed_at: observedAt(5),
    equity_range: range,
    equity_window_start: observedAt(0),
    equity_window_end: observedAt(5),
    equity_sample_interval_seconds: 60,
    account_config: { hedge_mode: true, multi_assets_mode: false, fee_tier: 1 },
    reconciliation: { status: "ready", mismatch_count: 0 },
    summary: {
      usdt_wallet_balance: 1000 + snapshotVersion,
      usdt_available_balance: 800 + snapshotVersion,
      total_unrealized_pnl: 10 + snapshotVersion,
      gross_position_notional: 3000 + snapshotVersion,
      position_count: snapshotVersion > 1 ? 2 : 1,
      open_order_count: 1,
      recent_trade_count: snapshotVersion,
    },
    balances: [{
      asset: "USDT",
      wallet_balance: 1000 + snapshotVersion,
      available_balance: 800 + snapshotVersion,
      unrealized_pnl: 10 + snapshotVersion,
    }],
    positions: [
      {
        symbol: "BTCUSDT",
        position_side: "BOTH",
        position_amt: 0.01 + snapshotVersion / 1000,
        entry_price: 60000,
        mark_price: 60100 + snapshotVersion,
        leverage: 3,
        margin_type: "cross",
        entry_notional: 600,
        notional: 601 + snapshotVersion,
        unrealized_pnl: 1 + snapshotVersion,
      },
      ...(snapshotVersion > 1 ? [{
        symbol: "ETHUSDT",
        position_side: "BOTH",
        position_amt: 0.2,
        entry_price: 3000,
        mark_price: 3010,
        leverage: 2,
        margin_type: "cross",
        entry_notional: 600,
        notional: 602,
        unrealized_pnl: 2,
      }] : []),
    ],
    open_orders: [{
      order_id: "order-1",
      symbol: "BTCUSDT",
      side: "BUY",
      order_type: "LIMIT",
      price: 59000 + snapshotVersion,
      executed_quantity: 0,
      original_quantity: 0.01,
      status: "NEW",
      reduce_only: false,
      observed_at: observedAt(5),
    }],
    fills: [
      {
        trade_id: "fill-1",
        order_id: "order-1",
        trade_at: observedAt(4),
        symbol: "BTCUSDT",
        side: "BUY",
        price: 59000,
        quantity: 0.01,
        realized_pnl: 0,
        fee: 0.01,
        fee_asset: "USDT",
      },
      ...(snapshotVersion > 1 ? [{
        trade_id: "fill-2",
        order_id: "order-2",
        trade_at: observedAt(5),
        symbol: "ETHUSDT",
        side: "SELL",
        price: 3020,
        quantity: 0.2,
        realized_pnl: 3,
        fee: 0.02,
        fee_asset: "USDT",
      }] : []),
    ],
    equity_curve: [
      { observed_at: observedAt(0), equity: 2000 + snapshotVersion },
      { observed_at: observedAt(2), equity: 2005 + snapshotVersion },
      { observed_at: observedAt(5), equity: 2010 + snapshotVersion },
    ],
  };
}

function metricsSnapshot(range, snapshotVersion) {
  return {
    equity_range: range,
    equity_window_start: observedAt(0),
    equity_window_end: observedAt(5),
    equity_sample_interval_seconds: 60,
    accounts: [{
      account_label: "primary",
      metrics_curve: [0, 2, 5].map((minute, index) => ({
        observed_at: observedAt(minute),
        equity: 2000 + snapshotVersion + index * 5,
        equity_change_ratio: index * 0.003 + snapshotVersion / 100_000,
        margin_used: 100 + snapshotVersion + index * 10,
        margin_occupancy_ratio: 0.1 + index * 0.01,
        drawdown: -index * (snapshotVersion + 1),
        drawdown_ratio: -index * 0.001,
      })),
    }],
  };
}

async function requestJson(path) {
  requests.push(path);
  if (offline) throw new Error("synthetic offline state");
  const url = new URL(path, window.location.href);
  const range = url.searchParams.get("equity_range") || "24h";
  if (url.pathname.endsWith("/api/account")) {
    return accountSnapshot(range, version);
  }
  if (url.pathname.endsWith("/api/live-account-metrics")) {
    return metricsSnapshot(range, version);
  }
  throw new Error(`Unexpected synthetic request: ${path}`);
}

function countRequests(pathPrefix, range) {
  return requests.filter((path) => (
    path.startsWith(pathPrefix) && path.includes(`equity_range=${range}`)
  )).length;
}

function run(label, action) {
  return Promise.resolve().then(action).then(
    (detail = "") => result(label, true, detail),
    (error) => result(label, false, error?.message || String(error)),
  );
}

async function exerciseAccountRefresh() {
  const root = document.querySelector("#scenario");
  const directoryData = {
    status: "READY",
    accounts: [{
      account_label: "primary",
      environment: "live",
      status: "READY",
      readiness: "ready_readonly",
      observed_at: observedAt(5),
    }],
  };
  const [, html] = renderLiveAccounts(directoryData);
  root.innerHTML = html;
  wireEcharts(document);
  wireLiveAccounts(root, directoryData, { requestJson });
  await waitFor(() => (
    root.querySelector('[data-live-account-detail][data-rendered-account="primary"]')
      && root.querySelector('[data-live-account-metrics][data-rendered-range="24h"]')
      && window.echarts.getInstanceByDom(root.querySelector(".echart-surface"))
  ), "initial account detail, metrics, and ECharts instance did not load");

  const detailSlot = root.querySelector("[data-live-account-detail-content]");
  const metricsSlot = root.querySelector("[data-live-account-metrics]");
  const btcRow = detailSlot.querySelector('[data-row-key="BTCUSDT:BOTH"]');
  const orderRow = detailSlot.querySelector('[data-row-key="order-1"]');
  const fillRow = detailSlot.querySelector('[data-row-key="fill-1"]');
  const accountRangeButton = detailSlot.querySelector('[data-account-equity-range="7d"]');
  const metricRangeButton = metricsSlot.querySelector('[data-live-account-metrics-range="7d"]');
  const positionsDetails = detailSlot.querySelector('[data-state-key="account-positions"]');
  const chartShell = metricsSlot.querySelector('[data-echart-id="live-account-metric-margin_used"]');
  const chartSurface = chartShell.querySelector(".echart-surface");
  const chartInstance = window.echarts.getInstanceByDom(chartSurface);
  const oldChartData = JSON.stringify(chartInstance.getOption().series[0].data);
  assert(btcRow && orderRow && fillRow && accountRangeButton && metricRangeButton && positionsDetails && chartInstance,
    "initial render omitted a keyed table row, range button, disclosure, or chart");

  positionsDetails.open = false;
  version = 2;
  const beforeTop = accountRangeButton.getBoundingClientRect().top;
  await updateLiveAccountsDynamic(root, directoryData);
  await waitFor(() => (
    detailSlot.querySelector('[data-row-key="BTCUSDT:BOTH"]')?.textContent.includes("60,102.00")
      && detailSlot.querySelector('[data-row-key="ETHUSDT:BOTH"]')
      && metricsSlot.querySelector("[data-refresh-state]")
  ), "same-structure poll did not update account detail and add new position");

  assert(root.querySelector("[data-live-account-detail-content]") === detailSlot,
    "account detail content wrapper was replaced during refresh");
  assert(detailSlot.querySelector('[data-row-key="BTCUSDT:BOTH"]') === btcRow,
    "stable BTC position row was replaced instead of updated");
  assert(detailSlot.querySelector('[data-row-key="order-1"]') === orderRow,
    "stable order row was replaced instead of updated");
  assert(detailSlot.querySelector('[data-row-key="fill-1"]') === fillRow,
    "stable fill row was replaced instead of updated");
  assert(detailSlot.querySelector('[data-row-key="ETHUSDT:BOTH"]'), "new ETH row was not inserted");
  assert(detailSlot.querySelector('[data-row-key="fill-2"]'), "new fill row was not inserted");
  assert(!positionsDetails.open, "same-root content morph reset a user-collapsed disclosure");
  assert(accountRangeButton.isConnected && metricsSlot.querySelector('[data-live-account-metrics-range="7d"]') === metricRangeButton,
    "range controls were replaced during refresh");
  assert(metricsSlot.querySelector('[data-echart-id="live-account-metric-margin_used"]') === chartShell,
    "chart shell was replaced during metrics refresh");
  assert(window.echarts.getInstanceByDom(chartSurface) === chartInstance,
    "existing ECharts instance was discarded during metrics refresh");
  const newChartData = JSON.stringify(chartInstance.getOption().series[0].data);
  assert(newChartData !== oldChartData, "existing chart instance retained stale series data");
  assert(Math.abs(accountRangeButton.getBoundingClientRect().top - beforeTop) <= 3,
    "same-root patch moved the focused reading position");
}

async function exerciseRangeListeners() {
  const root = document.querySelector("#scenario");
  const detailSlot = root.querySelector("[data-live-account-detail-content]");
  const metricsSlot = root.querySelector("[data-live-account-metrics]");
  const accountButton = detailSlot.querySelector('[data-account-equity-range="7d"]');
  const metricsButton = metricsSlot.querySelector('[data-live-account-metrics-range="7d"]');
  const accountBefore = countRequests("api/account?", "7d");
  accountButton.click();
  await waitFor(() => (
    countRequests("api/account?", "7d") === accountBefore + 1
      && !accountButton.disabled
      && root.querySelector('[data-live-account-detail][data-rendered-range="7d"]')
  ), "account 7d click did not complete exactly one selected-range request");
  assert(countRequests("api/account?", "7d") === accountBefore + 1,
    "one account range click issued duplicate HTTP requests");

  const metricsBefore = countRequests("api/live-account-metrics?", "7d");
  metricsButton.click();
  await waitFor(() => (
    countRequests("api/live-account-metrics?", "7d") === metricsBefore + 1
      && !metricsButton.disabled
      && root.querySelector('[data-live-account-metrics][data-rendered-range="7d"]')
  ), "metrics 7d click did not complete exactly one selected-range request");
  assert(countRequests("api/live-account-metrics?", "7d") === metricsBefore + 1,
    "one metrics range click issued duplicate HTTP requests");

  version = 3;
  const nextAccountBefore = countRequests("api/account?", "7d");
  const nextMetricsBefore = countRequests("api/live-account-metrics?", "7d");
  await updateLiveAccountsDynamic(root, {
    status: "READY",
    accounts: [{ account_label: "primary", status: "READY", observed_at: observedAt(5) }],
  });
  assert(countRequests("api/account?", "7d") === nextAccountBefore + 1,
    "same-structure poll did not preserve selected account equity range");
  assert(countRequests("api/live-account-metrics?", "7d") === nextMetricsBefore + 1,
    "same-structure poll did not preserve selected fleet-metrics range");
}

async function exerciseLastGoodRefresh() {
  const root = document.querySelector("#scenario");
  const detailSlot = root.querySelector("[data-live-account-detail-content]");
  const metricsSlot = root.querySelector("[data-live-account-metrics]");
  const btcRow = detailSlot.querySelector('[data-row-key="BTCUSDT:BOTH"]');
  const chartShell = metricsSlot.querySelector('[data-echart-id="live-account-metric-margin_used"]');
  assert(chartShell, "margin-used chart shell is missing after range refresh");
  const chartSurface = chartShell.querySelector(".echart-surface");
  assert(chartSurface, "margin-used chart surface is missing after range refresh");
  const chartInstance = window.echarts.getInstanceByDom(chartSurface);
  assert(chartInstance,
    `margin-used ECharts instance is missing after range refresh (mounted=${chartShell.dataset.echartMounted}, connected=${chartShell.isConnected})`);
  const walletText = detailSlot.querySelector(".account-kpi-grid")?.textContent;
  const chartData = JSON.stringify(chartInstance.getOption().series[0].data);
  offline = true;
  version = 4;
  await updateLiveAccountsDynamic(root, {
    status: "READY",
    accounts: [{ account_label: "primary", status: "READY", observed_at: observedAt(5) }],
  });
  assert(detailSlot.querySelector('[data-row-key="BTCUSDT:BOTH"]') === btcRow,
    "failed detail refresh dropped the last-good table row");
  assert(detailSlot.querySelector(".account-kpi-grid")?.textContent === walletText,
    "failed detail refresh dropped last-good balances and summary");
  assert(metricsSlot.querySelector('[data-echart-id="live-account-metric-margin_used"]') === chartShell,
    "failed metrics refresh dropped the last-good chart shell");
  assert(window.echarts.getInstanceByDom(chartShell.querySelector(".echart-surface")) === chartInstance,
    "failed metrics refresh dropped the last-good ECharts instance");
  assert(JSON.stringify(chartInstance.getOption().series[0].data) === chartData,
    "failed metrics refresh altered chart series data");
  assert([...detailSlot.querySelectorAll("[data-refresh-state]")].some((state) => !state.hidden),
    "failed detail refresh did not mark retained content stale");
  assert([...metricsSlot.querySelectorAll("[data-refresh-state]")].some((state) => !state.hidden),
    "failed metrics refresh did not mark retained content stale");

  offline = false;
  version = 5;
  await updateLiveAccountsDynamic(root, {
    status: "READY",
    accounts: [{ account_label: "primary", status: "READY", observed_at: observedAt(5) }],
  });
  assert([...detailSlot.querySelectorAll("[data-refresh-state]")].every((state) => state.hidden),
    "successful account refresh did not clear the visible stale state");
  assert([...metricsSlot.querySelectorAll("[data-refresh-state]")].every((state) => state.hidden),
    "successful metrics refresh did not clear the visible stale state");
}

function heightMarkup(versionName, height) {
  return `<div data-race-version="${versionName}" style="height:${height}px">${versionName}</div>`;
}

async function exerciseSameRootRender() {
  const root = document.querySelector("#race-a");
  const sibling = document.querySelector("#race-b");
  const anchor = document.querySelector("#race-anchor");
  root.style.minHeight = "";
  sibling.style.minHeight = "";
  replaceChildrenFromHtml(root, heightMarkup("same-root-before", 610));
  replaceChildrenFromHtml(sibling, heightMarkup("same-root-sibling", 540));
  await new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve)));
  window.scrollTo({ top: Math.max(0, anchor.getBoundingClientRect().top + window.scrollY - 80), behavior: "instant" });
  await new Promise((resolve) => requestAnimationFrame(resolve));
  const anchorBefore = anchor.getBoundingClientRect().top;
  assert(Math.abs(anchorBefore - 80) <= 3, `could not place same-root anchor at test offset (top=${anchorBefore})`);

  replaceChildrenFromHtml(root, heightMarkup("same-root-first", 700));
  replaceChildrenFromHtml(root, heightMarkup("same-root-last", 180));
  assert(root.style.minHeight.endsWith("px"), "same-root replacement did not install its height lock");
  await new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve)));
  assert(root.querySelector("[data-race-version='same-root-last']"),
    "same-root rapid render lost the last DOM version");
  assert(root.style.minHeight === "", "same-root rapid render left a stale min-height lock");
  assert(root.offsetHeight < 250, "same-root rapid render left the old height after cleanup");
  const anchorAfter = anchor.getBoundingClientRect().top;
  assert(Math.abs(anchorAfter - anchorBefore) <= 3,
    `same-root rapid height changes moved the reading anchor by ${(anchorAfter - anchorBefore).toFixed(1)}px`);
}

async function exerciseMultiRootAnchor() {
  const rootA = document.querySelector("#race-a");
  const rootB = document.querySelector("#race-b");
  const anchor = document.querySelector("#race-anchor");
  rootA.style.minHeight = "";
  rootB.style.minHeight = "";
  replaceChildrenFromHtml(rootA, heightMarkup("root-a-before", 610));
  replaceChildrenFromHtml(rootB, heightMarkup("root-b-before", 540));
  await new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve)));

  const plainMarker = document.querySelector("#plain-reading-marker");
  window.scrollTo({ top: Math.max(0, plainMarker.getBoundingClientRect().top + window.scrollY - 80), behavior: "instant" });
  await new Promise((resolve) => requestAnimationFrame(resolve));
  const plainBefore = plainMarker.getBoundingClientRect().top;
  assert(Math.abs(plainBefore - 80) <= 3, `could not place plain reading position (top=${plainBefore})`);
  assert(captureViewState(rootA).anchor === null,
    "plain reading-position test unexpectedly found a keyed semantic anchor");

  replaceChildrenFromHtml(rootA, heightMarkup("root-a-no-anchor-after", 210));
  replaceChildrenFromHtml(rootB, heightMarkup("root-b-no-anchor-after", 280));
  await new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve)));
  const plainAfter = plainMarker.getBoundingClientRect().top;
  assert(Math.abs(plainAfter - plainBefore) <= 3,
    `fallback without a semantic anchor replayed stale scroll and moved the reading position by ${(plainAfter - plainBefore).toFixed(1)}px`);
  assert(rootA.style.minHeight === "" && rootB.style.minHeight === "",
    "no-anchor cross-root restore left a stale min-height lock");

  replaceChildrenFromHtml(rootA, heightMarkup("root-a-before", 610));
  replaceChildrenFromHtml(rootB, heightMarkup("root-b-before", 540));
  await new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve)));
  window.scrollTo({ top: Math.max(0, anchor.getBoundingClientRect().top + window.scrollY - 80), behavior: "instant" });
  await new Promise((resolve) => requestAnimationFrame(resolve));
  const before = anchor.getBoundingClientRect().top;
  assert(Math.abs(before - 80) <= 3, `could not place reading anchor at test offset (top=${before})`);
  assert(captureViewState(rootA).anchor?.stateKey === "race-reading-anchor",
    "semantic-anchor cross-root test did not capture the reading heading");

  replaceChildrenFromHtml(rootA, heightMarkup("root-a-after", 210));
  replaceChildrenFromHtml(rootB, heightMarkup("root-b-after", 280));
  await new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve)));
  const after = anchor.getBoundingClientRect().top;
  assert(Math.abs(after - before) <= 3,
    `two roots changing height in one frame moved the reading anchor by ${(after - before).toFixed(1)}px`);
  assert(rootA.style.minHeight === "" && rootB.style.minHeight === "",
    "cross-root restore left a stale min-height lock");
}

async function exerciseFocusRace() {
  const root = document.querySelector("#race-a");
  const oldMarkup = `<div style="height:100px"><button id="race-old-focus" type="button">Old focus target</button></div>`;
  const newMarkup = `<div style="height:120px"><button id="race-old-focus" type="button">Updated focus target</button></div>`;
  replaceChildrenFromHtml(root, oldMarkup);
  const oldFocus = root.querySelector("#race-old-focus");
  oldFocus.focus({ preventScroll: true });
  replaceChildrenFromHtml(root, newMarkup);
  assert(document.activeElement === root.querySelector("#race-old-focus"),
    "same-root restore did not preserve focus before the animation frame");

  const userTarget = document.querySelector("#race-user-focus");
  userTarget.dispatchEvent(new PointerEvent("pointerdown", {
    bubbles: true,
    pointerType: "mouse",
    isPrimary: true,
    button: 0,
  }));
  userTarget.click();
  userTarget.focus({ preventScroll: true });
  await new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve)));
  assert(document.activeElement === userTarget,
    "stale same-root animation-frame restore stole focus from the user's newer selection");
}

await run("real DOM: same-structure account/metrics poll updates balances, positions, orders, fills, and chart", exerciseAccountRefresh);
await run("real DOM: keyed rows, disclosure state, chart shell, and ECharts instance survive morph", () => {
  const root = document.querySelector("#scenario");
  assert(root.querySelector('[data-row-key="ETHUSDT:BOTH"]'), "ETH row missing after refresh");
  assert(root.querySelector('[data-row-key="fill-2"]'), "new fill missing after refresh");
});
await run("real DOM: account and metrics range listeners issue one request and preserve 7d", exerciseRangeListeners);
await run("real DOM: offline refresh retains last-good balances, rows, charts, and recovers stale state", exerciseLastGoodRefresh);
await run("real DOM: same-root rapid render keeps the last version and releases min-height", exerciseSameRootRender);
await run("real DOM: same-frame height changes in two roots keep the reading anchor stable", exerciseMultiRootAnchor);
await run("real DOM: newer user focus wins over a queued same-root restore", exerciseFocusRace);

document.documentElement.dataset.browserTestsComplete = "true";
