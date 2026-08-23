import assert from "node:assert/strict";
import test from "node:test";

import {
  buildChartOption,
  getChartPayload,
} from "../../src/crypto_momentum_lab/operator_dashboard/static/dashboard-chart-engine.js";
import { renderAccount } from "../../src/crypto_momentum_lab/operator_dashboard/static/sections/account.js";


test("live account renderer exposes equity range controls and yearly dates", () => {
  const [, html] = renderAccount({
    status: "READY",
    observed_at: "2026-08-24T00:00:00Z",
    equity_range: "1y",
    equity_window_start: "2025-08-24T00:00:00Z",
    equity_window_end: "2026-08-24T00:00:00Z",
    equity_sample_interval_seconds: 172800,
    equity_curve: [
      { observed_at: "2026-08-20T00:00:00Z", equity: "280" },
      { observed_at: "2026-08-22T00:00:00Z", equity: "282" },
    ],
    balances: [],
    positions: [],
    open_orders: [],
    fills: [],
  });

  assert.match(html, /data-account-equity-range="24h"/);
  assert.match(html, /data-account-equity-range="7d"/);
  assert.match(html, /data-account-equity-range="30d"/);
  assert.match(html, /data-account-equity-range="1y" aria-pressed="true"/);
  assert.match(html, /ROLLING 1Y · 2 DAY BUCKETS/);
  assert.match(html, /2025-08-24/);
  assert.match(html, /更长区间会随实盘运行逐步积累/);

  const payload = getChartPayload("live-account-equity");
  const option = buildChartOption(payload);
  assert.equal(
    option.xAxis.axisLabel.formatter(Date.parse("2026-08-01T00:00:00Z")),
    "2026-08",
  );
});
