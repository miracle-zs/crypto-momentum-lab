import assert from "node:assert/strict";
import test from "node:test";

import { renderReports } from "../../src/crypto_momentum_lab/operator_dashboard/static/sections/reports.js";

test("empty live reports render a useful empty state", () => {
  const [status, html] = renderReports({ status: "NO_DATA", live_sessions: [] });
  assert.equal(status, "NO_DATA");
  assert.match(html, /暂无实盘运行记录/);
  assert.doesNotMatch(html, /ledger-event-marker/);
});

test("live timeline shows the newest event first without changing its input", () => {
  const sessions = [
    { session_id: "earlier-run", state: "live_enabled", occurred_at: "2026-10-04T00:00:00Z" },
    { session_id: "latest-run", state: "live_enabled", occurred_at: "2026-10-04T01:00:00Z" },
  ];
  const original = structuredClone(sessions);
  const [status, html] = renderReports({ status: "READY", live_sessions: sessions });
  assert.equal(status, "READY");
  const timeline = html.slice(0, html.indexOf("</ol>"));
  assert.ok(timeline.indexOf("latest-run") < timeline.indexOf("earlier-run"));
  assert.deepEqual(sessions, original);
});

test("report identities are escaped instead of executing markup", () => {
  const [, html] = renderReports({
    status: "READY",
    live_sessions: [{ session_id: "<script>alert(1)</script>", state: "live_enabled", occurred_at: "2026-10-04T00:00:00Z" }],
  });
  assert.doesNotMatch(html, /<script>/);
  assert.match(html, /&lt;script&gt;/);
});
