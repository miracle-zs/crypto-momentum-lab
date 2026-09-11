# Decision SLO telemetry

This runbook describes the low-cardinality telemetry plane used to answer
whether the live decision path is healthy. It deliberately reuses
`strategy_runtime_events`; it does not add Prometheus, a second metrics store,
or a synchronous database call to the trading path.

## Dashboard query

The read-only dashboard exposes:

```text
GET /api/decision-slo?window=1h|6h|24h|7d
```

The response contains:

- `phase_latency`: p50/p95/max milliseconds and sample count for the four
  decision transitions;
- `consumers`: observed events, recovery count, unavailable count, lag-event
  count, last availability, and the last recovery reason for each hub;
- `terminal_reasons`: counts grouped by lane, trigger source, and reason;
- `persisted_event_count` and `truncated`, so a bounded query cannot be
  mistaken for a complete export.

`recovery_count` means a consumer became available after being unavailable.
`unavailable_event_count` counts the preceding degraded transitions; a
historical lag event is not treated as a currently unhealthy consumer when a
later recovery is present.

## Persistence boundary

All live phase points remain available in the bounded in-process telemetry
trace and latency rollup. Only sparse order-lifecycle events are durable. The
events for `candidate_accepted`, `intent_saved`, and the first submit request
carry the completed decision-SLO transition samples:

```text
market_state_received → context_ready
context_ready → candidate_accepted
candidate_accepted → intent_saved
intent_saved → exchange_request_started
```

This preserves the historical decision path without writing every 15-second
market or strategy event to PostgreSQL. `consumer_health` and the low-cardinality
`terminal_reason` rollup are also durable through the best-effort observability
writer. A database outage or a full telemetry queue can still lose diagnostic
events; it must not block or change order execution.

The query reads all persisted runtime events in the selected time window. It
is bounded at 50,000 rows and uses `truncated=true` when more rows are
available. The window is based on `occurred_at`, not dashboard request time.
Migration `20260911_0036` adds the standalone `occurred_at` index used by this
time-bounded query; apply it to every configured observability database before
enabling the endpoint in a deployment.

## Verification

Run the focused checks before a release:

```bash
.venv/bin/python -m pytest -q \
  tests/unit/live_rollout/test_telemetry.py \
  tests/unit/live_rollout/test_telemetry_source.py \
  tests/unit/operator_dashboard/test_queries.py \
  tests/unit/operator_dashboard/test_api.py \
  tests/unit/apps/operator_dashboard/test_main.py
.venv/bin/ruff check src/crypto_momentum_lab/live_rollout/telemetry.py \
  src/crypto_momentum_lab/operator_dashboard \
  tests/unit/live_rollout/test_telemetry.py \
  tests/unit/live_rollout/test_telemetry_source.py
```

For a live session, treat a missing or stale response as an observability
failure, not as permission to trade. The existing lease, hub fail-closed
gates, durable intent barrier, and execution reconciliation remain the safety
authority.
