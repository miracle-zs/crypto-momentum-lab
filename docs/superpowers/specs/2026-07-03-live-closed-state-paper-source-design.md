# Live Closed-State Paper Source Design

Date: 2026-07-03

## 1. Status and Scope

This document defines the backend phase after paper-run PostgreSQL
persistence. The purpose is to connect live-style closed 15-second market
states from the `market-data` process to the `strategy-runner paper` path
without adding account access or real order execution.

This phase turns the current offline paper runner into a bounded runtime paper
runner:

```text
archived raw envelopes -> normalized events -> closed MarketState15s rows
-> PostgreSQL handoff -> paper runner polling source -> persisted paper report
```

This phase includes:

- a PostgreSQL handoff table for closed `MarketState15s` rows;
- a repository for idempotent closed-state writes and deterministic polling;
- a `market-data` closed-state publisher that normalizes archived raw
  envelopes, aggregates 15-second buckets, and writes only closed buckets;
- a `strategy-runner` PostgreSQL polling source implementing the existing
  `PaperMarketStateSource` contract;
- a bounded paper CLI path that can run from live closed-state rows for tests
  and controlled paper sessions;
- readiness and gap handling sufficient to prevent paper mode from treating
  stale or incomplete state as current;
- unit and integration tests for state closure, polling order, idempotency, and
  CLI behavior.

This phase excludes:

- Binance private REST or user-data websocket clients;
- balances, positions, margin, leverage, fills, funding, and account
  reconciliation;
- real order submission, cancellation, replacement, or emergency flattening;
- risk approval, symbol quantization, and account trading leases;
- multi-strategy arbitration;
- continuous daemon supervision for strategy-runner;
- strategy optimization or parameter selection.

## 2. Design Position

The project now has:

- raw market capture and archive durability;
- offline normalized-event and 15-second state derivation;
- deterministic replay and paper runners;
- paper-run PostgreSQL persistence.

The missing boundary is a durable runtime handoff of closed market states. The
next safe increment is not `execution-account`; it is proving that a selected
strategy can consume live-style closed state in deterministic order while still
using simulated paper fills.

PostgreSQL is appropriate for V0 because the architecture already uses it for
low-volume inter-process coordination and durable state. It also gives the
paper runner restart semantics: after a process restart, it can resume polling
from the last processed `(bucket_start, symbol)` instead of relying on an
in-memory queue.

## 3. Handoff Table

Add a PostgreSQL table named `runtime_market_states_15s`.

Primary key:

- `environment`;
- `symbol`;
- `bucket_start`.

Fields mirror `MarketState15s`:

- `schema_version`;
- `exchange`;
- `environment`;
- `symbol`;
- `bucket_start`;
- `bucket_end`;
- `open_price`;
- `high_price`;
- `low_price`;
- `close_price`;
- `trade_count`;
- `trade_notional`;
- `aggressive_buy_notional`;
- `aggressive_sell_notional`;
- `last_bid_price`;
- `last_ask_price`;
- `spread`;
- `midpoint`;
- `liquidation_count`;
- `liquidation_notional`;
- `mark_price`;
- `closed_kline_count`;
- `source_event_count`;
- `first_received_at`;
- `last_received_at`.

Runtime metadata fields:

- `created_at`;
- `updated_at`;
- `source_watermark_at`;
- `closure_reason`, initially `watermark_elapsed`;
- `input_sequence_min`;
- `input_sequence_max`.

Indexes:

- `(environment, bucket_start, symbol)` for polling;
- `(environment, symbol, bucket_start)` for per-symbol resume and inspection;
- `(environment, created_at)` for monitoring ingestion lag.

Writes are idempotent. A repeated write of the same primary key with identical
state values is accepted. A conflicting write raises an explicit error because
closed states are evidence and must not be silently mutated.

## 4. Closed-State Publisher

Add a `ClosedMarketStatePublisher` inside the `market_data` boundary.

Responsibilities:

1. receive raw envelopes only after they have been durably archived;
2. normalize Binance envelopes into `NormalizedMarketEvent`;
3. maintain per `(environment, symbol, bucket_start)` accumulators using the
   existing 15-second aggregation semantics;
4. close buckets when the event-time watermark has moved past `bucket_end` by a
   configurable delay;
5. write closed states through the runtime state repository;
6. expose simple counters for received events, closed states, rejected
   envelopes, and latest watermark.

The publisher must not emit a bucket before it is closed. V0 uses event time,
not wall-clock time, to determine the watermark:

```text
watermark = max_seen_event_at - closure_delay_seconds
closed when bucket_end <= watermark
```

The default `closure_delay_seconds` is 30 seconds. This leaves room for late
events while keeping paper mode close to runtime. A late event for an already
closed bucket is recorded as a quality event and ignored for V0; it does not
rewrite the closed row.

The capture coordinator should call the publisher in dequeue order after the
raw envelope archive append succeeds. If archiving fails, the envelope is not
published to the state builder. This keeps the traceability invariant intact:
paper-visible states are derived only from raw messages that were durably
archived first.

## 5. Runtime State Repository

Add `PostgresRuntimeMarketStateRepository`.

Methods:

```text
save_closed_states(states, source_watermark_at, sequence_range)
load_after(environment, after_bucket_start, after_symbol, limit)
load_latest_bucket(environment)
```

`save_closed_states()` stores a tuple of closed `MarketState15s` rows in one
transaction. It validates:

- timezone-aware `bucket_start`, `bucket_end`, `first_received_at`, and
  `last_received_at` when present;
- `bucket_end > bucket_start`;
- non-empty `environment` and `symbol`;
- duplicate states inside the same call;
- idempotent replay of existing rows.

`load_after()` returns rows in strict `(bucket_start, symbol)` order. The
cursor is:

```text
after_bucket_start: datetime | None
after_symbol: str | None
```

When the cursor is `None`, polling starts at the oldest available row unless a
CLI start time is supplied.

## 6. Paper Polling Source

Add `PostgresPaperMarketStateSource`, implementing the existing
`PaperMarketStateSource` protocol.

Configuration:

- `environment`;
- `start_at`;
- `poll_interval_seconds`;
- `idle_timeout_seconds`;
- `max_states`;
- `batch_size`.

Behavior:

1. poll `PostgresRuntimeMarketStateRepository.load_after()`;
2. yield rows in deterministic `(bucket_start, symbol)` order;
3. advance the cursor only after yielding each state;
4. sleep `poll_interval_seconds` when no new row is available;
5. stop when `max_states` is reached or when `idle_timeout_seconds` elapses
   without new states;
6. expose `description` containing environment and start cursor for run
   identity.

V0 remains a bounded source. It is suitable for controlled paper sessions and
tests, not yet a supervised always-on strategy process. Continuous restart,
leases, and halt handling are later runtime concerns.

## 7. CLI Behavior

Keep the existing offline command unchanged:

```text
cml-strategy-runner paper --states-root ...
```

Add a separate command:

```text
cml-strategy-runner paper-live-source \
  --strategy compression_breakout \
  --database-url postgresql+asyncpg://cml:cml@localhost:54329/cml \
  --environment research \
  --output reports/compression-breakout-paper-live-source.json \
  --max-states 1000 \
  --idle-timeout-seconds 60 \
  --persist
```

The new command:

- requires `--database-url` or `CML_DATABASE_URL`;
- reads closed states from PostgreSQL instead of Parquet;
- runs the same `run_paper_trading()` function;
- writes the same JSON report shape;
- can optionally persist the report through the existing
  `PostgresStrategyRunRepository`;
- fails explicitly when no states are received before idle timeout.

Using a separate command avoids overloading `--states-root` semantics and keeps
offline replay/paper behavior stable.

## 8. Error Handling And Readiness

The market-data publisher rejects and records quality events for:

- unsupported raw envelope stream;
- normalization failure;
- naive timestamps;
- event time moving materially backward for the same symbol;
- late event arriving after its bucket has closed;
- conflicting closed-state write.

The paper polling source fails or stops explicitly for:

- missing database URL;
- non-positive polling configuration;
- no rows before idle timeout;
- repository rows with naive timestamps;
- state order moving backward;
- duplicate state primary keys in one batch.

V0 readiness is conservative: paper mode can consume only states already
written to the closed-state table. It does not infer market-data freshness from
raw websocket connection state. Later phases can add freshness gates and
process-state halt integration before live trading.

## 9. Testing Strategy

Unit tests cover:

- `ClosedMarketStatePublisher` closes only buckets behind the watermark;
- late events for closed buckets are rejected and do not rewrite state;
- closed state row mapping preserves Decimal and timestamp values;
- repository validation rejects duplicate or naive states;
- polling source yields deterministic `(bucket_start, symbol)` order;
- polling source stops at `max_states` and idle timeout;
- `paper-live-source` CLI option validation and persistence behavior.

Integration tests cover, when local PostgreSQL is available:

- migrations create `runtime_market_states_15s`;
- repository save/load is idempotent and ordered;
- conflicting closed-state writes fail explicitly;
- paper runner consumes states from PostgreSQL and writes a JSON report;
- optional report persistence writes strategy run artifacts.

Full verification requires:

- targeted unit tests for runtime state publishing and polling;
- targeted PostgreSQL integration tests when Docker/PostgreSQL is available;
- existing paper runner and strategy persistence tests;
- `ruff check .`;
- `mypy src`;
- full non-live pytest when the local database service is available.

## 10. Acceptance Criteria

This phase is complete when:

1. market-data can derive and persist closed 15-second runtime states after raw
   messages are archived;
2. closed state writes are idempotent and conflicting writes fail explicitly;
3. strategy-runner can run paper mode from PostgreSQL closed-state rows;
4. polling order is deterministic and cursor-based;
5. bounded paper sessions stop predictably by `max_states` or idle timeout;
6. paper report JSON and optional PostgreSQL report persistence still work;
7. no authenticated Binance client, account state, risk engine, trading lease,
   or real order execution is introduced;
8. unit tests, targeted integration tests, ruff, and mypy pass in the available
   environment.

## 11. Later Phases

Later backend phases are:

1. add periodic checkpoint persistence for long-running paper mode;
2. add market-data freshness gates and process-state halt integration;
3. add account synchronization and risk controls in the separate
   `execution-account` boundary;
4. add live order submission only after paper closed-state operation and
   account reconciliation are stable.
