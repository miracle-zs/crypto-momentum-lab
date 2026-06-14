# Crypto Momentum Lab Architecture Design

Date: 2026-06-14

## 1. Status and Scope

This document defines the target architecture for `crypto-momentum-lab`.

It supersedes the fixed BTC/ETH/SOL universe in Section 3.1, the hierarchical
strategy-combination guidance in Section 7, and the engineering boundary,
suggested project structure, and development-order guidance in Sections 9
through 11 of
`docs/research/short-horizon-momentum-strategy-research-design.md`. It does not
replace the three individual strategy hypotheses, their feature research, or
their validation standards.

The target system:

- runs on one Linux server;
- trades one Binance USD-M Futures account;
- supports three independently researched strategies;
- allows exactly one strategy to own live trading at a time;
- selects a dynamic universe from all active USDT perpetual contracts;
- progresses from research to replay, paper trading, shadow operation, and
  small-capital live trading;
- uses the same deterministic strategy core in replay, paper, and live modes.

The three strategies are:

1. `compression_breakout`
2. `orderflow_impulse`
3. `liquidation_cascade`

They share market-data, execution, account, and risk infrastructure. They do
not share derived features, signals, parameters, positions, or performance
attribution.

## 2. Architecture Decision

The project will use a modular Python monolith deployed as three long-running
processes:

1. `market-data`
2. `strategy-runner`
3. `execution-account`

PostgreSQL provides durable runtime state and low-volume inter-process
coordination. Raw market events are archived to compressed append-only files.
Research datasets are stored as partitioned Parquet and queried with DuckDB or
Polars.

Kafka, Redis, ClickHouse, Kubernetes, and independent strategy microservices
are explicitly excluded from the initial architecture. The expected workload
does not justify their operational cost on a single server.

## 3. Core Invariants

The implementation must preserve these invariants:

1. A strategy never calls the Binance trading API directly.
2. Exactly one live strategy holds the account trading lease at a time.
3. Switching strategies requires all positions to be closed, all open orders
   to be resolved, and the account to be reconciled.
4. Binance is the final source of truth for balances, positions, orders, and
   fills.
5. PostgreSQL is the durable local record, not the authority over exchange
   state.
6. Raw captured messages are immutable.
7. Every derived record is traceable to a schema version, source session, and
   input data manifest.
8. The historical universe is reconstructed point in time. A backtest may not
   use the current contract list or current ranking.
9. A symbol may open a new position only while it belongs to the active hourly
   target universe and passes the current trading-eligibility checks.
10. A held symbol remains monitored until its position and orders are fully
    resolved, regardless of its current rank.
11. Loss of data freshness, account certainty, or durable state prevents new
    exposure.
12. Identical ordered inputs, configuration, code version, and clock events
    produce identical strategy outputs.

## 4. Dynamic Universe

### 4.1 Ranking Population

Every active Binance USD-M USDT-margined perpetual contract participates in
the hourly ranking. Listing age, turnover, spread, and depth do not remove a
contract from the ranking population.

Delivery contracts, inactive contracts, non-USDT contracts, and contracts
without a valid current price or UTC-day opening price do not participate in
that snapshot. Their exclusion reason is recorded.

### 4.2 Return Definition

For symbol \(s\) at ranking time \(t\):

```text
utc_day_return(s, t) = current_price(s, t) / utc_day_open(s) - 1
```

`utc_day_open` is the opening price of the first valid one-minute kline at or
after 00:00:00 UTC for that contract. For a contract listed after midnight, it
is the opening price of its first valid one-minute kline.

`current_price` is the last valid traded price observed at the ranking cutoff.
The snapshot records the exact source event and cutoff time.

Ranking runs once per hour after the first complete minute following the hour.
The 00:00 UTC snapshot is recorded but is not activated because its
measurement horizon is effectively zero. The previous day's 23:00 target
universe remains active until the 01:00 UTC snapshot is ready.

Ties are resolved deterministically by symbol name.

### 4.3 Target and Monitoring Universes

The target universe is the unique union of:

- the 20 highest `utc_day_return` contracts; and
- the 20 lowest `utc_day_return` contracts.

The target universe is the only set eligible to open new positions.

The monitoring universe contains:

- the current target universe;
- a retained symbol that was previously in the target universe, remains in
  the corresponding top or bottom 30, and has been outside the target top or
  bottom 20 for less than two hours;
- every symbol with an open order or non-zero account position.

A retained symbol is removed when it leaves the corresponding top or bottom
30 or reaches two hours outside the target universe, unless an order or
position still requires monitoring.

The ranking direction is metadata for research. It does not force a strategy
to trade in the same direction.

### 4.4 Warm-Up and Readiness

Entering the target universe starts high-resolution subscription and warm-up.
It does not immediately make a contract tradable.

Each strategy declares:

- required market streams;
- required live-history duration;
- required slower historical windows;
- maximum accepted event gap;
- maximum accepted data age.

The market-data process marks a symbol `READY` only when the requirements of
the currently selected strategy are satisfied. Historical one-minute data may
bootstrap slower market-state windows. Trade-flow, spread, liquidation, and
event-order requirements must be satisfied by captured live data unless the
strategy specification explicitly accepts a validated historical source.

Readiness is revoked on a material stream gap, stale state, schema mismatch,
or failed consistency check. Revocation prevents new positions but does not
prevent risk-reducing orders.

### 4.5 Ranking Versus Trading Eligibility

All active USDT perpetuals participate in ranking, but opening a position
requires a second eligibility gate.

The execution layer evaluates configurable limits for:

- listing age;
- recent notional turnover;
- bid-ask spread;
- executable depth and estimated impact;
- contract status;
- price and quantity precision;
- minimum quantity and minimum notional;
- market-data freshness and warm-up readiness;
- abnormal price or market-state conditions.

Rejected opportunities remain recorded with the failed rule and measured
values. Numeric limits belong to versioned strategy and risk configurations,
not to the architecture.

## 5. Runtime Processes

### 5.1 `market-data`

This process owns public market connectivity and universe maintenance.

Responsibilities:

- load and refresh Binance contract metadata;
- maintain UTC-day opening prices and latest prices for the full ranking
  population;
- produce and persist hourly universe snapshots;
- maintain target, retained, and position-forced subscriptions;
- archive raw websocket and REST responses before transformation;
- normalize aggregate trades, best bid/ask, klines, mark price, and
  liquidation events;
- build closed 15-second market states;
- evaluate strategy-specific data readiness;
- detect stream gaps, delayed events, reconnects, and schema violations;
- publish durable market-state notifications.

Universe selection is a module inside this process for the initial deployment.
It has a separate interface and persistence model so it can be extracted later
without changing consumers.

### 5.2 `strategy-runner`

This process loads exactly one selected strategy in `paper` or `live` mode.
Offline replay may run additional isolated instances without a live trading
lease.

Responsibilities:

- acquire and renew the account trading lease;
- load a strategy by name, version, and immutable configuration;
- read closed market states in deterministic order;
- maintain strategy-local state;
- evaluate entry, adjustment, and exit logic;
- emit standardized order intents;
- consume account and execution updates;
- record features, signals, decisions, and rejection context;
- stop producing new intents when its lease or input readiness is lost.

The strategy interface returns decisions. It does not contain exchange REST or
websocket clients, database-specific queries, or file-system writes.

### 5.3 `execution-account`

This process is the only owner of authenticated Binance connectivity.

Responsibilities:

- synchronize balances, positions, open orders, fills, commissions, funding,
  margin settings, and leverage settings;
- validate that account position mode and symbol settings match the live
  configuration;
- claim and validate order intents;
- apply symbol, strategy, and account risk controls;
- quantize prices and quantities using current exchange metadata;
- submit, amend where supported, cancel, and reconcile orders;
- manage partial fills, rejects, timeouts, and reduce-only exits;
- publish durable execution and account updates;
- own global halt, cancel-all, and emergency-flatten operations.

Initial live support is limited to Binance one-way position mode. Margin mode
and leverage are explicit versioned configuration and are verified before
trading is enabled. An account configuration mismatch prevents new positions.

## 6. Module Boundaries

The codebase will use these top-level package boundaries:

```text
src/crypto_momentum_lab/
  apps/
    market_data/
    strategy_runner/
    execution_account/
  domain/
    market/
    universe/
    strategy/
    execution/
    account/
    risk/
  contracts/
    events/
    intents/
    schemas/
  market_data/
    binance/
    capture/
    normalization/
    aggregation/
    quality/
  universe/
    ranking/
    membership/
    readiness/
  strategies/
    compression_breakout/
    orderflow_impulse/
    liquidation_cascade/
  execution/
    binance/
    planning/
    orders/
    reconciliation/
  risk/
    symbol/
    strategy/
    account/
  persistence/
    postgres/
    raw_files/
    parquet/
  replay/
  research/
  observability/
```

The `domain` and `contracts` packages contain stable types and interfaces.
Infrastructure packages depend on them, not the reverse.

Each strategy package owns its feature calculations, state machine, signal
logic, exit logic, configuration schema, and research reports. Common market
primitives such as returns, spread, and aggressive-side normalization may be
shared. Strategy-derived features may not be placed in a generic shared
`features` package.

## 7. Data and Persistence

### 7.1 Immutable Raw Event Archive

Raw websocket and REST payloads are written as compressed JSON Lines files.
Files are partitioned by UTC date, exchange, stream, and symbol where
applicable.

Every envelope includes:

- schema version;
- exchange name and environment;
- stream name;
- symbol when applicable;
- exchange event time;
- local wall-clock receive time;
- local monotonic receive value;
- connection session ID;
- sequence or update identifiers when supplied;
- raw payload.

Files are append-only. Rotation uses a temporary file followed by an atomic
rename. A manifest stores file size, row count, first and last timestamps,
checksum, capture version, and known gaps.

### 7.2 Research Data

Partitioned Parquet stores:

```text
universe_snapshots/
market_events/
market_states_15s/
strategy_features/compression_breakout/
strategy_features/orderflow_impulse/
strategy_features/liquidation_cascade/
strategy_signals/compression_breakout/
strategy_signals/orderflow_impulse/
strategy_signals/liquidation_cascade/
experiments/<strategy_name>/
```

DuckDB and Polars are the default analytical engines. Parquet datasets are
derived artifacts and can be rebuilt from raw inputs and manifests.

### 7.3 PostgreSQL Runtime State

PostgreSQL stores:

- contract metadata versions;
- hourly universe snapshots and memberships;
- symbol readiness and data-quality state;
- recent closed 15-second market states;
- active strategy, version, configuration hash, and trading lease;
- strategy checkpoints and order intents;
- risk state and halt reasons;
- orders, fills, positions, balance snapshots, and reconciliation results;
- raw-file and Parquet manifests;
- process heartbeats and operational audit events.

PostgreSQL does not store raw high-volume market messages. Recent 15-second
states may be retained for operational recovery and then archived to Parquet
according to a configured retention policy.

### 7.4 Inter-Process Coordination

No external message broker is required.

- Durable records are inserted into PostgreSQL first.
- PostgreSQL `LISTEN/NOTIFY` carries lightweight wake-up notifications.
- Consumers recover missed notifications by reading records after their
  durable checkpoint.
- Order intents use a transactional outbox and are claimed with database
  locking.
- Every consumer operation is idempotent.

Notifications are an optimization, never the only copy of an event.

## 8. Strategy Contract and Isolation

Every strategy implements a common lifecycle:

```text
load(config, metadata)
required_data()
restore(checkpoint)
on_market_state(state, account_view)
on_execution_update(update)
checkpoint()
shutdown(reason)
```

The strategy returns zero or more standardized intents:

```text
intent_id
strategy_name
strategy_version
config_hash
signal_id
symbol
side
desired_notional
entry_type
limit_price
stop_policy
reduce_only
expires_at
created_at
```

The execution process may reject or resize an intent. The strategy receives
the result as an execution update.

Each run is identified by:

```text
strategy_name
strategy_version
config_hash
code_commit
data_manifest
run_mode
run_id
```

Feature tables, signal tables, metrics, and reports are namespaced by strategy
and run. One strategy may not consume another strategy's derived feature,
signal, or position state.

## 9. Replay, Paper, and Live Parity

The strategy core receives an injected clock and ordered domain events.

Adapters define the environment:

- replay adapter reads recorded events and uses a simulated exchange;
- paper adapter reads live events and uses simulated fills;
- live adapter reads live events and submits approved intents to Binance.

The same strategy package and configuration schema are used in all modes.
Environment-specific behavior is limited to data, clock, and execution
adapters.

Replay processes events in captured local receive order, not only exchange
event time. A replay run records its input manifests, code commit,
configuration hash, cost model, latency model, and random seed where a
stochastic fill model is used.

## 10. Execution and Idempotency

Before opening exposure, `execution-account` verifies:

1. the requesting strategy owns the current trading lease;
2. the intent has not expired or already been processed;
3. the symbol is in the current target universe;
4. the symbol is `READY`;
5. market and account data are fresh;
6. trading-eligibility rules pass;
7. risk limits permit the resulting exposure;
8. current exchange metadata supports the quantized order;
9. no unresolved order uncertainty exists for the symbol.

Each exchange order uses a deterministic unique client order ID derived from
the run and intent. If submission returns an ambiguous result, the process
queries Binance by client order ID before any retry. It never creates a second
order merely because a response timed out.

Risk-reducing and emergency orders remain allowed when target-universe
membership or normal entry eligibility is lost.

## 11. Strategy Switching

The only supported live switch is a flat-account handover:

1. mark the current strategy `DRAINING`;
2. stop accepting its new entry intents;
3. cancel or resolve all non-reduce-only open orders;
4. let the old strategy close positions or invoke an operator-approved
   controlled flatten;
5. reconcile until Binance reports no open orders and no non-zero positions;
6. persist final strategy and account checkpoints;
7. release the old trading lease;
8. load and validate the new strategy and configuration;
9. acquire a new trading lease;
10. enable new entries only after market readiness and account reconciliation
    both pass.

The new strategy never inherits the old strategy's position.

## 12. Risk Architecture

Risk controls are layered.

### 12.1 Symbol Layer

- contract and trading-status validation;
- spread and impact limits;
- turnover and depth limits;
- data freshness and gap checks;
- abnormal-price and volatility guards;
- symbol-level cooldown and rejection tracking.

### 12.2 Strategy Layer

- per-trade risk;
- maximum position notional;
- maximum concurrent positions;
- entry frequency and cooldown;
- maximum holding duration;
- consecutive-loss pause;
- strategy drawdown and session-loss limits.

### 12.3 Account Layer

- gross and net exposure;
- margin utilization;
- available-balance reserve;
- daily loss and maximum drawdown;
- maximum order rate and rejection rate;
- global halt;
- cancel-all and emergency flatten.

All numeric values are explicit, versioned configuration. A live configuration
cannot contain implicit library defaults for capital-at-risk limits.

## 13. Failure Handling and Recovery

The runtime state machine includes:

```text
STARTING
SYNCING
READY
TRADING
DRAINING
HALTED
STOPPED
```

The system enters or remains in `HALTED` when it cannot prove that opening new
exposure is safe. Examples include:

- stale or gapped required market streams;
- lost authenticated account stream without successful REST reconciliation;
- unresolved order submission;
- database unavailability;
- trading-lease loss;
- exchange metadata mismatch;
- clock drift beyond the configured bound;
- repeated order rejects;
- breached account risk limit.

In `HALTED`:

- new and exposure-increasing orders are rejected;
- account synchronization continues;
- cancel and reduce-only actions remain available;
- held symbols remain subscribed;
- recovery requires the failed condition to clear and reconciliation to pass.

After restart, `execution-account` first fetches Binance balances, positions,
open orders, and recent fills. It reconciles them with local records before
the strategy can acquire a trading lease. Local state is corrected through
audited reconciliation records rather than silently overwritten.

## 14. Observability and Operations

Every process emits structured JSON logs containing at least:

- timestamp;
- service;
- environment;
- run ID;
- strategy name when applicable;
- symbol when applicable;
- event or correlation ID;
- state transition;
- error category.

The initial deployment exposes health and metrics endpoints and supports a
configurable alert webhook. A full Prometheus and Grafana deployment is
optional until metric volume and operational needs justify it.

Critical alerts include:

- market or account disconnect;
- universe refresh failure;
- stale data or readiness revocation;
- unresolved order;
- reconciliation mismatch;
- risk halt;
- process restart;
- disk-space pressure;
- raw capture or database write failure;
- clock drift.

## 15. Deployment

The system is deployed with Docker Compose on one Linux server.

Services:

```text
postgres
market-data
strategy-runner
execution-account
```

Persistent volumes hold PostgreSQL data, raw archives, Parquet datasets, run
artifacts, and backups. Secrets are injected at deployment time and are never
committed to Git.

The host must provide:

- reliable UTC time synchronization;
- enough local disk for the configured raw-data retention;
- monitored free space;
- automated PostgreSQL and configuration backups;
- restart policies and health checks;
- outbound access to required Binance endpoints and the alert sink.

Research and replay commands may run in ephemeral containers using the same
package build. They do not share a live trading lease.

## 16. Configuration Layout

Configuration is layered and immutable for a run:

```text
configs/
  environments/
    research.yaml
    replay.yaml
    paper.yaml
    live.yaml
  strategies/
    compression_breakout.yaml
    orderflow_impulse.yaml
    liquidation_cascade.yaml
  risk/
    paper.yaml
    live.yaml
  universe/
    utc_day_top_bottom.yaml
```

Environment variables provide secrets and deployment-specific connection
strings. They do not override strategy or risk behavior silently.

The resolved configuration is validated, normalized, hashed, and stored before
a run begins. Configuration changes require a new run ID. Live risk limits
cannot be hot-reloaded without an audited state transition.

## 17. Testing and Acceptance

### 17.1 Unit Tests

- UTC-day return and hourly ranking;
- target and retention membership;
- warm-up and readiness transitions;
- event normalization and 15-second aggregation;
- strategy state machines;
- quantity and price quantization;
- risk calculations and state transitions.

### 17.2 Contract Tests

- recorded Binance message fixtures;
- REST and websocket schema compatibility;
- PostgreSQL migrations and event contracts;
- deterministic client order IDs;
- strategy interface compatibility.

### 17.3 Deterministic Replay

The same raw manifests and configuration must reproduce:

- universe snapshots;
- normalized events;
- 15-second states;
- strategy features;
- signals and intents;
- simulated execution outcomes when the fill model is deterministic.

### 17.4 Failure Injection

Tests cover:

- websocket disconnect and reconnect;
- duplicate, delayed, and out-of-order events;
- missing market intervals;
- PostgreSQL interruption;
- ambiguous order submission;
- partial fills and cancel races;
- authenticated-stream loss;
- process restart with open orders or positions;
- strategy lease expiry;
- UTC midnight universe rollover.

### 17.5 Promotion Gates

A strategy progresses through:

1. descriptive event study;
2. cost-aware deterministic replay;
3. untouched walk-forward evaluation;
4. live-data paper trading;
5. shadow operation with no order submission;
6. small-capital live operation.

Promotion evidence is strategy-specific and cannot be borrowed from another
strategy. Only one strategy is promoted to live trading at a time.

## 18. Delivery Sequence

The architecture is implemented in independently verifiable increments:

1. Project foundation, domain contracts, configuration, and PostgreSQL
   migrations.
2. Full-population hourly ranking and point-in-time universe snapshots.
3. Dynamic subscriptions, immutable raw capture, normalization, and data
   quality.
4. Deterministic 15-second aggregation, manifests, and replay.
5. Independent event-study pipeline for each strategy.
6. The first strategy that passes research gates, connected to paper
   execution.
7. Authenticated account synchronization, risk, order execution, and recovery.
8. Shadow operation and small-capital live deployment.
9. Independent promotion of the remaining strategies.

No strategy implementation is required to complete the foundation, universe,
capture, and replay layers. Execution development may use a deterministic
fixture strategy before any research strategy is approved.

## 19. Explicit Non-Goals

- Combining the three strategy signals.
- Running multiple live strategies on the account at the same time.
- Allowing a new strategy to inherit an old strategy's position.
- Multi-account or multi-tenant operation.
- Sub-second latency competition.
- Full order-book reconstruction in the first phase.
- Ranking only currently liquid or mature contracts.
- Treating ranking membership as sufficient permission to trade.
- Storing raw tick traffic in PostgreSQL.
- Introducing distributed infrastructure before a measured need exists.
