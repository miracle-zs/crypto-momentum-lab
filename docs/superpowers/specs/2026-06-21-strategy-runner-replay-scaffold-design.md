# Strategy Runner Replay Scaffold Design

Date: 2026-06-21

## 1. Status and Scope

This document defines the backend phase after the three independent descriptive
event studies. The purpose is to introduce the first deterministic
`strategy-runner` and replay scaffold without connecting to a Binance account.

This phase turns one approved event detector into a strategy core that can be
driven by ordered `MarketState15s` inputs and can emit standardized signals and
order-intent candidates. It is the bridge between research reports and later
paper/live operation.

The phase includes:

- stable strategy domain contracts for run identity, data requirements,
  signals, decisions, checkpoints, and order-intent candidates;
- a deterministic replay runner that feeds closed 15-second market states to one
  selected strategy;
- a Strategy B `compression_breakout` replay adapter using the existing V0
  event definition;
- a JSON replay report containing inputs, configuration, signals, candidate
  intents, checkpoints, and summary metrics;
- a local CLI command for replaying one strategy from derived Parquet states.

The phase excludes:

- authenticated Binance connectivity;
- paper fills, simulated PnL, live orders, or account state;
- risk approval, position sizing, leverage, margin, and order quantization;
- strategy switching and trading leases;
- combining signals from multiple strategies;
- optimizing parameters or selecting a live strategy.

## 2. Design Position

The next safe increment is not `execution-account`. The existing strategy
modules currently produce event-study records and forward labels. They do not
yet implement a runtime lifecycle, standardized signal output, checkpoints, or
an environment-neutral decision contract.

Building the replay scaffold first keeps the most important architecture
invariant intact:

```text
research detector -> strategy core -> standardized decision -> later execution
```

The strategy core must not know whether it is being used by offline replay,
paper trading, or live trading. V0 implements only the replay adapter, but the
domain contracts must be shaped so paper and live adapters can reuse them
without changing strategy code.

## 3. Strategy Contract V0

Every runtime strategy implements a small deterministic interface:

```text
metadata()
required_data()
restore(checkpoint)
on_market_state(state, context)
checkpoint()
```

The interface is synchronous in V0 because it receives already materialized
`MarketState15s` rows. Future live adapters may wrap it in asynchronous process
loops, but strategy logic remains a pure decision step over ordered inputs.

### 3.1 Run Identity

Each replay run records:

- `run_id`
- `strategy_name`
- `strategy_version`
- `config_hash`
- `run_mode`
- `code_commit`
- `created_at`
- input dataset paths

`run_mode` supports the stable enum values `replay`, `paper`, and `live`, but
this phase only starts `replay` runs.

### 3.2 Data Requirements

Each strategy declares:

- required base state interval, initially `15s`;
- minimum warm-up buckets;
- required fields from `MarketState15s`;
- maximum accepted data gap;
- whether a symbol can generate entries before warm-up completes.

The replay runner uses these requirements to skip or record rejections for
insufficient history. It does not infer strategy-specific readiness by reading
the strategy internals.

### 3.3 Decisions

For each input state, the strategy returns a `StrategyDecision` containing:

- zero or more `StrategySignal` records;
- zero or more `OrderIntentCandidate` records;
- zero or more rejection records;
- an updated checkpoint payload.

A signal describes a detected opportunity. An order-intent candidate describes
what the strategy would like to do if a future execution layer approves it.
Candidates are not executable orders.

## 4. Standard Records

### 4.1 StrategySignal

Signals are durable research/runtime records. They include:

- deterministic `signal_id`;
- run identity fields;
- `symbol`;
- `side`;
- `detected_at`;
- `source_state_at`;
- `reason`;
- strategy-specific feature values as string-safe JSON;
- optional reference prices such as close, midpoint, spread, and breakout
  boundary.

Signal IDs are deterministic from run ID, strategy, symbol, side, detection
timestamp, and a strategy-local sequence number. Replaying the same ordered
inputs with the same configuration must reproduce the same IDs.

### 4.2 OrderIntentCandidate

Intent candidates include:

- deterministic `candidate_id`;
- `signal_id`;
- run identity fields;
- `symbol`;
- `side`;
- `entry_type`, initially `market`;
- optional `limit_price`;
- optional `desired_notional`;
- `reduce_only`, always `false` in V0;
- `expires_at`;
- `created_at`;
- reason and feature snapshot.

V0 may use a fixed `desired_notional` from replay configuration so reports are
comparable. This value is not risk approval and is not an exchange order size.
Later risk and execution layers may reject, resize, quantize, or ignore it.

### 4.3 Rejections

Replay must record why a candidate was not emitted when the strategy evaluated
a state but declined to act. V0 rejection reasons include:

- insufficient warm-up;
- missing required price;
- missing required feature field;
- duplicate event inside cooldown;
- event detector did not trigger;
- candidate expired before replay could record it.

Rejections are summary-first. The report stores counts by reason and symbol,
not a full row for every non-event bucket by default.

### 4.4 Checkpoint

The checkpoint records enough strategy-local state to resume deterministically:

- last processed symbol timestamp;
- per-symbol cooldown state;
- per-symbol warm-up counters;
- strategy-specific state payload.

V0 checkpoints are JSON-serializable dataclasses. They are written into replay
reports but not persisted to PostgreSQL.

## 5. Compression Breakout Strategy Adapter

The first adapter is `compression_breakout` because it has the lowest latency
sensitivity and the simplest event-study contract.

The adapter reuses the existing compression breakout V0 event definition:

- frozen lookback range before candidate evaluation;
- breakout threshold;
- acceptance buckets;
- cooldown buckets;
- forward-label logic is not used for signal generation.

The adapter must not call the existing event-study function across the whole
dataset on every state. It should maintain a per-symbol rolling buffer and
evaluate only the current state. This keeps the runtime contract compatible with
future paper/live operation.

The emitted signal contains the same feature family as the event study:

- direction;
- compression window start and end;
- range high and low;
- range width percentage;
- breakout price;
- breakout distance;
- spread and midpoint when available.

Forward returns, MFE, and MAE remain labels for research reports. They are not
included in live-style signals.

## 6. Replay Runner

The replay runner accepts:

- selected strategy name;
- immutable strategy configuration;
- ordered `MarketState15s` rows or Parquet state paths;
- run identity metadata;
- replay output path.

It performs:

1. load and validate the strategy;
2. read and sort states by `(bucket_start, symbol)` unless the caller supplies
   an already ordered tuple;
3. feed each closed state to the strategy;
4. collect decisions, signals, candidates, and rejection summaries;
5. write a deterministic JSON replay report.

Sorting by `(bucket_start, symbol)` is sufficient for derived 15-second states.
Raw-message replay remains handled by the existing raw archive replay layer.

The runner stops with an explicit error when:

- an unknown strategy name is requested;
- configuration validation fails;
- a state has a naive timestamp;
- input paths contain no state rows;
- the strategy returns a candidate without a matching signal;
- the same deterministic signal or candidate ID appears twice.

## 7. Replay Report

The JSON report contains:

- schema version;
- generated timestamp;
- run identity;
- strategy configuration;
- source state paths;
- input state count;
- processed symbol count;
- ordered signal records;
- ordered order-intent candidates;
- rejection summary by reason and symbol;
- final checkpoint;
- summary counts by side and symbol.

Decimal values serialize as strings. Timestamps serialize as ISO-8601 strings.
The report is a research/runtime artifact, not a trading audit log.

## 8. CLI

Add a new application entry point:

```text
cml-strategy-runner replay \
  --strategy compression_breakout \
  --states-root data/derived/market_states_15s \
  --output reports/compression-breakout-replay.json
```

Initial CLI options:

- `--strategy`
- `--states-root`
- `--output`
- strategy-specific compression breakout thresholds;
- `--candidate-notional`;
- `--candidate-ttl-buckets`;
- `--run-id` for deterministic tests, optional for normal use.

The command prints a short summary:

```text
Replay completed: states=<n> signals=<n> candidates=<n>
```

## 9. Persistence Boundary

This phase writes only JSON report files. It does not add PostgreSQL strategy
tables. The domain contracts should be designed so PostgreSQL persistence can
be added later without changing strategy logic.

No migration is required in V0.

## 10. Testing Strategy

Unit tests cover:

- strategy domain model validation;
- deterministic signal and candidate IDs;
- compression breakout adapter triggering one upward and one downward signal;
- cooldown and warm-up behavior;
- rejection summary counts;
- checkpoint contents;
- replay runner duplicate-ID validation;
- report serialization;
- CLI option parsing and output.

Regression tests must prove that the adapter does not use forward labels or
future states. A test should construct a future continuation that would improve
the label but verify that detection is unchanged when those future rows are
removed.

Full verification requires:

- targeted strategy-runner tests;
- `ruff check .`;
- `mypy src`;
- full non-live pytest.

## 11. Acceptance Criteria

This phase is complete when:

1. `compression_breakout` can run through the strategy-runner replay path from
   `market_states_15s` Parquet input;
2. replay emits deterministic `StrategySignal` and `OrderIntentCandidate`
   records;
3. replay reports include run identity, configuration, source paths, signals,
   candidates, rejection summaries, checkpoint, and summary counts;
4. detection uses only states available at the processed timestamp;
5. no account, risk, fill simulation, order execution, or Binance private API
   code is introduced;
6. the new CLI command works locally;
7. unit tests, ruff, mypy, and full non-live tests pass.

## 12. Later Phases

The next backend phases after this scaffold are:

1. add cost-aware deterministic replay with spread, fee, latency, and simple
   fill assumptions;
2. connect the same strategy core to live-data paper trading;
3. add PostgreSQL persistence for runs, checkpoints, signals, and candidates;
4. implement account synchronization, risk, and order execution only after the
   intent contract has stabilized.
