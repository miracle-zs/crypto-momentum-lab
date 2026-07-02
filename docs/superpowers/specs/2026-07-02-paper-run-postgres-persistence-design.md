# Paper Run PostgreSQL Persistence Design

Date: 2026-07-02

## 1. Status and Scope

This document defines the backend phase after the paper-trading runner
scaffold. The purpose is to persist paper-run artifacts to PostgreSQL so paper
trading can be queried, resumed from durable evidence, and compared across
runs without relying only on local JSON reports.

This phase includes:

- PostgreSQL tables for strategy runs, signals, order-intent candidates,
  paper fills, and checkpoints;
- a repository boundary that saves a complete `PaperTradingRunReport`
  transactionally;
- idempotent save behavior for repeated writes of the same deterministic paper
  run;
- query helpers for loading run summaries and run artifacts in deterministic
  order;
- an optional CLI persistence path for `cml-strategy-runner paper`;
- migrations and tests for the new persistence model.

This phase excludes:

- Binance private REST or user-data websocket clients;
- real account balances, positions, margin, leverage, or reconciliation;
- live order submission, cancellation, replacement, or emergency flattening;
- account trading leases and multi-strategy live arbitration;
- risk sizing, order quantization, or approval logic;
- a live closed-state source.

## 2. Design Position

The paper runner currently produces a deterministic in-memory report and can
write it to JSON. That is enough for local inspection, but it is not enough for
runtime operation. Later phases need durable run identity, checkpoint state,
signal and candidate records, and paper fill outcomes.

The safe next increment is therefore persistence of existing paper artifacts,
not account execution. The strategy core and paper execution model remain
unchanged. PostgreSQL records what happened; it does not become the authority
for market data, account state, or exchange execution.

## 3. Persistence Model

The new tables are append-oriented around `run_id`.

### 3.1 `strategy_runs`

One row per replay, paper, or future live run.

Fields:

- `run_id` primary key;
- `strategy_name`;
- `strategy_version`;
- `config_hash`;
- `run_mode`;
- `code_commit`;
- `created_at`;
- `generated_at`;
- `schema_version`;
- `source_paths` as JSONB string array;
- `source_description`;
- `execution_config` as JSONB;
- `input_state_count`;
- `processed_symbol_count`;
- `signal_count`;
- `candidate_count`;
- `fill_count`;
- `pending_candidate_count`;
- `rejection_summary` as JSONB;
- `summary_counts` as JSONB;
- `fill_summary` as JSONB.

`run_id` is deterministic or user-supplied. Saving the same run again with the
same identity and records is idempotent. Saving a row with the same `run_id` but
conflicting core metadata fails explicitly.

### 3.2 `strategy_signals`

One row per standardized signal.

Fields:

- `signal_id` primary key;
- `run_id` foreign key to `strategy_runs`;
- `strategy_name`;
- `strategy_version`;
- `config_hash`;
- `symbol`;
- `side`;
- `detected_at`;
- `source_state_at`;
- `reason`;
- `features` as JSONB;
- `reference_prices` as JSONB.

Indexes support lookup by `(run_id, detected_at, symbol)` and by
`(run_id, symbol)`.

### 3.3 `order_intent_candidates`

One row per paper/replay order-intent candidate.

Fields:

- `candidate_id` primary key;
- `signal_id` foreign key to `strategy_signals`;
- `run_id` foreign key to `strategy_runs`;
- `strategy_name`;
- `strategy_version`;
- `config_hash`;
- `symbol`;
- `side`;
- `entry_type`;
- `limit_price`;
- `desired_notional`;
- `reduce_only`;
- `expires_at`;
- `created_at`;
- `reason`;
- `features` as JSONB.

The database enforces that candidates reference existing signals. V0 still
stores candidates, not executable exchange orders.

### 3.4 `paper_fills`

One row per simulated paper fill outcome.

Fields:

- `fill_id` primary key;
- `candidate_id` foreign key to `order_intent_candidates`;
- `signal_id` foreign key to `strategy_signals`;
- `run_id` foreign key to `strategy_runs`;
- `symbol`;
- `side`;
- `status`;
- `target_fill_at`;
- `filled_at`;
- `requested_notional`;
- `filled_notional`;
- `quantity`;
- `reference_midpoint`;
- `spread`;
- `fill_price`;
- `fee`;
- `total_cost`;
- `cost_bps`;
- `reason`.

Indexes support lookup by `(run_id, target_fill_at, symbol)` and by
`(run_id, status)`.

### 3.5 `strategy_checkpoints`

One row per saved final checkpoint in V0.

Fields:

- `run_id` primary key and foreign key to `strategy_runs`;
- `last_processed_at_by_symbol` as JSONB;
- `warmup_buckets_by_symbol` as JSONB;
- `cooldown_buckets_remaining_by_symbol` as JSONB;
- `payload` as JSONB;
- `saved_at`.

V0 stores only the final checkpoint from a paper run. Periodic checkpointing is
a later live-runtime concern.

## 4. Repository Boundary

Add `PostgresStrategyRunRepository` under `persistence.postgres`.

The repository exposes:

```text
save_paper_report(report)
load_run_summary(run_id)
load_paper_report_artifacts(run_id)
```

`save_paper_report(report)` writes the run row, signals, candidates, fills, and
checkpoint in one transaction. It validates deterministic relationships before
writing:

- every signal belongs to the report run;
- every candidate belongs to the report run;
- every candidate references a saved signal;
- every fill belongs to the report run;
- every fill references a saved candidate and signal;
- the final checkpoint is JSON-serializable.

If an insert conflicts with an existing primary key, the repository treats it
as idempotent only when the persisted core fields match the incoming record.
Conflicting rows raise `ValueError` with a direct message such as
`strategy run conflict`, `signal conflict`, or `paper fill conflict`.

## 5. CLI Behavior

`cml-strategy-runner paper` keeps the existing local JSON output behavior.
Persistence is opt-in:

```text
cml-strategy-runner paper \
  --strategy compression_breakout \
  --states-root data/derived/market_states_15s \
  --output reports/compression-breakout-paper.json \
  --persist \
  --database-url postgresql+asyncpg://cml:cml@localhost:54329/cml
```

Rules:

- `--persist` requires `--database-url` or `CML_DATABASE_URL`;
- without `--persist`, no database connection is created;
- persistence happens after a successful paper run and JSON write;
- a persistence failure makes the command fail rather than silently losing the
  durable record;
- the command summary includes `persisted=true` only after the transaction
  commits.

## 6. Error Handling

The persistence layer raises explicit errors for:

- missing run identity fields;
- mismatched run IDs across report artifacts;
- unknown candidate or signal references;
- primary-key conflicts with different core fields;
- non-JSON-serializable checkpoint or summary payloads;
- database constraint failures.

The runner remains responsible for strategy execution errors. The repository is
responsible only for durable storage and relationship validation.

## 7. Testing Strategy

Unit tests cover:

- mapping a `PaperTradingRunReport` to row dictionaries;
- relationship validation before database writes;
- idempotency comparison detects matching and conflicting records;
- CLI does not connect to the database unless `--persist` is set;
- CLI rejects `--persist` without a database URL.

Integration tests cover:

- migrations create all strategy-run persistence tables;
- saving a paper report persists run, signal, candidate, fill, and checkpoint
  rows;
- saving the same report twice is idempotent;
- conflicting records with the same IDs fail explicitly;
- loaded summaries and artifacts are ordered deterministically.

Full verification requires:

- targeted strategy-run persistence unit tests;
- targeted PostgreSQL integration tests when the local test database is
  available;
- existing paper runner and replay tests;
- `ruff check .`;
- `mypy src`;
- full non-live pytest when Docker/PostgreSQL is available.

## 8. Acceptance Criteria

This phase is complete when:

1. paper-run artifacts can be saved to PostgreSQL through a focused repository;
2. repeated saves of the same deterministic report are idempotent;
3. conflicting records fail explicitly instead of overwriting evidence;
4. `cml-strategy-runner paper` can optionally persist a successful report;
5. JSON output remains supported and remains the default;
6. no authenticated Binance client, account state, real orders, risk engine, or
   live state source is introduced;
7. unit tests, targeted integration tests, ruff, and mypy pass in the available
   environment.

## 9. Later Phases

Later backend phases are:

1. connect a live closed-state source to the paper runner;
2. add periodic checkpoint persistence for long-running paper mode;
3. add account synchronization and risk controls in the separate
   `execution-account` boundary;
4. add live order submission only after paper persistence and account
   reconciliation are both stable.
