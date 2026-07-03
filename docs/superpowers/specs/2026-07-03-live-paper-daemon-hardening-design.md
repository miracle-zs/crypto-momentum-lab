# Live Paper Daemon Hardening Design

Date: 2026-07-03

## 1. Status And Scope

This document defines the phase after `paper-live-source`. The goal is to turn
bounded live-data paper sessions into a supervised paper daemon that can run for
hours or days, stop on stale inputs, and resume from durable checkpoints.

This phase includes:

- a daemon mode for one selected strategy in paper mode;
- periodic checkpoint persistence while the run is active;
- restart from the last durable `(bucket_start, symbol)` cursor and strategy
  checkpoint;
- market-data freshness checks before each state is accepted;
- process-state and halt integration;
- structured runtime events for status, checkpoint, stale input, and shutdown;
- tests for restart, stale-state halt, and checkpoint cadence.

This phase excludes:

- Binance private APIs;
- account balances, positions, orders, and fills;
- risk approval for real exposure;
- real order submission;
- multi-strategy live arbitration.

## 2. Design Position

The existing `paper-live-source` command is bounded by `max_states` and idle
timeout. That is useful for controlled verification, but live trading needs a
long-running strategy process that behaves like production even before order
submission exists.

The daemon should still simulate fills. Its purpose is to validate runtime
behavior:

```text
closed runtime states -> strategy core -> paper candidates/fills
-> periodic checkpoint -> resumable paper run
```

## 3. Daemon Mode

Add a separate command:

```text
cml-strategy-runner paper-live-daemon \
  --strategy compression_breakout \
  --database-url "$CML_DATABASE_URL" \
  --environment research \
  --run-id paper-live-daemon-20260703-001
```

The command:

- loads exactly one strategy;
- creates or resumes one paper run;
- polls `runtime_market_states_15s` continuously;
- persists periodic strategy checkpoints;
- persists paper report artifacts incrementally where safe;
- exits non-zero on unrecoverable configuration or repository errors;
- enters a halted runtime state for stale data or market-data halt.

## 4. Checkpoint Model

V0 may reuse the existing `strategy_checkpoints` table as the latest
checkpoint per run. It should upsert:

- `run_id`;
- `last_processed_at_by_symbol`;
- `warmup_buckets_by_symbol`;
- `cooldown_buckets_remaining_by_symbol`;
- `payload`;
- `saved_at`.

Add a lightweight runtime event table only if existing strategy-run tables
cannot represent daemon status. The event shape should include:

- `event_id`;
- `run_id`;
- `event_type`;
- `occurred_at`;
- `symbol`;
- `bucket_start`;
- `details`.

The checkpoint cadence is configurable by processed state count and wall-clock
interval. A checkpoint is also written on graceful shutdown.

## 5. Freshness And Halt Integration

Before yielding a market state to the strategy, the daemon checks:

- the row is newer than the last processed cursor;
- `bucket_end` is not older than `max_market_state_age_seconds`;
- market-data process state is not `HALTED`;
- the closed-state publisher watermark has advanced recently enough;
- the repository does not report conflicting or duplicate rows.

If freshness fails, the daemon stops opening new simulated positions and
persists a halted status. In paper mode, it may continue processing states only
for diagnostics after an explicit `--continue-while-halted` option. The default
is conservative halt.

## 6. Restart Semantics

On startup with an existing `run_id`, the daemon:

1. loads the latest checkpoint;
2. restores the strategy state;
3. starts polling after the checkpoint cursor;
4. refuses to resume if the strategy name, config hash, or code commit does
   not match the existing run;
5. records a resume event.

If no checkpoint exists, the daemon starts from `--start-at` or from the oldest
available closed state.

## 7. Error Handling

Recoverable errors:

- no new states yet;
- temporary database connection failure within retry limits;
- market-data process in `SYNCING` or `DEGRADED`.

Unrecoverable errors:

- strategy/config mismatch for a resumed run;
- state order moving backward;
- checkpoint decode failure;
- database conflict on a deterministic artifact;
- market-data `HALTED` without explicit diagnostic override.

All unrecoverable errors persist a terminal event before exit when the database
is available.

## 8. Testing Strategy

Unit tests cover:

- daemon resumes from a checkpoint cursor;
- checkpoint cadence by state count;
- checkpoint cadence by elapsed time with injected clock;
- stale state triggers halt;
- config mismatch rejects resume;
- graceful shutdown writes a checkpoint.

Integration tests cover:

- checkpoint upsert and reload through PostgreSQL;
- paper daemon processing rows from `runtime_market_states_15s`;
- market-data halted state preventing new processing.

## 9. Acceptance Criteria

This phase is complete when:

1. a selected strategy can run as a supervised paper daemon from closed
   PostgreSQL states;
2. the daemon can resume from its last checkpoint;
3. stale data and market-data halt prevent new simulated exposure by default;
4. checkpoints are deterministic and idempotent;
5. existing bounded `paper-live-source` remains unchanged;
6. unit, targeted integration, ruff, mypy, and non-live tests pass.
