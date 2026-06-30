# Paper Trading Runner Scaffold Design

Date: 2026-06-30

## 1. Status and Scope

This document defines the backend phase after deterministic strategy replay and
cost-aware replay fills. The purpose is to introduce a paper-trading runner
scaffold that can drive one approved strategy core from an ordered stream of
closed `MarketState15s` records and produce runtime artifacts without
connecting to a Binance account.

This phase moves the system from batch replay toward continuous operation while
preserving the existing boundary:

```text
market states -> strategy core -> candidates -> paper execution model
```

The phase includes:

- a runner lifecycle for one selected strategy in `paper` mode;
- an injectable market-state source abstraction for tests and later live data;
- deterministic paper fill simulation using the existing replay execution
  assumptions;
- checkpoint, signal, candidate, fill, and rejection records held in an
  in-memory run report;
- a bounded local CLI command that runs paper mode over supplied state input;
- tests proving the strategy core is reused without authenticated exchange
  clients.

The phase excludes:

- Binance private REST or user-data WebSocket clients;
- account balances, positions, margin, leverage, and order reconciliation;
- real exchange orders, cancel/replace, or emergency flattening;
- PostgreSQL persistence for strategy runs;
- multi-strategy arbitration or account trading leases;
- dynamic risk sizing beyond the fixed candidate notional already supported.

## 2. Design Position

The previous replay scaffold proves that `compression_breakout` can emit
standardized signals and order-intent candidates from deterministic state input.
The cost-aware replay addition proves that a candidate can be transformed into a
simulated fill using latency, spread, fee, and slippage assumptions.

The next safe increment is a paper runner loop, not account execution. A paper
runner lets the project exercise the production-style control flow while the
execution side remains simulated. This produces useful runtime artifacts and
surfaces lifecycle bugs before any private-account code exists.

## 3. Runner Contract

The paper runner owns environment orchestration. The strategy core remains
synchronous and environment-neutral.

The V0 runner contract is:

```text
PaperRunnerConfig
PaperMarketStateSource
PaperTradingRunner.run()
PaperTradingRunReport
```

`PaperMarketStateSource` yields closed `MarketState15s` records in deterministic
order. In tests and local CLI usage, the source may be backed by Parquet data.
Future live-data usage can implement the same source interface from a closed
state notification stream.

The runner performs:

1. build a `StrategyRunIdentity` in `paper` mode;
2. instantiate the selected runtime strategy;
3. read each closed market state from the source;
4. call `strategy.on_market_state(state)`;
5. collect signals, candidates, and rejections;
6. simulate fills for newly emitted candidates using the paper execution model;
7. update the latest strategy checkpoint;
8. stop when the source ends or a configured maximum state count is reached.

V0 supports only `compression_breakout`, matching the existing replay support.

## 4. Paper Execution Model

Paper fills reuse the existing replay execution concepts:

- latency in 15-second buckets;
- marketable side pricing from best bid/ask;
- taker fee rate;
- fixed slippage in basis points;
- expired or rejected fills when no eligible state or price is available.

The key difference from batch replay is timing. The paper runner can only fill a
candidate after enough later states have arrived. It keeps pending candidates in
memory and evaluates them as each new state arrives. At shutdown, remaining
pending candidates are marked expired if their expiry has passed or left pending
with a clear status if the source ended before fill eligibility.

V0 does not simulate exits, PnL, inventory, or position netting. It records
entry-style fills only because the current strategy emits entry candidates only.

## 5. Runtime Artifacts

The paper run report contains:

- schema version;
- generated timestamp;
- run identity;
- strategy configuration hash;
- source description;
- processed state count and symbol count;
- ordered signals;
- ordered order-intent candidates;
- ordered paper fill records;
- pending candidate summary;
- rejection summary by reason and symbol;
- final strategy checkpoint;
- summary counts by side, symbol, and fill status.

The report is local and deterministic for a deterministic source. It is not a
trading audit log.

## 6. CLI

Add a new command:

```text
cml-strategy-runner paper \
  --strategy compression_breakout \
  --states-root data/derived/market_states_15s \
  --output reports/compression-breakout-paper.json
```

Initial options:

- `--strategy`;
- `--states-root`;
- `--output`;
- strategy-specific compression breakout thresholds;
- `--candidate-notional`;
- `--candidate-ttl-buckets`;
- `--execution-latency-buckets`;
- `--taker-fee-rate`;
- `--slippage-bps`;
- `--max-states`;
- `--run-id`;
- `--generated-at`.

The command prints a short summary:

```text
Paper run completed: states=<n> signals=<n> candidates=<n> fills=<n>
```

## 7. Error Handling

The runner raises explicit errors when:

- an unknown strategy is requested;
- the market-state source yields no records;
- a state timestamp is naive;
- input states move backward for the same symbol;
- a candidate references an unknown signal;
- duplicate signal, candidate, or fill IDs are produced;
- configuration values are invalid.

Data gaps and missing prices do not crash the runner when the strategy or paper
execution model can record a rejection instead.

## 8. Testing Strategy

Unit tests cover:

- source-backed paper runner emits the same strategy signals as replay for the
  same ordered states;
- candidates are filled only after the configured latency states arrive;
- candidates remain pending or expire when the source ends too early;
- missing bid/ask or midpoint produces a rejected paper fill;
- max-state limits stop the runner deterministically;
- duplicate IDs and unknown strategies fail explicitly;
- CLI option parsing and output path writing;
- report serialization of Decimal and timestamp values.

Regression tests must prove that the strategy runtime receives only the current
closed state at each step and that paper fill simulation does not feed execution
results back into the strategy in V0.

Full verification requires:

- targeted paper-runner tests;
- existing replay and compression runtime tests;
- `ruff check .`;
- `mypy src`;
- full non-live pytest.

## 9. Acceptance Criteria

This phase is complete when:

1. `compression_breakout` can run through `paper` mode from ordered
   `MarketState15s` input;
2. paper mode reuses the existing strategy core and standardized records;
3. paper fills are produced from later market states using explicit execution
   assumptions;
4. pending, expired, filled, and rejected candidate outcomes are represented
   deterministically;
5. the CLI can write a local paper run report;
6. no authenticated Binance, real account, real order, risk engine, or
   PostgreSQL strategy persistence code is introduced;
7. targeted tests, ruff, mypy, and full non-live tests pass.

## 10. Later Phases

The next backend phases after this scaffold are:

1. add PostgreSQL persistence for paper runs, checkpoints, signals, candidates,
   and paper fills;
2. connect a live closed-state source to the paper runner;
3. add account synchronization and risk controls in a separate
   `execution-account` boundary;
4. only then add live order submission for one selected strategy.
