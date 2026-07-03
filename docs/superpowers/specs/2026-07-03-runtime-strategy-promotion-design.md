# Runtime Strategy Promotion Design

Date: 2026-07-03

## 1. Status And Scope

This document defines the work required to make all three independently
researched strategy families selectable in replay, paper, daemon, shadow, and
future live modes.

The three strategy families are:

1. `compression_breakout`
2. `orderflow_impulse`
3. `liquidation_cascade`

This phase includes:

- a shared runtime strategy registry;
- runtime strategy implementations for `orderflow_impulse` and
  `liquidation_cascade`;
- configuration schemas for all runtime strategies;
- strategy-specific checkpoint payloads;
- CLI strategy selection through the existing runner commands;
- paper validation for each strategy from deterministic states.

This phase excludes:

- combining strategies into one rule set;
- portfolio allocation across strategies;
- account synchronization;
- risk approval for real orders;
- real order submission.

## 2. Design Position

The current code can take `compression_breakout` through runtime paper flow.
The other two families have research/event-study code but do not yet expose the
same runtime contract.

To let the operator choose any one strategy for a Binance account, each
strategy must present the same interface:

```text
MarketState15s + strategy checkpoint -> decision
decision -> signals, order-intent candidates, rejections, next checkpoint
```

Each strategy remains independent. No strategy may read another strategy's
features, signals, checkpoint, or performance attribution.

## 3. Strategy Registry

Add a registry under `strategy_runner` or `strategies` that maps:

```text
strategy_name -> factory(config, identity) -> runtime strategy
```

The registry owns:

- allowed strategy names;
- config parsing;
- version string;
- default paper parameters;
- checkpoint restore validation.

CLI commands should stop constructing `CompressionBreakoutRuntimeStrategy`
directly. They should ask the registry to build the selected strategy. Unknown
strategy names fail before any run record is created.

## 4. Runtime Configuration

Each strategy has a versioned runtime config dataclass:

- `compression_breakout`: existing compression, breakout, cooldown, and
  forward-horizon fields;
- `orderflow_impulse`: impulse window, imbalance threshold, trade intensity,
  spread guard, confirmation buckets, cooldown, exit policy;
- `liquidation_cascade`: liquidation notional threshold, price-break context,
  mark-price divergence, continuation window, spread guard, cooldown, exit
  policy.

Config hashes must be deterministic and recorded in every run, signal,
candidate, fill, and checkpoint.

## 5. Runtime Behavior

Each strategy must implement:

- warm-up tracking;
- deterministic signal IDs;
- deterministic candidate IDs;
- cooldown;
- rejection reasons;
- checkpoint serialization;
- no side effects outside its returned decision.

The runtime contract should not contain database, filesystem, Binance, or
network calls.

## 6. Strategy-Specific Notes

### 6.1 `orderflow_impulse`

Consumes recent 15-second market states and detects directional price impulse
confirmed by aggressive notional imbalance and trade intensity. It must reject
signals when spread or missing midpoint makes execution assumptions unsafe.

### 6.2 `liquidation_cascade`

Consumes liquidation counts/notional, price break context, and continuation
signals. It must be conservative because the public liquidation stream is
sampled and incomplete. Missing liquidation evidence produces no signal rather
than a weak signal.

## 7. Testing Strategy

Unit tests cover:

- registry accepts the three approved names and rejects unknown names;
- each runtime strategy emits deterministic signals for fixture states;
- each runtime strategy serializes and restores checkpoints;
- cooldown and warm-up behavior are deterministic;
- missing prices/spread/liquidation context produce explicit rejections.

Paper tests cover:

- each strategy can run through `run_paper_trading`;
- each strategy writes a valid paper report;
- strategy names and config hashes propagate into persisted artifacts.

## 8. Acceptance Criteria

This phase is complete when:

1. `compression_breakout`, `orderflow_impulse`, and `liquidation_cascade` are
   selectable by `--strategy`;
2. each selected strategy can run in replay/paper/live-paper daemon mode;
3. all strategy artifacts remain namespaced by strategy and config hash;
4. no strategy-combination logic is added;
5. no account or real-order code is introduced;
6. unit, paper, persistence, ruff, mypy, and non-live tests pass.
