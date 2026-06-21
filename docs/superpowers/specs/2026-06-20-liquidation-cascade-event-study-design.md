# Liquidation Cascade Event Study Design

Date: 2026-06-20

## 1. Status and Scope

This document defines the independent event-study pipeline for Strategy C:
liquidation-cascade momentum.

The phase is descriptive research only. It studies whether short liquidation
activity clusters are followed by directional continuation when price breaks a
recent boundary and aggressive order flow remains aligned.

The phase includes:

- reading `market_states_15s` rows produced by the derived dataset pipeline;
- detecting liquidation-activity clusters with directional price displacement;
- requiring same-direction aggressive-flow continuation;
- recording liquidation, price-break, spread, mark-price, and order-flow context
  available at detection time;
- measuring directional forward returns, maximum favorable excursion, and
  maximum adverse excursion over predeclared horizons;
- writing a JSON event-study report;
- exposing a local research CLI command.

The phase excludes:

- live signal generation;
- order intents, risk, account state, paper trading, or execution;
- Strategy A order-flow impulse logic;
- Strategy B compression-breakout logic;
- strategy combination or duplicate-position handling;
- machine-learning models or optimized parameters.

## 2. Current Data Limitation

`MarketState15s` currently stores:

- `liquidation_count`
- `liquidation_notional`

It does not yet store directional liquidation notional. Binance force-order
messages contain an order side, but the first 15-second aggregate state keeps
only total liquidation activity.

Therefore V0 must not claim to identify long-liquidation versus
short-liquidation events directly. V0 direction is inferred from:

- price displacement during the liquidation cluster;
- break of a frozen recent high or low;
- aggressive trade imbalance aligned with the price move.

This still supports a useful first event study: whether reported liquidation
activity plus same-direction continuation has predictive value. Directional
liquidation fields can be added in a later derived-state schema if this study
shows enough signal to justify the extra contract change.

## 3. Research Contract

The detector must keep event definition separate from labels:

- all event features use only completed 15-second states up to and including
  the detection bucket;
- the recent high/low breakout boundary is frozen before the candidate bucket;
- liquidation cluster activity is measured only within the configured cluster
  window;
- forward returns, MFE, and MAE are labels and never influence detection;
- missing prices, insufficient history, or zero event prices skip the candidate;
- output ordering is deterministic by symbol and detection timestamp.

## 4. Event Definition V0

Input is a set of `MarketState15s` rows grouped by symbol and sorted by
`bucket_start`.

For each candidate bucket:

1. Select the previous `liquidation_window_buckets - 1` states plus the
   candidate state as the liquidation cluster window.
2. Select the previous `breakout_window_buckets` completed states before the
   candidate as the frozen price-break boundary.
3. Require cluster `liquidation_count >= min_liquidation_count`.
4. Require cluster `liquidation_notional >= min_liquidation_notional`.
5. Compute cluster price displacement from first cluster price to candidate
   price.
6. Compute cluster aggressive imbalance:

```text
(aggressive_buy_notional - aggressive_sell_notional)
/ (aggressive_buy_notional + aggressive_sell_notional)
```

7. Detect an upward continuation candidate when:

- cluster price displacement is at least `min_price_move_pct`;
- aggressive imbalance is at least `min_aggressive_imbalance`;
- candidate price breaks the frozen recent high.

8. Detect a downward continuation candidate symmetrically:

- directional cluster displacement is at least `min_price_move_pct`;
- aggressive imbalance is at most `-min_aggressive_imbalance`;
- candidate price breaks the frozen recent low.

9. Require `confirmation_buckets` consecutive states to remain beyond the
   frozen boundary and retain aligned aggressive imbalance.
10. Apply `cooldown_buckets` after an event so one liquidation burst does not
    produce an event on every bucket.

## 5. Event Output

Each event records:

- symbol, inferred direction, and detection timestamp;
- cluster start/end timestamps;
- cluster start/end prices and directional price move;
- breakout level and breakout distance;
- liquidation count and notional in the cluster window;
- cluster trade count and trade notional;
- cluster aggressive buy/sell notional and imbalance;
- detection-bucket spread, midpoint, and mark price when available;
- forward directional returns by configured horizon;
- maximum favorable and adverse returns over available horizons.

Returns are directional: positive values mean continuation in the inferred event
direction.

## 6. Report Output

The JSON report contains:

- schema version;
- generated timestamp;
- configuration;
- source dataset paths;
- ordered event records;
- summary by inferred direction;
- total event count;
- mean forward return by direction and horizon.

The report is a research artifact and is not a trading contract.

## 7. CLI

Add a command:

```text
cml-research liquidation-cascade-study \
  --states-root data/derived/market_states_15s \
  --output reports/liquidation-cascade.json
```

The command reads `.parquet` files recursively, applies the configured V0
detector, writes the JSON report, and prints the event count.

## 8. Acceptance Criteria

This phase is complete when:

1. upward and downward liquidation-cascade continuation events are detected
   deterministically;
2. liquidation-free price moves are rejected;
3. liquidation clusters without aligned aggressive flow are rejected;
4. missing prices and insufficient history are skipped;
5. labels are computed separately from event detection;
6. JSON reports include events, summaries, configuration, and source paths;
7. `cml-research liquidation-cascade-study` works from derived Parquet states;
8. no execution, risk, account, or strategy-combination code is added;
9. unit/integration tests, ruff, mypy, and full non-live tests pass.
