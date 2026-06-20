# Order-Flow Impulse Event Study Design

Date: 2026-06-20

## 1. Status and Scope

This document defines the independent event-study pipeline for Strategy A:
order-flow impulse momentum.

The phase is descriptive research only. It studies whether short-horizon price
impulses continue when aggressive order flow and trade intensity remain aligned.

The phase includes:

- reading `market_states_15s` rows produced by the derived dataset pipeline;
- detecting upward and downward order-flow impulse events;
- recording frozen price-break, trade-imbalance, notional-intensity, spread, and
  liquidation context available at detection time;
- measuring directional forward returns, maximum favorable excursion, and
  maximum adverse excursion over predeclared horizons;
- writing a JSON event-study report;
- exposing a local research CLI command.

The phase excludes:

- live signal generation;
- order intents, risk, account state, paper trading, or execution;
- Strategy B compression logic;
- Strategy C liquidation-cascade logic;
- combining strategy outputs;
- machine-learning models or optimized parameters.

## 2. Research Contract

The detector must keep the event definition separate from labels:

- all event features use only completed 15-second states up to and including
  the detection bucket;
- the recent price-break boundary is frozen before the candidate bucket is
  evaluated;
- historical notional baseline uses only states before the impulse window;
- forward returns, MFE, and MAE are labels and never influence detection;
- missing prices or insufficient history skip the candidate;
- output ordering is deterministic by symbol and detection timestamp.

The V0 implementation uses explicit fixed thresholds to validate the pipeline.
Later research can replace fixed thresholds with rolling quantiles and
walk-forward parameter selection.

## 3. Event Definition V0

Input is a set of `MarketState15s` rows grouped by symbol and sorted by
`bucket_start`.

For each candidate bucket:

1. Select the previous `impulse_window_buckets - 1` states plus the candidate
   state as the impulse window.
2. Select the previous `baseline_window_buckets` states ending before the
   impulse window as the activity baseline.
3. Select the previous `breakout_window_buckets` completed states before the
   candidate bucket as the frozen price-break boundary.
4. Compute directional return from impulse-window start price to candidate
   price.
5. Compute impulse-window aggressive imbalance:

```text
(aggressive_buy_notional - aggressive_sell_notional)
/ (aggressive_buy_notional + aggressive_sell_notional)
```

6. Compute notional intensity:

```text
impulse_window_notional / baseline_average_notional_for_same_bucket_count
```

7. Detect an upward event when:

- return is at least `min_return_pct`;
- aggressive imbalance is at least `min_aggressive_imbalance`;
- notional intensity is at least `min_notional_intensity`;
- candidate price breaks the frozen recent high.

8. Detect a downward event symmetrically:

- directional return is at least `min_return_pct`;
- aggressive imbalance is at most `-min_aggressive_imbalance`;
- notional intensity is at least `min_notional_intensity`;
- candidate price breaks the frozen recent low.

9. Require `confirmation_buckets` consecutive states to remain beyond the
   frozen boundary and retain aligned aggressive imbalance.
10. Apply `cooldown_buckets` after an event so one impulse does not produce an
    event on every bucket.

## 4. Event Output

Each event records:

- symbol, direction, and detection timestamp;
- impulse start/end timestamps;
- breakout boundary and breakout distance;
- impulse return percentage;
- impulse trade count and trade notional;
- impulse aggressive buy/sell notional and imbalance;
- baseline notional and notional intensity;
- detection-bucket spread and midpoint when available;
- liquidation count and notional in the impulse window;
- forward directional returns by configured horizon;
- maximum favorable and adverse returns over available horizons.

Returns are directional: positive values mean continuation in the event
direction.

## 5. Report Output

The JSON report contains:

- schema version;
- generated timestamp;
- configuration;
- source dataset paths;
- ordered event records;
- summary by direction;
- total event count;
- mean forward return by direction and horizon.

The report is a research artifact and is not a trading contract.

## 6. CLI

Add a command:

```text
cml-research order-flow-impulse-study \
  --states-root data/derived/market_states_15s \
  --output reports/order-flow-impulse.json
```

The command reads `.parquet` files recursively, applies the configured V0
detector, writes the JSON report, and prints the event count.

## 7. Acceptance Criteria

This phase is complete when:

1. upward and downward order-flow impulse events are detected deterministically;
2. price-only impulses without aligned aggressive flow are rejected;
3. high-imbalance moves without notional expansion are rejected;
4. missing prices and insufficient history are skipped;
5. labels are computed separately from event detection;
6. JSON reports include events, summaries, configuration, and source paths;
7. `cml-research order-flow-impulse-study` works from derived Parquet states;
8. no execution, risk, account, or strategy-combination code is added;
9. unit/integration tests, ruff, mypy, and full non-live tests pass.
