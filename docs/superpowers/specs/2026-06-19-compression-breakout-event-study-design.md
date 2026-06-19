# Compression Breakout Event Study Design

Date: 2026-06-19

## 1. Status and Scope

This document defines the first independent strategy research pipeline after
raw capture, replay normalization, 15-second aggregation, and derived Parquet
exports.

The phase studies Strategy B: volatility-compression breakout momentum. It is a
descriptive event study only.

The phase includes:

- reading `market_states_15s` Parquet rows;
- reconstructing typed `MarketState15s` records;
- detecting pre-event compression ranges from completed 15-second states;
- detecting upward and downward breakouts beyond the frozen range boundary;
- recording participation, order-flow imbalance, spread, and liquidation
  context available at the event time;
- measuring forward returns, maximum favorable excursion, and maximum adverse
  excursion over predeclared horizons;
- writing a JSON research report for offline review;
- exposing a local research CLI command.

The phase excludes:

- live signal generation;
- order intents, execution, paper trading, or risk management;
- combining compression with order-flow impulse or liquidation strategies;
- machine-learning model training;
- parameter optimization or symbol-specific tuning;
- writing strategy features back into PostgreSQL.

## 2. Research Contract

The event study must preserve the boundary between event definition and labels:

- compression and breakout fields are computed only from states whose
  `bucket_start` is at or before the event bucket;
- the compression range is frozen before the breakout bucket is evaluated;
- forward returns, MFE, and MAE are labels and never influence event detection;
- every output event records enough configuration metadata to reproduce its
  definition;
- missing prices or incomplete lookback windows fail closed by skipping the
  candidate.

The implementation is intentionally deterministic. Given the same derived
dataset rows and configuration, it must emit the same ordered event list and
summary.

## 3. Event Definition V0

Input is one or more `MarketState15s` rows grouped by symbol and sorted by
`bucket_start`.

For each candidate bucket:

1. Select the previous `compression_window_buckets` completed states.
2. Require every lookback state to have a usable price.
3. Compute the frozen range high, range low, and range midpoint from lookback
   highs/lows, falling back to close or midpoint when high/low is missing.
4. Define compression when:
   `range_width / range_midpoint <= max_range_width_pct`.
5. Define an upward breakout when the candidate price is above:
   `range_high * (1 + min_breakout_pct)`.
6. Define a downward breakout when the candidate price is below:
   `range_low * (1 - min_breakout_pct)`.
7. Require price acceptance for `acceptance_buckets` consecutive states outside
   the prior range boundary.
8. Apply `cooldown_buckets` after an event so the same breakout does not produce
   one event per 15-second bucket.

This fixed-threshold V0 is not the final research definition. It exists to
validate the pipeline and establish a reproducible baseline before percentile or
walk-forward parameter selection is added.

## 4. Event Output

Each event records:

- symbol, direction, and detection timestamp;
- range start/end timestamps;
- range high, range low, range midpoint, and range width percentage;
- breakout price and breakout distance percentage;
- trade count and trade notional in the detection bucket;
- aggressive buy/sell notional and imbalance;
- spread and midpoint when available;
- liquidation count and notional;
- forward returns for configured horizons;
- maximum favorable and adverse returns over the configured horizon window.

Returns are directional. A positive value means the move continued in the event
direction. Upward events use `(future_price - event_price) / event_price`.
Downward events use `(event_price - future_price) / event_price`.

## 5. Report Output

The JSON report contains:

- schema version;
- configuration;
- source dataset paths;
- generated timestamp;
- ordered event records;
- summary by direction;
- total event count;
- mean forward return per horizon when labels are available.

The report is a local research artifact. It is not an execution contract.

## 6. CLI

Add a `cml-research compression-breakout-study` command:

```text
cml-research compression-breakout-study \
  --states-root data/derived/market_states_15s \
  --output reports/compression-breakout.json
```

The command reads `.parquet` files recursively from the state root, applies the
configured detector, writes the JSON report, and prints a short summary with
event counts.

The CLI accepts explicit configuration flags for V0 thresholds and horizons so
research runs are reproducible from the command line.

## 7. Acceptance Criteria

This phase is complete when:

1. `market_states_15s` Parquet files can be read back into typed states;
2. upward and downward compression breakouts are detected deterministically;
3. non-compressed ranges and missing-price windows are skipped;
4. forward labels are computed separately from event detection;
5. JSON reports contain event records, summaries, configuration, and source
   paths;
6. the research CLI can run the event study from derived Parquet files;
7. no live signal, paper execution, or account code is added;
8. unit/integration tests, ruff, mypy, and the full non-live test suite pass.
