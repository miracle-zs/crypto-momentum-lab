# Replay Normalization and 15-Second State Design

Date: 2026-06-17

## 1. Status and Scope

This document defines the backend phase after WebSocket capture and raw archive
stabilization. The phase turns immutable raw archive files into deterministic
normalized market events and closed 15-second market states for research and
replay.

The phase includes:

- reading finalized Zstandard JSONL archive files;
- reconstructing `RawEnvelope` records exactly from archived rows;
- replaying archived envelopes in deterministic local receive order;
- normalizing Binance aggregate trades, best bid/ask updates, mark-price
  updates, one-minute klines, and liquidation snapshots;
- aggregating normalized events into 15-second symbol states.

The phase excludes:

- Parquet dataset writing and compaction;
- PostgreSQL storage for derived state;
- strategy-specific feature tables;
- signal generation;
- paper trading;
- authenticated account state;
- execution and risk controls.

## 2. Architecture Decision

The implementation adds three small layers under the existing market-data and
persistence boundaries:

```text
RawJsonlArchive files
        |
        v
RawArchiveReader
        |
        v
ReplayEventStream
        |
        v
BinanceMarketNormalizer
        |
        v
MarketStateAggregator15s
```

The reader belongs in `persistence.raw_files` because it understands the raw
archive row format and compression. The normalizer and aggregator belong in
`market_data.normalization` and `market_data.aggregation` because they are
derived market-data concerns, not strategy concerns.

Domain dataclasses for normalized events and 15-second states live in
`domain.market.models`. These are stable contracts that future replay,
research, paper, and live adapters can consume without depending on Binance
payload details.

## 3. Deterministic Replay Order

Replay order is based on local observation, not exchange event time. Finalized
files can be read in any filesystem order, but the replay stream emits
envelopes sorted by:

```text
received_at
received_monotonic_ns
connection_session_id
local_sequence
```

This preserves the architecture requirement that replay processes information
in the order the system observed it. Exchange event time remains part of each
event and is used for bucket assignment where available.

## 4. Normalized Event Contracts

Each normalized event carries:

- `schema_version`;
- `exchange`;
- `environment`;
- `symbol`;
- `event_at`;
- `received_at`;
- `source_connection_session_id`;
- `source_local_sequence`;
- `source_stream`.

The first normalized contracts are:

- `NormalizedAggTrade`
- `NormalizedBookTicker`
- `NormalizedMarkPrice`
- `NormalizedKline1m`
- `NormalizedLiquidation`

Aggregate trade aggressive side is inferred from Binance's buyer-maker flag:
`m=true` means the buyer was maker, so the aggressive side was sell;
`m=false` means the aggressive side was buy.

Liquidation events preserve Binance order side as `order_side`. This phase
does not infer which account side was liquidated because the public stream is
a censored snapshot, not a complete liquidation ledger.

## 5. 15-Second Market State

The 15-second aggregator consumes normalized events and emits one state per
symbol and UTC-aligned bucket:

```text
[bucket_start, bucket_start + 15 seconds)
```

Bucket assignment uses `event_at`. If an event does not have an exchange event
time, the normalizer uses the raw envelope's `received_at` as its event time
and the event remains traceable to its raw source.

The first state schema includes:

- trade OHLC from aggregate trades;
- trade count and total notional;
- aggressive buy and sell notional;
- last best bid and ask observed in the bucket;
- spread and midpoint from the last best bid and ask;
- liquidation count and reported liquidation notional;
- last mark price;
- number of closed one-minute kline updates observed in the bucket;
- first and last local receive timestamp;
- source event count.

The aggregator does not forward-fill across buckets in this phase. Missing
trade, book, mark, or liquidation fields remain `None` or zero so downstream
research can decide how to handle gaps.

## 6. Error Handling

Archive reading rejects malformed rows, unsupported schema versions, invalid
enum values, and naive timestamps with explicit exceptions. It does not skip
bad archived records silently.

Normalization rejects malformed Binance payloads with stream-specific errors.
Malformed records remain recoverable from raw archive and can be counted by
future quality reports.

Aggregation assumes it receives normalized events. It ignores no events
silently; every supported normalized event either updates a bucket or raises a
type error if the event type is unknown.

## 7. Testing

Unit tests cover:

- round-tripping archived rows back into `RawEnvelope`;
- reading multiple finalized archive files in deterministic replay order;
- normalization for all five Binance stream types;
- aggregate-trade aggressive side inference;
- rejection of malformed payloads;
- 15-second bucket boundaries;
- OHLC, aggressive notional, top-of-book, liquidation, mark-price, and closed
  kline aggregation.

Integration tests reuse the existing archive writer to create compressed
archive files, then read them back through the new reader.

## 8. Acceptance Criteria

This phase is complete when:

1. finalized `.jsonl.zst` raw files can be read into validated `RawEnvelope`
   objects;
2. replay emits records in deterministic local receive order;
3. all five captured Binance streams normalize into typed market events;
4. 15-second market states are reproducible from the same ordered event input;
5. normalized and aggregated records retain raw source references;
6. no strategy feature, signal, execution, or Parquet writing logic is added;
7. unit/integration tests, ruff, and mypy pass.
