# Derived Dataset Export Design

Date: 2026-06-18

## 1. Status and Scope

This document defines the backend phase after deterministic raw replay,
Binance normalization, and 15-second aggregation. The phase exports rebuildable
research datasets from immutable raw archive files.

The phase includes:

- reading finalized raw `.jsonl.zst` archive files;
- normalizing raw envelopes into typed market events;
- aggregating normalized events into closed 15-second market states;
- writing `market_events` and `market_states_15s` Parquet files;
- writing local JSON manifests for each derived Parquet file;
- exposing a local research command for one-shot dataset derivation.

The phase excludes:

- PostgreSQL persistence for Parquet manifests;
- dataset compaction across many small files;
- strategy-specific feature tables;
- signal generation;
- event-study reports;
- paper trading or execution simulation.

## 2. Architecture Decision

The implementation adds a file-backed derived-data path:

```text
RawArchiveReader
        |
        v
BinanceMarketNormalizer
        |
        +--> ParquetDatasetWriter(market_events)
        |
        v
MarketStateAggregator15s
        |
        v
ParquetDatasetWriter(market_states_15s)
```

Parquet writing belongs in `persistence.parquet`. The end-to-end one-shot
builder belongs in `research.datasets` because it is an offline research
operation, not part of the long-running `market-data` process.

The writer uses `pyarrow` because it provides direct Parquet read/write
support without introducing a larger dataframe dependency. DuckDB and Polars
remain analytical consumers and are not required for this write path.

## 3. Dataset Layout

Output root:

```text
data/derived/
  market_events/
    date=YYYY-MM-DD/
      stream=<stream>/
        symbol=<symbol>/
          part-<manifest-id>.parquet
  market_states_15s/
    date=YYYY-MM-DD/
      symbol=<symbol>/
        part-<manifest-id>.parquet
  _manifests/
    <manifest-id>.json
```

`manifest-id` is deterministic from dataset name, output relative path, input
raw paths, input raw checksums, row count, and output checksum. Re-running the
same build into the same empty output root produces the same manifest ID and
same relative paths.

## 4. Row Schemas

`market_events` uses one flat sparse schema with common source fields and
stream-specific nullable fields. This keeps the first derived dataset easy to
query across streams while preserving source traceability.

Common fields:

- `schema_version`
- `exchange`
- `environment`
- `symbol`
- `event_at`
- `received_at`
- `source_connection_session_id`
- `source_local_sequence`
- `source_stream`
- `event_type`

Stream-specific fields include trade, top-of-book, mark-price, kline, and
liquidation columns. Non-applicable fields are null.

`market_states_15s` stores the current `MarketState15s` contract as one row per
symbol and UTC-aligned bucket.

Decimal values are written as strings in this phase. This avoids accidental
precision loss and keeps schemas stable across symbols with different decimal
scales. Analytical consumers can cast to decimal or double explicitly.

## 5. Manifest Contract

Each Parquet output file has one JSON manifest:

```text
manifest_id
dataset_name
schema_version
relative_path
row_count
input_relative_paths
input_sha256
output_sha256
first_event_at
last_event_at
created_at
```

The manifest is file-local. PostgreSQL manifest storage is intentionally
deferred; the local manifest is enough for deterministic rebuild and testable
research workflows.

## 6. Error Handling

The builder fails closed:

- invalid raw archive rows raise reader errors;
- malformed Binance payloads raise normalization errors;
- writing an empty dataset raises a validation error unless the caller
  explicitly filters to no inputs before calling the writer;
- output paths are always relative under the configured output root;
- temporary Parquet and manifest files are atomically renamed into place.

The builder does not silently skip malformed records. Research can add explicit
quality-reporting later, but raw-to-derived conversion must remain auditable.

## 7. Acceptance Criteria

This phase is complete when:

1. `pyarrow` is declared as a project dependency;
2. normalized market events can be converted into deterministic Parquet rows;
3. 15-second market states can be converted into deterministic Parquet rows;
4. Parquet files and JSON manifests are written under the approved layout;
5. a one-shot command can derive both datasets from raw archive files;
6. derived rows retain raw source references;
7. no strategy feature, signal, paper trading, or execution logic is added;
8. unit/integration tests, ruff, and mypy pass.
