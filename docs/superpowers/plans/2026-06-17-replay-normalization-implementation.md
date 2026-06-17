# Replay Normalization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first deterministic raw-replay, Binance normalization, and 15-second market-state layer from immutable raw archive files.

**Architecture:** Add a raw archive reader in `persistence.raw_files`, normalized market event contracts in `domain.market.models`, Binance payload normalization in `market_data.normalization`, and pure-Python 15-second aggregation in `market_data.aggregation`. Keep this phase derived-data only: no Parquet writer, no strategy signals, no execution path.

**Tech Stack:** Python 3.13, dataclasses, Decimal, zstandard JSONL archives, pytest, ruff, mypy.

---

## File Structure

- Modify: `src/crypto_momentum_lab/domain/market/models.py`
  - Add normalized event dataclasses, trade side enum, order side enum, and 15-second state dataclass.
- Create: `src/crypto_momentum_lab/persistence/raw_files/reader.py`
  - Deserialize archived JSONL rows into `RawEnvelope` and replay finalized Zstandard files in deterministic local receive order.
- Create: `src/crypto_momentum_lab/market_data/normalization/__init__.py`
  - Export normalizer API.
- Create: `src/crypto_momentum_lab/market_data/normalization/binance.py`
  - Convert `RawEnvelope.raw_payload` values into typed normalized market events.
- Create: `src/crypto_momentum_lab/market_data/aggregation/__init__.py`
  - Export aggregation API.
- Create: `src/crypto_momentum_lab/market_data/aggregation/state_15s.py`
  - Aggregate normalized market events into closed UTC-aligned 15-second market states.
- Create: `tests/unit/persistence/raw_files/test_reader.py`
  - Cover archive row deserialization and replay ordering.
- Create: `tests/unit/market_data/normalization/test_binance.py`
  - Cover all five stream normalizers and malformed payload rejection.
- Create: `tests/unit/market_data/aggregation/test_state_15s.py`
  - Cover bucket boundaries and state field aggregation.
- Modify: `tests/integration/raw_files/test_archive.py`
  - Add integration coverage that writes archive files with the existing archive writer and reads them back through the new reader.

## Task 1: Domain Market Contracts

**Files:**
- Modify: `src/crypto_momentum_lab/domain/market/models.py`
- Test: `tests/unit/domain/market/test_models.py`

- [ ] **Step 1: Write failing model tests**

Add tests that instantiate `NormalizedAggTrade` and `MarketState15s`, verify Decimal fields are preserved, and verify bucket timestamps must be aware.

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/domain/market/test_models.py -v
```

Expected: fail because `NormalizedAggTrade`, `AggressorSide`, and `MarketState15s` do not exist.

- [ ] **Step 3: Add minimal contracts**

Add:

- `AggressorSide`
- `OrderSide`
- `NormalizedAggTrade`
- `NormalizedBookTicker`
- `NormalizedMarkPrice`
- `NormalizedKline1m`
- `NormalizedLiquidation`
- `type NormalizedMarketEvent`
- `MarketState15s`

Each new time-bearing dataclass must reject naive datetimes in `__post_init__`.

- [ ] **Step 4: Run model tests**

Run the same test command. Expected: pass.

## Task 2: Raw Archive Reader

**Files:**
- Create: `src/crypto_momentum_lab/persistence/raw_files/reader.py`
- Create: `tests/unit/persistence/raw_files/test_reader.py`
- Modify: `tests/integration/raw_files/test_archive.py`

- [ ] **Step 1: Write failing reader unit tests**

Test `deserialize_envelope_row()` with one serialized row and assert all enum, UUID, datetime, and raw payload fields round-trip. Test `replay_envelopes()` with two files whose rows are deliberately out of filename order and assert output is sorted by local receive order.

- [ ] **Step 2: Run reader tests to verify they fail**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/persistence/raw_files/test_reader.py -v
```

Expected: fail because `persistence.raw_files.reader` does not exist.

- [ ] **Step 3: Implement reader**

Implement:

- `RawArchiveRowError`
- `deserialize_envelope_row(row: str) -> RawEnvelope`
- `iter_archive_file(path: Path) -> Iterator[RawEnvelope]`
- `replay_envelopes(paths: Iterable[Path]) -> tuple[RawEnvelope, ...]`

Use `zstandard.open(path, "rt", encoding="utf-8")` and sort replay output by
`(received_at, received_monotonic_ns, str(connection_session_id), local_sequence)`.

- [ ] **Step 4: Run reader tests**

Run the same reader unit test command. Expected: pass.

- [ ] **Step 5: Add archive integration test**

Write an integration test that uses `ZstdJsonlArchive` to write two envelopes,
closes the archive, reads the finalized file through `replay_envelopes()`, and
asserts both source local sequences are recovered.

- [ ] **Step 6: Run archive integration tests**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/integration/raw_files/test_archive.py -v
```

Expected: pass.

## Task 3: Binance Market Normalizer

**Files:**
- Create: `src/crypto_momentum_lab/market_data/normalization/__init__.py`
- Create: `src/crypto_momentum_lab/market_data/normalization/binance.py`
- Create: `tests/unit/market_data/normalization/test_binance.py`

- [ ] **Step 1: Write failing normalizer tests**

Create one test per stream:

- aggTrade parses price, quantity, trade id, notional, and aggressive side;
- bookTicker parses bid/ask price and quantity plus update id;
- markPrice parses mark price, index price, funding rate, and next funding time;
- kline_1m parses OHLC, volume, quote volume, and closed flag;
- forceOrder parses order side, price, average price, quantity, and reported notional.

Also add a malformed aggTrade payload test that raises `BinanceNormalizationError`.

- [ ] **Step 2: Run normalizer tests to verify they fail**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/market_data/normalization/test_binance.py -v
```

Expected: fail because normalizer module does not exist.

- [ ] **Step 3: Implement normalizer**

Implement:

- `BinanceNormalizationError`
- `normalize_binance_envelope(envelope: RawEnvelope) -> NormalizedMarketEvent`

Use `Decimal(str(value))` for numeric fields. Use raw envelope source fields
for traceability. For `aggTrade`, map `m=true` to `AggressorSide.SELL` and
`m=false` to `AggressorSide.BUY`.

- [ ] **Step 4: Run normalizer tests**

Run the same normalizer test command. Expected: pass.

## Task 4: 15-Second Aggregator

**Files:**
- Create: `src/crypto_momentum_lab/market_data/aggregation/__init__.py`
- Create: `src/crypto_momentum_lab/market_data/aggregation/state_15s.py`
- Create: `tests/unit/market_data/aggregation/test_state_15s.py`

- [ ] **Step 1: Write failing aggregation tests**

Create tests for:

- UTC-aligned bucket calculation at `00`, `14.999`, `15`, and `30` seconds;
- two aggregate trades in the same bucket produce correct OHLC, notional, count, and aggressive buy/sell notional;
- book ticker produces last bid, last ask, spread, and midpoint;
- liquidation, mark price, and closed kline updates are counted in the bucket;
- events from different symbols produce separate states.

- [ ] **Step 2: Run aggregation tests to verify they fail**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/market_data/aggregation/test_state_15s.py -v
```

Expected: fail because aggregation module does not exist.

- [ ] **Step 3: Implement aggregation**

Implement:

- `bucket_start_15s(value: datetime) -> datetime`
- `aggregate_market_states_15s(events: Iterable[NormalizedMarketEvent]) -> tuple[MarketState15s, ...]`

Keep the implementation pure and deterministic. Do not forward-fill missing
fields between buckets.

- [ ] **Step 4: Run aggregation tests**

Run the same aggregation test command. Expected: pass.

## Task 5: Full Verification and Commit

**Files:**
- All files touched in Tasks 1-4.

- [ ] **Step 1: Run targeted tests**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/domain/market/test_models.py tests/unit/persistence/raw_files/test_reader.py tests/unit/market_data/normalization/test_binance.py tests/unit/market_data/aggregation/test_state_15s.py tests/integration/raw_files/test_archive.py -v
```

Expected: pass.

- [ ] **Step 2: Run full non-live test suite**

Run with elevated permissions if local PostgreSQL TCP is blocked by the sandbox:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest -m "not live" -v
```

Expected: `82+` tests pass and live tests remain deselected.

- [ ] **Step 3: Run static checks**

Run:

```bash
/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/ruff check .
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/mypy src
```

Expected: both pass.

- [ ] **Step 4: Commit**

Run:

```bash
git add docs src tests
git commit -m "feat: derive replay normalization states"
```
