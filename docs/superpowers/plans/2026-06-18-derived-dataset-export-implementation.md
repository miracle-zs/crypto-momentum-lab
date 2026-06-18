# Derived Dataset Export Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Export rebuildable `market_events` and `market_states_15s` Parquet datasets plus local JSON manifests from immutable raw archive files.

**Architecture:** Add a small Parquet persistence package that maps normalized events and 15-second states to flat rows, writes partitioned Parquet files atomically, and emits JSON manifests. Add an offline research builder and CLI command that chains raw replay, Binance normalization, 15-second aggregation, and Parquet writing.

**Tech Stack:** Python 3.13, pyarrow 22, dataclasses, Decimal-as-string row serialization, Zstandard raw archives, pytest, ruff, mypy.

---

## File Structure

- Modify: `pyproject.toml`
  - Add `pyarrow>=20,<23`.
- Create: `src/crypto_momentum_lab/persistence/parquet/__init__.py`
  - Export dataset writer API.
- Create: `src/crypto_momentum_lab/persistence/parquet/datasets.py`
  - Row mapping for normalized events and 15-second states, Parquet write logic, local manifest creation.
- Create: `src/crypto_momentum_lab/research/__init__.py`
  - Research package marker.
- Create: `src/crypto_momentum_lab/research/datasets.py`
  - One-shot raw archive to derived dataset builder.
- Create: `src/crypto_momentum_lab/apps/research/__init__.py`
  - Research CLI package marker.
- Create: `src/crypto_momentum_lab/apps/research/main.py`
  - Typer CLI for deriving datasets from raw archive paths.
- Modify: `pyproject.toml`
  - Add `cml-research = "crypto_momentum_lab.apps.research.main:app"`.
- Create: `tests/unit/persistence/parquet/test_datasets.py`
  - Row mapping and manifest unit tests.
- Create: `tests/integration/persistence/parquet/test_writer.py`
  - Parquet writer integration tests with pyarrow reads.
- Create: `tests/unit/research/test_datasets.py`
  - End-to-end builder unit tests using small archive fixtures.
- Create: `tests/unit/apps/research/test_main.py`
  - CLI smoke tests.

## Task 1: Dependency and Row Mapping

**Files:**
- Modify: `pyproject.toml`
- Create: `src/crypto_momentum_lab/persistence/parquet/__init__.py`
- Create: `src/crypto_momentum_lab/persistence/parquet/datasets.py`
- Create: `tests/unit/persistence/parquet/test_datasets.py`

- [ ] **Step 1: Write failing row mapping tests**

Add tests for:

- `market_event_row()` converts a `NormalizedAggTrade` into common source fields, `event_type="agg_trade"`, price/quantity/notional strings, and aggressive side.
- `market_state_15s_row()` converts `MarketState15s` into bucket timestamps and Decimal strings.
- `partition_for_market_event()` and `partition_for_market_state()` return the documented relative partition directories.

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/persistence/parquet/test_datasets.py -v
```

Expected: fail because the parquet package does not exist.

- [ ] **Step 3: Implement row mapping**

Add `pyarrow>=20,<23` to project dependencies. Implement:

- `DatasetName` enum with `MARKET_EVENTS` and `MARKET_STATES_15S`;
- `market_event_row(event: NormalizedMarketEvent) -> dict[str, object]`;
- `market_state_15s_row(state: MarketState15s) -> dict[str, object]`;
- `partition_for_market_event(event: NormalizedMarketEvent) -> Path`;
- `partition_for_market_state(state: MarketState15s) -> Path`.

- [ ] **Step 4: Run row mapping tests**

Run the same unit test command. Expected: pass.

## Task 2: Parquet Writer and Manifest

**Files:**
- Modify: `src/crypto_momentum_lab/persistence/parquet/datasets.py`
- Create: `tests/integration/persistence/parquet/test_writer.py`

- [ ] **Step 1: Write failing writer tests**

Add integration tests that:

- write two market events to a temporary dataset root;
- read the resulting Parquet file with `pyarrow.parquet.read_table`;
- assert row count and key columns;
- assert the JSON manifest exists, references the output relative path, records input paths, and stores the Parquet SHA-256.

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/integration/persistence/parquet/test_writer.py -v
```

Expected: fail because writer functions do not exist.

- [ ] **Step 3: Implement writer**

Implement:

- `DerivedDatasetManifest` dataclass;
- `write_market_events_dataset(root, events, input_paths)`;
- `write_market_states_15s_dataset(root, states, input_paths)`.

Group rows by partition path. Write `part-<deterministic-id>.parquet.tmp`,
rename atomically to `.parquet`, calculate checksum, then write manifest JSON
under `_manifests/<manifest-id>.json.tmp` and atomically rename.

- [ ] **Step 4: Run writer tests**

Run the same integration test command. Expected: pass.

## Task 3: Raw Archive to Derived Dataset Builder

**Files:**
- Create: `src/crypto_momentum_lab/research/__init__.py`
- Create: `src/crypto_momentum_lab/research/datasets.py`
- Create: `tests/unit/research/test_datasets.py`

- [ ] **Step 1: Write failing builder test**

Create a test that writes a small raw archive with `ZstdJsonlArchive`, runs
`derive_market_datasets(raw_paths, output_root)`, and asserts both
`market_events` and `market_states_15s` Parquet manifests are returned.

- [ ] **Step 2: Run builder test to verify failure**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/research/test_datasets.py -v
```

Expected: fail because the research builder does not exist.

- [ ] **Step 3: Implement builder**

Implement:

- `DerivedMarketDatasets` dataclass;
- `derive_market_datasets(raw_paths: tuple[Path, ...], output_root: Path)`.

The builder must:

1. replay raw envelopes;
2. normalize each envelope;
3. aggregate 15-second states;
4. write both datasets with the same input path list;
5. return both manifest tuples.

- [ ] **Step 4: Run builder test**

Run the same builder test command. Expected: pass.

## Task 4: Research CLI

**Files:**
- Create: `src/crypto_momentum_lab/apps/research/__init__.py`
- Create: `src/crypto_momentum_lab/apps/research/main.py`
- Modify: `pyproject.toml`
- Create: `tests/unit/apps/research/test_main.py`

- [ ] **Step 1: Write failing CLI test**

Use `typer.testing.CliRunner` to call:

```text
derive-datasets --output-root <tmp> <raw-file>
```

Assert exit code `0` and output includes `market_events` and
`market_states_15s`.

- [ ] **Step 2: Run CLI test to verify failure**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/apps/research/test_main.py -v
```

Expected: fail because the research CLI does not exist.

- [ ] **Step 3: Implement CLI**

Add Typer app with:

- positional `raw_paths`;
- option `--output-root`;
- command `derive-datasets`;
- summary print with manifest counts and relative output paths.

Add `cml-research` project script in `pyproject.toml`.

- [ ] **Step 4: Run CLI test**

Run the same CLI test command. Expected: pass.

## Task 5: Verification and Commit

**Files:**
- All files touched in Tasks 1-4.

- [ ] **Step 1: Run targeted tests**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/persistence/parquet/test_datasets.py tests/integration/persistence/parquet/test_writer.py tests/unit/research/test_datasets.py tests/unit/apps/research/test_main.py -v
```

Expected: pass.

- [ ] **Step 2: Run full non-live tests**

Run with elevated permissions if local PostgreSQL TCP is blocked by sandbox:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest -m "not live" -v
```

Expected: pass with live tests deselected.

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
git add docs pyproject.toml src tests
git commit -m "feat: export derived market datasets"
```
