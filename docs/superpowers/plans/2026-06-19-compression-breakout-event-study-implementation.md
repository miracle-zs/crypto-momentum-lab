# Compression Breakout Event Study Implementation Plan

> **For agentic workers:** Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a deterministic V0 event-study pipeline for volatility
compression breakouts using derived `market_states_15s` Parquet data.

**Architecture:** Add a Parquet state reader, a strategy-owned compression
breakout event-study module, a small research report writer, and a
`cml-research` CLI command. Keep this phase descriptive; do not generate live
signals or orders.

**Tech Stack:** Python 3.13, dataclasses, Decimal, pyarrow, Typer, pytest, ruff,
mypy.

---

## File Structure

- Modify: `src/crypto_momentum_lab/persistence/parquet/__init__.py`
  - Export market-state read API.
- Modify: `src/crypto_momentum_lab/persistence/parquet/datasets.py`
  - Add `read_market_states_15s_dataset()`.
- Create: `src/crypto_momentum_lab/strategies/__init__.py`
  - Strategy package marker.
- Create: `src/crypto_momentum_lab/strategies/compression_breakout/__init__.py`
  - Export event-study API.
- Create: `src/crypto_momentum_lab/strategies/compression_breakout/event_study.py`
  - Compression breakout config, event model, detection, labels, and summaries.
- Create: `src/crypto_momentum_lab/research/compression_breakout.py`
  - Dataset-to-report orchestration and JSON report serialization.
- Modify: `src/crypto_momentum_lab/apps/research/main.py`
  - Add `compression-breakout-study` command.
- Create: `tests/unit/persistence/parquet/test_read_states.py`
  - Parquet readback tests.
- Create: `tests/unit/strategies/compression_breakout/test_event_study.py`
  - Detector, labels, and summary tests.
- Create: `tests/unit/research/test_compression_breakout.py`
  - Report-generation tests.
- Modify: `tests/unit/apps/research/test_research_main.py`
  - CLI smoke test.

## Task 1: Parquet Market-State Reader

**Files:**
- Modify: `src/crypto_momentum_lab/persistence/parquet/__init__.py`
- Modify: `src/crypto_momentum_lab/persistence/parquet/datasets.py`
- Create: `tests/unit/persistence/parquet/test_read_states.py`

- [ ] **Step 1: Write failing readback tests**

Use the existing writer to write a `MarketState15s`, read the partition back,
and assert Decimal strings and Hive `symbol` partition values reconstruct a
typed `MarketState15s`.

- [ ] **Step 2: Implement state reader**

Implement:

- `read_market_states_15s_dataset(paths: Iterable[Path]) -> tuple[MarketState15s, ...]`

The reader accepts file paths or directories. Directories are scanned
recursively for `.parquet` files. Rows are sorted by `(symbol, bucket_start)`.

- [ ] **Step 3: Run readback tests**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/persistence/parquet/test_read_states.py -v
```

Expected: pass.

## Task 2: Compression Breakout Event Study Core

**Files:**
- Create: `src/crypto_momentum_lab/strategies/__init__.py`
- Create: `src/crypto_momentum_lab/strategies/compression_breakout/__init__.py`
- Create: `src/crypto_momentum_lab/strategies/compression_breakout/event_study.py`
- Create: `tests/unit/strategies/compression_breakout/test_event_study.py`

- [ ] **Step 1: Write failing detector tests**

Add tests for:

- upward breakout after a compressed range;
- downward breakout after a compressed range;
- rejection when the lookback range is too wide;
- skipping missing-price lookback windows;
- directional forward returns and MFE/MAE labels;
- summary counts and mean forward returns by direction.

- [ ] **Step 2: Implement event study**

Implement:

- `CompressionBreakoutConfig`;
- `BreakoutDirection`;
- `CompressionBreakoutEvent`;
- `CompressionBreakoutSummary`;
- `find_compression_breakouts(states, config)`;
- `summarize_compression_breakouts(events, horizons)`.

The detector must group states by symbol, sort by bucket start, freeze the
lookback range before evaluating the candidate, and apply cooldown.

- [ ] **Step 3: Run event-study tests**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/strategies/compression_breakout/test_event_study.py -v
```

Expected: pass.

## Task 3: Research Report Builder

**Files:**
- Create: `src/crypto_momentum_lab/research/compression_breakout.py`
- Create: `tests/unit/research/test_compression_breakout.py`

- [ ] **Step 1: Write failing report tests**

Build a small in-memory state dataset, generate a report, write it to JSON, and
assert it includes configuration, source paths, events, and summary fields.

- [ ] **Step 2: Implement report builder**

Implement:

- `CompressionBreakoutReport`;
- `run_compression_breakout_event_study(states, config, source_paths)`;
- `write_compression_breakout_report(report, output_path)`.

The JSON representation must use strings for Decimal values and ISO strings for
timestamps.

- [ ] **Step 3: Run report tests**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/research/test_compression_breakout.py -v
```

Expected: pass.

## Task 4: Research CLI

**Files:**
- Modify: `src/crypto_momentum_lab/apps/research/main.py`
- Modify: `tests/unit/apps/research/test_research_main.py`

- [ ] **Step 1: Write failing CLI test**

Patch the report runner, call:

```text
compression-breakout-study --states-root <path> --output <report.json>
```

Assert exit code `0`, the patched runner receives the expected state root and
output path, and stdout includes the total event count.

- [ ] **Step 2: Implement CLI command**

Add Typer command options for:

- `--states-root`
- `--output`
- `--compression-window-buckets`
- `--max-range-width-pct`
- `--min-breakout-pct`
- `--acceptance-buckets`
- `--cooldown-buckets`
- repeated `--forward-horizon-buckets`

- [ ] **Step 3: Run CLI tests**

Run:

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/apps/research/test_research_main.py -v
```

Expected: pass.

## Task 5: Verification, Commit, and Merge

- [ ] **Step 1: Run targeted tests**

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/persistence/parquet/test_read_states.py tests/unit/strategies/compression_breakout/test_event_study.py tests/unit/research/test_compression_breakout.py tests/unit/apps/research/test_research_main.py -v
```

- [ ] **Step 2: Run static checks**

```bash
/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/ruff check .
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/mypy src
```

- [ ] **Step 3: Run full non-live tests**

Use the local environment workaround if necessary:

```bash
env -u ALL_PROXY -u all_proxy -u HTTP_PROXY -u http_proxy -u HTTPS_PROXY -u https_proxy -u NO_PROXY -u no_proxy PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest -m "not live" -v
```

- [ ] **Step 4: Commit and merge**

```bash
git add docs src tests
git commit -m "feat: study compression breakout events"
git checkout main
git merge compression-event-study
```
