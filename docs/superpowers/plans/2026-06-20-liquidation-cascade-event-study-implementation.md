# Liquidation Cascade Event Study Implementation Plan

**Goal:** Build a deterministic V0 event-study pipeline for Strategy C:
liquidation-cascade momentum, using derived `market_states_15s` data.

**Architecture:** Add a strategy-owned event-study module, research report
builder, CLI command, and tests. Reuse the existing Parquet state reader. Keep
this phase descriptive; do not generate live signals or orders.

**Tech Stack:** Python 3.13, dataclasses, Decimal, Typer, pyarrow dataset input,
pytest, ruff, mypy.

---

## File Structure

- Create: `src/crypto_momentum_lab/strategies/liquidation_cascade/__init__.py`
  - Export event-study API.
- Create: `src/crypto_momentum_lab/strategies/liquidation_cascade/event_study.py`
  - Config, event model, detection, labels, and summary.
- Create: `src/crypto_momentum_lab/research/liquidation_cascade.py`
  - Dataset-to-report orchestration and JSON serialization.
- Modify: `src/crypto_momentum_lab/apps/research/main.py`
  - Add `liquidation-cascade-study` command.
- Create: `tests/unit/strategies/liquidation_cascade/test_liquidation_cascade_event_study.py`
  - Detector, labels, and summary tests.
- Create: `tests/unit/research/test_liquidation_cascade.py`
  - Report-generation tests.
- Modify: `tests/unit/apps/research/test_research_main.py`
  - CLI smoke test.

## Task 1: Event-Study Core Tests

**Files:**
- Create: `tests/unit/strategies/liquidation_cascade/test_liquidation_cascade_event_study.py`

- [ ] **Step 1: Write failing tests**

Add tests for:

- upward continuation after a liquidation cluster;
- downward continuation after a liquidation cluster;
- rejection when the move has no liquidation activity;
- rejection when liquidation activity exists but aggressive imbalance is not
  aligned;
- skipping missing-price or insufficient-history windows;
- confirmation and cooldown behavior;
- directional forward returns and summary means.

- [ ] **Step 2: Run tests to verify failure**

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/strategies/liquidation_cascade/test_liquidation_cascade_event_study.py -v
```

Expected: fail because the module does not exist.

## Task 2: Event-Study Core Implementation

**Files:**
- Create: `src/crypto_momentum_lab/strategies/liquidation_cascade/__init__.py`
- Create: `src/crypto_momentum_lab/strategies/liquidation_cascade/event_study.py`

- [ ] **Step 1: Implement event-study API**

Implement:

- `LiquidationCascadeDirection`;
- `LiquidationCascadeConfig`;
- `LiquidationCascadeEvent`;
- `LiquidationCascadeDirectionSummary`;
- `LiquidationCascadeSummary`;
- `find_liquidation_cascades(states, config)`;
- `summarize_liquidation_cascades(events, horizons)`.

The detector groups states by symbol, sorts by time, freezes historical breakout
boundaries, then applies liquidation-activity, price-displacement, aggressive
flow, confirmation, and cooldown filters.

- [ ] **Step 2: Run event-study tests**

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/strategies/liquidation_cascade/test_liquidation_cascade_event_study.py -v
```

Expected: pass.

## Task 3: Research Report Builder

**Files:**
- Create: `src/crypto_momentum_lab/research/liquidation_cascade.py`
- Create: `tests/unit/research/test_liquidation_cascade.py`

- [ ] **Step 1: Write failing report tests**

Build a small in-memory state dataset, generate a report, write it to JSON, and
assert it includes configuration, source paths, events, and summary fields.

- [ ] **Step 2: Implement report builder**

Implement:

- `LiquidationCascadeReport`;
- `run_liquidation_cascade_event_study(states, config, source_paths)`;
- `build_liquidation_cascade_report(state_paths, output_path, config)`;
- `write_liquidation_cascade_report(report, output_path)`.

Decimal values must serialize as strings and timestamps as ISO strings.

- [ ] **Step 3: Run report tests**

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/research/test_liquidation_cascade.py -v
```

Expected: pass.

## Task 4: Research CLI

**Files:**
- Modify: `src/crypto_momentum_lab/apps/research/main.py`
- Modify: `tests/unit/apps/research/test_research_main.py`

- [ ] **Step 1: Write failing CLI test**

Patch the report runner, call:

```text
liquidation-cascade-study --states-root <path> --output <report.json>
```

Assert exit code `0`, config parsing, state root, output path, and stdout event
count.

- [ ] **Step 2: Implement CLI command**

Add options for:

- `--states-root`
- `--output`
- `--liquidation-window-buckets`
- `--breakout-window-buckets`
- `--min-liquidation-count`
- `--min-liquidation-notional`
- `--min-price-move-pct`
- `--min-aggressive-imbalance`
- `--confirmation-buckets`
- `--cooldown-buckets`
- repeated `--forward-horizon-buckets`

- [ ] **Step 3: Run CLI tests**

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/apps/research/test_research_main.py -v
```

Expected: pass.

## Task 5: Verification, Commit, and Merge

- [ ] **Step 1: Run targeted tests**

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/strategies/liquidation_cascade/test_liquidation_cascade_event_study.py tests/unit/research/test_liquidation_cascade.py tests/unit/apps/research/test_research_main.py -v
```

- [ ] **Step 2: Run static checks**

```bash
/Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/ruff check .
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/mypy src
```

- [ ] **Step 3: Run full non-live tests**

```bash
env -u ALL_PROXY -u all_proxy -u HTTP_PROXY -u http_proxy -u HTTPS_PROXY -u https_proxy -u NO_PROXY -u no_proxy PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest -m "not live" -v
```

- [ ] **Step 4: Commit and merge**

```bash
git add docs src tests
git commit -m "feat: study liquidation cascade events"
git checkout main
git merge liquidation-cascade-event-study
```
