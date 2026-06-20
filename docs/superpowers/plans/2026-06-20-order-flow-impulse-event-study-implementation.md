# Order-Flow Impulse Event Study Implementation Plan

**Goal:** Build a deterministic V0 event-study pipeline for Strategy A:
order-flow impulse momentum, using derived `market_states_15s` data.

**Architecture:** Add a strategy-owned event-study module, research report
builder, CLI command, and tests. Reuse the existing Parquet state reader. Keep
this phase descriptive; do not generate live signals or orders.

**Tech Stack:** Python 3.13, dataclasses, Decimal, Typer, pyarrow dataset input,
pytest, ruff, mypy.

---

## File Structure

- Create: `src/crypto_momentum_lab/strategies/order_flow_impulse/__init__.py`
  - Export event-study API.
- Create: `src/crypto_momentum_lab/strategies/order_flow_impulse/event_study.py`
  - Config, event model, detection, labels, and summary.
- Create: `src/crypto_momentum_lab/research/order_flow_impulse.py`
  - Dataset-to-report orchestration and JSON serialization.
- Modify: `src/crypto_momentum_lab/apps/research/main.py`
  - Add `order-flow-impulse-study` command.
- Create: `tests/unit/strategies/order_flow_impulse/test_order_flow_impulse_event_study.py`
  - Detector, labels, and summary tests.
- Create: `tests/unit/research/test_order_flow_impulse.py`
  - Report-generation tests.
- Modify: `tests/unit/apps/research/test_research_main.py`
  - CLI smoke test.

## Task 1: Event-Study Core Tests

**Files:**
- Create: `tests/unit/strategies/order_flow_impulse/test_event_study.py`

- [ ] **Step 1: Write failing tests**

Add tests for:

- upward impulse with aligned aggressive buying and notional expansion;
- downward impulse with aligned aggressive selling and notional expansion;
- rejection when price moves but aggressive imbalance is weak;
- rejection when aggressive imbalance exists but notional intensity is too low;
- skipping missing-price or insufficient-history windows;
- confirmation and cooldown behavior;
- directional forward returns and summary means.

- [ ] **Step 2: Run tests to verify failure**

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/strategies/order_flow_impulse/test_order_flow_impulse_event_study.py -v
```

Expected: fail because the module does not exist.

## Task 2: Event-Study Core Implementation

**Files:**
- Create: `src/crypto_momentum_lab/strategies/order_flow_impulse/__init__.py`
- Create: `src/crypto_momentum_lab/strategies/order_flow_impulse/event_study.py`

- [ ] **Step 1: Implement event-study API**

Implement:

- `OrderFlowDirection`;
- `OrderFlowImpulseConfig`;
- `OrderFlowImpulseEvent`;
- `OrderFlowImpulseDirectionSummary`;
- `OrderFlowImpulseSummary`;
- `find_order_flow_impulses(states, config)`;
- `summarize_order_flow_impulses(events, horizons)`.

The detector groups states by symbol, sorts by time, freezes historical
breakout boundaries and baselines, then applies confirmation and cooldown.

- [ ] **Step 2: Run event-study tests**

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/strategies/order_flow_impulse/test_order_flow_impulse_event_study.py -v
```

Expected: pass.

## Task 3: Research Report Builder

**Files:**
- Create: `src/crypto_momentum_lab/research/order_flow_impulse.py`
- Create: `tests/unit/research/test_order_flow_impulse.py`

- [ ] **Step 1: Write failing report tests**

Build a small in-memory state dataset, generate a report, write it to JSON, and
assert it includes configuration, source paths, events, and summary fields.

- [ ] **Step 2: Implement report builder**

Implement:

- `OrderFlowImpulseReport`;
- `run_order_flow_impulse_event_study(states, config, source_paths)`;
- `build_order_flow_impulse_report(state_paths, output_path, config)`;
- `write_order_flow_impulse_report(report, output_path)`.

Decimal values must serialize as strings and timestamps as ISO strings.

- [ ] **Step 3: Run report tests**

```bash
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/research/test_order_flow_impulse.py -v
```

Expected: pass.

## Task 4: Research CLI

**Files:**
- Modify: `src/crypto_momentum_lab/apps/research/main.py`
- Modify: `tests/unit/apps/research/test_research_main.py`

- [ ] **Step 1: Write failing CLI test**

Patch the report runner, call:

```text
order-flow-impulse-study --states-root <path> --output <report.json>
```

Assert exit code `0`, config parsing, state root, output path, and stdout event
count.

- [ ] **Step 2: Implement CLI command**

Add options for:

- `--states-root`
- `--output`
- `--impulse-window-buckets`
- `--baseline-window-buckets`
- `--breakout-window-buckets`
- `--min-return-pct`
- `--min-aggressive-imbalance`
- `--min-notional-intensity`
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
PYTHONPATH=src /Users/zhangshuai/PycharmProjects/crypto-momentum-lab/.venv/bin/python -m pytest tests/unit/strategies/order_flow_impulse/test_order_flow_impulse_event_study.py tests/unit/research/test_order_flow_impulse.py tests/unit/apps/research/test_research_main.py -v
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
git commit -m "feat: study order-flow impulse events"
git checkout main
git merge order-flow-impulse-event-study
```
