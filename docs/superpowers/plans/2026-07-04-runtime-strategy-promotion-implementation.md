# Runtime Strategy Promotion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `compression_breakout`, `orderflow_impulse`, and `liquidation_cascade` selectable through the same runtime strategy contract.

**Architecture:** Introduce a strategy registry that builds runtime strategy instances by CLI name. Keep implementation packages independent: CLI name `orderflow_impulse` maps to package `strategies/order_flow_impulse`, while CLI name `liquidation_cascade` maps to `strategies/liquidation_cascade`.

**Tech Stack:** Python 3.13 dataclasses, existing strategy domain models, Typer CLI, pytest, ruff, mypy.

---

## File Structure

- Create: `src/crypto_momentum_lab/strategy_runner/registry.py`
  - Strategy factory registry and config parsing.
- Modify: `src/crypto_momentum_lab/strategy_runner/paper.py`
  - Use registry instead of hard-coded compression strategy.
- Modify: `src/crypto_momentum_lab/apps/strategy_runner/main.py`
  - Route strategy-specific options into registry configs.
- Create: `src/crypto_momentum_lab/strategies/order_flow_impulse/runtime.py`
  - Runtime strategy for `orderflow_impulse`.
- Create: `src/crypto_momentum_lab/strategies/liquidation_cascade/runtime.py`
  - Runtime strategy for `liquidation_cascade`.
- Modify: `src/crypto_momentum_lab/strategies/order_flow_impulse/__init__.py`
- Modify: `src/crypto_momentum_lab/strategies/liquidation_cascade/__init__.py`
- Create: `tests/unit/strategy_runner/test_registry.py`
- Create: `tests/unit/strategies/order_flow_impulse/test_runtime.py`
- Create: `tests/unit/strategies/liquidation_cascade/test_runtime.py`
- Modify: `tests/unit/strategy_runner/test_paper.py`
- Modify: `tests/unit/apps/strategy_runner/test_strategy_runner_main.py`

---

### Task 1: Strategy Registry

**Files:**
- Create: `src/crypto_momentum_lab/strategy_runner/registry.py`
- Create: `tests/unit/strategy_runner/test_registry.py`

- [ ] **Step 1: Write failing registry tests**

Create tests:

```python
def test_registry_lists_supported_strategy_names() -> None:
    assert supported_strategy_names() == (
        "compression_breakout",
        "orderflow_impulse",
        "liquidation_cascade",
    )


def test_registry_rejects_unknown_strategy() -> None:
    with pytest.raises(StrategyRegistryError, match="unsupported strategy"):
        build_runtime_strategy("unknown", config={}, identity=fixture_identity())
```

- [ ] **Step 2: Verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/strategy_runner/test_registry.py -v
```

Expected: FAIL because `strategy_runner.registry` does not exist.

- [ ] **Step 3: Implement registry**

Implement a registry module with:

- `StrategyRegistryError`, raised for unknown strategy names or invalid config payloads;
- `supported_strategy_names()`, returning an ordered tuple with the three runtime strategy names;
- `build_runtime_strategy()`, constructing a strategy from `strategy_name`, parsed config, and `StrategyRunIdentity`.

The registry initially delegates `compression_breakout` to existing `CompressionBreakoutRuntimeStrategy`.

- [ ] **Step 4: Verify and commit**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/strategy_runner/test_registry.py -v
.venv/bin/ruff check src/crypto_momentum_lab/strategy_runner tests/unit/strategy_runner
.venv/bin/mypy src
```

Commit:

```bash
git add src/crypto_momentum_lab/strategy_runner/registry.py tests/unit/strategy_runner/test_registry.py
git commit -m "feat: add runtime strategy registry"
```

---

### Task 2: Order-Flow Impulse Runtime

**Files:**
- Create: `src/crypto_momentum_lab/strategies/order_flow_impulse/runtime.py`
- Modify: `src/crypto_momentum_lab/strategies/order_flow_impulse/__init__.py`
- Create: `tests/unit/strategies/order_flow_impulse/test_runtime.py`

- [ ] **Step 1: Write failing runtime tests**

Write tests named:

- `test_orderflow_impulse_emits_long_signal_on_buy_imbalance`: uses a rising close, valid midpoint, and buy-side notional above sell-side notional.
- `test_orderflow_impulse_rejects_missing_midpoint`: builds a state without valid bid/ask midpoint and expects no candidate.
- `test_orderflow_impulse_restores_checkpoint`: saves runtime memory to a checkpoint and verifies a new instance resumes cooldown and recent-state history.

Use `MarketState15s` fixtures with rising close price, aggressive buy notional greater than aggressive sell notional, and valid bid/ask midpoint.

- [ ] **Step 2: Verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/strategies/order_flow_impulse/test_runtime.py -v
```

Expected: FAIL because runtime module does not exist.

- [ ] **Step 3: Implement config and strategy**

Create `OrderFlowImpulseConfig` with fields:

```python
impulse_window_buckets: int
min_return_pct: Decimal
min_imbalance_ratio: Decimal
min_trade_notional: Decimal
max_spread_pct: Decimal
confirmation_buckets: int
cooldown_buckets: int
candidate_notional: Decimal | None
candidate_ttl_buckets: int
```

Implement `OrderFlowImpulseRuntimeStrategy.on_market_state()` using only recent states held in strategy-local memory.

- [ ] **Step 4: Register strategy**

Add registry mapping from CLI name `orderflow_impulse` to `OrderFlowImpulseRuntimeStrategy`.

- [ ] **Step 5: Verify and commit**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/strategies/order_flow_impulse/test_runtime.py tests/unit/strategy_runner/test_registry.py -v
.venv/bin/ruff check src/crypto_momentum_lab/strategies/order_flow_impulse tests/unit/strategies/order_flow_impulse
.venv/bin/mypy src
```

Commit:

```bash
git add src/crypto_momentum_lab/strategies/order_flow_impulse src/crypto_momentum_lab/strategy_runner/registry.py tests/unit/strategies/order_flow_impulse/test_runtime.py tests/unit/strategy_runner/test_registry.py
git commit -m "feat: add orderflow impulse runtime strategy"
```

---

### Task 3: Liquidation Cascade Runtime

**Files:**
- Create: `src/crypto_momentum_lab/strategies/liquidation_cascade/runtime.py`
- Modify: `src/crypto_momentum_lab/strategies/liquidation_cascade/__init__.py`
- Create: `tests/unit/strategies/liquidation_cascade/test_runtime.py`

- [ ] **Step 1: Write failing runtime tests**

Write tests named:

- `test_liquidation_cascade_emits_signal_after_liquidation_and_break`: feeds liquidation notional above threshold plus a qualifying price break.
- `test_liquidation_cascade_ignores_states_without_liquidation`: feeds a price move without liquidation notional and expects no candidate.
- `test_liquidation_cascade_restores_checkpoint`: verifies lookback state and cooldown survive checkpoint restore.

- [ ] **Step 2: Verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/strategies/liquidation_cascade/test_runtime.py -v
```

Expected: FAIL because runtime module does not exist.

- [ ] **Step 3: Implement config and strategy**

Create `LiquidationCascadeConfig` with fields:

```python
lookback_buckets: int
min_liquidation_notional: Decimal
min_price_break_pct: Decimal
max_spread_pct: Decimal
confirmation_buckets: int
cooldown_buckets: int
candidate_notional: Decimal | None
candidate_ttl_buckets: int
```

The V0 runtime emits a signal only when liquidation notional threshold and price-break threshold both pass.

- [ ] **Step 4: Register strategy**

Add registry mapping from CLI name `liquidation_cascade` to `LiquidationCascadeRuntimeStrategy`.

- [ ] **Step 5: Verify and commit**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/strategies/liquidation_cascade/test_runtime.py tests/unit/strategy_runner/test_registry.py -v
.venv/bin/ruff check src/crypto_momentum_lab/strategies/liquidation_cascade tests/unit/strategies/liquidation_cascade
.venv/bin/mypy src
```

Commit:

```bash
git add src/crypto_momentum_lab/strategies/liquidation_cascade src/crypto_momentum_lab/strategy_runner/registry.py tests/unit/strategies/liquidation_cascade/test_runtime.py tests/unit/strategy_runner/test_registry.py
git commit -m "feat: add liquidation cascade runtime strategy"
```

---

### Task 4: Paper Runner Uses Registry

**Files:**
- Modify: `src/crypto_momentum_lab/strategy_runner/paper.py`
- Modify: `src/crypto_momentum_lab/apps/strategy_runner/main.py`
- Modify: `tests/unit/strategy_runner/test_paper.py`
- Modify: `tests/unit/apps/strategy_runner/test_strategy_runner_main.py`

- [ ] **Step 1: Write failing paper tests**

Add tests proving `run_paper_trading()` accepts `orderflow_impulse` and `liquidation_cascade` without `PaperRunnerError`.

- [ ] **Step 2: Verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/strategy_runner/test_paper.py -v
```

Expected: FAIL because paper runner only supports `compression_breakout`.

- [ ] **Step 3: Refactor paper runner**

Replace hard-coded compression strategy construction with registry construction. Preserve existing compression behavior.

- [ ] **Step 4: Update CLI tests**

Add CLI tests for:

```text
cml-strategy-runner paper --strategy orderflow_impulse --states-path tests/fixtures/market_states.parquet
cml-strategy-runner paper-live-source --strategy liquidation_cascade --database-url postgresql+asyncpg://cml:cml@localhost:54329/cml
```

- [ ] **Step 5: Verify and commit**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/strategy_runner tests/unit/apps/strategy_runner -q
.venv/bin/ruff check src/crypto_momentum_lab/strategy_runner src/crypto_momentum_lab/apps/strategy_runner tests/unit/strategy_runner tests/unit/apps/strategy_runner
.venv/bin/mypy src
```

Commit:

```bash
git add src/crypto_momentum_lab/strategy_runner src/crypto_momentum_lab/apps/strategy_runner tests/unit/strategy_runner tests/unit/apps/strategy_runner
git commit -m "feat: route paper runner through strategy registry"
```

---

## Completion Criteria

- Runtime strategy registry lists `compression_breakout`, `orderflow_impulse`, and `liquidation_cascade`.
- Unknown strategy names fail with a clear registry error.
- Order-flow impulse and liquidation cascade runtime modules emit candidates only from their own strategy-local rules.
- Checkpoint restore works for all runtime strategies.
- Paper runner and CLI use the registry instead of hard-coded compression strategy construction.
- Unit, ruff, and mypy verification commands pass.
