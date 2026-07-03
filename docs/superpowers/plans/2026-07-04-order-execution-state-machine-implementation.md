# Order Execution State Machine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert risk-approved order intents into idempotent Binance order plans and, after explicit live enablement, real exchange submissions.

**Architecture:** Add durable order execution tables and a state machine around deterministic client order IDs. All ambiguous submissions go through query-by-client-order-id before retry; unresolved uncertainty blocks new exposure.

**Tech Stack:** Python 3.13, SQLAlchemy 2 async ORM, Alembic, PostgreSQL, fake Binance client tests, manual-gated live tests, pytest, ruff, mypy.

---

## File Structure

- Create: `src/crypto_momentum_lab/domain/execution/order_state.py`
- Create: `src/crypto_momentum_lab/execution_account/orders/state_machine.py`
- Create: `src/crypto_momentum_lab/execution_account/orders/ids.py`
- Create: `src/crypto_momentum_lab/execution_account/orders/quantization.py`
- Modify: `src/crypto_momentum_lab/execution_account/binance/client.py`
  - Add live-write methods only behind explicit interface.
- Create: `src/crypto_momentum_lab/persistence/postgres/order_repository.py`
- Modify: `src/crypto_momentum_lab/persistence/postgres/models.py`
- Create: `alembic/versions/20260704_0008_order_execution_state.py`
- Create: `tests/unit/execution_account/orders/test_ids.py`
- Create: `tests/unit/execution_account/orders/test_quantization.py`
- Create: `tests/unit/execution_account/orders/test_state_machine.py`
- Create: `tests/integration/persistence/test_order_repository.py`
- Create: `tests/e2e/test_order_execution_fake_exchange.py`

---

### Task 1: Order Execution Domain And IDs

**Files:**
- Create: `src/crypto_momentum_lab/domain/execution/order_state.py`
- Create: `src/crypto_momentum_lab/execution_account/orders/ids.py`
- Create: `tests/unit/execution_account/orders/test_ids.py`

- [ ] **Step 1: Write failing ID tests**

Write tests named:

- `test_client_order_id_is_deterministic`: passes the same run and intent IDs twice and asserts identical output.
- `test_client_order_id_changes_with_intent_id`: changes only the intent ID and asserts a different client order ID.
- `test_client_order_id_is_short_enough_for_binance`: verifies the generated ID respects the documented Binance length limit.

- [ ] **Step 2: Verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/execution_account/orders/test_ids.py -v
```

Expected: FAIL because `execution_account.orders.ids` does not exist.

- [ ] **Step 3: Implement enums and ID function**

Create order states: `INTENT_APPROVED`, `CLAIMED`, `PLANNED`, `SUBMITTING`, `SUBMITTED`, `ACKNOWLEDGED`, `PARTIALLY_FILLED`, `FILLED`, `CANCELED`, `REJECTED`, `EXPIRED`, `UNKNOWN_PENDING_RECONCILIATION`.

Implement `deterministic_client_order_id(run_id, intent_id)` with a stable prefix and SHA-256 digest.

Use a stable prefix plus SHA-256 digest and enforce the Binance client order ID length limit after verifying current docs during implementation.

- [ ] **Step 4: Verify and commit**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/execution_account/orders/test_ids.py -v
.venv/bin/ruff check src/crypto_momentum_lab/domain/execution src/crypto_momentum_lab/execution_account/orders tests/unit/execution_account/orders
.venv/bin/mypy src
```

Commit:

```bash
git add src/crypto_momentum_lab/domain/execution/order_state.py src/crypto_momentum_lab/execution_account/orders/ids.py tests/unit/execution_account/orders/test_ids.py
git commit -m "feat: add deterministic exchange order ids"
```

---

### Task 2: Order Persistence

**Files:**
- Modify: `src/crypto_momentum_lab/persistence/postgres/models.py`
- Create: `src/crypto_momentum_lab/persistence/postgres/order_repository.py`
- Create: `alembic/versions/20260704_0008_order_execution_state.py`
- Create: `tests/integration/persistence/test_order_repository.py`

- [ ] **Step 1: Write failing repository tests**

Write tests named:

- `test_claim_intent_allows_one_worker`: races two claim attempts and asserts only one succeeds.
- `test_save_exchange_order_event_is_idempotent`: saves the same event twice and asserts one durable event row.
- `test_load_unresolved_orders_returns_unknown_state`: stores an unresolved order and verifies it is returned for reconciliation.

- [ ] **Step 2: Verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/integration/persistence/test_order_repository.py -v
```

Expected: FAIL because order tables and repository do not exist.

- [ ] **Step 3: Add tables and repository**

Add tables:

```text
order_intents
order_intent_claims
exchange_orders
exchange_order_events
exchange_fills
execution_commands
execution_reconciliation_events
```

Implement repository methods for claiming, saving planned orders, appending order events, saving fills, loading unresolved orders, and marking terminal state.

- [ ] **Step 4: Verify and commit**

Run:

```bash
docker compose up -d postgres
CML_DATABASE_URL=postgresql+asyncpg://cml:cml@localhost:54329/cml .venv/bin/alembic upgrade head
CML_TEST_ASYNC_DATABASE_URL=postgresql+asyncpg://cml:cml@localhost:54329/cml PYTHONPATH=src .venv/bin/python -m pytest tests/integration/persistence/test_order_repository.py -v
.venv/bin/ruff check alembic src/crypto_momentum_lab/persistence/postgres tests/integration/persistence
.venv/bin/mypy src
```

Commit:

```bash
git add alembic/versions/20260704_0008_order_execution_state.py src/crypto_momentum_lab/persistence/postgres tests/integration/persistence/test_order_repository.py
git commit -m "feat: persist exchange order execution state"
```

---

### Task 3: Quantization

**Files:**
- Create: `src/crypto_momentum_lab/execution_account/orders/quantization.py`
- Create: `tests/unit/execution_account/orders/test_quantization.py`

- [ ] **Step 1: Write failing quantization tests**

Write tests named:

- `test_quantize_market_quantity_to_step_size`: verifies market quantity is rounded down to exchange step size.
- `test_rejects_below_min_notional`: verifies orders below minimum notional return a quantization rejection.
- `test_rejects_resize_beyond_tolerance`: verifies rounding that changes notional beyond tolerance is rejected.

- [ ] **Step 2: Verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/execution_account/orders/test_quantization.py -v
```

Expected: FAIL because quantization module does not exist.

- [ ] **Step 3: Implement quantizer**

Create `SymbolTradingRules` and `quantize_order_plan(intent, rules, tolerance)` returning either `QuantizedOrderPlan` or `QuantizationRejection`.

- [ ] **Step 4: Verify and commit**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/execution_account/orders/test_quantization.py -v
.venv/bin/ruff check src/crypto_momentum_lab/execution_account/orders tests/unit/execution_account/orders
.venv/bin/mypy src
```

Commit:

```bash
git add src/crypto_momentum_lab/execution_account/orders/quantization.py tests/unit/execution_account/orders/test_quantization.py
git commit -m "feat: add exchange order quantization"
```

---

### Task 4: State Machine With Fake Exchange

**Files:**
- Create: `src/crypto_momentum_lab/execution_account/orders/state_machine.py`
- Modify: `src/crypto_momentum_lab/execution_account/binance/client.py`
- Create: `tests/unit/execution_account/orders/test_state_machine.py`
- Create: `tests/e2e/test_order_execution_fake_exchange.py`

- [ ] **Step 1: Write failing state machine tests**

Write tests named:

- `test_timeout_queries_by_client_order_id_before_retry`: simulates submit timeout and asserts lookup by deterministic client order ID before retry.
- `test_clear_reject_persists_rejected_state`: simulates exchange rejection and asserts terminal rejected state.
- `test_partial_fill_remains_unresolved`: simulates partial fill and asserts the order remains unresolved.
- `test_terminal_fill_updates_order_state`: simulates full fill and asserts terminal filled state plus persisted fill event.

- [ ] **Step 2: Verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/execution_account/orders/test_state_machine.py -v
```

Expected: FAIL because state machine does not exist.

- [ ] **Step 3: Implement state machine**

Implement `OrderExecutionStateMachine.execute_approved_intent()` with submit policy `shadow_suppress` or `live_submit`. In this plan, real submit paths remain guarded by `live_submit_enabled`.

- [ ] **Step 4: Add fake exchange e2e**

Create a fake exchange client that simulates success, timeout-then-found, timeout-then-not-found, reject, partial fill, and terminal fill.

- [ ] **Step 5: Verify and commit**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/execution_account/orders/test_state_machine.py tests/e2e/test_order_execution_fake_exchange.py -v
.venv/bin/ruff check src/crypto_momentum_lab/execution_account tests/unit/execution_account tests/e2e
.venv/bin/mypy src
```

Commit:

```bash
git add src/crypto_momentum_lab/execution_account tests/unit/execution_account/orders/test_state_machine.py tests/e2e/test_order_execution_fake_exchange.py
git commit -m "feat: add order execution state machine"
```

---

## Completion Criteria

- Client order IDs are deterministic, unique by intent, and within Binance limits verified during implementation.
- Order intents, claims, exchange orders, order events, fills, commands, and reconciliation events are persisted durably.
- Quantization enforces tick size, step size, min notional, and resize tolerance.
- State machine handles success, reject, timeout-then-found, timeout-then-not-found, partial fill, and terminal fill paths.
- Ambiguous orders are queried by client order ID before retry and block new exposure while unresolved.
- Real exchange submission remains gated by explicit live enablement.
