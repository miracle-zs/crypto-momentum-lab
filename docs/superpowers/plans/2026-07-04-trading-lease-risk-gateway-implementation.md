# Trading Lease And Risk Gateway Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a single-strategy trading lease and deterministic risk gateway that approves or rejects order intents before execution.

**Architecture:** Store lease state and immutable risk evaluations in PostgreSQL. The gateway consumes market/account/strategy state and returns durable approvals or rejections without submitting orders.

**Tech Stack:** Python 3.13 dataclasses, SQLAlchemy 2 async ORM, Alembic, PostgreSQL, pytest concurrency tests, ruff, mypy.

---

## File Structure

- Create: `src/crypto_momentum_lab/domain/risk/models.py`
- Create: `src/crypto_momentum_lab/risk/gateway.py`
- Create: `src/crypto_momentum_lab/persistence/postgres/risk_repository.py`
- Modify: `src/crypto_momentum_lab/persistence/postgres/models.py`
- Create: `alembic/versions/20260704_0007_trading_lease_risk_gateway.py`
- Create: `tests/unit/domain/risk/test_models.py`
- Create: `tests/unit/risk/test_gateway.py`
- Create: `tests/unit/persistence/postgres/test_risk_repository.py`
- Create: `tests/integration/persistence/test_risk_repository.py`
- Create: `tests/integration/risk/test_trading_lease_concurrency.py`

---

### Task 1: Risk Domain Models

**Files:**
- Create: `src/crypto_momentum_lab/domain/risk/models.py`
- Create: `tests/unit/domain/risk/test_models.py`

- [ ] **Step 1: Write failing tests**

Write tests named:

- `test_risk_config_hash_is_deterministic`: builds the same numeric config in different dictionary order and asserts identical hash.
- `test_order_intent_rejects_expired_timestamp_without_timezone`: passes a naive expiration timestamp and expects validation failure.
- `test_trading_lease_rejects_invalid_state`: constructs a lease with an unsupported state and expects validation failure.

- [ ] **Step 2: Verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/domain/risk/test_models.py -v
```

Expected: FAIL because `domain.risk.models` does not exist.

- [ ] **Step 3: Implement models**

Create:

```python
TradingLease
TradingLeaseState
RiskConfigSnapshot
RiskEvaluation
RiskDecision
RiskRejection
RiskHalt
StrategyLiveState
```

Decision values: `APPROVED`, `REJECTED`, `HALTED`.

- [ ] **Step 4: Verify and commit**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/domain/risk/test_models.py -v
.venv/bin/ruff check src/crypto_momentum_lab/domain/risk tests/unit/domain/risk
.venv/bin/mypy src
```

Commit:

```bash
git add src/crypto_momentum_lab/domain/risk tests/unit/domain/risk/test_models.py
git commit -m "feat: add risk gateway domain models"
```

---

### Task 2: Lease And Risk Persistence

**Files:**
- Modify: `src/crypto_momentum_lab/persistence/postgres/models.py`
- Create: `src/crypto_momentum_lab/persistence/postgres/risk_repository.py`
- Create: `alembic/versions/20260704_0007_trading_lease_risk_gateway.py`
- Create: `tests/unit/persistence/postgres/test_risk_repository.py`
- Create: `tests/integration/persistence/test_risk_repository.py`

- [ ] **Step 1: Write failing repository tests**

Write tests named:

- `test_acquire_lease_persists_active_lease`: acquires a lease and verifies load returns the same owner and expiration.
- `test_save_risk_evaluation_preserves_rejection_reason`: saves a rejection and verifies the reason code and numeric inputs remain intact.
- `test_load_active_halt_returns_global_halt`: saves a global halt and verifies it blocks the account label.

- [ ] **Step 2: Verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/persistence/postgres/test_risk_repository.py -v
```

Expected: FAIL because `risk_repository` does not exist.

- [ ] **Step 3: Add tables**

Add:

```text
trading_leases
risk_config_snapshots
risk_evaluations
risk_rejections
risk_halts
strategy_live_states
```

Add a partial unique index so one `(environment, account_label)` can have only one `ACTIVE` lease.

- [ ] **Step 4: Implement repository**

Create `PostgresRiskRepository` with:

```python
acquire_lease()
renew_lease()
release_lease()
load_active_lease()
save_risk_config()
save_risk_evaluation()
save_halt()
load_active_halts()
save_strategy_live_state()
```

- [ ] **Step 5: Verify and commit**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/persistence/postgres/test_risk_repository.py -v
docker compose up -d postgres
CML_DATABASE_URL=postgresql+asyncpg://cml:cml@localhost:54329/cml .venv/bin/alembic upgrade head
CML_TEST_ASYNC_DATABASE_URL=postgresql+asyncpg://cml:cml@localhost:54329/cml PYTHONPATH=src .venv/bin/python -m pytest tests/integration/persistence/test_risk_repository.py -v
.venv/bin/ruff check alembic src/crypto_momentum_lab/persistence/postgres tests/unit/persistence/postgres tests/integration/persistence
.venv/bin/mypy src
```

Commit:

```bash
git add alembic/versions/20260704_0007_trading_lease_risk_gateway.py src/crypto_momentum_lab/persistence/postgres tests/unit/persistence/postgres/test_risk_repository.py tests/integration/persistence/test_risk_repository.py
git commit -m "feat: persist trading leases and risk decisions"
```

---

### Task 3: Risk Gateway Service

**Files:**
- Create: `src/crypto_momentum_lab/risk/gateway.py`
- Create: `tests/unit/risk/test_gateway.py`

- [ ] **Step 1: Write failing gateway tests**

Write tests named:

- `test_gateway_rejects_missing_active_lease`: passes no lease and expects `REJECTED`.
- `test_gateway_rejects_stale_market_state`: passes an old market timestamp and expects a stale-data rejection.
- `test_gateway_rejects_account_not_ready`: passes degraded account state and expects rejection.
- `test_gateway_approves_small_entry_when_all_limits_pass`: passes a small entry within every limit and expects `APPROVED`.
- `test_gateway_allows_reduce_only_while_draining`: passes draining strategy state and reduce-only intent and expects approval.

- [ ] **Step 2: Verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/risk/test_gateway.py -v
```

Expected: FAIL because `risk.gateway` does not exist.

- [ ] **Step 3: Implement gateway**

Create `RiskGateway.evaluate(intent, context)` returning one `RiskEvaluation` for each order intent.

`RiskContext` contains active lease, latest market state, account view, current positions, current open orders, active halts, and risk config.

- [ ] **Step 4: Verify and commit**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/risk/test_gateway.py -v
.venv/bin/ruff check src/crypto_momentum_lab/risk tests/unit/risk
.venv/bin/mypy src
```

Commit:

```bash
git add src/crypto_momentum_lab/risk tests/unit/risk/test_gateway.py
git commit -m "feat: add deterministic risk gateway"
```

---

### Task 4: Lease Concurrency Verification

**Files:**
- Create: `tests/integration/risk/test_trading_lease_concurrency.py`

- [ ] **Step 1: Write concurrency test**

Create a test that starts two concurrent `acquire_lease()` calls for the same `(environment, account_label)` and asserts exactly one succeeds.

- [ ] **Step 2: Run test**

Run:

```bash
CML_TEST_ASYNC_DATABASE_URL=postgresql+asyncpg://cml:cml@localhost:54329/cml PYTHONPATH=src .venv/bin/python -m pytest tests/integration/risk/test_trading_lease_concurrency.py -v
```

- [ ] **Step 3: Fix repository transaction behavior if needed**

If both attempts succeed, update `PostgresRiskRepository.acquire_lease()` to rely on the partial unique index and translate `IntegrityError` into `LeaseAlreadyHeldError`.

- [ ] **Step 4: Verify and commit**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/risk tests/unit/persistence/postgres/test_risk_repository.py -q
CML_TEST_ASYNC_DATABASE_URL=postgresql+asyncpg://cml:cml@localhost:54329/cml PYTHONPATH=src .venv/bin/python -m pytest tests/integration/risk/test_trading_lease_concurrency.py -v
.venv/bin/ruff check src tests/unit/risk tests/integration/risk
.venv/bin/mypy src
```

Commit:

```bash
git add src/crypto_momentum_lab/persistence/postgres/risk_repository.py tests/integration/risk/test_trading_lease_concurrency.py
git commit -m "test: verify trading lease concurrency"
```

---

## Completion Criteria

- Only one active trading lease can exist for an `(environment, account_label)` pair.
- Risk config snapshots have deterministic hashes and durable numeric limits.
- Every order intent receives exactly one persisted approval, rejection, or halt decision.
- Gateway rejects missing lease, stale market data, degraded account state, active halts, and limit breaches.
- Reduce-only intents can be approved while a strategy is draining.
- Lease concurrency test proves parallel acquisition cannot create two active leases.
