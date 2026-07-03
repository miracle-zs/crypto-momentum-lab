# Execution Account Read-Only Sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a read-only `execution-account` process that authenticates to Binance USD-M Futures, persists account state, and reconciles local state without placing orders.

**Architecture:** Add account/execution domain models, PostgreSQL persistence, a private Binance read client boundary, and an app CLI. The private client is read-only in this phase; tests use fake clients and manual-gated live checks for credentials.

**Tech Stack:** Python 3.13, HTTPX as already used by Binance REST code, SQLAlchemy 2 async ORM, Alembic, PostgreSQL JSONB/Numeric/timestamptz, Typer, pytest, ruff, mypy.

---

## File Structure

- Create: `src/crypto_momentum_lab/domain/account/models.py`
- Create: `src/crypto_momentum_lab/domain/execution/models.py`
- Create: `src/crypto_momentum_lab/execution_account/binance/client.py`
- Create: `src/crypto_momentum_lab/execution_account/sync.py`
- Create: `src/crypto_momentum_lab/apps/execution_account/main.py`
- Modify: `pyproject.toml`
  - Add `cml-execution-account` script entrypoint.
- Modify: `src/crypto_momentum_lab/persistence/postgres/models.py`
- Create: `src/crypto_momentum_lab/persistence/postgres/account_repository.py`
- Create: `alembic/versions/20260704_0006_account_readonly_sync.py`
- Create: `tests/unit/domain/account/test_models.py`
- Create: `tests/unit/execution_account/test_sync.py`
- Create: `tests/unit/apps/execution_account/test_main.py`
- Create: `tests/unit/persistence/postgres/test_account_repository.py`
- Create: `tests/integration/persistence/test_account_repository.py`
- Create: `tests/manual/test_binance_account_readonly.py`

---

### Task 1: Account Domain Models

**Files:**
- Create: `src/crypto_momentum_lab/domain/account/models.py`
- Create: `tests/unit/domain/account/test_models.py`

- [ ] **Step 1: Write failing domain tests**

Create tests named:

- `test_account_balance_snapshot_requires_aware_time`: passes a naive timestamp and expects validation failure.
- `test_account_position_snapshot_rejects_empty_symbol`: passes an empty symbol and expects validation failure.
- `test_account_sync_state_rejects_unknown_state`: attempts to construct an invalid state and expects validation failure.

- [ ] **Step 2: Verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/domain/account/test_models.py -v
```

Expected: FAIL because `domain.account.models` does not exist.

- [ ] **Step 3: Implement domain models**

Create dataclasses:

```python
AccountBalanceSnapshot
AccountPositionSnapshot
AccountOpenOrderSnapshot
AccountFillEvent
AccountFundingEvent
AccountConfigSnapshot
AccountReconciliationRun
ExecutionAccountProcessState
```

Use explicit `StrEnum` values for process state: `STARTING`, `SYNCING`, `READY_READONLY`, `DEGRADED`, `HALTED_READONLY`, `STOPPED`.

- [ ] **Step 4: Verify and commit**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/domain/account/test_models.py -v
.venv/bin/ruff check src/crypto_momentum_lab/domain/account tests/unit/domain/account
.venv/bin/mypy src
```

Commit:

```bash
git add src/crypto_momentum_lab/domain/account tests/unit/domain/account/test_models.py
git commit -m "feat: add account sync domain models"
```

---

### Task 2: PostgreSQL Account Persistence

**Files:**
- Modify: `src/crypto_momentum_lab/persistence/postgres/models.py`
- Create: `src/crypto_momentum_lab/persistence/postgres/account_repository.py`
- Create: `alembic/versions/20260704_0006_account_readonly_sync.py`
- Create: `tests/unit/persistence/postgres/test_account_repository.py`
- Create: `tests/integration/persistence/test_account_repository.py`

- [ ] **Step 1: Write failing repository mapping tests**

Test row mapping for balance, position, open order, fill, config, and reconciliation run.

- [ ] **Step 2: Verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/persistence/postgres/test_account_repository.py -v
```

Expected: FAIL because `account_repository` does not exist.

- [ ] **Step 3: Add ORM models and migration**

Add tables from the spec:

```text
account_balance_snapshots
account_position_snapshots
account_open_orders
account_order_events
account_fill_events
account_funding_events
account_config_snapshots
account_reconciliation_runs
execution_account_process_states
```

Use `environment`, `account_label`, timestamps, `source_type`, `schema_version`, and compact `raw_payload` JSONB on all exchange-derived rows.

- [ ] **Step 4: Implement repository**

Create `PostgresAccountRepository` with save/load methods:

```python
save_balance_snapshot()
save_position_snapshot()
upsert_open_order()
save_order_event()
save_fill_event()
save_config_snapshot()
save_reconciliation_run()
save_process_state()
load_latest_account_view()
```

- [ ] **Step 5: Verify and commit**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/persistence/postgres/test_account_repository.py -v
docker compose up -d postgres
CML_DATABASE_URL=postgresql+asyncpg://cml:cml@localhost:54329/cml .venv/bin/alembic upgrade head
CML_TEST_ASYNC_DATABASE_URL=postgresql+asyncpg://cml:cml@localhost:54329/cml PYTHONPATH=src .venv/bin/python -m pytest tests/integration/persistence/test_account_repository.py -v
.venv/bin/ruff check alembic src/crypto_momentum_lab/persistence/postgres tests/unit/persistence/postgres tests/integration/persistence
.venv/bin/mypy src
```

Commit:

```bash
git add alembic/versions/20260704_0006_account_readonly_sync.py src/crypto_momentum_lab/persistence/postgres tests/unit/persistence/postgres/test_account_repository.py tests/integration/persistence/test_account_repository.py
git commit -m "feat: persist read-only account sync state"
```

---

### Task 3: Read-Only Binance Client Boundary

**Files:**
- Create: `src/crypto_momentum_lab/execution_account/binance/client.py`
- Create: `tests/unit/execution_account/test_binance_client.py`

- [ ] **Step 1: Verify current official Binance docs**

Before implementation, check Binance USD-M Futures official documentation for account read endpoints, listen-key/user-data stream behavior, timestamp signing, and permission errors. Record endpoint names in a short comment in the client module header.

- [ ] **Step 2: Write failing client tests with fake transport**

Write tests named:

- `test_signed_request_includes_timestamp_and_signature`: verifies the fake transport receives signed query parameters.
- `test_client_fetches_account_snapshot`: returns fixture account JSON and asserts normalized account snapshots.
- `test_client_does_not_expose_order_submit_methods`: asserts the read-only client has no submit, cancel, amend, or flatten attributes.

- [ ] **Step 3: Implement client**

Implement a minimal `BinanceUsdMPrivateReadClient` with async methods that fetch account config, balances, positions, open orders, recent fills, and close the underlying HTTP client. Return immutable sequences for multi-row responses.

Do not add submit, cancel, amend, or flatten methods in this phase.

- [ ] **Step 4: Verify and commit**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/execution_account/test_binance_client.py -v
.venv/bin/ruff check src/crypto_momentum_lab/execution_account tests/unit/execution_account
.venv/bin/mypy src
```

Commit:

```bash
git add src/crypto_momentum_lab/execution_account/binance/client.py tests/unit/execution_account/test_binance_client.py
git commit -m "feat: add read-only binance account client"
```

---

### Task 4: Synchronization Service And CLI

**Files:**
- Create: `src/crypto_momentum_lab/execution_account/sync.py`
- Create: `src/crypto_momentum_lab/apps/execution_account/main.py`
- Modify: `pyproject.toml`
- Create: `tests/unit/execution_account/test_sync.py`
- Create: `tests/unit/apps/execution_account/test_main.py`

- [ ] **Step 1: Write failing service and CLI tests**

Write tests named:

- `test_sync_once_persists_snapshot_and_ready_state`: uses a fake client and asserts repository writes plus `READY_READONLY` process state.
- `test_sync_once_halts_on_account_mode_mismatch`: returns an unexpected account mode and asserts `HALTED_READONLY`.
- `test_execution_account_sync_once_requires_credentials`: invokes CLI without key/secret and asserts a non-zero exit.

- [ ] **Step 2: Verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/execution_account/test_sync.py tests/unit/apps/execution_account/test_main.py -v
```

Expected: FAIL because sync service and app do not exist.

- [ ] **Step 3: Implement sync service**

Create `ExecutionAccountSyncService.sync_once()`:

1. save `STARTING`;
2. validate clock/config using client data;
3. fetch balances, positions, open orders, fills, config;
4. persist snapshots;
5. save reconciliation run;
6. save `READY_READONLY` or `HALTED_READONLY`.

- [ ] **Step 4: Implement CLI**

Add `cml-execution-account sync-once` with options for database URL, environment, account label, base URL, and credential env var names.

- [ ] **Step 5: Verify and commit**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/execution_account tests/unit/apps/execution_account -q
PYTHONPATH=src .venv/bin/cml-execution-account --help
.venv/bin/ruff check src/crypto_momentum_lab/execution_account src/crypto_momentum_lab/apps/execution_account tests/unit/execution_account tests/unit/apps/execution_account
.venv/bin/mypy src
```

Commit:

```bash
git add pyproject.toml src/crypto_momentum_lab/execution_account src/crypto_momentum_lab/apps/execution_account tests/unit/execution_account tests/unit/apps/execution_account
git commit -m "feat: add read-only execution account sync cli"
```

---

## Completion Criteria

- Account domain models validate timezone-aware timestamps, non-empty symbols, and non-negative numeric fields.
- PostgreSQL persistence stores balances, positions, open orders, fills, funding, account config, reconciliation runs, and process state.
- Binance private client is read-only and exposes no submit, cancel, amend, or flatten methods.
- `cml-execution-account sync-once` persists one account snapshot and marks state `READY_READONLY` only when reconciliation passes.
- Account mode or config mismatch halts the read-only process instead of implying readiness.
- Unit, integration, migration, ruff, and mypy verification commands pass.
