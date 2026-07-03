# Shadow Operation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan.

**Goal:** Run the selected strategy through the real live execution path with real market data, real account state, real risk checks, and real Binance exchange metadata, while durably suppressing every exchange write.

**Architecture:** Add a `SHADOW` run mode that orchestrates account readiness, strategy lease, risk gateway, order quantization, order execution state machine, and shadow suppression records. The live order submit boundary remains centralized in the order state machine; shadow mode proves that the same path can run without calling Binance write endpoints.

**Tech Stack:** Python 3.13, Typer CLI, SQLAlchemy 2 async ORM, Alembic, PostgreSQL JSONB/Numeric/timestamptz, existing market-data and strategy-runner services, existing execution-account and risk modules, pytest, ruff, mypy.

---

### Task 1: Shadow Mode Domain And Submit Policy

**Files:**

- Modify: `src/crypto_momentum_lab/domain/execution/models.py`
- Modify: `src/crypto_momentum_lab/domain/execution/order_state.py`
- Modify: `src/crypto_momentum_lab/execution_account/orders/state_machine.py`
- Create: `tests/unit/domain/execution/test_shadow_mode.py`
- Create: `tests/unit/execution_account/orders/test_shadow_submit_policy.py`

**Step 1: Write failing tests**

Write tests named:

- `test_run_mode_accepts_shadow_and_live_as_distinct_modes`: constructs the execution run mode value object and verifies `SHADOW` is not treated as `LIVE`.
- `test_shadow_submit_policy_records_suppression_without_submit`: passes a risk-approved quantized order plan to the state machine with `shadow_suppress` and asserts the fake exchange client has zero submit calls.
- `test_live_submit_policy_uses_submit_boundary`: passes the same order plan with `live_submit` and asserts the fake exchange receives exactly one submit call.
- `test_shadow_policy_still_requires_quantized_order_plan`: passes an unquantized plan and asserts the state machine rejects it before suppression.

**Step 2: Verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/domain/execution/test_shadow_mode.py tests/unit/execution_account/orders/test_shadow_submit_policy.py -v
```

Expected: FAIL because shadow run mode and submit policy are not implemented.

**Step 3: Implement domain values**

Add explicit run mode and submit policy values:

```python
class ExecutionRunMode(StrEnum):
    PAPER = "paper"
    PAPER_DAEMON = "paper_daemon"
    SHADOW = "shadow"
    LIVE = "live"


class SubmitPolicy(StrEnum):
    SHADOW_SUPPRESS = "shadow_suppress"
    LIVE_SUBMIT = "live_submit"
```

Ensure only `SubmitPolicy.LIVE_SUBMIT` can reach the Binance write client method.

**Step 4: Implement suppression event creation**

In the state machine, add a branch that persists a terminal local event:

```python
ShadowSuppressionEvent(
    order_plan_id=order_plan.order_plan_id,
    client_order_id=order_plan.client_order_id,
    suppressed_at=clock.now(),
    reason="shadow_submit_policy",
)
```

The event must contain the exact order payload that would have been sent.

**Step 5: Verify and commit**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/domain/execution/test_shadow_mode.py tests/unit/execution_account/orders/test_shadow_submit_policy.py -v
.venv/bin/ruff check src/crypto_momentum_lab/domain src/crypto_momentum_lab/execution_account tests/unit/domain tests/unit/execution_account
.venv/bin/mypy src
```

Commit:

```bash
git add src/crypto_momentum_lab/domain/execution src/crypto_momentum_lab/execution_account/orders tests/unit/domain/execution tests/unit/execution_account/orders
git commit -m "feat: add shadow submit policy"
```

---

### Task 2: Shadow Persistence And Reports

**Files:**

- Modify: `src/crypto_momentum_lab/persistence/postgres/models.py`
- Create: `src/crypto_momentum_lab/persistence/postgres/shadow_repository.py`
- Create: `alembic/versions/20260704_0009_shadow_operation.py`
- Create: `src/crypto_momentum_lab/shadow_operation/reports.py`
- Create: `tests/unit/persistence/postgres/test_shadow_models.py`
- Create: `tests/integration/persistence/test_shadow_repository.py`
- Create: `tests/unit/shadow_operation/test_reports.py`

**Step 1: Write failing persistence tests**

Write tests named:

- `test_shadow_order_plan_row_requires_order_payload`: creates a row without payload and expects validation or database constraint failure.
- `test_save_shadow_suppression_is_idempotent_by_order_plan`: saves the same suppression event twice and verifies a single row remains.
- `test_shadow_report_counts_rejected_and_suppressed_orders`: builds fixture rows and verifies report counts.
- `test_shadow_report_includes_latency_buckets`: verifies state-close to order-plan latency is calculated from stored timestamps.

**Step 2: Verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/persistence/postgres/test_shadow_models.py tests/integration/persistence/test_shadow_repository.py tests/unit/shadow_operation/test_reports.py -v
```

Expected: FAIL because the shadow tables and report code do not exist.

**Step 3: Add database schema**

Add tables for:

- `shadow_sessions`
- `shadow_order_plans`
- `shadow_suppression_events`
- `shadow_decision_metrics`
- `shadow_drill_results`

Required constraints:

- unique `(run_id, order_intent_id)` for order plans;
- unique `(order_plan_id)` for suppression events;
- non-null account readiness, market freshness, risk result, and order payload fields;
- indexes on `run_id`, `symbol`, `created_at`, and `decision_state`.

**Step 4: Implement repository**

The repository must support:

- starting and ending a shadow session;
- saving an order plan;
- saving a suppression event idempotently;
- loading unresolved shadow plans;
- aggregating report rows by run ID.

**Step 5: Implement report generation**

Report output must include:

- signal count;
- approved intents;
- rejected intents grouped by reason;
- would-submit order count;
- suppression count;
- paper fill comparison count;
- spread and min-notional block count;
- stale data block count;
- account/risk block count;
- state-close to plan latency percentiles;
- restart and drill outcomes.

**Step 6: Verify and commit**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/persistence/postgres/test_shadow_models.py tests/integration/persistence/test_shadow_repository.py tests/unit/shadow_operation/test_reports.py -v
PYTHONPATH=src .venv/bin/python -m pytest tests/integration/persistence/test_migrations.py -v
.venv/bin/ruff check src/crypto_momentum_lab/persistence/postgres src/crypto_momentum_lab/shadow_operation tests/unit tests/integration
.venv/bin/mypy src
```

Commit:

```bash
git add alembic/versions/20260704_0009_shadow_operation.py src/crypto_momentum_lab/persistence/postgres src/crypto_momentum_lab/shadow_operation tests/unit tests/integration
git commit -m "feat: persist shadow operation records"
```

---

### Task 3: Shadow Runner And CLI

**Files:**

- Create: `src/crypto_momentum_lab/shadow_operation/service.py`
- Create: `src/crypto_momentum_lab/shadow_operation/drills.py`
- Create: `src/crypto_momentum_lab/apps/shadow_operation/__init__.py`
- Create: `src/crypto_momentum_lab/apps/shadow_operation/main.py`
- Modify: `pyproject.toml`
- Create: `tests/unit/shadow_operation/test_service.py`
- Create: `tests/unit/shadow_operation/test_drills.py`
- Create: `tests/unit/apps/shadow_operation/test_main.py`
- Create: `tests/e2e/test_shadow_operation_fake_services.py`

**Step 1: Write failing service tests**

Write tests named:

- `test_shadow_service_requires_active_strategy_lease`: starts without lease and expects a halt result.
- `test_shadow_service_requires_ready_account_sync`: starts with stale account state and expects no strategy decisions.
- `test_shadow_service_persists_suppression_for_approved_intent`: feeds one market state and verifies a persisted shadow suppression.
- `test_shadow_service_halts_on_market_staleness`: feeds a state older than the configured freshness threshold and verifies a halt event.
- `test_shadow_service_rejects_any_write_client_call`: injects a write-throwing fake client and verifies the run fails if the write method is reached.

**Step 2: Verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/shadow_operation/test_service.py tests/unit/apps/shadow_operation/test_main.py tests/e2e/test_shadow_operation_fake_services.py -v
```

Expected: FAIL because the shadow runner does not exist.

**Step 3: Implement service orchestration**

The service loop must:

1. load selected strategy from the runtime registry;
2. validate active lease ownership;
3. validate account sync state is `READY`;
4. validate market-data freshness;
5. run strategy on closed market states;
6. send generated order intents through the risk gateway;
7. quantize approved orders using exchange metadata;
8. call order state machine with `SubmitPolicy.SHADOW_SUPPRESS`;
9. persist metrics and checkpoint after each processed state;
10. halt on stale data, account mismatch, risk halt, expired lease, quantization failure, or attempted write call.

**Step 4: Implement CLI**

Add script entry:

```toml
cml-shadow-operation = "crypto_momentum_lab.apps.shadow_operation.main:app"
```

Required CLI commands:

- `run`: executes a bounded or time-windowed shadow session.
- `report`: prints a completed session report as JSON.
- `drill`: executes configured shadow drills against fake services or local PostgreSQL state.

CLI options must include:

- `--database-url`
- `--account-label`
- `--strategy`
- `--run-id`
- `--max-runtime-seconds`
- `--state-stale-after-seconds`
- `--checkpoint-every-states`
- `--require-lease-owner`
- `--json`

**Step 5: Add drill coverage**

Implement drill records for:

- market-data reconnect;
- account stream reconnect;
- database temporary failure;
- process restart with active lease;
- strategy halt;
- stale market data;
- risk daily-loss halt using fixture data;
- order submission ambiguity using fake exchange client.

**Step 6: Verify and commit**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/shadow_operation tests/unit/apps/shadow_operation tests/e2e/test_shadow_operation_fake_services.py -v
.venv/bin/ruff check src/crypto_momentum_lab/shadow_operation src/crypto_momentum_lab/apps/shadow_operation tests/unit/shadow_operation tests/unit/apps/shadow_operation tests/e2e
.venv/bin/mypy src
```

Commit:

```bash
git add pyproject.toml src/crypto_momentum_lab/shadow_operation src/crypto_momentum_lab/apps/shadow_operation tests/unit/shadow_operation tests/unit/apps/shadow_operation tests/e2e/test_shadow_operation_fake_services.py
git commit -m "feat: add shadow operation runner"
```

---

### Task 4: Manual-Gated Shadow Session

**Files:**

- Create: `docs/runbooks/shadow-operation-session.md`
- Create: `tests/smoke/test_shadow_operation_manifest.py`

**Step 1: Write runbook**

The runbook must define:

- preflight checks for current git commit, config hash, database migration head, account label, account readiness, active lease, and global halt state;
- exact command sequence for `cml-execution-account sync-once`, `cml-shadow-operation run`, and `cml-shadow-operation report`;
- expected report fields that must be reviewed before small-capital live;
- halt and restart drill commands;
- explicit statement that no Binance write endpoint is allowed in this phase.

**Step 2: Add smoke manifest test**

The smoke test must assert that the runbook contains the CLI names, report names, and no live-submit command.

**Step 3: Run manual-gated session**

Use real Binance credentials only in a local operator environment and run:

```bash
cml-execution-account sync-once --account-label primary --database-url "$CML_DATABASE_URL"
cml-shadow-operation run --account-label primary --strategy compression_breakout --database-url "$CML_DATABASE_URL" --max-runtime-seconds 3600 --require-lease-owner shadow-preflight
cml-shadow-operation report --run-id "$RUN_ID" --database-url "$CML_DATABASE_URL" --json
```

Record the report artifact path in the operator notes.

**Step 4: Verify and commit**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/smoke/test_shadow_operation_manifest.py -v
.venv/bin/ruff check tests/smoke
```

Commit:

```bash
git add docs/runbooks/shadow-operation-session.md tests/smoke/test_shadow_operation_manifest.py
git commit -m "docs: add shadow operation runbook"
```

---

## Completion Criteria

- `SHADOW` mode uses the same strategy, risk, quantization, and order state-machine path as live submit.
- Binance write endpoints are unreachable in shadow mode by automated test.
- Every would-be order is persisted as a quantized shadow order plan and suppression event.
- Shadow reports include latency, rejects, account blocks, stale-data blocks, and paper/live assumption gaps.
- Restart and halt drills produce durable drill results.
- Small-capital live remains disabled until the live rollout phase records operator approval.
