# Small-Capital Live Rollout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan.

**Goal:** Enable one selected strategy to place small real Binance USD-M Futures orders only after explicit operator approval, strict risk limits, active reconciliation, and tested rollback controls.

**Architecture:** Add a live rollout service that gates `SubmitPolicy.LIVE_SUBMIT` behind persisted operator approval, active strategy lease, account readiness, risk gateway state, and fixed small-capital limits. The live session owns runbook state, reports, halt transitions, cancel controls, and final reconciliation.

**Tech Stack:** Python 3.13, Typer CLI, SQLAlchemy 2 async ORM, Alembic, PostgreSQL JSONB/Numeric/timestamptz, existing Binance execution client, existing risk gateway, existing order execution state machine, pytest, ruff, mypy.

---

### Task 1: Live Enablement Gate And Approval Record

**Files:**

- Create: `src/crypto_momentum_lab/domain/live_rollout/models.py`
- Create: `src/crypto_momentum_lab/live_rollout/gates.py`
- Modify: `src/crypto_momentum_lab/persistence/postgres/models.py`
- Create: `src/crypto_momentum_lab/persistence/postgres/live_rollout_repository.py`
- Create: `alembic/versions/20260704_0010_small_capital_live_rollout.py`
- Create: `tests/unit/domain/live_rollout/test_models.py`
- Create: `tests/unit/live_rollout/test_gates.py`
- Create: `tests/integration/persistence/test_live_rollout_repository.py`

**Step 1: Write failing tests**

Write tests named:

- `test_live_gate_rejects_without_operator_approval`: builds a complete context except approval and expects a blocked decision.
- `test_live_gate_rejects_when_live_submit_disabled`: persists approval but keeps config flag false and expects no live submit.
- `test_live_gate_rejects_without_ready_account_sync`: sets account state to stale and expects rejection.
- `test_live_gate_rejects_when_strategy_lease_missing`: uses no active lease and expects rejection.
- `test_live_gate_accepts_complete_preflight_context`: passes approval, lease, ready account, no halt, and risk config with explicit numeric limits.

**Step 2: Verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/domain/live_rollout/test_models.py tests/unit/live_rollout/test_gates.py tests/integration/persistence/test_live_rollout_repository.py -v
```

Expected: FAIL because live rollout domain and repository do not exist.

**Step 3: Implement approval schema**

Persist approval records with:

- approval ID;
- account label;
- strategy name;
- strategy config hash;
- risk config hash;
- git commit hash;
- database migration revision;
- approved notional cap;
- approved max open positions;
- approved max daily loss;
- approver name;
- approval text;
- expiration time;
- created time.

Approval text must match a configured confirmation phrase such as `ENABLE SMALL LIVE TRADING`.

**Step 4: Implement gate evaluation**

The gate returns `LiveGateDecision` with status `APPROVED` or `BLOCKED`. It must evaluate:

1. `live_submit_enabled=true`;
2. account label selected;
3. selected strategy lease active and owned by the live service;
4. submit policy requested as `live_submit`;
5. explicit risk config numeric limits present;
6. operator approval present, unexpired, and matching config hashes;
7. account reconciliation state `READY`;
8. no global halt;
9. no unresolved ambiguous order;
10. database migration revision matches approval.

**Step 5: Verify and commit**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/domain/live_rollout/test_models.py tests/unit/live_rollout/test_gates.py tests/integration/persistence/test_live_rollout_repository.py -v
PYTHONPATH=src .venv/bin/python -m pytest tests/integration/persistence/test_migrations.py -v
.venv/bin/ruff check src/crypto_momentum_lab/domain/live_rollout src/crypto_momentum_lab/live_rollout src/crypto_momentum_lab/persistence/postgres tests/unit tests/integration
.venv/bin/mypy src
```

Commit:

```bash
git add alembic/versions/20260704_0010_small_capital_live_rollout.py src/crypto_momentum_lab/domain/live_rollout src/crypto_momentum_lab/live_rollout src/crypto_momentum_lab/persistence/postgres tests/unit/domain/live_rollout tests/unit/live_rollout tests/integration/persistence/test_live_rollout_repository.py
git commit -m "feat: add live rollout gate"
```

---

### Task 2: Live Session Lifecycle And Fixed Limits

**Files:**

- Create: `src/crypto_momentum_lab/live_rollout/session.py`
- Create: `src/crypto_momentum_lab/live_rollout/limits.py`
- Create: `src/crypto_momentum_lab/apps/live_rollout/__init__.py`
- Create: `src/crypto_momentum_lab/apps/live_rollout/main.py`
- Modify: `pyproject.toml`
- Create: `tests/unit/live_rollout/test_session.py`
- Create: `tests/unit/live_rollout/test_limits.py`
- Create: `tests/unit/apps/live_rollout/test_main.py`
- Create: `tests/e2e/test_small_capital_live_fake_exchange.py`

**Step 1: Write failing tests**

Write tests named:

- `test_session_preflight_runs_shadow_before_live`: verifies the service runs a configured shadow preflight before live submit.
- `test_session_submits_only_after_gate_approval`: verifies no exchange submit happens until gate status is `APPROVED`.
- `test_fixed_notional_limit_caps_entry_size`: verifies candidate notional is capped to approved small notional.
- `test_one_position_limit_blocks_second_symbol`: verifies the second exposure is rejected while one position is open.
- `test_unresolved_order_halts_new_entries`: verifies unresolved order state blocks new orders.
- `test_cli_requires_confirmation_flag_for_live_run`: verifies live run command rejects missing explicit confirmation.

**Step 2: Verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/live_rollout/test_session.py tests/unit/live_rollout/test_limits.py tests/unit/apps/live_rollout/test_main.py tests/e2e/test_small_capital_live_fake_exchange.py -v
```

Expected: FAIL because the live session service and CLI do not exist.

**Step 3: Implement fixed limit checks**

Add deterministic checks for:

- fixed candidate notional;
- maximum one open position;
- maximum one new entry per symbol per cooldown window;
- daily realized and unrealized loss limit;
- gross exposure limit;
- spread limit;
- min-notional compliance;
- account freshness;
- market freshness;
- unresolved order uncertainty.

All limits must reject by default when required data is missing.

**Step 4: Implement session lifecycle**

Session states:

- `PREFLIGHT`
- `SHADOW_PREFLIGHT`
- `LIVE_ENABLED`
- `DRAINING`
- `HALTED`
- `RECONCILING`
- `COMPLETED`

State transitions must be persisted with timestamps, reason, operator, and config hashes.

**Step 5: Implement CLI**

Add script entry:

```toml
cml-live-rollout = "crypto_momentum_lab.apps.live_rollout.main:app"
```

Required commands:

- `approve`: persists an operator approval record.
- `preflight`: checks account, lease, migration, config hash, and shadow readiness.
- `run`: executes the live session with `SubmitPolicy.LIVE_SUBMIT`.
- `status`: prints current session, account, risk, and unresolved order state.
- `disable-new-entries`: transitions session to draining.
- `report`: exports final live report.

The `run` command must require an explicit flag such as `--i-understand-this-places-real-orders`.

**Step 6: Verify and commit**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/live_rollout tests/unit/apps/live_rollout tests/e2e/test_small_capital_live_fake_exchange.py -v
.venv/bin/ruff check src/crypto_momentum_lab/live_rollout src/crypto_momentum_lab/apps/live_rollout tests/unit/live_rollout tests/unit/apps/live_rollout tests/e2e
.venv/bin/mypy src
```

Commit:

```bash
git add pyproject.toml src/crypto_momentum_lab/live_rollout src/crypto_momentum_lab/apps/live_rollout tests/unit/live_rollout tests/unit/apps/live_rollout tests/e2e/test_small_capital_live_fake_exchange.py
git commit -m "feat: add small-capital live session"
```

---

### Task 3: Rollback, Cancel, And Emergency Controls

**Files:**

- Create: `src/crypto_momentum_lab/live_rollout/rollback.py`
- Create: `src/crypto_momentum_lab/live_rollout/commands.py`
- Modify: `src/crypto_momentum_lab/execution_account/binance/client.py`
- Modify: `src/crypto_momentum_lab/execution_account/orders/state_machine.py`
- Create: `tests/unit/live_rollout/test_rollback.py`
- Create: `tests/unit/live_rollout/test_commands.py`
- Create: `tests/unit/execution_account/test_cancel_controls.py`

**Step 1: Write failing tests**

Write tests named:

- `test_disable_new_entries_allows_reduce_only_orders`: verifies draining blocks entries but allows reduce-only exits.
- `test_cancel_all_requires_operator_command_record`: verifies cancel calls are rejected until a command record exists.
- `test_cancel_non_reduce_only_orders_uses_idempotent_client_ids`: verifies repeated cancel commands do not duplicate state transitions.
- `test_emergency_flatten_requires_explicit_confirmation`: verifies flatten cannot run without configured confirmation text.
- `test_reconcile_until_flat_before_releasing_lease`: verifies lease release is rejected while positions remain.

**Step 2: Verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/live_rollout/test_rollback.py tests/unit/live_rollout/test_commands.py tests/unit/execution_account/test_cancel_controls.py -v
```

Expected: FAIL because rollback command records and cancel controls do not exist.

**Step 3: Implement command record model**

Persist command records for:

- global halt;
- disable new entries;
- strategy drain;
- cancel all open orders;
- emergency flatten;
- release lease after flat reconciliation.

Each record must include command ID, requested by, confirmation text, request time, idempotency key, target account label, target strategy, current session ID, status, completion time, and failure reason.

**Step 4: Implement rollback operations**

Rollback order:

1. disable new entries;
2. mark selected strategy `DRAINING`;
3. cancel non-reduce-only open orders;
4. continue account reconciliation until open orders and positions match Binance;
5. flatten only through explicit emergency command;
6. release strategy lease only after local and Binance state agree that the account is flat.

**Step 5: Verify and commit**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/live_rollout/test_rollback.py tests/unit/live_rollout/test_commands.py tests/unit/execution_account/test_cancel_controls.py -v
.venv/bin/ruff check src/crypto_momentum_lab/live_rollout src/crypto_momentum_lab/execution_account tests/unit/live_rollout tests/unit/execution_account
.venv/bin/mypy src
```

Commit:

```bash
git add src/crypto_momentum_lab/live_rollout src/crypto_momentum_lab/execution_account tests/unit/live_rollout tests/unit/execution_account
git commit -m "feat: add live rollback controls"
```

---

### Task 4: Final Reconciliation Report And Manual Live Probe

**Files:**

- Create: `src/crypto_momentum_lab/live_rollout/reports.py`
- Create: `docs/runbooks/small-capital-live-session.md`
- Create: `tests/unit/live_rollout/test_reports.py`
- Create: `tests/smoke/test_small_capital_live_runbook.py`

**Step 1: Write failing tests**

Write tests named:

- `test_live_report_includes_fees_slippage_and_drawdown`: verifies the final report contains realized fees, estimated slippage, realized PnL, and drawdown.
- `test_live_report_flags_reconciliation_mismatch`: verifies report status is blocked when local and Binance states disagree.
- `test_runbook_requires_live_submit_disable_after_session`: scans the runbook for the post-session disable step.
- `test_runbook_mentions_real_money_confirmation`: scans the runbook for the exact CLI confirmation flag.

**Step 2: Verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/live_rollout/test_reports.py tests/smoke/test_small_capital_live_runbook.py -v
```

Expected: FAIL because report generation and runbook are missing.

**Step 3: Implement report generation**

Final live report must include:

- signal count;
- approved and rejected intents;
- submitted order count;
- filled, partially filled, canceled, and rejected order counts;
- realized fees;
- estimated slippage;
- state-close to exchange-ack latency;
- position holding time;
- risk halt events;
- reconciliation mismatches;
- realized PnL;
- drawdown;
- account flat confirmation;
- lease release confirmation.

**Step 4: Write runbook**

The runbook must cover:

- exact pre-session checklist;
- command sequence for approval, preflight, live run, status, drain, and report;
- emergency cancel and flatten commands;
- post-session reconciliation;
- final review criteria before any capital increase.

**Step 5: Run manual-gated live probe**

Only after shadow acceptance and operator approval, run one tiny live order on the selected Binance USD-M Futures account using the configured small notional. Immediately cancel or close according to the runbook and reconcile account state.

Record:

- command transcript path;
- live session ID;
- Binance order ID;
- client order ID;
- fill status;
- fees;
- final account status;
- report artifact path.

**Step 6: Verify and commit**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/live_rollout/test_reports.py tests/smoke/test_small_capital_live_runbook.py -v
.venv/bin/ruff check src/crypto_momentum_lab/live_rollout tests/unit/live_rollout tests/smoke
.venv/bin/mypy src
```

Commit:

```bash
git add src/crypto_momentum_lab/live_rollout/reports.py docs/runbooks/small-capital-live-session.md tests/unit/live_rollout/test_reports.py tests/smoke/test_small_capital_live_runbook.py
git commit -m "docs: add small-capital live runbook"
```

---

## Completion Criteria

- Live submit cannot start unless every gate passes and an approval record matches the current account, strategy, risk config, git commit, and migration revision.
- A single selected strategy can submit small real orders through the order state machine with idempotent client order IDs.
- Fixed notional, daily loss, gross exposure, spread, freshness, cooldown, and unresolved-order limits are enforced.
- Rollback, cancel, emergency flatten, reconciliation, and lease release are implemented as audited commands.
- Final report proves account state, strategy state, order state, fees, slippage, PnL, and drawdown.
- No second strategy can trade concurrently.
