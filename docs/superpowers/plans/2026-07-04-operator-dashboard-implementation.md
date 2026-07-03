# Operator Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan.

**Goal:** Provide a local read-only operator dashboard that shows whether the real-trading system is safe, stale, halted, shadowing, or live, without exposing Binance credentials or bypassing backend command records.

**Architecture:** Add a small FastAPI dashboard app with read-only PostgreSQL query adapters and package-local static HTML/CSS/JavaScript. The dashboard never calls Binance directly and initially exposes no write actions; future controls are represented as disabled UI sections until audited backend command records exist.

**Tech Stack:** Python 3.13, FastAPI, Uvicorn, Pydantic, SQLAlchemy 2 async ORM, PostgreSQL, vanilla HTML/CSS/JavaScript with polling, pytest, ruff, mypy.

---

### Task 1: Dashboard Dependencies, App Boundary, And Read Models

**Files:**

- Modify: `pyproject.toml`
- Create: `src/crypto_momentum_lab/operator_dashboard/__init__.py`
- Create: `src/crypto_momentum_lab/operator_dashboard/schemas.py`
- Create: `src/crypto_momentum_lab/operator_dashboard/status.py`
- Create: `src/crypto_momentum_lab/operator_dashboard/queries.py`
- Create: `src/crypto_momentum_lab/apps/operator_dashboard/__init__.py`
- Create: `src/crypto_momentum_lab/apps/operator_dashboard/main.py`
- Create: `tests/unit/operator_dashboard/test_status.py`
- Create: `tests/unit/operator_dashboard/test_schemas.py`
- Create: `tests/unit/apps/operator_dashboard/test_main.py`

**Step 1: Write failing tests**

Write tests named:

- `test_status_unknown_when_timestamp_missing`: verifies missing freshness data renders as `UNKNOWN`.
- `test_status_stale_when_age_exceeds_threshold`: verifies stale account or market timestamps do not render as safe.
- `test_dashboard_overview_schema_excludes_secret_fields`: verifies API schemas do not contain API keys, secrets, or raw credential names.
- `test_dashboard_app_serves_health_endpoint`: verifies `/api/health` returns database and app status.
- `test_dashboard_app_mounts_static_index`: verifies `/` returns the packaged dashboard HTML.

**Step 2: Verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/operator_dashboard/test_status.py tests/unit/operator_dashboard/test_schemas.py tests/unit/apps/operator_dashboard/test_main.py -v
```

Expected: FAIL because the dashboard package and dependencies do not exist.

**Step 3: Add dependencies and script**

Add project dependencies:

```toml
"fastapi>=0.116,<1",
"uvicorn[standard]>=0.35,<1",
```

Add script entry:

```toml
cml-operator-dashboard = "crypto_momentum_lab.apps.operator_dashboard.main:main"
```

**Step 4: Define read models**

Create Pydantic schemas for:

- `SystemOverviewResponse`
- `ServiceStatusResponse`
- `UniverseStatusResponse`
- `StrategyRunResponse`
- `AccountOverviewResponse`
- `RiskExecutionResponse`
- `RunReportSummaryResponse`

All timestamps must be timezone-aware ISO 8601 strings or `null` with an explicit `UNKNOWN` status.

**Step 5: Verify and commit**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/operator_dashboard/test_status.py tests/unit/operator_dashboard/test_schemas.py tests/unit/apps/operator_dashboard/test_main.py -v
.venv/bin/ruff check src/crypto_momentum_lab/operator_dashboard src/crypto_momentum_lab/apps/operator_dashboard tests/unit/operator_dashboard tests/unit/apps/operator_dashboard
.venv/bin/mypy src
```

Commit:

```bash
git add pyproject.toml src/crypto_momentum_lab/operator_dashboard src/crypto_momentum_lab/apps/operator_dashboard tests/unit/operator_dashboard tests/unit/apps/operator_dashboard
git commit -m "feat: add operator dashboard app boundary"
```

---

### Task 2: Read-Only Dashboard API

**Files:**

- Modify: `src/crypto_momentum_lab/operator_dashboard/queries.py`
- Create: `src/crypto_momentum_lab/operator_dashboard/api.py`
- Create: `tests/unit/operator_dashboard/test_api.py`
- Create: `tests/integration/test_operator_dashboard_api.py`

**Step 1: Write failing tests**

Write tests named:

- `test_overview_endpoint_aggregates_service_status`: seeds fixture state and verifies `/api/overview` includes market-data, strategy-runner, execution-account, database, halt, and lease status.
- `test_universe_endpoint_returns_top_gainers_losers_and_monitored_symbols`: verifies current UTC+0 top 20 gainers, top 20 losers, and monitoring universe are present.
- `test_strategy_endpoint_returns_signals_and_rejection_summary`: verifies selected strategy, run ID, config hash, checkpoint age, latest signals, and rejects.
- `test_account_endpoint_returns_positions_without_credentials`: verifies balances, positions, open orders, fills, and no secrets.
- `test_risk_execution_endpoint_returns_unresolved_orders`: verifies risk decisions, planned orders, submitted orders, ambiguous orders, and halt reasons.

**Step 2: Verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/operator_dashboard/test_api.py tests/integration/test_operator_dashboard_api.py -v
```

Expected: FAIL because API routes and query adapters are missing.

**Step 3: Implement query adapters**

Each query adapter must read from existing repositories or SQLAlchemy selects. It must never instantiate Binance clients or read credential environment variables.

Required adapters:

- system overview query;
- universe and market freshness query;
- strategy run query;
- account query;
- risk and execution query;
- paper, shadow, and live report summary query.

**Step 4: Implement API routes**

Routes:

- `GET /api/health`
- `GET /api/overview`
- `GET /api/universe`
- `GET /api/strategy-runs/current`
- `GET /api/account`
- `GET /api/risk-execution`
- `GET /api/reports`

Failures must return clear degraded statuses rather than HTTP 500 for missing optional subsystem rows. Database connection failure can return HTTP 503 with `database_status="DOWN"`.

**Step 5: Verify and commit**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/operator_dashboard/test_api.py tests/integration/test_operator_dashboard_api.py -v
.venv/bin/ruff check src/crypto_momentum_lab/operator_dashboard tests/unit/operator_dashboard tests/integration/test_operator_dashboard_api.py
.venv/bin/mypy src
```

Commit:

```bash
git add src/crypto_momentum_lab/operator_dashboard tests/unit/operator_dashboard tests/integration/test_operator_dashboard_api.py
git commit -m "feat: add read-only operator dashboard api"
```

---

### Task 3: Static Dashboard UI

**Files:**

- Create: `src/crypto_momentum_lab/operator_dashboard/static/index.html`
- Create: `src/crypto_momentum_lab/operator_dashboard/static/dashboard.css`
- Create: `src/crypto_momentum_lab/operator_dashboard/static/dashboard.js`
- Modify: `pyproject.toml`
- Create: `tests/unit/operator_dashboard/test_static_assets.py`
- Create: `tests/smoke/test_operator_dashboard_static.py`

**Step 1: Write failing tests**

Write tests named:

- `test_static_index_contains_dashboard_mount`: verifies the HTML contains the expected root containers for overview, universe, strategy, account, risk, and reports.
- `test_static_javascript_uses_relative_api_paths`: verifies JavaScript calls `/api/overview` style paths and no Binance hostnames.
- `test_static_assets_are_packaged`: verifies package metadata includes the static files.
- `test_degraded_status_labels_are_visible`: verifies the HTML or JavaScript renders `UNKNOWN`, `STALE`, `HALTED`, and `LIVE`.

**Step 2: Verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/operator_dashboard/test_static_assets.py tests/smoke/test_operator_dashboard_static.py -v
```

Expected: FAIL because static dashboard assets do not exist.

**Step 3: Implement UI layout**

Create sections:

- system overview;
- universe and market data;
- selected strategy run;
- account;
- risk and execution;
- reports;
- disabled controlled actions.

Visual requirements:

- safe state is not the default color;
- stale, unknown, halted, live, and ambiguous-order states are high contrast;
- empty states explicitly say `UNKNOWN` or `NO DATA`;
- dangerous controls are disabled and explain which backend command record is missing.

**Step 4: Implement polling**

Use vanilla JavaScript polling with:

- independent request per API section;
- per-section stale age display;
- failed request rendering as `UNKNOWN`;
- no credential fields in DOM;
- no Binance network calls from the browser.

**Step 5: Package static files**

Update Hatch configuration so the static assets are included in the wheel and served by the FastAPI app.

**Step 6: Verify and commit**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/operator_dashboard/test_static_assets.py tests/smoke/test_operator_dashboard_static.py -v
.venv/bin/ruff check tests/unit/operator_dashboard tests/smoke
.venv/bin/mypy src
```

Commit:

```bash
git add pyproject.toml src/crypto_momentum_lab/operator_dashboard/static tests/unit/operator_dashboard/test_static_assets.py tests/smoke/test_operator_dashboard_static.py
git commit -m "feat: add read-only operator dashboard ui"
```

---

### Task 4: Local Run, Browser Smoke, And Future Action Guards

**Files:**

- Create: `docs/runbooks/operator-dashboard.md`
- Create: `tests/e2e/test_operator_dashboard_local.py`
- Create: `tests/unit/operator_dashboard/test_action_guards.py`

**Step 1: Write failing tests**

Write tests named:

- `test_future_action_routes_return_not_implemented`: verifies halt, drain, cancel-all, and flatten endpoints do not exist or return a disabled response until command records are implemented.
- `test_dashboard_local_server_renders_overview`: starts the app against fixture data and verifies `/` plus `/api/overview` work.
- `test_dashboard_runbook_mentions_no_direct_binance_calls`: scans the runbook for the browser safety rule.

**Step 2: Verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/operator_dashboard/test_action_guards.py tests/e2e/test_operator_dashboard_local.py -v
```

Expected: FAIL because runbook and local server smoke coverage are missing.

**Step 3: Implement action guards**

The dashboard backend must reject or omit write routes for:

- global halt;
- strategy drain;
- release lease;
- cancel all open orders;
- emergency flatten.

If future-action UI buttons exist, they must remain disabled and link to the corresponding backend command record requirements.

**Step 4: Add runbook**

The runbook must include:

- local start command;
- required `DATABASE_URL`;
- expected screens;
- degraded status meanings;
- statement that dashboard never calls Binance directly;
- statement that write actions require backend command records and audit events.

**Step 5: Browser smoke**

Run the local server:

```bash
cml-operator-dashboard --database-url "$CML_DATABASE_URL" --host 127.0.0.1 --port 8765
```

Open:

```text
http://127.0.0.1:8765/
```

Verify the overview, universe, strategy, account, risk, and reports sections render against fixture or local PostgreSQL data.

**Step 6: Verify and commit**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/operator_dashboard/test_action_guards.py tests/e2e/test_operator_dashboard_local.py tests/smoke/test_operator_dashboard_static.py -v
.venv/bin/ruff check src/crypto_momentum_lab/operator_dashboard tests/unit/operator_dashboard tests/e2e tests/smoke
.venv/bin/mypy src
```

Commit:

```bash
git add docs/runbooks/operator-dashboard.md src/crypto_momentum_lab/operator_dashboard tests/unit/operator_dashboard tests/e2e/test_operator_dashboard_local.py
git commit -m "docs: add operator dashboard runbook"
```

---

## Completion Criteria

- Local dashboard shows system overview, universe, strategy, account, risk, execution, and report state.
- Stale, unknown, halted, shadow, and live states are visually distinct.
- API responses and DOM never expose Binance credentials.
- Browser never calls Binance directly.
- Dashboard is read-only until audited backend command records exist.
- Operator can run the dashboard locally against PostgreSQL before shadow or small-capital live sessions.
