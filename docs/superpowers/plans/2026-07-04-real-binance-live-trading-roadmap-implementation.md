# Real Binance Live Trading Roadmap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Execute the remaining real Binance live-trading work in ordered, independently verifiable phases.

**Architecture:** Treat the roadmap as a program-level dependency plan. Each downstream phase has its own implementation plan and must be completed, tested, and merged before the next riskier phase enables additional capability.

**Tech Stack:** Markdown design docs, Python 3.13, Typer CLIs, SQLAlchemy 2 async ORM, Alembic, PostgreSQL, pytest, ruff, mypy, Docker Compose.

---

## File Structure

- Read: `docs/superpowers/specs/2026-07-03-real-binance-live-trading-roadmap-design.md`
  - Source of phase ordering and invariants.
- Read: `docs/superpowers/plans/2026-07-04-live-paper-daemon-hardening-implementation.md`
  - First implementation phase.
- Read: `docs/superpowers/plans/2026-07-04-runtime-strategy-promotion-implementation.md`
  - Makes all three strategies selectable before account execution.
- Read: `docs/superpowers/plans/2026-07-04-execution-account-readonly-sync-implementation.md`
  - First private Binance account phase.
- Read: `docs/superpowers/plans/2026-07-04-trading-lease-risk-gateway-implementation.md`
  - Gating and risk phase before execution.
- Read: `docs/superpowers/plans/2026-07-04-order-execution-state-machine-implementation.md`
  - First phase that can submit orders after explicit enablement.
- Read: `docs/superpowers/plans/2026-07-04-shadow-operation-implementation.md`
  - Production path without order submission.
- Read: `docs/superpowers/plans/2026-07-04-small-capital-live-rollout-implementation.md`
  - Small-capital live rollout gates and runbook.
- Read: `docs/superpowers/plans/2026-07-04-operator-dashboard-implementation.md`
  - Operator visibility and controlled actions.

---

### Task 1: Establish Phase Gate Checklist

**Files:**
- Modify: `README.md`
- Create: `docs/runbooks/live-trading-phase-gates.md`

- [ ] **Step 1: Add a failing documentation check**

Create `tests/unit/docs/test_live_phase_gates.py`:

```python
from pathlib import Path


def test_live_phase_gate_runbook_lists_ordered_phases() -> None:
    text = Path("docs/runbooks/live-trading-phase-gates.md").read_text()

    expected = (
        "1. Live Paper Daemon Hardening",
        "2. Runtime Strategy Promotion",
        "3. Execution Account Read-Only Sync",
        "4. Trading Lease And Risk Gateway",
        "5. Order Execution State Machine",
        "6. Shadow Operation",
        "7. Small-Capital Live Rollout",
        "8. Operator Dashboard",
    )
    for item in expected:
        assert item in text
```

- [ ] **Step 2: Run the doc check to verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/docs/test_live_phase_gates.py -v
```

Expected: FAIL because `docs/runbooks/live-trading-phase-gates.md` does not exist.

- [ ] **Step 3: Create the runbook**

Create `docs/runbooks/live-trading-phase-gates.md` with the ordered phase list, each phase's prerequisite, and the verification command required before the next phase begins.

- [ ] **Step 4: Link the runbook from README**

Add a short `Real Binance Live Trading Roadmap` section to `README.md` pointing to the runbook and stating that live order submission is disabled until phases 1-6 pass.

- [ ] **Step 5: Verify and commit**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/docs/test_live_phase_gates.py -v
.venv/bin/ruff check tests/unit/docs
.venv/bin/mypy src
```

Commit:

```bash
git add README.md docs/runbooks/live-trading-phase-gates.md tests/unit/docs/test_live_phase_gates.py
git commit -m "docs: add live trading phase gate runbook"
```

---

### Task 2: Execute Phase Plans In Order

**Files:**
- Read-only: `docs/superpowers/plans/2026-07-04-*-implementation.md`

- [ ] **Step 1: Implement live paper daemon hardening**

Follow `docs/superpowers/plans/2026-07-04-live-paper-daemon-hardening-implementation.md`.

- [ ] **Step 2: Implement runtime strategy promotion**

Follow `docs/superpowers/plans/2026-07-04-runtime-strategy-promotion-implementation.md`.

- [ ] **Step 3: Implement execution-account read-only sync**

Follow `docs/superpowers/plans/2026-07-04-execution-account-readonly-sync-implementation.md`.

- [ ] **Step 4: Implement trading lease and risk gateway**

Follow `docs/superpowers/plans/2026-07-04-trading-lease-risk-gateway-implementation.md`.

- [ ] **Step 5: Implement order execution state machine**

Follow `docs/superpowers/plans/2026-07-04-order-execution-state-machine-implementation.md`.

- [ ] **Step 6: Implement shadow operation**

Follow `docs/superpowers/plans/2026-07-04-shadow-operation-implementation.md`.

- [ ] **Step 7: Implement small-capital live rollout**

Follow `docs/superpowers/plans/2026-07-04-small-capital-live-rollout-implementation.md`.

- [ ] **Step 8: Implement operator dashboard**

Follow `docs/superpowers/plans/2026-07-04-operator-dashboard-implementation.md`.

---

### Task 3: Program-Level Final Verification

**Files:**
- No planned source edits.

- [ ] **Step 1: Run offline verification**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit -q
.venv/bin/ruff check .
.venv/bin/mypy src
```

Expected: all unit tests pass, ruff reports `All checks passed!`, and mypy reports no issues.

- [ ] **Step 2: Run PostgreSQL verification**

Run:

```bash
docker compose up -d postgres
CML_DATABASE_URL=postgresql+asyncpg://cml:cml@localhost:54329/cml .venv/bin/alembic upgrade head
CML_TEST_DATABASE_URL=postgresql+psycopg://cml:cml@localhost:54329/cml \
CML_TEST_ASYNC_DATABASE_URL=postgresql+asyncpg://cml:cml@localhost:54329/cml \
  PYTHONPATH=src .venv/bin/python -m pytest tests/integration -q
```

- [ ] **Step 3: Run fake-exchange end-to-end checks**

Run the e2e tests added by the account, risk, execution, and shadow plans:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/e2e -q
```

- [ ] **Step 4: Confirm live submission gate remains closed by default**

Run:

```bash
PYTHONPATH=src .venv/bin/cml-execution-account --help
PYTHONPATH=src .venv/bin/cml-strategy-runner --help
```

Expected: live submission options are explicit and default to disabled.

---

## Completion Criteria

- All downstream implementation plans exist and are referenced by the phase gate checklist.
- Phases execute in order from live paper hardening through small-capital live rollout.
- Each phase has automated verification commands and explicit manual gates where real credentials or real orders are involved.
- Program-level unit, integration, e2e, ruff, and mypy checks pass before small-capital live.
- Live submit remains disabled by default until the small-capital rollout approval record exists.
