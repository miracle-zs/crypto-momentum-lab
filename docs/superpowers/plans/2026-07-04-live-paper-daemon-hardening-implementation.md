# Live Paper Daemon Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn bounded live-source paper runs into a resumable supervised paper daemon with checkpoints and stale-data halts.

**Architecture:** Add a strategy runtime event table and repository, then build a daemon loop around the existing `run_paper_trading` components without changing offline paper behavior. The daemon polls `runtime_market_states_15s`, processes states incrementally, persists checkpoints, and halts when market data or process state is stale.

**Tech Stack:** Python 3.13, SQLAlchemy 2 async ORM, Alembic, PostgreSQL JSONB/timestamptz, Typer, pytest, ruff, mypy.

---

## File Structure

- Modify: `src/crypto_momentum_lab/persistence/postgres/models.py`
  - Add `StrategyRuntimeEventRow`.
- Create: `alembic/versions/20260704_0005_strategy_runtime_events.py`
  - Create `strategy_runtime_events`.
- Create: `src/crypto_momentum_lab/persistence/postgres/paper_daemon_repository.py`
  - Save runtime events and upsert latest checkpoint.
- Create: `src/crypto_momentum_lab/strategy_runner/daemon.py`
  - Add daemon config, loop, stale-data checks, and checkpoint cadence.
- Modify: `src/crypto_momentum_lab/apps/strategy_runner/main.py`
  - Add `paper-live-daemon` command.
- Create: `tests/unit/strategy_runner/test_daemon.py`
  - Unit-test checkpoint cadence, resume, and stale-data halt.
- Create: `tests/unit/persistence/postgres/test_paper_daemon_repository.py`
  - Unit-test row mapping.
- Create: `tests/integration/persistence/test_paper_daemon_repository.py`
  - Integration-test checkpoint/event persistence.

---

### Task 1: Runtime Event Persistence

**Files:**
- Modify: `src/crypto_momentum_lab/persistence/postgres/models.py`
- Create: `alembic/versions/20260704_0005_strategy_runtime_events.py`
- Create: `src/crypto_momentum_lab/persistence/postgres/paper_daemon_repository.py`
- Create: `tests/unit/persistence/postgres/test_paper_daemon_repository.py`
- Create: `tests/integration/persistence/test_paper_daemon_repository.py`

- [ ] **Step 1: Write failing row mapping tests**

Create `tests/unit/persistence/postgres/test_paper_daemon_repository.py`:

```python
from datetime import UTC, datetime
from uuid import UUID

from crypto_momentum_lab.persistence.postgres.paper_daemon_repository import (
    runtime_event_row,
)


def test_runtime_event_row_preserves_values() -> None:
    row = runtime_event_row(
        event_id=UUID(int=1),
        run_id="paper-live-daemon-1",
        event_type="checkpoint_saved",
        occurred_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        symbol="BTCUSDT",
        bucket_start=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        details={"state_count": 10},
    )

    assert row["event_id"] == UUID(int=1)
    assert row["run_id"] == "paper-live-daemon-1"
    assert row["event_type"] == "checkpoint_saved"
    assert row["details"] == {"state_count": 10}
```

- [ ] **Step 2: Verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/persistence/postgres/test_paper_daemon_repository.py -v
```

Expected: FAIL with `ModuleNotFoundError` for `paper_daemon_repository`.

- [ ] **Step 3: Add model and migration**

Add `StrategyRuntimeEventRow` with columns: `event_id`, `run_id`, `event_type`, `occurred_at`, `symbol`, `bucket_start`, `details`. Index `(run_id, occurred_at)` and `(event_type, occurred_at)`.

- [ ] **Step 4: Implement repository**

Create `PostgresPaperDaemonRepository` with methods that:

- save one `StrategyRuntimeEvent` and ignore duplicate `event_id` values;
- upsert the latest `StrategyCheckpoint` for a `run_id` with `saved_at`;
- load the latest checkpoint for a `run_id` and return `None` when no checkpoint exists.

Reuse existing `StrategyCheckpointRow` for the latest checkpoint.

- [ ] **Step 5: Add integration tests**

Test that saving a checkpoint twice is idempotent and saving events preserves order by `occurred_at`.

- [ ] **Step 6: Verify and commit**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest \
  tests/unit/persistence/postgres/test_paper_daemon_repository.py \
  tests/integration/persistence/test_paper_daemon_repository.py -v
.venv/bin/ruff check src/crypto_momentum_lab/persistence/postgres tests/unit/persistence/postgres tests/integration/persistence
.venv/bin/mypy src
```

Commit:

```bash
git add alembic/versions/20260704_0005_strategy_runtime_events.py \
  src/crypto_momentum_lab/persistence/postgres \
  tests/unit/persistence/postgres/test_paper_daemon_repository.py \
  tests/integration/persistence/test_paper_daemon_repository.py
git commit -m "feat: persist paper daemon runtime events"
```

---

### Task 2: Daemon Loop And Checkpoint Cadence

**Files:**
- Create: `src/crypto_momentum_lab/strategy_runner/daemon.py`
- Create: `tests/unit/strategy_runner/test_daemon.py`

- [ ] **Step 1: Write failing daemon tests**

Create `tests/unit/strategy_runner/test_daemon.py` with tests named:

- `test_daemon_saves_checkpoint_after_state_count_threshold`: feeds enough closed states to cross `checkpoint_every_states` and asserts one checkpoint save.
- `test_daemon_resumes_from_checkpoint_cursor`: seeds a saved checkpoint and asserts the source starts after the saved cursor.
- `test_daemon_halts_on_stale_market_state`: feeds an old `bucket_end` and asserts the daemon returns a stale-data halt reason.

Use fake source, fake strategy, fake repository, and injected clock.

- [ ] **Step 2: Verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/strategy_runner/test_daemon.py -v
```

Expected: FAIL because `strategy_runner.daemon` does not exist.

- [ ] **Step 3: Implement daemon config and result types**

Create:

```python
@dataclass(frozen=True, slots=True)
class PaperLiveDaemonConfig:
    run_id: str
    strategy_name: str
    environment: str
    checkpoint_every_states: int
    checkpoint_every_seconds: float
    max_market_state_age_seconds: float
    continue_while_halted: bool = False
```

Create `PaperLiveDaemonResult` with processed count, halt reason, final cursor, and final checkpoint time.

- [ ] **Step 4: Implement loop**

Implement `run_paper_live_daemon(source, strategy, repository, config, clock)` so it:

1. loads existing checkpoint;
2. restores strategy state;
3. iterates closed states;
4. rejects stale states by `bucket_end`;
5. persists checkpoints by count or elapsed seconds;
6. writes final checkpoint on graceful shutdown.

- [ ] **Step 5: Verify and commit**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/strategy_runner/test_daemon.py -v
.venv/bin/ruff check src/crypto_momentum_lab/strategy_runner tests/unit/strategy_runner
.venv/bin/mypy src
```

Commit:

```bash
git add src/crypto_momentum_lab/strategy_runner/daemon.py tests/unit/strategy_runner/test_daemon.py
git commit -m "feat: add live paper daemon loop"
```

---

### Task 3: CLI Command

**Files:**
- Modify: `src/crypto_momentum_lab/apps/strategy_runner/main.py`
- Modify: `tests/unit/apps/strategy_runner/test_strategy_runner_main.py`

- [ ] **Step 1: Write failing CLI tests**

Add tests named:

- `test_paper_live_daemon_requires_database_url`: invokes the command without `--database-url` and asserts a non-zero exit plus a clear error.
- `test_paper_live_daemon_builds_daemon_config`: invokes the command with all cadence options and asserts the constructed config reaches the daemon runner.

- [ ] **Step 2: Verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/apps/strategy_runner/test_strategy_runner_main.py -v
```

Expected: FAIL because `paper-live-daemon` command does not exist.

- [ ] **Step 3: Implement command**

Add `paper-live-daemon` with options: `--strategy`, `--database-url`, `--environment`, `--run-id`, `--start-at`, `--checkpoint-every-states`, `--checkpoint-every-seconds`, `--max-market-state-age-seconds`, and `--continue-while-halted`.

- [ ] **Step 4: Verify and commit**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit/apps/strategy_runner/test_strategy_runner_main.py tests/unit/strategy_runner/test_daemon.py -v
PYTHONPATH=src .venv/bin/cml-strategy-runner paper-live-daemon --help
.venv/bin/ruff check src/crypto_momentum_lab/apps/strategy_runner src/crypto_momentum_lab/strategy_runner tests/unit/apps/strategy_runner tests/unit/strategy_runner
.venv/bin/mypy src
```

Commit:

```bash
git add src/crypto_momentum_lab/apps/strategy_runner/main.py tests/unit/apps/strategy_runner/test_strategy_runner_main.py
git commit -m "feat: add paper live daemon cli"
```

---

## Completion Criteria

- Paper live daemon persists runtime events and resumable checkpoints.
- Daemon resumes from the latest checkpoint cursor without replaying already processed states.
- Stale market states halt the daemon with a durable runtime event.
- `paper-live-daemon` CLI requires a database URL and exposes checkpoint and freshness controls.
- Offline paper behavior remains unchanged.
- Unit, integration, ruff, and mypy verification commands pass.
