# Paper Run PostgreSQL Persistence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist paper-trading run reports to PostgreSQL with idempotent saves, deterministic reads, and an opt-in CLI persistence path.

**Architecture:** Add strategy-run persistence as a focused PostgreSQL repository beside the existing universe and capture repositories. The paper runner remains unchanged; persistence consumes the existing `PaperTradingRunReport` and writes run metadata, signals, candidates, paper fills, and the final checkpoint transactionally. CLI persistence is opt-in and runs after the JSON report is successfully written.

**Tech Stack:** Python 3.13, SQLAlchemy 2 async ORM, Alembic migrations, PostgreSQL JSONB/Numeric/timestamptz, Typer CLI, pytest, ruff, mypy.

---

## File Structure

- Modify: `src/crypto_momentum_lab/persistence/postgres/models.py`
  - Add ORM rows for `strategy_runs`, `strategy_signals`, `order_intent_candidates`, `paper_fills`, and `strategy_checkpoints`.
- Create: `alembic/versions/20260702_0003_strategy_run_persistence.py`
  - Add the corresponding database tables, constraints, and indexes.
- Create: `src/crypto_momentum_lab/persistence/postgres/strategy_run_repository.py`
  - Map `PaperTradingRunReport` into rows, validate relationships, perform idempotent transactional saves, and load summaries/artifacts.
- Modify: `src/crypto_momentum_lab/persistence/postgres/__init__.py`
  - Export `PostgresStrategyRunRepository`.
- Modify: `src/crypto_momentum_lab/apps/strategy_runner/main.py`
  - Add `--persist` and `--database-url` options for the `paper` command.
- Create: `tests/unit/persistence/postgres/test_strategy_run_repository.py`
  - Unit-test mapping, validation, idempotency comparison helpers, and load ordering helpers without requiring PostgreSQL.
- Modify: `tests/unit/apps/strategy_runner/test_strategy_runner_main.py`
  - Unit-test CLI persistence option behavior.
- Create: `tests/integration/persistence/test_strategy_run_repository.py`
  - Integration-test migrations and repository saves when local PostgreSQL is available.
- Modify: `tests/conftest.py`
  - Clear new strategy-run tables in test database fixtures so existing integration tests remain isolated.

---

### Task 1: Add ORM Models And Migration

**Files:**
- Modify: `src/crypto_momentum_lab/persistence/postgres/models.py`
- Create: `alembic/versions/20260702_0003_strategy_run_persistence.py`
- Test: `tests/integration/persistence/test_migrations.py`

- [ ] **Step 1: Write the failing migration test**

Modify `tests/integration/persistence/test_migrations.py` so
`test_initial_migration_creates_universe_tables` also asserts the strategy-run
tables exist:

```python
def test_initial_migration_creates_universe_tables(database_url: str) -> None:
    engine = create_sync_engine(database_url)
    try:
        table_names = set(inspect(engine).get_table_names())
    finally:
        engine.dispose()

    assert {
        "contract_metadata",
        "daily_open_prices",
        "universe_snapshots",
        "universe_entries",
        "monitoring_memberships",
        "raw_archive_manifests",
        "market_data_quality_events",
        "market_data_process_states",
        "strategy_runs",
        "strategy_signals",
        "order_intent_candidates",
        "paper_fills",
        "strategy_checkpoints",
    }.issubset(table_names)
```

- [ ] **Step 2: Run migration test to verify it fails**

Run:

```bash
CML_TEST_DATABASE_URL=postgresql+psycopg://cml:cml@localhost:54329/cml \
  .venv/bin/python -m pytest tests/integration/persistence/test_migrations.py -v
```

Expected: FAIL because the new table names are missing.

- [ ] **Step 3: Add ORM rows**

Append these classes to `src/crypto_momentum_lab/persistence/postgres/models.py`:

```python
class StrategyRunRow(Base):
    __tablename__ = "strategy_runs"

    run_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    strategy_name: Mapped[str] = mapped_column(String(64))
    strategy_version: Mapped[str] = mapped_column(String(32))
    config_hash: Mapped[str] = mapped_column(String(64))
    run_mode: Mapped[str] = mapped_column(String(16))
    code_commit: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    schema_version: Mapped[int] = mapped_column(Integer)
    source_paths: Mapped[list[str]] = mapped_column(JSONB)
    source_description: Mapped[str] = mapped_column(Text)
    execution_config: Mapped[dict[str, object]] = mapped_column(JSONB)
    input_state_count: Mapped[int] = mapped_column(Integer)
    processed_symbol_count: Mapped[int] = mapped_column(Integer)
    signal_count: Mapped[int] = mapped_column(Integer)
    candidate_count: Mapped[int] = mapped_column(Integer)
    fill_count: Mapped[int] = mapped_column(Integer)
    pending_candidate_count: Mapped[int] = mapped_column(Integer)
    rejection_summary: Mapped[dict[str, object]] = mapped_column(JSONB)
    summary_counts: Mapped[dict[str, object]] = mapped_column(JSONB)
    fill_summary: Mapped[dict[str, object]] = mapped_column(JSONB)


class StrategySignalRow(Base):
    __tablename__ = "strategy_signals"

    signal_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    run_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("strategy_runs.run_id", ondelete="CASCADE"),
    )
    strategy_name: Mapped[str] = mapped_column(String(64))
    strategy_version: Mapped[str] = mapped_column(String(32))
    config_hash: Mapped[str] = mapped_column(String(64))
    symbol: Mapped[str] = mapped_column(String(32))
    side: Mapped[str] = mapped_column(String(16))
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    source_state_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    reason: Mapped[str] = mapped_column(Text)
    features: Mapped[dict[str, object]] = mapped_column(JSONB)
    reference_prices: Mapped[dict[str, object]] = mapped_column(JSONB)

    __table_args__ = (
        Index("ix_strategy_signals_run_time_symbol", "run_id", "detected_at", "symbol"),
        Index("ix_strategy_signals_run_symbol", "run_id", "symbol"),
    )


class OrderIntentCandidateRow(Base):
    __tablename__ = "order_intent_candidates"

    candidate_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    signal_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("strategy_signals.signal_id", ondelete="CASCADE"),
    )
    run_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("strategy_runs.run_id", ondelete="CASCADE"),
    )
    strategy_name: Mapped[str] = mapped_column(String(64))
    strategy_version: Mapped[str] = mapped_column(String(32))
    config_hash: Mapped[str] = mapped_column(String(64))
    symbol: Mapped[str] = mapped_column(String(32))
    side: Mapped[str] = mapped_column(String(16))
    entry_type: Mapped[str] = mapped_column(String(16))
    limit_price: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    desired_notional: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    reduce_only: Mapped[bool] = mapped_column(Boolean)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    reason: Mapped[str] = mapped_column(Text)
    features: Mapped[dict[str, object]] = mapped_column(JSONB)

    __table_args__ = (
        Index("ix_order_intent_candidates_run_created_symbol", "run_id", "created_at", "symbol"),
        Index("ix_order_intent_candidates_run_symbol", "run_id", "symbol"),
    )


class PaperFillRow(Base):
    __tablename__ = "paper_fills"

    fill_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    candidate_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("order_intent_candidates.candidate_id", ondelete="CASCADE"),
    )
    signal_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("strategy_signals.signal_id", ondelete="CASCADE"),
    )
    run_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("strategy_runs.run_id", ondelete="CASCADE"),
    )
    symbol: Mapped[str] = mapped_column(String(32))
    side: Mapped[str] = mapped_column(String(16))
    status: Mapped[str] = mapped_column(String(16))
    target_fill_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    filled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    requested_notional: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    filled_notional: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    quantity: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    reference_midpoint: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    spread: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    fill_price: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    fee: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    total_cost: Mapped[Decimal] = mapped_column(Numeric(38, 18))
    cost_bps: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    reason: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        Index("ix_paper_fills_run_target_symbol", "run_id", "target_fill_at", "symbol"),
        Index("ix_paper_fills_run_status", "run_id", "status"),
    )


class StrategyCheckpointRow(Base):
    __tablename__ = "strategy_checkpoints"

    run_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("strategy_runs.run_id", ondelete="CASCADE"),
        primary_key=True,
    )
    last_processed_at_by_symbol: Mapped[dict[str, object]] = mapped_column(JSONB)
    warmup_buckets_by_symbol: Mapped[dict[str, int]] = mapped_column(JSONB)
    cooldown_buckets_remaining_by_symbol: Mapped[dict[str, int]] = mapped_column(JSONB)
    payload: Mapped[dict[str, object]] = mapped_column(JSONB)
    saved_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
```

- [ ] **Step 4: Add Alembic migration**

Create `alembic/versions/20260702_0003_strategy_run_persistence.py` with:

```python
from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "20260702_0003"
down_revision: str | None = "20260615_0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "strategy_runs",
        sa.Column("run_id", sa.String(128), nullable=False),
        sa.Column("strategy_name", sa.String(64), nullable=False),
        sa.Column("strategy_version", sa.String(32), nullable=False),
        sa.Column("config_hash", sa.String(64), nullable=False),
        sa.Column("run_mode", sa.String(16), nullable=False),
        sa.Column("code_commit", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("source_paths", postgresql.JSONB(), nullable=False),
        sa.Column("source_description", sa.Text(), nullable=False),
        sa.Column("execution_config", postgresql.JSONB(), nullable=False),
        sa.Column("input_state_count", sa.Integer(), nullable=False),
        sa.Column("processed_symbol_count", sa.Integer(), nullable=False),
        sa.Column("signal_count", sa.Integer(), nullable=False),
        sa.Column("candidate_count", sa.Integer(), nullable=False),
        sa.Column("fill_count", sa.Integer(), nullable=False),
        sa.Column("pending_candidate_count", sa.Integer(), nullable=False),
        sa.Column("rejection_summary", postgresql.JSONB(), nullable=False),
        sa.Column("summary_counts", postgresql.JSONB(), nullable=False),
        sa.Column("fill_summary", postgresql.JSONB(), nullable=False),
        sa.PrimaryKeyConstraint("run_id", name="pk_strategy_runs"),
    )
    op.create_table(
        "strategy_signals",
        sa.Column("signal_id", sa.String(128), nullable=False),
        sa.Column("run_id", sa.String(128), nullable=False),
        sa.Column("strategy_name", sa.String(64), nullable=False),
        sa.Column("strategy_version", sa.String(32), nullable=False),
        sa.Column("config_hash", sa.String(64), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("side", sa.String(16), nullable=False),
        sa.Column("detected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_state_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("features", postgresql.JSONB(), nullable=False),
        sa.Column("reference_prices", postgresql.JSONB(), nullable=False),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["strategy_runs.run_id"],
            name="fk_strategy_signals_run_id_strategy_runs",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("signal_id", name="pk_strategy_signals"),
    )
    op.create_index(
        "ix_strategy_signals_run_time_symbol",
        "strategy_signals",
        ["run_id", "detected_at", "symbol"],
    )
    op.create_index(
        "ix_strategy_signals_run_symbol",
        "strategy_signals",
        ["run_id", "symbol"],
    )
    op.create_table(
        "order_intent_candidates",
        sa.Column("candidate_id", sa.String(128), nullable=False),
        sa.Column("signal_id", sa.String(128), nullable=False),
        sa.Column("run_id", sa.String(128), nullable=False),
        sa.Column("strategy_name", sa.String(64), nullable=False),
        sa.Column("strategy_version", sa.String(32), nullable=False),
        sa.Column("config_hash", sa.String(64), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("side", sa.String(16), nullable=False),
        sa.Column("entry_type", sa.String(16), nullable=False),
        sa.Column("limit_price", sa.Numeric(38, 18), nullable=True),
        sa.Column("desired_notional", sa.Numeric(38, 18), nullable=True),
        sa.Column("reduce_only", sa.Boolean(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("features", postgresql.JSONB(), nullable=False),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["strategy_runs.run_id"],
            name="fk_order_intent_candidates_run_id_strategy_runs",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["signal_id"],
            ["strategy_signals.signal_id"],
            name="fk_order_intent_candidates_signal_id_strategy_signals",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("candidate_id", name="pk_order_intent_candidates"),
    )
    op.create_index(
        "ix_order_intent_candidates_run_created_symbol",
        "order_intent_candidates",
        ["run_id", "created_at", "symbol"],
    )
    op.create_index(
        "ix_order_intent_candidates_run_symbol",
        "order_intent_candidates",
        ["run_id", "symbol"],
    )
    op.create_table(
        "paper_fills",
        sa.Column("fill_id", sa.String(128), nullable=False),
        sa.Column("candidate_id", sa.String(128), nullable=False),
        sa.Column("signal_id", sa.String(128), nullable=False),
        sa.Column("run_id", sa.String(128), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("side", sa.String(16), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("target_fill_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("filled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("requested_notional", sa.Numeric(38, 18), nullable=True),
        sa.Column("filled_notional", sa.Numeric(38, 18), nullable=True),
        sa.Column("quantity", sa.Numeric(38, 18), nullable=True),
        sa.Column("reference_midpoint", sa.Numeric(38, 18), nullable=True),
        sa.Column("spread", sa.Numeric(38, 18), nullable=True),
        sa.Column("fill_price", sa.Numeric(38, 18), nullable=True),
        sa.Column("fee", sa.Numeric(38, 18), nullable=False),
        sa.Column("total_cost", sa.Numeric(38, 18), nullable=False),
        sa.Column("cost_bps", sa.Numeric(38, 18), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["candidate_id"],
            ["order_intent_candidates.candidate_id"],
            name="fk_paper_fills_candidate_id_order_intent_candidates",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["strategy_runs.run_id"],
            name="fk_paper_fills_run_id_strategy_runs",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["signal_id"],
            ["strategy_signals.signal_id"],
            name="fk_paper_fills_signal_id_strategy_signals",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("fill_id", name="pk_paper_fills"),
    )
    op.create_index(
        "ix_paper_fills_run_target_symbol",
        "paper_fills",
        ["run_id", "target_fill_at", "symbol"],
    )
    op.create_index(
        "ix_paper_fills_run_status",
        "paper_fills",
        ["run_id", "status"],
    )
    op.create_table(
        "strategy_checkpoints",
        sa.Column("run_id", sa.String(128), nullable=False),
        sa.Column("last_processed_at_by_symbol", postgresql.JSONB(), nullable=False),
        sa.Column("warmup_buckets_by_symbol", postgresql.JSONB(), nullable=False),
        sa.Column("cooldown_buckets_remaining_by_symbol", postgresql.JSONB(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("saved_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["strategy_runs.run_id"],
            name="fk_strategy_checkpoints_run_id_strategy_runs",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("run_id", name="pk_strategy_checkpoints"),
    )


def downgrade() -> None:
    op.drop_index("ix_paper_fills_run_status", table_name="paper_fills")
    op.drop_index("ix_paper_fills_run_target_symbol", table_name="paper_fills")
    op.drop_index("ix_order_intent_candidates_run_symbol", table_name="order_intent_candidates")
    op.drop_index("ix_order_intent_candidates_run_created_symbol", table_name="order_intent_candidates")
    op.drop_index("ix_strategy_signals_run_symbol", table_name="strategy_signals")
    op.drop_index("ix_strategy_signals_run_time_symbol", table_name="strategy_signals")
    op.drop_table("strategy_checkpoints")
    op.drop_table("paper_fills")
    op.drop_table("order_intent_candidates")
    op.drop_table("strategy_signals")
    op.drop_table("strategy_runs")
```

- [ ] **Step 5: Run targeted verification**

Run:

```bash
.venv/bin/ruff check alembic src/crypto_momentum_lab/persistence/postgres
.venv/bin/mypy src
```

If PostgreSQL is available, run the migration test from Step 2 again. Expected:
PASS.

- [ ] **Step 6: Commit**

```bash
git add alembic/versions/20260702_0003_strategy_run_persistence.py \
  src/crypto_momentum_lab/persistence/postgres/models.py \
  tests/integration/persistence/test_migrations.py
git commit -m "feat: add strategy run persistence tables"
```

---

### Task 2: Add Strategy Run Repository

**Files:**
- Create: `src/crypto_momentum_lab/persistence/postgres/strategy_run_repository.py`
- Modify: `src/crypto_momentum_lab/persistence/postgres/__init__.py`
- Modify: `tests/conftest.py`
- Create: `tests/unit/persistence/postgres/test_strategy_run_repository.py`
- Create: `tests/integration/persistence/test_strategy_run_repository.py`

- [ ] **Step 1: Write failing unit tests for mapping and validation**

Create `tests/unit/persistence/postgres/test_strategy_run_repository.py` with
tests that build a minimal `PaperTradingRunReport` and assert:

```python
def test_report_rows_convert_decimals_and_enums_to_json_values() -> None:
    report = fixture_paper_report()

    rows = strategy_run_report_rows(report)

    assert rows.run["run_id"] == "paper-test-run"
    assert rows.run["run_mode"] == "paper"
    assert rows.run["execution_config"]["taker_fee_rate"] == "0.0004"
    assert rows.signals[0]["side"] == "long"
    assert rows.fills[0]["status"] == "filled"


def test_report_validation_rejects_unknown_candidate_signal() -> None:
    report = replace(
        fixture_paper_report(),
        candidates=(replace(fixture_paper_report().candidates[0], signal_id="missing"),),
    )

    with pytest.raises(ValueError, match="candidate references unknown signal_id"):
        validate_paper_report(report)


def test_core_field_match_detects_conflict() -> None:
    assert core_fields_match({"run_id": "a", "config_hash": "x"}, {"run_id": "a", "config_hash": "x"})
    assert not core_fields_match({"run_id": "a", "config_hash": "x"}, {"run_id": "a", "config_hash": "y"})
```

The fixture should construct real `StrategyRunIdentity`, `StrategySignal`,
`OrderIntentCandidate`, `SimulatedFill`, `StrategyCheckpoint`, and
`PaperTradingRunReport` objects.

- [ ] **Step 2: Run unit tests to verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest \
  tests/unit/persistence/postgres/test_strategy_run_repository.py -v
```

Expected: FAIL because `strategy_run_repository` does not exist.

- [ ] **Step 3: Implement mapping helpers and repository skeleton**

Create `src/crypto_momentum_lab/persistence/postgres/strategy_run_repository.py`
with:

```python
from dataclasses import asdict, dataclass
from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.strategy_runner.paper import PaperTradingRunReport
from crypto_momentum_lab.persistence.postgres.models import (
    OrderIntentCandidateRow,
    PaperFillRow,
    StrategyCheckpointRow,
    StrategyRunRow,
    StrategySignalRow,
)


@dataclass(frozen=True, slots=True)
class StrategyRunReportRows:
    run: dict[str, object]
    signals: Sequence[dict[str, object]]
    candidates: Sequence[dict[str, object]]
    fills: Sequence[dict[str, object]]
    checkpoint: dict[str, object]


def validate_paper_report(report: PaperTradingRunReport) -> None:
    run_id = report.run.run_id
    signal_ids = {signal.signal_id for signal in report.signals}
    candidate_ids = {candidate.candidate_id for candidate in report.candidates}
    for signal in report.signals:
        if signal.run_id != run_id:
            raise ValueError("signal run_id mismatch")
    for candidate in report.candidates:
        if candidate.run_id != run_id:
            raise ValueError("candidate run_id mismatch")
        if candidate.signal_id not in signal_ids:
            raise ValueError("candidate references unknown signal_id")
    for fill in report.paper_fills:
        if fill.candidate_id not in candidate_ids:
            raise ValueError("fill references unknown candidate_id")
        if fill.signal_id not in signal_ids:
            raise ValueError("fill references unknown signal_id")


def strategy_run_report_rows(report: PaperTradingRunReport) -> StrategyRunReportRows:
    validate_paper_report(report)
    return StrategyRunReportRows(
        run={
            "run_id": report.run.run_id,
            "strategy_name": report.run.strategy_name,
            "strategy_version": report.run.strategy_version,
            "config_hash": report.run.config_hash,
            "run_mode": report.run.run_mode.value,
            "code_commit": report.run.code_commit,
            "created_at": report.run.created_at,
            "generated_at": report.generated_at,
            "schema_version": report.schema_version,
            "source_paths": list(report.run.source_paths),
            "source_description": report.source_description,
            "execution_config": _jsonable(asdict(report.execution_config)),
            "input_state_count": report.input_state_count,
            "processed_symbol_count": report.processed_symbol_count,
            "signal_count": len(report.signals),
            "candidate_count": len(report.candidates),
            "fill_count": len(report.paper_fills),
            "pending_candidate_count": report.pending_candidate_count,
            "rejection_summary": _jsonable(report.rejection_summary),
            "summary_counts": _jsonable(report.summary_counts),
            "fill_summary": _jsonable(report.fill_summary),
        },
        signals=tuple(
            {
                "signal_id": signal.signal_id,
                "run_id": signal.run_id,
                "strategy_name": signal.strategy_name,
                "strategy_version": signal.strategy_version,
                "config_hash": signal.config_hash,
                "symbol": signal.symbol,
                "side": signal.side.value,
                "detected_at": signal.detected_at,
                "source_state_at": signal.source_state_at,
                "reason": signal.reason,
                "features": _jsonable(signal.features),
                "reference_prices": _jsonable(signal.reference_prices),
            }
            for signal in report.signals
        ),
        candidates=tuple(
            {
                "candidate_id": candidate.candidate_id,
                "signal_id": candidate.signal_id,
                "run_id": candidate.run_id,
                "strategy_name": candidate.strategy_name,
                "strategy_version": candidate.strategy_version,
                "config_hash": candidate.config_hash,
                "symbol": candidate.symbol,
                "side": candidate.side.value,
                "entry_type": candidate.entry_type.value,
                "limit_price": candidate.limit_price,
                "desired_notional": candidate.desired_notional,
                "reduce_only": candidate.reduce_only,
                "expires_at": candidate.expires_at,
                "created_at": candidate.created_at,
                "reason": candidate.reason,
                "features": _jsonable(candidate.features),
            }
            for candidate in report.candidates
        ),
        fills=tuple(
            {
                "fill_id": fill.fill_id,
                "candidate_id": fill.candidate_id,
                "signal_id": fill.signal_id,
                "run_id": report.run.run_id,
                "symbol": fill.symbol,
                "side": fill.side.value,
                "status": fill.status.value,
                "target_fill_at": fill.target_fill_at,
                "filled_at": fill.filled_at,
                "requested_notional": fill.requested_notional,
                "filled_notional": fill.filled_notional,
                "quantity": fill.quantity,
                "reference_midpoint": fill.reference_midpoint,
                "spread": fill.spread,
                "fill_price": fill.fill_price,
                "fee": fill.fee,
                "total_cost": fill.total_cost,
                "cost_bps": fill.cost_bps,
                "reason": fill.reason,
            }
            for fill in report.paper_fills
        ),
        checkpoint={
            "run_id": report.run.run_id,
            "last_processed_at_by_symbol": _jsonable(
                report.final_checkpoint.last_processed_at_by_symbol
            ),
            "warmup_buckets_by_symbol": _jsonable(
                report.final_checkpoint.warmup_buckets_by_symbol
            ),
            "cooldown_buckets_remaining_by_symbol": _jsonable(
                report.final_checkpoint.cooldown_buckets_remaining_by_symbol
            ),
            "payload": _jsonable(report.final_checkpoint.payload),
            "saved_at": report.generated_at,
        },
    )


def core_fields_match(left: dict[str, object], right: dict[str, object]) -> bool:
    return left == right


class PostgresStrategyRunRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def save_paper_report(self, report: PaperTradingRunReport) -> None:
        rows = strategy_run_report_rows(report)
        async with self._session_factory() as session:
            async with session.begin():
                await _insert_idempotent(session, StrategyRunRow, rows.run, "strategy run conflict")
                await _insert_many_idempotent(session, StrategySignalRow, rows.signals, "signal conflict")
                await _insert_many_idempotent(session, OrderIntentCandidateRow, rows.candidates, "candidate conflict")
                await _insert_many_idempotent(session, PaperFillRow, rows.fills, "paper fill conflict")
                await _insert_idempotent(session, StrategyCheckpointRow, rows.checkpoint, "checkpoint conflict")
```

Fill in the row dictionaries completely. `_jsonable()` should convert
`Decimal`, `datetime`, and `StrEnum` values into JSON-safe values while leaving
database decimal/timestamp columns as their native types.

- [ ] **Step 4: Implement idempotent inserts**

In the same repository module, add:

```python
async def _insert_idempotent(
    session: AsyncSession,
    model: type[Any],
    values: dict[str, object],
    conflict_message: str,
) -> None:
    primary_key = tuple(column.name for column in model.__table__.primary_key.columns)
    existing = await session.scalar(
        select(model).where(
            *(getattr(model, key) == values[key] for key in primary_key)
        )
    )
    if existing is not None:
        existing_values = {
            key: getattr(existing, key)
            for key in values
        }
        if not core_fields_match(_normalize_for_compare(existing_values), _normalize_for_compare(values)):
            raise ValueError(conflict_message)
        return
    await session.execute(insert(model).values(values))
```

Add `_insert_many_idempotent()` as a loop over `_insert_idempotent()`.

- [ ] **Step 5: Export repository and clear tables in fixtures**

Modify `src/crypto_momentum_lab/persistence/postgres/__init__.py` to export
both `PostgresUniverseRepository` and `PostgresStrategyRunRepository`.

Modify `tests/conftest.py` database cleanup lists so `PaperFillRow`,
`OrderIntentCandidateRow`, `StrategySignalRow`, `StrategyCheckpointRow`, and
`StrategyRunRow` are deleted before existing tests run.

- [ ] **Step 6: Add integration tests**

Create `tests/integration/persistence/test_strategy_run_repository.py`:

```python
async def test_save_paper_report_is_idempotent(strategy_run_repository) -> None:
    report = fixture_paper_report()

    await strategy_run_repository.save_paper_report(report)
    await strategy_run_repository.save_paper_report(report)

    summary = await strategy_run_repository.load_run_summary(report.run.run_id)
    assert summary is not None
    assert summary["run_id"] == report.run.run_id
    assert summary["signal_count"] == 1
    assert summary["candidate_count"] == 1
    assert summary["fill_count"] == 1


async def test_save_paper_report_rejects_conflicting_run(strategy_run_repository) -> None:
    report = fixture_paper_report()
    await strategy_run_repository.save_paper_report(report)

    conflicting = replace(
        report,
        run=replace(report.run, config_hash="b" * 64),
    )

    with pytest.raises(ValueError, match="strategy run conflict"):
        await strategy_run_repository.save_paper_report(conflicting)
```

Add a `strategy_run_repository` fixture that uses the existing async session
factory and new repository.

- [ ] **Step 7: Run targeted tests**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest \
  tests/unit/persistence/postgres/test_strategy_run_repository.py -v
```

If PostgreSQL is available, run:

```bash
CML_TEST_ASYNC_DATABASE_URL=postgresql+asyncpg://cml:cml@localhost:54329/cml \
  PYTHONPATH=src .venv/bin/python -m pytest \
  tests/integration/persistence/test_strategy_run_repository.py -v
```

- [ ] **Step 8: Commit**

```bash
git add src/crypto_momentum_lab/persistence/postgres \
  tests/conftest.py \
  tests/unit/persistence/postgres/test_strategy_run_repository.py \
  tests/integration/persistence/test_strategy_run_repository.py
git commit -m "feat: persist paper trading reports"
```

---

### Task 3: Add Opt-In CLI Persistence

**Files:**
- Modify: `src/crypto_momentum_lab/apps/strategy_runner/main.py`
- Modify: `tests/unit/apps/strategy_runner/test_strategy_runner_main.py`
- Modify: `README.md`

- [ ] **Step 1: Write failing CLI tests**

Add tests to `tests/unit/apps/strategy_runner/test_strategy_runner_main.py`:

```python
def test_paper_command_rejects_persist_without_database_url(tmp_path: Path) -> None:
    states_root = write_states_dataset(tmp_path)
    result = runner.invoke(
        app,
        [
            "paper",
            "--strategy",
            "compression_breakout",
            "--states-root",
            str(states_root),
            "--output",
            str(tmp_path / "paper.json"),
            "--persist",
        ],
    )

    assert result.exit_code != 0
    assert "--persist requires --database-url or CML_DATABASE_URL" in result.output


def test_paper_command_does_not_persist_by_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    states_root = write_states_dataset(tmp_path)
    called = False

    async def fake_save(*args: object, **kwargs: object) -> None:
        nonlocal called
        called = True

    monkeypatch.setattr(strategy_runner_main, "persist_paper_report", fake_save)
    result = runner.invoke(
        app,
        [
            "paper",
            "--strategy",
            "compression_breakout",
            "--states-root",
            str(states_root),
            "--output",
            str(tmp_path / "paper.json"),
        ],
    )

    assert result.exit_code == 0
    assert called is False
```

Add a test proving
`--persist --database-url postgresql+asyncpg://cml:cml@localhost:54329/cml`
calls the helper and prints `persisted=true`.

- [ ] **Step 2: Run CLI tests to verify failure**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest \
  tests/unit/apps/strategy_runner/test_strategy_runner_main.py -v
```

Expected: FAIL because the options and helper do not exist.

- [ ] **Step 3: Implement CLI options and persistence helper**

In `src/crypto_momentum_lab/apps/strategy_runner/main.py`, add:

```python
import anyio
import os
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.persistence.postgres import (
    PostgresStrategyRunRepository,
    create_async_database_engine,
)
```

Add options to `paper()`:

```python
persist: bool = typer.Option(False, "--persist", help="Persist paper report to PostgreSQL."),
database_url: str | None = typer.Option(None, "--database-url", help="PostgreSQL async database URL."),
```

After `write_paper_trading_report(report, output)`, add:

```python
persisted = False
if persist:
    resolved_database_url = database_url or os.environ.get("CML_DATABASE_URL")
    if not resolved_database_url:
        raise typer.BadParameter(
            "--persist requires --database-url or CML_DATABASE_URL"
        )
    anyio.run(persist_paper_report, report, resolved_database_url)
    persisted = True
typer.echo(
    "Paper run completed: "
    f"states={report.input_state_count} "
    f"signals={len(report.signals)} "
    f"candidates={len(report.candidates)} "
    f"fills={len(report.paper_fills)} "
    f"persisted={str(persisted).lower()}"
)
```

Create:

```python
async def persist_paper_report(
    report: PaperTradingRunReport,
    database_url: str,
) -> None:
    engine = create_async_database_engine(database_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        repository = PostgresStrategyRunRepository(factory)
        await repository.save_paper_report(report)
    finally:
        await engine.dispose()
```

- [ ] **Step 4: Update README**

Extend the Paper Trading Runner section with:

```bash
.venv/bin/cml-strategy-runner paper \
  --strategy compression_breakout \
  --states-root data/derived/market_states_15s \
  --output reports/compression-breakout-paper.json \
  --persist \
  --database-url "$CML_DATABASE_URL"
```

State that persistence is opt-in and the default remains local JSON only.

- [ ] **Step 5: Run targeted verification**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest \
  tests/unit/apps/strategy_runner/test_strategy_runner_main.py \
  tests/unit/persistence/postgres/test_strategy_run_repository.py -v
.venv/bin/cml-strategy-runner paper --help
.venv/bin/ruff check src/crypto_momentum_lab/apps/strategy_runner \
  src/crypto_momentum_lab/persistence/postgres \
  tests/unit/apps/strategy_runner \
  tests/unit/persistence/postgres
.venv/bin/mypy src
```

- [ ] **Step 6: Commit**

```bash
git add README.md src/crypto_momentum_lab/apps/strategy_runner/main.py \
  tests/unit/apps/strategy_runner/test_strategy_runner_main.py
git commit -m "feat: add paper report persistence cli"
```

---

### Task 4: Final Verification And Cleanup

**Files:**
- No planned file edits unless verification exposes a defect.

- [ ] **Step 1: Run offline verification**

Run:

```bash
PYTHONPATH=src .venv/bin/python -m pytest tests/unit -q
.venv/bin/ruff check .
.venv/bin/mypy src
```

Expected:

- all unit tests pass;
- ruff reports `All checks passed!`;
- mypy reports no issues.

- [ ] **Step 2: Run PostgreSQL verification when available**

If Docker is running, run:

```bash
docker compose up -d postgres
.venv/bin/alembic upgrade head
CML_TEST_DATABASE_URL=postgresql+psycopg://cml:cml@localhost:54329/cml \
CML_TEST_ASYNC_DATABASE_URL=postgresql+asyncpg://cml:cml@localhost:54329/cml \
  PYTHONPATH=src .venv/bin/python -m pytest \
  tests/integration/persistence/test_migrations.py \
  tests/integration/persistence/test_strategy_run_repository.py -v
```

Expected: targeted integration tests pass. If Docker is not running, record the
environment limitation and keep the offline verification result.

- [ ] **Step 3: Run final status check**

Run:

```bash
git status --short --branch
git log --oneline --decorate -8
```

Expected: clean branch with implementation commits after the plan commit.

- [ ] **Step 4: Report outcome**

Summarize:

- implemented tables/repository/CLI behavior;
- verification commands and results;
- whether PostgreSQL integration verification ran or was blocked by Docker;
- remaining excluded scope: no Binance private API, no real orders, no risk engine.
