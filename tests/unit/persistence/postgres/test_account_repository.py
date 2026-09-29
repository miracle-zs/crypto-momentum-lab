from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

from sqlalchemy.dialects import postgresql

from crypto_momentum_lab.domain.account import (
    AccountBalanceSnapshot,
    ExecutionAccountProcessState,
    ExecutionAccountStatus,
)
from crypto_momentum_lab.persistence.postgres.account_repository import (
    PostgresAccountRepository,
    _position_state_from_run,
    balance_snapshot_row,
    process_state_row,
)
from crypto_momentum_lab.persistence.postgres.models import (
    AccountReconciliationHeadRow,
    AccountReconciliationRunRow,
)


def test_balance_snapshot_row_preserves_numeric_values() -> None:
    snapshot = AccountBalanceSnapshot(
        environment="live",
        account_label="primary",
        asset="USDT",
        wallet_balance=Decimal("100.5"),
        available_balance=Decimal("80.25"),
        unrealized_pnl=Decimal("1.5"),
        observed_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        raw_payload={"asset": "USDT"},
    )

    row = balance_snapshot_row(snapshot)

    assert row["environment"] == "live"
    assert row["account_label"] == "primary"
    assert row["asset"] == "USDT"
    assert row["wallet_balance"] == Decimal("100.5")
    assert row["available_balance"] == Decimal("80.25")
    assert row["unrealized_pnl"] == Decimal("1.5")


def test_process_state_row_uses_state_value() -> None:
    state = ExecutionAccountProcessState(
        environment="live",
        account_label="primary",
        state=ExecutionAccountStatus.READY_READONLY,
        occurred_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        reason=None,
    )

    row = process_state_row(state)

    assert row["state"] == "ready_readonly"
    assert row["reason"] is None


def test_account_reconciliation_head_validates_fields() -> None:
    from crypto_momentum_lab.domain.account import AccountReconciliationHead

    head = AccountReconciliationHead(
        environment="live",
        account_label="primary",
        reconciliation_id="test:1",
        status="ready",
        observed_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        balance_count=1,
        position_count=2,
        open_order_count=0,
        fill_count=5,
        mismatch_count=0,
        details={"ok": True},
    )
    assert head.environment == "live"
    assert head.position_count == 2
    assert head.projection_schema_version == 1


def test_position_state_run_details_preserve_complete_empty_snapshot() -> None:
    run = AccountReconciliationRunRow(
        reconciliation_id="run-flat",
        environment="live",
        account_label="primary",
        status="ready",
        observed_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        position_count=0,
        details={
            "position_state_schema_version": 1,
            "position_keys": [],
        },
    )

    state = _position_state_from_run(run)

    assert state is not None
    assert state.complete is True
    assert state.position_count == 0
    assert state.position_keys == ()
    assert state.symbols == frozenset()


def test_position_state_run_details_mark_count_mismatch_incomplete() -> None:
    run = AccountReconciliationRunRow(
        reconciliation_id="run-incomplete",
        environment="live",
        account_label="primary",
        status="ready",
        observed_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        position_count=2,
        details={
            "position_state_schema_version": 1,
            "position_keys": [
                {"symbol": "BTCUSDT", "position_side": "LONG"},
            ],
        },
    )

    state = _position_state_from_run(run)

    assert state is not None
    assert state.complete is False
    assert state.position_count == 2
    assert state.position_keys == (("BTCUSDT", "LONG"),)


async def test_account_repository_reads_versioned_position_state() -> None:
    run = AccountReconciliationRunRow(
        reconciliation_id="run-open",
        environment="live",
        account_label="primary",
        status="catching_up",
        observed_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        position_count=2,
        details={
            "position_state_schema_version": 1,
            "position_keys": [
                {"symbol": "BTCUSDT", "position_side": "LONG"},
                {"symbol": "ETHUSDT", "position_side": "BOTH"},
            ],
        },
    )

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def scalar(self, _statement):
            return run

    repository = PostgresAccountRepository(lambda: Session())

    state = await repository.load_active_position_state(
        environment="live",
        account_label="primary",
    )

    assert state is not None
    assert state.complete is True
    assert state.position_count == 2
    assert state.symbols == frozenset({"BTCUSDT", "ETHUSDT"})


async def test_legacy_position_reconstruction_is_per_key_and_fenced_to_run() -> None:
    run = AccountReconciliationRunRow(
        reconciliation_id="legacy-run",
        environment="live",
        account_label="primary",
        status="ready",
        observed_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        position_count=1,
        details={},
    )
    latest_rows = [
        SimpleNamespace(
            symbol="BTCUSDT",
            position_side="BOTH",
            position_amt=Decimal("0"),
        ),
        SimpleNamespace(
            symbol="ETHUSDT",
            position_side="BOTH",
            position_amt=Decimal("1"),
        ),
    ]

    class Result:
        def all(self):
            return latest_rows

    class Session:
        statement = None

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def scalar(self, _statement):
            return run

        async def execute(self, statement):
            self.statement = statement
            return Result()

    session = Session()
    repository = PostgresAccountRepository(lambda: session)

    state = await repository.load_active_position_state(
        environment="live",
        account_label="primary",
    )

    assert state is not None
    assert state.complete is False
    assert state.position_keys == (("ETHUSDT", "BOTH"),)
    sql = str(session.statement.compile(dialect=postgresql.dialect()))
    assert "DISTINCT ON" in sql
    assert "observed_at <= " in sql


async def test_active_position_label_discovery_fills_partial_head_projection() -> None:
    head = AccountReconciliationHeadRow(
        environment="live",
        account_label="head-present",
        reconciliation_id="head-run",
        status="ready",
        observed_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        balance_count=0,
        position_count=0,
        open_order_count=0,
        fill_count=0,
        mismatch_count=0,
        details={},
        projection_schema_version=1,
        projected_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
    )
    missing_run = AccountReconciliationRunRow(
        reconciliation_id="missing-run",
        environment="live",
        account_label="head-missing",
        status="ready",
        observed_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        position_count=1,
        details={},
    )

    class Scalars:
        def __init__(self, rows):
            self._rows = rows

        def all(self):
            return self._rows

    class Session:
        calls = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def scalars(self, _statement):
            self.calls += 1
            return Scalars([head] if self.calls == 1 else [missing_run])

    repository = PostgresAccountRepository(lambda: Session())

    labels = await repository.load_active_position_account_labels(
        environment="live",
        account_labels=("head-present", "head-missing"),
    )

    assert labels == frozenset({"head-missing"})
