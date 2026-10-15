from datetime import UTC, datetime
from decimal import Decimal

import pytest
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
    AccountReconciliationRunRow,
)


@pytest.mark.parametrize("position_count", [0, 1])
@pytest.mark.parametrize("has_flat_marker", [False, True])
async def test_retention_watermark_uses_only_current_position_episode(
    position_count: int,
    has_flat_marker: bool,
) -> None:
    from types import SimpleNamespace

    head_at = datetime(2026, 10, 7, tzinfo=UTC)
    flat_at = datetime(2026, 10, 5, tzinfo=UTC)
    opened_at = datetime(2026, 10, 6, tzinfo=UTC)
    run = AccountReconciliationRunRow(
        environment="live",
        account_label="primary",
        status="ready",
        reconciliation_id="current",
        observed_at=head_at,
        position_count=position_count,
        details={
            "position_state_schema_version": 1,
            "position_keys": (
                [{"symbol": "BTCUSDT", "position_side": "LONG"}]
                if position_count
                else []
            ),
        },
    )
    statements = []
    answers = iter((flat_at if has_flat_marker else None, opened_at))

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            pass

        async def scalars(self, statement):
            return SimpleNamespace(all=lambda: [run])

        async def scalar(self, statement):
            statements.append(statement)
            return next(answers)

    repository = PostgresAccountRepository(Session)
    actual = await repository.load_active_position_retention_watermark()
    assert actual == (opened_at if position_count else None)
    assert len(statements) == (2 if position_count else 0)
    if position_count:
        sql = str(statements[1])
        assert ("observed_at >" in sql) == has_flat_marker
        assert "position_side =" in sql
        assert "account_label =" in sql
        assert "LIMIT" in sql
        if has_flat_marker:
            assert flat_at in statements[1].compile().params.values()


async def test_position_retention_missing_open_snapshot_fails_closed() -> None:
    from types import SimpleNamespace

    run = AccountReconciliationRunRow(
        environment="live",
        account_label="primary",
        status="ready",
        reconciliation_id="current",
        observed_at=datetime(2026, 10, 7, tzinfo=UTC),
        position_count=1,
        details={
            "position_state_schema_version": 1,
            "position_keys": [{"symbol": "BTCUSDT", "position_side": "LONG"}],
        },
    )

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            pass

        async def scalars(self, _statement):
            return SimpleNamespace(all=lambda: [run])

        async def scalar(self, _statement):
            return None

    with pytest.raises(ValueError, match="snapshot"):
        await PostgresAccountRepository(
            Session
        ).load_active_position_retention_watermark()


async def test_position_retention_selects_latest_ready_runs() -> None:
    from types import SimpleNamespace

    statements = []

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            pass

        async def scalars(self, statement):
            statements.append(statement)
            return SimpleNamespace(all=lambda: [])

    actual = await PostgresAccountRepository(
        Session
    ).load_active_position_retention_watermark()
    assert actual is None
    sql = str(
        statements[0].compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    assert "account_reconciliation_runs.status = 'ready'" in sql


async def test_position_retention_missing_configured_account_fails_closed() -> None:
    from types import SimpleNamespace

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            pass

        async def scalars(self, _statement):
            return SimpleNamespace(all=lambda: [])

    with pytest.raises(ValueError, match="Missing ready position reconciliation runs"):
        repository = PostgresAccountRepository(Session)
        await repository.load_active_position_retention_watermark(
            expected_account_labels=frozenset({"primary"})
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

    assert state.position_count == 0
    assert state.position_keys == ()
    assert state.symbols == frozenset()


def test_position_state_run_details_reject_count_mismatch() -> None:
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

    with pytest.raises(ValueError, match="count must match its keys"):
        _position_state_from_run(run)


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
    assert state.position_count == 2
    assert state.symbols == frozenset({"BTCUSDT", "ETHUSDT"})


async def test_unversioned_position_state_is_rejected() -> None:
    run = AccountReconciliationRunRow(
        reconciliation_id="legacy-run",
        environment="live",
        account_label="primary",
        status="ready",
        observed_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        position_count=1,
        details={},
    )

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def scalar(self, _statement):
            return run

    repository = PostgresAccountRepository(Session)

    with pytest.raises(KeyError, match="position_state_schema_version"):
        await repository.load_active_position_state(
            environment="live",
            account_label="primary",
        )


async def test_active_position_labels_use_one_latest_run_query() -> None:
    flat_run = AccountReconciliationRunRow(
        reconciliation_id="flat-run",
        environment="live",
        account_label="flat",
        status="ready",
        observed_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        position_count=0,
        details={},
    )
    open_run = AccountReconciliationRunRow(
        reconciliation_id="open-run",
        environment="live",
        account_label="open",
        status="catching_up",
        observed_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        position_count=1,
        details={},
    )

    class Scalars:
        def all(self):
            return [flat_run, open_run]

    class Session:
        calls = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def scalars(self, _statement):
            self.calls += 1
            return Scalars()

    session = Session()
    repository = PostgresAccountRepository(lambda: session)

    labels = await repository.load_active_position_account_labels(
        environment="live",
        account_labels=("flat", "open"),
    )

    assert labels == frozenset({"open"})
    assert session.calls == 1
