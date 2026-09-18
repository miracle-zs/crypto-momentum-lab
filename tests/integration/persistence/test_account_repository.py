from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.account import (
    AccountFillReconciliationCursor,
    AccountPositionSnapshot,
    AccountReconciliationRun,
)
from crypto_momentum_lab.persistence.postgres.account_repository import (
    PostgresAccountRepository,
)
from crypto_momentum_lab.persistence.postgres.models import (
    AccountFillReconciliationCursorRow,
    AccountPositionSnapshotRow,
    AccountReconciliationHeadRow,
    AccountReconciliationRunRow,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)

ENVIRONMENT = "test-live-label-discovery"
NOW = datetime(2026, 9, 10, 0, 0, tzinfo=UTC)


@pytest.fixture
async def account_repository(
    async_database_url: str,
) -> AsyncIterator[PostgresAccountRepository]:
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        async with session.begin():
            await session.execute(
                delete(AccountFillReconciliationCursorRow).where(
                    AccountFillReconciliationCursorRow.environment == ENVIRONMENT
                )
            )
            await session.execute(
                delete(AccountPositionSnapshotRow).where(
                    AccountPositionSnapshotRow.environment == ENVIRONMENT
                )
            )
            await session.execute(
                delete(AccountReconciliationHeadRow).where(
                    AccountReconciliationHeadRow.environment == ENVIRONMENT
                )
            )
            await session.execute(
                delete(AccountReconciliationRunRow).where(
                    AccountReconciliationRunRow.environment == ENVIRONMENT
                )
            )
    yield PostgresAccountRepository(factory)
    async with factory() as session:
        async with session.begin():
            await session.execute(
                delete(AccountFillReconciliationCursorRow).where(
                    AccountFillReconciliationCursorRow.environment == ENVIRONMENT
                )
            )
            await session.execute(
                delete(AccountPositionSnapshotRow).where(
                    AccountPositionSnapshotRow.environment == ENVIRONMENT
                )
            )
            await session.execute(
                delete(AccountReconciliationHeadRow).where(
                    AccountReconciliationHeadRow.environment == ENVIRONMENT
                )
            )
            await session.execute(
                delete(AccountReconciliationRunRow).where(
                    AccountReconciliationRunRow.environment == ENVIRONMENT
                )
            )
    await engine.dispose()


def _run(
    account_label: str,
    observed_at: datetime,
    *,
    position_count: int,
    status: str = "ready",
) -> AccountReconciliationRun:
    return AccountReconciliationRun(
        reconciliation_id=(
            f"{account_label}:{observed_at.isoformat()}:{status}"
        ),
        environment=ENVIRONMENT,
        account_label=account_label,
        status=status,
        observed_at=observed_at,
        balance_count=0,
        position_count=position_count,
        open_order_count=0,
        fill_count=0,
        mismatch_count=0,
        details={},
    )


def _position(
    account_label: str,
    observed_at: datetime,
    *,
    symbol: str = "BTCUSDT",
) -> AccountPositionSnapshot:
    return AccountPositionSnapshot(
        environment=ENVIRONMENT,
        account_label=account_label,
        symbol=symbol,
        position_side="BOTH",
        position_amt=Decimal("1"),
        entry_price=Decimal("100"),
        mark_price=Decimal("101"),
        unrealized_pnl=Decimal("1"),
        notional=Decimal("101"),
        leverage=5,
        margin_type="CROSSED",
        observed_at=observed_at,
        raw_payload={},
    )


async def test_load_active_position_account_labels_uses_latest_ready_run(
    account_repository: PostgresAccountRepository,
) -> None:
    stopped_at = NOW
    await account_repository.save_reconciliation_run(
        _run("stopped", stopped_at, position_count=1)
    )
    await account_repository.save_position_snapshot(
        _position("stopped", stopped_at)
    )
    await account_repository.save_reconciliation_run(
        _run(
            "stopped",
            stopped_at + timedelta(minutes=1),
            position_count=0,
        )
    )

    open_at = NOW + timedelta(minutes=2)
    await account_repository.save_reconciliation_run(
        _run("open", open_at, position_count=1)
    )
    await account_repository.save_position_snapshot(_position("open", open_at))

    await account_repository.save_reconciliation_run(
        _run(
            "failed",
            NOW + timedelta(minutes=3),
            position_count=1,
            status="failed",
        )
    )

    assert await account_repository.load_active_position_account_labels(
        environment=ENVIRONMENT
    ) == frozenset({"open"})


async def test_load_active_position_symbols_uses_ready_run_timestamp_fence(
    account_repository: PostgresAccountRepository,
) -> None:
    ready_at = NOW
    await account_repository.save_position_snapshot(
        _position("primary", ready_at)
    )
    await account_repository.save_reconciliation_run(
        _run("primary", ready_at, position_count=1)
    )
    # This observation is newer than the ready run and may belong to a
    # reconciliation that has not committed its run row yet.  It must not be
    # mixed into the result of the older ready run.
    await account_repository.save_position_snapshot(
        _position(
            "primary",
            ready_at + timedelta(minutes=1),
            symbol="ETHUSDT",
        )
    )

    assert await account_repository.load_active_position_symbols(
        environment=ENVIRONMENT,
        account_label="primary",
    ) == frozenset({"BTCUSDT"})


async def test_fill_cursor_upsert_is_monotonic_and_switches_modes(
    account_repository: PostgresAccountRepository,
) -> None:
    await account_repository.save_fill_reconciliation_cursors(
        (
            AccountFillReconciliationCursor(
                environment=ENVIRONMENT,
                account_label="primary",
                symbol="BTCUSDT",
                from_id=10,
                start_time_ms=None,
                last_checked_at=NOW + timedelta(minutes=1),
            ),
        )
    )
    await account_repository.save_fill_reconciliation_cursors(
        (
            AccountFillReconciliationCursor(
                environment=ENVIRONMENT,
                account_label="primary",
                symbol="BTCUSDT",
                from_id=None,
                start_time_ms=100,
                last_checked_at=NOW + timedelta(minutes=2),
            ),
        )
    )
    await account_repository.save_fill_reconciliation_cursors(
        (
            AccountFillReconciliationCursor(
                environment=ENVIRONMENT,
                account_label="primary",
                symbol="BTCUSDT",
                from_id=5,
                start_time_ms=None,
                last_checked_at=NOW + timedelta(minutes=1),
            ),
        )
    )

    loaded = await account_repository.load_fill_reconciliation_cursors(
        environment=ENVIRONMENT,
        account_label="primary",
    )

    assert loaded["BTCUSDT"].from_id is None
    assert loaded["BTCUSDT"].start_time_ms == 100
    assert loaded["BTCUSDT"].last_checked_at == NOW + timedelta(minutes=2)


async def test_reconciliation_head_upsert_is_monotonic_and_ignores_non_ready(
    account_repository: PostgresAccountRepository,
) -> None:
    t1 = NOW
    t2 = NOW + timedelta(minutes=5)
    t3 = NOW + timedelta(minutes=10)
    t4 = NOW + timedelta(minutes=15)

    # 1. Persist ready run at t2 with position_count=0 (flattened)
    run_t2 = _run("acc_test", t2, position_count=0)
    await account_repository.save_reconciliation_run(run_t2)

    heads = await account_repository.load_reconciliation_heads(
        environment=ENVIRONMENT
    )
    assert "acc_test" in heads
    assert heads["acc_test"].observed_at == t2
    assert heads["acc_test"].position_count == 0
    assert (
        await account_repository.load_active_position_account_labels(
            environment=ENVIRONMENT
        )
        == frozenset()
    )

    # 2. Out-of-order stale run at t1 with position_count=2 arrives late:
    # Must NOT overwrite newer t2 state.
    run_t1 = _run("acc_test", t1, position_count=2)
    await account_repository.save_reconciliation_run(run_t1)

    heads = await account_repository.load_reconciliation_heads(
        environment=ENVIRONMENT
    )
    assert heads["acc_test"].observed_at == t2
    assert heads["acc_test"].position_count == 0
    assert (
        await account_repository.load_active_position_account_labels(
            environment=ENVIRONMENT
        )
        == frozenset()
    )

    # 3. Newer non-ready run at t3:
    # Must NOT overwrite valid ready head.
    run_t3 = _run("acc_test", t3, position_count=5, status="failed")
    await account_repository.save_reconciliation_run(run_t3)

    heads = await account_repository.load_reconciliation_heads(
        environment=ENVIRONMENT
    )
    assert heads["acc_test"].observed_at == t2
    assert heads["acc_test"].position_count == 0

    # 4. Strictly newer ready run at t4 with position_count=3:
    # Must advance the head projection.
    run_t4 = _run("acc_test", t4, position_count=3)
    await account_repository.save_reconciliation_run(run_t4)

    heads = await account_repository.load_reconciliation_heads(
        environment=ENVIRONMENT
    )
    assert heads["acc_test"].observed_at == t4
    assert heads["acc_test"].position_count == 3
    assert await account_repository.load_active_position_account_labels(
        environment=ENVIRONMENT
    ) == frozenset({"acc_test"})


async def test_reconciliation_heads_survive_service_stop_and_empty_positions(
    account_repository: PostgresAccountRepository,
) -> None:
    # Account "stopped_with_pos" ceased publishing, last run has position_count=1
    await account_repository.save_reconciliation_run(
        _run("stopped_with_pos", NOW, position_count=1)
    )
    # Account "flat_account" has position_count=0 (legitimate empty state)
    await account_repository.save_reconciliation_run(
        _run("flat_account", NOW, position_count=0)
    )

    heads = await account_repository.load_reconciliation_heads(
        environment=ENVIRONMENT
    )
    assert len(heads) == 2
    assert heads["stopped_with_pos"].position_count == 1
    assert heads["flat_account"].position_count == 0

    active_labels = (
        await account_repository.load_active_position_account_labels(
            environment=ENVIRONMENT
        )
    )
    # The stopped account MUST still be discovered for market risk protection
    assert active_labels == frozenset({"stopped_with_pos"})
