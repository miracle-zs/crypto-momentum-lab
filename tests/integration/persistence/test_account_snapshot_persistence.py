from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import delete, select
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.account.models import (
    AccountBalanceSnapshot,
    AccountConfigSnapshot,
    AccountOpenOrderSnapshot,
    ExecutionAccountStatus,
)
from crypto_momentum_lab.domain.account.snapshot_models import AccountSnapshot
from crypto_momentum_lab.execution_account.sync import ExecutionAccountSyncService
from crypto_momentum_lab.execution_account.sync_models import (
    ExecutionAccountSyncConfig,
    ExecutionAccountSyncResult,
)
from crypto_momentum_lab.persistence.postgres.account_repository import (
    PostgresAccountRepository,
)
from crypto_momentum_lab.persistence.postgres.models import (
    AccountBalanceSnapshotRow,
    AccountConfigSnapshotRow,
    AccountOpenOrderRow,
    AccountPositionSnapshotRow,
    AccountReconciliationHeadRow,
    AccountReconciliationRunRow,
    ExecutionAccountProcessStateRow,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)


async def test_reconciliation_snapshot_rolls_back_as_one_transaction(
    async_database_url,
):
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    repository = PostgresAccountRepository(factory)
    environment = "test-acct-" + uuid4().hex[:12]
    account = "account-3"
    now = datetime(2026, 10, 1, tzinfo=UTC)
    config = AccountConfigSnapshot(environment, account, False, False, 0, now, {})
    balances = (
        AccountBalanceSnapshot(
            environment,
            account,
            "USDT",
            Decimal("101"),
            Decimal("101"),
            Decimal("0"),
            now,
            {},
        ),
    )
    snapshot = AccountSnapshot(config, balances, (), ())
    service = ExecutionAccountSyncService(
        client=None,
        repository=repository,
        config=ExecutionAccountSyncConfig(environment, account, False, False, now),
    )
    initial = ExecutionAccountSyncResult(
        status=ExecutionAccountStatus.READY_READONLY,
        reconciliation_id="baseline-" + environment,
        mismatch_count=0,
        snapshot=snapshot,
    )
    try:
        await service.persist_reconciliation_result(initial)

        bad_at = datetime(2026, 10, 1, 0, 0, 1, tzinfo=UTC)
        bad_order = AccountOpenOrderSnapshot(
            environment,
            account,
            "X" * 70,
            "1",
            "client-1",
            "BUY",
            "LIMIT",
            "NEW",
            Decimal("1"),
            Decimal("1"),
            Decimal("0"),
            False,
            bad_at,
            {},
        )
        bad_snapshot = replace(
            snapshot,
            config=replace(config, observed_at=bad_at),
            balances=tuple(replace(row, observed_at=bad_at) for row in balances),
            open_orders=(bad_order,),
        )
        with pytest.raises(DBAPIError):
            await service.persist_reconciliation_result(
                ExecutionAccountSyncResult(
                    status=ExecutionAccountStatus.READY_READONLY,
                    reconciliation_id="failed-" + environment,
                    mismatch_count=0,
                    snapshot=bad_snapshot,
                )
            )

        async with factory() as session:
            balance_rows = (
                await session.scalars(
                    select(AccountBalanceSnapshotRow).where(
                        AccountBalanceSnapshotRow.environment == environment
                    )
                )
            ).all()
            config_rows = (
                await session.scalars(
                    select(AccountConfigSnapshotRow).where(
                        AccountConfigSnapshotRow.environment == environment
                    )
                )
            ).all()
            run_rows = (
                await session.scalars(
                    select(AccountReconciliationRunRow).where(
                        AccountReconciliationRunRow.environment == environment
                    )
                )
            ).all()
            open_orders = (
                await session.scalars(
                    select(AccountOpenOrderRow).where(
                        AccountOpenOrderRow.environment == environment
                    )
                )
            ).all()
        assert len(balance_rows) == 1
        assert balance_rows[0].observed_at == now
        assert len(config_rows) == 1
        assert len(run_rows) == 1
        assert open_orders == []
    finally:
        async with factory() as session:
            async with session.begin():
                for model in (
                    AccountReconciliationHeadRow,
                    AccountReconciliationRunRow,
                    AccountBalanceSnapshotRow,
                    AccountPositionSnapshotRow,
                    AccountConfigSnapshotRow,
                    AccountOpenOrderRow,
                    ExecutionAccountProcessStateRow,
                ):
                    await session.execute(
                        delete(model).where(model.environment == environment)
                    )
        await engine.dispose()
