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
    AccountReconciliationRun,
)
from crypto_momentum_lab.persistence.postgres.account_repository import (
    PostgresAccountRepository,
)
from crypto_momentum_lab.persistence.postgres.models import (
    AccountBalanceSnapshotRow,
    AccountConfigSnapshotRow,
    AccountOpenOrderRow,
    AccountPositionSnapshotRow,
    AccountReconciliationRunRow,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)


async def test_reconciliation_snapshot_rolls_back_as_one_transaction(
    async_database_url: str,
) -> None:
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
    initial_run = AccountReconciliationRun(
        "baseline-" + environment,
        environment,
        account,
        "ready",
        now,
        balance_count=1,
        position_count=0,
        open_order_count=1,
        fill_count=0,
        mismatch_count=0,
        details={},
    )
    initial_order = AccountOpenOrderSnapshot(
        environment,
        account,
        "BTCUSDT",
        "100",
        "client-initial",
        "BUY",
        "LIMIT",
        "NEW",
        Decimal("1"),
        Decimal("2"),
        Decimal("0"),
        False,
        now,
        {},
    )
    try:
        await repository.save_reconciliation_snapshot(
            config=config,
            balances=balances,
            positions=(),
            open_orders=(initial_order,),
            fills=(),
            run=initial_run,
        )

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
        with pytest.raises(DBAPIError):
            await repository.save_reconciliation_snapshot(
                config=replace(config, observed_at=bad_at),
                balances=tuple(replace(row, observed_at=bad_at) for row in balances),
                positions=(),
                open_orders=(bad_order,),
                fills=(),
                run=replace(
                    initial_run,
                    reconciliation_id="failed-" + environment,
                    observed_at=bad_at,
                    open_order_count=1,
                ),
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
        assert config_rows[0].observed_at == now
        assert len(run_rows) == 1
        assert run_rows[0].reconciliation_id == initial_run.reconciliation_id
        assert len(open_orders) == 1
        assert open_orders[0].order_id == initial_order.order_id
        assert open_orders[0].observed_at == now
    finally:
        async with factory() as session:
            async with session.begin():
                for model in (
                    AccountReconciliationRunRow,
                    AccountBalanceSnapshotRow,
                    AccountPositionSnapshotRow,
                    AccountConfigSnapshotRow,
                    AccountOpenOrderRow,
                ):
                    await session.execute(
                        delete(model).where(model.environment == environment)
                    )
        await engine.dispose()
