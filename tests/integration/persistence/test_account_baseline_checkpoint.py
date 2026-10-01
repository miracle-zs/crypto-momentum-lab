from dataclasses import replace
from datetime import UTC, datetime, timedelta
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
from crypto_momentum_lab.execution_account.baseline_checkpoint import (
    AccountBaselineCheckpoint,
)
from crypto_momentum_lab.execution_account.binance.user_data_parser import (
    parse_user_data_event,
)
from crypto_momentum_lab.execution_account.snapshot_models import AccountSnapshot
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
    AccountUserDataJournalRow,
    ExecutionAccountProcessStateRow,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)


async def test_checkpoint_binds_complete_baseline_to_journal_prefix_and_rolls_back_atomically(
    async_database_url,
):
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    repository = PostgresAccountRepository(factory)
    environment = "test-cp-" + uuid4().hex[:12]
    account = "account-3"
    now = datetime(2026, 10, 1, tzinfo=UTC)
    config = AccountConfigSnapshot(environment, account, False, False, 0, now, {})
    balances = tuple(
        AccountBalanceSnapshot(
            environment, account, asset, amount, amount, Decimal("0"), now, {}
        )
        for asset, amount in (("USDT", Decimal("101")), ("BNB", Decimal("0")))
    )
    snapshot = AccountSnapshot(config, balances, (), ())
    sync_config = ExecutionAccountSyncConfig(environment, account, False, False, now)
    # Only the materialized persistence path is exercised, never exchange I/O.
    service = ExecutionAccountSyncService(
        client=None, repository=repository, config=sync_config
    )

    async def append(timestamp):
        event = parse_user_data_event(
            {"e": "ACCOUNT_CONFIG_UPDATE", "E": timestamp}, received_at=now
        )
        sequence = await repository.append_user_data_event(
            environment=environment,
            account_label=account,
            receiver_session_id="receiver-a",
            stream_token=1,
            event=event,
        )
        return sequence, event

    try:
        first, _ = await append(1790784000000)
        assert await service.user_data_journal_cursor() == first
        checkpoint = AccountBaselineCheckpoint(
            1, "baseline-" + environment, first, "receiver-a", 1, snapshot
        )
        result = ExecutionAccountSyncResult(
            ExecutionAccountStatus.READY_READONLY,
            checkpoint.baseline_id,
            0,
            snapshot=snapshot,
            baseline_checkpoint=checkpoint,
        )
        await service.persist_reconciliation_result(result)
        second, pending = await append(1790784001000)
        reopened = PostgresAccountRepository(factory)
        loaded = await reopened.load_baseline_checkpoint(
            environment=environment, account_label=account
        )
        assert loaded == checkpoint
        assert (
            len(loaded.snapshot.balances) == 2
        )  # Full baseline, including omitted zero history.
        tail = await reopened.load_user_data_events(
            environment=environment,
            account_label=account,
            after_sequence=loaded.journal_sequence,
        )
        assert [(row.sequence, row.event) for row in tail] == [(second, pending)]
        assert (
            await reopened.load_baseline_checkpoint(
                environment=environment, account_label="account-4"
            )
            is None
        )
        foreign_event = parse_user_data_event(
            {"e": "ACCOUNT_CONFIG_UPDATE", "E": 1790784002000}, received_at=now
        )
        foreign_sequence = await repository.append_user_data_event(
            environment=environment,
            account_label="account-4",
            receiver_session_id="receiver-a",
            stream_token=1,
            event=foreign_event,
        )
        wrong_cursor = replace(
            checkpoint,
            baseline_id="wrong-cursor-" + environment,
            journal_sequence=foreign_sequence,
        )
        with pytest.raises(ValueError, match="journal cursor"):
            await service.persist_reconciliation_result(
                replace(
                    result,
                    reconciliation_id=wrong_cursor.baseline_id,
                    baseline_checkpoint=wrong_cursor,
                )
            )
        bad_at = now + timedelta(seconds=2)
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
        bad_checkpoint = replace(
            checkpoint,
            baseline_id="failed-" + environment,
            journal_sequence=second,
            snapshot=bad_snapshot,
        )
        with pytest.raises(DBAPIError):
            await service.persist_reconciliation_result(
                replace(
                    result,
                    reconciliation_id=bad_checkpoint.baseline_id,
                    snapshot=bad_snapshot,
                    baseline_checkpoint=bad_checkpoint,
                )
            )
        assert (
            await reopened.load_baseline_checkpoint(
                environment=environment, account_label=account
            )
            == checkpoint
        )
        async with factory() as session:
            rows = (
                await session.scalars(
                    select(AccountBalanceSnapshotRow).where(
                        AccountBalanceSnapshotRow.environment == environment
                    )
                )
            ).all()
            assert len(rows) == 1
            assert rows[0].observed_at == now
    finally:
        async with factory() as session:
            async with session.begin():
                for model in (
                    AccountUserDataJournalRow,
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
