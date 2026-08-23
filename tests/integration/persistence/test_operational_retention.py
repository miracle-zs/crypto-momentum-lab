from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import delete, select

from crypto_momentum_lab.persistence.postgres.models import (
    AccountBalanceSnapshotRow,
    ContractMetadataRow,
)
from crypto_momentum_lab.persistence.postgres.operational_retention import (
    PostgresOperationalRetentionRepository,
)


async def test_contract_metadata_retention_keeps_latest_snapshot(repository) -> None:
    factory = repository._session_factory
    older_at = datetime(2026, 6, 14, 10, 0, tzinfo=UTC)
    latest_at = datetime(2026, 6, 14, 11, 0, tzinfo=UTC)

    async with factory() as session:
        async with session.begin():
            await session.execute(delete(ContractMetadataRow))
            session.add_all(
                [
                    ContractMetadataRow(
                        symbol="BTCUSDT",
                        effective_at=older_at,
                        contract_type="PERPETUAL",
                        status="TRADING",
                        quote_asset="USDT",
                        margin_asset="USDT",
                        onboard_at=older_at,
                        raw_payload={"version": 1},
                    ),
                    ContractMetadataRow(
                        symbol="BTCUSDT",
                        effective_at=latest_at,
                        contract_type="PERPETUAL",
                        status="TRADING",
                        quote_asset="USDT",
                        margin_asset="USDT",
                        onboard_at=older_at,
                        raw_payload={"version": 2},
                    ),
                ]
            )

    retention = PostgresOperationalRetentionRepository(factory)
    deleted = await retention.prune_contract_metadata(
        before=datetime(2026, 6, 14, 12, 0, tzinfo=UTC),
        batch_size=10,
    )

    async with factory() as session:
        rows = (
            await session.scalars(
                select(ContractMetadataRow).where(
                    ContractMetadataRow.symbol == "BTCUSDT"
                )
            )
        ).all()

    assert deleted == 1
    assert [(row.effective_at, row.raw_payload) for row in rows] == [
        (latest_at, {"version": 2})
    ]


async def test_account_snapshot_retention_keeps_latest_row(repository) -> None:
    factory = repository._session_factory
    environment = f"retention-{uuid4().hex}"
    account_label = "primary"
    older_at = datetime(2026, 6, 14, 10, 0, tzinfo=UTC)
    latest_at = datetime(2026, 6, 14, 11, 0, tzinfo=UTC)

    async with factory() as session:
        async with session.begin():
            session.add_all(
                [
                    AccountBalanceSnapshotRow(
                        snapshot_id=uuid4(),
                        environment=environment,
                        account_label=account_label,
                        asset="USDT",
                        wallet_balance=Decimal("10"),
                        available_balance=Decimal("9"),
                        unrealized_pnl=Decimal("0"),
                        observed_at=older_at,
                        raw_payload={"version": 1},
                    ),
                    AccountBalanceSnapshotRow(
                        snapshot_id=uuid4(),
                        environment=environment,
                        account_label=account_label,
                        asset="USDT",
                        wallet_balance=Decimal("11"),
                        available_balance=Decimal("10"),
                        unrealized_pnl=Decimal("0"),
                        observed_at=latest_at,
                        raw_payload={"version": 2},
                    ),
                ]
            )

    retention = PostgresOperationalRetentionRepository(factory)
    deleted = await retention.prune_account_snapshots(
        environment=environment,
        account_label=account_label,
        before=datetime(2026, 6, 14, 12, 0, tzinfo=UTC),
        batch_size=10,
    )

    async with factory() as session:
        rows = (
            await session.scalars(
                select(AccountBalanceSnapshotRow).where(
                    AccountBalanceSnapshotRow.environment == environment,
                    AccountBalanceSnapshotRow.account_label == account_label,
                )
            )
        ).all()

    assert deleted["account_balance_snapshots"] == 1
    assert [(row.observed_at, row.raw_payload) for row in rows] == [
        (latest_at, {"version": 2})
    ]


async def test_account_snapshot_retention_thins_old_balance_history_hourly(
    repository,
) -> None:
    factory = repository._session_factory
    environment = f"retention-{uuid4().hex}"
    account_label = "primary"
    first_at = datetime(2026, 6, 14, 10, 5, tzinfo=UTC)
    latest_in_hour_at = datetime(2026, 6, 14, 10, 55, tzinfo=UTC)
    next_hour_at = datetime(2026, 6, 14, 11, 5, tzinfo=UTC)

    async with factory() as session:
        async with session.begin():
            for observed_at, version in (
                (first_at, 1),
                (latest_in_hour_at, 2),
                (next_hour_at, 3),
            ):
                session.add(
                    AccountBalanceSnapshotRow(
                        snapshot_id=uuid4(),
                        environment=environment,
                        account_label=account_label,
                        asset="USDT",
                        wallet_balance=Decimal(str(10 + version)),
                        available_balance=Decimal(str(9 + version)),
                        unrealized_pnl=Decimal("0"),
                        observed_at=observed_at,
                        raw_payload={"version": version},
                    )
                )

    retention = PostgresOperationalRetentionRepository(factory)
    deleted = await retention.prune_account_snapshots(
        environment=environment,
        account_label=account_label,
        before=datetime(2026, 6, 15, tzinfo=UTC),
        equity_before=datetime(2025, 6, 15, tzinfo=UTC),
        batch_size=10,
    )

    async with factory() as session:
        rows = (
            await session.scalars(
                select(AccountBalanceSnapshotRow)
                .where(
                    AccountBalanceSnapshotRow.environment == environment,
                    AccountBalanceSnapshotRow.account_label == account_label,
                )
                .order_by(AccountBalanceSnapshotRow.observed_at)
            )
        ).all()

    assert deleted["account_balance_snapshots"] == 1
    assert [(row.observed_at, row.raw_payload) for row in rows] == [
        (latest_in_hour_at, {"version": 2}),
        (next_hour_at, {"version": 3}),
    ]
