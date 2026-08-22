from datetime import UTC, datetime

from sqlalchemy import delete, select

from crypto_momentum_lab.persistence.postgres.models import ContractMetadataRow
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
