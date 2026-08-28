from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import delete, select

from crypto_momentum_lab.domain.universe.models import (
    ContractMetadata,
    MarketCandidate,
    MembershipStatus,
    RankEntry,
    RankingResult,
    RankingSide,
    TrackedMembership,
    UniverseSnapshot,
)
from crypto_momentum_lab.persistence.postgres.models import ContractMetadataRow
from crypto_momentum_lab.persistence.postgres.repository import (
    PostgresUniverseRepository,
)


async def test_save_snapshot_is_idempotent(
    repository: PostgresUniverseRepository,
) -> None:
    observed_at = datetime(2026, 6, 14, 11, 1, tzinfo=UTC)
    snapshot = UniverseSnapshot(
        snapshot_id=uuid4(),
        observed_at=observed_at,
        utc_day=observed_at.date(),
        config_hash="a" * 64,
        activated=True,
        ranking=RankingResult(
            candidates=(
                MarketCandidate(
                    "BTCUSDT",
                    Decimal("100"),
                    Decimal("110"),
                    observed_at,
                ),
            ),
            gainers=(
                RankEntry(
                    "BTCUSDT",
                    Decimal("0.1"),
                    1,
                    RankingSide.GAINER,
                ),
            ),
            losers=(
                RankEntry(
                    "BTCUSDT",
                    Decimal("0.1"),
                    1,
                    RankingSide.LOSER,
                ),
            ),
            target_symbols=frozenset({"BTCUSDT"}),
            exclusions={},
        ),
        memberships=(
            TrackedMembership(
                "BTCUSDT",
                MembershipStatus.TARGET,
                RankingSide.GAINER,
                None,
            ),
        ),
    )

    await repository.save_snapshot(snapshot)
    await repository.save_snapshot(snapshot)
    loaded = await repository.load_snapshot(observed_at)

    assert loaded is not None
    assert loaded.snapshot_id == snapshot.snapshot_id
    assert loaded.memberships == snapshot.memberships


async def test_save_contract_metadata_rehydrates_after_retention_delete(
    repository: PostgresUniverseRepository,
) -> None:
    first_at = datetime(2026, 6, 14, 11, 1, tzinfo=UTC)
    second_at = datetime(2026, 6, 14, 11, 6, tzinfo=UTC)
    contract = ContractMetadata(
        "BTCUSDT",
        "PERPETUAL",
        "TRADING",
        "USDT",
        "USDT",
        first_at,
        {"filters": []},
    )

    await repository.save_contract_metadata((contract,), effective_at=first_at)

    async with repository._session_factory() as session:
        async with session.begin():
            await session.execute(delete(ContractMetadataRow))

    await repository.save_contract_metadata((contract,), effective_at=second_at)

    async with repository._session_factory() as session:
        rows = (
            await session.scalars(
                select(ContractMetadataRow).where(
                    ContractMetadataRow.symbol == "BTCUSDT"
                )
            )
        ).all()

    assert [(row.symbol, row.effective_at) for row in rows] == [
        ("BTCUSDT", second_at)
    ]


async def test_load_active_memberships_ignores_unactivated_snapshot(
    repository: PostgresUniverseRepository,
    snapshot_factory,
) -> None:
    active = snapshot_factory(
        day=14,
        hour=23,
        activated=True,
        symbol="BTCUSDT",
    )
    midnight = snapshot_factory(
        day=15,
        hour=0,
        activated=False,
        symbol="ETHUSDT",
    )

    await repository.save_snapshot(active)
    await repository.save_snapshot(midnight)

    memberships = await repository.load_active_memberships()

    assert set(memberships) == {"BTCUSDT"}


async def test_load_active_memberships_at_uses_latest_activated_snapshot(
    repository: PostgresUniverseRepository,
    snapshot_factory,
) -> None:
    first = snapshot_factory(
        day=14,
        hour=22,
        activated=True,
        symbol="BTCUSDT",
    )
    second = snapshot_factory(
        day=14,
        hour=23,
        activated=True,
        symbol="ETHUSDT",
    )
    await repository.save_snapshot(first)
    await repository.save_snapshot(second)

    memberships = await repository.load_active_memberships_at(first.observed_at)

    assert set(memberships) == {"BTCUSDT"}
