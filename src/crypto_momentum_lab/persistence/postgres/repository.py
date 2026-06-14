from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.universe.models import (
    ContractMetadata,
    DailyOpen,
    MarketCandidate,
    MembershipStatus,
    RankEntry,
    RankingResult,
    RankingSide,
    TrackedMembership,
    UniverseSnapshot,
)
from crypto_momentum_lab.persistence.postgres.models import (
    ContractMetadataRow,
    DailyOpenRow,
    MonitoringMembershipRow,
    UniverseEntryRow,
    UniverseSnapshotRow,
)


def _return_by_symbol(snapshot: UniverseSnapshot) -> dict[str, Decimal]:
    return {
        entry.symbol: entry.utc_day_return
        for entry in (*snapshot.ranking.gainers, *snapshot.ranking.losers)
    }


def _rank_by_symbol(entries: tuple[RankEntry, ...]) -> dict[str, int]:
    return {entry.symbol: entry.rank for entry in entries}


def _candidate_return(candidate: MarketCandidate) -> Decimal | None:
    if (
        candidate.open_price is None
        or candidate.current_price is None
        or candidate.open_price <= 0
        or candidate.current_price <= 0
    ):
        return None
    return candidate.current_price / candidate.open_price - Decimal(1)


class PostgresUniverseRepository:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._session_factory = session_factory

    async def save_contract_metadata(
        self,
        contracts: tuple[ContractMetadata, ...],
        *,
        effective_at: datetime,
    ) -> None:
        if not contracts:
            return
        statement = insert(ContractMetadataRow).values(
            [
                {
                    "symbol": item.symbol,
                    "effective_at": effective_at,
                    "contract_type": item.contract_type,
                    "status": item.status,
                    "quote_asset": item.quote_asset,
                    "margin_asset": item.margin_asset,
                    "onboard_at": item.onboard_at,
                    "raw_payload": item.raw,
                }
                for item in contracts
            ]
        )
        statement = statement.on_conflict_do_update(
            index_elements=["symbol", "effective_at"],
            set_={
                "contract_type": statement.excluded.contract_type,
                "status": statement.excluded.status,
                "quote_asset": statement.excluded.quote_asset,
                "margin_asset": statement.excluded.margin_asset,
                "onboard_at": statement.excluded.onboard_at,
                "raw_payload": statement.excluded.raw_payload,
            },
        )
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(statement)

    async def load_daily_opens(
        self,
        utc_day: date,
        symbols: frozenset[str],
    ) -> dict[str, Decimal]:
        if not symbols:
            return {}
        async with self._session_factory() as session:
            rows = (
                await session.execute(
                    select(DailyOpenRow).where(
                        DailyOpenRow.utc_day == utc_day,
                        DailyOpenRow.symbol.in_(symbols),
                    )
                )
            ).scalars()
            return {row.symbol: row.open_price for row in rows}

    async def save_daily_opens(
        self,
        opens: tuple[DailyOpen, ...],
        *,
        captured_at: datetime,
    ) -> None:
        if not opens:
            return
        statement = insert(DailyOpenRow).values(
            [
                {
                    "utc_day": item.utc_day,
                    "symbol": item.symbol,
                    "open_price": item.open_price,
                    "open_time": item.open_time,
                    "captured_at": captured_at,
                }
                for item in opens
            ]
        )
        statement = statement.on_conflict_do_update(
            index_elements=["utc_day", "symbol"],
            set_={
                "open_price": statement.excluded.open_price,
                "open_time": statement.excluded.open_time,
                "captured_at": statement.excluded.captured_at,
            },
        )
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(statement)

    async def load_active_memberships(
        self,
    ) -> dict[str, TrackedMembership]:
        async with self._session_factory() as session:
            snapshot_id = await session.scalar(
                select(UniverseSnapshotRow.snapshot_id)
                .where(UniverseSnapshotRow.activated.is_(True))
                .order_by(UniverseSnapshotRow.observed_at.desc())
                .limit(1)
            )
            if snapshot_id is None:
                return {}
            rows = (
                await session.execute(
                    select(MonitoringMembershipRow)
                    .where(MonitoringMembershipRow.snapshot_id == snapshot_id)
                    .order_by(MonitoringMembershipRow.symbol)
                )
            ).scalars()
            return {
                row.symbol: TrackedMembership(
                    symbol=row.symbol,
                    status=MembershipStatus(row.status),
                    side=None if row.side is None else RankingSide(row.side),
                    left_target_at=row.left_target_at,
                )
                for row in rows
            }

    async def save_snapshot(self, snapshot: UniverseSnapshot) -> None:
        returns = _return_by_symbol(snapshot)
        gainer_ranks = _rank_by_symbol(snapshot.ranking.gainers)
        loser_ranks = _rank_by_symbol(snapshot.ranking.losers)

        async with self._session_factory() as session:
            async with session.begin():
                statement = insert(UniverseSnapshotRow).values(
                    snapshot_id=snapshot.snapshot_id,
                    observed_at=snapshot.observed_at,
                    utc_day=snapshot.utc_day,
                    config_hash=snapshot.config_hash,
                    activated=snapshot.activated,
                )
                await session.execute(
                    statement.on_conflict_do_update(
                        index_elements=["snapshot_id"],
                        set_={
                            "observed_at": statement.excluded.observed_at,
                            "utc_day": statement.excluded.utc_day,
                            "config_hash": statement.excluded.config_hash,
                            "activated": statement.excluded.activated,
                        },
                    )
                )
                await session.execute(
                    delete(MonitoringMembershipRow).where(
                        MonitoringMembershipRow.snapshot_id
                        == snapshot.snapshot_id
                    )
                )
                await session.execute(
                    delete(UniverseEntryRow).where(
                        UniverseEntryRow.snapshot_id == snapshot.snapshot_id
                    )
                )

                entries = [
                    {
                        "snapshot_id": snapshot.snapshot_id,
                        "symbol": candidate.symbol,
                        "open_price": candidate.open_price,
                        "current_price": candidate.current_price,
                        "price_time": candidate.price_time,
                        "utc_day_return": returns.get(
                            candidate.symbol,
                            _candidate_return(candidate),
                        ),
                        "gainer_rank": gainer_ranks.get(candidate.symbol),
                        "loser_rank": loser_ranks.get(candidate.symbol),
                        "is_target": (
                            candidate.symbol in snapshot.ranking.target_symbols
                        ),
                        "exclusion_reason": snapshot.ranking.exclusions.get(
                            candidate.symbol
                        ),
                    }
                    for candidate in snapshot.ranking.candidates
                ]
                if entries:
                    await session.execute(insert(UniverseEntryRow).values(entries))
                memberships = [
                    {
                        "snapshot_id": snapshot.snapshot_id,
                        "symbol": item.symbol,
                        "status": item.status.value,
                        "side": None if item.side is None else item.side.value,
                        "left_target_at": item.left_target_at,
                    }
                    for item in snapshot.memberships
                ]
                if memberships:
                    await session.execute(
                        insert(MonitoringMembershipRow).values(memberships)
                    )

    async def load_snapshot(
        self,
        observed_at: datetime,
    ) -> UniverseSnapshot | None:
        async with self._session_factory() as session:
            snapshot_row = await session.scalar(
                select(UniverseSnapshotRow).where(
                    UniverseSnapshotRow.observed_at == observed_at
                )
            )
            if snapshot_row is None:
                return None
            entries = tuple(
                (
                    await session.execute(
                        select(UniverseEntryRow)
                        .where(
                            UniverseEntryRow.snapshot_id
                            == snapshot_row.snapshot_id
                        )
                        .order_by(UniverseEntryRow.symbol)
                    )
                ).scalars()
            )
            membership_rows = tuple(
                (
                    await session.execute(
                        select(MonitoringMembershipRow)
                        .where(
                            MonitoringMembershipRow.snapshot_id
                            == snapshot_row.snapshot_id
                        )
                        .order_by(MonitoringMembershipRow.symbol)
                    )
                ).scalars()
            )

        candidates = tuple(
            MarketCandidate(
                row.symbol,
                row.open_price,
                row.current_price,
                row.price_time,
            )
            for row in entries
        )
        gainers = tuple(
            RankEntry(
                row.symbol,
                row.utc_day_return,
                row.gainer_rank,
                RankingSide.GAINER,
            )
            for row in sorted(
                (item for item in entries if item.gainer_rank is not None),
                key=lambda item: item.gainer_rank or 0,
            )
            if row.utc_day_return is not None and row.gainer_rank is not None
        )
        losers = tuple(
            RankEntry(
                row.symbol,
                row.utc_day_return,
                row.loser_rank,
                RankingSide.LOSER,
            )
            for row in sorted(
                (item for item in entries if item.loser_rank is not None),
                key=lambda item: item.loser_rank or 0,
            )
            if row.utc_day_return is not None and row.loser_rank is not None
        )
        ranking = RankingResult(
            candidates=candidates,
            gainers=gainers,
            losers=losers,
            target_symbols=frozenset(
                row.symbol for row in entries if row.is_target
            ),
            exclusions={
                row.symbol: row.exclusion_reason
                for row in entries
                if row.exclusion_reason is not None
            },
        )
        return UniverseSnapshot(
            snapshot_id=snapshot_row.snapshot_id,
            observed_at=snapshot_row.observed_at,
            utc_day=snapshot_row.utc_day,
            config_hash=snapshot_row.config_hash,
            activated=snapshot_row.activated,
            ranking=ranking,
            memberships=tuple(
                TrackedMembership(
                    row.symbol,
                    MembershipStatus(row.status),
                    None if row.side is None else RankingSide(row.side),
                    row.left_target_at,
                )
                for row in membership_rows
            ),
        )
