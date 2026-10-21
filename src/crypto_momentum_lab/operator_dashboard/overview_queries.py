"""Shared dashboard queries for health, research collection, and universe."""

import asyncio
from collections.abc import Callable, Sequence
from datetime import datetime
from pathlib import Path

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.operator_dashboard.collector_status import (
    read_research_collector_status,
)
from crypto_momentum_lab.operator_dashboard.schemas import (
    LiveAccountSummaryResponse,
    ResearchCollectorResponse,
    ServiceStatusResponse,
    SystemOverviewResponse,
    UniverseStatusResponse,
)
from crypto_momentum_lab.operator_dashboard.status import (
    OperationalStatus,
    freshness_status,
)
from crypto_momentum_lab.persistence.postgres.models import (
    MonitoringMembershipRow,
    RuntimeMarketState15sRow,
    UniverseEntryRow,
    UniverseSnapshotRow,
)


def account_label_sort_key(account_label: str) -> tuple[int, int | str]:
    """Keep the canonical fleet order while allowing custom labels."""
    if account_label == "primary":
        return (0, 0)
    suffix = account_label.removeprefix("account-")
    return (1, int(suffix)) if suffix.isdigit() else (2, account_label)


def live_account_fleet_status(
    accounts: Sequence[LiveAccountSummaryResponse],
) -> OperationalStatus:
    if not accounts:
        return OperationalStatus.NO_DATA
    if any(account.status is OperationalStatus.HALTED for account in accounts):
        return OperationalStatus.HALTED
    if any(account.status is OperationalStatus.STALE for account in accounts):
        return OperationalStatus.STALE
    if any(account.status is OperationalStatus.UNKNOWN for account in accounts):
        return OperationalStatus.UNKNOWN
    if any(account.status is OperationalStatus.DEGRADED for account in accounts):
        return OperationalStatus.DEGRADED
    return (
        OperationalStatus.READY
        if all(account.status is OperationalStatus.READY for account in accounts)
        else OperationalStatus.DEGRADED
    )


class OverviewQueries:
    """Deep query module for the dashboard's operational overview."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        clock: Callable[[], datetime],
        stale_after_seconds: float,
        research_collector_root: Path,
    ) -> None:
        self._session_factory = session_factory
        self._clock = clock
        self._stale_after_seconds = stale_after_seconds
        self._research_collector_root = research_collector_root

    async def health(self) -> dict[str, str]:
        try:
            async with self._session_factory() as session:
                await session.execute(text("SELECT 1"))
            return {"app_status": "UP", "database_status": "UP"}
        except Exception:
            return {"app_status": "UP", "database_status": "DOWN"}

    async def research_collector(self) -> ResearchCollectorResponse:
        return await asyncio.to_thread(
            read_research_collector_status,
            self._research_collector_root,
            now=self._clock(),
        )

    async def overview(self) -> SystemOverviewResponse:
        now = self._clock()
        async with self._session_factory() as session:
            market_at = await session.scalar(
                select(RuntimeMarketState15sRow.bucket_end)
                .order_by(RuntimeMarketState15sRow.bucket_start.desc())
                .limit(1)
            )
        services = [
            service("market-data", now, market_at, self._stale_after_seconds),
            ServiceStatusResponse(
                name="database",
                status=OperationalStatus.READY,
                observed_at=now,
                age_seconds=0,
            ),
        ]
        return SystemOverviewResponse(
            generated_at=now,
            database_status=OperationalStatus.READY,
            services=services,
            active_halt_count=0,
        )

    async def universe(self) -> UniverseStatusResponse:
        async with self._session_factory() as session:
            snapshot = await session.scalar(
                select(UniverseSnapshotRow)
                .where(UniverseSnapshotRow.activated.is_(True))
                .order_by(UniverseSnapshotRow.observed_at.desc())
                .limit(1)
            )
            if snapshot is None:
                return UniverseStatusResponse(
                    status=OperationalStatus.NO_DATA,
                    observed_at=None,
                    gainers=[],
                    losers=[],
                    monitored_symbols=[],
                )
            entries = (
                await session.scalars(
                    select(UniverseEntryRow).where(
                        UniverseEntryRow.snapshot_id == snapshot.snapshot_id
                    )
                )
            ).all()
            memberships = (
                await session.scalars(
                    select(MonitoringMembershipRow).where(
                        MonitoringMembershipRow.snapshot_id == snapshot.snapshot_id
                    )
                )
            ).all()
            entries_by_symbol = {entry.symbol: entry for entry in entries}
        gainers = sorted(
            (entry for entry in entries if entry.gainer_rank is not None),
            key=lambda item: item.gainer_rank or 999,
        )[:20]
        return UniverseStatusResponse(
            status=freshness_status(
                now=self._clock(),
                observed_at=snapshot.observed_at,
                stale_after_seconds=3600,
            ),
            observed_at=snapshot.observed_at,
            gainers=[universe_entry(row, "gainer") for row in gainers],
            # Loser ranks remain in the database for historical/research
            # compatibility, but the operational dashboard is gainer-only.
            losers=[],
            monitored_symbols=[
                universe_membership(row, entries_by_symbol.get(row.symbol))
                for row in sorted(
                    memberships,
                    key=lambda row: (
                        {"target": 0, "retained": 1, "forced": 2}.get(
                            row.status,
                            99,
                        ),
                        {"gainer": 0, "loser": 1}.get(row.side or "", 2),
                        row.symbol,
                    ),
                )
            ],
        )


def service(
    name: str,
    now: datetime,
    observed_at: datetime | None,
    stale_after_seconds: float,
) -> ServiceStatusResponse:
    return ServiceStatusResponse(
        name=name,
        status=freshness_status(
            now=now,
            observed_at=observed_at,
            stale_after_seconds=stale_after_seconds,
        ),
        observed_at=observed_at,
        age_seconds=None if observed_at is None else age(now, observed_at),
    )


def age(now: datetime, observed_at: datetime) -> float:
    return max(0.0, (now - observed_at).total_seconds())


def universe_entry(row: UniverseEntryRow, side: str) -> dict[str, JsonValue]:
    rank = row.gainer_rank if side == "gainer" else row.loser_rank
    return {
        "symbol": row.symbol,
        "rank": rank,
        "utc_day_return": None
        if row.utc_day_return is None
        else str(row.utc_day_return),
        "current_price": None if row.current_price is None else str(row.current_price),
    }


def universe_membership(
    row: MonitoringMembershipRow,
    entry: UniverseEntryRow | None,
) -> dict[str, JsonValue]:
    rank = None
    utc_day_return = None
    current_price = None
    if entry is not None:
        rank = (
            entry.gainer_rank
            if row.side == "gainer"
            else entry.loser_rank
            if row.side == "loser"
            else None
        )
        utc_day_return = (
            None if entry.utc_day_return is None else str(entry.utc_day_return)
        )
        current_price = (
            None if entry.current_price is None else str(entry.current_price)
        )
    return {
        "symbol": row.symbol,
        "status": row.status,
        "side": row.side,
        "rank": rank,
        "utc_day_return": utc_day_return,
        "current_price": current_price,
    }


__all__ = [
    "OverviewQueries",
    "account_label_sort_key",
    "age",
    "live_account_fleet_status",
    "service",
    "universe_entry",
    "universe_membership",
]
