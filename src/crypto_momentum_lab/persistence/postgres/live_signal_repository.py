"""Best-effort persistence adapter for live strategy-signal observations."""

from collections.abc import Mapping, Sequence

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.persistence.postgres.models import LiveStrategySignalRow


class PostgresLiveSignalRepository:
    """Persist live signals on the isolated observability session pool."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def save_signals(
        self,
        signals: Sequence[Mapping[str, object]],
    ) -> None:
        if not signals:
            return
        async with self._session_factory() as session:
            async with session.begin():
                # Signal observations are diagnostic. They must never make an
                # exchange order wait for a WAL flush on the observability
                # plane.
                await session.execute(text("SET LOCAL synchronous_commit = OFF"))
                await session.execute(
                    insert(LiveStrategySignalRow)
                    .values(tuple(dict(signal) for signal in signals))
                    .on_conflict_do_nothing(index_elements=["observation_id"])
                )


__all__ = ["PostgresLiveSignalRepository"]
