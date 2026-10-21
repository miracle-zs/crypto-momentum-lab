"""Read paper positions whose market symbols must remain subscribed."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.persistence.postgres.models import PaperPositionRow


class PostgresPaperPositionProtectionRepository:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._session_factory = session_factory

    async def load_open_position_symbols(
        self,
        run_ids: frozenset[str],
    ) -> frozenset[str]:
        if not run_ids:
            return frozenset()
        async with self._session_factory() as session:
            symbols = (
                await session.scalars(
                    select(PaperPositionRow.symbol)
                    .where(
                        PaperPositionRow.run_id.in_(run_ids),
                        PaperPositionRow.status == "open",
                    )
                    .distinct()
                    .order_by(PaperPositionRow.symbol)
                )
            ).all()
        return frozenset(symbols)
