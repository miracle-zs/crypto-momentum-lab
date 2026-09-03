"""Backfill source adapters used by the collector's recovery path."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.market_data.hub import MarketStateBatch
from crypto_momentum_lab.persistence.postgres.runtime_state_repository import (
    PostgresRuntimeMarketStateRepository,
    RuntimeStateCursor,
)
from crypto_momentum_lab.research_collector.models import require_utc


class PostgresMarketStateBackfillSource:
    """Read the short-retention runtime table in cursor order.

    PostgreSQL rows are grouped by bucket where possible.  A large database
    page can split one bucket into two batches; the collector's natural-key
    deduplication makes that case safe and avoids loading the entire recovery
    window into memory.
    """

    def __init__(
        self,
        repository: PostgresRuntimeMarketStateRepository,
        *,
        environment: str,
        page_size: int = 500,
    ) -> None:
        if not environment.strip():
            raise ValueError("environment must not be empty")
        if page_size <= 0:
            raise ValueError("page_size must be positive")
        self._repository = repository
        self._environment = environment
        self._page_size = page_size

    async def latest_bucket(self) -> datetime | None:
        return await self._repository.load_latest_bucket(
            environment=self._environment,
        )

    async def batches_after(
        self,
        cursor: RuntimeStateCursor,
        *,
        until: datetime,
    ) -> AsyncIterator[MarketStateBatch]:
        until = require_utc(until, "until")
        current_cursor = RuntimeStateCursor(
            bucket_start=(
                None
                if cursor.bucket_start is None
                else require_utc(cursor.bucket_start, "cursor.bucket_start")
            ),
            symbol=cursor.symbol,
        )
        while True:
            rows = await self._repository.load_after(
                environment=self._environment,
                cursor=current_cursor,
                limit=self._page_size,
            )
            if not rows:
                return
            eligible = tuple(state for state in rows if state.bucket_start <= until)
            if not eligible:
                return
            for bucket_states in _group_by_bucket(eligible):
                yield MarketStateBatch(
                    sequence=0,
                    published_at=max(state.bucket_end for state in bucket_states),
                    environment=self._environment,
                    states=bucket_states,
                    stream_id=None,
                )
            last = eligible[-1]
            current_cursor = RuntimeStateCursor(
                bucket_start=last.bucket_start.astimezone(UTC),
                symbol=last.symbol,
            )
            if len(eligible) < len(rows) or len(rows) < self._page_size:
                return


def _group_by_bucket(
    states: tuple[MarketState15s, ...],
) -> tuple[tuple[MarketState15s, ...], ...]:
    groups: list[list[MarketState15s]] = []
    current_bucket: datetime | None = None
    for state in states:
        if current_bucket != state.bucket_start:
            groups.append([])
            current_bucket = state.bucket_start
        groups[-1].append(state)
    return tuple(tuple(group) for group in groups)
