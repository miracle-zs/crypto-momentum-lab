import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Protocol

from crypto_momentum_lab.domain.universe.models import UniverseSnapshot


class RefreshService(Protocol):
    async def refresh(
        self,
        *,
        observed_at: datetime,
    ) -> UniverseSnapshot: ...


def next_refresh_at(
    now: datetime,
    *,
    activation_minute: int,
) -> datetime:
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    utc_now = now.astimezone(UTC)
    candidate = utc_now.replace(
        minute=activation_minute,
        second=0,
        microsecond=0,
    )
    if candidate <= utc_now:
        candidate += timedelta(hours=1)
    return candidate


async def run_scheduler_loop(
    service: RefreshService,
    *,
    activation_minute: int,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    while True:
        now = clock()
        scheduled = next_refresh_at(
            now,
            activation_minute=activation_minute,
        )
        await sleeper(max(0.0, (scheduled - now).total_seconds()))
        await service.refresh(observed_at=scheduled)
