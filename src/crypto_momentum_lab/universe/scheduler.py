import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Protocol

import structlog

from crypto_momentum_lab.domain.universe.models import UniverseSnapshot

log = structlog.get_logger(__name__)


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
    refresh_interval_minutes: int = 60,
) -> datetime:
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    if refresh_interval_minutes <= 0:
        raise ValueError("refresh_interval_minutes must be positive")
    utc_now = now.astimezone(UTC)
    anchor = utc_now.replace(
        minute=activation_minute,
        second=0,
        microsecond=0,
    )
    if anchor > utc_now:
        return anchor
    interval_seconds = refresh_interval_minutes * 60
    elapsed_seconds = (utc_now - anchor).total_seconds()
    steps = int(elapsed_seconds // interval_seconds) + 1
    return anchor + timedelta(seconds=steps * interval_seconds)


async def run_scheduler_loop(
    service: RefreshService,
    *,
    activation_minute: int,
    refresh_interval_minutes: int = 60,
    retry_delay_seconds: float = 5.0,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    if retry_delay_seconds < 0:
        raise ValueError("retry_delay_seconds must be non-negative")

    scheduled: datetime | None = None
    while True:
        now = clock()
        if scheduled is None:
            scheduled = next_refresh_at(
                now,
                activation_minute=activation_minute,
                refresh_interval_minutes=refresh_interval_minutes,
            )
        await sleeper(max(0.0, (scheduled - now).total_seconds()))
        try:
            await service.refresh(observed_at=scheduled)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            log.exception(
                "universe_refresh_failed",
                observed_at=scheduled.isoformat(),
                error=str(error),
            )
            if retry_delay_seconds > 0:
                await sleeper(retry_delay_seconds)
            continue
        scheduled = None
