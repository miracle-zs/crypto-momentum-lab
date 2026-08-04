import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from crypto_momentum_lab.execution_account.sync import (
    ExecutionAccountSyncResult,
)


class AccountSyncCycle(Protocol):
    async def sync_once(
        self,
        *,
        observed_at: datetime,
        publish_transient_states: bool,
        include_fills: bool,
    ) -> ExecutionAccountSyncResult: ...


@dataclass(frozen=True, slots=True)
class ContinuousAccountSyncConfig:
    interval_seconds: float = 5.0
    fill_interval_seconds: float = 60.0

    def __post_init__(self) -> None:
        if self.interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        if self.fill_interval_seconds < self.interval_seconds:
            raise ValueError(
                "fill_interval_seconds must not be below interval_seconds"
            )


@dataclass(frozen=True, slots=True)
class ContinuousAccountSyncResult:
    cycle_count: int
    failure_count: int
    last_sync: ExecutionAccountSyncResult | None


class ContinuousAccountSyncDaemon:
    def __init__(
        self,
        *,
        service: AccountSyncCycle,
        config: ContinuousAccountSyncConfig,
        clock: Callable[[], datetime] = lambda: datetime.now(tz=UTC),
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        on_error: Callable[[Exception], None] | None = None,
    ) -> None:
        self._service = service
        self._config = config
        self._clock = clock
        self._sleep = sleep
        self._on_error = on_error

    async def run(
        self,
        *,
        max_cycles: int | None = None,
    ) -> ContinuousAccountSyncResult:
        if max_cycles is not None and max_cycles <= 0:
            raise ValueError("max_cycles must be positive when present")
        cycle_count = 0
        failure_count = 0
        last_sync: ExecutionAccountSyncResult | None = None
        last_fill_sync_at: datetime | None = None
        publish_transient_states = True
        while max_cycles is None or cycle_count < max_cycles:
            observed_at = self._now()
            include_fills = (
                last_fill_sync_at is None
                or observed_at
                >= last_fill_sync_at
                + timedelta(seconds=self._config.fill_interval_seconds)
            )
            try:
                last_sync = await self._service.sync_once(
                    observed_at=observed_at,
                    publish_transient_states=publish_transient_states,
                    include_fills=include_fills,
                )
                if include_fills:
                    last_fill_sync_at = observed_at
            except Exception as error:
                failure_count += 1
                if self._on_error is not None:
                    self._on_error(error)
            publish_transient_states = False
            cycle_count += 1
            if max_cycles is None or cycle_count < max_cycles:
                await self._sleep_for_interval()
        return ContinuousAccountSyncResult(
            cycle_count=cycle_count,
            failure_count=failure_count,
            last_sync=last_sync,
        )

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("clock must return timezone-aware datetime")
        return value

    async def _sleep_for_interval(self) -> None:
        await self._sleep(self._config.interval_seconds)
