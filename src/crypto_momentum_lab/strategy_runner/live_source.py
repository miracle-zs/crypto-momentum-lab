import asyncio
import time
from collections.abc import Awaitable, Callable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.persistence.postgres.repository import (
    PostgresUniverseRepository,
)
from crypto_momentum_lab.persistence.postgres.runtime_state_repository import (
    PostgresRuntimeMarketStateRepository,
    RuntimeStateCursor,
)


class RuntimeStateLoader(Protocol):
    def load_after(
        self,
        *,
        cursor: RuntimeStateCursor,
        limit: int,
    ) -> tuple[MarketState15s, ...]: ...

    def load_recovery_window(
        self,
        *,
        last_processed_at_by_symbol: Mapping[str, datetime],
        lookback_seconds: int,
        limit: int,
    ) -> tuple[MarketState15s, ...]: ...

    def load_active_symbols(self) -> frozenset[str]: ...

    def load_active_symbols_at(self, observed_at: datetime) -> frozenset[str]: ...

    def load_positive_gainer_symbols_at(
        self,
        observed_at: datetime,
        *,
        top_count: int,
    ) -> frozenset[str]: ...

    def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class PaperLiveSourceConfig:
    environment: str
    start_at: datetime | None
    poll_interval_seconds: float
    idle_timeout_seconds: float
    max_states: int
    batch_size: int

    def __post_init__(self) -> None:
        if not self.environment.strip():
            raise ValueError("environment must not be empty")
        if self.start_at is not None and not _is_aware(self.start_at):
            raise ValueError("start_at must be timezone-aware")
        if self.poll_interval_seconds < 0:
            raise ValueError("poll_interval_seconds must be non-negative")
        if self.idle_timeout_seconds < 0:
            raise ValueError("idle_timeout_seconds must be non-negative")
        if self.max_states <= 0:
            raise ValueError("max_states must be positive")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")


@dataclass(frozen=True, slots=True)
class PostgresPaperMarketStateSource:
    loader: RuntimeStateLoader
    config: PaperLiveSourceConfig

    @property
    def description(self) -> str:
        return f"postgres-runtime-states:{self.config.environment}"

    def load_active_symbols(self) -> frozenset[str]:
        return self.loader.load_active_symbols()

    def load_recovery_window(
        self,
        *,
        last_processed_at_by_symbol: Mapping[str, datetime],
        lookback_seconds: int,
        limit: int,
    ) -> tuple[MarketState15s, ...]:
        return self.loader.load_recovery_window(
            last_processed_at_by_symbol=last_processed_at_by_symbol,
            lookback_seconds=lookback_seconds,
            limit=limit,
        )

    def load_active_symbols_at(self, observed_at: datetime) -> frozenset[str]:
        return self.loader.load_active_symbols_at(observed_at)

    def load_positive_gainer_symbols_at(
        self,
        observed_at: datetime,
        *,
        top_count: int,
    ) -> frozenset[str]:
        return self.loader.load_positive_gainer_symbols_at(
            observed_at,
            top_count=top_count,
        )

    def __iter__(self) -> Iterator[MarketState15s]:
        start_at = self.config.start_at
        cursor = _initial_cursor(start_at)
        yielded = 0
        idle_started_at = time.monotonic()
        try:
            while yielded < self.config.max_states:
                limit = min(
                    self.config.batch_size,
                    self.config.max_states - yielded,
                )
                batch = self.loader.load_after(cursor=cursor, limit=limit)
                if batch:
                    idle_started_at = time.monotonic()
                    for state in batch:
                        if state.environment != self.config.environment:
                            raise ValueError("runtime state environment mismatch")
                        yield state
                        yielded += 1
                        cursor = RuntimeStateCursor(
                            bucket_start=state.bucket_start,
                            symbol=state.symbol,
                        )
                        if yielded >= self.config.max_states:
                            return
                    continue

                elapsed_idle = time.monotonic() - idle_started_at
                if elapsed_idle >= self.config.idle_timeout_seconds:
                    return
                sleep_seconds = min(
                    self.config.poll_interval_seconds,
                    self.config.idle_timeout_seconds - elapsed_idle,
                )
                if sleep_seconds <= 0:
                    sleep_seconds = min(
                        0.01,
                        self.config.idle_timeout_seconds - elapsed_idle,
                    )
                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)
        finally:
            self.loader.close()


@dataclass(frozen=True, slots=True)
class AsyncPostgresRuntimeStateLoader:
    repository: PostgresRuntimeMarketStateRepository
    environment: str
    universe_repository: PostgresUniverseRepository | None = None
    shutdown: Callable[[], Awaitable[None]] | None = None
    _event_loop: asyncio.AbstractEventLoop = field(
        default_factory=asyncio.new_event_loop,
        repr=False,
        compare=False,
    )

    def load_after(
        self,
        *,
        cursor: RuntimeStateCursor,
        limit: int,
    ) -> tuple[MarketState15s, ...]:
        return self._event_loop.run_until_complete(
            self.repository.load_after(
                environment=self.environment,
                cursor=cursor,
                limit=limit,
            )
        )

    def load_recovery_window(
        self,
        *,
        last_processed_at_by_symbol: Mapping[str, datetime],
        lookback_seconds: int,
        limit: int,
    ) -> tuple[MarketState15s, ...]:
        return self._event_loop.run_until_complete(
            self.repository.load_recovery_window(
                environment=self.environment,
                last_processed_at_by_symbol=last_processed_at_by_symbol,
                lookback_seconds=lookback_seconds,
                limit=limit,
            )
        )

    def load_active_symbols(self) -> frozenset[str]:
        if self.universe_repository is None:
            return frozenset()
        loader = getattr(
            self.universe_repository,
            "load_active_entry_symbols_at",
            None,
        )
        if callable(loader):
            return self._event_loop.run_until_complete(loader(None))
        memberships = self._event_loop.run_until_complete(
            self.universe_repository.load_active_memberships()
        )
        return frozenset(memberships)

    def load_active_symbols_at(self, observed_at: datetime) -> frozenset[str]:
        if self.universe_repository is None:
            return frozenset()
        loader = getattr(
            self.universe_repository,
            "load_active_entry_symbols_at",
            None,
        )
        if callable(loader):
            return self._event_loop.run_until_complete(loader(observed_at))
        memberships = self._event_loop.run_until_complete(
            self.universe_repository.load_active_memberships_at(observed_at)
        )
        return frozenset(memberships)

    def load_positive_gainer_symbols_at(
        self,
        observed_at: datetime,
        *,
        top_count: int,
    ) -> frozenset[str]:
        if self.universe_repository is None:
            return frozenset()
        loader = getattr(
            self.universe_repository,
            "load_positive_gainer_symbols_at",
            None,
        )
        if not callable(loader):
            return frozenset()
        return self._event_loop.run_until_complete(
            loader(observed_at, top_count=top_count)
        )

    def close(self) -> None:
        if self._event_loop.is_closed():
            return
        if self.shutdown is not None:
            self._event_loop.run_until_complete(self.shutdown())
        self._event_loop.close()


def _initial_cursor(start_at: datetime | None) -> RuntimeStateCursor:
    if start_at is None:
        return RuntimeStateCursor()
    return RuntimeStateCursor(bucket_start=start_at, symbol="")


def _is_aware(value: datetime) -> bool:
    return value.tzinfo is not None and value.utcoffset() is not None
