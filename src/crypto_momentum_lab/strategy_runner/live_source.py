import asyncio
import time
from collections.abc import Awaitable, Callable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol

import asyncpg  # type: ignore[import-untyped]
import structlog

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.persistence.postgres.repository import (
    PostgresUniverseRepository,
)
from crypto_momentum_lab.persistence.postgres.runtime_state_repository import (
    RUNTIME_STATE_READY_CHANNEL,
    PostgresRuntimeMarketStateRepository,
    RuntimeStateCursor,
)

_MAX_IDLE_POLL_INTERVAL_SECONDS = 3.0
_NOTIFICATION_RETRY_SECONDS = 5.0
log = structlog.get_logger(__name__)


class RuntimeStateLoader(Protocol):
    def load_after(
        self,
        *,
        cursor: RuntimeStateCursor,
        limit: int,
        upper_bound: datetime | None = None,
    ) -> tuple[MarketState15s, ...]: ...

    def load_recovery_window(
        self,
        *,
        last_processed_at_by_symbol: Mapping[str, datetime],
        lookback_seconds: int,
        limit: int,
        upper_bound: datetime | None = None,
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
    notification_wait_seconds: float = 30.0

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
        if self.notification_wait_seconds <= 0:
            raise ValueError("notification_wait_seconds must be positive")


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
        upper_bound: datetime | None = None,
    ) -> tuple[MarketState15s, ...]:
        kwargs: dict[str, object] = {
            "last_processed_at_by_symbol": last_processed_at_by_symbol,
            "lookback_seconds": lookback_seconds,
            "limit": limit,
        }
        if upper_bound is not None:
            kwargs["upper_bound"] = upper_bound
        return self.loader.load_recovery_window(
            **kwargs  # type: ignore[arg-type]
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
        idle_poll_interval = self.config.poll_interval_seconds
        try:
            prepare_wakeup = getattr(self.loader, "prepare_wakeup", None)
            wakeup_enabled = callable(prepare_wakeup)
            if callable(prepare_wakeup):
                try:
                    wakeup_enabled = prepare_wakeup() is not False
                except Exception:
                    # The durable cursor and fallback polling remain authoritative.
                    wakeup_enabled = False
            while yielded < self.config.max_states:
                limit = min(
                    self.config.batch_size,
                    self.config.max_states - yielded,
                )
                batch = self.loader.load_after(cursor=cursor, limit=limit)
                if batch:
                    idle_started_at = time.monotonic()
                    idle_poll_interval = self.config.poll_interval_seconds
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
                    log.error(
                        "paper_market_state_source_idle_timeout",
                        environment=self.config.environment,
                        idle_timeout_seconds=self.config.idle_timeout_seconds,
                        elapsed_idle_seconds=elapsed_idle,
                        yielded_state_count=yielded,
                        cursor_bucket_start=(
                            None
                            if cursor.bucket_start is None
                            else cursor.bucket_start.isoformat()
                        ),
                        cursor_symbol=cursor.symbol,
                        action="exit_for_container_restart",
                    )
                    return
                sleep_seconds = min(
                    idle_poll_interval,
                    self.config.idle_timeout_seconds - elapsed_idle,
                )
                if sleep_seconds <= 0:
                    sleep_seconds = min(
                        0.01,
                        self.config.idle_timeout_seconds - elapsed_idle,
                    )
                if sleep_seconds > 0:
                    wait_for_data = getattr(self.loader, "wait_for_data", None)
                    if wakeup_enabled and callable(wait_for_data):
                        wait_for_data(
                            min(
                                self.config.notification_wait_seconds,
                                self.config.idle_timeout_seconds - elapsed_idle,
                            )
                        )
                    else:
                        time.sleep(sleep_seconds)
                        idle_poll_interval = min(
                            _MAX_IDLE_POLL_INTERVAL_SECONDS,
                            max(
                                self.config.poll_interval_seconds,
                                sleep_seconds * 2,
                            ),
                        )
        finally:
            self.loader.close()


class _AsyncPostgresRuntimeStateWakeup:
    def __init__(
        self,
        *,
        database_url: str,
        environment: str,
        channel: str,
    ) -> None:
        self._database_url = database_url
        self._environment = environment
        self._channel = channel
        self._event = asyncio.Event()
        self._connection: asyncpg.Connection | None = None

    async def ensure_started(self) -> None:
        connection = self._connection
        if connection is not None and not connection.is_closed():
            return
        connection = await asyncpg.connect(
            self._notification_dsn(),
            timeout=5.0,
        )
        try:
            await connection.add_listener(
                self._channel,
                self._on_notification,
            )
        except Exception:
            await connection.close()
            raise
        self._connection = connection

    async def wait(self, timeout_seconds: float) -> bool:
        if timeout_seconds <= 0:
            return False
        try:
            await self.ensure_started()
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(min(_NOTIFICATION_RETRY_SECONDS, timeout_seconds))
            return False
        if self._event.is_set():
            self._event.clear()
            return True
        try:
            await asyncio.wait_for(
                self._event.wait(),
                timeout=timeout_seconds,
            )
        except TimeoutError:
            return False
        except asyncio.CancelledError:
            raise
        except Exception:
            await self._close_connection()
            return False
        self._event.clear()
        return True

    async def close(self) -> None:
        await self._close_connection()

    async def _close_connection(self) -> None:
        connection = self._connection
        self._connection = None
        if connection is None:
            return
        try:
            await connection.remove_listener(
                self._channel,
                self._on_notification,
            )
        except Exception:
            pass
        try:
            await connection.close()
        except Exception:
            pass

    def _on_notification(
        self,
        _connection: asyncpg.Connection,
        _pid: int,
        channel: str,
        payload: str,
    ) -> None:
        if channel != self._channel:
            return
        if payload == self._environment or payload.startswith(
            f"{self._environment}|"
        ):
            self._event.set()

    def _notification_dsn(self) -> str:
        for prefix in ("postgresql+asyncpg://", "postgresql+psycopg://"):
            if self._database_url.startswith(prefix):
                return "postgresql://" + self._database_url[len(prefix) :]
        return self._database_url


@dataclass(frozen=True, slots=True)
class AsyncPostgresRuntimeStateLoader:
    repository: PostgresRuntimeMarketStateRepository
    environment: str
    universe_repository: PostgresUniverseRepository | None = None
    shutdown: Callable[[], Awaitable[None]] | None = None
    notification_database_url: str | None = None
    notification_channel: str = RUNTIME_STATE_READY_CHANNEL
    _event_loop: asyncio.AbstractEventLoop = field(
        default_factory=asyncio.new_event_loop,
        repr=False,
        compare=False,
    )
    _notification_wakeup: _AsyncPostgresRuntimeStateWakeup | None = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        if self.notification_database_url is not None:
            object.__setattr__(
                self,
                "_notification_wakeup",
                _AsyncPostgresRuntimeStateWakeup(
                    database_url=self.notification_database_url,
                    environment=self.environment,
                    channel=self.notification_channel,
                ),
            )

    def prepare_wakeup(self) -> bool:
        if self._notification_wakeup is None:
            return False
        try:
            self._event_loop.run_until_complete(
                self._notification_wakeup.ensure_started()
            )
        except Exception:
            # Database polling remains the fallback if LISTEN is unavailable.
            pass
        return True

    def wait_for_data(self, timeout_seconds: float) -> None:
        wakeup = self._notification_wakeup
        if wakeup is None:
            time.sleep(timeout_seconds)
            return
        try:
            self._event_loop.run_until_complete(wakeup.wait(timeout_seconds))
        except asyncio.CancelledError:
            raise
        except Exception:
            time.sleep(min(_NOTIFICATION_RETRY_SECONDS, timeout_seconds))

    def load_after(
        self,
        *,
        cursor: RuntimeStateCursor,
        limit: int,
        upper_bound: datetime | None = None,
    ) -> tuple[MarketState15s, ...]:
        kwargs: dict[str, object] = {
            "environment": self.environment,
            "cursor": cursor,
            "limit": limit,
        }
        if upper_bound is not None:
            kwargs["upper_bound"] = upper_bound
        return self._event_loop.run_until_complete(
            self.repository.load_after(**kwargs)  # type: ignore[arg-type]
        )

    def load_recovery_window(
        self,
        *,
        last_processed_at_by_symbol: Mapping[str, datetime],
        lookback_seconds: int,
        limit: int,
        upper_bound: datetime | None = None,
    ) -> tuple[MarketState15s, ...]:
        kwargs: dict[str, object] = {
            "environment": self.environment,
            "last_processed_at_by_symbol": last_processed_at_by_symbol,
            "lookback_seconds": lookback_seconds,
            "limit": limit,
        }
        if upper_bound is not None:
            kwargs["upper_bound"] = upper_bound
        return self._event_loop.run_until_complete(
            self.repository.load_recovery_window(
                **kwargs  # type: ignore[arg-type]
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
        try:
            if self._notification_wakeup is not None:
                self._event_loop.run_until_complete(
                    self._notification_wakeup.close()
                )
            if self.shutdown is not None:
                self._event_loop.run_until_complete(self.shutdown())
        finally:
            self._event_loop.close()


def _initial_cursor(start_at: datetime | None) -> RuntimeStateCursor:
    if start_at is None:
        return RuntimeStateCursor()
    return RuntimeStateCursor(bucket_start=start_at, symbol="")


def _is_aware(value: datetime) -> bool:
    return value.tzinfo is not None and value.utcoffset() is not None
