import time
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

import structlog

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.market.runtime_state_models import RuntimeStateCursor

_MAX_IDLE_POLL_INTERVAL_SECONDS = 3.0
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


def _initial_cursor(start_at: datetime | None) -> RuntimeStateCursor:
    if start_at is None:
        return RuntimeStateCursor()
    return RuntimeStateCursor(bucket_start=start_at, symbol="")


def _is_aware(value: datetime) -> bool:
    return value.tzinfo is not None and value.utcoffset() is not None
