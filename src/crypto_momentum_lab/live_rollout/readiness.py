"""Structured readiness state for a live strategy process.

The container healthcheck answers whether the process is alive and still
touching its database path.  This module publishes the smaller, business-level
readiness view that deployment and operators need after that probe passes:
which entry pool is active, how much strategy history is warm, and whether the
entry gate currently admits new orders.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

import structlog

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import StrategyCheckpoint
from crypto_momentum_lab.health import LocalHealthWriter

log = structlog.get_logger()


class ReadinessStrategy(Protocol):
    """The compact strategy surface needed by readiness reporting."""

    def required_data(self) -> object: ...

    def checkpoint(
        self,
        *,
        include_market_state_buffers: bool = True,
    ) -> StrategyCheckpoint: ...


@dataclass(frozen=True, slots=True)
class LiveWarmupStatus:
    """Validated startup warmup coverage for the configured entry symbols."""

    required_buckets: int
    expected_symbols: frozenset[str]
    complete_symbols: frozenset[str]
    cutover_at: datetime

    def __post_init__(self) -> None:
        if self.required_buckets <= 0:
            raise ValueError("required_buckets must be positive")
        if (
            self.cutover_at.tzinfo is None
            or self.cutover_at.utcoffset() is None
        ):
            raise ValueError("cutover_at must be timezone-aware")
        if not self.complete_symbols <= self.expected_symbols:
            raise ValueError(
                "complete_symbols must be a subset of expected_symbols"
            )

    @property
    def deferred_symbols(self) -> frozenset[str]:
        return self.expected_symbols - self.complete_symbols


class LiveReadinessPublisher:
    """Publish a compact JSON readiness snapshot without affecting trading.

    The public interface intentionally consists of three state updates:
    startup warmup coverage, entry-gate state, and observed market progress.
    File I/O is best-effort; a diagnostics failure must never stop a live
    process or change its fail-closed entry behaviour.
    """

    schema_version = 1

    def __init__(
        self,
        *,
        health: LocalHealthWriter | None,
        account_label: str,
        session_id: str,
        strategy: str,
        code_commit: str,
        migration_revision: str,
        entry_universe_target_count: int | None,
        warmup_required_buckets: int,
    ) -> None:
        for value, field_name in (
            (account_label, "account_label"),
            (session_id, "session_id"),
            (strategy, "strategy"),
            (code_commit, "code_commit"),
            (migration_revision, "migration_revision"),
        ):
            if not value.strip():
                raise ValueError(f"{field_name} must not be empty")
        if (
            entry_universe_target_count is not None
            and entry_universe_target_count <= 0
        ):
            raise ValueError("entry_universe_target_count must be positive")
        if warmup_required_buckets <= 0:
            raise ValueError("warmup_required_buckets must be positive")
        self._health = health
        self._account_label = account_label
        self._session_id = session_id
        self._strategy = strategy
        self._code_commit = code_commit
        self._migration_revision = migration_revision
        self._entry_universe_target_count = entry_universe_target_count
        self._entry_universe_count = 0
        self._warmup_required_buckets = warmup_required_buckets
        self._warmup_expected_symbols: frozenset[str] = frozenset()
        self._warmup_complete_symbols: frozenset[str] = frozenset()
        self._warmup_cutover_at: datetime | None = None
        self._latest_market_state_at: datetime | None = None
        self._latest_market_state_age_seconds: float | None = None
        self._last_published_market_bucket: datetime | None = None
        self._entry_enabled = False
        self._entry_enabled_reason = "initializing"
        self.publish()

    def set_expected_warmup_symbols(self, symbols: Collection[str]) -> None:
        """Set the expected warmup universe before recovery starts."""

        self._warmup_expected_symbols = _normalized_symbols(symbols)
        self._warmup_complete_symbols = frozenset()
        self.publish()

    def update_warmup(self, status: LiveWarmupStatus) -> None:
        """Publish the exact coverage calculated by startup recovery."""

        self._warmup_required_buckets = status.required_buckets
        self._warmup_expected_symbols = status.expected_symbols
        self._warmup_complete_symbols = status.complete_symbols
        self._warmup_cutover_at = status.cutover_at
        self.publish()

    def update_warmup_progress(
        self,
        strategy: ReadinessStrategy,
        *,
        expected_symbols: Collection[str] | None = None,
    ) -> None:
        """Refresh symbol counts from the strategy's compact checkpoint."""

        try:
            requirement = strategy.required_data()
            required_buckets = int(requirement.warmup_buckets)
            checkpoint = strategy.checkpoint(
                include_market_state_buffers=False
            )
            if expected_symbols is not None:
                normalized_expected = _normalized_symbols(expected_symbols)
            elif self._warmup_expected_symbols:
                normalized_expected = self._warmup_expected_symbols
            else:
                normalized_expected = frozenset(
                    checkpoint.warmup_buckets_by_symbol
                )
            warmup_by_symbol = checkpoint.warmup_buckets_by_symbol
            complete = frozenset(
                symbol
                for symbol in normalized_expected
                if int(warmup_by_symbol.get(symbol, 0)) >= required_buckets
            )
            self._warmup_required_buckets = required_buckets
            self._warmup_expected_symbols = normalized_expected
            self._warmup_complete_symbols = complete
            self.publish()
        except Exception as error:
            log.warning(
                "live_readiness_warmup_progress_failed",
                error_type=type(error).__name__,
            )

    def update_entry_gate(
        self,
        *,
        entry_universe_count: int,
        entry_enabled: bool,
        entry_enabled_reason: str,
    ) -> None:
        """Publish the current entry pool and fail-closed gate state."""

        if entry_universe_count < 0:
            raise ValueError("entry_universe_count must not be negative")
        if not isinstance(entry_enabled, bool):
            raise TypeError("entry_enabled must be a bool")
        if not entry_enabled_reason.strip():
            raise ValueError("entry_enabled_reason must not be empty")
        self._entry_universe_count = entry_universe_count
        self._entry_enabled = entry_enabled
        self._entry_enabled_reason = entry_enabled_reason
        self.publish()

    def observe_market_state(
        self,
        state: MarketState15s,
        *,
        strategy: ReadinessStrategy,
        entry_universe_count: int,
    ) -> None:
        """Record market freshness and update rolling warmup progress."""

        observed_at = datetime.now(tz=UTC)
        self._latest_market_state_at = state.bucket_end
        self._latest_market_state_age_seconds = max(
            0.0,
            (observed_at - state.bucket_end).total_seconds(),
        )
        self._entry_universe_count = entry_universe_count
        if (
            self._last_published_market_bucket is not None
            and state.bucket_start <= self._last_published_market_bucket
        ):
            return
        self._last_published_market_bucket = state.bucket_start
        if self._warmup_expected_symbols:
            self.update_warmup_progress(
                strategy,
                expected_symbols=self._warmup_expected_symbols,
            )
            return
        self.publish()

    def publish(self) -> None:
        """Best-effort atomic publication of the current JSON snapshot."""

        if self._health is None:
            return
        payload: Mapping[str, object] = {
            "schema_version": self.schema_version,
            "observed_at": datetime.now(tz=UTC).isoformat(),
            "account_label": self._account_label,
            "session_id": self._session_id,
            "strategy": self._strategy,
            "code_commit": self._code_commit,
            "migration_revision": self._migration_revision,
            "entry_universe_target_count": self._entry_universe_target_count,
            "entry_universe_count": self._entry_universe_count,
            "warmup_required_buckets": self._warmup_required_buckets,
            "warmup_expected_symbols": len(self._warmup_expected_symbols),
            "warmup_complete_symbols": len(self._warmup_complete_symbols),
            "warmup_deferred_symbols": len(
                self._warmup_expected_symbols - self._warmup_complete_symbols
            ),
            "warmup_cutover_at": _isoformat(self._warmup_cutover_at),
            "latest_market_state_at": _isoformat(self._latest_market_state_at),
            "latest_market_state_age_seconds": (
                self._latest_market_state_age_seconds
            ),
            "entry_enabled": self._entry_enabled,
            "entry_enabled_reason": self._entry_enabled_reason,
        }
        try:
            self._health.write_readiness(payload)
        except Exception as error:
            log.warning(
                "live_readiness_publish_failed",
                error_type=type(error).__name__,
            )


def _normalized_symbols(symbols: Collection[str]) -> frozenset[str]:
    return frozenset(
        symbol.strip().upper() for symbol in symbols if symbol.strip()
    )


def _isoformat(value: datetime | None) -> str | None:
    return None if value is None else value.astimezone(UTC).isoformat()


__all__ = ["LiveReadinessPublisher", "LiveWarmupStatus", "ReadinessStrategy"]
