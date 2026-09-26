"""Structured readiness state for a live strategy process.

The container healthcheck answers whether the process is alive and still
touching its database path.  This module publishes the smaller, business-level
readiness view that deployment and operators need after that probe passes:
which entry pool is active, how much strategy history is warm, and whether the
entry gate currently admits new orders.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Protocol

import structlog

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import (
    StrategyCheckpoint,
    StrategyDataRequirement,
)
from crypto_momentum_lab.health import LocalHealthWriter

log = structlog.get_logger()


class TradeabilityMode(StrEnum):
    """Business execution safety mode for live operations."""

    FULLY_TRADEABLE = "FULLY_TRADEABLE"
    EXIT_ONLY = "EXIT_ONLY"
    HALTED = "HALTED"
    DEGRADED = "DEGRADED"


@dataclass(frozen=True, slots=True)
class TradeabilitySnapshot:
    """Explicit tradeability state separating business gates from process liveness."""

    mode: TradeabilityMode
    entry_gate_open: bool
    entry_gate_reason: str
    exit_gate_open: bool
    exit_gate_reason: str
    unmanaged_risk_clear: bool
    halt_active: bool

    @classmethod
    def create(
        cls,
        *,
        entry_gate_open: bool,
        entry_gate_reason: str,
        exit_gate_open: bool = True,
        exit_gate_reason: str = "normal",
        unmanaged_risk_clear: bool = True,
        halt_active: bool = False,
    ) -> TradeabilitySnapshot:
        if halt_active or not exit_gate_open:
            mode = TradeabilityMode.HALTED
        elif not unmanaged_risk_clear:
            mode = TradeabilityMode.DEGRADED
        elif not entry_gate_open:
            mode = TradeabilityMode.EXIT_ONLY
        else:
            mode = TradeabilityMode.FULLY_TRADEABLE

        return cls(
            mode=mode,
            entry_gate_open=entry_gate_open,
            entry_gate_reason=entry_gate_reason,
            exit_gate_open=exit_gate_open,
            exit_gate_reason=exit_gate_reason,
            unmanaged_risk_clear=unmanaged_risk_clear,
            halt_active=halt_active,
        )


@dataclass(frozen=True, slots=True)
class StreamReadinessSnapshot:
    """Availability across upstream real-time market and account streams."""

    overall: str
    streams: dict[str, str]

    @classmethod
    def from_streams(cls, streams: Mapping[str, str]) -> StreamReadinessSnapshot:
        values = [v.upper() for v in streams.values()]
        if not values:
            overall = "UNKNOWN"
        elif any(v == "DISRUPTED" for v in values):
            overall = "DISRUPTED"
        elif any(v == "RECOVERING" for v in values):
            overall = "RECOVERING"
        elif any(v == "CONNECTING" for v in values):
            overall = "CONNECTING"
        elif all(v == "READY" for v in values):
            overall = "READY"
        else:
            overall = "UNKNOWN"
        return cls(overall=overall, streams=dict(streams))


class TradeabilityAlertManager:
    """Edge-triggered logger for tradeability and readiness transitions.

    Emits alerts on:
    1. Mode change (edge trigger)
    2. Reason change (edge trigger)
    3. Severity change (edge trigger)
    4. Fallback heartbeat interval for non-healthy states to prevent
       silent stale states.
    5. Clear recovery logging when transitioning back to FULLY_TRADEABLE.
    """

    def __init__(
        self,
        *,
        fallback_heartbeat_seconds: float = 60.0,
        clock: Callable[[], float] | None = None,
    ) -> None:
        if fallback_heartbeat_seconds <= 0:
            raise ValueError("fallback_heartbeat_seconds must be positive")
        self._fallback_heartbeat_seconds = fallback_heartbeat_seconds
        self._clock = clock or time.monotonic
        self._last_mode: str | None = None
        self._last_reason: str | None = None
        self._last_severity: str | None = None
        self._last_alerted_at: float = 0.0
        self._alert_count: int = 0

    @property
    def last_mode(self) -> str | None:
        return self._last_mode

    @property
    def last_reason(self) -> str | None:
        return self._last_reason

    @property
    def last_severity(self) -> str | None:
        return self._last_severity

    @property
    def alert_count(self) -> int:
        return self._alert_count

    def observe(
        self,
        *,
        mode: str | TradeabilityMode,
        reason: str,
        severity: str = "WARNING",
        details: Mapping[str, Any] | None = None,
    ) -> bool:
        mode_str = getattr(mode, "value", str(mode))
        now = self._clock()
        details_dict = dict(details) if details else {}

        if self._last_mode is None:
            self._last_mode = mode_str
            self._last_reason = reason
            self._last_severity = severity
            self._last_alerted_at = now
            self._alert_count = 1
            if mode_str != TradeabilityMode.FULLY_TRADEABLE.value:
                self._emit_alert(
                    mode_str,
                    reason,
                    severity,
                    is_heartbeat=False,
                    **details_dict,
                )
                return True
            return False

        mode_changed = mode_str != self._last_mode
        reason_changed = reason != self._last_reason
        severity_changed = severity != self._last_severity

        is_heartbeat = False
        heartbeat_due = (
            mode_str != TradeabilityMode.FULLY_TRADEABLE.value
            and (now - self._last_alerted_at) >= self._fallback_heartbeat_seconds
        )

        should_alert = False

        if mode_changed:
            should_alert = True
            if mode_str == TradeabilityMode.FULLY_TRADEABLE.value:
                log.info(
                    "tradeability_recovered",
                    current_mode=mode_str,
                    current_reason=reason,
                    previous_mode=self._last_mode,
                    previous_reason=self._last_reason,
                    **details_dict,
                )
                self._last_mode = mode_str
                self._last_reason = reason
                self._last_severity = severity
                self._last_alerted_at = now
                self._alert_count += 1
                return True
        elif reason_changed or severity_changed:
            should_alert = True
        elif heartbeat_due:
            should_alert = True
            is_heartbeat = True

        if should_alert and mode_str != TradeabilityMode.FULLY_TRADEABLE.value:
            self._alert_count += 1
            self._emit_alert(
                mode_str,
                reason,
                severity,
                is_heartbeat=is_heartbeat,
                **details_dict,
            )
            self._last_mode = mode_str
            self._last_reason = reason
            self._last_severity = severity
            self._last_alerted_at = now
            return True

        return False

    def _emit_alert(
        self,
        mode: str,
        reason: str,
        severity: str,
        *,
        is_heartbeat: bool,
        **details: Any,
    ) -> None:
        log_event = (
            "tradeability_heartbeat" if is_heartbeat else "tradeability_state_changed"
        )
        if severity.upper() == "CRITICAL":
            log.critical(
                log_event,
                mode=mode,
                reason=reason,
                severity=severity,
                alert_count=self._alert_count,
                **details,
            )
        elif severity.upper() == "INFO":
            log.info(
                log_event,
                mode=mode,
                reason=reason,
                severity=severity,
                alert_count=self._alert_count,
                **details,
            )
        else:
            log.warning(
                log_event,
                mode=mode,
                reason=reason,
                severity=severity,
                alert_count=self._alert_count,
                **details,
            )


class ReadinessStrategy(Protocol):
    """The compact strategy surface needed by readiness reporting."""

    def required_data(self) -> StrategyDataRequirement: ...

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
        if self.cutover_at.tzinfo is None or self.cutover_at.utcoffset() is None:
            raise ValueError("cutover_at must be timezone-aware")
        if not self.complete_symbols <= self.expected_symbols:
            raise ValueError("complete_symbols must be a subset of expected_symbols")

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
        alert_manager: TradeabilityAlertManager | None = None,
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
        if entry_universe_target_count is not None and entry_universe_target_count <= 0:
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
        self._exit_gate_open = True
        self._exit_gate_reason = "normal"
        self._unmanaged_risk_clear = True
        self._halt_active = False
        self._stream_states: dict[str, str] = {}
        self._alert_manager = alert_manager or TradeabilityAlertManager()
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
            checkpoint = strategy.checkpoint(include_market_state_buffers=False)
            if expected_symbols is not None:
                normalized_expected = _normalized_symbols(expected_symbols)
            elif self._warmup_expected_symbols:
                normalized_expected = self._warmup_expected_symbols
            else:
                normalized_expected = frozenset(checkpoint.warmup_buckets_by_symbol)
            warmup_by_symbol = checkpoint.warmup_buckets_by_symbol
            complete = frozenset(
                symbol
                for symbol in normalized_expected
                if int(warmup_by_symbol.get(symbol, 0)) >= required_buckets
            )
            if (
                self._warmup_required_buckets == required_buckets
                and self._warmup_expected_symbols == normalized_expected
                and self._warmup_complete_symbols == complete
            ):
                return
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
        if (
            self._entry_universe_count == entry_universe_count
            and self._entry_enabled == entry_enabled
            and self._entry_enabled_reason == entry_enabled_reason
        ):
            return
        self._entry_universe_count = entry_universe_count
        self._entry_enabled = entry_enabled
        self._entry_enabled_reason = entry_enabled_reason

        tradeability = self.current_tradeability()
        severity = (
            "CRITICAL" if self._halt_active or not self._exit_gate_open else "WARNING"
        )
        self._alert_manager.observe(
            mode=tradeability.mode,
            reason=tradeability.entry_gate_reason,
            severity=severity,
            details={"entry_universe_count": entry_universe_count},
        )
        self.publish()

    @property
    def alert_manager(self) -> TradeabilityAlertManager:
        return self._alert_manager

    def current_tradeability(self) -> TradeabilitySnapshot:
        return TradeabilitySnapshot.create(
            entry_gate_open=self._entry_enabled,
            entry_gate_reason=self._entry_enabled_reason,
            exit_gate_open=self._exit_gate_open,
            exit_gate_reason=self._exit_gate_reason,
            unmanaged_risk_clear=self._unmanaged_risk_clear,
            halt_active=self._halt_active,
        )

    def current_stream_readiness(self) -> StreamReadinessSnapshot:
        return StreamReadinessSnapshot.from_streams(self._stream_states)

    def update_stream_readiness(
        self,
        stream_name: str,
        state: str,
    ) -> None:
        """Record the availability of a specific stream source."""
        if not stream_name.strip():
            raise ValueError("stream_name must not be empty")
        state_str = getattr(state, "value", str(state)).upper()
        if self._stream_states.get(stream_name) != state_str:
            self._stream_states[stream_name] = state_str
            self.publish()

    def update_tradeability(
        self,
        *,
        entry_enabled: bool | None = None,
        entry_reason: str | None = None,
        exit_enabled: bool | None = None,
        exit_reason: str | None = None,
        unmanaged_risk_clear: bool | None = None,
        halt_active: bool | None = None,
    ) -> None:
        """Update layered tradeability gates and trigger edge alerting."""
        changed = False
        if entry_enabled is not None and entry_enabled != self._entry_enabled:
            self._entry_enabled = entry_enabled
            changed = True
        if entry_reason is not None and entry_reason != self._entry_enabled_reason:
            self._entry_enabled_reason = entry_reason
            changed = True
        if exit_enabled is not None and exit_enabled != self._exit_gate_open:
            self._exit_gate_open = exit_enabled
            changed = True
        if exit_reason is not None and exit_reason != self._exit_gate_reason:
            self._exit_gate_reason = exit_reason
            changed = True
        if (
            unmanaged_risk_clear is not None
            and unmanaged_risk_clear != self._unmanaged_risk_clear
        ):
            self._unmanaged_risk_clear = unmanaged_risk_clear
            changed = True
        if halt_active is not None and halt_active != self._halt_active:
            self._halt_active = halt_active
            changed = True

        tradeability = self.current_tradeability()
        severity = (
            "CRITICAL"
            if self._halt_active
            or not self._exit_gate_open
            or not self._unmanaged_risk_clear
            else "WARNING"
        )
        self._alert_manager.observe(
            mode=tradeability.mode,
            reason=(
                "halt_active"
                if self._halt_active
                else self._exit_gate_reason
                if not self._exit_gate_open
                else "unmanaged_risk_present"
                if not self._unmanaged_risk_clear
                else self._entry_enabled_reason
            ),
            severity=severity,
        )

        if changed:
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
        tradeability = self.current_tradeability()
        streams = self.current_stream_readiness()
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
            "latest_market_state_age_seconds": (self._latest_market_state_age_seconds),
            "entry_enabled": self._entry_enabled,
            "entry_enabled_reason": self._entry_enabled_reason,
            "tradeability": {
                "mode": tradeability.mode.value,
                "entry_gate_open": tradeability.entry_gate_open,
                "entry_gate_reason": tradeability.entry_gate_reason,
                "exit_gate_open": tradeability.exit_gate_open,
                "exit_gate_reason": tradeability.exit_gate_reason,
                "unmanaged_risk_clear": tradeability.unmanaged_risk_clear,
                "halt_active": tradeability.halt_active,
            },
            "stream_readiness": {
                "overall": streams.overall,
                "streams": streams.streams,
            },
        }
        try:
            self._health.write_readiness(payload)
        except Exception as error:
            log.warning(
                "live_readiness_publish_failed",
                error_type=type(error).__name__,
            )


def _normalized_symbols(symbols: Collection[str]) -> frozenset[str]:
    return frozenset(symbol.strip().upper() for symbol in symbols if symbol.strip())


def _isoformat(value: datetime | None) -> str | None:
    return None if value is None else value.astimezone(UTC).isoformat()


__all__ = [
    "LiveReadinessPublisher",
    "LiveWarmupStatus",
    "ReadinessStrategy",
    "StreamReadinessSnapshot",
    "TradeabilityAlertManager",
    "TradeabilityMode",
    "TradeabilitySnapshot",
]
