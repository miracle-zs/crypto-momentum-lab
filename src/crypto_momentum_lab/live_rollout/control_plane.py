"""Control-plane callbacks shared by the live execution lanes.

The account-event consumer and lease heartbeat run independently from the
market loop.  This module owns the small amount of state they publish into
the entry gate and the context providers, so the application composition root
does not also become the owner of recovery semantics.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Protocol

import structlog

from crypto_momentum_lab.domain.account import ExecutionAccountStatus
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.risk import TradingLease
from crypto_momentum_lab.execution_account.hub import AccountEvent
from crypto_momentum_lab.execution_account.sync import AccountSnapshot
from crypto_momentum_lab.live_rollout.context import (
    LiveContextProvider,
)
from crypto_momentum_lab.live_rollout.gates import LiveGateContext
from crypto_momentum_lab.live_rollout.telemetry import LiveTelemetrySink

log = structlog.get_logger(__name__)


def is_consumer_lag_reason(reason: str | None) -> bool:
    """Classify stream reasons that indicate a consumer fell behind."""

    if reason is None:
        return False
    normalized = reason.lower()
    return any(
        marker in normalized
        for marker in (
            "lag",
            "overflow",
            "sequence_gap",
            "sequencegap",
            "replay_unavailable",
        )
    )


class LiveControlPlaneContextProvider(LiveContextProvider, Protocol):
    """Context provider operations needed by control-plane publications."""

    def update_account_snapshot(
        self,
        snapshot: AccountSnapshot,
        *,
        sequence: int,
        account_state: ExecutionAccountStatus,
    ) -> None: ...

    def invalidate_account_snapshot(self) -> None: ...

    def update_lease(self, lease: TradingLease) -> None: ...

    def invalidate_cache(self) -> None: ...


class LatestMarketStateSource(Protocol):
    """Read the latest market state required for lease recovery."""

    def for_symbols(
        self,
        symbols: tuple[str, ...],
    ) -> tuple[MarketState15s, ...]: ...


LeaseReacquirer = Callable[
    [LiveGateContext],
    Awaitable[TradingLease | None],
]
Clock = Callable[[], datetime]


class LiveControlPlaneRuntime:
    """Publish account and lease control-plane state to live decision gates."""

    def __init__(
        self,
        *,
        session_id: str,
        context_provider: LiveControlPlaneContextProvider,
        heartbeat_context_provider: LiveControlPlaneContextProvider,
        latest_market_states: LatestMarketStateSource,
        reacquire_lease: LeaseReacquirer,
        market_state_available: bool,
        notify_market_state_gap: Callable[[str], None],
        refresh_entry_gate: Callable[[], None],
        mark_database_ok: Callable[[], None],
        telemetry: LiveTelemetrySink | None = None,
        clock: Clock | None = None,
    ) -> None:
        if not session_id.strip():
            raise ValueError("session_id must not be empty")
        self._session_id = session_id
        self._context_provider = context_provider
        self._heartbeat_context_provider = heartbeat_context_provider
        self._latest_market_states = latest_market_states
        self._reacquire_lease = reacquire_lease
        self._notify_market_state_gap = notify_market_state_gap
        self._refresh_entry_gate = refresh_entry_gate
        self._mark_database_ok = mark_database_ok
        self._telemetry = telemetry
        self._clock = clock or (lambda: datetime.now(tz=UTC))
        self._account_snapshot_available = True
        self._lease_heartbeat_degraded = False
        self._market_state_available = market_state_available
        self._market_state_unavailable_reason = (
            "market_state_hub_ready"
            if market_state_available
            else "market_state_hub_connecting"
        )

    @property
    def account_snapshot_available(self) -> bool:
        return self._account_snapshot_available

    @property
    def lease_heartbeat_degraded(self) -> bool:
        return self._lease_heartbeat_degraded

    @property
    def market_state_available(self) -> bool:
        return self._market_state_available

    @property
    def market_state_unavailable_reason(self) -> str:
        return self._market_state_unavailable_reason

    def on_market_connection_change(
        self,
        available: bool,
        reason: str | None,
    ) -> None:
        """Publish market-source liveness and reset strategy state on gaps."""

        was_available = self._market_state_available
        self._market_state_available = available
        if self._telemetry is not None:
            self._telemetry.consumer_health(
                consumer="market_state_hub",
                available=available,
                occurred_at=self._clock(),
                reason=reason,
                recovery=available and not was_available,
                lag=is_consumer_lag_reason(reason),
            )
        if available:
            self._market_state_unavailable_reason = "market_state_hub_ready"
        else:
            self._market_state_unavailable_reason = (
                reason or "market_state_hub_unavailable"
            )
            if reason is not None and (
                reason.startswith("market_state_consumer_lagged")
                or (
                    reason.startswith("MarketStateHubSequenceGap")
                    and "consumer queue overflowed" not in reason
                )
            ):
                self._notify_market_state_gap(reason)
        self._refresh_entry_gate()

    def on_account_snapshot(self, event: AccountEvent) -> None:
        """Publish a complete account projection and reopen entry admission."""

        was_available = self._account_snapshot_available
        snapshot = event.account_snapshot
        account_state = event.account_state
        if snapshot is None or account_state is None:
            # Older Hub publishers may still send notification-only events
            # during a rolling deploy.  Keep the database bootstrap view until
            # a complete snapshot is received.
            return
        self._context_provider.update_account_snapshot(
            snapshot,
            sequence=event.sequence,
            account_state=account_state,
        )
        self._heartbeat_context_provider.update_account_snapshot(
            snapshot,
            sequence=event.sequence,
            account_state=account_state,
        )
        self._account_snapshot_available = True
        if self._telemetry is not None and event.snapshot_kind == "full":
            self._telemetry.consumer_health(
                consumer="account_event_hub",
                available=True,
                occurred_at=event.received_at,
                reason="full_snapshot_received",
                recovery=not was_available,
                sequence=event.sequence,
            )
        self._refresh_entry_gate()

    def on_account_snapshot_recovery(self, reason: str) -> None:
        """Fail closed while the account-event consumer rebuilds its snapshot."""

        self._account_snapshot_available = False
        self._context_provider.invalidate_account_snapshot()
        self._heartbeat_context_provider.invalidate_account_snapshot()
        if self._telemetry is not None:
            self._telemetry.consumer_health(
                consumer="account_event_hub",
                available=False,
                occurred_at=self._clock(),
                reason=reason,
                lag=is_consumer_lag_reason(reason),
            )
        self._refresh_entry_gate()
        log.warning(
            "live_account_snapshot_recovery_requested",
            reason=reason,
            session_id=self._session_id,
        )

    async def recover_live_lease(self) -> TradingLease | None:
        """Rebuild context from the latest market state before reacquiring."""

        states = self._latest_market_states.for_symbols(())
        if not states:
            return None
        latest_state = states[-1]
        self._heartbeat_context_provider.invalidate_cache()
        recovery_context = await self._heartbeat_context_provider(latest_state)
        return await self._reacquire_lease(recovery_context.gate_context)

    def on_lease_renewed(self, lease: TradingLease) -> None:
        """Publish a committed lease renewal to both live context readers."""

        self._lease_heartbeat_degraded = False
        self._context_provider.update_lease(lease)
        self._heartbeat_context_provider.update_lease(lease)
        self._refresh_entry_gate()
        self._mark_database_ok()
        log.info(
            "live_lease_renewed",
            session_id=self._session_id,
            lease_id=lease.lease_id,
            lease_expires_at=lease.expires_at.isoformat(),
        )

    def on_lease_error(self, error: Exception) -> None:
        """Fail closed after a heartbeat renewal failure."""

        self._lease_heartbeat_degraded = True
        self._refresh_entry_gate()
        log.warning(
            "live_lease_renewal_failed",
            session_id=self._session_id,
            error_type=type(error).__name__,
        )


__all__ = [
    "LatestMarketStateSource",
    "LiveControlPlaneContextProvider",
    "LiveControlPlaneRuntime",
    "is_consumer_lag_reason",
]
