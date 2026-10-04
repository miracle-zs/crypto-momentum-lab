"""Control-plane callbacks shared by the live execution lanes.

The account-event consumer runs independently from the market loop.  This module owns the small amount of state they publish into
the entry gate and the context providers, so the application composition root
does not also become the owner of recovery semantics.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Protocol

import structlog

from crypto_momentum_lab.domain.account import ExecutionAccountStatus
from crypto_momentum_lab.domain.account.snapshot_models import (
    AccountSnapshot,
)
from crypto_momentum_lab.execution_account.hub import AccountEvent
from crypto_momentum_lab.live_rollout.telemetry_ports import ConsumerHealthSink

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
            "stream_reset",
            "stream reset",
            "replay is unavailable",
            "market_state_replaying",
            "market_state_rewarming",
            "continuity",
            "missing market-state bucket",
        )
    )


class LiveControlPlaneContextProvider(Protocol):
    """Context provider operations needed by control-plane publications."""

    def update_account_snapshot(
        self,
        snapshot: AccountSnapshot,
        *,
        sequence: int,
        account_state: ExecutionAccountStatus,
    ) -> None: ...

    def invalidate_account_snapshot(self) -> None: ...


Clock = Callable[[], datetime]


class LiveControlPlaneRuntime:
    """Publish account and market availability to live entry checks."""

    def __init__(
        self,
        *,
        session_id: str,
        context_provider: LiveControlPlaneContextProvider,
        market_state_available: bool,
        notify_market_state_gap: Callable[[str], None],
        refresh_entry_gate: Callable[[], None],
        telemetry: ConsumerHealthSink | None = None,
        clock: Clock = lambda: datetime.now(UTC),
        strategy_warmup_ready: bool = False,
    ) -> None:
        if not session_id.strip():
            raise ValueError("session_id must not be empty")
        if not isinstance(strategy_warmup_ready, bool):
            raise TypeError("strategy_warmup_ready must be a bool")
        self._session_id = session_id
        self._context_provider = context_provider
        self._notify_market_state_gap = notify_market_state_gap
        self._refresh_entry_gate = refresh_entry_gate
        self._telemetry = telemetry
        self._clock = clock
        self._account_snapshot_available = True
        self._market_state_available = market_state_available
        self._market_state_unavailable_reason = (
            "market_state_hub_ready"
            if market_state_available
            else "market_state_hub_connecting"
        )
        self._strategy_warmup_ready = strategy_warmup_ready
        self._strategy_warmup_reason = (
            "strategy_warmup_ready"
            if strategy_warmup_ready
            else "strategy_warmup_incomplete"
        )

    @property
    def account_snapshot_available(self) -> bool:
        return self._account_snapshot_available

    @property
    def market_state_available(self) -> bool:
        return self._market_state_available

    @property
    def market_state_unavailable_reason(self) -> str:
        return self._market_state_unavailable_reason

    @property
    def strategy_warmup_ready(self) -> bool:
        return self._strategy_warmup_ready

    @property
    def strategy_warmup_reason(self) -> str:
        return self._strategy_warmup_reason

    def set_strategy_warmup_ready(self, ready: bool, *, reason: str) -> None:
        """Publish strategy-buffer readiness as an independent entry gate."""

        if not isinstance(ready, bool):
            raise TypeError("ready must be a bool")
        if not reason.strip():
            raise ValueError("reason must not be empty")
        if (
            self._strategy_warmup_ready == ready
            and self._strategy_warmup_reason == reason
        ):
            return
        self._strategy_warmup_ready = ready
        self._strategy_warmup_reason = reason
        self._refresh_entry_gate()

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
            # Any transition away from an available stream invalidates the
            # rolling strategy state.  A reconnect is safe only after the
            # source has replayed the exact cursor or the worker has rebuilt
            # from durable history; both paths must remain fail-closed.
            if reason is not None and (was_available or is_consumer_lag_reason(reason)):
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



__all__ = [
    "LiveControlPlaneContextProvider",
    "LiveControlPlaneRuntime",
    "is_consumer_lag_reason",
]
