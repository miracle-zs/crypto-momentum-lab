"""Independent lease-heartbeat support for live trading workers.

The live market-state loop must not be responsible for keeping the trading
lease alive.  This module owns that small piece of lifecycle state so a slow
market event, order reconciliation, or database read cannot silently turn a
healthy worker into an expired lease.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from crypto_momentum_lab.domain.risk import TradingLease


class LeaseRenewalRepository(Protocol):
    async def renew_lease(
        self,
        *,
        lease_id: str,
        owner: str,
        expires_at: datetime,
    ) -> TradingLease: ...


@dataclass(frozen=True)
class LeaseHeartbeatConfig:
    """Timing policy for one live worker's lease heartbeat."""

    lease_ttl_seconds: int = 300
    renew_before_seconds: int = 120
    poll_interval_seconds: float = 15.0
    failure_backoff_initial_seconds: float = 1.0
    failure_backoff_max_seconds: float = 30.0

    def __post_init__(self) -> None:
        if self.lease_ttl_seconds <= 0:
            raise ValueError("lease_ttl_seconds must be positive")
        if self.renew_before_seconds <= 0:
            raise ValueError("renew_before_seconds must be positive")
        if self.renew_before_seconds >= self.lease_ttl_seconds:
            raise ValueError("renew_before_seconds must be less than lease_ttl_seconds")
        if self.poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive")
        if self.failure_backoff_initial_seconds <= 0:
            raise ValueError("failure_backoff_initial_seconds must be positive")
        if self.failure_backoff_max_seconds < self.failure_backoff_initial_seconds:
            raise ValueError(
                "failure_backoff_max_seconds must be at least the initial backoff"
            )


LeaseRenewedCallback = Callable[[TradingLease], None]
LeaseErrorCallback = Callable[[Exception], None]
LeaseRecovery = Callable[[], Awaitable[TradingLease | None]]
Clock = Callable[[], datetime]
Sleep = Callable[[float], Awaitable[None]]


class LiveLeaseHeartbeat:
    """Renew one already-acquired lease until the task is cancelled.

    The heartbeat deliberately does not acquire a missing lease.  Acquisition
    is a startup/operator decision; silently acquiring here could allow two
    workers to become active after a split-brain or deployment race.  Renewal
    failures are retried with bounded backoff while the current lease is still
    valid, and cancellation is always allowed to stop the worker promptly.
    """

    def __init__(
        self,
        *,
        repository: LeaseRenewalRepository,
        lease: TradingLease,
        owner: str,
        config: LeaseHeartbeatConfig | None = None,
        clock: Clock | None = None,
        sleep: Sleep | None = None,
        on_renewed: LeaseRenewedCallback | None = None,
        on_error: LeaseErrorCallback | None = None,
        recover: LeaseRecovery | None = None,
    ) -> None:
        self._repository = repository
        self._lease = lease
        self._owner = owner
        self._config = config or LeaseHeartbeatConfig()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._sleep = sleep or asyncio.sleep
        self._on_renewed = on_renewed
        self._on_error = on_error
        self._recover = recover

    @property
    def current_lease(self) -> TradingLease:
        return self._lease

    async def run(self) -> None:
        """Run until cancelled; unexpected renewal errors are retried."""

        consecutive_failures = 0
        while True:
            now = self._clock()
            renew_at = self._lease.expires_at - timedelta(
                seconds=self._config.renew_before_seconds
            )
            seconds_until_renew = (renew_at - now).total_seconds()
            if seconds_until_renew > 0:
                await self._sleep(
                    min(self._config.poll_interval_seconds, seconds_until_renew)
                )
                continue

            try:
                if self._lease.expires_at <= now and self._recover is not None:
                    recovered = await self._recover()
                    if recovered is None:
                        raise RuntimeError("live lease recovery is not available")
                    self._lease = recovered
                    consecutive_failures = 0
                    if self._on_renewed is not None:
                        self._on_renewed(recovered)
                    continue
                renewed = await self._repository.renew_lease(
                    lease_id=self._lease.lease_id,
                    owner=self._owner,
                    expires_at=now
                    + timedelta(seconds=self._config.lease_ttl_seconds),
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                consecutive_failures += 1
                if self._on_error is not None:
                    self._on_error(error)
                backoff = min(
                    self._config.failure_backoff_max_seconds,
                    self._config.failure_backoff_initial_seconds
                    * (2 ** (consecutive_failures - 1)),
                )
                await self._sleep(backoff)
                continue

            self._lease = renewed
            consecutive_failures = 0
            if self._on_renewed is not None:
                self._on_renewed(renewed)
