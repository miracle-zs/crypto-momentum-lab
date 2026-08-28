import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from crypto_momentum_lab.domain.risk import TradingLease, TradingLeaseState
from crypto_momentum_lab.live_rollout.lease import (
    LeaseHeartbeatConfig,
    LiveLeaseHeartbeat,
)


def _lease(*, expires_at: datetime) -> TradingLease:
    acquired_at = expires_at - timedelta(seconds=300)
    return TradingLease(
        lease_id="lease-1",
        environment="live",
        account_label="primary",
        strategy_name="orderflow_impulse",
        owner="live-worker",
        state=TradingLeaseState.ACTIVE,
        acquired_at=acquired_at,
        expires_at=expires_at,
    )


@pytest.mark.asyncio
async def test_heartbeat_renews_before_expiration_and_publishes_lease() -> None:
    now = datetime(2026, 8, 22, 1, 0, tzinfo=UTC)
    initial = _lease(expires_at=now + timedelta(seconds=100))
    renewed: list[TradingLease] = []
    calls: list[tuple[str, str, datetime]] = []

    class Repository:
        async def renew_lease(
            self,
            *,
            lease_id: str,
            owner: str,
            expires_at: datetime,
        ) -> TradingLease:
            calls.append((lease_id, owner, expires_at))
            return replace(initial, expires_at=expires_at)

    async def stop_after_next_sleep(_: float) -> None:
        raise asyncio.CancelledError

    heartbeat = LiveLeaseHeartbeat(
        repository=Repository(),
        lease=initial,
        owner="live-worker",
        config=LeaseHeartbeatConfig(
            lease_ttl_seconds=300,
            renew_before_seconds=120,
            poll_interval_seconds=15,
        ),
        clock=lambda: now,
        sleep=stop_after_next_sleep,
        on_renewed=renewed.append,
    )

    with pytest.raises(asyncio.CancelledError):
        await heartbeat.run()

    assert len(calls) == 1
    assert calls[0][0:2] == ("lease-1", "live-worker")
    assert calls[0][2] == now + timedelta(seconds=300)
    assert renewed == [heartbeat.current_lease]


@pytest.mark.asyncio
async def test_heartbeat_retries_transient_errors_with_bounded_backoff() -> None:
    now = datetime(2026, 8, 22, 1, 0, tzinfo=UTC)
    initial = _lease(expires_at=now + timedelta(seconds=100))
    sleeps: list[float] = []
    errors: list[Exception] = []
    attempts = 0

    class Repository:
        async def renew_lease(
            self,
            *,
            lease_id: str,
            owner: str,
            expires_at: datetime,
        ) -> TradingLease:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise TimeoutError("database temporarily unavailable")
            return replace(initial, expires_at=expires_at)

    async def controlled_sleep(delay: float) -> None:
        sleeps.append(delay)
        if len(sleeps) == 2:
            raise asyncio.CancelledError

    heartbeat = LiveLeaseHeartbeat(
        repository=Repository(),
        lease=initial,
        owner="live-worker",
        config=LeaseHeartbeatConfig(
            lease_ttl_seconds=300,
            renew_before_seconds=120,
            poll_interval_seconds=15,
            failure_backoff_initial_seconds=1,
            failure_backoff_max_seconds=4,
        ),
        clock=lambda: now,
        sleep=controlled_sleep,
        on_error=errors.append,
    )

    with pytest.raises(asyncio.CancelledError):
        await heartbeat.run()

    assert attempts == 2
    assert sleeps == [1, 15]
    assert [type(error) for error in errors] == [TimeoutError]


@pytest.mark.asyncio
async def test_heartbeat_recovers_an_expired_lease_without_stopping() -> None:
    now = datetime(2026, 8, 22, 1, 0, tzinfo=UTC)
    initial = _lease(expires_at=now - timedelta(seconds=1))
    recovered = replace(
        initial,
        lease_id="lease-2",
        acquired_at=now,
        expires_at=now + timedelta(seconds=300),
    )
    renewed: list[TradingLease] = []
    recovery_calls = 0

    async def recover() -> TradingLease:
        nonlocal recovery_calls
        recovery_calls += 1
        return recovered

    async def stop_after_recovery(_: float) -> None:
        raise asyncio.CancelledError

    heartbeat = LiveLeaseHeartbeat(
        repository=object(),  # type: ignore[arg-type]
        lease=initial,
        owner="live-worker",
        clock=lambda: now,
        sleep=stop_after_recovery,
        on_renewed=renewed.append,
        recover=recover,
    )

    with pytest.raises(asyncio.CancelledError):
        await heartbeat.run()

    assert recovery_calls == 1
    assert heartbeat.current_lease == recovered
    assert renewed == [recovered]
