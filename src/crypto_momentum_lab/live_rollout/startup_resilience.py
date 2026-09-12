"""Fail-closed startup retry and lease recovery policy for live workers."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any
from uuid import uuid4

import structlog
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.live_rollout import LiveSessionState
from crypto_momentum_lab.domain.risk import TradingLease, TradingLeaseState
from crypto_momentum_lab.execution_account.binance import BinanceRateLimitError
from crypto_momentum_lab.live_rollout.gates import (
    LiveGateContext,
    evaluate_live_gate,
)
from crypto_momentum_lab.live_rollout.market_loop import LiveDaemonResult
from crypto_momentum_lab.persistence.postgres.models import LiveSessionTransitionRow
from crypto_momentum_lab.persistence.postgres.risk_repository import (
    PostgresRiskRepository,
)

log = structlog.get_logger(__name__)

STARTUP_RETRY_INITIAL_SECONDS = 15
STARTUP_RETRY_MAX_SECONDS = 300


class LiveStartupRetryableError(RuntimeError):
    """Signal that the live worker may retry startup after a backoff."""

    def __init__(self, cause: Exception) -> None:
        super().__init__(str(cause))
        self.retry_after_seconds = getattr(cause, "retry_after_seconds", None)


async def run_with_live_startup_backoff(
    run_once: Callable[[], Awaitable[LiveDaemonResult]],
    *,
    stop_requested: asyncio.Event | None = None,
) -> LiveDaemonResult:
    """Retry only failures explicitly wrapped as startup-retryable."""

    consecutive_failures = 0
    while True:
        if stop_requested is not None and stop_requested.is_set():
            log.info("live_startup_stopped_before_attempt")
            return LiveDaemonResult(0, 0, 0, "shutdown_requested", None)
        try:
            return await run_once()
        except LiveStartupRetryableError as error:
            consecutive_failures += 1
            delay = live_startup_retry_delay(
                consecutive_failures,
                retry_after_seconds=error.retry_after_seconds,
            )
            log.warning(
                "live_startup_retry_scheduled",
                attempt=consecutive_failures,
                delay_seconds=delay,
                error_type=type(error.__cause__ or error).__name__,
                error=str(error),
            )
            if stop_requested is None:
                await asyncio.sleep(delay)
            else:
                try:
                    await asyncio.wait_for(stop_requested.wait(), timeout=delay)
                except TimeoutError:
                    continue
                log.info("live_startup_stopped_during_backoff")
                return LiveDaemonResult(0, 0, 0, "shutdown_requested", None)


def is_retryable_live_startup_error(error: Exception) -> bool:
    """Classify only transient startup failures as eligible for retry."""

    return (
        isinstance(error, BinanceRateLimitError)
        or (
            isinstance(error, RuntimeError)
            and str(error).startswith("live gate blocked:")
        )
        or isinstance(error, (SQLAlchemyError, TimeoutError, ConnectionError, OSError))
    )


def should_auto_reacquire_live_lease(
    *,
    lease_present: bool,
    session_was_live_enabled: bool,
    draining: bool,
    gate_reasons: tuple[str, ...],
) -> bool:
    """Allow recovery only for an already-enabled, non-draining session."""

    return (
        not lease_present
        and session_was_live_enabled
        and not draining
        and gate_reasons == ("missing_active_lease",)
    )


async def session_was_live_enabled(
    factory: async_sessionmaker[AsyncSession],
    session_id: str,
) -> bool:
    """Read the latest durable transition before attempting lease recovery."""

    async with factory() as database_session:
        latest_state = await database_session.scalar(
            select(LiveSessionTransitionRow.state)
            .where(
                LiveSessionTransitionRow.session_id == session_id,
                LiveSessionTransitionRow.state.not_in(
                    (
                        LiveSessionState.PREFLIGHT.value,
                        LiveSessionState.SHADOW_PREFLIGHT.value,
                    )
                ),
            )
            .order_by(LiveSessionTransitionRow.occurred_at.desc())
            .limit(1)
        )
    return latest_state == LiveSessionState.LIVE_ENABLED.value


async def maybe_auto_reacquire_live_lease(
    *,
    factory: async_sessionmaker[AsyncSession],
    risk_repository: PostgresRiskRepository,
    gate_context: LiveGateContext,
    session_id: str,
    draining: bool,
    lease_ttl_seconds: int,
) -> TradingLease | None:
    """Recover one lost lease without bypassing the live gate."""

    gate = evaluate_live_gate(gate_context)
    if gate.approved or gate_context.active_lease is not None:
        return gate_context.active_lease
    if not await session_was_live_enabled(factory, session_id):
        return None
    if not should_auto_reacquire_live_lease(
        lease_present=False,
        session_was_live_enabled=True,
        draining=draining,
        gate_reasons=gate.reasons,
    ):
        return None
    now = gate_context.now
    lease = TradingLease(
        lease_id=f"lease-{uuid4()}",
        environment="live",
        account_label=gate_context.account_label,
        strategy_name=gate_context.strategy_name,
        owner=gate_context.required_lease_owner,
        code_generation=gate_context.git_commit_hash,
        state=TradingLeaseState.ACTIVE,
        acquired_at=now,
        expires_at=now + timedelta(seconds=lease_ttl_seconds),
    )
    await risk_repository.acquire_lease(lease)
    log.info(
        "live_lease_auto_reacquired",
        account_label=lease.account_label,
        session_id=session_id,
        lease_id=lease.lease_id,
        lease_expires_at=lease.expires_at.isoformat(),
    )
    return lease


def live_startup_retry_delay(
    attempt: int,
    *,
    retry_after_seconds: Any,
) -> float:
    """Return bounded exponential backoff honoring exchange retry-after."""

    if attempt <= 0:
        raise ValueError("attempt must be positive")
    exponent = min(attempt - 1, 30)
    delay = min(
        STARTUP_RETRY_INITIAL_SECONDS * (2**exponent),
        STARTUP_RETRY_MAX_SECONDS,
    )
    if isinstance(retry_after_seconds, int | float) and not isinstance(
        retry_after_seconds, bool
    ):
        if retry_after_seconds >= 0:
            delay = max(delay, float(retry_after_seconds))
    return float(min(delay, STARTUP_RETRY_MAX_SECONDS))


__all__ = [
    "LiveStartupRetryableError",
    "is_retryable_live_startup_error",
    "live_startup_retry_delay",
    "maybe_auto_reacquire_live_lease",
    "run_with_live_startup_backoff",
    "session_was_live_enabled",
    "should_auto_reacquire_live_lease",
]
