"""Fail-closed startup retry and lease recovery policy for live workers."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import structlog

import crypto_momentum_lab.live_rollout.runtime_errors as runtime_errors
from crypto_momentum_lab.execution_account.binance.client import (
    BinanceRateLimitError,
)
from crypto_momentum_lab.live_rollout.market_runtime_contracts import LiveDaemonResult

log = structlog.get_logger(__name__)

STARTUP_RETRY_INITIAL_SECONDS = 15
STARTUP_RETRY_MAX_SECONDS = 300


class LiveStartupRetryableError(RuntimeError):
    """Signal that the live worker may retry startup after a backoff."""

    def __init__(self, cause: Exception) -> None:
        super().__init__(str(cause))
        self.retry_after_seconds = (
            cause.retry_after_seconds if isinstance(cause, BinanceRateLimitError) else None
        )


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
        or runtime_errors.is_transient_runtime_error(error)
    )



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
    "run_with_live_startup_backoff",
]
