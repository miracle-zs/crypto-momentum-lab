import asyncio

from crypto_momentum_lab.live_rollout.market_loop import LiveDaemonResult
from crypto_momentum_lab.live_rollout.startup_resilience import (
    LiveStartupRetryableError,
    run_with_live_startup_backoff,
)


async def test_startup_backoff_stops_without_waiting_for_retry_delay() -> None:
    stop_requested = asyncio.Event()
    attempts = 0

    async def run_once() -> LiveDaemonResult:
        nonlocal attempts
        attempts += 1
        stop_requested.set()
        raise LiveStartupRetryableError(RuntimeError("temporary startup failure"))

    result = await run_with_live_startup_backoff(
        run_once,
        stop_requested=stop_requested,
    )

    assert attempts == 1
    assert result.halt_reason == "shutdown_requested"
