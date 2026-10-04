import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.live_rollout import execution_runtime as runtime


def arguments():
    callback = AsyncMock()
    return dict(
        sessions=async_sessionmaker(),
        exchange=Mock(),
        order_repository=Mock(),
        event_repository=Mock(),
        account_label="account-3",
        strategy_name="momentum",
        callbacks=runtime.LiveExecutionCallbacks(
            on_event=callback,
            on_before_submit=callback,
            on_exchange_request=callback,
            on_exchange_response=callback,
        ),
    )


@pytest.mark.asyncio
async def test_recovery_completes_before_submission_coordinator_exists(monkeypatch):
    entered, release = asyncio.Event(), asyncio.Event()
    restored_accounts = []

    async def restore(book, *, account_label):
        restored_accounts.append(account_label)
        entered.set()
        await release.wait()

    monkeypatch.setattr(runtime.ExecutionBook, "restore", restore)
    constructor = Mock(wraps=runtime.OrderExecutionCoordinator)
    monkeypatch.setattr(runtime, "OrderExecutionCoordinator", constructor)
    args = arguments()
    task = asyncio.create_task(runtime.build_live_execution_runtime(**args))
    await asyncio.wait_for(entered.wait(), timeout=1)
    try:
        constructor.assert_not_called()
        assert not task.done()
    finally:
        release.set()
    result = await task
    try:
        assert restored_accounts == ["account-3"]
    finally:
        await result.coordinator.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error", [RuntimeError("recovery failed"), asyncio.CancelledError()]
)
async def test_failed_or_cancelled_restore_never_exposes_submission(monkeypatch, error):
    monkeypatch.setattr(runtime.ExecutionBook, "restore", AsyncMock(side_effect=error))
    constructor = Mock()
    monkeypatch.setattr(runtime, "OrderExecutionCoordinator", constructor)
    args = arguments()
    with pytest.raises(type(error)):
        await runtime.build_live_execution_runtime(**args)
    constructor.assert_not_called()
    args["exchange"].assert_not_called()
    assert not args["exchange"].method_calls
