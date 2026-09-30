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
        account_label="account-3",
        strategy_name="momentum",
        callbacks=runtime.LiveExecutionCallbacks(
            on_event=callback,
            on_before_submit=callback,
            on_before_exchange_submit=callback,
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
        assert result.coordinator._execution_book is result.book
        call = constructor.call_args.kwargs
        assert call["environment"] == "live"
        assert call["domain_coordinator"] is result.book.coordinator
        backend = call["backend"]
        assert backend._submit_policy is runtime.SubmitPolicy.LIVE_SUBMIT
        assert backend._live_submit_enabled is True
        assert backend._lock is None
        assert backend._clock().utcoffset().total_seconds() == 0
        assert backend._exchange is args["exchange"]
        assert backend._repository is args["order_repository"]
        assert (
            backend._on_before_exchange_submit
            is args["callbacks"].on_before_exchange_submit
        )
        assert backend._on_event is args["callbacks"].on_event
        assert backend._on_before_submit is args["callbacks"].on_before_submit
        assert backend._on_exchange_request is args["callbacks"].on_exchange_request
        assert backend._on_exchange_response is args["callbacks"].on_exchange_response
        uow = result.book._execution_unit_of_work
        assert uow._session_factory is args["sessions"]
        assert uow._command_repository is result.book._command_repo
        assert uow._reservation_repository is call["reservation_repository"]
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
