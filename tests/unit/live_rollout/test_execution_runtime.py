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
        event_repository=Mock(),
        submission_repository=Mock(),
        account_label="account-3",
        strategy_name="momentum",
        on_event=callback,
        on_before_submit=callback,
        on_exchange_request=callback,
        on_exchange_response=callback,
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
    args["exchange"].submit_order.assert_not_called()
    args["exchange"].query_order_by_client_id.assert_not_called()
    args["exchange"].cancel_order_by_client_id.assert_not_called()


@pytest.mark.parametrize("order_type", ["market", "limit"])
def test_live_policy_uses_entry_configuration_and_only_candle_exits(order_type):
    from datetime import UTC, datetime, timedelta
    from decimal import Decimal
    from types import SimpleNamespace

    from crypto_momentum_lab.domain.strategy.models import EntryType, StrategySide
    from crypto_momentum_lab.domain.strategy.position_exit import position_exit_reason

    policy = runtime.build_live_policy(
        config=SimpleNamespace(
            strategy=SimpleNamespace(entry_order_type=EntryType(order_type)),
            execution=SimpleNamespace(max_concurrency_per_symbol=2),
        ),
        target_notional=Decimal("100"),
        risk_config=SimpleNamespace(max_open_positions=500),
        strategy_name="orderflow_impulse",
    )
    assert policy.order_type is EntryType(order_type)
    assert (
        policy.target_notional == policy.sizing_model.target_notional == Decimal("100")
    )
    assert policy.max_open_positions == 500
    assert policy.max_concurrency_per_symbol == 2
    opened = datetime(2026, 10, 2, 3, 15, 3, tzinfo=UTC)
    assert (
        position_exit_reason(
            held_until=opened + timedelta(minutes=20, seconds=12),
            opened_at=opened,
            symbol="USUSDT",
            side=StrategySide.LONG,
            policy=policy.exit_policy,
            closed_candle=None,
        )
        is None
    )


@pytest.mark.parametrize("notional", ["0", "-100"])
def test_live_policy_rejects_non_positive_notional(notional):
    from decimal import Decimal
    from types import SimpleNamespace

    with pytest.raises(ValueError, match="target_notional must be positive"):
        runtime.build_live_policy(
            config=SimpleNamespace(),
            target_notional=Decimal(notional),
            risk_config=SimpleNamespace(),
            strategy_name="orderflow_impulse",
        )
