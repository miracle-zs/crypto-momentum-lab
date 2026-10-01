from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from crypto_momentum_lab.domain.execution.order_submission import (
    OrderPreSubmissionError,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionStateMachine,
    SubmitPolicy,
)
from crypto_momentum_lab.persistence.postgres.order_plan_repository import (
    PostgresOrderPlanRepository,
)
from tests.unit.execution_account.orders.test_state_machine import NOW, _plan


def ports():
    return (
        SimpleNamespace(save_planned_order=AsyncMock()),
        SimpleNamespace(append_order_event=AsyncMock(), save_fill=AsyncMock()),
        SimpleNamespace(save_shadow_suppression=AsyncMock()),
        SimpleNamespace(submit_order=AsyncMock()),
    )


def test_shadow_policy_requires_explicit_suppression_port_before_execution():
    plans, events, _, exchange = ports()
    with pytest.raises(ValueError, match="suppression repository"):
        OrderExecutionStateMachine(
            exchange=exchange,
            repository=plans,
            event_repository=events,
            submit_policy=SubmitPolicy.SHADOW_SUPPRESS,
            live_submit_enabled=False,
        )
    plans.save_planned_order.assert_not_called()
    exchange.submit_order.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [False, True])
async def test_shadow_uses_distinct_suppression_port_and_never_calls_exchange(failure):
    plans, events, shadow, exchange = ports()
    if failure:
        shadow.save_shadow_suppression.side_effect = RuntimeError("suppression failed")
    machine = OrderExecutionStateMachine(
        exchange=exchange,
        repository=plans,
        event_repository=events,
        shadow_repository=shadow,
        submit_policy=SubmitPolicy.SHADOW_SUPPRESS,
        live_submit_enabled=False,
        clock=lambda: NOW,
    )
    if failure:
        with pytest.raises(OrderPreSubmissionError, match="suppression failed"):
            await machine.submit(_plan())
    else:
        result = await machine.submit(_plan())
        assert result.suppressed is True
    plans.save_planned_order.assert_awaited_once_with(_plan())
    shadow.save_shadow_suppression.assert_awaited_once()
    if failure:
        events.append_order_event.assert_not_awaited()
    else:
        events.append_order_event.assert_awaited_once()
    events.save_fill.assert_not_awaited()
    exchange.submit_order.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [False, True])
async def test_plan_and_intent_state_write_share_transaction(failure):
    session = AsyncMock()
    session.__aenter__.return_value = session
    transaction = AsyncMock()
    session.begin = Mock(return_value=transaction)
    factory = Mock(return_value=session)
    repository = PostgresOrderPlanRepository(factory)
    error = RuntimeError("intent state write failed")
    if failure:
        session.execute.side_effect = [None, error]
        with pytest.raises(RuntimeError, match="intent state write failed"):
            await repository.save_planned_order(_plan())
        assert transaction.__aexit__.await_args.args[1] is error
    else:
        await repository.save_planned_order(_plan())
        transaction.__aexit__.assert_awaited_once_with(None, None, None)
    assert [call.args[0].table.name for call in session.execute.await_args_list] == [
        "exchange_orders",
        "order_intents",
    ]
    factory.assert_called_once_with()
    session.begin.assert_called_once_with()
    session.commit.assert_not_awaited()


def test_plan_repository_exposes_only_plan_write():
    assert hasattr(PostgresOrderPlanRepository, "save_planned_order")
    assert not hasattr(PostgresOrderPlanRepository, "save_shadow_suppression")
