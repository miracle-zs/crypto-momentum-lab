from unittest.mock import AsyncMock, Mock

import pytest

from crypto_momentum_lab.persistence.postgres.order_plan_repository import (
    PostgresOrderPlanRepository,
)
from tests.unit.execution_account.orders.test_state_machine import _plan


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
