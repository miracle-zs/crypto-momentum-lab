from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from crypto_momentum_lab.domain.execution import ExchangeOrderFill, ExchangeOrderState
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionStateMachine,
    SubmitPolicy,
)
from tests.unit.execution_account.orders.test_state_machine import (
    NOW,
    FakeExchange,
    _plan,
    _snapshot,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("inserted", [False, True])
async def test_execution_uses_distinct_event_port_and_notifies_only_inserted(inserted):
    plan = _plan()
    fill = ExchangeOrderFill(
        fill_id="fill",
        client_order_id=plan.client_order_id,
        exchange_trade_id="trade",
        price=Decimal("30000"),
        quantity=Decimal("0.003"),
        fee=Decimal("0"),
        fee_asset="USDT",
        filled_at=NOW,
        details={},
    )
    orders = SimpleNamespace(
        save_planned_order=AsyncMock(),
        save_shadow_suppression=AsyncMock(),
    )
    events = SimpleNamespace(
        append_order_event=AsyncMock(return_value=inserted),
        save_fill=AsyncMock(return_value=True),
    )
    callback = AsyncMock()
    machine = OrderExecutionStateMachine(
        exchange=FakeExchange(
            submit_result=_snapshot(ExchangeOrderState.FILLED, fills=(fill,))
        ),
        repository=orders,
        event_repository=events,
        submit_policy=SubmitPolicy.LIVE_SUBMIT,
        live_submit_enabled=True,
        clock=lambda: NOW,
        on_event=callback,
    )
    result = await machine.execute_approved_intent(plan)
    assert result.state is ExchangeOrderState.FILLED
    orders.save_planned_order.assert_awaited_once_with(plan)
    events.save_fill.assert_awaited_once_with(fill)
    assert events.append_order_event.await_count > 0
    assert callback.await_count == (
        events.append_order_event.await_count if inserted else 0
    )
