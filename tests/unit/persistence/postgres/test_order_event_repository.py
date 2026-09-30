from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderFill,
    ExchangeOrderState,
)
from crypto_momentum_lab.persistence.postgres.order_event_repository import (
    PostgresOrderEventRepository,
)
from crypto_momentum_lab.persistence.postgres.order_plan_repository import (
    PostgresOrderPlanRepository,
)

NOW = datetime(2026, 9, 30, tzinfo=UTC)


def database(scalar_values, *, rowcount=1):
    session = AsyncMock()
    session.__aenter__.return_value = session
    transaction = AsyncMock()
    session.begin = Mock(return_value=transaction)
    session.scalar.side_effect = scalar_values
    session.execute.return_value = SimpleNamespace(rowcount=rowcount)
    factory = Mock(return_value=session)
    return PostgresOrderEventRepository(factory), session, transaction, factory


def event(state=ExchangeOrderState.FILLED):
    return ExchangeOrderEvent(
        event_id="event",
        client_order_id="order",
        state=state,
        occurred_at=NOW,
        exchange_order_id="exchange-order",
        details={"executed_quantity": "2"},
    )


@pytest.mark.asyncio
async def test_duplicate_event_does_not_change_order_or_release_claims():
    repository, session, transaction, factory = database([None])
    assert await repository.append_order_event(event()) is False
    session.execute.assert_not_awaited()
    factory.assert_called_once_with()
    transaction.__aexit__.assert_awaited_once_with(None, None, None)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state,current_state,rowcount,expected_writes",
    [
        (ExchangeOrderState.FILLED, "filled", 1, 4),
        (ExchangeOrderState.FILLED, "filled", 0, 1),
        (ExchangeOrderState.ACKNOWLEDGED, "acknowledged", 1, 2),
        (ExchangeOrderState.ACKNOWLEDGED, "filled", 1, 3),
    ],
)
async def test_claim_release_uses_durable_order_state_in_same_transaction(
    state,
    current_state,
    rowcount,
    expected_writes,
):
    repository, session, transaction, factory = database(
        ["event", "intent", current_state],
        rowcount=rowcount,
    )
    assert await repository.append_order_event(event(state)) is True
    assert session.execute.await_count == expected_writes
    writes = session.execute.await_args_list
    assert writes[0].args[0].table.name == "exchange_orders"
    if expected_writes >= 3:
        assert writes[-1].args[0].table.name == "live_exposure_claims"
        assert writes[-2].args[0].table.name == "exit_episode_reservations"
    factory.assert_called_once_with()
    session.begin.assert_called_once_with()
    transaction.__aexit__.assert_awaited_once_with(None, None, None)
    session.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_release_failure_propagates_to_order_event_transaction():
    repository, session, transaction, _ = database(["event", "intent", "filled"])
    failure = RuntimeError("claim release failed")
    session.execute.side_effect = [SimpleNamespace(rowcount=1), None, failure]
    with pytest.raises(RuntimeError, match="claim release failed"):
        await repository.append_order_event(event())
    assert transaction.__aexit__.await_args.args[1] is failure


@pytest.mark.asyncio
@pytest.mark.parametrize("inserted", [None, "fill"])
async def test_fill_conflict_result_is_returned_without_updating_order(inserted):
    repository, session, transaction, factory = database([inserted])
    fill = ExchangeOrderFill(
        fill_id="fill",
        client_order_id="order",
        exchange_trade_id="trade",
        price=Decimal("100"),
        quantity=Decimal("2"),
        fee=Decimal("0"),
        fee_asset="USDT",
        filled_at=NOW,
        details={},
    )
    assert await repository.save_fill(fill) is (inserted is not None)
    session.execute.assert_not_awaited()
    assert session.scalar.await_args.args[0].table.name == "exchange_fills"
    factory.assert_called_once_with()
    transaction.__aexit__.assert_awaited_once_with(None, None, None)


def test_order_repository_no_longer_owns_events_or_fills():
    assert not hasattr(PostgresOrderPlanRepository, "append_order_event")
    assert not hasattr(PostgresOrderPlanRepository, "save_fill")
