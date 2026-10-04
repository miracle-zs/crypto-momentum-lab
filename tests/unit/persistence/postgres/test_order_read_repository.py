from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    FuturesPositionSide,
)
from crypto_momentum_lab.persistence.postgres.order_read_repository import (
    PostgresOrderReadRepository,
)

NOW = datetime(2026, 9, 30, tzinfo=UTC)


def row(executed_quantity=Decimal("0.5")):
    return SimpleNamespace(
        intent_id="intent",
        run_id="run",
        client_order_id="order",
        exchange_order_id="exchange",
        symbol="BTCUSDT",
        side="SELL",
        order_type="LIMIT",
        quantity=Decimal("2"),
        price=Decimal("100"),
        reduce_only=True,
        position_side="SHORT",
        state="acknowledged",
        created_at=NOW,
        updated_at=NOW,
        time_in_force="GTC",
        expires_at=None,
        executed_quantity=executed_quantity,
    )


def database():
    session = AsyncMock()
    session.__aenter__.return_value = session
    session.execute.return_value = Mock(all=Mock(return_value=[]))
    factory = Mock(return_value=session)
    return PostgresOrderReadRepository(factory), session


@pytest.mark.asyncio
@pytest.mark.parametrize("quantity", [None, Decimal("0.5")])
async def test_load_preserves_durable_order_fields_and_quantity_baseline(quantity):
    repository, session = database()
    session.scalar.return_value = row(quantity)
    result = await repository.load_order("order")
    assert result.plan.intent_id == "intent"
    assert result.plan.client_order_id == "order"
    assert result.plan.position_side is FuturesPositionSide.SHORT
    assert result.plan.reduce_only is True
    assert result.plan.time_in_force == "GTC"
    assert result.plan.quantized is True
    assert result.state is ExchangeOrderState.ACKNOWLEDGED
    assert result.exchange_order_id == "exchange"
    assert result.executed_quantity == (quantity or Decimal("0"))
    query = session.scalar.await_args.args[0]
    assert query.compile().params["client_order_id_1"] == "order"
    session.begin.assert_not_called()
    session.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_missing_order_returns_none_without_write_transaction():
    repository, session = database()
    session.scalar.return_value = None
    assert await repository.load_order("missing") is None
    session.begin.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("run_id", [None, "run"])
async def test_unresolved_query_keeps_terminal_filter_scope_and_stable_order(run_id):
    repository, session = database()
    session.scalars.return_value = Mock(all=Mock(return_value=[row()]))
    result = await repository.load_unresolved_orders(run_id)
    assert len(result) == 1
    query = session.scalars.await_args.args[0]
    compiled = query.compile()
    assert ExchangeOrderState.FILLED.value in compiled.params["state_1"]
    assert ("run_id_1" in compiled.params) is (run_id is not None)
    if run_id is not None:
        assert compiled.params["run_id_1"] == run_id
    assert (
        "ORDER BY exchange_orders.updated_at, exchange_orders.client_order_id"
        in str(compiled)
    )
    session.begin.assert_not_called()


@pytest.mark.asyncio
async def test_read_failure_is_not_reported_as_empty_order_set():
    repository, session = database()
    session.scalars.side_effect = RuntimeError("database unavailable")
    with pytest.raises(RuntimeError, match="database unavailable"):
        await repository.load_unresolved_orders("run")


@pytest.mark.asyncio
@pytest.mark.parametrize("bulk", [False, True])
@pytest.mark.parametrize("prepared_price", [None, "105"])
async def test_market_order_uses_prepared_price_before_legacy_exposure_claim(bulk, prepared_price):
    repository, session = database()
    order = row()
    order.price = None
    order.order_type = "MARKET"
    order.time_in_force = None
    metadata = [("order", {"execution_plan": {"reference_price": prepared_price}})]
    session.scalar.return_value = order
    if bulk:
        session.scalars.return_value = Mock(all=Mock(return_value=[order]))
        session.execute.side_effect = [
            Mock(all=Mock(return_value=metadata)),
            Mock(all=Mock(return_value=[("intent", Decimal("200"))])),
        ]
        restored = (await repository.load_unresolved_orders())[0]
        assert session.execute.await_count == (1 if prepared_price else 2)
    else:
        session.execute.return_value = Mock(all=Mock(return_value=metadata))
        session.scalars.return_value = Mock(all=Mock(return_value=[Decimal("200")]))
        restored = await repository.load_order("order")
        assert session.scalars.await_count == (0 if prepared_price else 1)
    assert restored.plan.reference_price == Decimal(prepared_price or "100")
