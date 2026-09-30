from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from crypto_momentum_lab.live_rollout.entry_order_cancellation import (
    LiveEntryOrderCanceller,
    external_open_order_cancellation_plan,
)
from crypto_momentum_lab.persistence.postgres.order_adoption_repository import (
    PostgresOrderAdoptionRepository,
)
from crypto_momentum_lab.persistence.postgres.order_repository import (
    PostgresOrderRepository,
)
from tests.unit.live_rollout.test_entry_order_cancellation import NOW, _open_order


def plan():
    return external_open_order_cancellation_plan(
        _open_order(order_id="exchange", client_order_id="orphan"),
        run_id="run",
    )


def database(scalars):
    session = AsyncMock()
    session.__aenter__.return_value = session
    transaction = AsyncMock()
    session.begin = Mock(return_value=transaction)
    session.scalar.side_effect = scalars
    factory = Mock(return_value=session)
    return PostgresOrderAdoptionRepository(factory), session, transaction, factory


async def adopt(repository, **kwargs):
    await repository.adopt_external_order_for_cancellation(
        plan(),
        exchange_order_id=kwargs.get("exchange_order_id", "exchange"),
        observed_at=kwargs.get("observed_at", NOW),
    )


@pytest.mark.asyncio
async def test_new_adoption_journals_synthetic_intent_and_order_in_one_transaction():
    repository, session, transaction, factory = database(["orphan"])
    await adopt(repository)
    factory.assert_called_once_with()
    session.begin.assert_called_once_with()
    intent = session.execute.await_args.args[0]
    order = session.scalar.await_args.args[0]
    assert intent.table.name == "order_intents"
    assert order.table.name == "exchange_orders"
    assert intent.compile().params["intent_id"] == "orphan-cancel:orphan"
    assert order.compile().params["intent_id"] == "orphan-cancel:orphan"
    assert order.compile().params["exchange_order_id"] == "exchange"
    transaction.__aexit__.assert_awaited_once_with(None, None, None)
    session.commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("existing_kind", ["same", "different", "missing"])
async def test_existing_client_identity_is_verified_before_adoption_returns(
    existing_kind,
):
    p = plan()
    fields = {
        name: getattr(p, name)
        for name in (
            "run_id",
            "symbol",
            "side",
            "order_type",
            "quantity",
            "price",
            "time_in_force",
            "expires_at",
            "reduce_only",
            "created_at",
        )
    }
    fields.update(intent_id="orphan-cancel:orphan", position_side=p.position_side.value)
    if existing_kind == "different":
        fields["quantity"] = Decimal("99")
    existing = None if existing_kind == "missing" else SimpleNamespace(**fields)
    repository, session, transaction, _ = database([None, existing])
    if existing_kind == "same":
        await adopt(repository)
        transaction.__aexit__.assert_awaited_once_with(None, None, None)
    else:
        with pytest.raises(ValueError, match="different order"):
            await adopt(repository)
        assert transaction.__aexit__.await_args.args[0] is ValueError
    assert session.execute.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid", [{"exchange_order_id": " "}, {"observed_at": NOW.replace(tzinfo=None)}]
)
async def test_invalid_adoption_does_not_open_transaction(invalid):
    repository, _, _, factory = database([])
    with pytest.raises(ValueError):
        await adopt(repository, **invalid)
    factory.assert_not_called()


@pytest.mark.asyncio
async def test_order_insert_failure_propagates_through_intent_transaction():
    repository, session, transaction, _ = database([])
    failure = RuntimeError("order write failed")
    session.scalar.side_effect = failure
    with pytest.raises(RuntimeError, match="order write failed"):
        await adopt(repository)
    session.execute.assert_awaited_once()
    assert transaction.__aexit__.await_args.args[1] is failure


@pytest.mark.asyncio
async def test_failed_adoption_prevents_exchange_cancellation():
    exchange = SimpleNamespace(
        fetch_open_orders=AsyncMock(
            return_value=(_open_order(order_id="exchange", client_order_id="orphan"),)
        )
    )
    state_machine = SimpleNamespace(cancel_order=AsyncMock())
    repository = SimpleNamespace(
        adopt_external_order_for_cancellation=AsyncMock(
            side_effect=RuntimeError("adoption failed")
        ),
    )
    canceller = LiveEntryOrderCanceller(
        exchange=exchange,
        state_machine=state_machine,
        repository=repository,
        run_id="run",
    )
    with pytest.raises(RuntimeError, match="adoption failed"):
        await canceller.cancel(())
    repository.adopt_external_order_for_cancellation.assert_awaited_once()
    state_machine.cancel_order.assert_not_awaited()


def test_plan_repository_no_longer_exposes_external_adoption():
    assert not hasattr(PostgresOrderRepository, "adopt_external_order_for_cancellation")
