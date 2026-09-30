from decimal import Decimal, InvalidOperation
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.dialects import postgresql

from crypto_momentum_lab.persistence.postgres.order_read_repository import (
    PostgresOrderReadRepository,
)


@pytest.mark.parametrize(
    "details, expected",
    [
        (None, None),
        ([], None),
        ({}, None),
        ({"desired_notional": None}, None),
        ({"desired_notional": "12.345"}, Decimal("12.345")),
        ({"desired_notional": 0}, Decimal("0")),
        ({"desired_notional": 12.5}, Decimal("12.5")),
    ],
)
async def test_reads_durable_approved_notional(details, expected) -> None:
    session = AsyncMock()
    session.scalar.return_value = details
    factory = MagicMock()
    factory.return_value.__aenter__ = AsyncMock(return_value=session)
    factory.return_value.__aexit__ = AsyncMock(return_value=False)
    assert (
        await PostgresOrderReadRepository(factory).load_approved_intent_notional(
            "intent-3"
        )
        == expected
    )
    statement = session.scalar.await_args.args[0]
    sql = str(
        statement.compile(
            dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
        )
    )
    assert "intent_id = 'intent-3'" in sql
    assert "details" in sql


async def test_invalid_durable_notional_does_not_silently_disappear() -> None:
    session = AsyncMock()
    session.scalar.return_value = {"desired_notional": "invalid"}
    factory = MagicMock()
    factory.return_value.__aenter__ = AsyncMock(return_value=session)
    factory.return_value.__aexit__ = AsyncMock(return_value=False)
    with pytest.raises(InvalidOperation):
        await PostgresOrderReadRepository(factory).load_approved_intent_notional("i1")
