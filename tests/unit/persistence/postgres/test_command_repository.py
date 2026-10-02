from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy.sql import Select

from crypto_momentum_lab.persistence.postgres.command_repository import (
    PostgresCommandRepository,
)
from crypto_momentum_lab.persistence.postgres.models import (
    ExchangeFillRow,
    ExchangeOrderEventRow,
    ExecutionCommandRow,
)


def _scope(account_label: str = "account-a") -> dict[str, str]:
    return {
        "environment": "live",
        "account_label": account_label,
        "symbol": "BTCUSDT",
        "position_side": "both",
    }


def _command(
    command_id: str,
    *,
    status: str = "terminal",
    scope: dict[str, str] | None = None,
    quantity: str | None = None,
    quote: str | None = None,
) -> SimpleNamespace:
    details: dict[str, Any] = {}
    if scope is not None:
        details["scope"] = scope
    if quantity is not None:
        details["cumulative_filled_quantity"] = quantity
    if quote is not None:
        details["cumulative_filled_quote"] = quote
    return SimpleNamespace(
        command_id=command_id,
        client_order_id=f"order-{command_id}",
        status=status,
        details=details,
    )


class _Result:
    def __init__(self, rows: list[object]) -> None:
        self._rows = rows

    def all(self) -> list[object]:
        return self._rows


class _FakeSession:
    def __init__(
        self,
        *,
        commands: list[object],
        events: list[object] | None = None,
        fills: list[object] | None = None,
    ) -> None:
        self._rows = {
            ExecutionCommandRow: commands,
            ExchangeOrderEventRow: events or [],
            ExchangeFillRow: fills or [],
        }

    async def __aenter__(self) -> _FakeSession:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    async def scalars(self, statement: Select[Any]) -> _Result:
        model = statement.column_descriptions[0]["entity"]
        return _Result(self._rows[model])


class _FakeSessionFactory:
    def __init__(self, **rows: list[object]) -> None:
        self._rows = rows

    def __call__(self) -> _FakeSession:
        return _FakeSession(**self._rows)


def _repository(**rows: list[object]) -> PostgresCommandRepository:
    return PostgresCommandRepository(_FakeSessionFactory(**rows))  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_watermark_loader_includes_terminal_and_explicit_prepared_zero() -> None:
    repository = _repository(
        commands=[
            _command(
                "terminal",
                scope=_scope(),
                quantity="2.5",
                quote="251.25",
            ),
            _command(
                "prepared",
                status="prepared",
                scope=_scope(),
                quantity="0",
                quote="0",
            ),
        ]
    )

    rows = await repository.load_execution_order_watermarks("account-a")

    assert [(row["status"], row["cumulative_filled_quantity"]) for row in rows] == [
        ("terminal", "2.5"),
        ("prepared", "0"),
    ]


@pytest.mark.asyncio
async def test_incomplete_other_account_terminal_does_not_block_restore() -> None:
    repository = _repository(
        commands=[
            _command(
                "target",
                scope=_scope(),
                quantity="1",
                quote="100",
            ),
            _command("other", scope=_scope("account-b")),
        ]
    )

    rows = await repository.load_execution_order_watermarks("account-a")

    assert [row["client_order_id"] for row in rows] == ["order-target"]


@pytest.mark.asyncio
async def test_old_terminal_watermark_is_recovered_from_cumulative_order_event() -> (
    None
):
    command = _command("legacy", scope=_scope())
    event = SimpleNamespace(
        client_order_id=command.client_order_id,
        details={
            "account_label": "account-a",
            "symbol": "BTCUSDT",
            "position_side": "both",
            "executed_quantity": "4",
            "cumulative_quote_quantity": "404",
        },
    )
    repository = _repository(commands=[command], events=[event])

    rows = await repository.load_execution_order_watermarks("account-a")

    assert len(rows) == 1
    assert rows[0]["cumulative_filled_quantity"] == Decimal("4")
    assert rows[0]["cumulative_filled_quote"] == Decimal("404")


@pytest.mark.asyncio
async def test_old_terminal_watermark_is_recovered_from_persisted_fills() -> None:
    command = _command("legacy", scope=_scope())
    fills = [
        SimpleNamespace(
            client_order_id=command.client_order_id,
            quantity=Decimal("2"),
            price=Decimal("100"),
        ),
        SimpleNamespace(
            client_order_id=command.client_order_id,
            quantity=Decimal("3"),
            price=Decimal("101"),
        ),
    ]
    repository = _repository(commands=[command], fills=fills)

    rows = await repository.load_execution_order_watermarks("account-a")

    assert len(rows) == 1
    assert rows[0]["cumulative_filled_quantity"] == Decimal("5")
    assert rows[0]["cumulative_filled_quote"] == Decimal("503")


@pytest.mark.asyncio
async def test_old_terminal_without_trusted_fill_facts_fails_closed() -> None:
    repository = _repository(commands=[_command("legacy", scope=_scope())])

    with pytest.raises(ValueError, match="migration/recovery required"):
        await repository.load_execution_order_watermarks("account-a")


@pytest.mark.asyncio
async def test_positive_cumulative_quantity_requires_positive_quote() -> None:
    repository = _repository(
        commands=[
            _command(
                "inconsistent",
                scope=_scope(),
                quantity="1",
                quote="0",
            )
        ]
    )

    with pytest.raises(ValueError, match="zero quote with positive quantity"):
        await repository.load_execution_order_watermarks("account-a")


@pytest.mark.asyncio
async def test_unpriced_intermediate_event_followed_by_priced_event_recovers() -> None:
    command = _command("intermediate", scope=_scope())
    events = [
        SimpleNamespace(
            client_order_id=command.client_order_id,
            details={
                "account_label": "account-a",
                "symbol": "BTCUSDT",
                "position_side": "both",
                "executed_quantity": "2",
                "average_price": "0",
            },
        ),
        SimpleNamespace(
            client_order_id=command.client_order_id,
            details={
                "account_label": "account-a",
                "symbol": "BTCUSDT",
                "position_side": "both",
                "executed_quantity": "2",
                "average_price": "50000",
            },
        ),
    ]
    repository = _repository(commands=[command], events=events)
    rows = await repository.load_execution_order_watermarks("account-a")

    assert len(rows) == 1
    assert rows[0]["cumulative_filled_quantity"] == Decimal("2")
    assert rows[0]["cumulative_filled_quote"] == Decimal("100000")


@pytest.mark.asyncio
async def test_matching_fill_and_event_with_rounding_difference_reconciles() -> None:
    command = _command("rounding", scope=_scope())
    events = [
        SimpleNamespace(
            client_order_id=command.client_order_id,
            details={
                "account_label": "account-a",
                "symbol": "BTCUSDT",
                "position_side": "both",
                "executed_quantity": "3",
                "cumulative_quote_quantity": "300.00",
            },
        ),
    ]
    fills = [
        SimpleNamespace(
            client_order_id=command.client_order_id,
            quantity=Decimal("3"),
            price=Decimal("100.001"),
        ),
    ]
    repository = _repository(commands=[command], events=events, fills=fills)
    rows = await repository.load_execution_order_watermarks("account-a")

    assert len(rows) == 1
    assert rows[0]["cumulative_filled_quantity"] == Decimal("3")
    # Prefers the fill watermark (3 * 100.001 = 300.003)
    assert rows[0]["cumulative_filled_quote"] == Decimal("300.003")


@pytest.mark.asyncio
async def test_in_session_outbox_insert_preserves_caller_transaction():
    from datetime import UTC, datetime
    from unittest.mock import AsyncMock, Mock

    session = AsyncMock()
    session.get.return_value = None
    session.add = Mock()
    repository = PostgresCommandRepository(None)
    await repository.upsert_execution_command_in_session(
        session,
        command_id="command-1",
        client_order_id="command-1",
        command="exit",
        status="prepared",
        requested_at=datetime(2026, 9, 30, tzinfo=UTC),
        details={"reservations": ["r1"]},
    )
    session.get.assert_awaited_once_with(
        ExecutionCommandRow, "command-1", with_for_update=True
    )
    inserted = session.add.call_args.args[0]
    assert inserted.command_id == "command-1"
    assert inserted.details == {"reservations": ["r1"]}
    session.commit.assert_not_awaited()
    session.rollback.assert_not_awaited()


@pytest.mark.asyncio
async def test_in_session_outbox_identity_conflict_does_not_mutate_row():
    from datetime import UTC, datetime
    from unittest.mock import AsyncMock

    existing = ExecutionCommandRow(
        command_id="command-1",
        client_order_id="other-order",
        command="exit",
        status="prepared",
        details={},
    )
    session = AsyncMock()
    session.get.return_value = existing
    with pytest.raises(ValueError, match="durable identity"):
        await PostgresCommandRepository(None).upsert_execution_command_in_session(
            session,
            command_id="command-1",
            client_order_id="command-1",
            command="exit",
            status="unknown",
            requested_at=datetime(2026, 9, 30, tzinfo=UTC),
            details={},
        )
    assert existing.status == "prepared"
    session.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_execution_transaction_uses_command_repository_with_same_session():
    from datetime import UTC, datetime
    from unittest.mock import AsyncMock

    from crypto_momentum_lab.persistence.postgres.execution_unit_of_work import (
        ExecutionTransaction,
    )

    session, commands = AsyncMock(), AsyncMock()
    tx = ExecutionTransaction(
        session,
        journal_store=AsyncMock(),
        command_repository=commands,
        reservation_repository=AsyncMock(),
    )
    values = dict(
        command_id="command-1",
        client_order_id="command-1",
        command="exit",
        status="unknown",
        requested_at=datetime(2026, 9, 30, tzinfo=UTC),
        details={},
    )
    await tx.upsert_outbox(**values)
    commands.upsert_execution_command_in_session.assert_awaited_once_with(
        session, **values
    )
    session.commit.assert_not_awaited()
