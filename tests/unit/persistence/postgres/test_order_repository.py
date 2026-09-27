from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy.sql import Select

from crypto_momentum_lab.persistence.postgres.models import (
    ExchangeFillRow,
    ExchangeOrderEventRow,
    ExecutionCommandRow,
)
from crypto_momentum_lab.persistence.postgres.order_repository import (
    PostgresOrderRepository,
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


def _repository(**rows: list[object]) -> PostgresOrderRepository:
    return PostgresOrderRepository(_FakeSessionFactory(**rows))  # type: ignore[arg-type]


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
