from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy.sql import Select

from crypto_momentum_lab.persistence.postgres.command_repository import (
    PostgresCommandRepository,
)
from crypto_momentum_lab.persistence.postgres.models import ExecutionCommandRow


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
        command="entry",
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
    ) -> None:
        self._rows = {
            ExecutionCommandRow: commands,
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
async def test_incomplete_terminal_watermark_is_rejected() -> None:
    repository = _repository(commands=[_command("legacy", scope=_scope())])

    with pytest.raises(ValueError, match="incomplete cumulative watermark"):
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
