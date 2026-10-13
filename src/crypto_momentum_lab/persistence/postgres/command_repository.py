"""Execution commands, recovery identities and order-watermark persistence."""

from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import (
    and_,
    exists,
    func,
    or_,
    select,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.execution.order_state import ExchangeOrderState
from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
    ExecutionBookHeadRow,
)
from crypto_momentum_lab.persistence.postgres.models import (
    ExchangeOrderRow,
    ExecutionCommandRow,
    ExecutionReconciliationEventRow,
)
from crypto_momentum_lab.persistence.postgres.serialization import jsonable


def active_execution_command_condition():
    """Select outstanding dispatch/settlement responsibility, not closed history."""
    return and_(
        ExecutionCommandRow.command.not_in(
            (
                "resolve_unknown_order",
                "manual_reduce_only_recovery",
                "manual_recovery_result",
            )
        ),
        or_(
            ExchangeOrderRow.state.not_in(
                tuple(state.value for state in ExchangeOrderState if state.terminal)
            ),
            and_(
                ExchangeOrderRow.executed_quantity > 0,
                ExecutionCommandRow.status == "rejected",
            ),
            ExecutionCommandRow.status.in_(
                ("prepared", "dispatching", "acknowledged", "unknown")
            ),
            exists(
                select(ExecutionBookHeadRow.symbol).where(
                    ExecutionBookHeadRow.environment
                    == ExecutionCommandRow.details["scope"]["environment"].astext,
                    ExecutionBookHeadRow.account_label
                    == ExecutionCommandRow.details["scope"]["account_label"].astext,
                    ExecutionBookHeadRow.symbol
                    == ExecutionCommandRow.details["scope"]["symbol"].astext,
                    ExecutionBookHeadRow.position_side
                    == ExecutionCommandRow.details["scope"]["position_side"].astext,
                    ExecutionBookHeadRow.state_payload["recovery_command_ids"].contains(
                        func.jsonb_build_array(ExecutionCommandRow.command_id)
                    ),
                )
            ),
        ),
    )


def _execution_decimal(
    value: object,
    *,
    command_id: str,
    field_name: str,
) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as err:
        raise ValueError(
            f"execution command {command_id} has invalid {field_name}; "
            "migration/recovery required"
        ) from err
    if not result.is_finite() or result < Decimal("0"):
        raise ValueError(
            f"execution command {command_id} has invalid {field_name}; "
            "migration/recovery required"
        )
    return result


class PostgresCommandRepository:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._session_factory = session_factory

    async def save_execution_command(
        self,
        *,
        command_id: str,
        client_order_id: str | None,
        command: str,
        status: str,
        requested_at: datetime,
        details: dict[str, JsonValue],
    ) -> None:
        await self._insert_immutable(
            ExecutionCommandRow,
            {
                "command_id": command_id,
                "client_order_id": client_order_id,
                "command": command,
                "status": status,
                "requested_at": requested_at,
                "details": jsonable(details),
            },
        )

    async def upsert_execution_command(
        self,
        *,
        command_id: str,
        client_order_id: str | None,
        command: str,
        status: str,
        requested_at: datetime,
        details: dict[str, JsonValue],
    ) -> None:
        async with self._session_factory() as session:
            async with session.begin():
                stmt = (
                    insert(ExecutionCommandRow)
                    .values(
                        command_id=command_id,
                        client_order_id=client_order_id,
                        command=command,
                        status=status,
                        requested_at=requested_at,
                        details=jsonable(details),
                    )
                    .on_conflict_do_update(
                        index_elements=[ExecutionCommandRow.command_id],
                        set_={
                            "status": status,
                            "details": jsonable(details),
                        },
                    )
                )
                await session.execute(stmt)

    async def upsert_execution_command_in_session(
        self,
        session: AsyncSession,
        *,
        command_id: str,
        client_order_id: str | None,
        command: str,
        status: str,
        requested_at: datetime,
        details: dict[str, JsonValue],
    ) -> None:
        """Upsert an outbox state without committing the caller's transaction."""
        normalized_details = jsonable(details)
        existing = await session.get(
            ExecutionCommandRow, command_id, with_for_update=True
        )
        if existing is None:
            session.add(
                ExecutionCommandRow(
                    command_id=command_id,
                    client_order_id=client_order_id,
                    command=command,
                    status=status,
                    requested_at=requested_at,
                    details=normalized_details,
                )
            )
            return
        if existing.client_order_id != client_order_id or existing.command != command:
            raise ValueError(
                f"execution command {command_id} conflicts with its durable identity"
            )
        existing.status = status
        existing.details = normalized_details

    async def load_active_execution_commands(
        self,
        account_label: str | None = None,
    ) -> tuple[dict[str, object], ...]:
        async with self._session_factory() as session:
            query = (
                select(ExecutionCommandRow, ExchangeOrderRow)
                .outerjoin(
                    ExchangeOrderRow,
                    (
                        ExecutionCommandRow.client_order_id
                        == ExchangeOrderRow.client_order_id
                    ),
                )
                .where(
                    active_execution_command_condition(),
                )
            )
            if account_label is not None:
                query = query.where(
                    ExecutionCommandRow.details["scope"]["account_label"].astext
                    == account_label
                )
            rows = (
                await session.execute(query.order_by(ExecutionCommandRow.requested_at))
            ).all()
            result = []
            for r, order in rows:
                if r.command in (
                    "resolve_unknown_order",
                    "manual_reduce_only_recovery",
                    "manual_recovery_result",
                ):
                    continue
                dtls = dict(r.details)
                if dtls.get("external_order_id") is None:
                    dtls["external_order_id"] = (
                        order.exchange_order_id if order is not None else None
                    )
                # The account scope is already filtered in SQL above. A row
                # without a scope mapping cannot be restored (the command codec
                # requires one), so there is no second Python-side filter here:
                # adding one would only hide which rows recovery must report.
                result.append(
                    {
                        "command_id": r.command_id,
                        "client_order_id": r.client_order_id,
                        "command": r.command,
                        "status": _restored_dispatch_status(order, r.status),
                        "requested_at": r.requested_at,
                        "details": dtls,
                    }
                )
            return tuple(result)

    async def save_reconciliation_event(
        self,
        *,
        reconciliation_event_id: str,
        client_order_id: str,
        outcome: str,
        occurred_at: datetime,
        details: dict[str, JsonValue],
    ) -> None:
        await self._insert_immutable(
            ExecutionReconciliationEventRow,
            {
                "reconciliation_event_id": reconciliation_event_id,
                "client_order_id": client_order_id,
                "outcome": outcome,
                "occurred_at": occurred_at,
                "details": jsonable(details),
            },
        )

    async def _insert_immutable(
        self,
        model: Any,
        values: dict[str, object],
    ) -> None:
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    insert(model).values(values).on_conflict_do_nothing()
                )


def _restored_dispatch_status(
    order: ExchangeOrderRow | None, settlement_status: str
) -> str:
    """Use the order for submission state; terminal projection still needs settlement."""
    if order is None:
        return settlement_status
    state = ExchangeOrderState(order.state)
    if state.terminal:
        if settlement_status == "rejected" and order.executed_quantity > 0:
            return "unknown"
        return (
            settlement_status
            if settlement_status in {"terminal", "rejected"}
            else "unknown"
        )
    if state in {
        ExchangeOrderState.ACKNOWLEDGED,
        ExchangeOrderState.SUBMITTED,
        ExchangeOrderState.PARTIALLY_FILLED,
    }:
        return "acknowledged"
    return "unknown"
