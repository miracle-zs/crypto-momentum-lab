"""Execution commands, recovery identities and order-watermark persistence."""

from collections.abc import Mapping
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
    ExchangeFillRow,
    ExchangeOrderEventRow,
    ExchangeOrderRow,
    ExecutionCommandRow,
    ExecutionReconciliationEventRow,
)
from crypto_momentum_lab.persistence.postgres.serialization import jsonable


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
                    or_(
                        ExchangeOrderRow.state.not_in(tuple(
                            state.value for state in ExchangeOrderState if state.terminal
                        )),
                        and_(
                            ExchangeOrderRow.executed_quantity > 0,
                            ExecutionCommandRow.status == "rejected",
                        ),
                        ExecutionCommandRow.status.in_(
                            ["prepared", "dispatching", "acknowledged", "unknown"]
                        ),
                        exists(select(ExecutionBookHeadRow.symbol).where(
                            ExecutionBookHeadRow.environment == ExecutionCommandRow.details["scope"]["environment"].astext,
                            ExecutionBookHeadRow.account_label == ExecutionCommandRow.details["scope"]["account_label"].astext,
                            ExecutionBookHeadRow.symbol == ExecutionCommandRow.details["scope"]["symbol"].astext,
                            ExecutionBookHeadRow.position_side == ExecutionCommandRow.details["scope"]["position_side"].astext,
                            ExecutionBookHeadRow.state_payload["recovery_command_ids"].contains(
                                func.jsonb_build_array(ExecutionCommandRow.command_id)
                            ),
                        )),
                    ),
                )
            )
            if account_label is not None:
                query = query.where(
                    ExecutionCommandRow.details["scope"]["account_label"].astext == account_label
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
                    dtls["external_order_id"] = order.exchange_order_id if order is not None else None
                scope = dtls.get("scope")
                acc = (
                    scope.get("account_label")
                    if isinstance(scope, dict)
                    else dtls.get("account_label")
                )
                if account_label is not None:
                    if acc != account_label:
                        continue
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

    async def load_execution_order_watermarks(
        self,
        account_label: str | None = None,
    ) -> tuple[dict[str, object], ...]:
        """Load cumulative quantity/quote cuts for every persisted command.

        Terminal commands are included because a later order response can be
        stale or duplicated after the active outbox row has closed.

        Each command must persist its complete scope and cumulative watermarks.
        """
        async with self._session_factory() as session:
            rows = (
                await session.scalars(
                    select(ExecutionCommandRow).order_by(
                        ExecutionCommandRow.requested_at
                    )
                )
            ).all()

        result: list[dict[str, object]] = []
        for row in rows:
            if row.command in (
                "resolve_unknown_order",
                "manual_reduce_only_recovery",
                "manual_recovery_result",
            ):
                continue
            details = dict(row.details)
            raw_scope = details.get("scope")
            scope = dict(raw_scope) if isinstance(raw_scope, Mapping) else {}
            status = str(row.status)
            quantity = details.get("cumulative_filled_quantity")
            quote = details.get("cumulative_filled_quote")
            if (
                account_label is not None
                and isinstance(scope.get("account_label"), str)
                and scope["account_label"] != account_label
            ):
                continue

            client_order_id = row.client_order_id
            if not isinstance(client_order_id, str) or not client_order_id.strip():
                raise ValueError(
                    f"execution command {row.command_id} has no client order ID; "
                    "migration/recovery required"
                )

            missing_scope = tuple(
                field_name
                for field_name in (
                    "environment",
                    "account_label",
                    "symbol",
                    "position_side",
                )
                if not isinstance(scope.get(field_name), str)
                or not scope[field_name].strip()
            )
            if missing_scope:
                raise ValueError(
                    f"execution command {row.command_id} has incomplete scope "
                    f"({', '.join(missing_scope)})"
                )

            if quantity is None or quote is None:
                raise ValueError(
                    f"execution command {row.command_id} has incomplete cumulative "
                    "watermark"
                )

            cumulative_quantity = _execution_decimal(
                quantity,
                command_id=row.command_id,
                field_name="cumulative_filled_quantity",
            )
            cumulative_quote = _execution_decimal(
                quote,
                command_id=row.command_id,
                field_name="cumulative_filled_quote",
            )
            if cumulative_quantity == Decimal("0") and cumulative_quote != Decimal("0"):
                raise ValueError(
                    f"execution command {row.command_id} has quote without quantity; "
                    "migration/recovery required"
                )
            if cumulative_quantity > Decimal("0") and cumulative_quote == Decimal("0"):
                raise ValueError(
                    f"execution command {row.command_id} has zero quote with positive "
                    "quantity; migration/recovery required"
                )

            result.append(
                {
                    "scope": scope,
                    "client_order_id": client_order_id,
                    "cumulative_filled_quantity": quantity,
                    "cumulative_filled_quote": quote,
                    "status": status,
                }
            )
        return tuple(result)

    async def load_seen_event_ids(
        self,
        limit: int = 2000,
    ) -> tuple[str, ...]:
        async with self._session_factory() as session:
            rows = (
                await session.scalars(
                    select(ExchangeOrderEventRow.event_id)
                    .order_by(ExchangeOrderEventRow.occurred_at.desc())
                    .limit(limit)
                )
            ).all()
            return tuple(str(r) for r in rows if r)

    async def load_seen_fill_trade_ids(
        self,
        limit: int = 2000,
    ) -> tuple[str, ...]:
        async with self._session_factory() as session:
            rows = (
                await session.scalars(
                    select(ExchangeFillRow.exchange_trade_id)
                    .order_by(ExchangeFillRow.filled_at.desc())
                    .limit(limit)
                )
            ).all()
            return tuple(str(r) for r in rows if r)

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


def _restored_dispatch_status(order: ExchangeOrderRow | None, settlement_status: str) -> str:
    """Use the order for submission state; terminal projection still needs settlement."""
    if order is None:
        return settlement_status
    state = ExchangeOrderState(order.state)
    if state.terminal:
        if settlement_status == "rejected" and order.executed_quantity > 0:
            return "unknown"
        return settlement_status if settlement_status in {"terminal", "rejected"} else "unknown"
    if state in {ExchangeOrderState.ACKNOWLEDGED, ExchangeOrderState.SUBMITTED,
                 ExchangeOrderState.PARTIALLY_FILLED}:
        return "acknowledged"
    return "unknown"
