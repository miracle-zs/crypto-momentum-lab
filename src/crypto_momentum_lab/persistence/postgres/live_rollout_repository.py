from dataclasses import asdict
from datetime import datetime
from typing import Any, cast

from sqlalchemy import or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.live_rollout import (
    LiveOperatorApproval,
    LiveSessionState,
    LiveSessionTransition,
    RollbackCommand,
)
from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.persistence.postgres.models import (
    LiveOperatorApprovalRow,
    LiveRollbackCommandRow,
    LiveSessionTransitionRow,
)

_LIVE_SESSION_REASON_MAX_LENGTH = 128


class PostgresLiveRolloutRepository:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._session_factory = session_factory

    async def save_approval(self, approval: LiveOperatorApproval) -> None:
        await self._insert(LiveOperatorApprovalRow, asdict(approval))

    async def load_active_approval(
        self,
        *,
        account_label: str,
        strategy_name: str,
        now: datetime,
    ) -> LiveOperatorApproval | None:
        async with self._session_factory() as session:
            row = await session.scalar(
                select(LiveOperatorApprovalRow)
                .where(
                    LiveOperatorApprovalRow.account_label == account_label,
                    LiveOperatorApprovalRow.strategy_name == strategy_name,
                    or_(
                        LiveOperatorApprovalRow.expires_at.is_(None),
                        LiveOperatorApprovalRow.expires_at > now,
                    ),
                )
                .order_by(LiveOperatorApprovalRow.created_at.desc())
                .limit(1)
            )
        if row is None:
            return None
        return LiveOperatorApproval(
            approval_id=row.approval_id,
            account_label=row.account_label,
            strategy_name=row.strategy_name,
            strategy_config_hash=row.strategy_config_hash,
            risk_config_hash=row.risk_config_hash,
            git_commit_hash=row.git_commit_hash,
            database_migration_revision=row.database_migration_revision,
            approved_notional_cap=row.approved_notional_cap,
            approved_max_open_positions=row.approved_max_open_positions,
            approved_max_daily_loss=row.approved_max_daily_loss,
            approver_name=row.approver_name,
            approval_text=row.approval_text,
            expires_at=row.expires_at,
            created_at=row.created_at,
        )

    async def save_transition(self, transition: LiveSessionTransition) -> None:
        values = _prepare_transition_values(transition)
        await self._insert(LiveSessionTransitionRow, values)

    async def load_latest_transition(
        self,
        session_id: str,
    ) -> LiveSessionTransition | None:
        async with self._session_factory() as session:
            row = await session.scalar(
                select(LiveSessionTransitionRow)
                .where(LiveSessionTransitionRow.session_id == session_id)
                .order_by(LiveSessionTransitionRow.occurred_at.desc())
                .limit(1)
            )
        if row is None:
            return None
        return LiveSessionTransition(
            transition_id=row.transition_id,
            session_id=row.session_id,
            state=LiveSessionState(row.state),
            occurred_at=row.occurred_at,
            operator=row.operator,
            strategy_config_hash=row.strategy_config_hash,
            risk_config_hash=row.risk_config_hash,
            reason=row.reason,
            details=cast(dict[str, JsonValue], row.details),
        )

    async def save_command(self, command: RollbackCommand) -> bool:
        async with self._session_factory() as session:
            async with session.begin():
                inserted = await session.scalar(
                    insert(LiveRollbackCommandRow)
                    .values(asdict(command))
                    .on_conflict_do_nothing()
                    .returning(LiveRollbackCommandRow.command_id)
                )
        return inserted is not None

    async def load_command(self, command_id: str) -> RollbackCommand | None:
        async with self._session_factory() as session:
            row = await session.scalar(
                select(LiveRollbackCommandRow).where(
                    LiveRollbackCommandRow.command_id == command_id
                )
            )
        return None if row is None else _rollback_command_from_row(row)

    async def load_command_by_idempotency(
        self,
        idempotency_key: str,
    ) -> RollbackCommand | None:
        async with self._session_factory() as session:
            row = await session.scalar(
                select(LiveRollbackCommandRow).where(
                    LiveRollbackCommandRow.idempotency_key == idempotency_key
                )
            )
        return None if row is None else _rollback_command_from_row(row)

    async def claim_command(
        self,
        command_id: str,
        *,
        account_label: str,
        strategy_name: str,
        session_id: str,
    ) -> RollbackCommand | None:
        """Atomically move one requested command into execution."""

        async with self._session_factory() as session:
            async with session.begin():
                claimed_id = await session.scalar(
                    update(LiveRollbackCommandRow)
                    .where(
                        LiveRollbackCommandRow.command_id == command_id,
                        LiveRollbackCommandRow.account_label == account_label,
                        LiveRollbackCommandRow.strategy_name == strategy_name,
                        LiveRollbackCommandRow.session_id == session_id,
                        LiveRollbackCommandRow.status == "requested",
                    )
                    .values(status="executing")
                    .returning(LiveRollbackCommandRow.command_id)
                )
                if claimed_id is None:
                    return None
                row = await session.scalar(
                    select(LiveRollbackCommandRow).where(
                        LiveRollbackCommandRow.command_id == claimed_id
                    )
                )
        return None if row is None else _rollback_command_from_row(row)

    async def complete_command(
        self,
        command_id: str,
        *,
        status: str,
        completed_at: datetime,
        failure_reason: str | None,
    ) -> bool:
        if status not in {"completed", "failed"}:
            raise ValueError("command completion status must be completed or failed")
        async with self._session_factory() as session:
            async with session.begin():
                result = await session.execute(
                    update(LiveRollbackCommandRow)
                    .where(
                        LiveRollbackCommandRow.command_id == command_id,
                        LiveRollbackCommandRow.status == "executing",
                    )
                    .values(
                        status=status,
                        completed_at=completed_at,
                        failure_reason=failure_reason,
                    )
                )
        rowcount = cast(int, getattr(result, "rowcount", 0))
        return rowcount == 1

    async def _insert(self, model: Any, values: dict[str, object]) -> None:
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    insert(model).values(values).on_conflict_do_nothing()
                )


def _prepare_transition_values(
    transition: LiveSessionTransition,
) -> dict[str, object]:
    values = asdict(transition)
    values["state"] = transition.state.value
    reason = values.get("reason")
    if isinstance(reason, str) and len(reason) > _LIVE_SESSION_REASON_MAX_LENGTH:
        details = values.get("details")
        normalized_details = dict(details) if isinstance(details, dict) else {}
        normalized_details.setdefault("full_reason", reason)
        values["details"] = normalized_details
        values["reason"] = (
            reason[: _LIVE_SESSION_REASON_MAX_LENGTH - 3] + "..."
        )
    return values


def _rollback_command_from_row(row: LiveRollbackCommandRow) -> RollbackCommand:
    return RollbackCommand(
        command_id=row.command_id,
        command_type=row.command_type,
        requested_by=row.requested_by,
        confirmation_text=row.confirmation_text,
        requested_at=row.requested_at,
        idempotency_key=row.idempotency_key,
        account_label=row.account_label,
        strategy_name=row.strategy_name,
        session_id=row.session_id,
        status=row.status,
        completed_at=row.completed_at,
        failure_reason=row.failure_reason,
    )
