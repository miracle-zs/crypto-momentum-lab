from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, cast

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.domain.risk import (
    RiskConfigSnapshot,
    RiskDecision,
    RiskEvaluation,
    RiskHalt,
    StrategyLiveStateRecord,
    TradingLease,
    TradingLeaseState,
)
from crypto_momentum_lab.persistence.postgres.models import (
    RiskConfigSnapshotRow,
    RiskEvaluationRow,
    RiskHaltRow,
    RiskRejectionRow,
    StrategyLiveStateRow,
    TradingLeaseRow,
)


class LeaseAlreadyHeldError(RuntimeError):
    pass


class LeaseOwnershipError(RuntimeError):
    pass


def trading_lease_row(lease: TradingLease) -> dict[str, object]:
    return {
        "lease_id": lease.lease_id,
        "environment": lease.environment,
        "account_label": lease.account_label,
        "strategy_name": lease.strategy_name,
        "owner": lease.owner,
        "state": lease.state.value,
        "acquired_at": lease.acquired_at,
        "expires_at": lease.expires_at,
    }


def risk_config_row(config: RiskConfigSnapshot) -> dict[str, object]:
    return {
        "config_hash": config.config_hash,
        "environment": config.environment,
        "account_label": config.account_label,
        "max_order_notional": config.max_order_notional,
        "max_gross_notional": config.max_gross_notional,
        "max_daily_loss": config.max_daily_loss,
        "max_open_positions": config.max_open_positions,
        "max_market_state_age_seconds": Decimal(
            str(config.max_market_state_age_seconds)
        ),
        "max_account_state_age_seconds": Decimal(
            str(config.max_account_state_age_seconds)
        ),
        "allow_reduce_only_while_draining": (
            config.allow_reduce_only_while_draining
        ),
        "created_at": config.created_at,
    }


def risk_evaluation_row(evaluation: RiskEvaluation) -> dict[str, object]:
    return {
        "evaluation_id": evaluation.evaluation_id,
        "candidate_id": evaluation.candidate_id,
        "decision": evaluation.decision.value,
        "reason": evaluation.reason,
        "evaluated_at": evaluation.evaluated_at,
        "details": _jsonable(evaluation.details),
    }


def risk_halt_row(halt: RiskHalt) -> dict[str, object]:
    return {
        "halt_id": halt.halt_id,
        "environment": halt.environment,
        "account_label": halt.account_label,
        "reason": halt.reason,
        "active": halt.active,
        "created_at": halt.created_at,
        "details": _jsonable(halt.details),
    }


def strategy_live_state_row(state: StrategyLiveStateRecord) -> dict[str, object]:
    return {
        "environment": state.environment,
        "account_label": state.account_label,
        "strategy_name": state.strategy_name,
        "state": state.state.value,
        "changed_at": state.changed_at,
        "reason": state.reason,
    }


class PostgresRiskRepository:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._session_factory = session_factory

    async def acquire_lease(self, lease: TradingLease) -> None:
        if lease.state is not TradingLeaseState.ACTIVE:
            raise ValueError("acquired lease must be active")
        try:
            async with self._session_factory() as session:
                async with session.begin():
                    await session.execute(
                        update(TradingLeaseRow)
                        .where(
                            TradingLeaseRow.environment == lease.environment,
                            TradingLeaseRow.account_label == lease.account_label,
                            TradingLeaseRow.state == TradingLeaseState.ACTIVE.value,
                            TradingLeaseRow.expires_at <= lease.acquired_at,
                        )
                        .values(state=TradingLeaseState.EXPIRED.value)
                    )
                    await session.execute(
                        insert(TradingLeaseRow).values(trading_lease_row(lease))
                    )
        except IntegrityError as exc:
            raise LeaseAlreadyHeldError(
                f"active trading lease already held for "
                f"{lease.environment}/{lease.account_label}"
            ) from exc

    async def renew_lease(
        self,
        lease_id: str,
        owner: str,
        expires_at: datetime,
    ) -> TradingLease:
        async with self._session_factory() as session:
            async with session.begin():
                row = await session.scalar(
                    select(TradingLeaseRow)
                    .where(TradingLeaseRow.lease_id == lease_id)
                    .with_for_update()
                )
                owned_row = _require_owned_active_lease(row, owner)
                if expires_at <= owned_row.expires_at:
                    raise ValueError("renewed expiration must extend the lease")
                owned_row.expires_at = expires_at
                return _lease_from_row(owned_row)

    async def release_lease(self, lease_id: str, owner: str) -> None:
        async with self._session_factory() as session:
            async with session.begin():
                row = await session.scalar(
                    select(TradingLeaseRow)
                    .where(TradingLeaseRow.lease_id == lease_id)
                    .with_for_update()
                )
                owned_row = _require_owned_active_lease(row, owner)
                owned_row.state = TradingLeaseState.RELEASED.value

    async def load_active_lease(
        self,
        environment: str,
        account_label: str,
        now: datetime,
    ) -> TradingLease | None:
        async with self._session_factory() as session:
            row = await session.scalar(
                select(TradingLeaseRow).where(
                    TradingLeaseRow.environment == environment,
                    TradingLeaseRow.account_label == account_label,
                    TradingLeaseRow.state == TradingLeaseState.ACTIVE.value,
                    TradingLeaseRow.expires_at > now,
                )
            )
        return None if row is None else _lease_from_row(row)

    async def save_risk_config(self, config: RiskConfigSnapshot) -> None:
        await self._insert_immutable(RiskConfigSnapshotRow, risk_config_row(config))

    async def save_risk_evaluation(self, evaluation: RiskEvaluation) -> None:
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    insert(RiskEvaluationRow)
                    .values(risk_evaluation_row(evaluation))
                    .on_conflict_do_nothing()
                )
                if evaluation.decision is not RiskDecision.APPROVED:
                    await session.execute(
                        insert(RiskRejectionRow)
                        .values(
                            evaluation_id=evaluation.evaluation_id,
                            reason=evaluation.reason,
                            details=_jsonable(evaluation.details),
                        )
                        .on_conflict_do_nothing()
                    )

    async def save_halt(self, halt: RiskHalt) -> None:
        values = risk_halt_row(halt)
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    insert(RiskHaltRow)
                    .values(values)
                    .on_conflict_do_update(
                        index_elements=[RiskHaltRow.halt_id],
                        set_={
                            "reason": halt.reason,
                            "active": halt.active,
                            "details": _jsonable(halt.details),
                        },
                    )
                )

    async def load_active_halts(
        self,
        environment: str,
        account_label: str,
    ) -> tuple[RiskHalt, ...]:
        async with self._session_factory() as session:
            rows = (
                await session.scalars(
                    select(RiskHaltRow)
                    .where(
                        RiskHaltRow.environment == environment,
                        RiskHaltRow.account_label == account_label,
                        RiskHaltRow.active.is_(True),
                    )
                    .order_by(RiskHaltRow.created_at, RiskHaltRow.halt_id)
                )
            ).all()
        return tuple(_halt_from_row(row) for row in rows)

    async def save_strategy_live_state(
        self,
        state: StrategyLiveStateRecord,
    ) -> None:
        values = strategy_live_state_row(state)
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    insert(StrategyLiveStateRow)
                    .values(values)
                    .on_conflict_do_update(
                        index_elements=[
                            StrategyLiveStateRow.environment,
                            StrategyLiveStateRow.account_label,
                            StrategyLiveStateRow.strategy_name,
                        ],
                        set_={
                            "state": state.state.value,
                            "changed_at": state.changed_at,
                            "reason": state.reason,
                        },
                    )
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


def _require_owned_active_lease(
    row: TradingLeaseRow | None,
    owner: str,
) -> TradingLeaseRow:
    if row is None:
        raise LeaseOwnershipError("trading lease does not exist")
    lease = _lease_from_row(row)
    if lease.owner != owner:
        raise LeaseOwnershipError("trading lease is owned by another worker")
    if lease.state is not TradingLeaseState.ACTIVE:
        raise LeaseOwnershipError("trading lease is not active")
    return row


def _lease_from_row(row: TradingLeaseRow) -> TradingLease:
    return TradingLease(
        lease_id=row.lease_id,
        environment=row.environment,
        account_label=row.account_label,
        strategy_name=row.strategy_name,
        owner=row.owner,
        state=TradingLeaseState(row.state),
        acquired_at=row.acquired_at,
        expires_at=row.expires_at,
    )


def _halt_from_row(row: RiskHaltRow) -> RiskHalt:
    return RiskHalt(
        halt_id=row.halt_id,
        environment=row.environment,
        account_label=row.account_label,
        reason=row.reason,
        active=row.active,
        created_at=row.created_at,
        details=cast(dict[str, JsonValue], row.details),
    )


def _jsonable(value: object) -> JsonValue:
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    return str(value)
