from typing import Any, cast

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.domain.risk import (
    RiskConfigSnapshot,
    RiskDecision,
    RiskEvaluation,
    RiskHalt,
    StrategyLiveStateRecord,
)
from crypto_momentum_lab.persistence.postgres.models import (
    RiskConfigSnapshotRow,
    RiskEvaluationRow,
    RiskHaltRow,
    RiskRejectionRow,
    StrategyLiveStateRow,
)
from crypto_momentum_lab.persistence.postgres.serialization import jsonable


def risk_config_row(config: RiskConfigSnapshot) -> dict[str, object]:
    return {
        "config_hash": config.config_hash,
        "environment": config.environment,
        "account_label": config.account_label,
        "max_order_notional": config.max_order_notional,
        "max_gross_notional": config.max_gross_notional,
        "max_daily_loss": config.max_daily_loss,
        "max_open_positions": config.max_open_positions,
        "allow_reduce_only_while_draining": (config.allow_reduce_only_while_draining),
        "created_at": config.created_at,
    }


def risk_evaluation_row(evaluation: RiskEvaluation) -> dict[str, object]:
    return {
        "evaluation_id": evaluation.evaluation_id,
        "candidate_id": evaluation.candidate_id,
        "decision": evaluation.decision.value,
        "reason": evaluation.reason,
        "evaluated_at": evaluation.evaluated_at,
        "details": jsonable(evaluation.details),
    }


def risk_halt_row(halt: RiskHalt) -> dict[str, object]:
    return {
        "halt_id": halt.halt_id,
        "environment": halt.environment,
        "account_label": halt.account_label,
        "reason": halt.reason,
        "active": halt.active,
        "created_at": halt.created_at,
        "details": jsonable(halt.details),
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
                            details=jsonable(evaluation.details),
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
                            "details": jsonable(halt.details),
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
                statement = insert(StrategyLiveStateRow).values(values)
                await session.execute(
                    statement.on_conflict_do_update(
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
                        where=(
                            StrategyLiveStateRow.changed_at
                            <= statement.excluded.changed_at
                        ),
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
