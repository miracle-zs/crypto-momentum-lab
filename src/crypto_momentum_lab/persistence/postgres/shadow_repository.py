from dataclasses import asdict
from datetime import datetime
from typing import Any, cast

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.execution import ShadowSuppressionEvent
from crypto_momentum_lab.domain.market.models import JsonValue
from crypto_momentum_lab.persistence.postgres.models import (
    ShadowDecisionMetricRow,
    ShadowDrillResultRow,
    ShadowOrderPlanRow,
    ShadowSessionRow,
    ShadowSuppressionEventRow,
)
from crypto_momentum_lab.shadow_operation.models import (
    ShadowDecisionMetric,
    ShadowDrillResult,
    ShadowOrderPlan,
    ShadowSession,
)


class PostgresShadowRepository:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._session_factory = session_factory

    async def start_session(self, session_record: ShadowSession) -> None:
        await self._insert(ShadowSessionRow, asdict(session_record))

    async def end_session(
        self,
        run_id: str,
        *,
        state: str,
        ended_at: datetime,
    ) -> None:
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    update(ShadowSessionRow)
                    .where(ShadowSessionRow.run_id == run_id)
                    .values(state=state, ended_at=ended_at)
                )

    async def save_order_plan(self, plan: ShadowOrderPlan) -> None:
        await self._insert(ShadowOrderPlanRow, asdict(plan))

    async def save_shadow_suppression(
        self,
        event: ShadowSuppressionEvent,
    ) -> None:
        await self._insert(ShadowSuppressionEventRow, asdict(event))

    async def save_metric(self, metric: ShadowDecisionMetric) -> None:
        await self._insert(ShadowDecisionMetricRow, asdict(metric))

    async def save_drill_result(self, result: ShadowDrillResult) -> None:
        await self._insert(ShadowDrillResultRow, asdict(result))

    async def load_unresolved_plans(self, run_id: str) -> tuple[ShadowOrderPlan, ...]:
        async with self._session_factory() as session:
            rows = (
                await session.scalars(
                    select(ShadowOrderPlanRow)
                    .outerjoin(
                        ShadowSuppressionEventRow,
                        ShadowSuppressionEventRow.order_plan_id
                        == ShadowOrderPlanRow.order_plan_id,
                    )
                    .where(
                        ShadowOrderPlanRow.run_id == run_id,
                        ShadowSuppressionEventRow.order_plan_id.is_(None),
                    )
                    .order_by(ShadowOrderPlanRow.created_at)
                )
            ).all()
        return tuple(_plan_from_row(row) for row in rows)

    async def load_report_rows(
        self,
        run_id: str,
    ) -> tuple[
        tuple[ShadowOrderPlan, ...],
        tuple[ShadowSuppressionEvent, ...],
        tuple[ShadowDecisionMetric, ...],
        tuple[ShadowDrillResult, ...],
    ]:
        async with self._session_factory() as session:
            plans = (
                await session.scalars(
                    select(ShadowOrderPlanRow)
                    .where(ShadowOrderPlanRow.run_id == run_id)
                    .order_by(ShadowOrderPlanRow.created_at)
                )
            ).all()
            suppressions = (
                await session.scalars(
                    select(ShadowSuppressionEventRow)
                    .join(ShadowOrderPlanRow)
                    .where(ShadowOrderPlanRow.run_id == run_id)
                    .order_by(ShadowSuppressionEventRow.suppressed_at)
                )
            ).all()
            metrics = (
                await session.scalars(
                    select(ShadowDecisionMetricRow)
                    .where(ShadowDecisionMetricRow.run_id == run_id)
                    .order_by(ShadowDecisionMetricRow.occurred_at)
                )
            ).all()
            drills = (
                await session.scalars(
                    select(ShadowDrillResultRow)
                    .where(ShadowDrillResultRow.run_id == run_id)
                    .order_by(ShadowDrillResultRow.occurred_at)
                )
            ).all()
        return (
            tuple(_plan_from_row(row) for row in plans),
            tuple(
                ShadowSuppressionEvent(
                    order_plan_id=row.order_plan_id,
                    client_order_id=row.client_order_id,
                    suppressed_at=row.suppressed_at,
                    reason=row.reason,
                    order_payload=cast(dict[str, JsonValue], row.order_payload),
                )
                for row in suppressions
            ),
            tuple(
                ShadowDecisionMetric(
                    metric_id=row.metric_id,
                    run_id=row.run_id,
                    symbol=row.symbol,
                    category=row.category,
                    reason=row.reason,
                    occurred_at=row.occurred_at,
                    details=cast(dict[str, JsonValue], row.details),
                )
                for row in metrics
            ),
            tuple(
                ShadowDrillResult(
                    drill_result_id=row.drill_result_id,
                    run_id=row.run_id,
                    drill_name=row.drill_name,
                    outcome=row.outcome,
                    occurred_at=row.occurred_at,
                    details=cast(dict[str, JsonValue], row.details),
                )
                for row in drills
            ),
        )

    async def _insert(self, model: Any, values: dict[str, object]) -> None:
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    insert(model).values(values).on_conflict_do_nothing()
                )


def _plan_from_row(row: ShadowOrderPlanRow) -> ShadowOrderPlan:
    return ShadowOrderPlan(
        order_plan_id=row.order_plan_id,
        run_id=row.run_id,
        order_intent_id=row.order_intent_id,
        symbol=row.symbol,
        decision_state=row.decision_state,
        account_readiness=row.account_readiness,
        market_freshness=row.market_freshness,
        risk_result=row.risk_result,
        state_closed_at=row.state_closed_at,
        created_at=row.created_at,
        order_payload=cast(dict[str, JsonValue], row.order_payload),
    )
