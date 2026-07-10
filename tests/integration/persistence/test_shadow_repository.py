from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.execution import ShadowSuppressionEvent
from crypto_momentum_lab.persistence.postgres.models import (
    ShadowDecisionMetricRow,
    ShadowDrillResultRow,
    ShadowOrderPlanRow,
    ShadowSessionRow,
    ShadowSuppressionEventRow,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)
from crypto_momentum_lab.persistence.postgres.shadow_repository import (
    PostgresShadowRepository,
)
from crypto_momentum_lab.shadow_operation.models import ShadowOrderPlan, ShadowSession

NOW = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)


@pytest.fixture
async def shadow_repository(
    async_database_url: str,
) -> AsyncIterator[PostgresShadowRepository]:
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        async with session.begin():
            for model in (
                ShadowSuppressionEventRow,
                ShadowDecisionMetricRow,
                ShadowDrillResultRow,
                ShadowOrderPlanRow,
                ShadowSessionRow,
            ):
                await session.execute(delete(model))
    yield PostgresShadowRepository(factory)
    await engine.dispose()


async def test_save_shadow_suppression_is_idempotent_by_order_plan(
    shadow_repository: PostgresShadowRepository,
) -> None:
    await shadow_repository.start_session(_session())
    await shadow_repository.save_order_plan(_plan())
    event = _suppression()

    await shadow_repository.save_shadow_suppression(event)
    await shadow_repository.save_shadow_suppression(event)

    plans, suppressions, _, _ = await shadow_repository.load_report_rows("shadow-1")
    assert len(plans) == 1
    assert suppressions == (event,)
    assert await shadow_repository.load_unresolved_plans("shadow-1") == ()


def _session() -> ShadowSession:
    return ShadowSession(
        run_id="shadow-1",
        account_label="primary",
        strategy_name="compression_breakout",
        strategy_config_hash="a" * 64,
        state="running",
        account_readiness="ready_readonly",
        started_at=NOW,
        ended_at=None,
        details={},
    )


def _plan() -> ShadowOrderPlan:
    return ShadowOrderPlan(
        order_plan_id="plan-1",
        run_id="shadow-1",
        order_intent_id="intent-1",
        symbol="BTCUSDT",
        decision_state="approved",
        account_readiness="ready_readonly",
        market_freshness="fresh",
        risk_result="approved",
        state_closed_at=NOW,
        created_at=NOW,
        order_payload={"symbol": "BTCUSDT"},
    )


def _suppression() -> ShadowSuppressionEvent:
    return ShadowSuppressionEvent(
        order_plan_id="plan-1",
        client_order_id="cml_12345678901234567890123456789012",
        suppressed_at=NOW,
        reason="shadow_submit_policy",
        order_payload={"symbol": "BTCUSDT"},
    )
