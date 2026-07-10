from datetime import UTC, datetime

import pytest

from crypto_momentum_lab.shadow_operation.models import ShadowOrderPlan


def test_shadow_order_plan_requires_order_payload() -> None:
    now = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)

    with pytest.raises(ValueError, match="order_payload must not be empty"):
        ShadowOrderPlan(
            order_plan_id="plan-1",
            run_id="shadow-1",
            order_intent_id="intent-1",
            symbol="BTCUSDT",
            decision_state="approved",
            account_readiness="ready_readonly",
            market_freshness="fresh",
            risk_result="approved",
            state_closed_at=now,
            created_at=now,
            order_payload={},
        )
