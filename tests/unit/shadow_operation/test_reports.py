from datetime import UTC, datetime, timedelta

from crypto_momentum_lab.domain.execution import ShadowSuppressionEvent
from crypto_momentum_lab.shadow_operation.models import (
    ShadowDecisionMetric,
    ShadowOrderPlan,
)
from crypto_momentum_lab.shadow_operation.reports import build_shadow_report

NOW = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)


def test_shadow_report_counts_rejected_and_suppressed_orders() -> None:
    report = build_shadow_report(
        plans=(_plan(100),),
        suppressions=(_suppression(),),
        metrics=(
            _metric("signal"),
            _metric("approved_intent"),
            _metric("rejected", "stale_market_state"),
        ),
        drills=(),
    )

    assert report.signal_count == 1
    assert report.approved_intent_count == 1
    assert report.rejected_by_reason == {"stale_market_state": 1}
    assert report.would_submit_count == 1
    assert report.suppression_count == 1


def test_shadow_report_includes_latency_buckets() -> None:
    report = build_shadow_report(
        plans=(_plan(100), _plan(300, suffix="2")),
        suppressions=(),
        metrics=(),
        drills=(),
    )

    assert report.latency_ms_p50 == 100
    assert report.latency_ms_p95 == 300


def _plan(latency_ms: int, suffix: str = "1") -> ShadowOrderPlan:
    return ShadowOrderPlan(
        order_plan_id=f"plan-{suffix}",
        run_id="shadow-1",
        order_intent_id=f"intent-{suffix}",
        symbol="BTCUSDT",
        decision_state="approved",
        account_readiness="ready_readonly",
        market_freshness="fresh",
        risk_result="approved",
        state_closed_at=NOW,
        created_at=NOW + timedelta(milliseconds=latency_ms),
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


def _metric(category: str, reason: str | None = None) -> ShadowDecisionMetric:
    return ShadowDecisionMetric(
        metric_id=f"metric-{category}-{reason}",
        run_id="shadow-1",
        symbol="BTCUSDT",
        category=category,
        reason=reason,
        occurred_at=NOW,
        details={},
    )
