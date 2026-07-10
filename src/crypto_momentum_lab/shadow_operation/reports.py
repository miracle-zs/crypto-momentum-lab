from dataclasses import dataclass

from crypto_momentum_lab.domain.execution import ShadowSuppressionEvent
from crypto_momentum_lab.shadow_operation.models import (
    ShadowDecisionMetric,
    ShadowDrillResult,
    ShadowOrderPlan,
)


@dataclass(frozen=True, slots=True)
class ShadowReport:
    signal_count: int
    approved_intent_count: int
    rejected_by_reason: dict[str, int]
    would_submit_count: int
    suppression_count: int
    stale_data_block_count: int
    account_risk_block_count: int
    min_notional_block_count: int
    latency_ms_p50: float | None
    latency_ms_p95: float | None
    drill_outcomes: dict[str, str]


def build_shadow_report(
    *,
    plans: tuple[ShadowOrderPlan, ...],
    suppressions: tuple[ShadowSuppressionEvent, ...],
    metrics: tuple[ShadowDecisionMetric, ...],
    drills: tuple[ShadowDrillResult, ...],
) -> ShadowReport:
    rejected: dict[str, int] = {}
    for metric in metrics:
        if metric.category == "rejected":
            reason = metric.reason or "unknown"
            rejected[reason] = rejected.get(reason, 0) + 1
    latencies = sorted(
        (plan.created_at - plan.state_closed_at).total_seconds() * 1000
        for plan in plans
    )
    return ShadowReport(
        signal_count=sum(metric.category == "signal" for metric in metrics),
        approved_intent_count=sum(
            metric.category == "approved_intent" for metric in metrics
        ),
        rejected_by_reason=rejected,
        would_submit_count=len(plans),
        suppression_count=len(suppressions),
        stale_data_block_count=sum(
            metric.category == "stale_data_block" for metric in metrics
        ),
        account_risk_block_count=sum(
            metric.category in {"account_block", "risk_block"} for metric in metrics
        ),
        min_notional_block_count=sum(
            metric.reason == "below_min_notional" for metric in metrics
        ),
        latency_ms_p50=_percentile(latencies, 0.50),
        latency_ms_p95=_percentile(latencies, 0.95),
        drill_outcomes={drill.drill_name: drill.outcome for drill in drills},
    )


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    index = round((len(values) - 1) * percentile)
    return values[index]
