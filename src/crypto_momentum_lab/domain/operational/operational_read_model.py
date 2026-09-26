"""OperationalReadModel domain service and unified health schema (R5).

Obeys Astra Architecture Blueprint 2026-09-25:
- read_health(scope, dimensions, evidence_cut) -> OperationalView
- Health dimensions:
  1. process_liveness
  2. consumption_lag
  3. fact_integrity
  4. executable_capability
  5. reconciliation_concordance
- Green process heartbeat CANNOT mask factual gaps, lag, or ledger discordance;
- Retains original source_as_of without cache-hit time corruption.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


class HealthDimensionName(StrEnum):
    """Authoritative operational health dimensions."""

    PROCESS_LIVENESS = "process_liveness"
    CONSUMPTION_LAG = "consumption_lag"
    FACT_INTEGRITY = "fact_integrity"
    EXECUTABLE_CAPABILITY = "executable_capability"
    RECONCILIATION_CONCORDANCE = "reconciliation_concordance"


class HealthDimensionStatus(StrEnum):
    """Reliability status for individual health dimensions and overall view."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    STALE = "stale"
    UNKNOWN = "unknown"
    CRITICAL = "critical"


@dataclass(frozen=True, slots=True)
class HealthDimension:
    """Status assessment for a single operational dimension."""

    name: HealthDimensionName
    status: HealthDimensionStatus
    details: str
    observed_at: datetime
    metric_value: float | None = None

    def __post_init__(self) -> None:
        if self.observed_at.tzinfo is None:
            raise ValueError("observed_at must be timezone-aware")
        if not self.details.strip():
            raise ValueError("details must not be empty")


@dataclass(frozen=True, slots=True)
class OperationalView:
    """Authoritative composite health view for an operational scope."""

    scope: str
    overall_status: HealthDimensionStatus
    dimensions: tuple[HealthDimension, ...]
    source_as_of: datetime
    is_execution_ready: bool
    evaluated_at: datetime
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.scope.strip():
            raise ValueError("scope must not be empty")
        if self.source_as_of.tzinfo is None:
            raise ValueError("source_as_of must be timezone-aware")
        if self.evaluated_at.tzinfo is None:
            raise ValueError("evaluated_at must be timezone-aware")


def read_health(
    scope: str,
    dimensions: tuple[HealthDimension, ...],
    evidence_cut: datetime | None = None,
) -> OperationalView:
    """Evaluates multi-dimensional operational health.

    Invariants:
    - Green process liveness (heartbeat) CANNOT override degraded or
      critical dimensions;
    - If any dimension is CRITICAL -> overall is CRITICAL;
    - Else if any dimension is DEGRADED -> overall is DEGRADED;
    - Else if any dimension is STALE -> overall is STALE;
    - Else if any dimension is UNKNOWN -> overall is UNKNOWN;
    - Else overall is HEALTHY;
    - Execution readiness requires overall HEALTHY and zero critical or
      degraded dimensions.
    """
    if not dimensions:
        now = evidence_cut or datetime.now(UTC)
        return OperationalView(
            scope=scope,
            overall_status=HealthDimensionStatus.UNKNOWN,
            dimensions=(),
            source_as_of=now,
            is_execution_ready=False,
            evaluated_at=now,
            details={"reason": "no_health_dimensions_provided"},
        )

    # Determine overall status by severity priority:
    # CRITICAL > DEGRADED > STALE > UNKNOWN > HEALTHY
    statuses = {d.status for d in dimensions}
    if HealthDimensionStatus.CRITICAL in statuses:
        overall = HealthDimensionStatus.CRITICAL
    elif HealthDimensionStatus.DEGRADED in statuses:
        overall = HealthDimensionStatus.DEGRADED
    elif HealthDimensionStatus.STALE in statuses:
        overall = HealthDimensionStatus.STALE
    elif HealthDimensionStatus.UNKNOWN in statuses:
        overall = HealthDimensionStatus.UNKNOWN
    else:
        overall = HealthDimensionStatus.HEALTHY

    # Execution ready only when overall is healthy
    is_ready = overall == HealthDimensionStatus.HEALTHY

    # Source as-of is the earliest observed_at among dimensions (conservative freshness)
    source_as_of = min((d.observed_at for d in dimensions), default=datetime.now(UTC))
    eval_time = evidence_cut or datetime.now(UTC)

    return OperationalView(
        scope=scope,
        overall_status=overall,
        dimensions=dimensions,
        source_as_of=source_as_of,
        is_execution_ready=is_ready,
        evaluated_at=eval_time,
        details={
            "dimension_count": len(dimensions),
            "unhealthy_dimensions": [
                f"{d.name.value}:{d.status.value}"
                for d in dimensions
                if d.status != HealthDimensionStatus.HEALTHY
            ],
        },
    )


def aggregate_operational_views(
    views: tuple[OperationalView, ...],
    composite_scope: str = "system",
    evaluated_at: datetime | None = None,
) -> OperationalView:
    """Aggregates multiple operational views across scopes/accounts.

    Invariants (Blueprint R6 / Section 12.3):
    - 跨账户汇总必须同区间同口径；部分未知不能被其它账户绿色抵消；
    - If any view is CRITICAL -> overall is CRITICAL;
    - Else if any view is DEGRADED -> overall is DEGRADED;
    - Else if any view is STALE -> overall is STALE;
    - Else if any view is UNKNOWN -> overall is UNKNOWN;
    - Only when ALL views are HEALTHY is the composite HEALTHY;
    - source_as_of preserves the earliest timestamp (cache HIT or green peer
      cannot extend stale evidence);
    - is_execution_ready is True ONLY if all subviews are execution-ready.
    """
    if not views:
        now = evaluated_at or datetime.now(UTC)
        return OperationalView(
            scope=composite_scope,
            overall_status=HealthDimensionStatus.UNKNOWN,
            dimensions=(),
            source_as_of=now,
            is_execution_ready=False,
            evaluated_at=now,
            details={"reason": "no_subviews_provided_for_aggregation"},
        )

    # Severity priority: CRITICAL > DEGRADED > STALE > UNKNOWN > HEALTHY
    statuses = {v.overall_status for v in views}
    if HealthDimensionStatus.CRITICAL in statuses:
        overall = HealthDimensionStatus.CRITICAL
    elif HealthDimensionStatus.DEGRADED in statuses:
        overall = HealthDimensionStatus.DEGRADED
    elif HealthDimensionStatus.STALE in statuses:
        overall = HealthDimensionStatus.STALE
    elif HealthDimensionStatus.UNKNOWN in statuses:
        overall = HealthDimensionStatus.UNKNOWN
    else:
        overall = HealthDimensionStatus.HEALTHY

    is_ready = all(v.is_execution_ready for v in views) and (
        overall == HealthDimensionStatus.HEALTHY
    )
    source_as_of = min((v.source_as_of for v in views), default=datetime.now(UTC))
    eval_time = evaluated_at or datetime.now(UTC)

    # Flatten dimensions tagging origin scope
    all_dims: list[HealthDimension] = []
    for v in views:
        for d in v.dimensions:
            all_dims.append(
                HealthDimension(
                    name=d.name,
                    status=d.status,
                    details=f"[{v.scope}] {d.details}",
                    observed_at=d.observed_at,
                    metric_value=d.metric_value,
                )
            )

    unhealthy_scopes = [
        f"{v.scope}:{v.overall_status.value}"
        for v in views
        if v.overall_status != HealthDimensionStatus.HEALTHY
    ]

    return OperationalView(
        scope=composite_scope,
        overall_status=overall,
        dimensions=tuple(all_dims),
        source_as_of=source_as_of,
        is_execution_ready=is_ready,
        evaluated_at=eval_time,
        details={
            "subview_count": len(views),
            "unhealthy_scopes": unhealthy_scopes,
            "subview_statuses": {v.scope: v.overall_status.value for v in views},
        },
    )


def evaluate_standard_health(
    scope: str,
    *,
    liveness_ok: bool,
    liveness_details: str = "daemon_active",
    lag_seconds: float = 0.0,
    max_lag_seconds: float = 5.0,
    fact_gaps_count: int = 0,
    capability_permitted: bool = True,
    capability_reason: str = "normal",
    reconciliation_matched: bool = True,
    reconciliation_details: str = "ledger_matches_exchange",
    observed_at: datetime | None = None,
) -> OperationalView:
    """Builds an OperationalView with the 5 authoritative R6 dimensions."""
    now = observed_at or datetime.now(UTC)

    # 1. Process Liveness
    live_status = (
        HealthDimensionStatus.HEALTHY
        if liveness_ok
        else HealthDimensionStatus.CRITICAL
    )
    details_msg = (
        liveness_details
        if liveness_ok
        else f"liveness_failed: {liveness_details}"
    )
    dim_liveness = HealthDimension(
        name=HealthDimensionName.PROCESS_LIVENESS,
        status=live_status,
        details=details_msg,
        observed_at=now,
    )


    # 2. Consumption Lag
    if lag_seconds <= max_lag_seconds:
        lag_status = HealthDimensionStatus.HEALTHY
    elif lag_seconds <= max_lag_seconds * 5:
        lag_status = HealthDimensionStatus.DEGRADED
    else:
        lag_status = HealthDimensionStatus.CRITICAL
    dim_lag = HealthDimension(
        name=HealthDimensionName.CONSUMPTION_LAG,
        status=lag_status,
        details=f"lag_{lag_seconds:.1f}s",
        observed_at=now,
        metric_value=lag_seconds,
    )

    # 3. Fact Integrity
    if fact_gaps_count == 0:
        fact_status = HealthDimensionStatus.HEALTHY
        fact_details = "zero_fact_gaps"
    elif fact_gaps_count <= 2:
        fact_status = HealthDimensionStatus.DEGRADED
        fact_details = f"{fact_gaps_count}_fact_gaps_detected"
    else:
        fact_status = HealthDimensionStatus.CRITICAL
        fact_details = f"{fact_gaps_count}_critical_fact_gaps"
    dim_fact = HealthDimension(
        name=HealthDimensionName.FACT_INTEGRITY,
        status=fact_status,
        details=fact_details,
        observed_at=now,
        metric_value=float(fact_gaps_count),
    )

    # 4. Executable Capability
    cap_status = (
        HealthDimensionStatus.HEALTHY
        if capability_permitted
        else HealthDimensionStatus.DEGRADED
    )
    dim_cap = HealthDimension(
        name=HealthDimensionName.EXECUTABLE_CAPABILITY,
        status=cap_status,
        details=capability_reason,
        observed_at=now,
    )

    # 5. Reconciliation Concordance
    rec_status = (
        HealthDimensionStatus.HEALTHY
        if reconciliation_matched
        else HealthDimensionStatus.CRITICAL
    )
    dim_rec = HealthDimension(
        name=HealthDimensionName.RECONCILIATION_CONCORDANCE,
        status=rec_status,
        details=(
            reconciliation_details
            if reconciliation_matched
            else f"reconciliation_mismatch: {reconciliation_details}"
        ),
        observed_at=now,
    )

    return read_health(
        scope=scope,
        dimensions=(dim_liveness, dim_lag, dim_fact, dim_cap, dim_rec),
        evidence_cut=now,
    )

