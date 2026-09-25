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
