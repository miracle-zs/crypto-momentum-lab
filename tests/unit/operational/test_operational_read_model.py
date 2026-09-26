"""Unit tests for OperationalReadModel and multi-dimensional health schema (R5).

Tests:
1. Healthy dimensions yield overall HEALTHY and is_execution_ready=True;
2. Invariant: Green process_liveness CANNOT mask fact_integrity or
   reconciliation failures;
3. Status priority resolution: CRITICAL > DEGRADED > STALE > UNKNOWN > HEALTHY;
4. source_as_of preserves conservative earliest observation time;
5. Empty dimensions safely result in UNKNOWN with is_execution_ready=False.
"""

from datetime import UTC, datetime, timedelta

from crypto_momentum_lab.domain.operational.operational_read_model import (
    HealthDimension,
    HealthDimensionName,
    HealthDimensionStatus,
    aggregate_operational_views,
    evaluate_standard_health,
    read_health,
)



def test_operational_read_model_all_healthy() -> None:
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)

    dims = (
        HealthDimension(
            name=HealthDimensionName.PROCESS_LIVENESS,
            status=HealthDimensionStatus.HEALTHY,
            details="daemon running normally",
            observed_at=t0,
        ),
        HealthDimension(
            name=HealthDimensionName.CONSUMPTION_LAG,
            status=HealthDimensionStatus.HEALTHY,
            details="lag 0.2s within threshold",
            observed_at=t0,
            metric_value=0.2,
        ),
        HealthDimension(
            name=HealthDimensionName.FACT_INTEGRITY,
            status=HealthDimensionStatus.HEALTHY,
            details="zero known gaps",
            observed_at=t0,
        ),
        HealthDimension(
            name=HealthDimensionName.EXECUTABLE_CAPABILITY,
            status=HealthDimensionStatus.HEALTHY,
            details="all actions permitted",
            observed_at=t0,
        ),
        HealthDimension(
            name=HealthDimensionName.RECONCILIATION_CONCORDANCE,
            status=HealthDimensionStatus.HEALTHY,
            details="ledger reconciled with exchange",
            observed_at=t0,
        ),
    )

    view = read_health("primary", dims, evidence_cut=t0)
    assert view.scope == "primary"
    assert view.overall_status == HealthDimensionStatus.HEALTHY
    assert view.is_execution_ready is True
    assert view.source_as_of == t0
    assert len(view.details["unhealthy_dimensions"]) == 0


def test_green_heartbeat_cannot_mask_degraded_dimensions() -> None:
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)

    dims = (
        # Green process heartbeat
        HealthDimension(
            name=HealthDimensionName.PROCESS_LIVENESS,
            status=HealthDimensionStatus.HEALTHY,
            details="process heartbeat active",
            observed_at=t0,
        ),
        # Broken ledger concordance
        HealthDimension(
            name=HealthDimensionName.RECONCILIATION_CONCORDANCE,
            status=HealthDimensionStatus.CRITICAL,
            details="ledger position discrepancy detected",
            observed_at=t0,
        ),
        # Degraded fact coverage
        HealthDimension(
            name=HealthDimensionName.FACT_INTEGRITY,
            status=HealthDimensionStatus.DEGRADED,
            details="fill gaps detected",
            observed_at=t0,
        ),
    )

    view = read_health("primary", dims, evidence_cut=t0)
    # Heartbeat must NOT mask the critical error
    assert view.overall_status == HealthDimensionStatus.CRITICAL
    assert view.is_execution_ready is False
    assert "reconciliation_concordance:critical" in view.details["unhealthy_dimensions"]
    assert "fact_integrity:degraded" in view.details["unhealthy_dimensions"]


def test_source_as_of_conservative_timestamp() -> None:
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
    t_old = t0 - timedelta(minutes=5)
    t_new = t0 - timedelta(seconds=10)

    dims = (
        HealthDimension(
            name=HealthDimensionName.PROCESS_LIVENESS,
            status=HealthDimensionStatus.HEALTHY,
            details="healthy",
            observed_at=t_new,
        ),
        HealthDimension(
            name=HealthDimensionName.CONSUMPTION_LAG,
            status=HealthDimensionStatus.HEALTHY,
            details="healthy",
            observed_at=t_old,
        ),
    )

    view = read_health("primary", dims, evidence_cut=t0)
    # Conservative timestamp must be t_old
    assert view.source_as_of == t_old


def test_empty_dimensions_result_in_unknown() -> None:
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
    view = read_health("primary", (), evidence_cut=t0)

    assert view.overall_status == HealthDimensionStatus.UNKNOWN
    assert view.is_execution_ready is False


def test_aggregate_operational_views_partial_unknown_not_masked() -> None:
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)

    # Account 1 is HEALTHY
    view_1 = evaluate_standard_health(
        scope="account_1",
        liveness_ok=True,
        lag_seconds=0.5,
        fact_gaps_count=0,
        capability_permitted=True,
        reconciliation_matched=True,
        observed_at=t0,
    )
    assert view_1.overall_status == HealthDimensionStatus.HEALTHY
    assert view_1.is_execution_ready is True

    # Account 2 has UNKNOWN status (e.g. no dimensions)
    view_2 = read_health("account_2", (), evidence_cut=t0)
    assert view_2.overall_status == HealthDimensionStatus.UNKNOWN

    # Aggregate: "部分未知不能被其它账户绿色抵消"
    composite = aggregate_operational_views((view_1, view_2), composite_scope="fleet", evaluated_at=t0)
    assert composite.overall_status == HealthDimensionStatus.UNKNOWN
    assert composite.is_execution_ready is False
    assert "account_2:unknown" in composite.details["unhealthy_scopes"]


def test_aggregate_operational_views_critical_escalation() -> None:
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
    t_stale = t0 - timedelta(minutes=10)

    # Account 1 is HEALTHY
    view_1 = evaluate_standard_health(
        scope="account_1",
        liveness_ok=True,
        lag_seconds=0.1,
        fact_gaps_count=0,
        capability_permitted=True,
        reconciliation_matched=True,
        observed_at=t0,
    )

    # Account 2 has severe lag and failed reconciliation -> CRITICAL
    view_2 = evaluate_standard_health(
        scope="account_2",
        liveness_ok=True,
        lag_seconds=120.0,
        fact_gaps_count=3,
        capability_permitted=False,
        reconciliation_matched=False,
        reconciliation_details="position_mismatch",
        observed_at=t_stale,
    )
    assert view_2.overall_status == HealthDimensionStatus.CRITICAL

    composite = aggregate_operational_views((view_1, view_2), composite_scope="fleet", evaluated_at=t0)
    assert composite.overall_status == HealthDimensionStatus.CRITICAL
    assert composite.is_execution_ready is False
    # Preserves earliest source_as_of (t_stale)
    assert composite.source_as_of == t_stale
    assert "account_2:critical" in composite.details["unhealthy_scopes"]


def test_evaluate_standard_health_dimensions_structure() -> None:
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)

    view = evaluate_standard_health(
        scope="primary",
        liveness_ok=True,
        lag_seconds=1.2,
        fact_gaps_count=0,
        capability_permitted=True,
        reconciliation_matched=True,
        observed_at=t0,
    )

    assert view.scope == "primary"
    assert view.overall_status == HealthDimensionStatus.HEALTHY
    assert view.is_execution_ready is True
    assert len(view.dimensions) == 5

    names = {d.name for d in view.dimensions}
    assert names == {
        HealthDimensionName.PROCESS_LIVENESS,
        HealthDimensionName.CONSUMPTION_LAG,
        HealthDimensionName.FACT_INTEGRITY,
        HealthDimensionName.EXECUTABLE_CAPABILITY,
        HealthDimensionName.RECONCILIATION_CONCORDANCE,
    }

