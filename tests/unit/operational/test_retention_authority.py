"""Unit tests for RetentionAuthority and RecoveryCatalog domain contracts.

Obeys Astra Architecture Blueprint 2026-09-25 (R1):
- Verifies consumer dependency registration and epoch hashing;
- Verifies prune planning bounded by active consumer watermarks;
- Verifies dependency version fencing preventing deletion on new dependencies;
- Verifies fail-closed execution receipts.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from crypto_momentum_lab.domain.operational.retention_authority import (
    RetentionAuthority,
)
from crypto_momentum_lab.domain.operational.retention_models import (
    PrunePlanStatus,
    PruneReceiptStatus,
    RecoverySpec,
)


def test_recovery_spec_validation() -> None:
    now = datetime.now(UTC)
    spec = RecoverySpec(
        source_dataset="account_position_snapshots",
        earliest_needed_watermark=now,
        reason="Active episode zero crossing cut",
    )
    assert spec.source_dataset == "account_position_snapshots"
    assert spec.earliest_needed_watermark == now

    with pytest.raises(ValueError, match="source_dataset must not be empty"):
        RecoverySpec(
            source_dataset="  ",
            earliest_needed_watermark=now,
        )

    naive = datetime(2026, 9, 20, 0, 0)
    with pytest.raises(
        ValueError, match="earliest_needed_watermark must be timezone-aware"
    ):
        RecoverySpec(
            source_dataset="account_position_snapshots",
            earliest_needed_watermark=naive,
        )


def test_retention_authority_dependency_registration_and_versioning() -> None:
    authority = RetentionAuthority()
    t0 = datetime(2026, 9, 20, 0, 0, tzinfo=UTC)

    v0 = authority.compute_dependency_version("account_position_snapshots")
    assert v0 == "dep_v0_empty"

    spec1 = RecoverySpec(
        source_dataset="account_position_snapshots",
        earliest_needed_watermark=t0,
        reason="Consumer 1 needs Sep 20",
    )
    v1 = authority.register_dependency(
        consumer_id="strategy_primary",
        generation=1,
        recovery_spec=spec1,
    )
    assert v1.startswith("dep_")
    assert v1 != v0

    # Registering another dependency changes the epoch version
    spec2 = RecoverySpec(
        source_dataset="account_position_snapshots",
        earliest_needed_watermark=t0 - timedelta(days=2),
        reason="Consumer 2 needs Sep 18",
    )
    v2 = authority.register_dependency(
        consumer_id="strategy_account_2",
        generation=1,
        recovery_spec=spec2,
    )
    assert v2.startswith("dep_")
    assert v2 != v1


def test_plan_prune_unconstrained_when_no_dependencies() -> None:
    authority = RetentionAuthority()
    requested = datetime(2026, 9, 24, 0, 0, tzinfo=UTC)

    plan = authority.plan_prune(
        dataset_name="market_data_quality_events",
        requested_cutoff=requested,
    )
    assert plan.is_constrained is False
    assert plan.effective_cutoff == requested
    assert plan.binding_consumer_id is None
    assert plan.status == PrunePlanStatus.CREATED


def test_plan_prune_constrained_by_active_consumer_watermark() -> None:
    authority = RetentionAuthority()
    t_needed = datetime(2026, 9, 22, 12, 0, tzinfo=UTC)
    spec = RecoverySpec(
        source_dataset="account_position_snapshots",
        earliest_needed_watermark=t_needed,
        reason="Active trade episode opened Sep 22 12:00",
    )
    authority.register_dependency(
        consumer_id="live_strategy_account_3",
        generation=1,
        recovery_spec=spec,
    )

    requested = datetime(2026, 9, 24, 0, 0, tzinfo=UTC)
    plan = authority.plan_prune(
        dataset_name="account_position_snapshots",
        requested_cutoff=requested,
    )

    # Must be pulled back to t_needed!
    assert plan.is_constrained is True
    assert plan.effective_cutoff == t_needed
    assert plan.binding_consumer_id == "live_strategy_account_3"


def test_execute_prune_success_when_dependency_version_matches() -> None:
    authority = RetentionAuthority()
    t_needed = datetime(2026, 9, 22, 0, 0, tzinfo=UTC)
    spec = RecoverySpec(
        source_dataset="account_position_snapshots",
        earliest_needed_watermark=t_needed,
    )
    dep_version = authority.register_dependency(
        consumer_id="live_strategy",
        generation=1,
        recovery_spec=spec,
    )

    plan = authority.plan_prune(
        dataset_name="account_position_snapshots",
        requested_cutoff=datetime(2026, 9, 24, 0, 0, tzinfo=UTC),
        manifest_hash="sha256_dummy_manifest_123",
    )
    assert plan.expected_dependency_version == dep_version

    # Executor dummy callback
    def dummy_executor(p) -> tuple[int, int]:
        assert p.effective_cutoff == t_needed
        return (100, 100)

    receipt = authority.execute_prune(
        plan=plan,
        expected_dependency_version=dep_version,
        executor_fn=dummy_executor,
    )

    assert receipt.status == PruneReceiptStatus.SUCCESS
    assert receipt.rows_archived == 100
    assert receipt.rows_deleted == 100
    assert receipt.dependency_version_verified == dep_version


def test_execute_prune_rejects_when_new_dependency_registered_after_plan() -> None:
    """Invariant 5 & R1: Fail-closed if new dependency registered before execution."""
    authority = RetentionAuthority()
    t_needed = datetime(2026, 9, 22, 0, 0, tzinfo=UTC)
    spec1 = RecoverySpec(
        source_dataset="account_position_snapshots",
        earliest_needed_watermark=t_needed,
    )
    dep_version_at_plan = authority.register_dependency(
        consumer_id="live_strategy_1",
        generation=1,
        recovery_spec=spec1,
    )

    plan = authority.plan_prune(
        dataset_name="account_position_snapshots",
        requested_cutoff=datetime(2026, 9, 24, 0, 0, tzinfo=UTC),
    )

    # Now, another consumer registers a new requirement needing Sep 20
    spec2 = RecoverySpec(
        source_dataset="account_position_snapshots",
        earliest_needed_watermark=datetime(2026, 9, 20, 0, 0, tzinfo=UTC),
    )
    authority.register_dependency(
        consumer_id="live_strategy_account_4",
        generation=1,
        recovery_spec=spec2,
    )

    # Now attempt to execute the old plan
    executed = False

    def dummy_executor(p) -> tuple[int, int]:
        nonlocal executed
        executed = True
        return (100, 100)

    receipt = authority.execute_prune(
        plan=plan,
        expected_dependency_version=dep_version_at_plan,
        executor_fn=dummy_executor,
    )

    assert executed is False  # Never executed!
    assert receipt.status == PruneReceiptStatus.REJECTED_VERSION_MISMATCH
    assert receipt.rows_deleted == 0
    assert "Dependency version mismatch" in receipt.details


def test_unregister_dependency_requires_explicit_retired_by() -> None:
    authority = RetentionAuthority()
    spec = RecoverySpec(
        source_dataset="account_position_snapshots",
        earliest_needed_watermark=datetime(2026, 9, 22, 0, 0, tzinfo=UTC),
    )
    authority.register_dependency(
        consumer_id="retired_worker",
        generation=1,
        recovery_spec=spec,
    )

    with pytest.raises(
        ValueError, match="retired_by operator/migration must be specified"
    ):
        authority.unregister_dependency(
            consumer_id="retired_worker",
            dataset_name="account_position_snapshots",
            retired_by="  ",
        )

    v_after = authority.unregister_dependency(
        consumer_id="retired_worker",
        dataset_name="account_position_snapshots",
        retired_by="operator_confirmed_safe",
    )
    assert v_after == "dep_v0_empty"
