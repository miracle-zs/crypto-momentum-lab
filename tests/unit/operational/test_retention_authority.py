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


def test_execute_prune_rejects_when_caller_passes_current_version_with_stale_plan(
) -> None:
    """Regression test: Stale PrunePlan cannot bypass dependency fencing."""
    authority = RetentionAuthority()
    t_needed = datetime(2026, 9, 22, 0, 0, tzinfo=UTC)
    spec1 = RecoverySpec(
        source_dataset="account_position_snapshots",
        earliest_needed_watermark=t_needed,
    )
    authority.register_dependency(
        consumer_id="live_strategy_1",
        generation=1,
        recovery_spec=spec1,
    )

    stale_plan = authority.plan_prune(
        dataset_name="account_position_snapshots",
        requested_cutoff=datetime(2026, 9, 24, 0, 0, tzinfo=UTC),
    )

    # New dependency registered after plan was created
    spec2 = RecoverySpec(
        source_dataset="account_position_snapshots",
        earliest_needed_watermark=datetime(2026, 9, 20, 0, 0, tzinfo=UTC),
    )
    latest_version = authority.register_dependency(
        consumer_id="live_strategy_2",
        generation=1,
        recovery_spec=spec2,
    )

    executed = False

    def dummy_executor(p) -> tuple[int, int]:
        nonlocal executed
        executed = True
        return (100, 100)

    # Caller tries to pass latest_version with stale_plan
    receipt = authority.execute_prune(
        plan=stale_plan,
        expected_dependency_version=latest_version,
        executor_fn=dummy_executor,
    )

    assert executed is False
    assert receipt.status == PruneReceiptStatus.REJECTED_VERSION_MISMATCH
    assert receipt.rows_deleted == 0


def test_bind_manifest_locks_effective_cutoff_and_rejects_version_change() -> None:
    from crypto_momentum_lab.domain.operational.retention_authority import (
        DependencyVersionConflictError,
    )

    authority = RetentionAuthority()
    spec = RecoverySpec(
        source_dataset="market_data",
        earliest_needed_watermark=datetime(2026, 9, 21, 0, 0, tzinfo=UTC),
    )
    authority.register_dependency(consumer_id="c1", generation=1, recovery_spec=spec)

    plan = authority.plan_prune(
        dataset_name="market_data",
        requested_cutoff=datetime(2026, 9, 24, 0, 0, tzinfo=UTC),
    )
    assert plan.effective_cutoff == datetime(2026, 9, 21, 0, 0, tzinfo=UTC)

    # Bind manifest successfully
    bound = authority.bind_manifest(plan, manifest_hash="sha256_fake_manifest")
    assert bound.manifest_hash == "sha256_fake_manifest"
    assert bound.effective_cutoff == plan.effective_cutoff

    # Verify atomic fence passes when version is unchanged
    authority.verify_fence(bound)

    # If dependency changes mid-execution, verify_fence must fail-closed
    spec2 = RecoverySpec(
        source_dataset="market_data",
        earliest_needed_watermark=datetime(2026, 9, 19, 0, 0, tzinfo=UTC),
    )
    authority.register_dependency(consumer_id="c2", generation=1, recovery_spec=spec2)

    with pytest.raises(
        DependencyVersionConflictError,
        match="Dependency epoch fence violation mid-prune",
    ):
        authority.verify_fence(bound)


def test_dataset_scope_canonical_id_and_locks() -> None:
    from crypto_momentum_lab.domain.operational.retention_models import (
        DatasetId,
        DatasetScope,
        resolve_dataset_scope,
    )

    scope1 = resolve_dataset_scope("account_snapshots_binance_prod")
    assert isinstance(scope1, DatasetScope)
    assert scope1.dataset_id == DatasetId.ACCOUNT_SNAPSHOTS
    assert scope1.account_label == "binance_prod"
    assert scope1.canonical_id == "account_snapshots_binance_prod"
    # All physical tables covered and sorted
    assert "retention_account_balance_snapshots" in scope1.advisory_lock_keys
    assert "retention_account_position_snapshots" in scope1.advisory_lock_keys
    assert list(scope1.advisory_lock_keys) == sorted(scope1.advisory_lock_keys)

    scope2 = resolve_dataset_scope("account_position_snapshots")
    assert scope2.dataset_id == DatasetId.ACCOUNT_POSITIONS
    assert "retention_account_position_snapshots" in scope2.advisory_lock_keys

    # Generic / test table fallback
    scope3 = resolve_dataset_scope("custom_test_table")
    assert scope3.dataset_id == DatasetId.GENERIC
    assert scope3.canonical_id == "custom_test_table"
    assert scope3.advisory_lock_keys == ("retention_custom_test_table",)


def test_cross_dataset_dependency_resolution() -> None:
    """Proves that a dependency on a child table (e.g. account_position_snapshots)
    is observed and constrains a plan for account_snapshots_binance_prod.
    """
    authority = RetentionAuthority()
    t_needed = datetime(2026, 9, 21, 10, 0, tzinfo=UTC)
    spec = RecoverySpec(
        source_dataset="account_position_snapshots",
        earliest_needed_watermark=t_needed,
        reason="Active positions need protection",
    )
    authority.register_dependency(
        consumer_id="live_active_positions",
        generation=1,
        recovery_spec=spec,
    )

    requested = datetime(2026, 9, 24, 0, 0, tzinfo=UTC)
    plan = authority.plan_prune(
        dataset_name="account_snapshots_binance_prod",
        requested_cutoff=requested,
    )

    # Must be constrained by the child table's dependency!
    assert plan.is_constrained is True
    assert plan.effective_cutoff == t_needed
    assert plan.binding_consumer_id == "live_active_positions"


def test_execute_prune_with_structured_prune_outcome() -> None:
    from crypto_momentum_lab.domain.operational.retention_models import PruneOutcome

    authority = RetentionAuthority()
    plan = authority.plan_prune(
        dataset_name="market_data",
        requested_cutoff=datetime(2026, 9, 24, 0, 0, tzinfo=UTC),
    )

    outcome = PruneOutcome(
        rows_archived=100,
        rows_deleted=100,
        partitions_dropped=2,
        bytes_deleted=20480,
        batches=4,
        status=PruneReceiptStatus.SUCCESS,
        details="Archived 100 rows, deleted 100 rows, dropped 2 partitions.",
    )

    receipt = authority.execute_prune(
        plan=plan,
        expected_dependency_version=plan.expected_dependency_version,
        executor_fn=lambda p: outcome,
    )

    assert receipt.status == PruneReceiptStatus.SUCCESS
    assert receipt.rows_archived == 100
    assert receipt.rows_deleted == 100
    assert receipt.partitions_dropped == 2
    assert receipt.bytes_deleted == 20480
    assert receipt.batches == 4
    assert "dropped 2 partitions" in receipt.details

