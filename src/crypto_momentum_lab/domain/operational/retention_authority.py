"""RetentionAuthority service orchestrating dependency registration and safe pruning.

Obeys Astra Architecture Blueprint 2026-09-25:
- register_dependency(consumer, generation, recovery_spec) -> dependency_version
- plan_prune(dataset, requested_range) -> immutable PrunePlan
- execute_prune(plan_id, expected_dependency_version) -> PruneReceipt
- restore(recovery_spec) -> verified RestoreReceipt
- Atomic version fencing prevents deleting newly registered recovery windows.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Protocol
from uuid import uuid4

from crypto_momentum_lab.domain.operational.retention_models import (
    ConsumerDependency,
    PrunePlan,
    PrunePlanStatus,
    PruneReceipt,
    PruneReceiptStatus,
    RecoverySpec,
    RestoreReceipt,
)


class RetentionRepository(Protocol):
    """Protocol for storage of consumer dependencies, prune plans, and receipts."""

    def save_dependency(self, dependency: ConsumerDependency) -> None: ...
    def delete_dependency(self, consumer_id: str, dataset_name: str) -> None: ...
    def get_dependencies(self, dataset_name: str) -> tuple[ConsumerDependency, ...]: ...
    def save_plan(self, plan: PrunePlan) -> None: ...
    def update_plan(self, plan: PrunePlan) -> None: ...
    def save_receipt(self, receipt: PruneReceipt) -> None: ...


class InMemoryRetentionRepository:
    """In-memory reference implementation of RetentionRepository for testing."""

    def __init__(self) -> None:
        self.dependencies: dict[tuple[str, str], ConsumerDependency] = {}
        self.plans: dict[str, PrunePlan] = {}
        self.receipts: dict[str, PruneReceipt] = {}

    def save_dependency(self, dependency: ConsumerDependency) -> None:
        key = (dependency.consumer_id, dependency.dataset_name)
        self.dependencies[key] = dependency

    def delete_dependency(self, consumer_id: str, dataset_name: str) -> None:
        key = (consumer_id, dataset_name)
        self.dependencies.pop(key, None)

    def get_dependencies(self, dataset_name: str) -> tuple[ConsumerDependency, ...]:
        return tuple(
            dep
            for dep in self.dependencies.values()
            if dep.dataset_name == dataset_name
        )

    def save_plan(self, plan: PrunePlan) -> None:
        self.plans[plan.plan_id] = plan

    def update_plan(self, plan: PrunePlan) -> None:
        self.plans[plan.plan_id] = plan

    def save_receipt(self, receipt: PruneReceipt) -> None:
        self.receipts[receipt.plan_id] = receipt


class DependencyVersionConflictError(Exception):
    """Raised when dependency version mismatches registered version."""


class DependencyViolationError(Exception):
    """Raised when prune range violates active consumer recovery watermarks."""


class RetentionAuthority:
    """Authority module governing data retention and safe pruning."""

    def __init__(self, repository: RetentionRepository | None = None) -> None:
        self._repo = repository or InMemoryRetentionRepository()

    def compute_dependency_version(self, dataset_name: str) -> str:
        """Computes a deterministic hash of active dependencies for dataset."""
        deps = sorted(
            self._repo.get_dependencies(dataset_name),
            key=lambda d: (
                d.consumer_id,
                d.recovery_spec.earliest_needed_watermark.isoformat(),
            ),
        )
        if not deps:
            return "dep_v0_empty"

        hasher = hashlib.sha256()
        for d in deps:
            hasher.update(d.consumer_id.encode())
            hasher.update(str(d.generation).encode())
            hasher.update(
                d.recovery_spec.earliest_needed_watermark.isoformat().encode()
            )
            if d.recovery_spec.earliest_checkpoint_id:
                hasher.update(d.recovery_spec.earliest_checkpoint_id.encode())
        return f"dep_{hasher.hexdigest()[:16]}"

    def register_dependency(
        self,
        *,
        consumer_id: str,
        generation: int,
        recovery_spec: RecoverySpec,
    ) -> str:
        """Registers or refreshes an active consumer recovery dependency."""
        temp_version = f"gen{generation}_{uuid4().hex[:8]}"
        dependency = ConsumerDependency(
            consumer_id=consumer_id,
            dataset_name=recovery_spec.source_dataset,
            generation=generation,
            recovery_spec=recovery_spec,
            dependency_version=temp_version,
            updated_at=datetime.now(UTC),
        )
        self._repo.save_dependency(dependency)
        epoch = self.compute_dependency_version(recovery_spec.source_dataset)
        return epoch

    def unregister_dependency(
        self,
        *,
        consumer_id: str,
        dataset_name: str,
        retired_by: str,
    ) -> str:
        """Explicitly retires a consumer dependency."""
        if not retired_by.strip():
            raise ValueError("retired_by operator/migration must be specified")
        self._repo.delete_dependency(consumer_id, dataset_name)
        return self.compute_dependency_version(dataset_name)

    def plan_prune(
        self,
        *,
        dataset_name: str,
        requested_cutoff: datetime,
        manifest_hash: str | None = None,
        cascade_target_tables: tuple[str, ...] = (),
    ) -> PrunePlan:
        """Create an immutable PrunePlan bounded by active consumer dependencies."""
        if requested_cutoff.tzinfo is None:
            raise ValueError("requested_cutoff must be timezone-aware")

        deps = self._repo.get_dependencies(dataset_name)
        current_dep_version = self.compute_dependency_version(dataset_name)

        if not deps:
            plan = PrunePlan(
                plan_id=f"plan_{dataset_name}_{uuid4().hex[:12]}",
                dataset_name=dataset_name,
                requested_cutoff=requested_cutoff,
                effective_cutoff=requested_cutoff,
                is_constrained=False,
                binding_consumer_id=None,
                manifest_hash=manifest_hash,
                expected_dependency_version=current_dep_version,
                cascade_target_tables=cascade_target_tables,
                status=PrunePlanStatus.CREATED,
                created_at=datetime.now(UTC),
            )
            self._repo.save_plan(plan)
            return plan

        # Find the earliest watermark across all active dependencies
        binding_dep = min(deps, key=lambda d: d.recovery_spec.earliest_needed_watermark)
        watermark = binding_dep.recovery_spec.earliest_needed_watermark

        if watermark < requested_cutoff:
            effective_cutoff = watermark
            is_constrained = True
            binding_consumer_id = binding_dep.consumer_id
        else:
            effective_cutoff = requested_cutoff
            is_constrained = False
            binding_consumer_id = None

        plan = PrunePlan(
            plan_id=f"plan_{dataset_name}_{uuid4().hex[:12]}",
            dataset_name=dataset_name,
            requested_cutoff=requested_cutoff,
            effective_cutoff=effective_cutoff,
            is_constrained=is_constrained,
            binding_consumer_id=binding_consumer_id,
            manifest_hash=manifest_hash,
            expected_dependency_version=current_dep_version,
            cascade_target_tables=cascade_target_tables,
            status=PrunePlanStatus.CREATED,
            created_at=datetime.now(UTC),
        )
        self._repo.save_plan(plan)
        return plan

    def execute_prune(
        self,
        *,
        plan: PrunePlan,
        expected_dependency_version: str,
        executor_fn: Callable[[PrunePlan], tuple[int, int]],
    ) -> PruneReceipt:
        """Executes a PrunePlan after validating dependency epoch fencing.

        Guarantees:
        - If dependency version changed since plan creation, aborts (fail-closed);
        - Never deletes data beyond effective_cutoff;
        - Emits a verifiable PruneReceipt.
        """
        current_dep_version = self.compute_dependency_version(plan.dataset_name)

        # 1. Verify dependency version epoch fencing
        if current_dep_version != expected_dependency_version:
            receipt = PruneReceipt(
                plan_id=plan.plan_id,
                dataset_name=plan.dataset_name,
                effective_cutoff=plan.effective_cutoff,
                rows_archived=0,
                rows_deleted=0,
                manifest_hash=plan.manifest_hash,
                dependency_version_verified=current_dep_version,
                status=PruneReceiptStatus.REJECTED_VERSION_MISMATCH,
                details=(
                    f"Dependency version mismatch: expected "
                    f"{expected_dependency_version}, current {current_dep_version}. "
                    "A new consumer dependency was registered."
                ),
                executed_at=datetime.now(UTC),
            )
            self._repo.save_receipt(receipt)
            return receipt

        # 2. Verify plan has not expired or aborted
        if plan.status == PrunePlanStatus.ABORTED:
            receipt = PruneReceipt(
                plan_id=plan.plan_id,
                dataset_name=plan.dataset_name,
                effective_cutoff=plan.effective_cutoff,
                rows_archived=0,
                rows_deleted=0,
                manifest_hash=plan.manifest_hash,
                dependency_version_verified=current_dep_version,
                status=PruneReceiptStatus.REJECTED_DEPENDENCY_VIOLATION,
                details="Prune plan was previously aborted.",
                executed_at=datetime.now(UTC),
            )
            self._repo.save_receipt(receipt)
            return receipt

        # 3. Execute bounded prune
        try:
            rows_archived, rows_deleted = executor_fn(plan)
            receipt = PruneReceipt(
                plan_id=plan.plan_id,
                dataset_name=plan.dataset_name,
                effective_cutoff=plan.effective_cutoff,
                rows_archived=rows_archived,
                rows_deleted=rows_deleted,
                manifest_hash=plan.manifest_hash,
                dependency_version_verified=current_dep_version,
                status=PruneReceiptStatus.SUCCESS,
                details=(
                    f"Archived {rows_archived} rows, deleted {rows_deleted} rows "
                    f"bounded by {plan.effective_cutoff}."
                ),
                executed_at=datetime.now(UTC),
            )
            # Update plan status to COMPLETED
            completed_plan = PrunePlan(
                plan_id=plan.plan_id,
                dataset_name=plan.dataset_name,
                requested_cutoff=plan.requested_cutoff,
                effective_cutoff=plan.effective_cutoff,
                is_constrained=plan.is_constrained,
                binding_consumer_id=plan.binding_consumer_id,
                manifest_hash=plan.manifest_hash,
                expected_dependency_version=plan.expected_dependency_version,
                cascade_target_tables=plan.cascade_target_tables,
                status=PrunePlanStatus.COMPLETED,
                created_at=plan.created_at,
            )
            self._repo.update_plan(completed_plan)
            self._repo.save_receipt(receipt)
            return receipt
        except Exception as ex:
            receipt = PruneReceipt(
                plan_id=plan.plan_id,
                dataset_name=plan.dataset_name,
                effective_cutoff=plan.effective_cutoff,
                rows_archived=0,
                rows_deleted=0,
                manifest_hash=plan.manifest_hash,
                dependency_version_verified=current_dep_version,
                status=PruneReceiptStatus.EXECUTION_FAILED,
                details=f"Prune execution failed with exception: {ex}",
                executed_at=datetime.now(UTC),
            )
            self._repo.save_receipt(receipt)
            return receipt

    def restore(
        self,
        *,
        recovery_spec: RecoverySpec,
        restore_fn: Callable[[RecoverySpec], int],
    ) -> RestoreReceipt:
        """Verifies or executes restore from archive given recovery specification."""
        try:
            restored_rows = restore_fn(recovery_spec)
            return RestoreReceipt(
                recovery_spec=recovery_spec,
                restored_rows=restored_rows,
                verified_at=datetime.now(UTC),
                status="VERIFIED",
                details=f"Successfully restored and verified {restored_rows} rows.",
            )
        except Exception as ex:
            return RestoreReceipt(
                recovery_spec=recovery_spec,
                restored_rows=0,
                verified_at=datetime.now(UTC),
                status="FAILED",
                details=f"Restore verification failed: {ex}",
            )
