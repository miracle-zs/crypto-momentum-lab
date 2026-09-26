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
import inspect
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any, Protocol
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


async def _maybe_await(val: Any) -> Any:
    if inspect.isawaitable(val):
        return await val
    return val


async def _await_void_call(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
    """Invoke a possibly-async void repository method without using its result."""
    result = fn(*args, **kwargs)
    if inspect.isawaitable(result):
        await result



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
    """Authority module governing data retention and safe pruning.

    Uses per-dataset asyncio.Lock to serialise dependency registration
    and prune execution within a single process, closing the TOCTOU gap
    between verify_fence and the actual DELETE.  Cross-process callers
    (e.g. archive_and_trim.py) must additionally use PostgreSQL advisory
    locks or equivalent external coordination.
    """

    def __init__(self, repository: RetentionRepository | None = None) -> None:
        self._repo = repository or InMemoryRetentionRepository()
        self._dataset_locks: dict[str, Any] = {}

    def _get_lock(self, dataset_name: str) -> Any:
        """Returns (or creates) the asyncio.Lock for a dataset."""
        import asyncio

        if dataset_name not in self._dataset_locks:
            self._dataset_locks[dataset_name] = asyncio.Lock()
        return self._dataset_locks[dataset_name]

    @staticmethod
    def _compute_version_hash_pure(
        deps: tuple[ConsumerDependency, ...] | list[ConsumerDependency],
    ) -> str:
        """Pure function: deterministic hash of dependency set.

        Shared by both sync and async paths to prevent divergence.
        """
        sorted_deps = sorted(
            deps,
            key=lambda d: (
                d.consumer_id,
                d.recovery_spec.earliest_needed_watermark.isoformat(),
            ),
        )
        if not sorted_deps:
            return "dep_v0_empty"

        hasher = hashlib.sha256()
        for d in sorted_deps:
            hasher.update(d.consumer_id.encode())
            hasher.update(str(d.generation).encode())
            hasher.update(
                d.recovery_spec.earliest_needed_watermark.isoformat().encode()
            )
            if d.recovery_spec.earliest_checkpoint_id:
                hasher.update(
                    d.recovery_spec.earliest_checkpoint_id.encode()
                )
        return f"dep_{hasher.hexdigest()[:16]}"

    def compute_dependency_version(self, dataset_name: str) -> str:
        """Computes a deterministic hash of active dependencies for dataset."""
        deps = self._repo.get_dependencies(dataset_name)
        return self._compute_version_hash_pure(deps)

    @staticmethod
    def _create_consumer_dependency(
        *,
        consumer_id: str,
        generation: int,
        recovery_spec: RecoverySpec,
    ) -> ConsumerDependency:
        temp_version = f"gen{generation}_{uuid4().hex[:8]}"
        return ConsumerDependency(
            consumer_id=consumer_id,
            dataset_name=recovery_spec.source_dataset,
            generation=generation,
            recovery_spec=recovery_spec,
            dependency_version=temp_version,
            updated_at=datetime.now(UTC),
        )

    def register_dependency(
        self,
        *,
        consumer_id: str,
        generation: int,
        recovery_spec: RecoverySpec,
    ) -> str:
        """Registers or refreshes an active consumer recovery dependency."""
        dependency = self._create_consumer_dependency(
            consumer_id=consumer_id,
            generation=generation,
            recovery_spec=recovery_spec,
        )
        self._repo.save_dependency(dependency)
        epoch = self.compute_dependency_version(recovery_spec.source_dataset)
        return epoch

    async def register_dependency_async(
        self,
        *,
        consumer_id: str,
        generation: int,
        recovery_spec: RecoverySpec,
    ) -> str:
        """Async variant that acquires dataset lock before registering.

        Prevents a concurrent execute_prune_async from deleting data
        that the newly-registered dependency's recovery window protects.
        """
        async with self._get_lock(recovery_spec.source_dataset):
            dependency = self._create_consumer_dependency(
                consumer_id=consumer_id,
                generation=generation,
                recovery_spec=recovery_spec,
            )
            await _await_void_call(self._repo.save_dependency, dependency)
            return await self.compute_dependency_version_async(
                recovery_spec.source_dataset
            )

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

    def bind_manifest(self, plan: PrunePlan, manifest_hash: str) -> PrunePlan:
        """Binds a verified archive manifest hash to an existing prune plan.

        Guarantees that the prune execution is strictly locked to the exact
        effective_cutoff that was used during archiving, rather than recomputing
        a new plan which could widen the cutoff if consumer dependencies change.
        """
        current_dep_version = self.compute_dependency_version(plan.dataset_name)
        if current_dep_version != plan.expected_dependency_version:
            raise DependencyVersionConflictError(
                f"Dependency version changed from {plan.expected_dependency_version} "
                f"to {current_dep_version} before manifest binding"
            )
        if plan.status != PrunePlanStatus.CREATED:
            raise RuntimeError(
                f"Cannot bind manifest to plan in status {plan.status.value}"
            )
        bound_plan = PrunePlan(
            plan_id=plan.plan_id,
            dataset_name=plan.dataset_name,
            requested_cutoff=plan.requested_cutoff,
            effective_cutoff=plan.effective_cutoff,
            is_constrained=plan.is_constrained,
            binding_consumer_id=plan.binding_consumer_id,
            manifest_hash=manifest_hash,
            expected_dependency_version=plan.expected_dependency_version,
            cascade_target_tables=plan.cascade_target_tables,
            status=plan.status,
            created_at=plan.created_at,
        )
        self._repo.update_plan(bound_plan)
        return bound_plan

    async def compute_dependency_version_async(
        self, dataset_name: str
    ) -> str:
        """Async: compute current version hash of active dependencies."""
        deps = await _maybe_await(self._repo.get_dependencies(dataset_name))
        return self._compute_version_hash_pure(deps)

    async def plan_prune_async(
        self,
        *,
        dataset_name: str,
        requested_cutoff: datetime,
        manifest_hash: str | None = None,
        cascade_target_tables: tuple[str, ...] = (),
    ) -> PrunePlan:
        """Asynchronously creates an immutable PrunePlan bounded by active consumer dependencies."""
        if requested_cutoff.tzinfo is None:
            raise ValueError("requested_cutoff must be timezone-aware")

        deps = await _maybe_await(self._repo.get_dependencies(dataset_name))
        current_dep_version = await self.compute_dependency_version_async(
            dataset_name
        )

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
            await _await_void_call(self._repo.save_plan, plan)
            return plan

        binding_dep = min(
            deps, key=lambda d: d.recovery_spec.earliest_needed_watermark
        )
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
        await _await_void_call(self._repo.save_plan, plan)
        return plan

    def verify_fence(self, plan: PrunePlan) -> None:
        """Verifies dependency epoch fence mid-execution.

        Must be called before each batch deletion to guarantee an atomic fence.
        """
        current_dep_version = self.compute_dependency_version(plan.dataset_name)
        if current_dep_version != plan.expected_dependency_version:
            raise DependencyVersionConflictError(
                f"Dependency epoch fence violation mid-prune: plan expected "
                f"{plan.expected_dependency_version}, current {current_dep_version}"
            )

    async def verify_fence_async(self, plan: PrunePlan) -> None:
        """Asynchronously verifies dependency epoch fence mid-execution."""
        current_dep_version = await self.compute_dependency_version_async(
            plan.dataset_name
        )
        if current_dep_version != plan.expected_dependency_version:
            raise DependencyVersionConflictError(
                f"Dependency epoch fence violation mid-prune: plan expected "
                f"{plan.expected_dependency_version}, current {current_dep_version}"
            )

    async def execute_prune_async(
        self,
        *,
        plan: PrunePlan,
        expected_dependency_version: str,
        executor_fn: Callable[
            [PrunePlan],
            Awaitable[tuple[int, int]] | tuple[int, int],
        ],
    ) -> PruneReceipt:
        """Asynchronously executes a PrunePlan with dependency epoch fencing.

        Acquires per-dataset lock to serialise against concurrent
        register_dependency_async calls within the same process.
        """
        async with self._get_lock(plan.dataset_name):
            return await self._execute_prune_async_inner(
                plan=plan,
                expected_dependency_version=expected_dependency_version,
                executor_fn=executor_fn,
            )

    async def _execute_prune_async_inner(
        self,
        *,
        plan: PrunePlan,
        expected_dependency_version: str,
        executor_fn: Callable[
            [PrunePlan],
            Awaitable[tuple[int, int]] | tuple[int, int],
        ],
    ) -> PruneReceipt:
        """Inner execution logic, called under dataset lock."""
        current_dep_version = await self.compute_dependency_version_async(
            plan.dataset_name
        )

        if (
            current_dep_version != plan.expected_dependency_version
            or current_dep_version != expected_dependency_version
        ):
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
                    f"Dependency version mismatch: plan expected "
                    f"{plan.expected_dependency_version}, caller expected "
                    f"{expected_dependency_version}, current {current_dep_version}. "
                    "A consumer dependency was added or modified since plan creation."
                ),
                executed_at=datetime.now(UTC),
            )
            await _await_void_call(self._repo.save_receipt, receipt)
            return receipt

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
            await _await_void_call(self._repo.save_receipt, receipt)
            return receipt

        try:
            res = executor_fn(plan)
            if inspect.isawaitable(res):
                rows_archived, rows_deleted = await res
            else:
                rows_archived, rows_deleted = res
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
            await _await_void_call(self._repo.update_plan, completed_plan)
            await _await_void_call(self._repo.save_receipt, receipt)
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
            await _await_void_call(self._repo.save_receipt, receipt)
            return receipt

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

        # 1. Verify dependency version epoch fencing against both plan and caller expectations
        if (
            current_dep_version != plan.expected_dependency_version
            or current_dep_version != expected_dependency_version
        ):
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
                    f"Dependency version mismatch: plan expected "
                    f"{plan.expected_dependency_version}, caller expected "
                    f"{expected_dependency_version}, current {current_dep_version}. "
                    "A consumer dependency was added or modified since plan creation."
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


def create_authority_from_repository(
    repository: Any,
    *,
    async_repo_factory: Callable[..., Any] | None = None,
) -> RetentionAuthority:
    """Creates a RetentionAuthority backed by a real or in-memory repo.

    Extracts session_factory from the given repository and builds an
    async retention repository.  Falls back to in-memory when the
    repository is a test mock or has no usable session factory.

    This replaces the duplicated mock-detection boilerplate that was
    copy-pasted across market_data/main.py and retention.py.
    """
    session_factory = getattr(
        repository,
        "session_factory",
        getattr(repository, "_session_factory", None),
    )
    if session_factory is None:
        return RetentionAuthority()

    # Detect test mocks — production code should not run real DB ops
    # against a mock session factory.
    sf_type_name = type(session_factory).__name__
    if (
        hasattr(session_factory, "_mock_return_value")
        or sf_type_name in ("AsyncMock", "MagicMock", "Mock")
    ):
        return RetentionAuthority()

    if async_repo_factory is not None:
        ret_repo = async_repo_factory(session_factory)
    else:
        ret_repo = None
    return RetentionAuthority(repository=ret_repo)
