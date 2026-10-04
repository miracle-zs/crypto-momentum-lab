"""PostgreSQL persistence adapter for RetentionAuthority (R1).

Obeys Astra Architecture Blueprint 2026-09-25:
- Durable persistence of consumer recovery dependencies (ConsumerDependencyRow);
- Durable audit log of PrunePlans and PruneReceipt execution outcomes (PrunePlanRow);
- Satisfies RetentionRepository protocol.
"""

from __future__ import annotations

from sqlalchemy import delete, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.operational.retention_models import (
    ConsumerDependency,
    PrunePlan,
    PrunePlanStatus,
    PruneReceipt,
    PruneReceiptStatus,
    RecoverySpec,
    resolve_dataset_scope,
)
from crypto_momentum_lab.persistence.postgres.models import (
    ConsumerDependencyRow,
    PrunePlanRow,
)

_RETENTION_ADVISORY_LOCK_SQL = text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))")


async def _acquire_advisory_lock_async(
    session: AsyncSession, dataset_name: str
) -> None:
    scope = resolve_dataset_scope(dataset_name)
    for lock_key in scope.advisory_lock_keys:
        await session.execute(
            _RETENTION_ADVISORY_LOCK_SQL,
            {"lock_key": lock_key},
        )


def _row_to_dependency(row: ConsumerDependencyRow) -> ConsumerDependency:
    return ConsumerDependency(
        consumer_id=row.consumer_id,
        dataset_name=row.dataset_name,
        generation=row.generation,
        recovery_spec=RecoverySpec(
            source_dataset=row.dataset_name,
            earliest_needed_watermark=row.recovery_watermark,
            earliest_checkpoint_id=row.earliest_checkpoint_id,
            recovery_deadline=row.recovery_deadline,
            cold_recovery_supported=row.cold_recovery_supported,
            reason=row.reason,
        ),
        dependency_version=row.dependency_version,
        updated_at=row.updated_at,
    )




class AsyncPostgresRetentionRepository:
    """Asynchronous PostgreSQL implementation of RetentionRepository."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def save_dependency_in_session(
        self,
        session: AsyncSession,
        dependency: ConsumerDependency,
    ) -> None:
        """Persist a recovery dependency without ending the caller's transaction.

        The minimum recovery watermark is monotonic: a later decision cannot
        silently authorize pruning facts an earlier decision still needs.
        Lock failures propagate so a decision commit cannot claim durability
        without registering its retention contract.
        """
        await _acquire_advisory_lock_async(session, dependency.dataset_name)

        row = await session.get(
            ConsumerDependencyRow,
            (dependency.consumer_id, dependency.dataset_name),
            with_for_update=True,
        )
        spec = dependency.recovery_spec
        if row is not None:
            if row.recovery_watermark <= spec.earliest_needed_watermark:
                watermark = row.recovery_watermark
                checkpoint_id = row.earliest_checkpoint_id
            else:
                watermark = spec.earliest_needed_watermark
                checkpoint_id = spec.earliest_checkpoint_id
            recovery_deadline = row.recovery_deadline
            if spec.recovery_deadline is not None and (
                recovery_deadline is None or spec.recovery_deadline > recovery_deadline
            ):
                recovery_deadline = spec.recovery_deadline
            row.generation = max(row.generation, dependency.generation)
            row.recovery_watermark = watermark
            row.earliest_checkpoint_id = checkpoint_id
            row.recovery_deadline = recovery_deadline
            row.cold_recovery_supported = (
                row.cold_recovery_supported and spec.cold_recovery_supported
            )
            row.dependency_version = dependency.dependency_version
            row.reason = spec.reason
            row.updated_at = dependency.updated_at
            return

        session.add(
            ConsumerDependencyRow(
                consumer_id=dependency.consumer_id,
                dataset_name=dependency.dataset_name,
                generation=dependency.generation,
                recovery_watermark=spec.earliest_needed_watermark,
                earliest_checkpoint_id=spec.earliest_checkpoint_id,
                recovery_deadline=spec.recovery_deadline,
                cold_recovery_supported=spec.cold_recovery_supported,
                dependency_version=dependency.dependency_version,
                reason=spec.reason,
                updated_at=dependency.updated_at,
            )
        )

    async def save_dependency(self, dependency: ConsumerDependency) -> None:
        spec = dependency.recovery_spec
        async with self._session_factory() as session:
            await _acquire_advisory_lock_async(session, dependency.dataset_name)
            row = ConsumerDependencyRow(
                consumer_id=dependency.consumer_id,
                dataset_name=dependency.dataset_name,
                generation=dependency.generation,
                recovery_watermark=spec.earliest_needed_watermark,
                earliest_checkpoint_id=spec.earliest_checkpoint_id,
                recovery_deadline=spec.recovery_deadline,
                cold_recovery_supported=spec.cold_recovery_supported,
                dependency_version=dependency.dependency_version,
                reason=spec.reason,
                updated_at=dependency.updated_at,
            )
            await session.merge(row)
            await session.commit()

    async def delete_dependency(self, consumer_id: str, dataset_name: str) -> None:
        async with self._session_factory() as session:
            await _acquire_advisory_lock_async(session, dataset_name)
            await session.execute(
                delete(ConsumerDependencyRow).where(
                    ConsumerDependencyRow.consumer_id == consumer_id,
                    ConsumerDependencyRow.dataset_name == dataset_name,
                )
            )
            await session.commit()

    async def get_dependencies(
        self, dataset_name: str
    ) -> tuple[ConsumerDependency, ...]:
        scope = resolve_dataset_scope(dataset_name)
        related = scope.related_dataset_names()
        async with self._session_factory() as session:
            rows = (
                await session.scalars(
                    select(ConsumerDependencyRow).where(
                        ConsumerDependencyRow.dataset_name.in_(related)
                    )
                )
            ).all()
            return tuple(_row_to_dependency(r) for r in rows)

    async def save_plan(self, plan: PrunePlan) -> None:
        async with self._session_factory() as session:
            row = PrunePlanRow(
                plan_id=plan.plan_id,
                dataset_name=plan.dataset_name,
                requested_cutoff=plan.requested_cutoff,
                effective_cutoff=plan.effective_cutoff,
                is_constrained=plan.is_constrained,
                binding_consumer_id=plan.binding_consumer_id,
                manifest_hash=plan.manifest_hash,
                expected_dependency_version=plan.expected_dependency_version,
                status=plan.status.value,
                rows_archived=0,
                rows_deleted=0,
                created_at=plan.created_at,
                executed_at=None,
            )
            await session.merge(row)
            await session.commit()

    async def update_plan(self, plan: PrunePlan) -> None:
        async with self._session_factory() as session:
            row = await session.get(PrunePlanRow, plan.plan_id)
            if row is not None:
                row.status = plan.status.value
                row.manifest_hash = plan.manifest_hash
                await session.commit()

    async def save_receipt(self, receipt: PruneReceipt) -> None:
        async with self._session_factory() as session:
            row = await session.get(PrunePlanRow, receipt.plan_id)
            if row is not None:
                row.status = (
                    PrunePlanStatus.COMPLETED.value
                    if receipt.status == PruneReceiptStatus.SUCCESS
                    else PrunePlanStatus.ABORTED.value
                )
                row.rows_archived = receipt.rows_archived
                row.rows_deleted = receipt.rows_deleted
                row.executed_at = receipt.executed_at
                await session.commit()
            else:
                new_row = PrunePlanRow(
                    plan_id=receipt.plan_id,
                    dataset_name=receipt.dataset_name,
                    requested_cutoff=receipt.effective_cutoff,
                    effective_cutoff=receipt.effective_cutoff,
                    is_constrained=False,
                    binding_consumer_id=None,
                    manifest_hash=receipt.manifest_hash,
                    expected_dependency_version=receipt.dependency_version_verified,
                    status=(
                        PrunePlanStatus.COMPLETED.value
                        if receipt.status == PruneReceiptStatus.SUCCESS
                        else PrunePlanStatus.ABORTED.value
                    ),
                    rows_archived=receipt.rows_archived,
                    rows_deleted=receipt.rows_deleted,
                    created_at=receipt.executed_at,
                    executed_at=receipt.executed_at,
                )
                await session.merge(new_row)
                await session.commit()
