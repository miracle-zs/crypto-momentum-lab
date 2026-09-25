"""PostgreSQL persistence adapter for RetentionAuthority (R1).

Obeys Astra Architecture Blueprint 2026-09-25:
- Durable persistence of consumer recovery dependencies (ConsumerDependencyRow);
- Durable audit log of PrunePlans and PruneReceipt execution outcomes (PrunePlanRow);
- Satisfies RetentionRepository protocol.
"""

from __future__ import annotations

from sqlalchemy import delete, select
from sqlalchemy.orm import Session, sessionmaker

from crypto_momentum_lab.domain.operational.retention_models import (
    ConsumerDependency,
    PrunePlan,
    PrunePlanStatus,
    PruneReceipt,
    PruneReceiptStatus,
    RecoverySpec,
)
from crypto_momentum_lab.persistence.postgres.models import (
    ConsumerDependencyRow,
    PrunePlanRow,
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


class PostgresRetentionRepository:
    """Synchronous PostgreSQL implementation of RetentionRepository."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def save_dependency(self, dependency: ConsumerDependency) -> None:
        spec = dependency.recovery_spec
        with self._session_factory() as session:
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
            session.merge(row)
            session.commit()

    def delete_dependency(self, consumer_id: str, dataset_name: str) -> None:
        with self._session_factory() as session:
            session.execute(
                delete(ConsumerDependencyRow).where(
                    ConsumerDependencyRow.consumer_id == consumer_id,
                    ConsumerDependencyRow.dataset_name == dataset_name,
                )
            )
            session.commit()

    def get_dependencies(self, dataset_name: str) -> tuple[ConsumerDependency, ...]:
        with self._session_factory() as session:
            rows = session.scalars(
                select(ConsumerDependencyRow).where(
                    ConsumerDependencyRow.dataset_name == dataset_name
                )
            ).all()
            return tuple(_row_to_dependency(r) for r in rows)

    def save_plan(self, plan: PrunePlan) -> None:
        with self._session_factory() as session:
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
            session.merge(row)
            session.commit()

    def update_plan(self, plan: PrunePlan) -> None:
        with self._session_factory() as session:
            row = session.get(PrunePlanRow, plan.plan_id)
            if row is not None:
                row.status = plan.status.value
                session.commit()

    def save_receipt(self, receipt: PruneReceipt) -> None:
        with self._session_factory() as session:
            row = session.get(PrunePlanRow, receipt.plan_id)
            if row is not None:
                row.status = (
                    PrunePlanStatus.COMPLETED.value
                    if receipt.status == PruneReceiptStatus.SUCCESS
                    else PrunePlanStatus.ABORTED.value
                )
                row.rows_archived = receipt.rows_archived
                row.rows_deleted = receipt.rows_deleted
                row.executed_at = receipt.executed_at
                session.commit()
            else:
                # If plan row didn't exist, create it
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
                session.merge(new_row)
                session.commit()
