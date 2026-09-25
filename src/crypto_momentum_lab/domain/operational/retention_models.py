"""Domain models for RetentionAuthority, RecoveryCatalog, and PrunePlan protocols.

Obeys Astra Architecture Blueprint 2026-09-25:
- Invariant 5: Recovery dependencies must never be guessed by pruners;
- Prune plans are immutable and record expected dependency versions;
- Deletions verify dependency epoch before executing (fail-closed);
- Dependencies must be explicitly retired, never silently expired via heartbeats.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum


class PrunePlanStatus(StrEnum):
    """Lifecycle states of a PrunePlan."""

    CREATED = "CREATED"
    EXECUTING = "EXECUTING"
    COMPLETED = "COMPLETED"
    ABORTED = "ABORTED"


class PruneReceiptStatus(StrEnum):
    """Execution outcome status in PruneReceipt."""

    SUCCESS = "SUCCESS"
    REJECTED_VERSION_MISMATCH = "REJECTED_VERSION_MISMATCH"
    REJECTED_DEPENDENCY_VIOLATION = "REJECTED_DEPENDENCY_VIOLATION"
    EXECUTION_FAILED = "EXECUTION_FAILED"


@dataclass(frozen=True, slots=True)
class RecoverySpec:
    """Specification of recovery requirements declared by an active consumer."""

    source_dataset: str
    earliest_needed_watermark: datetime
    earliest_checkpoint_id: str | None = None
    recovery_deadline: datetime | None = None
    cold_recovery_supported: bool = False
    reason: str = ""

    def __post_init__(self) -> None:
        if not self.source_dataset.strip():
            raise ValueError("source_dataset must not be empty")
        if self.earliest_needed_watermark.tzinfo is None:
            raise ValueError("earliest_needed_watermark must be timezone-aware")
        if self.recovery_deadline is not None and self.recovery_deadline.tzinfo is None:
            raise ValueError("recovery_deadline must be timezone-aware")


@dataclass(frozen=True, slots=True)
class ConsumerDependency:
    """A registered consumer recovery dependency binding a dataset to an epoch."""

    consumer_id: str
    dataset_name: str
    generation: int
    recovery_spec: RecoverySpec
    dependency_version: str
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        if not self.consumer_id.strip():
            raise ValueError("consumer_id must not be empty")
        if not self.dataset_name.strip():
            raise ValueError("dataset_name must not be empty")
        if not self.dependency_version.strip():
            raise ValueError("dependency_version must not be empty")
        if self.updated_at.tzinfo is None:
            raise ValueError("updated_at must be timezone-aware")


@dataclass(frozen=True, slots=True)
class PrunePlan:
    """Immutable plan for pruning data bounded by consumer dependencies."""

    plan_id: str
    dataset_name: str
    requested_cutoff: datetime
    effective_cutoff: datetime
    is_constrained: bool
    binding_consumer_id: str | None
    manifest_hash: str | None
    expected_dependency_version: str
    cascade_target_tables: tuple[str, ...] = ()
    status: PrunePlanStatus = PrunePlanStatus.CREATED
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        if not self.plan_id.strip():
            raise ValueError("plan_id must not be empty")
        if not self.dataset_name.strip():
            raise ValueError("dataset_name must not be empty")
        if self.requested_cutoff.tzinfo is None:
            raise ValueError("requested_cutoff must be timezone-aware")
        if self.effective_cutoff.tzinfo is None:
            raise ValueError("effective_cutoff must be timezone-aware")
        if self.effective_cutoff > self.requested_cutoff:
            raise ValueError(
                f"effective_cutoff {self.effective_cutoff} must never be newer than "
                f"requested_cutoff {self.requested_cutoff}"
            )
        if not self.expected_dependency_version.strip():
            raise ValueError("expected_dependency_version must not be empty")


@dataclass(frozen=True, slots=True)
class PruneReceipt:
    """Verifiable receipt emitted after prune plan execution or rejection."""

    plan_id: str
    dataset_name: str
    effective_cutoff: datetime
    rows_archived: int
    rows_deleted: int
    manifest_hash: str | None
    dependency_version_verified: str
    status: PruneReceiptStatus
    details: str = ""
    executed_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True, slots=True)
class RestoreReceipt:
    """Verification receipt for recovery/restore dry-runs or executions."""

    recovery_spec: RecoverySpec
    restored_rows: int
    verified_at: datetime
    status: str
    details: str = ""
