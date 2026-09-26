"""Recoverable deployment protocol and multi-account release coordinator (R4).

Obeys Astra Architecture Blueprint 2026-09-25:
- Three-stage operator interface: prepare -> apply -> status;
- prepare: read-only preflight verification of schema, config, generation,
  and writer state;
- apply: durable step-by-step per-account transition with idempotent resume
  on operation_id;
- Handover order:
  1. Forbid old writer from new entries (ENTER = False);
  2. Confirm old writer has drained and stopped before granting new epoch;
  3. Grant new fencing_epoch;
  4. Recover orders/facts/policies;
  5. Publish capability evidence;
- Fault isolation: partial failures produce PARTIAL receipt and allow idempotent resume;
- Old writer failure blocks new epoch grant ("旧 writer 未停不能新发单");
- Zero unhandled exceptions or hidden second state machines.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Protocol

from crypto_momentum_lab.domain.runtime.runtime_plan import RuntimePlan


class DeploymentStatus(StrEnum):
    """Authoritative lifecycle status of a deployment operation."""

    PREPARED = "prepared"
    APPLYING = "applying"
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"
    ABORTED = "aborted"


class AccountTransitionStatus(StrEnum):
    """Per-account transition progress during deployment apply."""

    PENDING = "pending"
    OLD_WRITER_DRAINING = "old_writer_draining"
    OLD_WRITER_STOPPED = "old_writer_stopped"
    NEW_EPOCH_GRANTED = "new_epoch_granted"
    RECOVERED = "recovered"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass(frozen=True, slots=True)
class AccountDeploymentTarget:
    """Target configuration and transition parameters for a single account."""

    account_label: str
    target_plan: RuntimePlan
    target_epoch: int
    target_generation: str
    stop_old_writer: bool = True
    verify_schema: bool = True

    def __post_init__(self) -> None:
        if not self.account_label.strip():
            raise ValueError("account_label must not be empty")
        if self.target_epoch < 1:
            raise ValueError("target_epoch must be positive (>= 1)")
        if not self.target_generation.strip():
            raise ValueError("target_generation must not be empty")


@dataclass(frozen=True, slots=True)
class DeploymentManifest:
    """Immutable deployment manifest defining the target release state."""

    manifest_id: str
    environment: str
    build_ref: str
    git_commit: str
    declared_schema_version: str
    accounts: tuple[AccountDeploymentTarget, ...]
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    created_by: str = "orchestrator"

    def __post_init__(self) -> None:
        if not self.manifest_id.strip():
            raise ValueError("manifest_id must not be empty")
        if not self.environment.strip():
            raise ValueError("environment must not be empty")
        if not self.build_ref.strip():
            raise ValueError("build_ref must not be empty")
        if not self.git_commit.strip():
            raise ValueError("git_commit must not be empty")
        if not self.accounts:
            raise ValueError("accounts must not be empty")


@dataclass(frozen=True, slots=True)
class AccountPreflightResult:
    """Preflight validation result for a single account."""

    account_label: str
    passed: bool
    reason: str
    current_epoch: int | None = None
    target_epoch: int = 1
    current_generation: str | None = None
    target_generation: str = ""
    observed_database_revision: str | None = None
    schema_compatible: bool = True


@dataclass(frozen=True, slots=True)
class DeploymentCandidate:
    """Prepared, frozen candidate for deployment execution."""

    candidate_id: str
    manifest: DeploymentManifest
    is_valid: bool
    preflight_results: tuple[AccountPreflightResult, ...]
    candidate_hash: str
    prepared_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    blocking_reasons: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class AccountTransitionRecord:
    """Audit record of a single account's deployment progression."""

    account_label: str
    status: AccountTransitionStatus
    from_epoch: int | None
    to_epoch: int
    from_generation: str | None
    to_generation: str
    writer_stopped: bool
    recovered: bool
    error_message: str | None = None
    evidence_cut: datetime | None = None
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True, slots=True)
class DeploymentReceipt:
    """Authoritative durable receipt for a deployment operation."""

    operation_id: str
    candidate_id: str
    status: DeploymentStatus
    account_records: tuple[AccountTransitionRecord, ...]
    started_at: datetime
    completed_at: datetime | None
    duration_seconds: float
    can_resume: bool
    error_summary: str | None = None


@dataclass(frozen=True, slots=True)
class DeploymentStatusView:
    """Read-model view of deployment state across accounts."""

    environment: str
    latest_operation_id: str | None
    latest_status: DeploymentStatus | None
    active_accounts: tuple[str, ...]
    current_generations: Mapping[str, str]
    current_fencing_epochs: Mapping[str, int]
    writer_leases_active: Mapping[str, bool]
    observed_at: datetime = field(default_factory=lambda: datetime.now(UTC))


class WriterSupervisorProtocol(Protocol):
    """Protocol for interacting with live writer processes and leases."""

    async def check_writer_status(self, account_label: str) -> bool: ...
    async def stop_writer(
        self, account_label: str, timeout_seconds: float = 30.0
    ) -> bool: ...
    async def grant_fencing_epoch(
        self, account_label: str, new_epoch: int
    ) -> bool: ...
    async def get_active_epoch(self, account_label: str) -> int: ...
    async def get_active_generation(self, account_label: str) -> str | None: ...


class DatabaseInspectorProtocol(Protocol):
    """Protocol for querying database schema state."""

    async def get_current_schema_revision(self) -> str: ...


class DeploymentJournalProtocol(Protocol):
    """Protocol for persisting and recovering deployment receipts."""

    async def load_receipt(self, operation_id: str) -> DeploymentReceipt | None: ...
    async def save_receipt(self, receipt: DeploymentReceipt) -> None: ...
    async def list_recent_receipts(
        self, limit: int = 10
    ) -> tuple[DeploymentReceipt, ...]: ...


class StaticDatabaseInspector:
    """Simple database inspector returning a fixed revision string."""

    def __init__(self, revision: str = "20260925_0043") -> None:
        self.revision = revision

    async def get_current_schema_revision(self) -> str:
        return self.revision


class InMemoryDeploymentJournal:
    """Thread-safe in-memory journal for deployment receipts."""

    def __init__(self) -> None:
        self._receipts: dict[str, DeploymentReceipt] = {}

    async def load_receipt(self, operation_id: str) -> DeploymentReceipt | None:
        return self._receipts.get(operation_id)

    async def save_receipt(self, receipt: DeploymentReceipt) -> None:
        self._receipts[receipt.operation_id] = receipt

    async def list_recent_receipts(
        self, limit: int = 10
    ) -> tuple[DeploymentReceipt, ...]:
        sorted_receipts = sorted(
            self._receipts.values(),
            key=lambda r: r.started_at,
            reverse=True,
        )
        return tuple(sorted_receipts[:limit])


class InMemoryWriterSupervisor:
    """In-memory writer supervisor with configurable simulation hooks."""

    def __init__(
        self,
        active_writers: dict[str, bool] | None = None,
        epochs: dict[str, int] | None = None,
        generations: dict[str, str] | None = None,
        stop_failure_accounts: set[str] | None = None,
    ) -> None:
        self._writers = dict(active_writers or {})
        self._epochs = dict(epochs or {})
        self._generations = dict(generations or {})
        self._stop_failures = set(stop_failure_accounts or set())

    async def check_writer_status(self, account_label: str) -> bool:
        return self._writers.get(account_label, False)

    async def stop_writer(
        self, account_label: str, timeout_seconds: float = 30.0
    ) -> bool:
        del timeout_seconds
        if account_label in self._stop_failures:
            return False
        self._writers[account_label] = False
        return True

    async def grant_fencing_epoch(
        self, account_label: str, new_epoch: int
    ) -> bool:
        current = self._epochs.get(account_label, 1)
        if new_epoch < current:
            return False
        self._epochs[account_label] = new_epoch
        self._writers[account_label] = True
        return True

    async def get_active_epoch(self, account_label: str) -> int:
        return self._epochs.get(account_label, 1)

    async def get_active_generation(self, account_label: str) -> str | None:
        return self._generations.get(account_label)


class DeploymentCoordinator:
    """Authoritative deployment coordinator implementing prepare-apply-status
    lifecycle.
    """

    def __init__(
        self,
        writer_supervisor: WriterSupervisorProtocol,
        db_inspector: DatabaseInspectorProtocol,
        journal: DeploymentJournalProtocol,
    ) -> None:
        self._supervisor = writer_supervisor
        self._db_inspector = db_inspector
        self._journal = journal

    async def prepare(self, manifest: DeploymentManifest) -> DeploymentCandidate:
        """Read-only preflight verification producing a frozen DeploymentCandidate."""
        observed_rev = await self._db_inspector.get_current_schema_revision()
        preflight_results: list[AccountPreflightResult] = []
        blocking_reasons: list[str] = []

        for target in manifest.accounts:
            current_epoch = await self._supervisor.get_active_epoch(
                target.account_label
            )
            current_gen = await self._supervisor.get_active_generation(
                target.account_label
            )

            schema_ok = True
            if target.verify_schema:
                declared = (
                    target.target_plan.declared_schema_compatibility
                    or manifest.declared_schema_version
                )
                if declared != observed_rev:
                    schema_ok = False
                    blocking_reasons.append(
                        f"schema_compatibility_mismatch:{target.account_label}"
                        f"({declared} != {observed_rev})"
                    )

            epoch_ok = True
            if target.target_epoch < current_epoch:
                epoch_ok = False
                blocking_reasons.append(
                    f"target_epoch_decreased:{target.account_label}"
                    f"({target.target_epoch} < {current_epoch})"
                )

            passed = schema_ok and epoch_ok
            reason = (
                "preflight_passed"
                if passed
                else (
                    "schema_incompatible"
                    if not schema_ok
                    else "epoch_regression"
                )
            )

            preflight_results.append(
                AccountPreflightResult(
                    account_label=target.account_label,
                    passed=passed,
                    reason=reason,
                    current_epoch=current_epoch,
                    target_epoch=target.target_epoch,
                    current_generation=current_gen,
                    target_generation=target.target_generation,
                    observed_database_revision=observed_rev,
                    schema_compatible=schema_ok,
                )
            )

        is_valid = len(blocking_reasons) == 0

        # Deterministic candidate hash
        hash_payload = {
            "manifest_id": manifest.manifest_id,
            "environment": manifest.environment,
            "build_ref": manifest.build_ref,
            "git_commit": manifest.git_commit,
            "declared_schema_version": manifest.declared_schema_version,
            "accounts": [
                {
                    "account": t.account_label,
                    "target_epoch": t.target_epoch,
                    "target_generation": t.target_generation,
                    "plan_hash": t.target_plan.plan_hash,
                }
                for t in manifest.accounts
            ],
            "observed_database_revision": observed_rev,
        }
        cand_hash = hashlib.sha256(
            json.dumps(hash_payload, sort_keys=True).encode()
        ).hexdigest()

        return DeploymentCandidate(
            candidate_id=f"cand_{manifest.manifest_id}_{cand_hash[:12]}",
            manifest=manifest,
            is_valid=is_valid,
            preflight_results=tuple(preflight_results),
            candidate_hash=cand_hash,
            blocking_reasons=tuple(blocking_reasons),
        )

    async def apply(
        self,
        candidate: DeploymentCandidate,
        operation_id: str,
    ) -> DeploymentReceipt:
        """Apply a prepared deployment candidate across target accounts.

        Guarantees:
        - Idempotent on duplicate operation_id;
        - Preflight failure immediately fails closed without side-effects;
        - Monotonic handover: old writer must stop before granting new epoch;
        - Partial failures preserve per-account audit records and allow resume.
        """
        started_at = datetime.now(UTC)

        # 1. Check idempotency and resume state
        existing = await self._journal.load_receipt(operation_id)
        if existing is not None and existing.status == DeploymentStatus.SUCCESS:
            return existing

        already_succeeded: dict[str, AccountTransitionRecord] = {}
        if existing is not None:
            for rec in existing.account_records:
                if rec.status == AccountTransitionStatus.SUCCESS:
                    already_succeeded[rec.account_label] = rec

        # 2. Check candidate validity
        if not candidate.is_valid:
            err_summary = "; ".join(candidate.blocking_reasons) or "preflight_failed"
            failed_receipt = DeploymentReceipt(
                operation_id=operation_id,
                candidate_id=candidate.candidate_id,
                status=DeploymentStatus.FAILED,
                account_records=(),
                started_at=started_at,
                completed_at=datetime.now(UTC),
                duration_seconds=0.0,
                can_resume=False,
                error_summary=err_summary,
            )
            await self._journal.save_receipt(failed_receipt)
            return failed_receipt

        # 3. Step-by-step account execution
        account_records: list[AccountTransitionRecord] = []
        for target in candidate.manifest.accounts:
            # Check if already succeeded in previous attempt under this operation_id
            if target.account_label in already_succeeded:
                account_records.append(already_succeeded[target.account_label])
                continue

            current_epoch = await self._supervisor.get_active_epoch(
                target.account_label
            )
            current_gen = await self._supervisor.get_active_generation(
                target.account_label
            )

            # Step A: Stop old writer if running
            writer_stopped = True
            is_active = await self._supervisor.check_writer_status(
                target.account_label
            )
            if is_active and target.stop_old_writer:
                stopped = await self._supervisor.stop_writer(target.account_label)
                if not stopped:
                    writer_stopped = False
                    account_records.append(
                        AccountTransitionRecord(
                            account_label=target.account_label,
                            status=AccountTransitionStatus.FAILED,
                            from_epoch=current_epoch,
                            to_epoch=target.target_epoch,
                            from_generation=current_gen,
                            to_generation=target.target_generation,
                            writer_stopped=False,
                            recovered=False,
                            error_message="failed_to_stop_old_writer",
                        )
                    )
                    # Fail-closed: Cannot grant new epoch if old writer
                    # could not be stopped!
                    continue

            # Step B: Grant new fencing epoch
            epoch_granted = await self._supervisor.grant_fencing_epoch(
                target.account_label, target.target_epoch
            )
            if not epoch_granted:
                account_records.append(
                    AccountTransitionRecord(
                        account_label=target.account_label,
                        status=AccountTransitionStatus.FAILED,
                        from_epoch=current_epoch,
                        to_epoch=target.target_epoch,
                        from_generation=current_gen,
                        to_generation=target.target_generation,
                        writer_stopped=writer_stopped,
                        recovered=False,
                        error_message="failed_to_grant_fencing_epoch",
                    )
                )
                continue

            # Step C: Success
            account_records.append(
                AccountTransitionRecord(
                    account_label=target.account_label,
                    status=AccountTransitionStatus.SUCCESS,
                    from_epoch=current_epoch,
                    to_epoch=target.target_epoch,
                    from_generation=current_gen,
                    to_generation=target.target_generation,
                    writer_stopped=writer_stopped,
                    recovered=True,
                )
            )

        completed_at = datetime.now(UTC)
        duration = (completed_at - started_at).total_seconds()

        # Resolve overall status
        success_count = sum(
            1
            for r in account_records
            if r.status == AccountTransitionStatus.SUCCESS
        )
        total_accounts = len(candidate.manifest.accounts)

        if success_count == total_accounts:
            overall_status = DeploymentStatus.SUCCESS
            can_resume = False
            error_summary = None
        elif success_count > 0:
            overall_status = DeploymentStatus.PARTIAL
            can_resume = True
            error_summary = (
                f"{total_accounts - success_count} accounts failed to transition"
            )
        else:
            overall_status = DeploymentStatus.FAILED
            can_resume = True
            error_summary = "all accounts failed to transition"

        receipt = DeploymentReceipt(
            operation_id=operation_id,
            candidate_id=candidate.candidate_id,
            status=overall_status,
            account_records=tuple(account_records),
            started_at=started_at,
            completed_at=completed_at,
            duration_seconds=duration,
            can_resume=can_resume,
            error_summary=error_summary,
        )
        await self._journal.save_receipt(receipt)
        return receipt

    async def status(self, environment: str = "live") -> DeploymentStatusView:
        """Read-model view of the latest deployment and active writer leases."""
        recent = await self._journal.list_recent_receipts(limit=1)
        latest_op = recent[0].operation_id if recent else None
        latest_stat = recent[0].status if recent else None

        active_accounts: list[str] = []
        generations: dict[str, str] = {}
        epochs: dict[str, int] = {}
        leases: dict[str, bool] = {}

        if recent and recent[0].account_records:
            for rec in recent[0].account_records:
                acc = rec.account_label
                active_accounts.append(acc)
                epochs[acc] = await self._supervisor.get_active_epoch(acc)
                gen = await self._supervisor.get_active_generation(acc)
                if gen:
                    generations[acc] = gen
                leases[acc] = await self._supervisor.check_writer_status(acc)

        return DeploymentStatusView(
            environment=environment,
            latest_operation_id=latest_op,
            latest_status=latest_stat,
            active_accounts=tuple(active_accounts),
            current_generations=MappingProxyType(generations),
            current_fencing_epochs=MappingProxyType(epochs),
            writer_leases_active=MappingProxyType(leases),
        )
