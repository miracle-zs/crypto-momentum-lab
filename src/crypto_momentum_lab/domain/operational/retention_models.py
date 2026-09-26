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


class DatasetId(StrEnum):
    """Canonical dataset identifiers across storage and pruning."""

    MARKET_STATES = "market_states"
    STRATEGY_EVENTS = "strategy_events"
    ACCOUNT_SNAPSHOTS = "account_snapshots"
    ACCOUNT_BALANCES = "account_balances"
    ACCOUNT_POSITIONS = "account_positions"
    ACCOUNT_CONFIGS = "account_configs"
    ACCOUNT_RECONCILIATION = "account_reconciliation"
    EXCHANGE_ORDERS = "exchange_orders"
    PAPER_EQUITY = "paper_equity"
    STRATEGY_SIGNALS = "strategy_signals"
    EXECUTION_PROCESS_STATES = "execution_process_states"
    UNIVERSE_ENTRIES = "universe_entries"
    CONTRACT_METADATA = "contract_metadata"
    GENERIC = "generic"


DATASET_PHYSICAL_TABLES: dict[DatasetId | str, tuple[str, ...]] = {
    DatasetId.MARKET_STATES: ("runtime_market_states_15s",),
    DatasetId.STRATEGY_EVENTS: ("strategy_runtime_events",),
    DatasetId.ACCOUNT_BALANCES: ("account_balance_snapshots",),
    DatasetId.ACCOUNT_POSITIONS: ("account_position_snapshots",),
    DatasetId.ACCOUNT_CONFIGS: ("account_config_snapshots",),
    DatasetId.ACCOUNT_RECONCILIATION: ("account_reconciliation_runs",),
    DatasetId.ACCOUNT_SNAPSHOTS: (
        "account_balance_snapshots",
        "account_config_snapshots",
        "account_position_snapshots",
        "account_reconciliation_runs",
    ),
    DatasetId.EXCHANGE_ORDERS: ("exchange_order_events",),
    DatasetId.PAPER_EQUITY: ("paper_equity_snapshots",),
    DatasetId.STRATEGY_SIGNALS: ("live_strategy_signals",),
    DatasetId.EXECUTION_PROCESS_STATES: ("execution_account_process_states",),
    DatasetId.UNIVERSE_ENTRIES: (
        "monitoring_memberships",
        "universe_entries",
        "universe_snapshots",
    ),
    DatasetId.CONTRACT_METADATA: ("contract_metadata",),
}

TABLE_TO_DATASET: dict[str, DatasetId] = {
    "runtime_market_states_15s": DatasetId.MARKET_STATES,
    "strategy_runtime_events": DatasetId.STRATEGY_EVENTS,
    "account_balance_snapshots": DatasetId.ACCOUNT_BALANCES,
    "account_position_snapshots": DatasetId.ACCOUNT_POSITIONS,
    "account_config_snapshots": DatasetId.ACCOUNT_CONFIGS,
    "account_reconciliation_runs": DatasetId.ACCOUNT_RECONCILIATION,
    "exchange_order_events": DatasetId.EXCHANGE_ORDERS,
    "paper_equity_snapshots": DatasetId.PAPER_EQUITY,
    "live_strategy_signals": DatasetId.STRATEGY_SIGNALS,
    "execution_account_process_states": DatasetId.EXECUTION_PROCESS_STATES,
    "universe_entries": DatasetId.UNIVERSE_ENTRIES,
    "universe_snapshots": DatasetId.UNIVERSE_ENTRIES,
    "monitoring_memberships": DatasetId.UNIVERSE_ENTRIES,
    "contract_metadata": DatasetId.CONTRACT_METADATA,
}

DATASET_PARENT: dict[DatasetId | str, DatasetId] = {
    DatasetId.ACCOUNT_BALANCES: DatasetId.ACCOUNT_SNAPSHOTS,
    DatasetId.ACCOUNT_POSITIONS: DatasetId.ACCOUNT_SNAPSHOTS,
    DatasetId.ACCOUNT_CONFIGS: DatasetId.ACCOUNT_SNAPSHOTS,
    DatasetId.ACCOUNT_RECONCILIATION: DatasetId.ACCOUNT_SNAPSHOTS,
}

DATASET_CHILDREN: dict[DatasetId | str, tuple[DatasetId, ...]] = {
    DatasetId.ACCOUNT_SNAPSHOTS: (
        DatasetId.ACCOUNT_BALANCES,
        DatasetId.ACCOUNT_POSITIONS,
        DatasetId.ACCOUNT_CONFIGS,
        DatasetId.ACCOUNT_RECONCILIATION,
    ),
}


@dataclass(frozen=True, slots=True)
class DatasetScope:
    """Unified dataset scope binding logical identity, environment, and account."""

    dataset_id: DatasetId | str
    environment: str = "live"
    account_label: str | None = None
    custom_name: str | None = None

    @property
    def id_value(self) -> str:
        if isinstance(self.dataset_id, DatasetId):
            return self.dataset_id.value
        return str(self.dataset_id)

    @property
    def canonical_id(self) -> str:
        if self.custom_name and self.dataset_id == DatasetId.GENERIC:
            return self.custom_name
        base = self.id_value
        if self.account_label:
            return f"{base}_{self.account_label}"
        return base

    @property
    def advisory_lock_key(self) -> str:
        return f"retention_{self.canonical_id}"

    @property
    def advisory_lock_keys(self) -> tuple[str, ...]:
        """Sorted tuple of advisory lock keys for deterministic, deadlock-free locks."""
        keys = {self.advisory_lock_key}
        tables = DATASET_PHYSICAL_TABLES.get(self.dataset_id, ())
        for t in tables:
            keys.add(f"retention_{t}")
        if self.custom_name:
            keys.add(f"retention_{self.custom_name}")
        return tuple(sorted(keys))

    def related_dataset_names(self) -> tuple[str, ...]:
        """Set of dataset/table names that share recovery dependencies with scope."""
        names = {self.canonical_id, self.id_value}
        if self.custom_name:
            names.add(self.custom_name)
        if self.account_label:
            names.add(f"{self.id_value}_{self.account_label}")
            names.add(f"{self.id_value}_{self.environment}_{self.account_label}")

        tables = DATASET_PHYSICAL_TABLES.get(self.dataset_id, ())
        for t in tables:
            names.add(t)
            if self.account_label:
                names.add(f"{t}_{self.account_label}")

        if self.dataset_id in DATASET_PARENT:
            parent = DATASET_PARENT[self.dataset_id]
            names.add(parent.value)
            if self.account_label:
                names.add(f"{parent.value}_{self.account_label}")
                names.add(f"{parent.value}_{self.environment}_{self.account_label}")

        if self.dataset_id in DATASET_CHILDREN:
            for child in DATASET_CHILDREN[self.dataset_id]:
                names.add(child.value)
                if self.account_label:
                    names.add(f"{child.value}_{self.account_label}")
                    names.add(f"{child.value}_{self.environment}_{self.account_label}")

        return tuple(sorted(names))


def resolve_dataset_scope(
    name_or_scope: str | DatasetScope,
    *,
    environment: str = "live",
    account_label: str | None = None,
) -> DatasetScope:
    """Resolve a table name, legacy alias, or scope into a canonical DatasetScope."""
    if isinstance(name_or_scope, DatasetScope):
        resolved_env = (
            environment if environment != "live" else name_or_scope.environment
        )
        resolved_acc = (
            account_label if account_label is not None else name_or_scope.account_label
        )
        if (
            resolved_env != name_or_scope.environment
            or resolved_acc != name_or_scope.account_label
        ):
            return DatasetScope(
                dataset_id=name_or_scope.dataset_id,
                environment=resolved_env,
                account_label=resolved_acc,
                custom_name=name_or_scope.custom_name,
            )
        return name_or_scope

    raw = name_or_scope.strip()
    if not raw:
        raise ValueError("dataset name must not be empty")

    import re

    # Match account_snapshots_<account_label> or account_snapshots_<env>_<account_label>
    acc_snap_match = re.match(r"^account_snapshots(?:_live)?_([a-zA-Z0-9_-]+)$", raw)
    if acc_snap_match:
        extracted_acc = acc_snap_match.group(1)
        return DatasetScope(
            dataset_id=DatasetId.ACCOUNT_SNAPSHOTS,
            environment=environment,
            account_label=account_label or extracted_acc,
        )

    if raw == "account_snapshots":
        return DatasetScope(
            dataset_id=DatasetId.ACCOUNT_SNAPSHOTS,
            environment=environment,
            account_label=account_label,
        )

    # Check physical tables
    if raw in TABLE_TO_DATASET:
        return DatasetScope(
            dataset_id=TABLE_TO_DATASET[raw],
            environment=environment,
            account_label=account_label,
            custom_name=raw,
        )

    # Check DatasetId enum values
    for d in DatasetId:
        if d.value == raw:
            return DatasetScope(
                dataset_id=d,
                environment=environment,
                account_label=account_label,
            )

    # Unknown or generic dataset (e.g. test tables)
    return DatasetScope(
        dataset_id=DatasetId.GENERIC,
        environment=environment,
        account_label=account_label,
        custom_name=raw,
    )


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
    partitions_dropped: int = 0
    bytes_deleted: int = 0
    batches: int = 0
    details: str = ""
    executed_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True, slots=True)
class PruneOutcome:
    """Structured outcome emitted by prune execution per Section 10.2 RFC."""

    rows_archived: int = 0
    rows_deleted: int = 0
    partitions_dropped: int = 0
    bytes_deleted: int = 0
    batches: int = 0
    status: PruneReceiptStatus = PruneReceiptStatus.SUCCESS
    details: str = ""

    def to_receipt(
        self,
        *,
        plan: PrunePlan,
        dependency_version_verified: str,
        executed_at: datetime | None = None,
    ) -> PruneReceipt:
        summary_details = self.details or (
            f"Archived {self.rows_archived} rows, deleted {self.rows_deleted} rows, "
            f"dropped {self.partitions_dropped} partitions "
            f"across {self.batches} batches."
        )
        return PruneReceipt(
            plan_id=plan.plan_id,
            dataset_name=plan.dataset_name,
            effective_cutoff=plan.effective_cutoff,
            rows_archived=self.rows_archived,
            rows_deleted=self.rows_deleted,
            manifest_hash=plan.manifest_hash,
            dependency_version_verified=dependency_version_verified,
            status=self.status,
            partitions_dropped=self.partitions_dropped,
            bytes_deleted=self.bytes_deleted,
            batches=self.batches,
            details=summary_details,
            executed_at=executed_at or datetime.now(UTC),
        )


@dataclass(frozen=True, slots=True)
class RestoreReceipt:
    """Verification receipt for recovery/restore dry-runs or executions."""

    recovery_spec: RecoverySpec
    restored_rows: int
    verified_at: datetime
    status: str
    details: str = ""
