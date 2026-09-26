"""CapabilityEvaluator evaluating per-action execution permissions from evidence.

Obeys Astra Architecture Blueprint 2026-09-25:
- evaluate(action, versioned_evidence, runtime_plan) -> CapabilityDecision
- Actions: ENTER, NORMAL_EXIT, CANCEL, RECONCILE, EMERGENCY_REDUCE
- Never maps both 'stale market' and 'batch attribution conflict' to 'allow all exits'.
- NORMAL_EXIT requires trustworthy batch attribution (is_account_concordant=True),
  but is never blocked by expired live entry approvals or entry lane state.
- CANCEL is never blocked by lagging research archives or stale market data.
- RECONCILE is always permitted (read-only visibility and idempotent state recovery).
- EMERGENCY_REDUCE requires active writer lease and explicit emergency authorization.
- Binds scope, plan_hash, runtime_generation, fencing_epoch, and source_as_of.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from crypto_momentum_lab.domain.execution.execution_book import ExecutionScope
from crypto_momentum_lab.domain.runtime.runtime_plan import RuntimePlan


class SystemAction(StrEnum):
    """Distinct operational actions with dedicated safety prerequisites."""

    ENTER = "enter"
    NORMAL_EXIT = "normal_exit"
    CANCEL = "cancel"
    RECONCILE = "reconcile"
    EMERGENCY_REDUCE = "emergency_reduce"


@dataclass(frozen=True, slots=True)
class CapabilityEvidence:
    """Versioned point-in-time operational evidence snapshot."""

    evidence_version: str
    market_freshness_seconds: float
    is_account_concordant: bool
    is_account_identity_verified: bool = True
    unresolved_inflight_orders_count: int = 0
    is_approval_valid: bool = True
    is_lease_active: bool = True
    is_emergency_authorized: bool = False
    is_universe_ready: bool = True
    is_collector_healthy: bool = True
    scope: ExecutionScope | None = None
    plan_hash: str | None = None
    runtime_generation: str | None = None
    fencing_epoch: int | None = None
    declared_schema_compatibility: str | None = None
    observed_database_revision: str | None = None
    source_as_of: datetime | None = None
    observed_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True, slots=True)
class CapabilityDecision:
    """Authoritative permission decision for a specific SystemAction."""

    action: SystemAction
    allowed: bool
    reason: str
    evidence_version: str
    plan_id: str
    valid_until: datetime
    scope: ExecutionScope | None = None
    plan_hash: str = ""
    runtime_generation: str = ""
    fencing_epoch: int = 1
    source_as_of: datetime = field(default_factory=lambda: datetime.now(UTC))


class CapabilityEvaluator:
    """Evaluates whether a specific SystemAction is safe to execute right now."""

    def __init__(
        self,
        *,
        max_entry_market_age_seconds: float = 15.0,
        max_exit_market_age_seconds: float = 60.0,
        decision_ttl_seconds: int = 5,
    ) -> None:
        self._max_entry_age = max_entry_market_age_seconds
        self._max_exit_age = max_exit_market_age_seconds
        self._ttl = timedelta(seconds=decision_ttl_seconds)

    def evaluate(
        self,
        action: SystemAction,
        evidence: CapabilityEvidence,
        plan: RuntimePlan,
    ) -> CapabilityDecision:
        """Evaluates action safety against versioned evidence and RuntimePlan."""
        valid_until = evidence.observed_at + self._ttl
        source_as_of = (
            evidence.source_as_of
            if evidence.source_as_of is not None
            else evidence.observed_at
        )

        def _decision(allowed: bool, reason: str) -> CapabilityDecision:
            return CapabilityDecision(
                action=action,
                allowed=allowed,
                reason=reason,
                evidence_version=evidence.evidence_version,
                plan_id=plan.plan_id,
                valid_until=valid_until,
                scope=evidence.scope,
                plan_hash=plan.plan_hash,
                runtime_generation=plan.runtime_generation,
                fencing_epoch=plan.fencing_epoch,
                source_as_of=source_as_of,
            )

        # 1. RECONCILE is always allowed to restore system visibility
        if action == SystemAction.RECONCILE:
            return _decision(True, "reconcile_always_permitted")

        # Mutating exchange actions require an active writer lease and verified identity
        if not evidence.is_lease_active:
            return _decision(False, "writer_lease_inactive")

        if not evidence.is_account_identity_verified:
            return _decision(False, "account_identity_unverified")

        # Check fencing epoch alignment if supplied in evidence
        if (
            evidence.fencing_epoch is not None
            and evidence.fencing_epoch != plan.fencing_epoch
        ):
            return _decision(False, "fencing_epoch_mismatch")

        # Check plan hash alignment if supplied in evidence
        if (
            evidence.plan_hash is not None
            and evidence.plan_hash != plan.plan_hash
        ):
            return _decision(False, "plan_hash_mismatch")

        # 2. CANCEL: Allowed as long as lease and identity are valid
        # Never blocked by stale market data, discordant batches, or collector lag!
        if action == SystemAction.CANCEL:
            return _decision(True, "cancel_permitted_under_active_lease")

        # 3. EMERGENCY_REDUCE: Requires explicit emergency authorization
        if action == SystemAction.EMERGENCY_REDUCE:
            if not evidence.is_emergency_authorized:
                return _decision(False, "emergency_reduce_not_authorized")
            return _decision(True, "emergency_reduce_authorized")

        # 4. NORMAL_EXIT: Requires trustworthy batch attribution and no inflight orders
        if action == SystemAction.NORMAL_EXIT:
            if not evidence.is_account_concordant:
                return _decision(False, "batch_attribution_conflict_or_gap")
            if evidence.unresolved_inflight_orders_count > 0:
                return _decision(False, "unresolved_inflight_orders_present")
            if evidence.market_freshness_seconds > self._max_exit_age:
                return _decision(
                    False, "market_data_too_stale_for_normal_exit"
                )
            return _decision(True, "normal_exit_prerequisites_satisfied")

        # 5. ENTER: Strictest prerequisites
        if action == SystemAction.ENTER:
            # Check database schema compatibility
            if evidence.observed_database_revision is not None:
                expected_rev = (
                    evidence.declared_schema_compatibility
                    or plan.declared_schema_compatibility
                )
                if (
                    expected_rev
                    and evidence.observed_database_revision != expected_rev
                ):
                    return _decision(False, "schema_compatibility_mismatch")

            if not evidence.is_approval_valid:
                return _decision(False, "live_approval_invalid_or_expired")
            if not evidence.is_account_concordant:
                return _decision(False, "account_ledger_not_concordant")
            if evidence.unresolved_inflight_orders_count > 0:
                return _decision(False, "unresolved_inflight_orders_present")
            if not evidence.is_universe_ready:
                return _decision(False, "universe_or_warmup_not_ready")
            if evidence.market_freshness_seconds > self._max_entry_age:
                return _decision(False, "market_data_stale_for_entry")
            return _decision(True, "entry_prerequisites_satisfied")

        return _decision(False, "unknown_action")
