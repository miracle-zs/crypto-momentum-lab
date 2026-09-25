"""CapabilityEvaluator evaluating per-action execution permissions from evidence.

Obeys Astra Architecture Blueprint 2026-09-25:
- evaluate(action, versioned_evidence, runtime_plan) -> CapabilityDecision
- Actions: ENTER, NORMAL_EXIT, CANCEL, RECONCILE, EMERGENCY_REDUCE
- Never maps both 'stale market' and 'batch attribution conflict' to 'allow all exits'.
- NORMAL_EXIT requires trustworthy batch attribution (is_account_concordant=True).
- CANCEL is never blocked by lagging research archives or stale market data.
- EMERGENCY_REDUCE requires active writer lease and account identity verification.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum

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

        # 1. RECONCILE is always allowed to restore system visibility
        if action == SystemAction.RECONCILE:
            return CapabilityDecision(
                action=action,
                allowed=True,
                reason="reconcile_always_permitted",
                evidence_version=evidence.evidence_version,
                plan_id=plan.plan_id,
                valid_until=valid_until,
            )

        # Mutating exchange actions require an active writer lease and verified identity
        if not evidence.is_lease_active:
            return CapabilityDecision(
                action=action,
                allowed=False,
                reason="writer_lease_inactive",
                evidence_version=evidence.evidence_version,
                plan_id=plan.plan_id,
                valid_until=valid_until,
            )

        if not evidence.is_account_identity_verified:
            return CapabilityDecision(
                action=action,
                allowed=False,
                reason="account_identity_unverified",
                evidence_version=evidence.evidence_version,
                plan_id=plan.plan_id,
                valid_until=valid_until,
            )

        # 2. CANCEL: Allowed as long as lease and identity are valid
        # Never blocked by stale market data, discordant batches, or collector lag!
        if action == SystemAction.CANCEL:
            return CapabilityDecision(
                action=action,
                allowed=True,
                reason="cancel_permitted_under_active_lease",
                evidence_version=evidence.evidence_version,
                plan_id=plan.plan_id,
                valid_until=valid_until,
            )

        # 3. EMERGENCY_REDUCE: Requires explicit emergency authorization
        if action == SystemAction.EMERGENCY_REDUCE:
            if not evidence.is_emergency_authorized:
                return CapabilityDecision(
                    action=action,
                    allowed=False,
                    reason="emergency_reduce_not_authorized",
                    evidence_version=evidence.evidence_version,
                    plan_id=plan.plan_id,
                    valid_until=valid_until,
                )
            return CapabilityDecision(
                action=action,
                allowed=True,
                reason="emergency_reduce_authorized",
                evidence_version=evidence.evidence_version,
                plan_id=plan.plan_id,
                valid_until=valid_until,
            )

        # 4. NORMAL_EXIT: Requires trustworthy batch attribution and no inflight orders
        if action == SystemAction.NORMAL_EXIT:
            if not evidence.is_account_concordant:
                return CapabilityDecision(
                    action=action,
                    allowed=False,
                    reason="batch_attribution_conflict_or_gap",
                    evidence_version=evidence.evidence_version,
                    plan_id=plan.plan_id,
                    valid_until=valid_until,
                )
            if evidence.unresolved_inflight_orders_count > 0:
                return CapabilityDecision(
                    action=action,
                    allowed=False,
                    reason="unresolved_inflight_orders_present",
                    evidence_version=evidence.evidence_version,
                    plan_id=plan.plan_id,
                    valid_until=valid_until,
                )
            if evidence.market_freshness_seconds > self._max_exit_age:
                return CapabilityDecision(
                    action=action,
                    allowed=False,
                    reason="market_data_too_stale_for_normal_exit",
                    evidence_version=evidence.evidence_version,
                    plan_id=plan.plan_id,
                    valid_until=valid_until,
                )
            return CapabilityDecision(
                action=action,
                allowed=True,
                reason="normal_exit_prerequisites_satisfied",
                evidence_version=evidence.evidence_version,
                plan_id=plan.plan_id,
                valid_until=valid_until,
            )

        # 5. ENTER: Strictest prerequisites
        if action == SystemAction.ENTER:
            if not evidence.is_approval_valid:
                return CapabilityDecision(
                    action=action,
                    allowed=False,
                    reason="live_approval_invalid_or_expired",
                    evidence_version=evidence.evidence_version,
                    plan_id=plan.plan_id,
                    valid_until=valid_until,
                )
            if not evidence.is_account_concordant:
                return CapabilityDecision(
                    action=action,
                    allowed=False,
                    reason="account_ledger_not_concordant",
                    evidence_version=evidence.evidence_version,
                    plan_id=plan.plan_id,
                    valid_until=valid_until,
                )
            if evidence.unresolved_inflight_orders_count > 0:
                return CapabilityDecision(
                    action=action,
                    allowed=False,
                    reason="unresolved_inflight_orders_present",
                    evidence_version=evidence.evidence_version,
                    plan_id=plan.plan_id,
                    valid_until=valid_until,
                )
            if not evidence.is_universe_ready:
                return CapabilityDecision(
                    action=action,
                    allowed=False,
                    reason="universe_or_warmup_not_ready",
                    evidence_version=evidence.evidence_version,
                    plan_id=plan.plan_id,
                    valid_until=valid_until,
                )
            if evidence.market_freshness_seconds > self._max_entry_age:
                return CapabilityDecision(
                    action=action,
                    allowed=False,
                    reason="market_data_stale_for_entry",
                    evidence_version=evidence.evidence_version,
                    plan_id=plan.plan_id,
                    valid_until=valid_until,
                )
            return CapabilityDecision(
                action=action,
                allowed=True,
                reason="entry_prerequisites_satisfied",
                evidence_version=evidence.evidence_version,
                plan_id=plan.plan_id,
                valid_until=valid_until,
            )

        return CapabilityDecision(
            action=action,
            allowed=False,
            reason="unknown_action",
            evidence_version=evidence.evidence_version,
            plan_id=plan.plan_id,
            valid_until=valid_until,
        )
