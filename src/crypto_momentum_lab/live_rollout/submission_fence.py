"""Last-moment durable fencing before a live entry reaches the exchange."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Protocol

from crypto_momentum_lab.domain.execution import OrderExecutionPlan
from crypto_momentum_lab.domain.risk import RiskHalt, TradingLease
from crypto_momentum_lab.domain.runtime.capability_evaluator import (
    CapabilityEvaluator,
    CapabilityEvidence,
    SystemAction,
)
from crypto_momentum_lab.domain.runtime.runtime_plan import RuntimePlan
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderPreSubmissionError,
)


class LiveRiskStateReader(Protocol):
    async def load_active_lease(
        self,
        environment: str,
        account_label: str,
        now: datetime,
    ) -> TradingLease | None: ...

    async def load_active_halts(
        self,
        environment: str,
        account_label: str,
    ) -> tuple[RiskHalt, ...]: ...


class LiveSubmissionFence:
    """Re-read durable control state immediately before an entry POST."""

    def __init__(
        self,
        *,
        risk_state: LiveRiskStateReader,
        environment: str,
        account_label: str,
        strategy_name: str,
        lease_owner: str,
        code_generation: str,
        active_lease: Callable[[], TradingLease | None] | None = None,
        entry_enabled: Callable[[], bool] | None = None,
        is_draining: Callable[[], Awaitable[bool]] | None = None,
        capability_evaluator: CapabilityEvaluator | None = None,
        runtime_plan: RuntimePlan | None = None,
        evidence_provider: (
            Callable[
                [OrderExecutionPlan, datetime],
                CapabilityEvidence | Awaitable[CapabilityEvidence],
            ]
            | None
        ) = None,
    ) -> None:
        if not environment.strip():
            raise ValueError("environment must not be empty")
        if not account_label.strip():
            raise ValueError("account_label must not be empty")
        if not strategy_name.strip():
            raise ValueError("strategy_name must not be empty")
        if not lease_owner.strip():
            raise ValueError("lease_owner must not be empty")
        if not code_generation.strip():
            raise ValueError("code_generation must not be empty")
        self._risk_state = risk_state
        self._environment = environment
        self._account_label = account_label
        self._strategy_name = strategy_name
        self._lease_owner = lease_owner
        self._code_generation = code_generation
        self._active_lease = active_lease
        self._entry_enabled = entry_enabled
        self._is_draining = is_draining
        self._capability_evaluator = capability_evaluator
        self._runtime_plan = runtime_plan
        self._evidence_provider = evidence_provider

    async def validate(
        self,
        plan: OrderExecutionPlan,
        checked_at: datetime,
    ) -> None:
        """Raise ``OrderPreSubmissionError`` if an entry fence is stale."""

        client_order_id = getattr(plan, "client_order_id", None)
        if client_order_id is not None and not str(client_order_id).strip():
            raise OrderPreSubmissionError("client_order_id must not be empty")

        if plan.reduce_only:
            current_lease = await self._risk_state.load_active_lease(
                self._environment,
                self._account_label,
                checked_at,
            )
            if current_lease is None:
                raise OrderPreSubmissionError("active lease disappeared")
            if current_lease.owner != self._lease_owner:
                raise OrderPreSubmissionError("active lease owner changed")
            if current_lease.strategy_name != self._strategy_name:
                raise OrderPreSubmissionError("active lease strategy changed")
            if current_lease.code_generation != self._code_generation:
                raise OrderPreSubmissionError(
                    "active lease code generation changed"
                )
            expected_lease = (
                None if self._active_lease is None else self._active_lease()
            )
            if (
                expected_lease is not None
                and current_lease.lease_id != expected_lease.lease_id
            ):
                raise OrderPreSubmissionError(
                    "active lease fencing token changed"
                )

            if (
                self._capability_evaluator is not None
                and self._runtime_plan is not None
            ):
                evidence: CapabilityEvidence
                if self._evidence_provider is not None:
                    res = self._evidence_provider(plan, checked_at)
                    if inspect.isawaitable(res):
                        evidence = await res
                    else:
                        evidence = res
                else:
                    evidence = CapabilityEvidence(
                        evidence_version=f"ev_{self._account_label}_{checked_at.isoformat()}",
                        market_freshness_seconds=0.0,
                        is_account_concordant=True,
                        is_account_identity_verified=True,
                        unresolved_inflight_orders_count=0,
                        is_approval_valid=True,
                        is_lease_active=True,
                        is_emergency_authorized=False,
                        is_universe_ready=True,
                        plan_hash=self._runtime_plan.plan_hash,
                        runtime_generation=self._runtime_plan.runtime_generation,
                        fencing_epoch=self._runtime_plan.fencing_epoch,
                        declared_schema_compatibility=self._runtime_plan.declared_schema_compatibility,
                        observed_database_revision=self._runtime_plan.observed_database_revision,
                        observed_at=checked_at,
                    )
                decision = self._capability_evaluator.evaluate(
                    SystemAction.NORMAL_EXIT, evidence, self._runtime_plan
                )
                if not decision.allowed:
                    raise OrderPreSubmissionError(
                        f"capability_evaluator_blocked: {decision.reason}"
                    )
            return

        if self._entry_enabled is not None and not self._entry_enabled():
            raise OrderPreSubmissionError("live entry lane is disabled")
        if self._is_draining is not None and await self._is_draining():
            raise OrderPreSubmissionError("live session entries are disabled")

        current_lease = await self._risk_state.load_active_lease(
            self._environment,
            self._account_label,
            checked_at,
        )
        if current_lease is None:
            raise OrderPreSubmissionError("active lease disappeared")
        if current_lease.owner != self._lease_owner:
            raise OrderPreSubmissionError("active lease owner changed")
        if current_lease.strategy_name != self._strategy_name:
            raise OrderPreSubmissionError("active lease strategy changed")
        if current_lease.code_generation != self._code_generation:
            raise OrderPreSubmissionError("active lease code generation changed")
        expected_lease = (
            None if self._active_lease is None else self._active_lease()
        )
        if (
            expected_lease is not None
            and current_lease.lease_id != expected_lease.lease_id
        ):
            raise OrderPreSubmissionError("active lease fencing token changed")

        halts = await self._risk_state.load_active_halts(
            self._environment,
            self._account_label,
        )
        if halts:
            raise OrderPreSubmissionError("active risk halt")

        if self._capability_evaluator is not None and self._runtime_plan is not None:
            evidence_entry: CapabilityEvidence
            if self._evidence_provider is not None:
                res = self._evidence_provider(plan, checked_at)
                if inspect.isawaitable(res):
                    evidence_entry = await res
                else:
                    evidence_entry = res
            else:
                is_app_valid = (
                    self._entry_enabled is None or self._entry_enabled()
                ) and not bool(halts)
                evidence_entry = CapabilityEvidence(
                    evidence_version=f"ev_{self._account_label}_{checked_at.isoformat()}",
                    market_freshness_seconds=0.0,
                    is_account_concordant=True,
                    is_account_identity_verified=True,
                    unresolved_inflight_orders_count=0,
                    is_approval_valid=is_app_valid,
                    is_lease_active=True,
                    is_emergency_authorized=False,
                    is_universe_ready=True,
                    plan_hash=self._runtime_plan.plan_hash,
                    runtime_generation=self._runtime_plan.runtime_generation,
                    fencing_epoch=self._runtime_plan.fencing_epoch,
                    declared_schema_compatibility=self._runtime_plan.declared_schema_compatibility,
                    observed_database_revision=self._runtime_plan.observed_database_revision,
                    observed_at=checked_at,
                )
            decision = self._capability_evaluator.evaluate(
                SystemAction.ENTER, evidence_entry, self._runtime_plan
            )
            if not decision.allowed:
                raise OrderPreSubmissionError(
                    f"capability_evaluator_blocked: {decision.reason}"
                )


__all__ = ["LiveRiskStateReader", "LiveSubmissionFence"]
