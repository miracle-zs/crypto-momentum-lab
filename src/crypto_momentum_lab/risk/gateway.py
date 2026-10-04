from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

from crypto_momentum_lab.domain.risk import (
    RiskConfigSnapshot,
    RiskDecision,
    RiskEvaluation,
    RiskHalt,
    StrategyLiveState,
)
from crypto_momentum_lab.domain.risk.limits import (
    FixedLiveLimits,
    LiveLimitContext,
    evaluate_fixed_live_limits,
)
from crypto_momentum_lab.domain.strategy import OrderIntentCandidate


@dataclass(frozen=True, slots=True)
class RiskContext:
    now: datetime
    open_position_symbols: frozenset[str]
    active_halts: tuple[RiskHalt, ...]
    risk_config: RiskConfigSnapshot
    strategy_state: StrategyLiveState


@dataclass(frozen=True, slots=True)
class CandidateRiskAssessment:
    candidate: OrderIntentCandidate | None
    evaluation: RiskEvaluation
    approved_notional: Decimal | None = None


class RiskGateway:
    def __init__(self, *, limits: FixedLiveLimits | None = None) -> None:
        self._limits = limits

    @property
    def limits(self) -> FixedLiveLimits | None:
        return self._limits

    def evaluate(
        self,
        intent: OrderIntentCandidate,
        context: RiskContext,
        *,
        limit_context: LiveLimitContext | None = None,
    ) -> CandidateRiskAssessment:
        """Apply entry limits, then evaluate authority for the resulting candidate."""
        capped_notional: Decimal | None = None
        if self._limits is not None and not intent.reduce_only:
            if limit_context is None:
                raise ValueError("entry limit context is required")
            if (
                limit_context.symbol != intent.symbol
                or limit_context.requested_notional != intent.desired_notional
                or limit_context.open_position_symbols != context.open_position_symbols
            ):
                raise ValueError(
                    "entry limit context must match the candidate risk facts"
                )
            configured_max = context.risk_config.max_open_positions
            effective_limits = replace(
                self._limits,
                max_open_positions=(
                    configured_max
                    if self._limits.max_open_positions is None
                    else self._limits.max_open_positions
                    if configured_max is None
                    else min(self._limits.max_open_positions, configured_max)
                ),
            )
            decision = evaluate_fixed_live_limits(effective_limits, limit_context)
            if not decision.allowed:
                return CandidateRiskAssessment(
                    candidate=None,
                    evaluation=_evaluation(
                        intent,
                        context,
                        RiskDecision.REJECTED,
                        decision.reason,
                    ),
                    approved_notional=None,
                )
            intent = replace(intent, desired_notional=decision.capped_notional)
            capped_notional = decision.capped_notional
        elif intent.reduce_only:
            capped_notional = intent.desired_notional

        auth_eval = self._evaluate_authority(
            intent, context, position_limit_checked=self._limits is not None
        )
        is_approved = auth_eval.decision is RiskDecision.APPROVED
        return CandidateRiskAssessment(
            candidate=intent,
            evaluation=auth_eval,
            approved_notional=capped_notional if is_approved else None,
        )

    def validate_quantized_notional(
        self,
        actual_notional: Decimal,
        context: RiskContext,
        gross_exposure: Decimal | None = None,
        *,
        approved_notional: Decimal | None = None,
    ) -> tuple[bool, str | None]:
        """Unified hard risk check on quantized order notional for non-reduce_only entries."""
        if approved_notional is not None and actual_notional > approved_notional:
            return False, "quantized_order_notional_exceeds_approved_notional"
        if context.risk_config.max_order_notional is not None:
            if actual_notional > context.risk_config.max_order_notional:
                return False, "quantized_order_notional_exceeds_max_order_notional"
        if context.risk_config.max_gross_notional is not None:
            if gross_exposure is None:
                return False, "missing_gross_exposure"
            effective_gross = gross_exposure
            if (
                effective_gross + actual_notional
                > context.risk_config.max_gross_notional
            ):
                return False, "quantized_order_notional_exceeds_max_gross_notional"
        return True, None

    def _evaluate_authority(
        self,
        intent: OrderIntentCandidate,
        context: RiskContext,
        *,
        position_limit_checked: bool = False,
    ) -> RiskEvaluation:
        if context.now.tzinfo is None or context.now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        if intent.reduce_only:
            return _evaluation(intent, context, RiskDecision.APPROVED, "reduce_only")
        if context.active_halts:
            return _evaluation(intent, context, RiskDecision.HALTED, "active_halt")
        if context.strategy_state is StrategyLiveState.HALTED:
            return _evaluation(intent, context, RiskDecision.HALTED, "strategy_halted")
        if context.strategy_state is StrategyLiveState.DRAINING:
            return _evaluation(intent, context, RiskDecision.REJECTED, "entries_disabled")
        desired_notional = intent.desired_notional
        if desired_notional is None:
            return _evaluation(
                intent,
                context,
                RiskDecision.REJECTED,
                "missing_desired_notional",
            )
        if desired_notional <= 0:
            return _evaluation(
                intent,
                context,
                RiskDecision.REJECTED,
                "invalid_desired_notional",
            )
        if (
            context.risk_config.max_order_notional is None
            or desired_notional > context.risk_config.max_order_notional
        ):
            return _evaluation(
                intent,
                context,
                RiskDecision.REJECTED,
                "missing_max_order_notional_limit"
                if context.risk_config.max_order_notional is None
                else "max_order_notional_exceeded",
            )
        if context.risk_config.max_open_positions is None or (
            not position_limit_checked
            and len(context.open_position_symbols)
            >= context.risk_config.max_open_positions
            and intent.symbol not in context.open_position_symbols
        ):
            return _evaluation(
                intent,
                context,
                RiskDecision.REJECTED,
                "missing_max_open_positions_limit"
                if context.risk_config.max_open_positions is None
                else "max_open_positions_exceeded",
            )
        return _evaluation(intent, context, RiskDecision.APPROVED, "approved")


def _evaluation(
    intent: OrderIntentCandidate,
    context: RiskContext,
    decision: RiskDecision,
    reason: str,
) -> RiskEvaluation:
    return RiskEvaluation(
        evaluation_id=str(
            uuid5(
                NAMESPACE_URL,
                "risk-evaluation:"
                f"{intent.candidate_id}:{decision.value}:{reason}:"
                f"{context.now.isoformat()}",
            )
        ),
        candidate_id=intent.candidate_id,
        decision=decision,
        reason=reason,
        evaluated_at=context.now,
        details={
            "symbol": intent.symbol,
            "desired_notional": None
            if intent.desired_notional is None
            else str(intent.desired_notional),
        },
    )
