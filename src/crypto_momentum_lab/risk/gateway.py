from dataclasses import dataclass, replace
from datetime import datetime
from uuid import NAMESPACE_URL, uuid5

from crypto_momentum_lab.domain.account import ExecutionAccountStatus
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.risk import (
    RiskConfigSnapshot,
    RiskDecision,
    RiskEvaluation,
    RiskHalt,
    StrategyLiveState,
    TradingLease,
    TradingLeaseState,
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
    active_lease: TradingLease | None
    latest_market_state: MarketState15s
    account_state: ExecutionAccountStatus
    open_position_symbols: frozenset[str]
    active_halts: tuple[RiskHalt, ...]
    risk_config: RiskConfigSnapshot
    strategy_state: StrategyLiveState
    enforce_market_state_age: bool = True
    required_lease_owner: str | None = None
    required_lease_id: str | None = None
    required_account_label: str | None = None
    required_strategy_name: str | None = None


@dataclass(frozen=True, slots=True)
class CandidateRiskAssessment:
    candidate: OrderIntentCandidate | None
    evaluation: RiskEvaluation


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
            decision = evaluate_fixed_live_limits(self._limits, limit_context)
            if not decision.allowed:
                return CandidateRiskAssessment(
                    candidate=None,
                    evaluation=_evaluation(
                        intent,
                        context,
                        RiskDecision.REJECTED,
                        decision.reason,
                    ),
                )
            intent = replace(intent, desired_notional=decision.capped_notional)
        return CandidateRiskAssessment(
            candidate=intent,
            evaluation=self._evaluate_authority(intent, context),
        )

    def _evaluate_authority(
        self,
        intent: OrderIntentCandidate,
        context: RiskContext,
    ) -> RiskEvaluation:
        if context.now.tzinfo is None or context.now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        if context.active_halts:
            return _evaluation(intent, context, RiskDecision.HALTED, "active_halt")
        if context.active_lease is None:
            return _evaluation(
                intent,
                context,
                RiskDecision.REJECTED,
                "missing_active_lease",
            )
        if context.active_lease.state is not TradingLeaseState.ACTIVE:
            return _evaluation(intent, context, RiskDecision.REJECTED, "lease_inactive")
        if context.active_lease.expires_at <= context.now:
            return _evaluation(intent, context, RiskDecision.REJECTED, "lease_expired")
        if (
            context.required_lease_owner is not None
            and context.active_lease.owner != context.required_lease_owner
        ):
            return _evaluation(
                intent,
                context,
                RiskDecision.REJECTED,
                "lease_owner_mismatch",
            )
        if (
            context.required_lease_id is not None
            and context.active_lease.lease_id != context.required_lease_id
        ):
            return _evaluation(
                intent,
                context,
                RiskDecision.REJECTED,
                "lease_id_mismatch",
            )
        if (
            context.required_account_label is not None
            and context.active_lease.account_label != context.required_account_label
        ):
            return _evaluation(
                intent,
                context,
                RiskDecision.REJECTED,
                "lease_account_mismatch",
            )
        if (
            context.required_strategy_name is not None
            and context.active_lease.strategy_name != context.required_strategy_name
        ):
            return _evaluation(
                intent,
                context,
                RiskDecision.REJECTED,
                "lease_strategy_mismatch",
            )
        if context.enforce_market_state_age and _market_age_seconds(context) > (
            context.risk_config.max_market_state_age_seconds
        ):
            return _evaluation(
                intent,
                context,
                RiskDecision.REJECTED,
                "stale_market_state",
            )
        if context.strategy_state is StrategyLiveState.HALTED:
            return _evaluation(
                intent,
                context,
                RiskDecision.HALTED,
                "strategy_halted",
            )
        if intent.reduce_only:
            if context.strategy_state is StrategyLiveState.DRAINING:
                if context.risk_config.allow_reduce_only_while_draining:
                    return _evaluation(
                        intent,
                        context,
                        RiskDecision.APPROVED,
                        "reduce_only_draining",
                    )
                return _evaluation(
                    intent,
                    context,
                    RiskDecision.REJECTED,
                    "strategy_draining",
                )
            if context.account_state in (
                ExecutionAccountStatus.HALTED_READONLY,
                ExecutionAccountStatus.STOPPED,
            ):
                return _evaluation(
                    intent,
                    context,
                    RiskDecision.REJECTED,
                    "account_stopped",
                )
            return _evaluation(
                intent,
                context,
                RiskDecision.APPROVED,
                "reduce_only",
            )
        if context.account_state not in (
            ExecutionAccountStatus.RUNNING,
            ExecutionAccountStatus.READY_READONLY,
            ExecutionAccountStatus.SYNCING,
            ExecutionAccountStatus.DEGRADED,
        ):
            return _evaluation(
                intent,
                context,
                RiskDecision.REJECTED,
                "account_not_ready",
            )
        if context.strategy_state is StrategyLiveState.DRAINING:
            return _evaluation(
                intent,
                context,
                RiskDecision.REJECTED,
                "strategy_draining",
            )
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
            len(context.open_position_symbols) >= context.risk_config.max_open_positions
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


def _market_age_seconds(context: RiskContext) -> float:
    return (context.now - context.latest_market_state.bucket_end).total_seconds()


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
