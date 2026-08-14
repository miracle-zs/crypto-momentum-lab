from dataclasses import dataclass
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


class RiskGateway:
    def evaluate(
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
        if context.enforce_market_state_age and _market_age_seconds(context) > (
            context.risk_config.max_market_state_age_seconds
        ):
            return _evaluation(
                intent,
                context,
                RiskDecision.REJECTED,
                "stale_market_state",
            )
        if context.account_state is not ExecutionAccountStatus.READY_READONLY:
            return _evaluation(
                intent,
                context,
                RiskDecision.REJECTED,
                "account_not_ready",
            )
        if context.strategy_state is StrategyLiveState.HALTED:
            return _evaluation(
                intent,
                context,
                RiskDecision.HALTED,
                "strategy_halted",
            )
        if context.strategy_state is StrategyLiveState.DRAINING:
            if (
                intent.reduce_only
                and context.risk_config.allow_reduce_only_while_draining
            ):
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
        if intent.reduce_only:
            return _evaluation(
                intent,
                context,
                RiskDecision.APPROVED,
                "reduce_only",
            )
        if (
            context.risk_config.max_order_notional is not None
            and desired_notional > context.risk_config.max_order_notional
        ):
            return _evaluation(
                intent,
                context,
                RiskDecision.REJECTED,
                "max_order_notional_exceeded",
            )
        if (
            context.risk_config.max_open_positions is not None
            and len(context.open_position_symbols)
            >= context.risk_config.max_open_positions
            and intent.symbol not in context.open_position_symbols
        ):
            return _evaluation(
                intent,
                context,
                RiskDecision.REJECTED,
                "max_open_positions_exceeded",
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
