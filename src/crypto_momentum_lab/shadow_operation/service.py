from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol
from uuid import NAMESPACE_URL, uuid5

from crypto_momentum_lab.domain.account import ExecutionAccountStatus
from crypto_momentum_lab.domain.execution import OrderExecutionPlan
from crypto_momentum_lab.domain.market.models import JsonValue, MarketState15s
from crypto_momentum_lab.domain.risk import (
    RiskConfigSnapshot,
    RiskDecision,
    RiskEvaluation,
    RiskHalt,
    StrategyLiveState,
    TradingLease,
    TradingLeaseState,
)
from crypto_momentum_lab.domain.strategy import (
    OrderIntentCandidate,
    StrategyDecision,
)
from crypto_momentum_lab.execution_account.orders.quantization import (
    QuantizationRejection,
    SymbolTradingRules,
    quantize_order_plan,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionStateMachine,
)
from crypto_momentum_lab.risk.gateway import RiskContext, RiskGateway
from crypto_momentum_lab.shadow_operation.models import (
    ShadowDecisionMetric,
    ShadowOrderPlan,
    ShadowSession,
)


class ShadowRuntimeStrategy(Protocol):
    def on_market_state(self, state: MarketState15s) -> StrategyDecision:
        pass


class ShadowRepository(Protocol):
    async def start_session(self, session_record: ShadowSession) -> None:
        pass

    async def end_session(
        self,
        run_id: str,
        *,
        state: str,
        ended_at: datetime,
    ) -> None:
        pass

    async def save_order_plan(self, plan: ShadowOrderPlan) -> None:
        pass

    async def save_metric(self, metric: ShadowDecisionMetric) -> None:
        pass


class ApprovedIntentRepository(Protocol):
    async def save_approved_intent(
        self,
        intent: OrderIntentCandidate,
        evaluation: RiskEvaluation,
    ) -> None:
        pass


@dataclass(frozen=True, slots=True)
class ShadowOperationConfig:
    run_id: str
    account_label: str
    strategy_name: str
    strategy_config_hash: str
    lease_owner: str
    max_market_state_age_seconds: float
    resize_tolerance: Decimal


@dataclass(frozen=True, slots=True)
class ShadowOperationContext:
    now: datetime
    active_lease: TradingLease | None
    account_state: ExecutionAccountStatus
    open_position_symbols: frozenset[str]
    active_halts: tuple[RiskHalt, ...]
    risk_config: RiskConfigSnapshot
    strategy_state: StrategyLiveState
    trading_rules: dict[str, SymbolTradingRules]


@dataclass(frozen=True, slots=True)
class ShadowOperationResult:
    processed_state_count: int
    approved_intent_count: int
    suppression_count: int
    halt_reason: str | None


class ShadowOperationService:
    def __init__(
        self,
        *,
        strategy: ShadowRuntimeStrategy,
        risk_gateway: RiskGateway,
        shadow_repository: ShadowRepository,
        approved_intent_repository: ApprovedIntentRepository,
        state_machine: OrderExecutionStateMachine,
        config: ShadowOperationConfig,
    ) -> None:
        self._strategy = strategy
        self._risk_gateway = risk_gateway
        self._shadow_repository = shadow_repository
        self._approved_intent_repository = approved_intent_repository
        self._state_machine = state_machine
        self._config = config

    async def run(
        self,
        states: Iterable[MarketState15s],
        context: ShadowOperationContext,
    ) -> ShadowOperationResult:
        halt_reason = _preflight_reason(self._config, context)
        await self._shadow_repository.start_session(
            ShadowSession(
                run_id=self._config.run_id,
                account_label=self._config.account_label,
                strategy_name=self._config.strategy_name,
                strategy_config_hash=self._config.strategy_config_hash,
                state="halted" if halt_reason else "running",
                account_readiness=context.account_state.value,
                started_at=context.now,
                ended_at=context.now if halt_reason else None,
                details={} if halt_reason is None else {"reason": halt_reason},
            )
        )
        if halt_reason is not None:
            return ShadowOperationResult(0, 0, 0, halt_reason)

        processed = approved = suppressed = 0
        for state in states:
            if _market_age_seconds(context.now, state) > (
                self._config.max_market_state_age_seconds
            ):
                halt_reason = "stale_market_state"
                await self._save_metric(
                    "stale_data_block",
                    state,
                    halt_reason,
                    context.now,
                )
                break
            decision = self._strategy.on_market_state(state)
            processed += 1
            for signal in decision.signals:
                await self._save_metric("signal", state, None, signal.detected_at)
            for rejection in decision.rejections:
                await self._save_metric(
                    "rejected",
                    state,
                    rejection.reason.value,
                    context.now,
                )
            for intent in decision.candidates:
                evaluation = self._risk_gateway.evaluate(
                    intent,
                    RiskContext(
                        now=context.now,
                        active_lease=context.active_lease,
                        latest_market_state=state,
                        account_state=context.account_state,
                        open_position_symbols=context.open_position_symbols,
                        active_halts=context.active_halts,
                        risk_config=context.risk_config,
                        strategy_state=context.strategy_state,
                    ),
                )
                if evaluation.decision is not RiskDecision.APPROVED:
                    await self._save_metric(
                        "risk_block",
                        state,
                        evaluation.reason,
                        evaluation.evaluated_at,
                    )
                    continue
                rules = context.trading_rules.get(intent.symbol)
                reference_price = state.mark_price or state.close_price
                if rules is None or reference_price is None:
                    await self._save_metric(
                        "rejected",
                        state,
                        "missing_trading_rules_or_price",
                        context.now,
                    )
                    continue
                plan = quantize_order_plan(
                    intent,
                    rules,
                    reference_price=reference_price,
                    resize_tolerance=self._config.resize_tolerance,
                )
                if isinstance(plan, QuantizationRejection):
                    await self._save_metric(
                        "rejected",
                        state,
                        plan.reason,
                        context.now,
                    )
                    continue
                await self._approved_intent_repository.save_approved_intent(
                    intent,
                    evaluation,
                )
                await self._shadow_repository.save_order_plan(
                    ShadowOrderPlan(
                        order_plan_id=plan.client_order_id,
                        run_id=self._config.run_id,
                        order_intent_id=intent.candidate_id,
                        symbol=intent.symbol,
                        decision_state="approved",
                        account_readiness=context.account_state.value,
                        market_freshness="fresh",
                        risk_result=evaluation.decision.value,
                        state_closed_at=state.bucket_end,
                        created_at=context.now,
                        order_payload=_order_payload(plan),
                    )
                )
                result = await self._state_machine.execute_approved_intent(plan)
                approved += 1
                suppressed += int(result.suppressed)
                await self._save_metric(
                    "approved_intent",
                    state,
                    None,
                    context.now,
                )

        final_state = "halted" if halt_reason else "completed"
        await self._shadow_repository.end_session(
            self._config.run_id,
            state=final_state,
            ended_at=context.now,
        )
        return ShadowOperationResult(processed, approved, suppressed, halt_reason)

    async def _save_metric(
        self,
        category: str,
        state: MarketState15s,
        reason: str | None,
        occurred_at: datetime,
    ) -> None:
        metric_id = str(
            uuid5(
                NAMESPACE_URL,
                f"shadow-metric:{self._config.run_id}:{category}:"
                f"{state.symbol}:{state.bucket_start.isoformat()}:{reason or ''}",
            )
        )
        await self._shadow_repository.save_metric(
            ShadowDecisionMetric(
                metric_id=metric_id,
                run_id=self._config.run_id,
                symbol=state.symbol,
                category=category,
                reason=reason,
                occurred_at=occurred_at,
                details={},
            )
        )


def _preflight_reason(
    config: ShadowOperationConfig,
    context: ShadowOperationContext,
) -> str | None:
    lease = context.active_lease
    if lease is None:
        return "missing_active_lease"
    if lease.state is not TradingLeaseState.ACTIVE or lease.expires_at <= context.now:
        return "inactive_or_expired_lease"
    if lease.owner != config.lease_owner:
        return "lease_owner_mismatch"
    if lease.account_label != config.account_label:
        return "lease_account_mismatch"
    if lease.strategy_name != config.strategy_name:
        return "lease_strategy_mismatch"
    if context.account_state is not ExecutionAccountStatus.READY_READONLY:
        return "account_not_ready"
    if context.active_halts:
        return "active_risk_halt"
    return None


def _market_age_seconds(now: datetime, state: MarketState15s) -> float:
    return (now - state.bucket_end).total_seconds()


def _order_payload(plan: OrderExecutionPlan) -> dict[str, JsonValue]:
    values = asdict(plan)
    return {
        key: str(value) if isinstance(value, Decimal | datetime) else value
        for key, value in values.items()
    }
