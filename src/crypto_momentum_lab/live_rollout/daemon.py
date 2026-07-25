from collections.abc import AsyncIterable, Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from typing import Protocol

from crypto_momentum_lab.domain.account import ExecutionAccountStatus
from crypto_momentum_lab.domain.execution import ExchangeOrderState
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.risk import (
    RiskConfigSnapshot,
    RiskDecision,
    RiskEvaluation,
    RiskHalt,
    StrategyLiveState,
    TradingLease,
)
from crypto_momentum_lab.domain.strategy import (
    OrderIntentCandidate,
    StrategyCheckpoint,
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
from crypto_momentum_lab.live_rollout.gates import LiveGateContext, evaluate_live_gate
from crypto_momentum_lab.live_rollout.limits import (
    FixedLiveLimits,
    LiveLimitContext,
    evaluate_fixed_live_limits,
)
from crypto_momentum_lab.risk.gateway import RiskContext, RiskGateway


class LiveRuntimeStrategy(Protocol):
    def on_market_state(self, state: MarketState15s) -> StrategyDecision: ...


class LiveDaemonRepository(Protocol):
    async def save_approved_intent(
        self,
        intent: OrderIntentCandidate,
        evaluation: RiskEvaluation,
    ) -> None: ...

    async def save_checkpoint(
        self,
        run_id: str,
        checkpoint: StrategyCheckpoint,
        saved_at: datetime,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class LiveDaemonConfig:
    run_id: str
    max_market_state_age_seconds: float
    resize_tolerance: Decimal
    checkpoint_every_states: int

    def __post_init__(self) -> None:
        if not self.run_id.strip():
            raise ValueError("run_id must not be empty")
        if self.max_market_state_age_seconds <= 0:
            raise ValueError("max_market_state_age_seconds must be positive")
        if self.resize_tolerance < 0 or self.resize_tolerance >= 1:
            raise ValueError("resize_tolerance must be in [0, 1)")
        if self.checkpoint_every_states <= 0:
            raise ValueError("checkpoint_every_states must be positive")


@dataclass(frozen=True, slots=True)
class LiveDaemonRuntimeContext:
    now: datetime
    gate_context: LiveGateContext
    active_lease: TradingLease | None
    account_state: ExecutionAccountStatus
    account_observed_at: datetime | None
    open_position_symbols: frozenset[str] | None
    realized_pnl: Decimal | None
    unrealized_pnl: Decimal | None
    gross_exposure: Decimal | None
    active_halts: tuple[RiskHalt, ...]
    unresolved_order_states: tuple[ExchangeOrderState, ...]
    risk_config: RiskConfigSnapshot
    strategy_state: StrategyLiveState
    trading_rules: dict[str, SymbolTradingRules]
    last_entry_at_by_symbol: dict[str, datetime]


@dataclass(frozen=True, slots=True)
class LiveDaemonResult:
    processed_state_count: int
    approved_intent_count: int
    submitted_order_count: int
    halt_reason: str | None
    final_state_at: datetime | None


class LiveStrategyDaemon:
    def __init__(
        self,
        *,
        strategy: LiveRuntimeStrategy,
        risk_gateway: RiskGateway,
        limits: FixedLiveLimits,
        repository: LiveDaemonRepository,
        state_machine: OrderExecutionStateMachine,
        context_provider: Callable[
            [MarketState15s],
            Awaitable[LiveDaemonRuntimeContext],
        ],
        config: LiveDaemonConfig,
    ) -> None:
        self._strategy = strategy
        self._risk_gateway = risk_gateway
        self._limits = limits
        self._repository = repository
        self._state_machine = state_machine
        self._context_provider = context_provider
        self._config = config

    async def run(
        self,
        states: AsyncIterable[MarketState15s],
    ) -> LiveDaemonResult:
        processed = approved = submitted = 0
        final_state_at: datetime | None = None
        latest_checkpoint: StrategyCheckpoint | None = None
        latest_saved_at: datetime | None = None
        async for state in states:
            context = await self._context_provider(state)
            gate = evaluate_live_gate(
                replace(
                    context.gate_context,
                    now=context.now,
                    active_lease=context.active_lease,
                    account_state=context.account_state,
                    active_halts=context.active_halts,
                    unresolved_order_states=context.unresolved_order_states,
                )
            )
            if not gate.approved:
                await self._save_final_checkpoint(latest_checkpoint, latest_saved_at)
                return LiveDaemonResult(
                    processed,
                    approved,
                    submitted,
                    f"live_gate:{','.join(gate.reasons)}",
                    final_state_at,
                )
            if _market_age_seconds(context.now, state) > (
                self._config.max_market_state_age_seconds
            ):
                await self._save_final_checkpoint(latest_checkpoint, latest_saved_at)
                return LiveDaemonResult(
                    processed,
                    approved,
                    submitted,
                    "stale_market_state",
                    final_state_at,
                )
            decision = self._strategy.on_market_state(state)
            processed += 1
            final_state_at = state.bucket_start
            latest_checkpoint = decision.checkpoint
            latest_saved_at = context.now
            for candidate in decision.candidates:
                executable_candidate = candidate
                if not candidate.reduce_only:
                    limit_decision = evaluate_fixed_live_limits(
                        self._limits,
                        LiveLimitContext(
                            now=context.now,
                            symbol=candidate.symbol,
                            requested_notional=candidate.desired_notional,
                            open_position_symbols=context.open_position_symbols,
                            last_entry_at=context.last_entry_at_by_symbol.get(
                                candidate.symbol
                            ),
                            realized_pnl=context.realized_pnl,
                            unrealized_pnl=context.unrealized_pnl,
                            gross_exposure=context.gross_exposure,
                            spread=state.spread,
                            min_notional=_min_notional(
                                context.trading_rules.get(candidate.symbol)
                            ),
                            account_observed_at=context.account_observed_at,
                            market_observed_at=state.bucket_end,
                            has_unresolved_order=any(
                                not item.terminal
                                for item in context.unresolved_order_states
                            ),
                        ),
                    )
                    if not limit_decision.allowed:
                        continue
                    executable_candidate = replace(
                        candidate,
                        desired_notional=limit_decision.capped_notional,
                    )
                evaluation = self._risk_gateway.evaluate(
                    executable_candidate,
                    RiskContext(
                        now=context.now,
                        active_lease=context.active_lease,
                        latest_market_state=state,
                        account_state=context.account_state,
                        open_position_symbols=context.open_position_symbols
                        or frozenset(),
                        active_halts=context.active_halts,
                        risk_config=context.risk_config,
                        strategy_state=context.strategy_state,
                    ),
                )
                if evaluation.decision is not RiskDecision.APPROVED:
                    continue
                rules = context.trading_rules.get(candidate.symbol)
                reference_price = state.mark_price or state.close_price
                if rules is None or reference_price is None:
                    continue
                plan = quantize_order_plan(
                    executable_candidate,
                    rules,
                    reference_price=reference_price,
                    resize_tolerance=self._config.resize_tolerance,
                )
                if isinstance(plan, QuantizationRejection):
                    continue
                await self._repository.save_approved_intent(
                    executable_candidate,
                    evaluation,
                )
                approved += 1
                result = await self._state_machine.execute_approved_intent(plan)
                submitted += int(not result.suppressed)
                if result.state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION:
                    await self._save_final_checkpoint(
                        latest_checkpoint,
                        latest_saved_at,
                    )
                    return LiveDaemonResult(
                        processed,
                        approved,
                        submitted,
                        "unknown_order_pending_reconciliation",
                        final_state_at,
                    )
            if processed % self._config.checkpoint_every_states == 0:
                await self._repository.save_checkpoint(
                    self._config.run_id,
                    decision.checkpoint,
                    context.now,
                )
                latest_checkpoint = None
                latest_saved_at = None
        await self._save_final_checkpoint(latest_checkpoint, latest_saved_at)
        return LiveDaemonResult(
            processed,
            approved,
            submitted,
            None,
            final_state_at,
        )

    async def _save_final_checkpoint(
        self,
        checkpoint: StrategyCheckpoint | None,
        saved_at: datetime | None,
    ) -> None:
        if checkpoint is None or saved_at is None:
            return
        await self._repository.save_checkpoint(
            self._config.run_id,
            checkpoint,
            saved_at,
        )


def _market_age_seconds(now: datetime, state: MarketState15s) -> float:
    return (now - state.bucket_end).total_seconds()


def _min_notional(rules: SymbolTradingRules | None) -> Decimal | None:
    return None if rules is None else rules.min_notional
