from collections.abc import AsyncIterable, Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from typing import Protocol

from crypto_momentum_lab.domain.account import ExecutionAccountStatus
from crypto_momentum_lab.domain.execution import (
    ExchangeOrderState,
)
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
    OrderExecutionResult,
    OrderExecutionStateMachine,
)
from crypto_momentum_lab.live_rollout.exits import (
    LiveExitCancellationRequest,
    LiveExitManager,
    ManagedLivePosition,
)
from crypto_momentum_lab.live_rollout.gates import LiveGateContext, evaluate_live_gate
from crypto_momentum_lab.live_rollout.limits import (
    FixedLiveLimits,
    LiveLimitContext,
    evaluate_fixed_live_limits,
)
from crypto_momentum_lab.persistence.postgres.order_repository import (
    PersistedExchangeOrder,
)
from crypto_momentum_lab.risk.gateway import RiskContext, RiskGateway


class LiveRuntimeStrategy(Protocol):
    def on_market_state(self, state: MarketState15s) -> StrategyDecision: ...

    def checkpoint(self) -> StrategyCheckpoint: ...


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
    hedge_mode: bool = False
    entry_long_only: bool = False

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
    managed_positions: tuple[ManagedLivePosition, ...] = ()
    unmanaged_position_symbols: frozenset[str] = frozenset()
    unresolved_orders: tuple[PersistedExchangeOrder, ...] = ()


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
        exit_manager: LiveExitManager | None = None,
        reconcile_orders: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self._strategy = strategy
        self._risk_gateway = risk_gateway
        self._limits = limits
        self._repository = repository
        self._state_machine = state_machine
        self._context_provider = context_provider
        self._config = config
        self._exit_manager = exit_manager
        self._reconcile_orders = reconcile_orders

    async def run(
        self,
        states: AsyncIterable[MarketState15s],
    ) -> LiveDaemonResult:
        processed = approved = submitted = 0
        final_state_at: datetime | None = None
        checkpoint_dirty = False
        last_checkpoint_saved_at: datetime | None = None
        async for state in states:
            if self._reconcile_orders is not None:
                try:
                    await self._reconcile_orders()
                except Exception as error:
                    await self._save_final_checkpoint(
                        dirty=checkpoint_dirty,
                        saved_at=last_checkpoint_saved_at,
                    )
                    return LiveDaemonResult(
                        processed,
                        approved,
                        submitted,
                        f"order_reconciliation_failed:{type(error).__name__}",
                        final_state_at,
                    )
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
                await self._save_final_checkpoint(
                    dirty=checkpoint_dirty,
                    saved_at=last_checkpoint_saved_at,
                )
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
                await self._save_final_checkpoint(
                    dirty=checkpoint_dirty,
                    saved_at=last_checkpoint_saved_at,
                )
                return LiveDaemonResult(
                    processed,
                    approved,
                    submitted,
                    "stale_market_state",
                    final_state_at,
                )
            if context.unmanaged_position_symbols:
                await self._save_final_checkpoint(
                    dirty=checkpoint_dirty,
                    saved_at=last_checkpoint_saved_at,
                )
                symbols = ",".join(sorted(context.unmanaged_position_symbols))
                return LiveDaemonResult(
                    processed,
                    approved,
                    submitted,
                    f"unmanaged_live_positions:{symbols}",
                    final_state_at,
                )
            orphan_cancel_reason = await self._cancel_orphan_exit_orders(context)
            if orphan_cancel_reason is not None:
                await self._save_final_checkpoint(
                    dirty=checkpoint_dirty,
                    saved_at=last_checkpoint_saved_at,
                )
                return LiveDaemonResult(
                    processed,
                    approved,
                    submitted,
                    orphan_cancel_reason,
                    final_state_at,
                )
            exit_requests = (
                ()
                if self._exit_manager is None
                else await self._exit_manager.requests_for_state(
                    state,
                    context.managed_positions,
                )
            )
            decision = self._strategy.on_market_state(state)
            processed += 1
            final_state_at = state.bucket_start
            checkpoint_dirty = True
            last_checkpoint_saved_at = context.now
            for request in exit_requests:
                if isinstance(request, LiveExitCancellationRequest):
                    cancel_result = await self._state_machine.cancel_order(
                        request.cancel_plan
                    )
                    if (
                        cancel_result.state
                        is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
                    ):
                        await self._save_final_checkpoint(
                            dirty=checkpoint_dirty,
                            saved_at=last_checkpoint_saved_at,
                        )
                        return LiveDaemonResult(
                            processed,
                            approved,
                            submitted,
                            "unknown_cancel_pending_reconciliation",
                            final_state_at,
                        )
                    if not cancel_result.state.terminal:
                        await self._save_final_checkpoint(
                            dirty=checkpoint_dirty,
                            saved_at=last_checkpoint_saved_at,
                        )
                        return LiveDaemonResult(
                            processed,
                            approved,
                            submitted,
                            "cancel_not_confirmed",
                            final_state_at,
                        )
                    if cancel_result.state is ExchangeOrderState.REJECTED:
                        await self._save_final_checkpoint(
                            dirty=checkpoint_dirty,
                            saved_at=last_checkpoint_saved_at,
                        )
                        return LiveDaemonResult(
                            processed,
                            approved,
                            submitted,
                            "cancel_rejected",
                            final_state_at,
                        )
                    remaining = max(
                        Decimal("0"),
                        request.cancel_plan.quantity
                        - cancel_result.executed_quantity,
                    )
                    if remaining <= 0:
                        continue
                    result = await self._execute_candidate(
                        request.fallback_candidate,
                        requested_quantity=min(request.fallback_quantity, remaining),
                        state=state,
                        context=context,
                    )
                    if result is not None:
                        approved += 1
                        submitted += int(not result.suppressed)
                        if (
                            result.state
                            is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
                        ):
                            await self._save_final_checkpoint(
                                dirty=checkpoint_dirty,
                                saved_at=last_checkpoint_saved_at,
                            )
                            return LiveDaemonResult(
                                processed,
                                approved,
                                submitted,
                                "unknown_order_pending_reconciliation",
                                final_state_at,
                            )
                        if result.state is ExchangeOrderState.REJECTED:
                            await self._save_final_checkpoint(
                                dirty=checkpoint_dirty,
                                saved_at=last_checkpoint_saved_at,
                            )
                            return LiveDaemonResult(
                                processed,
                                approved,
                                submitted,
                                "grace_timeout_market_close_rejected",
                                final_state_at,
                            )
                    continue
                result = await self._execute_candidate(
                    request.candidate,
                    requested_quantity=request.quantity,
                    state=state,
                    context=context,
                )
                if result is not None:
                    approved += 1
                    submitted += int(not result.suppressed)
                    if (
                        result.state
                        is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
                    ):
                        await self._save_final_checkpoint(
                            dirty=checkpoint_dirty,
                            saved_at=last_checkpoint_saved_at,
                        )
                        return LiveDaemonResult(
                            processed,
                            approved,
                            submitted,
                            "unknown_order_pending_reconciliation",
                            final_state_at,
                        )
            for candidate in decision.candidates:
                if (
                    self._config.entry_long_only
                    and not candidate.reduce_only
                    and getattr(candidate.side, "value", candidate.side) != "long"
                ):
                    continue
                result = await self._execute_candidate(
                    candidate,
                    requested_quantity=None,
                    state=state,
                    context=context,
                )
                if result is not None:
                    approved += 1
                    submitted += int(not result.suppressed)
                    if (
                        result.state
                        is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
                    ):
                        await self._save_final_checkpoint(
                            dirty=checkpoint_dirty,
                            saved_at=last_checkpoint_saved_at,
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
                    self._strategy.checkpoint(),
                    context.now,
                )
                checkpoint_dirty = False
                last_checkpoint_saved_at = None
        await self._save_final_checkpoint(
            dirty=checkpoint_dirty,
            saved_at=last_checkpoint_saved_at,
        )
        return LiveDaemonResult(
            processed,
            approved,
            submitted,
            None,
            final_state_at,
        )

    async def _execute_candidate(
        self,
        candidate: OrderIntentCandidate,
        *,
        requested_quantity: Decimal | None,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
    ) -> OrderExecutionResult | None:
        executable_candidate = candidate
        if not candidate.reduce_only:
            if candidate.symbol in (context.open_position_symbols or frozenset()):
                return None
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
                    min_notional=_min_notional(context.trading_rules.get(candidate.symbol)),
                    account_observed_at=context.account_observed_at,
                    market_observed_at=state.bucket_end,
                    has_unresolved_order=any(
                        not item.terminal
                        for item in context.unresolved_order_states
                    ),
                ),
            )
            if not limit_decision.allowed:
                return None
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
                open_position_symbols=context.open_position_symbols or frozenset(),
                active_halts=context.active_halts,
                risk_config=context.risk_config,
                strategy_state=context.strategy_state,
            ),
        )
        if evaluation.decision is not RiskDecision.APPROVED:
            return None
        rules = context.trading_rules.get(candidate.symbol)
        reference_price = state.mark_price or state.close_price
        if rules is None or reference_price is None:
            return None
        plan = quantize_order_plan(
            executable_candidate,
            rules,
            reference_price=reference_price,
            resize_tolerance=self._config.resize_tolerance,
            hedge_mode=self._config.hedge_mode,
            requested_quantity=requested_quantity,
        )
        if isinstance(plan, QuantizationRejection):
            return None
        await self._repository.save_approved_intent(executable_candidate, evaluation)
        return await self._state_machine.execute_approved_intent(plan)

    async def _cancel_orphan_exit_orders(
        self,
        context: LiveDaemonRuntimeContext,
    ) -> str | None:
        open_symbols = context.open_position_symbols or frozenset()
        for item in context.unresolved_orders:
            plan = getattr(item, "plan", None)
            if plan is None or not plan.reduce_only:
                continue
            if plan.order_type != "LIMIT":
                continue
            if plan.symbol in open_symbols:
                continue
            result = await self._state_machine.cancel_order(plan)
            if result.state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION:
                return "unknown_orphan_cancel_pending_reconciliation"
            if not result.state.terminal:
                return "orphan_cancel_not_confirmed"
        return None

    async def _save_final_checkpoint(
        self,
        *,
        dirty: bool,
        saved_at: datetime | None,
    ) -> None:
        if not dirty or saved_at is None:
            return
        await self._repository.save_checkpoint(
            self._config.run_id,
            self._strategy.checkpoint(),
            saved_at,
        )


def _market_age_seconds(now: datetime, state: MarketState15s) -> float:
    return (now - state.bucket_end).total_seconds()


def _min_notional(rules: SymbolTradingRules | None) -> Decimal | None:
    return None if rules is None else rules.min_notional
