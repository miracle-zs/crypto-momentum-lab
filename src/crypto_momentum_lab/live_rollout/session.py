from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import NAMESPACE_URL, uuid5

from crypto_momentum_lab.domain.execution import OrderExecutionPlan
from crypto_momentum_lab.domain.live_rollout import (
    LiveGateDecision,
    LiveGateStatus,
    LiveSessionState,
    LiveSessionTransition,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionResult,
)
from crypto_momentum_lab.live_rollout.gates import LiveGateContext, evaluate_live_gate


class LiveTransitionRepository(Protocol):
    async def save_transition(self, transition: LiveSessionTransition) -> None:
        pass


class LivePlanExecutor(Protocol):
    async def __call__(self, plan: OrderExecutionPlan) -> OrderExecutionResult: ...


@dataclass(frozen=True, slots=True)
class LiveSessionConfig:
    session_id: str
    operator: str
    strategy_config_hash: str
    risk_config_hash: str


@dataclass(frozen=True, slots=True)
class LiveSessionResult:
    gate: LiveGateDecision
    state: LiveSessionState
    order_result: OrderExecutionResult | None


class LiveSessionLifecycle:
    """Persist the shared live-session transition contract.

    Long-running daemons and one-shot plans use the same transition shape.
    Keeping transition construction here prevents the composition root from
    growing a second, subtly different session-state implementation.
    """

    def __init__(
        self,
        *,
        repository: LiveTransitionRepository,
        config: LiveSessionConfig,
        clock: Callable[[], datetime],
    ) -> None:
        self._repository = repository
        self._config = config
        self._clock = clock
        self._state: LiveSessionState | None = None

    @property
    def state(self) -> LiveSessionState | None:
        return self._state

    async def transition(
        self,
        state: LiveSessionState,
        reason: str | None = None,
    ) -> LiveSessionTransition:
        occurred_at = self._clock()
        transition = LiveSessionTransition(
            transition_id=str(
                uuid5(
                    NAMESPACE_URL,
                    f"live-transition:{self._config.session_id}:"
                    f"{state.value}:{occurred_at.isoformat()}",
                )
            ),
            session_id=self._config.session_id,
            state=state,
            occurred_at=occurred_at,
            operator=self._config.operator,
            strategy_config_hash=self._config.strategy_config_hash,
            risk_config_hash=self._config.risk_config_hash,
            reason=reason,
            details={},
        )
        await self._repository.save_transition(transition)
        self._state = state
        return transition


class LiveRolloutSession:
    def __init__(
        self,
        *,
        repository: LiveTransitionRepository,
        execute_plan: LivePlanExecutor,
        config: LiveSessionConfig,
        clock: Callable[[], datetime],
    ) -> None:
        self._repository = repository
        self._execute_plan = execute_plan
        self._config = config
        self._clock = clock
        self._lifecycle = LiveSessionLifecycle(
            repository=repository,
            config=config,
            clock=clock,
        )

    async def run_one(
        self,
        *,
        gate_context: LiveGateContext,
        shadow_preflight: Callable[[], Awaitable[bool]],
        plan: OrderExecutionPlan,
    ) -> LiveSessionResult:
        await self._lifecycle.transition(LiveSessionState.PREFLIGHT)
        await self._lifecycle.transition(LiveSessionState.SHADOW_PREFLIGHT)
        if not await shadow_preflight():
            gate = LiveGateDecision(
                status=LiveGateStatus.BLOCKED,
                reasons=("shadow_preflight_failed",),
            )
            await self._lifecycle.transition(
                LiveSessionState.HALTED,
                "shadow_preflight_failed",
            )
            return LiveSessionResult(gate, LiveSessionState.HALTED, None)
        gate = evaluate_live_gate(gate_context)
        if not gate.approved:
            await self._lifecycle.transition(
                LiveSessionState.HALTED,
                ",".join(gate.reasons),
            )
            return LiveSessionResult(gate, LiveSessionState.HALTED, None)
        await self._lifecycle.transition(LiveSessionState.LIVE_ENABLED)
        order_result = await self._execute_plan(plan)
        return LiveSessionResult(gate, LiveSessionState.LIVE_ENABLED, order_result)


__all__ = [
    "LivePlanExecutor",
    "LiveRolloutSession",
    "LiveSessionConfig",
    "LiveSessionLifecycle",
    "LiveSessionResult",
    "LiveTransitionRepository",
]
