from dataclasses import replace
from datetime import UTC, datetime

from crypto_momentum_lab.domain.execution import ExchangeOrderState
from crypto_momentum_lab.domain.live_rollout import (
    LiveSessionState,
    LiveSessionTransition,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionStateMachine,
    SubmitPolicy,
)
from crypto_momentum_lab.live_rollout.session import (
    LiveRolloutSession,
    LiveSessionConfig,
)
from tests.unit.execution_account.orders.test_state_machine import (
    FakeExchange,
    FakeOrderRepository,
    _plan,
    _snapshot,
)
from tests.unit.live_rollout.test_gates import _context

NOW = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)


async def test_session_preflight_runs_shadow_before_live() -> None:
    calls: list[str] = []

    async def shadow_preflight() -> bool:
        calls.append("shadow")
        return True

    session, transitions, exchange = _session()

    result = await session.run_one(
        gate_context=_context(),
        shadow_preflight=shadow_preflight,
        plan=_plan(),
    )

    assert calls == ["shadow"]
    assert [item.state for item in transitions.items[:3]] == [
        LiveSessionState.PREFLIGHT,
        LiveSessionState.SHADOW_PREFLIGHT,
        LiveSessionState.LIVE_ENABLED,
    ]
    assert result.state is LiveSessionState.LIVE_ENABLED
    assert exchange.calls == ["submit"]


async def test_session_submits_only_after_gate_approval() -> None:
    async def shadow_preflight() -> bool:
        return True

    session, _, exchange = _session()
    blocked_context = replace(_context(), approval=None)

    result = await session.run_one(
        gate_context=blocked_context,
        shadow_preflight=shadow_preflight,
        plan=_plan(),
    )

    assert result.state is LiveSessionState.HALTED
    assert exchange.calls == []


class FakeTransitionRepository:
    def __init__(self) -> None:
        self.items: list[LiveSessionTransition] = []

    async def save_transition(self, transition: LiveSessionTransition) -> None:
        self.items.append(transition)


def _session() -> tuple[
    LiveRolloutSession,
    FakeTransitionRepository,
    FakeExchange,
]:
    exchange = FakeExchange(
        submit_result=_snapshot(ExchangeOrderState.ACKNOWLEDGED)
    )
    transitions = FakeTransitionRepository()
    machine = OrderExecutionStateMachine(
        exchange=exchange,
        repository=FakeOrderRepository(),
        submit_policy=SubmitPolicy.LIVE_SUBMIT,
        live_submit_enabled=True,
        clock=lambda: NOW,
    )
    return (
        LiveRolloutSession(
            repository=transitions,
            state_machine=machine,
            config=LiveSessionConfig(
                session_id="live-1",
                operator="operator",
                strategy_config_hash="a" * 64,
                risk_config_hash="b" * 64,
            ),
            clock=lambda: NOW,
        ),
        transitions,
        exchange,
    )
