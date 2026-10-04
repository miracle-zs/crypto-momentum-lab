from dataclasses import replace
from datetime import UTC, datetime

from crypto_momentum_lab.domain.execution.order_state import ExchangeOrderState
from crypto_momentum_lab.domain.live_rollout import (
    LiveSessionState,
    LiveSessionTransition,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionStateMachine,
)
from crypto_momentum_lab.live_rollout.gates import evaluate_live_gate
from crypto_momentum_lab.live_rollout.session import (
    LiveRolloutSession,
    LiveSessionConfig,
    LiveSessionLifecycle,
)
from tests.unit.execution_account.orders.test_state_machine import (
    FakeExchange,
    FakeOrderRepository,
    _plan,
    _snapshot,
)
from tests.unit.live_rollout.test_gates import _context

NOW = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)


async def test_session_lifecycle_persists_shared_transition_contract() -> None:
    repository = FakeTransitionRepository()
    lifecycle = LiveSessionLifecycle(
        repository=repository,
        config=LiveSessionConfig(
            session_id="live-1",
            operator="operator",
            strategy_config_hash="a" * 64,
            risk_config_hash="b" * 64,
        ),
        clock=lambda: NOW,
    )

    transition = await lifecycle.transition(
        LiveSessionState.PREFLIGHT,
        reason="startup",
    )

    assert transition.state is LiveSessionState.PREFLIGHT
    assert transition.reason == "startup"
    assert transition.session_id == "live-1"
    assert lifecycle.state is LiveSessionState.PREFLIGHT
    assert repository.items == [transition]


async def test_session_executes_approved_plan_with_single_live_transition() -> None:
    session, transitions, exchange = _session()

    result = await session.run_one(
        gate=evaluate_live_gate(_context()),
        plan=_plan(),
    )

    assert [item.state for item in transitions.items] == [
        LiveSessionState.LIVE_ENABLED,
    ]
    assert result.state is LiveSessionState.LIVE_ENABLED
    assert exchange.calls == ["submit"]


async def test_session_submits_only_after_gate_approval() -> None:
    session, _, exchange = _session()
    blocked_context = replace(_context(), live_submit_enabled=False)

    result = await session.run_one(
        gate=evaluate_live_gate(blocked_context),
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
    exchange = FakeExchange(submit_result=_snapshot(ExchangeOrderState.ACKNOWLEDGED))
    transitions = FakeTransitionRepository()
    machine = OrderExecutionStateMachine(
        exchange=exchange,
        repository=FakeOrderRepository(),
        event_repository=FakeOrderRepository(),
        live_submit_enabled=True,
        clock=lambda: NOW,
    )
    return (
        LiveRolloutSession(
            repository=transitions,
            execute_plan=machine.submit,
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
