from dataclasses import replace
from datetime import UTC, datetime
from typing import cast

from crypto_momentum_lab.domain.execution import ExchangeOrderState
from crypto_momentum_lab.domain.strategy import StrategyDecision
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionResult,
)
from crypto_momentum_lab.live_rollout.context import LiveDaemonRuntimeContext
from crypto_momentum_lab.live_rollout.entry_lane import (
    EntryExecutionLane,
    EntryLaneConfig,
)
from tests.unit.shadow_operation.test_service import _intent, _signal, _state

NOW = datetime(2026, 7, 4, 0, 0, 20, tzinfo=UTC)


def _decision(*candidates) -> StrategyDecision:
    signals = tuple(
        _signal()
        if candidate.signal_id == "signal-1"
        else replace(_signal(), signal_id=candidate.signal_id)
        for candidate in candidates
    )
    return StrategyDecision(
        signals=signals,
        candidates=tuple(candidates),
        rejections=(),
        checkpoint=None,
    )


async def test_entry_lane_executes_admitted_candidates() -> None:
    executed: list[str] = []
    invalidations = 0
    context = cast(LiveDaemonRuntimeContext, object())

    async def execute(
        candidate,
        *,
        requested_quantity,
        state,
        context: LiveDaemonRuntimeContext,
    ) -> OrderExecutionResult:
        del requested_quantity, state, context
        executed.append(candidate.candidate_id)
        return OrderExecutionResult(
            client_order_id=candidate.candidate_id,
            state=ExchangeOrderState.ACKNOWLEDGED,
            exchange_order_id="exchange-1",
        )

    def invalidate() -> None:
        nonlocal invalidations
        invalidations += 1

    lane = EntryExecutionLane(
        config=EntryLaneConfig(run_id="run-1"),
        clock=lambda: NOW,
        entry_enabled=lambda: True,
        entry_enabled_reason=lambda: "ready",
        execute_candidate=execute,
        invalidate_context=invalidate,
    )
    state = _state()
    outcome = await lane.process(
        decision=_decision(_intent()),
        state=state,
        context=context,
        gate_reasons=(),
        recorded_at=NOW,
    )

    assert executed == ["candidate-1"]
    assert invalidations == 1
    assert outcome.approved_intent_count == 1
    assert outcome.submitted_order_count == 1
    assert not outcome.pending_reconciliation


async def test_entry_lane_stops_after_uncertain_submission() -> None:
    executed: list[str] = []
    context = cast(LiveDaemonRuntimeContext, object())

    async def execute(
        candidate,
        *,
        requested_quantity,
        state,
        context: LiveDaemonRuntimeContext,
    ) -> OrderExecutionResult:
        del requested_quantity, state, context
        executed.append(candidate.candidate_id)
        return OrderExecutionResult(
            client_order_id=candidate.candidate_id,
            state=ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
            exchange_order_id=None,
        )

    lane = EntryExecutionLane(
        config=EntryLaneConfig(run_id="run-1"),
        clock=lambda: NOW,
        entry_enabled=lambda: True,
        entry_enabled_reason=lambda: "ready",
        execute_candidate=execute,
        invalidate_context=lambda: None,
    )
    first = _intent()
    second = replace(first, candidate_id="candidate-2", signal_id="signal-2")

    outcome = await lane.process(
        decision=_decision(first, second),
        state=_state(),
        context=context,
        gate_reasons=(),
        recorded_at=NOW,
    )

    assert executed == ["candidate-1"]
    assert outcome.approved_intent_count == 1
    assert outcome.submitted_order_count == 1
    assert outcome.pending_reconciliation
