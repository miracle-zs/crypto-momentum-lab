from dataclasses import replace
from datetime import UTC, datetime
from typing import cast

from crypto_momentum_lab.domain.execution.order_result import OrderExecutionResult
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
)
from crypto_momentum_lab.domain.strategy import StrategyDecision
from crypto_momentum_lab.live_rollout.context import LiveDaemonRuntimeContext
from crypto_momentum_lab.live_rollout.entry_control import TradingGateState
from crypto_momentum_lab.live_rollout.entry_lane import (
    EntryExecutionLane,
    EntryLaneConfig,
)
from tests.fixtures.live_market import _intent, _signal, _state

NOW = datetime(2026, 7, 4, 0, 0, 20, tzinfo=UTC)


def _symbol_entry_allowed(_symbol: str) -> tuple[bool, str]:
    return True, "entry_allowed"


def _decision(*candidates) -> StrategyDecision:
    signals = tuple(
        replace(
            _signal(),
            signal_id=candidate.signal_id,
            symbol=candidate.symbol,
        )
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
        entry_enabled_reason=lambda: "ready",
        execute_candidate=execute,
        invalidate_context=invalidate,
        trading_gate_state=lambda: TradingGateState.ACTIVE,
        is_symbol_entry_allowed=_symbol_entry_allowed,
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
        entry_enabled_reason=lambda: "ready",
        execute_candidate=execute,
        invalidate_context=lambda: None,
        trading_gate_state=lambda: TradingGateState.ACTIVE,
        is_symbol_entry_allowed=_symbol_entry_allowed,
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


async def test_entry_lane_blocks_when_trading_gate_not_active() -> None:
    executed: list[str] = []
    context = cast(LiveDaemonRuntimeContext, object())

    async def execute(candidate, *, requested_quantity, state, context):
        del requested_quantity, state, context
        executed.append(candidate.candidate_id)
        return OrderExecutionResult(
            client_order_id=candidate.candidate_id,
            state=ExchangeOrderState.ACKNOWLEDGED,
            exchange_order_id="ex-1",
        )

    current_gate = TradingGateState.EXIT_ONLY
    lane = EntryExecutionLane(
        config=EntryLaneConfig(run_id="run-1"),
        clock=lambda: NOW,
        entry_enabled_reason=lambda: "exit_only",
        execute_candidate=execute,
        invalidate_context=lambda: None,
        trading_gate_state=lambda: current_gate,
        is_symbol_entry_allowed=_symbol_entry_allowed,
    )

    outcome = await lane.process(
        decision=_decision(_intent()),
        state=_state(),
        context=context,
        gate_reasons=(),
        recorded_at=NOW,
    )
    assert executed == []
    assert outcome.approved_intent_count == 0
    assert outcome.submitted_order_count == 0


async def test_entry_lane_rechecks_gate_after_entry_pool_await() -> None:
    executed: list[str] = []
    context = cast(LiveDaemonRuntimeContext, object())
    current_gate = [TradingGateState.ACTIVE]

    async def load_entry_symbols(_at: datetime) -> frozenset[str]:
        current_gate[0] = TradingGateState.EXIT_ONLY
        return frozenset({"BTCUSDT"})

    async def execute(candidate, *, requested_quantity, state, context):
        del requested_quantity, state, context
        executed.append(candidate.candidate_id)
        return OrderExecutionResult(
            client_order_id=candidate.candidate_id,
            state=ExchangeOrderState.ACKNOWLEDGED,
            exchange_order_id="ex-1",
        )

    def is_symbol_allowed(_symbol: str) -> tuple[bool, str]:
        if current_gate[0] is not TradingGateState.ACTIVE:
            return False, "cutover_entry_stop"
        return True, "entry_allowed"

    lane = EntryExecutionLane(
        config=EntryLaneConfig(
            run_id="run-1",
            entry_symbol_loader=load_entry_symbols,
        ),
        clock=lambda: NOW,
        entry_enabled_reason=lambda: "cutover_entry_stop",
        execute_candidate=execute,
        invalidate_context=lambda: None,
        trading_gate_state=lambda: current_gate[0],
        is_symbol_entry_allowed=is_symbol_allowed,
    )

    outcome = await lane.process(
        decision=_decision(_intent()),
        state=_state(),
        context=context,
        gate_reasons=(),
        recorded_at=NOW,
    )

    assert executed == []
    assert outcome.approved_intent_count == 0
    assert outcome.submitted_order_count == 0


async def test_entry_lane_rejects_candidate_when_symbol_not_allowed() -> None:
    executed: list[str] = []
    context = cast(LiveDaemonRuntimeContext, object())

    async def execute(candidate, *, requested_quantity, state, context):
        del requested_quantity, state, context
        executed.append(candidate.candidate_id)
        return OrderExecutionResult(
            client_order_id=candidate.candidate_id,
            state=ExchangeOrderState.ACKNOWLEDGED,
            exchange_order_id="ex-1",
        )

    def is_symbol_allowed(symbol: str) -> tuple[bool, str]:
        if symbol == "BTCUSDT":
            return False, "symbol_not_prewarmed:BTCUSDT"
        return True, "entry_allowed"

    lane = EntryExecutionLane(
        config=EntryLaneConfig(run_id="run-1"),
        clock=lambda: NOW,
        entry_enabled_reason=lambda: "ready",
        execute_candidate=execute,
        invalidate_context=lambda: None,
        trading_gate_state=lambda: TradingGateState.ACTIVE,
        is_symbol_entry_allowed=is_symbol_allowed,
    )

    first = replace(_intent(), symbol="BTCUSDT")
    second = replace(
        _intent(),
        candidate_id="candidate-2",
        signal_id="signal-2",
        symbol="ETHUSDT",
    )
    outcome = await lane.process(
        decision=_decision(first, second),
        state=_state(),
        context=context,
        gate_reasons=(),
        recorded_at=NOW,
    )
    assert executed == ["candidate-2"]
    assert outcome.approved_intent_count == 1
    assert outcome.submitted_order_count == 1


async def test_entry_lane_rejects_candidate_when_symbol_facts_pending() -> None:
    from tests.unit.live_rollout.test_daemon import _runtime_context

    executed: list[str] = []
    context = replace(
        _runtime_context(),
        pending_position_symbols=frozenset({"BTCUSDT"}),
    )

    async def execute(
        candidate, *, requested_quantity, state, context
    ) -> OrderExecutionResult:
        del requested_quantity, state, context
        executed.append(candidate.candidate_id)
        return OrderExecutionResult(
            client_order_id=candidate.candidate_id,
            state=ExchangeOrderState.ACKNOWLEDGED,
            exchange_order_id="ex-1",
        )

    lane = EntryExecutionLane(
        config=EntryLaneConfig(run_id="run-1"),
        clock=lambda: NOW,
        entry_enabled_reason=lambda: "ready",
        execute_candidate=execute,
        invalidate_context=lambda: None,
        trading_gate_state=lambda: TradingGateState.ACTIVE,
        is_symbol_entry_allowed=_symbol_entry_allowed,
    )

    first = replace(_intent(), symbol="BTCUSDT")
    second = replace(
        _intent(),
        candidate_id="candidate-2",
        signal_id="signal-2",
        symbol="ETHUSDT",
    )
    outcome = await lane.process(
        decision=_decision(first, second),
        state=_state(),
        context=context,
        gate_reasons=(),
        recorded_at=NOW,
    )
    # BTCUSDT is rejected due to pending facts; ETHUSDT is executed!
    assert executed == ["candidate-2"]
    assert outcome.approved_intent_count == 1
    assert outcome.submitted_order_count == 1
