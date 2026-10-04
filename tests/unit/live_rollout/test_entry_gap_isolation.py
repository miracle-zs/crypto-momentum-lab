"""Entry processing remains local when another symbol has a position gap."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest

from crypto_momentum_lab.domain.execution.order_state import ExchangeOrderState
from crypto_momentum_lab.domain.strategy import StrategyDecision
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionResult,
)
from crypto_momentum_lab.live_rollout.context import LiveDaemonRuntimeContext
from crypto_momentum_lab.live_rollout.entry_lane import (
    EntryExecutionLane,
    EntryLaneConfig,
)
from tests.fixtures.live_market import _intent, _signal, _state

NOW = datetime(2026, 9, 29, 14, 6, 45, tzinfo=UTC)


def _decision_for(symbol: str) -> StrategyDecision:
    candidate = replace(
        _intent(),
        candidate_id=f"cand-{symbol}",
        signal_id=f"sig-{symbol}",
        symbol=symbol,
        created_at=NOW,
        expires_at=NOW + timedelta(minutes=5),
    )
    signal = replace(
        _signal(),
        signal_id=f"sig-{symbol}",
        symbol=symbol,
    )
    return StrategyDecision(
        signals=(signal,),
        candidates=(candidate,),
        rejections=(),
        checkpoint=None,
    )


@pytest.mark.asyncio
async def test_symbol_level_isolation_allows_healthy_symbol_entry_when_other_symbol_has_gap() -> (
    None
):
    """A reconciliation gap or stalled state on symbol A (e.g. GRASSUSDT) must NOT leak

    and block entry execution for an eligible, healthy symbol B (e.g. ESPORTSUSDT).
    """
    executed: list[str] = []

    class MockContext:
        # Symbol A has an unmanaged position gap
        unmanaged_position_symbols = frozenset({"GRASSUSDT"})
        managed_position_symbols = frozenset({"GRASSUSDT"})
        managed_positions = ()
        unresolved_orders = ()
        pending_position_symbols = frozenset()

    async def mock_execute(candidate, **kwargs):
        executed.append(candidate.symbol)
        return OrderExecutionResult(
            client_order_id=candidate.candidate_id,
            state=ExchangeOrderState.ACKNOWLEDGED,
            exchange_order_id="ex-1",
        )

    lane = EntryExecutionLane(
        config=EntryLaneConfig(
            run_id="run-live-1",
        ),
        clock=lambda: NOW,
        entry_enabled=lambda: True,
        entry_enabled_reason=lambda: "ready",
        execute_candidate=mock_execute,
        invalidate_context=lambda: None,
    )

    state = replace(_state(), symbol="ESPORTSUSDT")

    # Process ESPORTSUSDT decision
    outcome = await lane.process(
        decision=_decision_for("ESPORTSUSDT"),
        state=state,
        context=cast(LiveDaemonRuntimeContext, MockContext()),
        gate_reasons=(),
        recorded_at=NOW,
    )

    # Invariant: ESPORTSUSDT must execute despite GRASSUSDT having an unmanaged gap
    assert "ESPORTSUSDT" in executed
    assert outcome.submitted_order_count == 1
