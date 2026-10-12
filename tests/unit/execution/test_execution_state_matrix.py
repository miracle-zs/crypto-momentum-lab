
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import Mock

import pytest

from crypto_momentum_lab.domain.execution.command_models import (
    CommandExecutionState,
    DispatchState,
    ExecutionScope,
    OutboxEntry,
    to_command_execution_state,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    FactCoverageInterval,
    FactCoverageStatus,
    FactCredibility,
    PositionDiscrepancy,
    PositionHealthStatus,
    PositionKey,
    PositionLedgerBatch,
    PositionState,
    PositionView,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide
from crypto_momentum_lab.live_rollout.entry_control import (
    LiveEntryControlGate,
    TradingGateState,
)

pytestmark = pytest.mark.contract

NOW = datetime(2026, 10, 10, 12, 0, tzinfo=UTC)


def _scope() -> ExecutionScope:
    return ExecutionScope("live", "primary", "BTCUSDT", FuturesPositionSide.BOTH)


def _outbox(state: DispatchState) -> OutboxEntry:
    scope = _scope()
    cmd = TradeCommand(
        command_id="cmd-1",
        position_key=scope.to_position_key(),
        command_type=TradeCommandType.ENTRY,
        side=StrategySide.LONG,
        order_type=EntryType.LIMIT,
        requested_quantity=Decimal("1"),
        limit_price=Decimal("100"),
        created_at=NOW,
    )
    return OutboxEntry(
        command_id="cmd-1",
        request_id="req-1",
        scope=scope,
        command=cmd,
        state=state,
        created_at=NOW,
        updated_at=NOW,
    )


def _position_view(
    quantity: Decimal = Decimal("0"),
    health: PositionHealthStatus = PositionHealthStatus.READY,
    is_comparable: bool = True,
    gap: Decimal | None = None,
    discrepancy: PositionDiscrepancy | None = None,
    zero_confirmed: bool = False,
    coverage: FactCoverageInterval | None = None,
) -> PositionView:
    batches = (
        (
            PositionLedgerBatch(
                batch_id="b1",
                episode_id="ep1",
                quantity=quantity,
                original_quantity=max(quantity, Decimal("1")),
                entry_price=Decimal("100"),
                opened_at=NOW,
            ),
        )
        if quantity > 0
        else ()
    )
    return PositionView(
        key=PositionKey("live", "primary", "BTCUSDT", FuturesPositionSide.BOTH),
        projection_version="v1",
        input_revision=1,
        event_cut=NOW,
        policy_version="p1",
        schema_version="s1",
        coverage=coverage,
        active_episode=None,
        batches=batches,
        unallocated_quantity=Decimal("0"),
        health_status=health,
        is_comparable=is_comparable,
        reconciliation_gap=gap,
        discrepancy=discrepancy,
        zero_position_snapshot_confirmed=zero_confirmed,
    )


def test_g1_command_state_mapping() -> None:
    # 5 canonical command states
    assert (
        to_command_execution_state(DispatchState.PREPARED)
        == CommandExecutionState.PREPARED
    )
    assert (
        to_command_execution_state(DispatchState.DISPATCHING)
        == CommandExecutionState.SENDING
    )
    assert (
        to_command_execution_state(DispatchState.ACKNOWLEDGED)
        == CommandExecutionState.WORKING
    )
    assert (
        to_command_execution_state(DispatchState.UNKNOWN)
        == CommandExecutionState.UNKNOWN
    )
    assert (
        to_command_execution_state(DispatchState.TERMINAL)
        == CommandExecutionState.TERMINAL
    )
    assert (
        to_command_execution_state(DispatchState.REJECTED)
        == CommandExecutionState.TERMINAL
    )

    # Wire order strings mapping
    assert (
        to_command_execution_state("submitting") == CommandExecutionState.SENDING
    )
    assert (
        to_command_execution_state("partially_filled")
        == CommandExecutionState.WORKING
    )
    assert to_command_execution_state("canceled") == CommandExecutionState.TERMINAL
    assert to_command_execution_state("filled") == CommandExecutionState.TERMINAL

    # OutboxEntry.execution_state property
    entry = _outbox(DispatchState.DISPATCHING)
    assert entry.execution_state == CommandExecutionState.SENDING


def test_g1_position_and_credibility_states() -> None:
    # Position state: FLAT vs OPEN
    flat_view = _position_view(quantity=Decimal("0"))
    assert flat_view.position_state == PositionState.FLAT

    open_view = _position_view(quantity=Decimal("2.5"))
    assert open_view.position_state == PositionState.OPEN

    # Fact credibility: coverage=None must be PENDING (never blindly READY)
    assert flat_view.fact_credibility == FactCredibility.PENDING

    # Healthy view with confirmed authoritative coverage becomes READY
    confirmed_coverage = FactCoverageInterval(
        start_at=NOW,
        end_at=NOW,
        status=FactCoverageStatus.CONFIRMED,
    )
    ready_view = _position_view(
        quantity=Decimal("0"),
        coverage=confirmed_coverage,
        zero_confirmed=True,
    )
    assert ready_view.fact_credibility == FactCredibility.READY

    # Non-authoritative coverage (with known gaps) must remain PENDING
    non_auth_coverage = FactCoverageInterval(
        start_at=NOW,
        end_at=NOW,
        status=FactCoverageStatus.CONFIRMED,
        has_known_gaps=True,
    )
    non_auth_view = _position_view(
        quantity=Decimal("0"),
        coverage=non_auth_coverage,
        zero_confirmed=False,
    )
    assert non_auth_view.fact_credibility == FactCredibility.PENDING

    # Stream scope mismatch must remain PENDING
    key = PositionKey("live", "primary", "BTCUSDT", FuturesPositionSide.BOTH)
    scope1 = AccountFactStreamScope.for_position_key(
        key, stream_id="s1", stream_epoch="e1"
    )
    scope2 = AccountFactStreamScope.for_position_key(
        key, stream_id="s2", stream_epoch="e2"
    )
    mismatched_coverage = FactCoverageInterval(
        start_at=NOW,
        end_at=NOW,
        status=FactCoverageStatus.CONFIRMED,
        stream_scope=scope2,
    )
    stream_mismatch_view = replace(
        _position_view(
            quantity=Decimal("0"),
            coverage=mismatched_coverage,
            zero_confirmed=False,
        ),
        stream_scope=scope1,
    )
    assert stream_mismatch_view.fact_credibility == FactCredibility.PENDING

    catching_up_view = _position_view(
        health=PositionHealthStatus.CATCHING_UP,
        coverage=confirmed_coverage,
    )
    assert catching_up_view.fact_credibility == FactCredibility.PENDING

    gap_view = _position_view(
        gap=Decimal("0.5"),
        coverage=confirmed_coverage,
    )
    assert gap_view.fact_credibility == FactCredibility.PENDING

    conflict_view = _position_view(
        health=PositionHealthStatus.CONFLICT,
        coverage=confirmed_coverage,
    )
    assert conflict_view.fact_credibility == FactCredibility.CONFLICT


def test_g1_trading_gate_states() -> None:
    exit_enabled = True
    gate = LiveEntryControlGate(
        run_id="run-1",
        state_machine=Mock(),
        clock=lambda: NOW,
        is_exit_enabled=lambda: exit_enabled,
    )
    # Default active
    assert gate.trading_gate_state == TradingGateState.ACTIVE

    # Entry blocked by risk/window -> EXIT_ONLY
    gate.set_entry_enabled(False, reason="scheduled_risk_window")
    assert gate.trading_gate_state == TradingGateState.EXIT_ONLY

    # Halted by session
    gate.set_entry_enabled(False, reason="session_halted")
    assert gate.trading_gate_state == TradingGateState.HALTED

    # Exit disabled -> HALTED
    gate.set_entry_enabled(True, reason="cleared")
    exit_enabled = False
    assert gate.trading_gate_state == TradingGateState.HALTED
    assert not gate.entry_enabled
    assert gate.entry_enabled_reason == "exit_disabled"


def test_g1_composite_states_permitted() -> None:
    # 1. OPEN position while having resting/active entry commands
    open_view = _position_view(quantity=Decimal("5"))
    assert open_view.position_state == PositionState.OPEN
    working_cmd = _outbox(DispatchState.ACKNOWLEDGED)
    assert working_cmd.execution_state == CommandExecutionState.WORKING

    # 2. TERMINAL command while position fact credibility is PENDING
    terminal_cmd = _outbox(DispatchState.TERMINAL)
    assert terminal_cmd.execution_state == CommandExecutionState.TERMINAL
    pending_fact_view = _position_view(health=PositionHealthStatus.CATCHING_UP)
    assert pending_fact_view.fact_credibility == FactCredibility.PENDING

    # 3. FLAT does not equal zero_position_snapshot_confirmed
    flat_unconfirmed = _position_view(quantity=Decimal("0"), zero_confirmed=False)
    assert flat_unconfirmed.position_state == PositionState.FLAT
    assert flat_unconfirmed.zero_position_snapshot_confirmed is False
