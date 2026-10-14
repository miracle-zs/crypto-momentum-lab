
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.execution.command_models import (
    DispatchState,
    ExecutionScope,
    OutboxEntry,
)
from crypto_momentum_lab.domain.execution.evidence_lifecycle import plan_order_event
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderState,
    FuturesPositionSide,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide

NOW = datetime(2026, 9, 30, tzinfo=UTC)
OBSERVED = NOW + timedelta(seconds=1)


@pytest.fixture
def entry():
    scope = ExecutionScope("live", "account-3", "TESTUSDT", FuturesPositionSide.LONG)
    command = TradeCommand(
        "command-1",
        scope.to_position_key(),
        TradeCommandType.EXIT,
        StrategySide.LONG,
        EntryType.MARKET,
        Decimal("5"),
        created_at=NOW,
    )
    return OutboxEntry(
        command.command_id,
        "request",
        scope,
        command,
        last_error="prior",
        created_at=NOW,
        updated_at=NOW,
    )


def event(state):
    return ExchangeOrderEvent(
        event_id="event",
        client_order_id="command-1",
        state=state,
        occurred_at=NOW,
        exchange_order_id=None,
        details={},
    )


def plan(entry, state):
    return plan_order_event(
        entry,
        event(state),
        observed_at=OBSERVED,
    )


@pytest.mark.parametrize(
    "dispatch",
    [DispatchState.PREPARED, DispatchState.DISPATCHING, DispatchState.UNKNOWN],
)
@pytest.mark.parametrize(
    "state", [ExchangeOrderState.ACKNOWLEDGED, ExchangeOrderState.SUBMITTED]
)
def test_acknowledgement_advances_eligible_dispatch_without_release(
    entry, dispatch, state
):
    original = replace(entry, state=dispatch)
    result = plan(original, state)
    assert result.updated.state is DispatchState.ACKNOWLEDGED
    assert result.updated.updated_at == OBSERVED
    assert result.updated.last_error == original.last_error
    assert result.release_reason is None and not result.dispatch_reconciled
    assert original.state is dispatch and original.updated_at == NOW


@pytest.mark.parametrize(
    "dispatch",
    [DispatchState.PREPARED, DispatchState.ACKNOWLEDGED, DispatchState.UNKNOWN],
)
def test_unknown_requires_reconciliation_and_keeps_capacity(entry, dispatch):
    result = plan(
        replace(entry, state=dispatch),
        ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
    )
    assert result.updated.state is DispatchState.UNKNOWN
    assert result.updated.last_error == "Pending reconciliation"
    assert result.requires_dispatch_reconciliation
    assert not result.dispatch_reconciled and result.release_reason is None


@pytest.mark.parametrize(
    "state",
    [
        ExchangeOrderState.CANCELED,
        ExchangeOrderState.EXPIRED,
        ExchangeOrderState.REJECTED,
        ExchangeOrderState.ABSENT_RECONCILED,
    ],
)
def test_nonfilled_terminal_releases_capacity_with_original_reason(entry, state):
    result = plan(entry, state)
    expected = (
        DispatchState.REJECTED
        if state == ExchangeOrderState.REJECTED
        else DispatchState.TERMINAL
    )
    assert result.updated.state is expected
    assert result.updated.last_error == f"Order {state.value}"
    assert result.release_reason == f"order_finished_{state.value.lower()}"
    assert result.dispatch_reconciled


def test_filled_terminal_releases_unused_reservation_capacity(entry):
    result = plan(entry, ExchangeOrderState.FILLED)
    assert result.updated.state is DispatchState.TERMINAL
    assert result.updated.last_error == "prior"
    assert result.release_reason == "order_finished_filled"
    assert result.dispatch_reconciled


@pytest.mark.parametrize("dispatch", [DispatchState.TERMINAL, DispatchState.REJECTED])
@pytest.mark.parametrize(
    "state",
    [
        ExchangeOrderState.ACKNOWLEDGED,
        ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
        ExchangeOrderState.CANCELED,
    ],
)
def test_terminal_commands_ignore_late_events_without_second_release(
    entry, dispatch, state
):
    result = plan(replace(entry, state=dispatch), state)
    assert result.updated is None and result.release_reason is None
    assert (
        not result.requires_dispatch_reconciliation and not result.dispatch_reconciled
    )


def test_missing_outbox_has_no_lifecycle_effect():
    result = plan(None, ExchangeOrderState.FILLED)
    assert result.updated is None and result.release_reason is None


def test_partial_fill_event_does_not_manufacture_command_ack_or_terminal(entry):
    result = plan(entry, ExchangeOrderState.PARTIALLY_FILLED)
    assert result.updated is None and result.release_reason is None
