from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.domain.execution.command_models import (
    DispatchState,
    ExecutionScope,
    OutboxEntry,
)
from crypto_momentum_lab.domain.execution.evidence_lifecycle import plan_order_event
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderState,
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
    scope = ExecutionScope("live", "account-3", "TESTUSDT", "LONG")
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


def fill(quantity, *, order_id="command-1"):
    return AccountFillEvent(
        environment="live",
        account_label="account-3",
        symbol="TESTUSDT",
        trade_id="trade",
        order_id=order_id,
        side="SELL",
        quantity=Decimal(quantity),
        price=Decimal("12"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=NOW,
        raw_payload={},
    )


def plan(entry, state, *, durable=True, fills=()):
    return plan_order_event(
        entry,
        event(state),
        observed_at=OBSERVED,
        account_fills=fills,
        durable=durable,
        has_active_reservations=True,
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
    assert result.dispatch_reconciled and result.recovery_diagnostic is None


@pytest.mark.parametrize(
    "durable,quantity,release,recovery",
    [
        (True, "4", None, True),
        (False, "4", "order_finished_filled", False),
        (True, "5", "order_filled_with_confirmed_trades", False),
    ],
)
def test_filled_terminal_requires_real_trades_before_durable_release(
    entry, durable, quantity, release, recovery
):
    result = plan(
        entry, ExchangeOrderState.FILLED, durable=durable, fills=(fill(quantity),)
    )
    assert result.updated.state is DispatchState.TERMINAL
    assert result.updated.last_error == "prior"
    assert result.release_reason == release
    assert bool(result.recovery_diagnostic) is recovery
    assert result.dispatch_reconciled


def test_other_order_fills_cannot_release_current_command_reservation(entry):
    result = plan(
        entry, ExchangeOrderState.FILLED, fills=(fill("100", order_id="other"),)
    )
    assert result.release_reason is None
    assert result.recovery_diagnostic is not None


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
    assert result.recovery_diagnostic is None


def test_partial_fill_event_does_not_manufacture_command_ack_or_terminal(entry):
    result = plan(entry, ExchangeOrderState.PARTIALLY_FILLED)
    assert result.updated is None and result.release_reason is None
