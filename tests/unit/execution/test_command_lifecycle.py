"""Command lifecycle and capacity accounting through their pure interface."""

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.execution.command_lifecycle import (
    plan_command_transition,
    plan_reservation_release,
    plan_reservation_settlement,
)
from crypto_momentum_lab.domain.execution.command_models import (
    DispatchState,
    ExecutionScope,
    OutboxEntry,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    PositionReservation,
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide

NOW = datetime(2026, 9, 30, tzinfo=UTC)


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


def reservation(entry, identity="r1", reserved="5", consumed="1", released="1"):
    return PositionReservation(
        identity,
        entry.command_id,
        entry.scope.to_position_key(),
        "batch-" + identity,
        Decimal(reserved),
        Decimal(consumed),
        Decimal(released),
        NOW,
    )


def test_dispatch_attempt_is_incremented_once_and_cannot_be_repeated(entry):
    plan = plan_command_transition(entry, DispatchState.DISPATCHING, at=NOW)
    assert plan.updated.attempt_count == 1
    assert entry.attempt_count == 0
    with pytest.raises(ValueError, match="Cannot dispatch"):
        plan_command_transition(plan.updated, DispatchState.DISPATCHING, at=NOW)


@pytest.mark.parametrize(
    "state",
    [DispatchState.DISPATCHING, DispatchState.ACKNOWLEDGED, DispatchState.UNKNOWN],
)
def test_unknown_seals_submission_without_releasing_capacity(entry, state):
    plan = plan_command_transition(
        replace(entry, state=state), DispatchState.UNKNOWN, at=NOW, reason="timeout"
    )
    assert plan.requires_reconciliation
    assert plan.release_reason is None
    assert plan.updated.last_error == "timeout"


@pytest.mark.parametrize("state", [DispatchState.TERMINAL, DispatchState.REJECTED])
def test_terminal_commands_are_not_reopened_by_unknown(entry, state):
    terminal = replace(entry, state=state)
    plan = plan_command_transition(
        terminal, DispatchState.UNKNOWN, at=NOW, reason="timeout"
    )
    assert plan.updated is terminal
    assert not plan.requires_reconciliation and plan.release_reason is None


def test_terminal_reason_and_acknowledgement_preserve_existing_metadata_rules(entry):
    ack = plan_command_transition(
        entry, DispatchState.ACKNOWLEDGED, at=NOW, external_order_id="exchange-1"
    )
    assert ack.updated.external_order_id == "exchange-1"
    assert ack.updated.last_error == "prior"
    terminal = plan_command_transition(ack.updated, DispatchState.TERMINAL, at=NOW)
    assert terminal.updated.last_error == "prior"
    assert terminal.release_reason == "command_terminal"
    rejected = plan_command_transition(
        entry, DispatchState.REJECTED, at=NOW, reason="reject"
    )
    assert rejected.updated.last_error == "reject"
    assert rejected.release_reason == "command_rejected"


@pytest.mark.parametrize(
    "quantity,consumed,recovery",
    [("0", "0", False), ("2", "2", False), ("4", "4", False), ("7", "6", True)],
)
def test_settlement_consumes_available_capacity_in_order_without_inflating(
    entry, quantity, consumed, recovery
):
    linked = (reservation(entry), reservation(entry, "r2"))
    plan = plan_reservation_settlement(
        linked,
        order_id=entry.command_id,
        quantity=Decimal(quantity),
        reported_quantity=Decimal(quantity),
    )
    assert plan.consumed_quantity == Decimal(consumed)
    assert plan.recovery_required is recovery
    assert sum((r.active_quantity for r in linked), Decimal("0")) == Decimal("6")
    for updated in plan.updates:
        assert updated.released_quantity == Decimal("1")
        assert (
            updated.consumed_quantity + updated.released_quantity
            <= updated.reserved_quantity
        )
    if Decimal(quantity) == 4:
        assert [r.consumed_quantity for r in plan.updates] == [
            Decimal("4"),
            Decimal("2"),
        ]
    if recovery:
        assert "exceeds linked active reservations by 1" in plan.diagnostic


def test_missing_link_blocks_recovery_without_manufacturing_updates(entry):
    plan = plan_reservation_settlement(
        (),
        order_id=entry.command_id,
        quantity=Decimal("1"),
        reported_quantity=Decimal("1"),
    )
    assert not plan.updates and plan.consumed_quantity == 0
    assert plan.recovery_required and "No active reservation" in plan.diagnostic


def test_release_preserves_consumed_quantity_and_releases_only_remaining(entry):
    original = reservation(entry)
    plan = plan_reservation_release((original,))
    assert plan.released_quantity == Decimal("3")
    updated = plan.updates[0]
    assert updated.consumed_quantity == original.consumed_quantity
    assert updated.released_quantity == Decimal("4")
    assert updated.active_quantity == 0
    assert original.active_quantity == Decimal("3")
