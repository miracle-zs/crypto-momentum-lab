"""Durable command codec acceptance across restarts and legacy watermarks."""

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.execution.command_codec import (
    RestoredCommand,
    SkippedCommand,
    decode_active_command,
    decode_order_watermark,
    encode_outbox_details,
)
from crypto_momentum_lab.domain.execution.command_models import (
    DispatchState,
    ExecutionScope,
    OutboxEntry,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.trade_command import (
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide

NOW = datetime(2026, 9, 30, tzinfo=UTC)


@pytest.fixture
def row():
    scope = ExecutionScope("live", "account-3", "TESTUSDT", FuturesPositionSide.LONG)
    command = TradeCommand(
        command_id="command-1",
        position_key=scope.to_position_key(),
        command_type=TradeCommandType.EXIT,
        side=StrategySide.LONG,
        order_type=EntryType.MARKET,
        requested_quantity=Decimal("5.00"),
        reduce_only=True,
        expected_projection_version="token",
        created_at=NOW,
    )
    entry = OutboxEntry(
        command.command_id,
        "request",
        scope,
        command,
        attempt_count=1,
        external_order_id="exchange-1",
        last_error="prior",
        created_at=NOW,
        updated_at=NOW,
    )
    details = encode_outbox_details(
        entry,
        reservation_ids=("r1", "r2"),
        cumulative_quantity=Decimal("2"),
        cumulative_quote=Decimal("20"),
    )
    return dict(
        command_id=entry.command_id,
        client_order_id=entry.command_id,
        command=command.command_type.value,
        status=entry.state.value,
        requested_at=NOW,
        details=details,
    )


@pytest.mark.parametrize("state", list(DispatchState))
def test_round_trip_preserves_command_and_seals_interrupted_dispatch(row, state):
    row["status"] = state.value
    before = deepcopy(row)
    recovered = decode_active_command(
        row, account_label="account-3", restored_at=NOW + timedelta(seconds=1)
    )
    assert isinstance(recovered, RestoredCommand)
    assert recovered.entry.state == (
        DispatchState.UNKNOWN if state == DispatchState.DISPATCHING else state
    )
    assert recovered.reservation_ids == ("r1", "r2")
    assert recovered.entry.command.requested_quantity == Decimal("5.00")
    assert recovered.entry.command.reduce_only
    assert recovered.entry.command.expected_projection_version == "token"
    assert recovered.entry.external_order_id == "exchange-1"
    assert recovered.entry.attempt_count == 1
    assert recovered.requires_reconciliation == (
        state in (DispatchState.UNKNOWN, DispatchState.DISPATCHING)
    )
    assert recovered.needs_unknown_write == (state == DispatchState.DISPATCHING)
    assert row == before


@pytest.mark.parametrize(
    "field,value",
    [
        ("client_order_id", "different"),
        ("command_id", ""),
        ("status", "invalid"),
        ("details", None),
    ],
)
def test_malformed_identity_cannot_be_silently_skipped(row, field, value):
    row[field] = value
    with pytest.raises((ValueError, TypeError)):
        decode_active_command(row, account_label="account-3", restored_at=NOW)


@pytest.mark.parametrize(
    "field,value",
    [
        ("quantity", "NaN"),
        ("reduce_only", "true"),
        ("reservations", [None]),
        ("attempt_count", -1),
    ],
)
def test_legacy_skippable_payload_returns_explicit_diagnostic(row, field, value):
    row["details"][field] = value
    recovered = decode_active_command(row, account_label="account-3", restored_at=NOW)
    assert isinstance(recovered, SkippedCommand)
    assert recovered.command_id == "command-1" and recovered.reason


def test_other_account_is_filtered_before_command_payload_parsing(row):
    row["details"]["quantity"] = "invalid"
    assert (
        decode_active_command(row, account_label="account-4", restored_at=NOW) is None
    )


@pytest.mark.parametrize(
    "quantity,quote",
    [("-1", "0"), ("NaN", "0"), ("0", "1"), ("1", "0"), ("1", "Infinity")],
)
def test_invalid_watermark_cannot_become_fill_or_capacity(row, quantity, quote):
    watermark = dict(
        scope=row["details"]["scope"],
        client_order_id="command-1",
        cumulative_filled_quantity=quantity,
        cumulative_filled_quote=quote,
    )
    with pytest.raises(ValueError):
        decode_order_watermark(watermark, account_label="account-3")


def test_watermark_round_trip_and_account_filter(row):
    watermark = dict(
        scope=row["details"]["scope"],
        client_order_id="command-1",
        cumulative_filled_quantity=row["details"]["cumulative_filled_quantity"],
        cumulative_filled_quote=row["details"]["cumulative_filled_quote"],
    )
    recovered = decode_order_watermark(watermark, account_label="account-3")
    assert recovered.quantity == Decimal("2") and recovered.quote == Decimal("20")
    assert recovered.order_id == "command-1"
    assert decode_order_watermark(watermark, account_label="account-4") is None
