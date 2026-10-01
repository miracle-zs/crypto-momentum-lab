"""Unknown receipts never become terminal merely because the account is flat."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from crypto_momentum_lab.domain.account.models import AccountPositionSnapshot
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderSnapshot,
    ExchangeOrderState,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
from crypto_momentum_lab.domain.execution.trade_command import (
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.strategy.models import EntryType, StrategySide
from crypto_momentum_lab.live_rollout.exit_receipt_recovery import (
    LiveExitReceiptRecovery,
)

NOW = datetime(2026, 10, 1, tzinfo=UTC)


def case():
    command = TradeCommand(
        "exit-test",
        PositionKey("live", "primary", "BTCUSDT", "LONG"),
        TradeCommandType.EXIT,
        StrategySide.LONG,
        EntryType.MARKET,
        Decimal("2"),
        reduce_only=True,
        created_at=NOW - timedelta(hours=1),
    )
    snapshot = AccountPositionSnapshot(
        "live",
        "primary",
        "BTCUSDT",
        "LONG",
        Decimal("0"),
        Decimal("0"),
        Decimal("10"),
        Decimal("0"),
        Decimal("0"),
        2,
        "cross",
        NOW,
        {},
    )
    exchange = SimpleNamespace(
        fetch_positions=AsyncMock(return_value=(snapshot,)),
        fetch_open_orders=AsyncMock(return_value=()),
        query_order_by_client_id=AsyncMock(return_value=None),
    )
    session = MagicMock()
    session.scalar = AsyncMock(side_effect=[None, 0])
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=session)
    context.__aexit__ = AsyncMock(return_value=None)
    recovery = LiveExitReceiptRecovery(
        lambda: context,
        exchange=exchange,
        account_label="primary",
        run_id="run",
        clock=lambda: NOW,
    )
    return command, exchange, session, recovery


async def test_fresh_explicit_flat_and_verified_order_absence_supersede():
    command, exchange, _, recovery = case()
    # REST observes the position after the initial clock read.
    values = iter((NOW - timedelta(seconds=1), NOW))
    recovery._clock = lambda: next(values)
    result = await recovery(command)
    assert result.status == "SUPERSEDED"
    assert result.reason == "exchange_absence_verified_explicit_position_flat"
    exchange.query_order_by_client_id.assert_awaited_once_with("BTCUSDT", command.client_order_id("run"))


@pytest.mark.parametrize(
    "condition",
    [
        "missing_zero",
        "nonflat",
        "stale",
        "open_order",
        "local_order",
        "reservation",
        "too_old",
        "wrong_account",
    ],
)
async def test_unconfirmed_receipts_remain_pending(condition):
    command, exchange, session, recovery = case()
    position = exchange.fetch_positions.return_value[0]
    if condition == "missing_zero":
        exchange.fetch_positions.return_value = ()
    elif condition == "nonflat":
        exchange.fetch_positions.return_value = (
            replace(position, position_amt=Decimal("1")),
        )
    elif condition == "stale":
        exchange.fetch_positions.return_value = (
            replace(position, observed_at=NOW - timedelta(minutes=4)),
        )
    elif condition == "open_order":
        exchange.fetch_open_orders.return_value = (SimpleNamespace(symbol="BTCUSDT"),)
    elif condition == "local_order":
        session.scalar.side_effect = [
            SimpleNamespace(
                run_id="run",
                symbol="BTCUSDT",
                client_order_id=command.client_order_id("run"),
                reduce_only=True,
            ),
            0,
        ]
    elif condition == "reservation":
        session.scalar.side_effect = [None, 1]
    elif condition == "too_old":
        command = replace(command, created_at=NOW - timedelta(days=3))
    else:
        command = replace(
            command, position_key=PositionKey("live", "other", "BTCUSDT", "LONG")
        )
    assert (await recovery(command)).status == "PENDING"


@pytest.mark.parametrize(
    "quantity,expected", [(Decimal("2"), "DISPATCHED"), (Decimal("1"), "PENDING")]
)
async def test_filled_receipt_requires_complete_durable_trade_facts(quantity, expected):
    command, exchange, session, recovery = case()
    exchange.query_order_by_client_id.return_value = ExchangeOrderSnapshot(
        command.client_order_id("run"),
        "order-1",
        ExchangeOrderState.FILLED,
        NOW,
        Decimal("2"),
        Decimal("10"),
    )
    session.scalar.side_effect = [None, 0, quantity]
    assert (await recovery(command)).status == expected


async def test_exchange_failure_does_not_produce_terminal_disposition():
    command, exchange, _, recovery = case()
    exchange.query_order_by_client_id.side_effect = TimeoutError("exchange timeout")
    with pytest.raises(TimeoutError):
        await recovery(command)
    assert (await recovery(command)).reason == "exchange_receipt_retry_backoff"
    exchange.query_order_by_client_id.assert_awaited_once()


async def test_pending_batch_reuses_short_account_cut_but_queries_each_receipt():
    command, exchange, session, recovery = case()
    session.scalar.side_effect = [None, 0, None, 0, None, 0]
    assert (await recovery(command)).status == "SUPERSEDED"
    assert (
        await recovery(replace(command, command_id="exit-second"))
    ).status == "SUPERSEDED"
    exchange.fetch_positions.assert_awaited_once()
    exchange.fetch_open_orders.assert_awaited_once()
    assert exchange.query_order_by_client_id.await_count == 2
    recovery._clock = lambda: NOW + timedelta(seconds=6)
    await recovery(command)
    assert exchange.fetch_positions.await_count == 2


@pytest.mark.parametrize("command_id", ["exit-test", "cmd_exit_dec_龙虾USDT_6887952b5b9bfae9", "exit-" + "x" * 80])
async def test_recovery_uses_submission_identity(command_id):
    command, exchange, _, recovery = case()
    command = replace(command, command_id=command_id)
    await recovery(command)
    exchange.query_order_by_client_id.assert_awaited_once_with(
        "BTCUSDT", command.client_order_id("run")
    )
    assert command.client_order_id("run").isascii()
    assert len(command.client_order_id("run")) <= 36


async def test_recovery_preserves_explicit_submission_identity():
    command, exchange, _, recovery = case()
    command = replace(command, idempotency_key="existing-client-id")
    await recovery(command)
    exchange.query_order_by_client_id.assert_awaited_once_with("BTCUSDT", "existing-client-id")


async def test_historical_durable_identity_conflict_is_not_order_absence():
    command, exchange, session, recovery = case()
    session.scalar.side_effect = [SimpleNamespace(
        run_id="old-run", symbol="BTCUSDT", client_order_id="legacy-exit", reduce_only=True
    ), 0]
    result = await recovery(command)
    assert result.status == "PENDING"
    assert result.reason == "durable_order_identity_mismatch"
    exchange.query_order_by_client_id.assert_not_awaited()


async def test_existing_legacy_order_keeps_its_durable_identity():
    command, exchange, session, recovery = case()
    session.scalar.side_effect = [SimpleNamespace(
        run_id="run", symbol="BTCUSDT", client_order_id="legacy-exit", reduce_only=True
    ), 0]
    result = await recovery(command)
    assert result.status == "PENDING"
    assert result.reason == "durable_order_exists_but_exchange_receipt_unknown"
    exchange.query_order_by_client_id.assert_awaited_once_with("BTCUSDT", "legacy-exit")


async def test_explicit_identity_conflict_does_not_query_or_supersede():
    command, exchange, session, recovery = case()
    command = replace(command, idempotency_key="explicit-id")
    session.scalar.side_effect = [SimpleNamespace(
        run_id="run", symbol="BTCUSDT", client_order_id="different-id", reduce_only=True
    ), 0]
    assert (await recovery(command)).reason == "durable_order_identity_mismatch"
    exchange.query_order_by_client_id.assert_not_awaited()


@pytest.mark.parametrize("state,exchange_id,expected", [
    ("rejected", None, "SUPERSEDED"),
    ("unknown_pending", None, "PENDING"),
    ("rejected", "123", "PENDING"),
])
async def test_invalid_legacy_receipt_requires_proven_rejection(state, exchange_id, expected):
    command, exchange, session, recovery = case()
    session.scalar.side_effect = [SimpleNamespace(
        run_id="run", symbol="BTCUSDT", reduce_only=True,
        client_order_id="cmd_exit_dec_龙虾USDT_70b9111923319e92",
        state=state, exchange_order_id=exchange_id,
    ), 0]
    assert (await recovery(command)).status == expected
    exchange.query_order_by_client_id.assert_not_awaited()
