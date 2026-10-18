import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from crypto_momentum_lab.next_gen.engine import AccountState
from crypto_momentum_lab.next_gen.executor import OrderExecutor
from crypto_momentum_lab.next_gen.exit_manager import ExitManager
from crypto_momentum_lab.next_gen.models import OrderStatus, PositionBatch, SystemMode
from crypto_momentum_lab.next_gen.persistence import InMemoryTradingRepository
from tests.unit.next_gen.factories import risk_config, symbol_rules


def state_with_risk():
    repository = InMemoryTradingRepository()
    rest = AsyncMock()
    state = AccountState("live", "a1", repository, rest)
    state.risk_config = risk_config()
    state.set_system_mode(SystemMode.RUNNING)
    state.set_ws_connected(True)
    return state, repository, rest


def test_missing_risk_configuration_never_gets_default_limits():
    state = AccountState("live", "a1")
    assert not state.check_notional_limits("BTCUSDT", Decimal("1"), Decimal("100"))


def test_pending_entry_consumes_position_slot_and_gross_budget():
    state, _, _ = state_with_risk()
    state.risk_config = replace(
        state.risk_config, max_open_positions=1, max_gross_notional=Decimal("150")
    )
    state.latest_prices["BTCUSDT"] = Decimal("100")
    order = state.create_entry_order(
        "BTCUSDT",
        "LONG",
        Decimal("1"),
        Decimal("100"),
        "pending_risk",
    )
    assert not state.check_notional_limits("ETHUSDT", Decimal("1"), Decimal("10"))
    assert not state.check_notional_limits("BTCUSDT", Decimal("1"), Decimal("100"))
    assert state.check_notional_limits(
        "BTCUSDT", Decimal("1"), Decimal("100"), exclude_order_id=order.client_order_id
    )


@pytest.mark.asyncio
async def test_daily_exchange_loss_is_counted_once_and_blocks_new_risk():
    state, _, _ = state_with_risk()
    state.risk_config = replace(state.risk_config, max_daily_loss=Decimal("10"))
    state.daily_pnl_ready = True
    state.latest_prices["BTCUSDT"] = Decimal("100")
    batch = PositionBatch(
        "live",
        "a1",
        "loss",
        "BTCUSDT",
        "LONG",
        Decimal("2"),
        Decimal("100"),
        datetime.now(UTC) - timedelta(minutes=30),
    )
    state.batches[batch.batch_id] = batch
    order = state.create_exit_order(batch.batch_id, Decimal("2"), Decimal("80"))
    for _ in range(2):
        await state.on_order_trade_update(
            order.client_order_id,
            "BTCUSDT",
            "LONG",
            "SELL",
            OrderStatus.PARTIALLY_FILLED,
            Decimal("1"),
            Decimal("80"),
            Decimal("1"),
            Decimal("80"),
            trade_time=datetime.now(UTC),
            exchange_realized_pnl=Decimal("-20"),
        )
    assert state.daily_realized_pnl == Decimal("-20")
    assert not state.check_notional_limits("BTCUSDT", Decimal("0.1"), Decimal("100"))


@pytest.mark.asyncio
async def test_restored_grace_exit_keeps_original_deadline_and_only_sweeps():
    state, repository, rest = state_with_risk()
    executor = OrderExecutor(rest, state, repository)
    manager = ExitManager(
        rest,
        state,
        executor,
        symbol_rules(),
        grace_period_sec=7200,
        decision_profit_pct=Decimal("0.001"),
        grace_profit_pct=Decimal("0.0088"),
    )
    batch = PositionBatch(
        "live",
        "a1",
        "resume",
        "BTCUSDT",
        "LONG",
        Decimal("1"),
        Decimal("100"),
        datetime.now(UTC) - timedelta(hours=5),
    )
    state.batches[batch.batch_id] = batch
    order = state.create_exit_order(batch.batch_id, Decimal("1"), Decimal("101"))
    order.status = OrderStatus.OPEN
    anchor = datetime.now(UTC) - timedelta(hours=3)
    batch.exit_order_submitted_at = anchor

    async def cancel(**params):
        await state.on_order_trade_update(
            order.client_order_id,
            "BTCUSDT",
            "LONG",
            "SELL",
            OrderStatus.CANCELED,
            Decimal("0"),
            Decimal("0"),
            Decimal("0"),
            Decimal("0"),
        )

    rest.cancel_order.side_effect = cancel
    try:
        manager.resume_active_exits()
        await asyncio.gather(*manager._active_exit_tasks.values())
        rest.post_order.assert_awaited_once()
        assert rest.post_order.await_args.kwargs["order_type"] == "MARKET"
        assert batch.exit_order_submitted_at == anchor
        rest.fetch_startup_order.assert_not_awaited()
    finally:
        await manager.close()
