from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.next_gen.engine import AccountState
from crypto_momentum_lab.next_gen.models import (
    OrderStatus,
    PositionBatch,
    SystemMode,
    generate_batch_id,
)
from crypto_momentum_lab.next_gen.persistence import (
    InMemoryTradingRepository,
    compute_advisory_lock_key,
)
from tests.unit.next_gen.factories import risk_config


def test_compute_advisory_lock_key():
    key = compute_advisory_lock_key(environment="live", account_label="a1")
    assert key == "live:a1"


@pytest.mark.asyncio
async def test_in_memory_repository_save_and_retrieve():
    repo = InMemoryTradingRepository()
    batch_id = generate_batch_id(
        symbol="BTCUSDT",
        position_side="LONG",
        account_label="a1",
        timestamp_str="261018120000",
        salt="9f",
        counter=1,
    )
    batch = PositionBatch(
        environment="live",
        account_label="a1",
        batch_id=batch_id,
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("1.0"),
        entry_price=Decimal("65000"),
        entry_bar_time=datetime.now(UTC),
    )

    state = AccountState(
        environment="live",
        account_label="a1",
        db=repo,
    )
    state.risk_config = risk_config(state.environment, state.account_label)
    state.set_system_mode(SystemMode.RUNNING)
    state.ws_connected = True

    order = state.create_entry_order(
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("1.0"),
        price=Decimal("65000"),
        batch_id=batch_id,
    )

    # Initial save of order
    await repo.async_save_order(order)
    saved_order = repo.get_order("live", "a1", order.client_order_id)
    assert saved_order is not None
    assert saved_order.status == OrderStatus.PENDING

    # Save order and batch
    await repo.async_save_order_and_batch(order, batch)
    saved_batch = repo.get_batch("live", "a1", batch_id)
    assert saved_batch is not None
    assert saved_batch.quantity == Decimal("1.0")


@pytest.mark.asyncio
async def test_db_retry_succeeds_on_second_attempt():
    repo = InMemoryTradingRepository()
    # Configure 1 transient failure
    repo.fail_next_n_writes(1)

    state = AccountState(
        environment="live",
        account_label="a1",
        db=repo,
    )
    state.risk_config = risk_config(state.environment, state.account_label)
    state.set_system_mode(SystemMode.RUNNING)
    state.ws_connected = True

    batch_id = generate_batch_id(
        symbol="BTCUSDT",
        position_side="LONG",
        account_label="a1",
        timestamp_str="261018120000",
        salt="9f",
        counter=1,
    )
    order = state.create_entry_order(
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("1.0"),
        price=Decimal("65000"),
        batch_id=batch_id,
    )

    # on_order_trade_update should retry on failure and succeed on second attempt
    await state.on_order_trade_update(
        client_order_id=order.client_order_id,
        symbol="BTCUSDT",
        position_side="LONG",
        side="BUY",
        order_status=OrderStatus.FILLED,
        cum_filled_qty=Decimal("1.0"),
        avg_price=Decimal("65000"),
        last_filled_qty=Decimal("1.0"),
        last_filled_price=Decimal("65000"),
    )

    # Must stay RUNNING (not DEGRADED) because retry succeeded!
    assert state.system_mode == SystemMode.RUNNING
    assert not state.has_unrecoverable_fault


@pytest.mark.asyncio
async def test_db_failure_exhausted_enters_degraded():
    repo = InMemoryTradingRepository()
    # Configure 5 failures (exceeding 2 attempts)
    repo.fail_next_n_writes(5)

    state = AccountState(
        environment="live",
        account_label="a1",
        db=repo,
    )
    state.risk_config = risk_config(state.environment, state.account_label)
    state.set_system_mode(SystemMode.RUNNING)
    state.ws_connected = True

    batch_id = generate_batch_id(
        symbol="BTCUSDT",
        position_side="LONG",
        account_label="a1",
        timestamp_str="261018120000",
        salt="9f",
        counter=1,
    )
    order = state.create_entry_order(
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("1.0"),
        price=Decimal("65000"),
        batch_id=batch_id,
    )

    # on_order_trade_update fails both attempts
    await state.on_order_trade_update(
        client_order_id=order.client_order_id,
        symbol="BTCUSDT",
        position_side="LONG",
        side="BUY",
        order_status=OrderStatus.FILLED,
        cum_filled_qty=Decimal("1.0"),
        avg_price=Decimal("65000"),
        last_filled_qty=Decimal("1.0"),
        last_filled_price=Decimal("65000"),
    )

    # Must enter DEGRADED with unrecoverable fault
    assert state.system_mode == SystemMode.DEGRADED
    assert state.has_unrecoverable_fault
    assert not state.can_submit_entry_order()


@pytest.mark.asyncio
async def test_async_get_historical_market_states():
    repo = InMemoryTradingRepository()
    bars = await repo.async_get_historical_market_states(
        "research", ["BTCUSDT", "ETHUSDT"], 140
    )
    assert bars == []
