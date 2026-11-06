from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.next_gen.engine import AccountState, PositionSnapshot
from crypto_momentum_lab.next_gen.persistence import InMemoryTradingRepository
from crypto_momentum_lab.next_gen.sizing import EntrySizer, EntrySizingUnavailable


@pytest.fixture
def sizing_state():
    repository = InMemoryTradingRepository()
    state = AccountState("live", "account", repository)
    state.ws_connected = True
    state.multi_assets_mode = False
    state.wallet_balances["USDT"] = Decimal("1000")
    return state, repository


async def test_fixed_and_per_trade_amounts(account_config, sizing_state):
    state, repository = sizing_state
    config = account_config.execution_inputs
    now = datetime(2026, 10, 24, tzinfo=UTC)
    fixed = EntrySizer(
        state, replace(config, sizing_mode="fixed", target_notional=Decimal("100"))
    )
    assert await fixed.entry_notional(now) == Decimal("100")

    state.update_exchange_positions(
        [PositionSnapshot("BTCUSDT", "LONG", Decimal("2"), Decimal("100"))]
    )
    state.latest_prices["BTCUSDT"] = Decimal("110")
    per_trade = EntrySizer(
        state, replace(config, sizing_mode="per_trade", equity_fraction=Decimal("0.4"))
    )
    assert await per_trade.entry_notional(now) == Decimal("408")
    state.latest_prices["BTCUSDT"] = Decimal("120")
    assert await per_trade.entry_notional(now) == Decimal("416")
    assert repository.entry_sizing == {}


async def test_daily_anchor_survives_restart_and_rolls_at_utc_midnight(
    account_config, sizing_state
):
    state, repository = sizing_state
    config = replace(
        account_config.execution_inputs,
        sizing_mode="daily",
        equity_fraction=Decimal("0.4"),
    )
    before_midnight = datetime(2026, 10, 24, 23, 59, tzinfo=UTC)
    assert await EntrySizer(state, config).entry_notional(before_midnight) == Decimal(
        "400"
    )
    state.wallet_balances["USDT"] = Decimal("1500")
    restarted = EntrySizer(state, config)
    assert await restarted.entry_notional(before_midnight) == Decimal("400")
    assert await restarted.entry_notional(
        before_midnight + timedelta(minutes=1)
    ) == Decimal("600")
    assert repository.entry_sizing[("live", "account")].equity == Decimal("1500")


async def test_flat_anchor_waits_for_flat_account_and_survives_restart(
    account_config, sizing_state
):
    state, _ = sizing_state
    config = replace(
        account_config.execution_inputs,
        sizing_mode="flat_reset",
        equity_fraction=Decimal("0.4"),
    )
    now = datetime(2026, 10, 24, tzinfo=UTC)
    state.update_exchange_positions(
        [PositionSnapshot("BTCUSDT", "LONG", Decimal("1"), Decimal("100"))]
    )
    sizer = EntrySizer(state, config)
    with pytest.raises(
        EntrySizingUnavailable, match="flat_sizing_requires_empty_account"
    ):
        await sizer.entry_notional(now)
    state.exchange_positions.clear()
    assert await sizer.entry_notional(now) == Decimal("400")
    state.update_exchange_positions(
        [PositionSnapshot("BTCUSDT", "LONG", Decimal("1"), Decimal("100"))]
    )
    state.wallet_balances["USDT"] = Decimal("1500")
    restarted = EntrySizer(state, config)
    assert await restarted.entry_notional(now) == Decimal("400")
    state.exchange_positions.clear()
    assert await restarted.entry_notional(now) == Decimal("600")


async def test_compounding_requires_confirmed_account_state(
    account_config, sizing_state
):
    state, _ = sizing_state
    config = replace(
        account_config.execution_inputs,
        sizing_mode="per_trade",
        equity_fraction=Decimal("0.4"),
    )
    state.ws_connected = False
    with pytest.raises(
        EntrySizingUnavailable, match="private_account_state_unavailable"
    ):
        await EntrySizer(state, config).entry_notional(
            datetime(2026, 10, 24, tzinfo=UTC)
        )
