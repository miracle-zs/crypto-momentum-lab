from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.domain.execution.fill_attribution import (
    plan_fill_observation,
)

NOW = datetime(2026, 9, 30, tzinfo=UTC)


@pytest.fixture
def fill():
    return AccountFillEvent(
        environment="live",
        account_label="account-3",
        symbol="TESTUSDT",
        trade_id="trade",
        order_id="order",
        side="BUY",
        quantity=Decimal("5"),
        price=Decimal("12"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=NOW,
        raw_payload={},
    )


def test_new_trade_is_admitted_once(fill):
    result = plan_fill_observation(
        fill, trade_seen=False, can_adopt_prefix=False
    )
    assert result.fill is fill
    assert result.is_new_trade
    assert not result.adopted_prefix_trade


def test_seen_trade_is_idempotently_ignored(fill):
    result = plan_fill_observation(
        fill, trade_seen=True, can_adopt_prefix=False
    )
    assert not result.is_new_trade


def test_epoch_adoption_replays_seen_prefix_trade(fill):
    result = plan_fill_observation(fill, trade_seen=True, can_adopt_prefix=True)
    assert result.is_new_trade and result.adopted_prefix_trade


def test_order_total_cannot_be_misread_as_an_account_trade(fill):
    cumulative = replace(fill, raw_payload={"cum_qty": "5"})
    with pytest.raises(ValueError, match="individual trade"):
        plan_fill_observation(
            cumulative, trade_seen=False, can_adopt_prefix=False
        )
