from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.domain.execution.evidence_settlement import account_trade_delta
from crypto_momentum_lab.domain.execution.fill_attribution import (
    is_exit_fill,
    plan_fill_observation,
)
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderState,
    FuturesPositionSide,
)
from crypto_momentum_lab.domain.strategy import StrategySide

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


def plan(fill, **kwargs):
    return plan_fill_observation(
        fill,
        existing_trade=kwargs.get("existing_trade"),
        trade_seen=kwargs.get("trade_seen", False),
        previous_quantity=Decimal("2"),
        previous_quote=Decimal("20"),
        can_adopt_prefix=kwargs.get("can_adopt_prefix", False),
    )


def test_new_real_trade_keeps_original_payload_and_settlement(fill):
    result = plan(fill)
    assert result.fill is fill
    assert result.quantity == result.settlement_quantity == Decimal("5")
    assert result.is_new_trade and not result.is_cumulative
    assert result.watermark is None


@pytest.mark.parametrize("seen", [False, True])
def test_repeated_journal_trade_does_not_settle_twice(fill, seen):
    result = plan(fill, existing_trade=replace(fill, side="buy"), trade_seen=seen)
    assert result.quantity == result.settlement_quantity == 0
    assert result.is_new_trade is (not seen)


@pytest.mark.parametrize(
    "changed",
    [
        {"quantity": Decimal("6")},
        {"price": Decimal("13")},
        {"side": "SELL"},
        {"symbol": "OTHERUSDT"},
    ],
)
def test_existing_trade_identity_conflict_is_rejected(fill, changed):
    with pytest.raises(ValueError, match="conflicts with existing journal"):
        plan(fill, existing_trade=replace(fill, **changed))


def test_global_identity_without_journal_requires_recovery(fill):
    with pytest.raises(ValueError, match="recovery is required"):
        plan(fill, trade_seen=True)


def test_verified_epoch_prefix_is_rejournaled_without_second_settlement(fill):
    result = plan(fill, trade_seen=True, can_adopt_prefix=True)
    assert result.adopted_prefix_trade and result.is_new_trade
    assert result.quantity == fill.quantity
    assert result.settlement_quantity == 0
    assert result.fill is fill


def test_cumulative_plan_keeps_increment_price_and_watermark(fill):
    cumulative = replace(
        fill, raw_payload={"is_cumulative": True, "cum_qty": "5", "cum_quote": "60"}
    )
    result = plan(cumulative)
    assert result.is_cumulative
    assert result.quantity == result.settlement_quantity == Decimal("3")
    assert result.fill.price == Decimal("40") / Decimal("3")
    assert result.watermark == (Decimal("5"), Decimal("60"))
    assert cumulative.quantity == Decimal("5")


def test_real_trade_watermark_filters_order_and_subtracts_existing_report(fill):
    other = replace(
        fill, trade_id="other-trade", order_id="other-order", quantity=Decimal("100")
    )
    result = account_trade_delta(
        "order",
        previous_quantity=Decimal("2"),
        previous_quote=Decimal("20"),
        account_fills=(fill, other),
    )
    assert result.quantity == Decimal("3")
    assert result.watermark == (Decimal("5"), Decimal("60"))


def test_report_ahead_of_real_trades_cannot_rewind_watermark(fill):
    result = account_trade_delta(
        "order",
        previous_quantity=Decimal("6"),
        previous_quote=Decimal("70"),
        account_fills=(fill,),
    )
    assert result.quantity == 0 and result.watermark is None


def test_real_trade_quote_must_advance_with_quantity(fill):
    with pytest.raises(ValueError, match="quote does not advance"):
        account_trade_delta(
            "order",
            previous_quantity=Decimal("2"),
            previous_quote=Decimal("100"),
            account_fills=(fill,),
        )


@pytest.mark.parametrize(
    "position,episode,side,reserved,reduce_only,expected",
    [
        (FuturesPositionSide.LONG, None, "SELL", False, False, True),
        (FuturesPositionSide.SHORT, None, "BUY", False, False, True),
        (FuturesPositionSide.BOTH, StrategySide.LONG, "SELL", False, False, True),
        (FuturesPositionSide.BOTH, StrategySide.SHORT, "BUY", False, False, True),
        (FuturesPositionSide.BOTH, None, "BUY", True, False, True),
        (FuturesPositionSide.BOTH, None, "BUY", False, True, True),
        (FuturesPositionSide.LONG, None, "BUY", False, False, False),
    ],
)
def test_exit_classification_preserves_position_episode_and_explicit_evidence(
    fill,
    position,
    episode,
    side,
    reserved,
    reduce_only,
    expected,
):
    observed = replace(fill, side=side, raw_payload={"reduce_only": reduce_only})
    assert (
        is_exit_fill(
            observed,
            position_side=position,
            episode_side=episode,
            has_active_reservations=reserved,
            order_event=None,
        )
        is expected
    )


def test_reduce_only_order_event_classifies_exit_without_position_hint(fill):
    event = ExchangeOrderEvent(
        event_id="event",
        client_order_id="order",
        state=ExchangeOrderState.FILLED,
        occurred_at=NOW,
        exchange_order_id=None,
        details={"is_reduce_only": True},
    )
    assert is_exit_fill(
        fill,
        position_side=FuturesPositionSide.BOTH,
        episode_side=None,
        has_active_reservations=False,
        order_event=event,
    )
