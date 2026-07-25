from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import StrategySide
from crypto_momentum_lab.strategy_runner.fills import (
    SimulatedFill,
    SimulatedFillStatus,
)
from crypto_momentum_lab.strategy_runner.portfolio import (
    PaperExitConfig,
    PaperPositionStatus,
    mark_positions,
    position_from_entry_fill,
)


def test_long_position_closes_at_take_profit_with_net_pnl() -> None:
    position = position_from_entry_fill("run-1", _fill())
    assert position is not None

    updates = mark_positions(
        positions=(position,),
        state=_state(close=Decimal("102")),
        config=PaperExitConfig(
            take_profit_pct=Decimal("0.02"),
            stop_loss_pct=Decimal("0.01"),
            max_holding_buckets=80,
        ),
        taker_fee_rate=Decimal("0.0004"),
    )

    closed = updates[0]
    assert closed.status is PaperPositionStatus.CLOSED
    assert closed.close_reason == "take_profit"
    assert closed.exit_price == Decimal("102")
    assert closed.realized_pnl == Decimal("1.9192")
    assert closed.return_pct == Decimal("0.019192")


def test_short_position_closes_at_stop_loss() -> None:
    position = position_from_entry_fill(
        "run-1",
        replace(_fill(), side=StrategySide.SHORT),
    )
    assert position is not None

    closed = mark_positions(
        positions=(position,),
        state=_state(close=Decimal("101")),
        config=PaperExitConfig(),
        taker_fee_rate=Decimal("0.0004"),
    )[0]

    assert closed.status is PaperPositionStatus.CLOSED
    assert closed.close_reason == "stop_loss"
    assert closed.realized_pnl == Decimal("-1.0804")


def test_position_closes_after_maximum_holding_period() -> None:
    position = position_from_entry_fill("run-1", _fill())
    assert position is not None

    closed = mark_positions(
        positions=(position,),
        state=_state(
            close=Decimal("100.5"),
            bucket_start=position.opened_at + timedelta(minutes=20),
        ),
        config=PaperExitConfig(max_holding_buckets=80),
        taker_fee_rate=Decimal("0.0004"),
    )[0]

    assert closed.status is PaperPositionStatus.CLOSED
    assert closed.close_reason == "max_holding_period"


def test_open_position_updates_mark_and_unrealized_pnl() -> None:
    position = position_from_entry_fill("run-1", _fill())
    assert position is not None

    marked = mark_positions(
        positions=(position,),
        state=_state(close=Decimal("100.5")),
        config=PaperExitConfig(),
        taker_fee_rate=Decimal("0.0004"),
    )[0]

    assert marked.status is PaperPositionStatus.OPEN
    assert marked.last_mark_price == Decimal("100.5")
    assert marked.unrealized_pnl == Decimal("0.46")


def _fill() -> SimulatedFill:
    opened_at = datetime(2026, 7, 26, 0, 0, tzinfo=UTC)
    return SimulatedFill(
        fill_id="fill-1",
        candidate_id="candidate-1",
        signal_id="signal-1",
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        status=SimulatedFillStatus.FILLED,
        target_fill_at=opened_at,
        filled_at=opened_at,
        requested_notional=Decimal("100"),
        filled_notional=Decimal("100"),
        quantity=Decimal("1"),
        reference_midpoint=Decimal("100"),
        spread=None,
        fill_price=Decimal("100"),
        fee=Decimal("0.04"),
        total_cost=Decimal("0.04"),
        cost_bps=Decimal("4"),
        reason="filled",
    )


def _state(
    *,
    close: Decimal,
    bucket_start: datetime = datetime(2026, 7, 26, 0, 0, 15, tzinfo=UTC),
) -> MarketState15s:
    return MarketState15s(
        schema_version=1,
        exchange="binance-usdm",
        environment="research",
        symbol="BTCUSDT",
        bucket_start=bucket_start,
        bucket_end=bucket_start + timedelta(seconds=15),
        open_price=close,
        high_price=close,
        low_price=close,
        close_price=close,
        trade_count=1,
        trade_notional=Decimal("100"),
        aggressive_buy_notional=Decimal("60"),
        aggressive_sell_notional=Decimal("40"),
        last_bid_price=None,
        last_ask_price=None,
        spread=None,
        midpoint=None,
        liquidation_count=0,
        liquidation_notional=Decimal("0"),
        mark_price=close,
        closed_kline_count=0,
        source_event_count=1,
        first_received_at=bucket_start,
        last_received_at=bucket_start + timedelta(seconds=15),
    )
