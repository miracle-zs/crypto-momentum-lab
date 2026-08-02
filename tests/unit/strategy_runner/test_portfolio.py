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
    Candle15mAggregator,
    ClosedCandle15m,
    PaperExitConfig,
    PaperExitMode,
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


def test_15m_aggregator_requires_officially_closed_one_minute_klines() -> None:
    aggregator = Candle15mAggregator()
    candle_start = datetime(2026, 7, 26, 0, 0, tzinfo=UTC)

    for index in range(60):
        assert aggregator.observe(
            _state(
                open_price=Decimal("100"),
                close=Decimal("99") if index == 59 else Decimal("100.5"),
                bucket_start=candle_start + timedelta(seconds=index * 15),
            )
        ) is None

    closed: ClosedCandle15m | None = None
    for index in range(15):
        closed = aggregator.observe(
            _state_with_closed_1m(
                candle_start=candle_start,
                minute_index=index,
                open_price=Decimal("100"),
                close_price=Decimal("99") if index == 14 else Decimal("100.5"),
            )
        )
        if index < 14:
            assert closed is None

    assert closed == ClosedCandle15m(
        symbol="BTCUSDT",
        candle_start=candle_start,
        candle_end=candle_start + timedelta(minutes=15),
        open_price=Decimal("100"),
        close_price=Decimal("99"),
    )


def test_15m_aggregator_does_not_emit_an_incomplete_official_candle() -> None:
    aggregator = Candle15mAggregator()
    candle_start = datetime(2026, 7, 26, 0, 0, tzinfo=UTC)

    assert aggregator.observe(
        _state_with_closed_1m(
            candle_start=candle_start,
            minute_index=0,
            open_price=Decimal("100"),
            close_price=Decimal("100.5"),
        )
    ) is None

    closed = aggregator.observe(
        _state_with_closed_1m(
            candle_start=candle_start,
            minute_index=14,
            open_price=Decimal("101"),
            close_price=Decimal("99"),
        )
    )

    assert closed is None


def test_15m_aggregator_ignores_late_state_from_a_closed_candle() -> None:
    aggregator = Candle15mAggregator()
    candle_start = datetime(2026, 7, 26, 0, 0, tzinfo=UTC)

    for index in range(60):
        aggregator.observe(
            _state(
                open_price=Decimal("100"),
                close=Decimal("99") if index == 59 else Decimal("100.5"),
                bucket_start=candle_start + timedelta(seconds=index * 15),
            )
        )

    assert aggregator.observe(
        _state(
            open_price=Decimal("100"),
            close=Decimal("101"),
            bucket_start=candle_start + timedelta(minutes=14),
        )
    ) is None


def test_candle_exit_closes_on_first_opposite_candle() -> None:
    position = position_from_entry_fill("run-1", _fill())
    assert position is not None
    config = PaperExitConfig(
        exit_mode=PaperExitMode.CANDLE_15M,
        max_holding_buckets=5760,
    )

    closed = mark_positions(
        positions=(position,),
        state=_state(
            close=Decimal("99"),
            bucket_start=datetime(2026, 7, 26, 0, 15, tzinfo=UTC),
        ),
        config=config,
        taker_fee_rate=Decimal("0.0004"),
        closed_candle=ClosedCandle15m(
            symbol="BTCUSDT",
            candle_start=datetime(2026, 7, 26, 0, 0, tzinfo=UTC),
            candle_end=datetime(2026, 7, 26, 0, 15, tzinfo=UTC),
            open_price=Decimal("100"),
            close_price=Decimal("99"),
        ),
    )[0]

    assert closed.status is PaperPositionStatus.CLOSED
    assert closed.close_reason == "candle_15m_bearish"


def test_candle_exit_holds_aligned_and_doji_candles_and_reverses_short() -> None:
    long_position = position_from_entry_fill("run-1", _fill())
    short_position = position_from_entry_fill(
        "run-1",
        replace(_fill(), side=StrategySide.SHORT),
    )
    assert long_position is not None
    assert short_position is not None
    config = PaperExitConfig(
        exit_mode=PaperExitMode.CANDLE_15M,
        max_holding_buckets=5760,
    )

    aligned_long = mark_positions(
        positions=(long_position,),
        state=_state(
            close=Decimal("105"),
            bucket_start=datetime(2026, 7, 26, 0, 15, tzinfo=UTC),
        ),
        config=config,
        taker_fee_rate=Decimal("0.0004"),
        closed_candle=_candle(open_price="100", close_price="105"),
    )[0]
    doji_long = mark_positions(
        positions=(long_position,),
        state=_state(
            close=Decimal("100"),
            bucket_start=datetime(2026, 7, 26, 0, 15, tzinfo=UTC),
        ),
        config=config,
        taker_fee_rate=Decimal("0.0004"),
        closed_candle=_candle(open_price="100", close_price="100"),
    )[0]
    reversed_short = mark_positions(
        positions=(short_position,),
        state=_state(
            close=Decimal("101"),
            bucket_start=datetime(2026, 7, 26, 0, 15, tzinfo=UTC),
        ),
        config=config,
        taker_fee_rate=Decimal("0.0004"),
        closed_candle=_candle(open_price="100", close_price="101"),
    )[0]

    assert aligned_long.status is PaperPositionStatus.OPEN
    assert doji_long.status is PaperPositionStatus.OPEN
    assert reversed_short.status is PaperPositionStatus.CLOSED
    assert reversed_short.close_reason == "candle_15m_bullish"


def test_candle_exit_uses_candle_close_time_and_price() -> None:
    position = position_from_entry_fill(
        "run-1",
        replace(_fill(), side=StrategySide.SHORT),
    )
    assert position is not None
    candle = ClosedCandle15m(
        symbol=position.symbol,
        candle_start=position.opened_at + timedelta(minutes=15),
        candle_end=position.opened_at + timedelta(minutes=30),
        open_price=Decimal("99"),
        close_price=Decimal("101"),
    )
    closed = mark_positions(
        positions=(position,),
        state=_state(
            close=Decimal("102"),
            bucket_start=position.opened_at + timedelta(minutes=45),
        ),
        config=PaperExitConfig(
            exit_mode=PaperExitMode.CANDLE_15M,
            max_holding_buckets=5760,
        ),
        taker_fee_rate=Decimal("0.0004"),
        closed_candle=candle,
    )[0]

    assert closed.status is PaperPositionStatus.CLOSED
    assert closed.close_reason == "candle_15m_bullish"
    assert closed.closed_at == candle.candle_end
    assert closed.exit_price == candle.close_price


def test_executable_exit_uses_the_side_of_the_book() -> None:
    position = position_from_entry_fill("run-1", _fill())
    assert position is not None
    state = replace(
        _state(close=Decimal("103")),
        last_bid_price=Decimal("102"),
        last_ask_price=Decimal("104"),
        spread=Decimal("2"),
        midpoint=Decimal("103"),
    )

    closed = mark_positions(
        positions=(position,),
        state=state,
        config=PaperExitConfig(require_executable_quote=True),
        taker_fee_rate=Decimal("0.0004"),
    )[0]

    assert closed.status is PaperPositionStatus.CLOSED
    assert closed.exit_price == Decimal("102")


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
    open_price: Decimal | None = None,
    bucket_start: datetime = datetime(2026, 7, 26, 0, 0, 15, tzinfo=UTC),
) -> MarketState15s:
    if open_price is None:
        open_price = close
    return MarketState15s(
        schema_version=1,
        exchange="binance-usdm",
        environment="research",
        symbol="BTCUSDT",
        bucket_start=bucket_start,
        bucket_end=bucket_start + timedelta(seconds=15),
        open_price=open_price,
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


def _state_with_closed_1m(
    *,
    candle_start: datetime,
    minute_index: int,
    open_price: Decimal,
    close_price: Decimal,
) -> MarketState15s:
    bucket_start = candle_start + timedelta(minutes=minute_index + 1)
    return replace(
        _state(close=close_price, open_price=open_price, bucket_start=bucket_start),
        closed_kline_count=1,
        closed_kline_1m_open_time=candle_start
        + timedelta(minutes=minute_index),
        closed_kline_1m_close_time=candle_start
        + timedelta(minutes=minute_index + 1)
        - timedelta(microseconds=1),
        closed_kline_1m_open_price=open_price,
        closed_kline_1m_close_price=close_price,
    )


def _candle(*, open_price: str, close_price: str) -> ClosedCandle15m:
    candle_start = datetime(2026, 7, 26, 0, 0, tzinfo=UTC)
    return ClosedCandle15m(
        symbol="BTCUSDT",
        candle_start=candle_start,
        candle_end=candle_start + timedelta(minutes=15),
        open_price=Decimal(open_price),
        close_price=Decimal(close_price),
    )
