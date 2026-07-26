from dataclasses import fields
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import RunMode
from crypto_momentum_lab.strategies.compression_breakout import (
    CompressionBreakoutConfig,
)
from crypto_momentum_lab.strategy_runner import (
    InMemoryPaperMarketStateSource,
    PaperRunnerConfig,
    PaperRunnerError,
    ReplayExecutionConfig,
    SimulatedFillStatus,
    run_paper_trading,
)


def test_run_paper_trading_emits_incremental_fill_after_latency() -> None:
    report = run_paper_trading(
        source=InMemoryPaperMarketStateSource(
            _breakout_states() + (_state(5, close=Decimal("101.4")),)
        ),
        config=_paper_config(),
    )

    assert report.schema_version == 1
    assert report.run.run_mode is RunMode.PAPER
    assert report.input_state_count == 6
    assert report.processed_symbol_count == 1
    assert len(report.signals) == 1
    assert len(report.candidates) == 1
    assert len(report.paper_fills) == 1
    assert report.paper_fills[0].status is SimulatedFillStatus.FILLED
    assert report.paper_fills[0].filled_at == datetime(
        2026,
        6,
        22,
        0,
        1,
        15,
        tzinfo=UTC,
    )
    assert report.pending_candidate_count == 0
    assert report.fill_summary["fills_by_status"] == {"filled": 1}


def test_run_paper_trading_leaves_candidate_pending_when_source_ends() -> None:
    report = run_paper_trading(
        source=InMemoryPaperMarketStateSource(_breakout_states()),
        config=_paper_config(),
    )

    assert len(report.paper_fills) == 1
    assert report.paper_fills[0].status is SimulatedFillStatus.PENDING
    assert report.paper_fills[0].reason == "source_ended_before_fill"
    assert report.pending_candidate_count == 1


def test_run_paper_trading_rejects_missing_fill_price() -> None:
    missing_price_state = _without_fill_price(_state(5, close=Decimal("101.4")))

    report = run_paper_trading(
        source=InMemoryPaperMarketStateSource(
            _breakout_states() + (missing_price_state,)
        ),
        config=_paper_config(),
    )

    assert len(report.paper_fills) == 1
    assert report.paper_fills[0].status is SimulatedFillStatus.REJECTED
    assert report.paper_fills[0].reason == "missing_fill_price"


def test_run_paper_trading_respects_max_states() -> None:
    report = run_paper_trading(
        source=InMemoryPaperMarketStateSource(
            _breakout_states() + (_state(5, close=Decimal("101.4")),)
        ),
        config=_paper_config(max_states=4),
    )

    assert report.input_state_count == 4
    assert report.signals == ()
    assert report.candidates == ()
    assert report.paper_fills == ()


def test_run_paper_trading_rejects_backward_symbol_state() -> None:
    states = (_state(1, close=Decimal("100")), _state(0, close=Decimal("100")))

    with pytest.raises(PaperRunnerError, match="state moved backward"):
        run_paper_trading(
            source=InMemoryPaperMarketStateSource(states),
            config=_paper_config(),
        )


def test_run_paper_trading_accepts_orderflow_impulse_strategy() -> None:
    report = run_paper_trading(
        source=InMemoryPaperMarketStateSource(_orderflow_states()),
        config=_paper_config(strategy_name="orderflow_impulse"),
    )

    assert report.run.strategy_name == "orderflow_impulse"
    assert report.input_state_count == 7


def test_run_paper_trading_accepts_liquidation_cascade_strategy() -> None:
    report = run_paper_trading(
        source=InMemoryPaperMarketStateSource(_liquidation_states()),
        config=_paper_config(strategy_name="liquidation_cascade"),
    )

    assert report.run.strategy_name == "liquidation_cascade"
    assert report.input_state_count == 6


def test_run_paper_trading_rejects_empty_source() -> None:
    with pytest.raises(PaperRunnerError, match="no market states"):
        run_paper_trading(
            source=InMemoryPaperMarketStateSource(()),
            config=_paper_config(),
        )


def _paper_config(
    max_states: int | None = None,
    *,
    strategy_name: str = "compression_breakout",
) -> PaperRunnerConfig:
    return PaperRunnerConfig(
        strategy_name=strategy_name,
        run_id="paper-1",
        code_commit="unknown",
        generated_at=datetime(2026, 6, 22, 0, 0, tzinfo=UTC),
        compression_breakout=CompressionBreakoutConfig(
            compression_window_buckets=3,
            max_range_width_pct=Decimal("0.01"),
            min_breakout_pct=Decimal("0.001"),
            acceptance_buckets=2,
            cooldown_buckets=3,
            forward_horizon_buckets=(1,),
        ),
        candidate_notional=Decimal("100"),
        candidate_ttl_buckets=2,
        signal_interval_seconds=15,
        execution=ReplayExecutionConfig(latency_buckets=1),
        max_states=max_states,
    )


def _breakout_states() -> tuple[MarketState15s, ...]:
    return (
        _state(0, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(1, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(2, close=Decimal("100"), high=Decimal("100.1"), low=Decimal("99.9")),
        _state(3, close=Decimal("101.0")),
        _state(4, close=Decimal("101.2")),
    )


def _orderflow_states() -> tuple[MarketState15s, ...]:
    return (
        _state(0, close=Decimal("100"), trade_notional=Decimal("100")),
        _state(1, close=Decimal("100"), trade_notional=Decimal("100")),
        _state(2, close=Decimal("100"), trade_notional=Decimal("100")),
        _state(3, close=Decimal("100"), trade_notional=Decimal("100")),
        _state(
            4,
            close=Decimal("100"),
            trade_notional=Decimal("300"),
            aggressive_buy_notional=Decimal("250"),
        ),
        _state(
            5,
            close=Decimal("101"),
            trade_notional=Decimal("300"),
            aggressive_buy_notional=Decimal("250"),
        ),
        _state(
            6,
            close=Decimal("102"),
            trade_notional=Decimal("300"),
            aggressive_buy_notional=Decimal("250"),
        ),
    )


def _liquidation_states() -> tuple[MarketState15s, ...]:
    return (
        _state(0, close=Decimal("100")),
        _state(1, close=Decimal("100")),
        _state(2, close=Decimal("100")),
        _state(3, close=Decimal("100")),
        _state(
            4,
            close=Decimal("100"),
            aggressive_buy_notional=Decimal("250"),
            aggressive_sell_notional=Decimal("50"),
            liquidation_count=1,
            liquidation_notional=Decimal("300"),
        ),
        _state(
            5,
            close=Decimal("102"),
            aggressive_buy_notional=Decimal("250"),
            aggressive_sell_notional=Decimal("50"),
            liquidation_count=1,
            liquidation_notional=Decimal("300"),
        ),
    )


def _state(
    bucket_index: int,
    *,
    close: Decimal,
    high: Decimal | None = None,
    low: Decimal | None = None,
    trade_notional: Decimal = Decimal("1000"),
    aggressive_buy_notional: Decimal | None = None,
    aggressive_sell_notional: Decimal | None = None,
    liquidation_count: int = 0,
    liquidation_notional: Decimal = Decimal("0"),
) -> MarketState15s:
    bucket_start = datetime(2026, 6, 22, 0, 0, tzinfo=UTC) + timedelta(
        seconds=15 * bucket_index
    )
    bucket_end = bucket_start + timedelta(seconds=15)
    buy_notional = (
        trade_notional * Decimal("0.6")
        if aggressive_buy_notional is None
        else aggressive_buy_notional
    )
    sell_notional = (
        trade_notional - buy_notional
        if aggressive_sell_notional is None
        else aggressive_sell_notional
    )
    return MarketState15s(
        schema_version=1,
        exchange="binance-usdm",
        environment="research",
        symbol="BTCUSDT",
        bucket_start=bucket_start,
        bucket_end=bucket_end,
        open_price=close,
        high_price=high if high is not None else close,
        low_price=low if low is not None else close,
        close_price=close,
        trade_count=10,
        trade_notional=trade_notional,
        aggressive_buy_notional=buy_notional,
        aggressive_sell_notional=sell_notional,
        last_bid_price=close - Decimal("0.01"),
        last_ask_price=close + Decimal("0.01"),
        spread=Decimal("0.02"),
        midpoint=close,
        liquidation_count=liquidation_count,
        liquidation_notional=liquidation_notional,
        mark_price=close,
        closed_kline_count=0,
        source_event_count=10,
        first_received_at=bucket_start,
        last_received_at=bucket_end,
    )


def _without_fill_price(state: MarketState15s) -> MarketState15s:
    replacement = object.__new__(MarketState15s)
    changes = {
        "last_bid_price": None,
        "last_ask_price": None,
        "spread": None,
        "midpoint": None,
        "mark_price": None,
        "close_price": None,
    }
    for field in fields(MarketState15s):
        object.__setattr__(
            replacement,
            field.name,
            changes.get(field.name, getattr(state, field.name)),
        )
    return replacement
