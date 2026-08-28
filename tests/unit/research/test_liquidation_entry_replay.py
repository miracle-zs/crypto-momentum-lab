import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.strategies.liquidation_cascade.event_study import (
    LiquidationCascadeConfig,
    find_liquidation_cascades,
)

SCRIPT_PATH = (
    Path(__file__).resolve().parents[3]
    / "scripts"
    / "replay_liquidation_entry_variants.py"
)
SPEC = importlib.util.spec_from_file_location(
    "liquidation_entry_replay_script", SCRIPT_PATH
)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)

BASELINE_DETECTION = MODULE.BASELINE_DETECTION
Candidate = MODULE.Candidate
CascadeEvent = MODULE.CascadeEvent
ExitIndex = MODULE.ExitIndex
Entry = MODULE.Entry
OfficialCandle15m = MODULE.OfficialCandle15m
SymbolStates = MODULE.SymbolStates
candidate_entry = MODULE.candidate_entry
detect_events = MODULE.detect_events
gate_events = MODULE.gate_events
simulate_trade = MODULE.simulate_trade


START = datetime(2026, 8, 1, tzinfo=UTC)


def _row(
    index: int,
    *,
    price: float,
    buy: float = 100.0,
    sell: float = 0.0,
    liquidation_count: int = 0,
    liquidation_notional: float = 0.0,
) -> list[str]:
    observed_at = START + timedelta(seconds=15 * index)
    return [
        "TESTUSDT",
        observed_at.isoformat(),
        str(price),
        str(price),
        str(price),
        str(price),
        "10",
        "1000",
        str(buy),
        str(sell),
        str(price - 0.01),
        str(price + 0.01),
        str(price),
        str(liquidation_count),
        str(liquidation_notional),
        str(price),
        "0",
        "",
        "",
        "",
        "",
        "12",
    ]


def _states(rows: list[list[str]]) -> SymbolStates:
    states = SymbolStates(symbol="TESTUSDT")
    for row in rows:
        states.add_row(row)
    return states


def _domain_states(states: SymbolStates) -> tuple[MarketState15s, ...]:
    output = []
    for index, observed_at in enumerate(states.at):
        output.append(
            MarketState15s(
                schema_version=2,
                exchange="binance-usdm",
                environment="research",
                symbol=states.symbol,
                bucket_start=datetime.fromtimestamp(observed_at, UTC),
                bucket_end=datetime.fromtimestamp(observed_at + 15, UTC),
                open_price=Decimal(str(states.open_price[index])),
                high_price=Decimal(str(states.high_price[index])),
                low_price=Decimal(str(states.low_price[index])),
                close_price=Decimal(str(states.close_price[index])),
                trade_count=states.trade_count[index],
                trade_notional=Decimal(str(states.trade_notional[index])),
                aggressive_buy_notional=Decimal(str(states.aggressive_buy[index])),
                aggressive_sell_notional=Decimal(str(states.aggressive_sell[index])),
                last_bid_price=Decimal(str(states.bid[index])),
                last_ask_price=Decimal(str(states.ask[index])),
                spread=Decimal("0.02"),
                midpoint=Decimal(str(states.midpoint[index])),
                liquidation_count=states.liquidation_count[index],
                liquidation_notional=Decimal(str(states.liquidation_notional[index])),
                mark_price=Decimal(str(states.mark_price[index])),
                closed_kline_count=0,
                source_event_count=states.source_event_count[index],
                first_received_at=None,
                last_received_at=None,
            )
        )
    return tuple(output)


def _event(
    index: int = 4,
    *,
    strategy_index: int | None = None,
    segment: int = 0,
) -> CascadeEvent:
    return CascadeEvent(
        index=index,
        strategy_index=index if strategy_index is None else strategy_index,
        segment=segment,
        detected_at=int((START + timedelta(seconds=index * 15)).timestamp()),
        direction="up",
        breakout_level=100.0,
        cluster_move=0.02,
        breakout_distance=0.02,
        liquidation_count=1,
        liquidation_notional=1000.0,
        cluster_trade_count=20,
        cluster_trade_notional=2000.0,
        aggressive_imbalance=1.0,
        confirmation_imbalance=1.0,
    )


def _candidate(family: str, **kwargs: object) -> Candidate:
    return Candidate(
        candidate_id=f"test-{family}",
        family=family,
        detection=BASELINE_DETECTION,
        cooldown_seconds=300,
        **kwargs,
    )


def test_detector_matches_production_event_study_for_baseline() -> None:
    rows = [_row(index, price=100.0) for index in range(4)]
    rows.append(
        _row(
            4,
            price=102.0,
            liquidation_count=1,
            liquidation_notional=1000.0,
        )
    )
    states = _states(rows)

    replayed = detect_events(states)[BASELINE_DETECTION]
    production = find_liquidation_cascades(
        _domain_states(states),
        LiquidationCascadeConfig(
            liquidation_window_buckets=2,
            breakout_window_buckets=4,
            min_liquidation_count=1,
            min_liquidation_notional=Decimal("500"),
            min_price_move_pct=Decimal("0.01"),
            min_aggressive_imbalance=Decimal("0.33"),
            confirmation_buckets=1,
            cooldown_buckets=0,
            forward_horizon_buckets=(1,),
        ),
    )

    assert [(event.detected_at, event.direction) for event in replayed] == [
        (int(event.detected_at.timestamp()), event.direction.value)
        for event in production
    ]


def test_detector_requires_confirmation_bucket_imbalance() -> None:
    rows = [_row(index, price=100.0) for index in range(3)]
    rows.append(_row(3, price=100.0, buy=1000.0, sell=0.0))
    rows.append(
        _row(
            4,
            price=102.0,
            buy=0.0,
            sell=100.0,
            liquidation_count=1,
            liquidation_notional=1000.0,
        )
    )

    assert detect_events(_states(rows))[BASELINE_DETECTION] == []


def test_detector_ignores_liquidation_bucket_without_close_price() -> None:
    rows = [_row(index, price=100.0) for index in range(4)]
    liquidation_only = _row(
        4,
        price=100.0,
        liquidation_count=1,
        liquidation_notional=1000.0,
    )
    liquidation_only[2:6] = ["", "", "", ""]
    rows.extend([liquidation_only, _row(5, price=102.0)])

    states = _states(rows)

    assert len(states.strategy_rows) == 5
    assert detect_events(states)[BASELINE_DETECTION] == []


def test_delayed_continuation_uses_quote_after_condition_bucket() -> None:
    rows = [_row(index, price=100.0) for index in range(4)]
    rows.extend(
        [
            _row(4, price=102.0, liquidation_count=1, liquidation_notional=1000),
            _row(5, price=102.1),
            _row(6, price=102.2),
            _row(7, price=102.3),
        ]
    )
    states = _states(rows)
    exits = ExitIndex.build(states)
    candidate = _candidate(
        "C1",
        delay_buckets=2,
        entry_imbalance=0.33,
    )

    entry = candidate_entry(states, exits, _event(), candidate)

    assert entry is not None
    assert entry.condition_index == 6
    assert entry.quote_index == 7
    assert entry.opened_at == states.at[7] + 15
    assert entry.side == "long"


def test_online_entry_uses_quote_from_condition_bucket() -> None:
    rows = [_row(index, price=100.0) for index in range(4)]
    rows.extend(
        [
            _row(4, price=102.0, liquidation_count=1, liquidation_notional=1000),
            _row(5, price=103.0),
        ]
    )
    states = _states(rows)
    candidate = _candidate("C0")

    entry = candidate_entry(
        states,
        ExitIndex.build(states),
        _event(),
        candidate,
        entry_latency_buckets=0,
    )

    assert entry is not None
    assert entry.condition_index == 4
    assert entry.quote_index == 4
    assert entry.opened_at == states.at[4] + 15
    assert entry.entry_price == states.ask[4]


def test_official_15m_close_is_the_primary_adverse_exit_price() -> None:
    rows = [_row(index, price=100.0) for index in range(80)]
    states = _states(rows)
    exits = ExitIndex.build(
        states,
        official_candles=(
            OfficialCandle15m(
                candle_start=int(START.timestamp()),
                candle_end=int((START + timedelta(minutes=15)).timestamp()),
                open_price=100.0,
                close_price=90.0,
            ),
        ),
    )
    candidate = _candidate("C0")
    entry = Entry(
        condition_index=4,
        quote_index=4,
        opened_at=states.at[4],
        side="long",
        entry_price=states.ask[4],
    )

    trade = simulate_trade(
        states,
        exits,
        _event(),
        candidate,
        entry,
        "train",
    )

    assert trade.close_reason == "first_adverse_15m"
    assert trade.closed_at == int((START + timedelta(minutes=15)).timestamp())
    assert trade.exit_price == 90.0
    assert trade.official_exit_price == 90.0
    assert trade.pnl is not None and trade.pnl < 0


def test_delayed_continuation_dies_after_return_inside_breakout() -> None:
    rows = [_row(index, price=100.0) for index in range(4)]
    rows.extend(
        [
            _row(4, price=102.0, liquidation_count=1, liquidation_notional=1000),
            _row(5, price=99.9),
            _row(6, price=102.2),
            _row(7, price=102.3),
        ]
    )
    states = _states(rows)
    candidate = _candidate(
        "C1",
        delay_buckets=2,
        entry_imbalance=0.0,
    )

    assert candidate_entry(states, ExitIndex.build(states), _event(), candidate) is None


def test_delayed_continuation_counts_only_strategy_eligible_buckets() -> None:
    rows = [_row(index, price=100.0) for index in range(4)]
    rows.append(
        _row(4, price=102.0, liquidation_count=1, liquidation_notional=1000)
    )
    missing_close = _row(5, price=102.05)
    missing_close[2:6] = ["", "", "", ""]
    rows.extend(
        [
            missing_close,
            _row(6, price=102.1),
            _row(7, price=102.2),
            _row(8, price=102.3),
        ]
    )
    states = _states(rows)
    candidate = _candidate("C1", delay_buckets=2, entry_imbalance=0.0)

    entry = candidate_entry(states, ExitIndex.build(states), _event(), candidate)

    assert entry is not None
    assert entry.condition_index == 7
    assert entry.quote_index == 8


def test_failed_breakout_reverses_after_flow_flips() -> None:
    rows = [_row(index, price=100.0) for index in range(4)]
    rows.extend(
        [
            _row(4, price=102.0, liquidation_count=1, liquidation_notional=1000),
            _row(5, price=101.0),
            _row(6, price=99.9, buy=0.0, sell=100.0),
            _row(7, price=99.8),
        ]
    )
    states = _states(rows)
    candidate = _candidate(
        "C2",
        observation_buckets=4,
        exhaustion="flipped",
    )

    entry = candidate_entry(states, ExitIndex.build(states), _event(), candidate)

    assert entry is not None
    assert entry.condition_index == 6
    assert entry.quote_index == 7
    assert entry.side == "short"


def test_current_cooldown_skips_exactly_two_following_buckets() -> None:
    candidate = Candidate(
        candidate_id="baseline",
        family="C0",
        detection=BASELINE_DETECTION,
        cooldown_seconds=30,
        cooldown_buckets=2,
    )
    events = [_event(index=index) for index in (4, 5, 6, 7)]

    assert [event.index for event in gate_events(events, candidate)] == [4, 7]


def test_cooldown_resets_after_a_market_data_gap() -> None:
    candidate = Candidate(
        candidate_id="baseline",
        family="C0",
        detection=BASELINE_DETECTION,
        cooldown_seconds=30,
        cooldown_buckets=2,
    )
    first = _event(index=4)
    after_gap = _event(index=5, segment=1)

    assert gate_events([first, after_gap], candidate) == [first, after_gap]
