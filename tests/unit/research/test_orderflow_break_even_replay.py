import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

SCRIPT_PATH = (
    Path(__file__).resolve().parents[3]
    / "scripts"
    / "replay_orderflow_break_even.py"
)
SPEC = importlib.util.spec_from_file_location(
    "orderflow_break_even_replay_script", SCRIPT_PATH
)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)

Candle = MODULE.Candle
Quote = MODULE.Quote
replay_b1 = MODULE.replay_b1


def _position() -> dict:
    return {
        "position_id": "position-test",
        "symbol": "TESTUSDT",
        "side": "long",
        "opened_at": datetime(2026, 8, 1, 0, 0, tzinfo=UTC),
        "closed_at": None,
        "entry_price": 100.0,
        "quantity": 1.0,
        "entry_fee": 0.0,
        "b0_pnl": 0.0,
        "b0_close_reason": "test",
    }


def _candles(reverse_end: datetime, deadline: datetime) -> list:
    return [
        Candle(
            symbol="TESTUSDT",
            start=reverse_end - timedelta(minutes=15),
            end=reverse_end,
            open=100.0,
            high=101.0,
            low=89.0,
            close=90.0,
        ),
        Candle(
            symbol="TESTUSDT",
            start=deadline - timedelta(minutes=15),
            end=deadline,
            open=90.0,
            high=91.0,
            low=89.0,
            close=90.0,
        ),
    ]


def test_timeout_does_not_wait_for_an_arbitrary_later_quote() -> None:
    reverse_end = datetime(2026, 8, 1, 0, 15, tzinfo=UTC)
    deadline = reverse_end + timedelta(minutes=15)
    result = replay_b1(
        _position(),
        _candles(reverse_end, deadline),
        [
            Quote(at=reverse_end, bid=90.0, ask=91.0),
            Quote(at=deadline + timedelta(hours=2), bid=200.0, ask=201.0),
        ],
        spread=0.0,
        fee_rate=0.0,
    )

    assert result["b1_fill_kind"] == "timeout_official_close_fallback"
    assert result["b1_closed_at"] == deadline
    assert result["b1_exit_price"] == 90.0
    assert result["b1_pnl"] == -10.0


def test_timeout_uses_a_quote_exactly_at_the_deadline() -> None:
    reverse_end = datetime(2026, 8, 1, 0, 15, tzinfo=UTC)
    deadline = reverse_end + timedelta(minutes=15)
    result = replay_b1(
        _position(),
        _candles(reverse_end, deadline),
        [
            Quote(at=reverse_end, bid=90.0, ask=91.0),
            Quote(at=deadline, bid=92.0, ask=93.0),
        ],
        spread=0.0,
        fee_rate=0.0,
    )

    assert result["b1_fill_kind"] == "timeout_market_quote"
    assert result["b1_closed_at"] == deadline
    assert result["b1_exit_price"] == 92.0
    assert result["b1_pnl"] == -8.0


def test_timeout_does_not_use_an_arbitrary_later_candle() -> None:
    reverse_end = datetime(2026, 8, 1, 0, 15, tzinfo=UTC)
    deadline = reverse_end + timedelta(minutes=15)
    later_candle = Candle(
        symbol="TESTUSDT",
        start=deadline + timedelta(hours=2) - timedelta(minutes=15),
        end=deadline + timedelta(hours=2),
        open=90.0,
        high=91.0,
        low=89.0,
        close=90.0,
    )
    result = replay_b1(
        _position(),
        [_candles(reverse_end, deadline)[0], later_candle],
        [Quote(at=reverse_end, bid=90.0, ask=91.0)],
        spread=0.0,
        fee_rate=0.0,
    )

    assert result["b1_status"] == "no_timeout_mark"
    assert result["b1_closed_at"] is None
    assert result["b1_pnl"] is None
