from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.market.closed_candle import ClosedCandle15m
from crypto_momentum_lab.domain.strategy.models import StrategySide
from crypto_momentum_lab.domain.strategy.position_exit import double_bearish_exit_reason


def test_entry_bearish_then_bearish_requires_immediate_exit() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    entry = ClosedCandle15m(
        "BTCUSDT", start, start + timedelta(minutes=15), Decimal("100"), Decimal("99")
    )
    following = ClosedCandle15m(
        "BTCUSDT",
        entry.candle_end,
        entry.candle_end + timedelta(minutes=15),
        Decimal("99"),
        Decimal("98"),
    )
    assert (
        double_bearish_exit_reason(
            opened_at=start + timedelta(minutes=2),
            symbol="BTCUSDT",
            side=StrategySide.LONG,
            entry_candle=entry,
            closed_candle=following,
        )
        == "candle_15m_double_bearish"
    )


def test_entry_bullish_does_not_trigger_double_exit() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    entry = ClosedCandle15m(
        "BTCUSDT", start, start + timedelta(minutes=15), Decimal("99"), Decimal("100")
    )
    following = ClosedCandle15m(
        "BTCUSDT",
        entry.candle_end,
        entry.candle_end + timedelta(minutes=15),
        Decimal("99"),
        Decimal("98"),
    )
    assert (
        double_bearish_exit_reason(
            opened_at=start + timedelta(minutes=2),
            symbol="BTCUSDT",
            side=StrategySide.LONG,
            entry_candle=entry,
            closed_candle=following,
        )
        is None
    )


@pytest.mark.parametrize(
    ("entry_close", "next_close", "offset_minutes"),
    [("100", "98", 15), ("99", "99", 15), ("99", "100", 15), ("99", "98", 30)],
)
def test_double_bearish_requires_adjacent_bearish_candles(
    entry_close: str, next_close: str, offset_minutes: int
) -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    entry = ClosedCandle15m(
        "BTCUSDT",
        start,
        start + timedelta(minutes=15),
        Decimal("100"),
        Decimal(entry_close),
    )
    next_start = start + timedelta(minutes=offset_minutes)
    following = ClosedCandle15m(
        "BTCUSDT",
        next_start,
        next_start + timedelta(minutes=15),
        Decimal("99"),
        Decimal(next_close),
    )
    assert (
        double_bearish_exit_reason(
            opened_at=start + timedelta(minutes=2),
            symbol="BTCUSDT",
            side=StrategySide.LONG,
            entry_candle=entry,
            closed_candle=following,
        )
        is None
    )
