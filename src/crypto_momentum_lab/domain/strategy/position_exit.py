"""Exit a position on an opposing completed 15-minute candle."""

from datetime import datetime, timedelta
from enum import StrEnum

from crypto_momentum_lab.domain.market.closed_candle import ClosedCandle15m
from crypto_momentum_lab.domain.strategy.models import StrategySide


class PositionExitMode(StrEnum):
    CANDLE_15M = "candle_15m"


def first_candle_start_after_entry(opened_at: datetime) -> datetime:
    """The entry candle is observation-only; start with the next candle."""
    candle_start = opened_at.replace(
        minute=opened_at.minute - opened_at.minute % 15,
        second=0,
        microsecond=0,
    )
    return candle_start + timedelta(minutes=15)


def double_bearish_exit_reason(
    *,
    opened_at: datetime,
    symbol: str,
    side: StrategySide,
    entry_candle: ClosedCandle15m,
    closed_candle: ClosedCandle15m,
) -> str | None:
    """Identify the first two consecutive bearish candles for a long entry."""
    next_start = first_candle_start_after_entry(opened_at)
    if (
        side is StrategySide.LONG
        and entry_candle.symbol == symbol
        and closed_candle.symbol == symbol
        and entry_candle.candle_end == next_start
        and closed_candle.candle_start == next_start
        and entry_candle.close_price < entry_candle.open_price
        and closed_candle.close_price < closed_candle.open_price
    ):
        return "candle_15m_double_bearish"
    return None


def position_exit_reason(
    *,
    held_until: datetime,
    opened_at: datetime,
    symbol: str,
    side: StrategySide,
    closed_candle: ClosedCandle15m | None,
) -> str | None:
    if (
        closed_candle is None
        or closed_candle.symbol != symbol
        or closed_candle.candle_start < first_candle_start_after_entry(opened_at)
        or closed_candle.candle_end > held_until
    ):
        return None
    if (
        side is StrategySide.LONG
        and closed_candle.close_price < closed_candle.open_price
    ):
        return "candle_15m_bearish"
    if (
        side is StrategySide.SHORT
        and closed_candle.close_price > closed_candle.open_price
    ):
        return "candle_15m_bullish"
    return None
