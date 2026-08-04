from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum

from crypto_momentum_lab.domain.strategy import StrategySide


class PositionExitMode(StrEnum):
    FIXED = "fixed"
    CANDLE_15M = "candle_15m"


@dataclass(frozen=True, slots=True)
class PositionExitPolicy:
    take_profit_pct: Decimal = Decimal("0.02")
    stop_loss_pct: Decimal = Decimal("0.01")
    max_holding_seconds: int = 1200
    mode: PositionExitMode = PositionExitMode.FIXED

    def __post_init__(self) -> None:
        if not isinstance(self.mode, PositionExitMode):
            object.__setattr__(self, "mode", PositionExitMode(self.mode))
        if self.take_profit_pct <= 0:
            raise ValueError("take_profit_pct must be positive")
        if self.stop_loss_pct <= 0:
            raise ValueError("stop_loss_pct must be positive")
        if self.max_holding_seconds <= 0:
            raise ValueError("max_holding_seconds must be positive")


@dataclass(frozen=True, slots=True)
class ClosedCandle15m:
    symbol: str
    candle_start: datetime
    candle_end: datetime
    open_price: Decimal
    close_price: Decimal


def position_exit_reason(
    *,
    gross_return: Decimal,
    held_until: datetime,
    opened_at: datetime,
    symbol: str,
    side: StrategySide,
    policy: PositionExitPolicy,
    closed_candle: ClosedCandle15m | None,
) -> str | None:
    if policy.mode is PositionExitMode.CANDLE_15M:
        if (
            closed_candle is not None
            and closed_candle.symbol == symbol
            and opened_at < closed_candle.candle_end <= held_until
        ):
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
    else:
        if gross_return >= policy.take_profit_pct:
            return "take_profit"
        if gross_return <= -policy.stop_loss_pct:
            return "stop_loss"
    if held_until >= opened_at + timedelta(seconds=policy.max_holding_seconds):
        return "max_holding_period"
    return None
