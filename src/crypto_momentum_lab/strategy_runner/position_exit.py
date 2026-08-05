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
    minimum_holding_seconds: int = 0
    candle_confirmation_count: int = 1

    def __post_init__(self) -> None:
        if not isinstance(self.mode, PositionExitMode):
            object.__setattr__(self, "mode", PositionExitMode(self.mode))
        if self.take_profit_pct <= 0:
            raise ValueError("take_profit_pct must be positive")
        if self.stop_loss_pct <= 0:
            raise ValueError("stop_loss_pct must be positive")
        if self.max_holding_seconds <= 0:
            raise ValueError("max_holding_seconds must be positive")
        if self.minimum_holding_seconds < 0:
            raise ValueError("minimum_holding_seconds must not be negative")
        if self.candle_confirmation_count <= 0:
            raise ValueError("candle_confirmation_count must be positive")


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
    closed_candles: tuple[ClosedCandle15m, ...] = (),
) -> str | None:
    if policy.mode is PositionExitMode.CANDLE_15M:
        candle_reason = _candle_exit_reason(
            closed_candle=closed_candle,
            closed_candles=closed_candles,
            held_until=held_until,
            opened_at=opened_at,
            symbol=symbol,
            side=side,
            policy=policy,
        )
        if candle_reason is not None:
            return candle_reason
    else:
        if gross_return >= policy.take_profit_pct:
            return "take_profit"
        if gross_return <= -policy.stop_loss_pct:
            return "stop_loss"
    if held_until >= opened_at + timedelta(seconds=policy.max_holding_seconds):
        return "max_holding_period"
    return None


def _candle_exit_reason(
    *,
    closed_candle: ClosedCandle15m | None,
    closed_candles: tuple[ClosedCandle15m, ...],
    held_until: datetime,
    opened_at: datetime,
    symbol: str,
    side: StrategySide,
    policy: PositionExitPolicy,
) -> str | None:
    # A candle exit is evaluated only when a new completed candle arrives.
    # Keeping prior candles here is needed for the two-confirmation variant.
    if closed_candle is None or closed_candle.symbol != symbol:
        return None
    candles = list(closed_candles)
    if not candles or candles[-1].candle_start != closed_candle.candle_start:
        candles.append(closed_candle)
    minimum_end = opened_at + timedelta(
        seconds=policy.minimum_holding_seconds
    )
    eligible = [
        candle
        for candle in candles
        if (
            candle.symbol == symbol
            and opened_at < candle.candle_end <= held_until
            and candle.candle_end >= minimum_end
        )
    ]
    count = policy.candle_confirmation_count
    if len(eligible) < count:
        return None
    confirmed = tuple(eligible[-count:])
    if not _consecutive_candles(confirmed):
        return None
    if side is StrategySide.LONG and all(
        candle.close_price < candle.open_price for candle in confirmed
    ):
        return _candle_reason(
            direction="bearish",
            policy=policy,
        )
    if side is StrategySide.SHORT and all(
        candle.close_price > candle.open_price for candle in confirmed
    ):
        return _candle_reason(
            direction="bullish",
            policy=policy,
        )
    return None


def _consecutive_candles(candles: tuple[ClosedCandle15m, ...]) -> bool:
    return all(
        current.candle_start
        == previous.candle_start + timedelta(minutes=15)
        for previous, current in zip(candles, candles[1:], strict=False)
    )


def _candle_reason(
    *,
    direction: str,
    policy: PositionExitPolicy,
) -> str:
    if policy.candle_confirmation_count > 1:
        return f"candle_15m_{direction}_{policy.candle_confirmation_count}confirm"
    if policy.minimum_holding_seconds > 0:
        return f"candle_15m_{direction}_after_min_hold"
    return f"candle_15m_{direction}"
