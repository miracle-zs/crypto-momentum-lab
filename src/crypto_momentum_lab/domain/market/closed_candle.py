"""Market facts shared by feeds, caches, and position-exit policies."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class ClosedCandle15m:
    """One immutable, fully closed fifteen-minute market candle."""

    symbol: str
    candle_start: datetime
    candle_end: datetime
    open_price: Decimal
    close_price: Decimal

    def __post_init__(self) -> None:
        if not self.symbol.strip():
            raise ValueError("symbol must not be empty")
        if self.candle_start.tzinfo is None or self.candle_end.tzinfo is None:
            raise ValueError("candle timestamps must be timezone-aware (UTC)")
        if self.candle_end <= self.candle_start:
            raise ValueError("candle_end must be greater than candle_start")
        duration = self.candle_end - self.candle_start
        if duration != timedelta(minutes=15):
            raise ValueError(
                f"ClosedCandle15m duration must be exactly 15 minutes, got {duration}"
            )
        if self.open_price <= Decimal("0") or self.close_price <= Decimal("0"):
            raise ValueError("open_price and close_price must be positive")

    @property
    def duration(self) -> timedelta:
        return self.candle_end - self.candle_start
