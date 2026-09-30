"""Contracts shared by market-state producers and persistence adapters."""

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class RuntimeStateSequenceRange:
    minimum: int | None = None
    maximum: int | None = None


@dataclass(frozen=True, slots=True)
class RuntimeStateCursor:
    """Exclusive (bucket_start, symbol) cursor for durable market-state reads."""

    bucket_start: datetime | None = None
    symbol: str | None = None
