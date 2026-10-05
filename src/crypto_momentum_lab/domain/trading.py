"""Shared trading primitives independent of strategy implementation."""

from enum import StrEnum


class TradeSide(StrEnum):
    LONG = "long"
    SHORT = "short"


class OrderType(StrEnum):
    MARKET = "market"
    LIMIT = "limit"
