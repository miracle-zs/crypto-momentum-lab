"""Backwards compatibility re-export of domain position exit policies.

Canonical location: crypto_momentum_lab.domain.strategy.position_exit
"""

from __future__ import annotations

from crypto_momentum_lab.domain.strategy.position_exit import (
    ClosedCandle15m,
    PositionExitMode,
    PositionExitPolicy,
    first_candle_start_after_entry,
    position_exit_reason,
)

__all__ = [
    "ClosedCandle15m",
    "PositionExitMode",
    "PositionExitPolicy",
    "first_candle_start_after_entry",
    "position_exit_reason",
]
