"""Account-scoped strategy parameters for a Live order-flow lane.

The profile is the small interface between deployment configuration and the
runtime strategy builder.  It deliberately contains only deterministic
strategy inputs; account credentials, risk limits, and transport settings
remain outside this module.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class LiveOrderFlowImpulseProfile:
    """Validated per-account parameters for ``orderflow_impulse``."""

    impulse_window_buckets: int
    confirmation_buckets: int
    min_return_pct: Decimal
    min_aggressive_imbalance: Decimal
    min_notional_intensity: Decimal
    min_notional_5m_vs_30m: Decimal
    cooldown_buckets: int

    def __post_init__(self) -> None:
        if self.impulse_window_buckets <= 1:
            raise ValueError("impulse_window_buckets must be greater than 1")
        if self.confirmation_buckets <= 0:
            raise ValueError("confirmation_buckets must be positive")
        if self.cooldown_buckets < 0:
            raise ValueError("cooldown_buckets must be non-negative")
        for name, value in (
            ("min_return_pct", self.min_return_pct),
            ("min_aggressive_imbalance", self.min_aggressive_imbalance),
            ("min_notional_intensity", self.min_notional_intensity),
            ("min_notional_5m_vs_30m", self.min_notional_5m_vs_30m),
        ):
            if not value.is_finite():
                raise ValueError(f"{name} must be finite")
        if self.min_return_pct <= 0:
            raise ValueError("min_return_pct must be positive")
        if self.min_aggressive_imbalance < 0:
            raise ValueError("min_aggressive_imbalance must be non-negative")
        if self.min_notional_intensity <= 0:
            raise ValueError("min_notional_intensity must be positive")
        if self.min_notional_5m_vs_30m < 0:
            raise ValueError("min_notional_5m_vs_30m must be non-negative")

__all__ = ["LiveOrderFlowImpulseProfile"]
