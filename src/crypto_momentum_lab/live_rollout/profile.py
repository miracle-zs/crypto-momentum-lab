"""Account-scoped strategy parameters for a Live order-flow lane.

The profile is the small interface between deployment configuration and the
runtime strategy builder.  It deliberately contains only deterministic
strategy inputs; account credentials, risk limits, and transport settings
remain outside this module.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation


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

    def as_dict(self) -> dict[str, object]:
        """Return canonical values suitable for a strategy hash."""

        return {
            "impulse_window_buckets": self.impulse_window_buckets,
            "confirmation_buckets": self.confirmation_buckets,
            "min_return_pct": str(self.min_return_pct),
            "min_aggressive_imbalance": str(self.min_aggressive_imbalance),
            "min_notional_intensity": str(self.min_notional_intensity),
            "min_notional_5m_vs_30m": str(self.min_notional_5m_vs_30m),
            "cooldown_buckets": self.cooldown_buckets,
        }

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str] | None = None,
    ) -> LiveOrderFlowImpulseProfile:
        """Resolve one profile from ``CML_LIVE_*`` environment variables.

        All variables are strictly required. Missing or empty variables fail closed.
        """

        values = os.environ if environment is None else environment
        return cls(
            impulse_window_buckets=_read_required_int(
                values,
                "CML_LIVE_IMPULSE_WINDOW_BUCKETS",
            ),
            confirmation_buckets=_read_required_int(
                values,
                "CML_LIVE_CONFIRMATION_BUCKETS",
            ),
            min_return_pct=_read_required_decimal(
                values,
                "CML_LIVE_MIN_RETURN_PCT",
            ),
            min_aggressive_imbalance=_read_required_decimal(
                values,
                "CML_LIVE_MIN_IMBALANCE",
            ),
            min_notional_intensity=_read_required_decimal(
                values,
                "CML_LIVE_MIN_INTENSITY",
            ),
            min_notional_5m_vs_30m=_read_required_decimal(
                values,
                "CML_LIVE_MIN_NOTIONAL_5M_VS_30M",
            ),
            cooldown_buckets=_read_required_int(
                values,
                "CML_LIVE_COOLDOWN_BUCKETS",
            ),
        )


def _read_required_int(
    environment: Mapping[str, str],
    name: str,
) -> int:
    raw = environment.get(name)
    if raw is None or not raw.strip():
        raise ValueError(f"Missing required environment variable: {name}")
    try:
        return int(raw.strip())
    except ValueError as error:
        raise ValueError(f"{name} must be an integer") from error


def _read_required_decimal(
    environment: Mapping[str, str],
    name: str,
) -> Decimal:
    raw = environment.get(name)
    if raw is None or not raw.strip():
        raise ValueError(f"Missing required environment variable: {name}")
    try:
        return Decimal(raw.strip())
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"{name} must be a decimal") from error


__all__ = ["LiveOrderFlowImpulseProfile"]
