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

    impulse_window_buckets: int = 3
    confirmation_buckets: int = 1
    min_return_pct: Decimal = Decimal("0.01")
    min_aggressive_imbalance: Decimal = Decimal("0.40")
    min_notional_intensity: Decimal = Decimal("2")
    cooldown_buckets: int = 0

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
        ):
            if not value.is_finite():
                raise ValueError(f"{name} must be finite")
        if self.min_return_pct <= 0:
            raise ValueError("min_return_pct must be positive")
        if self.min_aggressive_imbalance < 0:
            raise ValueError("min_aggressive_imbalance must be non-negative")
        if self.min_notional_intensity <= 0:
            raise ValueError("min_notional_intensity must be positive")

    def as_dict(self) -> dict[str, object]:
        """Return canonical values suitable for a strategy hash."""

        return {
            "impulse_window_buckets": self.impulse_window_buckets,
            "confirmation_buckets": self.confirmation_buckets,
            "min_return_pct": str(self.min_return_pct),
            "min_aggressive_imbalance": str(self.min_aggressive_imbalance),
            "min_notional_intensity": str(self.min_notional_intensity),
            "cooldown_buckets": self.cooldown_buckets,
        }

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str] | None = None,
    ) -> LiveOrderFlowImpulseProfile:
        """Resolve one profile from ``CML_LIVE_*`` environment variables.

        Missing variables use the production primary-account defaults.  An
        invalid value fails closed before a worker can submit an order.
        """

        values = os.environ if environment is None else environment
        defaults = cls()
        return cls(
            impulse_window_buckets=_read_int(
                values,
                "CML_LIVE_IMPULSE_WINDOW_BUCKETS",
                defaults.impulse_window_buckets,
            ),
            confirmation_buckets=_read_int(
                values,
                "CML_LIVE_CONFIRMATION_BUCKETS",
                defaults.confirmation_buckets,
            ),
            min_return_pct=_read_decimal(
                values,
                "CML_LIVE_MIN_RETURN_PCT",
                defaults.min_return_pct,
            ),
            min_aggressive_imbalance=_read_decimal(
                values,
                "CML_LIVE_MIN_IMBALANCE",
                defaults.min_aggressive_imbalance,
            ),
            min_notional_intensity=_read_decimal(
                values,
                "CML_LIVE_MIN_INTENSITY",
                defaults.min_notional_intensity,
            ),
            cooldown_buckets=_read_int(
                values,
                "CML_LIVE_COOLDOWN_BUCKETS",
                defaults.cooldown_buckets,
            ),
        )


def _read_int(
    environment: Mapping[str, str],
    name: str,
    default: int,
) -> int:
    raw = environment.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw.strip())
    except ValueError as error:
        raise ValueError(f"{name} must be an integer") from error


def _read_decimal(
    environment: Mapping[str, str],
    name: str,
    default: Decimal,
) -> Decimal:
    raw = environment.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return Decimal(raw.strip())
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"{name} must be a decimal") from error


__all__ = ["LiveOrderFlowImpulseProfile"]
