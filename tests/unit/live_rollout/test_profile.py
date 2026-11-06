from decimal import Decimal

import pytest

from crypto_momentum_lab.live_rollout.profile import LiveOrderFlowImpulseProfile


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("impulse_window_buckets", 1, "impulse_window"),
        ("confirmation_buckets", 0, "confirmation"),
        ("min_return_pct", Decimal("0"), "min_return"),
        ("min_aggressive_imbalance", Decimal("-0.1"), "min_aggressive_imbalance"),
        ("min_notional_intensity", Decimal("0"), "min_notional_intensity"),
        ("min_notional_5m_vs_30m", Decimal("-0.1"), "min_notional_5m_vs_30m"),
        ("cooldown_buckets", -1, "cooldown"),
    ],
)
def test_profile_rejects_invalid_values(
    field: str, value: object, message: str
) -> None:
    inputs = {
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": Decimal("0.0075"),
        "min_aggressive_imbalance": Decimal("0.30"),
        "min_notional_intensity": Decimal("3.0"),
        "min_notional_5m_vs_30m": Decimal("1.25"),
        "cooldown_buckets": 0,
    }
    inputs[field] = value
    with pytest.raises(ValueError, match=message):
        LiveOrderFlowImpulseProfile(**inputs)
