
import pytest

from crypto_momentum_lab.live_rollout.profile import LiveOrderFlowImpulseProfile


def test_profile_resolves_account_specific_environment_values() -> None:
    profile = LiveOrderFlowImpulseProfile.from_environment(
        {
            "CML_LIVE_IMPULSE_WINDOW_BUCKETS": "4",
            "CML_LIVE_CONFIRMATION_BUCKETS": "1",
            "CML_LIVE_MIN_RETURN_PCT": "0.01",
            "CML_LIVE_MIN_IMBALANCE": "0.40",
            "CML_LIVE_MIN_INTENSITY": "2",
            "CML_LIVE_MIN_NOTIONAL_5M_VS_30M": "1.50",
            "CML_LIVE_COOLDOWN_BUCKETS": "0",
        }
    )

    assert profile.as_dict() == {
        "impulse_window_buckets": 4,
        "confirmation_buckets": 1,
        "min_return_pct": "0.01",
        "min_aggressive_imbalance": "0.40",
        "min_notional_intensity": "2",
        "min_notional_5m_vs_30m": "1.50",
        "cooldown_buckets": 0,
    }


def test_profile_fails_closed_when_environment_variable_missing() -> None:
    with pytest.raises(ValueError, match="Missing required environment variable"):
        LiveOrderFlowImpulseProfile.from_environment({})


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("CML_LIVE_IMPULSE_WINDOW_BUCKETS", "1", "impulse_window"),
        ("CML_LIVE_CONFIRMATION_BUCKETS", "0", "confirmation"),
        ("CML_LIVE_MIN_RETURN_PCT", "0", "min_return"),
        ("CML_LIVE_MIN_IMBALANCE", "-0.1", "min_aggressive_imbalance"),
        ("CML_LIVE_MIN_INTENSITY", "0", "min_notional_intensity"),
        (
            "CML_LIVE_MIN_NOTIONAL_5M_VS_30M",
            "-0.1",
            "min_notional_5m_vs_30m",
        ),
        ("CML_LIVE_COOLDOWN_BUCKETS", "-1", "cooldown"),
    ],
)
def test_profile_rejects_invalid_values(field: str, value: str, message: str) -> None:
    base_env = {
        "CML_LIVE_IMPULSE_WINDOW_BUCKETS": "2",
        "CML_LIVE_CONFIRMATION_BUCKETS": "1",
        "CML_LIVE_MIN_RETURN_PCT": "0.0075",
        "CML_LIVE_MIN_IMBALANCE": "0.30",
        "CML_LIVE_MIN_INTENSITY": "3.0",
        "CML_LIVE_MIN_NOTIONAL_5M_VS_30M": "1.25",
        "CML_LIVE_COOLDOWN_BUCKETS": "0",
    }
    base_env[field] = value
    with pytest.raises(ValueError, match=message):
        LiveOrderFlowImpulseProfile.from_environment(base_env)
