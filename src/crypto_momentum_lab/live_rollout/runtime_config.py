"""Shared live strategy configuration and approval hash calculation."""

from decimal import Decimal

from crypto_momentum_lab.domain.strategy import (
    EntryType,
    deterministic_config_hash,
)
from crypto_momentum_lab.live_rollout.profile import LiveOrderFlowImpulseProfile
from crypto_momentum_lab.strategies.order_flow_impulse import OrderFlowImpulseConfig
from crypto_momentum_lab.strategies.registry import build_runtime_config

_LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT = 30
_LIVE_ENTRY_PRICE_ABOVE_EMA5 = False
_LIVE_ENTRY_PRICE_ABOVE_EMA10 = False
_LIVE_ENTRY_ORDER_TYPE = EntryType.LIMIT
_LIVE_ENTRY_LIMIT_TTL_SECONDS = 900


def _live_strategy_config(
    profile: LiveOrderFlowImpulseProfile,
) -> dict[str, object]:
    event_config = OrderFlowImpulseConfig(
        impulse_window_buckets=profile.impulse_window_buckets,
        baseline_window_buckets=4,
        breakout_window_buckets=4,
        min_return_pct=profile.min_return_pct,
        min_aggressive_imbalance=profile.min_aggressive_imbalance,
        min_notional_intensity=profile.min_notional_intensity,
        confirmation_buckets=profile.confirmation_buckets,
        cooldown_buckets=profile.cooldown_buckets,
        forward_horizon_buckets=(1,),
        min_notional_5m_vs_30m=profile.min_notional_5m_vs_30m,
    )
    return {
        "candidate_notional": Decimal("100"),
        "candidate_ttl_buckets": 4,
        "order_flow_impulse": event_config,
    }


def _live_strategy_config_hash(
    strategy_name: str,
    *,
    profile: LiveOrderFlowImpulseProfile,
    entry_positive_gainer_top_count: int | None = _LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT,
    require_price_above_ema5: bool = _LIVE_ENTRY_PRICE_ABOVE_EMA5,
    require_price_above_ema10: bool = _LIVE_ENTRY_PRICE_ABOVE_EMA10,
    entry_order_type: EntryType = _LIVE_ENTRY_ORDER_TYPE,
    entry_limit_ttl_seconds: int = _LIVE_ENTRY_LIMIT_TTL_SECONDS,
) -> str:
    if (
        entry_positive_gainer_top_count is not None
        and entry_positive_gainer_top_count <= 0
    ):
        raise ValueError("entry_positive_gainer_top_count must be positive")
    if not isinstance(entry_order_type, EntryType):
        raise TypeError("entry_order_type must be an EntryType")
    if entry_limit_ttl_seconds < 601:
        raise ValueError("entry_limit_ttl_seconds must be at least 601")
    return deterministic_config_hash(
        {
            "strategy": build_runtime_config(
                strategy_name,
                config=_live_strategy_config(profile),
            ),
            "entry_filter": {
                # Keep the approval identity stable across the policy-mode
                # cleanup: the former `enforce` mode was hashed as True.
                "entry_policy_enforce": True,
                "entry_positive_gainer_top_count": entry_positive_gainer_top_count,
                "require_price_above_ema5": require_price_above_ema5,
                "require_price_above_ema10": require_price_above_ema10,
            },
            "entry_execution": {
                "order_type": entry_order_type.value,
                "limit_ttl_seconds": entry_limit_ttl_seconds,
            },
        }
    )
