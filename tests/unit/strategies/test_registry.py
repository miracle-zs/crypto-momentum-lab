from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.strategy import RunMode, StrategyRunIdentity
from crypto_momentum_lab.strategies.order_flow_impulse import OrderFlowImpulseConfig
from crypto_momentum_lab.strategies.registry import (
    StrategyRegistryError,
    build_runtime_config,
    build_runtime_strategy,
    supported_strategy_names,
)


def test_registry_lists_supported_strategy_names() -> None:
    assert supported_strategy_names() == ("orderflow_impulse",)


@pytest.mark.parametrize(
    "strategy_name", ["unknown", "compression_breakout", "liquidation_cascade"]
)
def test_registry_rejects_unsupported_strategy(strategy_name: str) -> None:
    with pytest.raises(StrategyRegistryError, match="unsupported strategy"):
        build_runtime_strategy(
            strategy_name,
            config={},
            identity=_identity(strategy_name),
        )


def test_registry_fails_closed_when_strategy_config_missing() -> None:

    with pytest.raises(
        StrategyRegistryError, match="orderflow_impulse configuration is required"
    ):
        build_runtime_strategy(
            "orderflow_impulse",
            config={
                "candidate_notional": Decimal("100"),
                "candidate_ttl_buckets": 2,
            },
            identity=_identity("orderflow_impulse"),
        )


def test_registry_builds_orderflow_runtime_strategy() -> None:
    event_config = OrderFlowImpulseConfig(
        impulse_window_buckets=2,
        baseline_window_buckets=4,
        breakout_window_buckets=4,
        min_return_pct=Decimal("0.0075"),
        min_aggressive_imbalance=Decimal("0.30"),
        min_notional_intensity=Decimal("3.0"),
        confirmation_buckets=1,
        cooldown_buckets=0,
        forward_horizon_buckets=(1,),
        min_notional_5m_vs_30m=Decimal("1.25"),
    )
    strategy = build_runtime_strategy(
        "orderflow_impulse",
        config={
            "order_flow_impulse": event_config,
            "candidate_notional": Decimal("100"),
            "candidate_ttl_buckets": 2,
        },
        identity=_identity("orderflow_impulse"),
    )

    assert strategy.metadata().name == "orderflow_impulse"


def test_registry_preserves_account_scoped_orderflow_profile() -> None:
    runtime_config = build_runtime_config(
        "orderflow_impulse",
        config={
            "order_flow_impulse": OrderFlowImpulseConfig(
                impulse_window_buckets=4,
                baseline_window_buckets=4,
                breakout_window_buckets=4,
                confirmation_buckets=1,
                min_return_pct=Decimal("0.01"),
                min_aggressive_imbalance=Decimal("0.40"),
                min_notional_intensity=Decimal("2"),
                min_notional_5m_vs_30m=Decimal("1.50"),
                cooldown_buckets=0,
                forward_horizon_buckets=(1,),
            ),
        },
    )

    assert runtime_config.event_config.impulse_window_buckets == 4
    assert runtime_config.event_config.confirmation_buckets == 1
    assert runtime_config.event_config.min_return_pct == Decimal("0.01")
    assert runtime_config.event_config.min_aggressive_imbalance == Decimal("0.40")
    assert runtime_config.event_config.min_notional_intensity == Decimal("2")
    assert runtime_config.event_config.min_notional_5m_vs_30m == Decimal("1.50")
    assert runtime_config.event_config.cooldown_buckets == 0


def _identity(strategy_name: str) -> StrategyRunIdentity:
    return StrategyRunIdentity(
        run_id="run-1",
        strategy_name=strategy_name,
        strategy_version="v0",
        config_hash="a" * 64,
        run_mode=RunMode.PAPER,
        code_commit="unknown",
        created_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        source_paths=("memory",),
    )
