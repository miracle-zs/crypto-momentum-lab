from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.strategy import RunMode, StrategyRunIdentity
from crypto_momentum_lab.strategies.liquidation_cascade import LiquidationCascadeConfig
from crypto_momentum_lab.strategies.order_flow_impulse import OrderFlowImpulseConfig
from crypto_momentum_lab.strategy_runner.registry import (
    StrategyRegistryError,
    build_runtime_config,
    build_runtime_strategy,
    supported_strategy_names,
)


def test_registry_lists_supported_strategy_names() -> None:
    assert supported_strategy_names() == (
        "compression_breakout",
        "orderflow_impulse",
        "liquidation_cascade",
    )


def test_registry_rejects_unknown_strategy() -> None:
    with pytest.raises(StrategyRegistryError, match="unsupported strategy"):
        build_runtime_strategy(
            "unknown",
            config={},
            identity=_identity("unknown"),
        )


def test_registry_fails_closed_when_strategy_config_missing() -> None:

    with pytest.raises(StrategyRegistryError, match="orderflow_impulse configuration is required"):
        build_runtime_strategy(
            "orderflow_impulse",
            config={
                "candidate_notional": Decimal("100"),
                "candidate_ttl_buckets": 2,
            },
            identity=_identity("orderflow_impulse"),
        )
    with pytest.raises(StrategyRegistryError, match="liquidation_cascade configuration is required"):
        build_runtime_config("liquidation_cascade", config={})
    with pytest.raises(StrategyRegistryError, match="compression_breakout configuration is required"):
        build_runtime_config("compression_breakout", config={})


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


def test_registry_uses_liquidation_imbalance_threshold() -> None:
    event_config = LiquidationCascadeConfig(
        liquidation_window_buckets=2,
        breakout_window_buckets=4,
        min_liquidation_count=1,
        min_liquidation_notional=Decimal("10000"),
        min_price_move_pct=Decimal("0.01"),
        min_aggressive_imbalance=Decimal("0.33"),
        confirmation_buckets=1,
        cooldown_buckets=2,
        forward_horizon_buckets=(1,),
    )
    runtime_config = build_runtime_config(
        "liquidation_cascade",
        config={"liquidation_cascade": event_config},
    )

    assert runtime_config.event_config.min_aggressive_imbalance == Decimal("0.33")


def test_registry_allows_account_scoped_orderflow_profile_overrides() -> None:
    runtime_config = build_runtime_config(
        "orderflow_impulse",
        config={
            "order_flow_impulse_impulse_window_buckets": 4,
            "order_flow_impulse_confirmation_buckets": 1,
            "order_flow_impulse_min_return_pct": Decimal("0.01"),
            "order_flow_impulse_min_aggressive_imbalance": Decimal("0.40"),
            "order_flow_impulse_min_notional_intensity": Decimal("2"),
            "order_flow_impulse_min_notional_5m_vs_30m": Decimal("1.50"),
            "cooldown_buckets": 0,
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
