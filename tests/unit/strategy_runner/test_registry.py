from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.strategy import RunMode, StrategyRunIdentity
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


def test_registry_builds_orderflow_runtime_strategy() -> None:
    strategy = build_runtime_strategy(
        "orderflow_impulse",
        config={
            "candidate_notional": Decimal("100"),
            "candidate_ttl_buckets": 2,
        },
        identity=_identity("orderflow_impulse"),
    )

    assert strategy.metadata().name == "orderflow_impulse"


def test_registry_uses_two_to_one_liquidation_imbalance_threshold() -> None:
    runtime_config = build_runtime_config("liquidation_cascade", config={})

    assert runtime_config.event_config.min_aggressive_imbalance == Decimal("0.33")


def test_registry_allows_live_orderflow_cooldown_override() -> None:
    runtime_config = build_runtime_config(
        "orderflow_impulse",
        config={"cooldown_buckets": 0},
    )

    assert runtime_config.event_config.cooldown_buckets == 0


def test_registry_allows_orderflow_imbalance_override() -> None:
    runtime_config = build_runtime_config(
        "orderflow_impulse",
        config={
            "order_flow_impulse_min_aggressive_imbalance": Decimal("0.40"),
        },
    )

    assert runtime_config.event_config.min_aggressive_imbalance == Decimal("0.40")


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
