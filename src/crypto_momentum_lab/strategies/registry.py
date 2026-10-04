from decimal import Decimal

from crypto_momentum_lab.domain.strategy import (
    StrategyRunIdentity,
)
from crypto_momentum_lab.domain.strategy.runtime import RuntimeStrategy
from crypto_momentum_lab.strategies.order_flow_impulse import (
    OrderFlowImpulseConfig,
    OrderFlowImpulseRuntimeConfig,
    OrderFlowImpulseRuntimeStrategy,
)


class StrategyRegistryError(ValueError):
    pass


type RuntimeConfig = OrderFlowImpulseRuntimeConfig


def supported_strategy_names() -> tuple[str, ...]:
    return ("orderflow_impulse",)


def build_runtime_config(
    strategy_name: str,
    *,
    config: dict[str, object],
) -> RuntimeConfig:
    if strategy_name != "orderflow_impulse":
        raise StrategyRegistryError(f"unsupported strategy: {strategy_name}")
    candidate_notional = _optional_decimal(config.get("candidate_notional"))
    candidate_ttl_buckets = _int_value(
        config.get("candidate_ttl_buckets"),
        default=4,
        field_name="candidate_ttl_buckets",
    )
    event_config = config.get("order_flow_impulse")
    if event_config is None:
        raise StrategyRegistryError("orderflow_impulse configuration is required")
    if not isinstance(event_config, OrderFlowImpulseConfig):
        raise StrategyRegistryError("orderflow_impulse config is invalid")
    return OrderFlowImpulseRuntimeConfig(
        event_config=event_config,
        candidate_notional=candidate_notional,
        candidate_ttl_buckets=candidate_ttl_buckets,
    )


def build_runtime_strategy(
    strategy_name: str,
    *,
    config: dict[str, object],
    identity: StrategyRunIdentity,
) -> RuntimeStrategy:
    runtime_config = build_runtime_config(strategy_name, config=config)
    return OrderFlowImpulseRuntimeStrategy(config=runtime_config, identity=identity)


def _optional_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int | str):
        return Decimal(str(value))
    raise StrategyRegistryError("candidate_notional is invalid")


def _int_value(value: object, *, default: int, field_name: str) -> int:
    if value is None:
        return default
    if isinstance(value, int):
        return value
    raise StrategyRegistryError(f"{field_name} is invalid")
