from dataclasses import replace
from decimal import Decimal, InvalidOperation
from typing import Protocol

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import (
    StrategyCheckpoint,
    StrategyDataRequirement,
    StrategyDecision,
    StrategyMetadata,
    StrategyRunIdentity,
)
from crypto_momentum_lab.strategies.compression_breakout import (
    CompressionBreakoutConfig,
    CompressionBreakoutRuntimeConfig,
    CompressionBreakoutRuntimeStrategy,
)
from crypto_momentum_lab.strategies.liquidation_cascade import (
    LiquidationCascadeConfig,
    LiquidationCascadeRuntimeConfig,
    LiquidationCascadeRuntimeStrategy,
)
from crypto_momentum_lab.strategies.order_flow_impulse import (
    OrderFlowImpulseConfig,
    OrderFlowImpulseRuntimeConfig,
    OrderFlowImpulseRuntimeStrategy,
)


class RuntimeStrategyProtocol(Protocol):
    def metadata(self) -> StrategyMetadata:
        pass

    def required_data(self) -> StrategyDataRequirement:
        pass

    def restore_checkpoint(self, checkpoint: StrategyCheckpoint) -> None:
        pass

    def on_market_state(self, state: MarketState15s) -> StrategyDecision:
        pass

    def checkpoint(
        self,
        *,
        include_market_state_buffers: bool = True,
    ) -> StrategyCheckpoint:
        pass

    def warm_market_state(self, state: MarketState15s) -> None:
        pass


class StrategyRegistryError(ValueError):
    pass


type RuntimeConfig = (
    CompressionBreakoutRuntimeConfig
    | OrderFlowImpulseRuntimeConfig
    | LiquidationCascadeRuntimeConfig
)


def supported_strategy_names() -> tuple[str, ...]:
    return (
        "compression_breakout",
        "orderflow_impulse",
        "liquidation_cascade",
    )


def build_runtime_config(
    strategy_name: str,
    *,
    config: dict[str, object],
) -> RuntimeConfig:
    candidate_notional = _optional_decimal(config.get("candidate_notional"))
    candidate_ttl_buckets = _int_value(
        config.get("candidate_ttl_buckets"),
        default=4,
        field_name="candidate_ttl_buckets",
    )
    if strategy_name == "compression_breakout":
        signal_interval_seconds = _int_value(
            config.get("signal_interval_seconds"),
            default=300,
            field_name="signal_interval_seconds",
        )
        event_config = config.get("compression_breakout")
        if event_config is None:
            event_config = _default_compression_config()
        if not isinstance(event_config, CompressionBreakoutConfig):
            raise StrategyRegistryError("compression_breakout config is invalid")
        return CompressionBreakoutRuntimeConfig(
            event_config=event_config,
            candidate_notional=candidate_notional,
            candidate_ttl_buckets=candidate_ttl_buckets,
            signal_interval_seconds=signal_interval_seconds,
        )
    if strategy_name == "orderflow_impulse":
        event_config = config.get("order_flow_impulse")
        if event_config is None:
            event_config = _default_order_flow_config()
        if not isinstance(event_config, OrderFlowImpulseConfig):
            raise StrategyRegistryError("orderflow_impulse config is invalid")
        event_config = _replace_order_flow_overrides(event_config, config)
        return OrderFlowImpulseRuntimeConfig(
            event_config=event_config,
            candidate_notional=candidate_notional,
            candidate_ttl_buckets=candidate_ttl_buckets,
        )
    if strategy_name == "liquidation_cascade":
        event_config = config.get("liquidation_cascade")
        if event_config is None:
            event_config = _default_liquidation_config()
        if not isinstance(event_config, LiquidationCascadeConfig):
            raise StrategyRegistryError("liquidation_cascade config is invalid")
        return LiquidationCascadeRuntimeConfig(
            event_config=event_config,
            candidate_notional=candidate_notional,
            candidate_ttl_buckets=candidate_ttl_buckets,
        )
    raise StrategyRegistryError(f"unsupported strategy: {strategy_name}")


def build_runtime_strategy(
    strategy_name: str,
    *,
    config: dict[str, object],
    identity: StrategyRunIdentity,
) -> RuntimeStrategyProtocol:
    runtime_config = build_runtime_config(strategy_name, config=config)
    if isinstance(runtime_config, CompressionBreakoutRuntimeConfig):
        return CompressionBreakoutRuntimeStrategy(
            config=runtime_config,
            identity=identity,
        )
    if isinstance(runtime_config, OrderFlowImpulseRuntimeConfig):
        return OrderFlowImpulseRuntimeStrategy(
            config=runtime_config,
            identity=identity,
        )
    return LiquidationCascadeRuntimeStrategy(
        config=runtime_config,
        identity=identity,
    )


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


def _default_compression_config() -> CompressionBreakoutConfig:
    return CompressionBreakoutConfig(
        compression_window_buckets=20,
        max_range_width_pct=Decimal("0.025"),
        min_breakout_pct=Decimal("0.003"),
        acceptance_buckets=1,
        cooldown_buckets=12,
        forward_horizon_buckets=(1, 3, 6, 12),
    )


def _default_order_flow_config() -> OrderFlowImpulseConfig:
    return OrderFlowImpulseConfig(
        impulse_window_buckets=3,
        baseline_window_buckets=4,
        breakout_window_buckets=4,
        min_return_pct=Decimal("0.01"),
        min_aggressive_imbalance=Decimal("0.50"),
        min_notional_intensity=Decimal("2"),
        confirmation_buckets=1,
        cooldown_buckets=2,
        forward_horizon_buckets=(1,),
    )


def _replace_order_flow_overrides(
    event_config: OrderFlowImpulseConfig,
    config: dict[str, object],
) -> OrderFlowImpulseConfig:
    """Apply deterministic deployment overrides to one event config."""

    return replace(
        event_config,
        impulse_window_buckets=_int_value(
            config.get("order_flow_impulse_impulse_window_buckets"),
            default=event_config.impulse_window_buckets,
            field_name="impulse_window_buckets",
        ),
        confirmation_buckets=_int_value(
            config.get("order_flow_impulse_confirmation_buckets"),
            default=event_config.confirmation_buckets,
            field_name="confirmation_buckets",
        ),
        min_return_pct=_decimal_value(
            config.get("order_flow_impulse_min_return_pct"),
            default=event_config.min_return_pct,
            field_name="min_return_pct",
        ),
        min_aggressive_imbalance=_decimal_value(
            config.get("order_flow_impulse_min_aggressive_imbalance"),
            default=event_config.min_aggressive_imbalance,
            field_name="min_aggressive_imbalance",
        ),
        min_notional_intensity=_decimal_value(
            config.get("order_flow_impulse_min_notional_intensity"),
            default=event_config.min_notional_intensity,
            field_name="min_notional_intensity",
        ),
        cooldown_buckets=_int_value(
            config.get("cooldown_buckets"),
            default=event_config.cooldown_buckets,
            field_name="cooldown_buckets",
        ),
    )


def _decimal_value(
    value: object,
    *,
    default: Decimal,
    field_name: str,
) -> Decimal:
    if value is None:
        return default
    try:
        decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise StrategyRegistryError(f"{field_name} is invalid") from error
    if not decimal_value.is_finite():
        raise StrategyRegistryError(f"{field_name} must be finite")
    return decimal_value


def _default_liquidation_config() -> LiquidationCascadeConfig:
    return LiquidationCascadeConfig(
        liquidation_window_buckets=2,
        breakout_window_buckets=4,
        min_liquidation_count=1,
        min_liquidation_notional=Decimal("500"),
        min_price_move_pct=Decimal("0.01"),
        min_aggressive_imbalance=Decimal("0.33"),
        confirmation_buckets=1,
        cooldown_buckets=2,
        forward_horizon_buckets=(1,),
    )
