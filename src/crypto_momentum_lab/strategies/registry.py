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

    def clear_market_state_buffers(self) -> None:
        pass

    def reset_symbol(self, symbol: str) -> None:
        pass


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
        if "order_flow_impulse_impulse_window_buckets" in config:
            try:
                event_config = OrderFlowImpulseConfig(
                    impulse_window_buckets=_int_value(
                        config.get("order_flow_impulse_impulse_window_buckets"),
                        default=0,
                        field_name="impulse_window_buckets",
                    ),
                    baseline_window_buckets=_int_value(
                        config.get("order_flow_impulse_baseline_window_buckets"),
                        default=4,
                        field_name="baseline_window_buckets",
                    ),
                    breakout_window_buckets=_int_value(
                        config.get("order_flow_impulse_breakout_window_buckets"),
                        default=4,
                        field_name="breakout_window_buckets",
                    ),
                    min_return_pct=_decimal_value(
                        config.get("order_flow_impulse_min_return_pct"),
                        default=Decimal("0"),
                        field_name="min_return_pct",
                    ),
                    min_aggressive_imbalance=_decimal_value(
                        config.get("order_flow_impulse_min_aggressive_imbalance"),
                        default=Decimal("0"),
                        field_name="min_aggressive_imbalance",
                    ),
                    min_notional_intensity=_decimal_value(
                        config.get("order_flow_impulse_min_notional_intensity"),
                        default=Decimal("0"),
                        field_name="min_notional_intensity",
                    ),
                    confirmation_buckets=_int_value(
                        config.get("order_flow_impulse_confirmation_buckets"),
                        default=0,
                        field_name="confirmation_buckets",
                    ),
                    cooldown_buckets=_int_value(
                        config.get("cooldown_buckets"),
                        default=0,
                        field_name="cooldown_buckets",
                    ),
                    forward_horizon_buckets=tuple(
                        config.get("order_flow_impulse_forward_horizon_buckets") or (1,)
                    ),
                    min_notional_5m_vs_30m=_decimal_value(
                        config.get("order_flow_impulse_min_notional_5m_vs_30m"),
                        default=Decimal("0"),
                        field_name="min_notional_5m_vs_30m",
                    ),
                )
            except Exception as error:
                raise StrategyRegistryError(
                    f"orderflow_impulse config is invalid: {error}"
                ) from error
        else:
            raise StrategyRegistryError("orderflow_impulse configuration is required")
    if not isinstance(event_config, OrderFlowImpulseConfig):
        raise StrategyRegistryError("orderflow_impulse config is invalid")
    event_config = _replace_order_flow_overrides(event_config, config)
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
) -> RuntimeStrategyProtocol:
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
        min_notional_5m_vs_30m=_decimal_value(
            config.get("order_flow_impulse_min_notional_5m_vs_30m"),
            default=event_config.min_notional_5m_vs_30m,
            field_name="min_notional_5m_vs_30m",
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
