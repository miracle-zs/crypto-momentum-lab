from collections.abc import Collection
from dataclasses import dataclass
from decimal import Decimal

from crypto_momentum_lab.domain.strategy import (
    EntryType,
    deterministic_config_hash,
)
from crypto_momentum_lab.live_rollout.profile import LiveOrderFlowImpulseProfile
from crypto_momentum_lab.strategy_runner.position_exit import PositionExitMode
from crypto_momentum_lab.strategy_runner.registry import build_runtime_config

_LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT = 100
_LIVE_ENTRY_PRICE_ABOVE_EMA5 = False
_LIVE_ENTRY_PRICE_ABOVE_EMA10 = False
_LIVE_ENTRY_ORDER_TYPE = EntryType.LIMIT
_LIVE_ENTRY_LIMIT_TTL_SECONDS = 900
_LIVE_ORDERFLOW_PROFILE = LiveOrderFlowImpulseProfile()
_LIVE_MARKET_WEBSOCKET_URL = "wss://fstream.binance.com/market/ws"
_BINANCE_SHARED_REQUEST_PACER_PATH_ENV = "CML_BINANCE_SHARED_REQUEST_PACER_PATH"
_BINANCE_SHARED_COMMAND_PACER_PATH_ENV = (
    "CML_BINANCE_SHARED_COMMAND_REQUEST_PACER_PATH"
)
_DEFAULT_PERSIST_EXCHANGE_OPERATIONS = frozenset({"submit", "cancel"})
_LIVE_STARTUP_BUFFER_LIMIT = 100_000
_LIVE_AUTO_REACQUIRE_LEASE_TTL_SECONDS = 300
_LIVE_LEASE_RENEW_BEFORE_SECONDS = 120
_LIVE_LEASE_HEARTBEAT_INTERVAL_SECONDS = 15.0
_LIVE_RUNTIME_SHUTDOWN_TIMEOUT_SECONDS = 15.0
_PENDING_POSITION_RETRY_DELAYS_SECONDS = (
    0.25,
    0.5,
    1.0,
    2.0,
    4.0,
    8.0,
    16.0,
    32.0,
)
_ORDER_IDENTITY_CONFLICT_MESSAGE = (
    "client order ID is already bound to a different order"
)


def _live_strategy_config(
    profile: LiveOrderFlowImpulseProfile | None = None,
) -> dict[str, object]:
    resolved_profile = profile or _LIVE_ORDERFLOW_PROFILE
    return {
        "candidate_notional": Decimal("100"),
        "candidate_ttl_buckets": 4,
        "order_flow_impulse_impulse_window_buckets": (
            resolved_profile.impulse_window_buckets
        ),
        "order_flow_impulse_confirmation_buckets": (
            resolved_profile.confirmation_buckets
        ),
        "order_flow_impulse_min_return_pct": resolved_profile.min_return_pct,
        "order_flow_impulse_min_aggressive_imbalance": (
            resolved_profile.min_aggressive_imbalance
        ),
        "order_flow_impulse_min_notional_intensity": (
            resolved_profile.min_notional_intensity
        ),
        "order_flow_impulse_min_notional_5m_vs_30m": (
            resolved_profile.min_notional_5m_vs_30m
        ),
        "cooldown_buckets": resolved_profile.cooldown_buckets,
    }


def _live_strategy_config_hash(
    strategy_name: str,
    *,
    profile: LiveOrderFlowImpulseProfile | None = None,
    entry_positive_gainer_top_count: int | None = _LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT,
    require_price_above_ema5: bool = _LIVE_ENTRY_PRICE_ABOVE_EMA5,
    require_price_above_ema10: bool = _LIVE_ENTRY_PRICE_ABOVE_EMA10,
    entry_policy_enforce: bool = False,
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
    if not isinstance(entry_policy_enforce, bool):
        raise TypeError("entry_policy_enforce must be a bool")
    if entry_limit_ttl_seconds < 601:
        raise ValueError("entry_limit_ttl_seconds must be at least 601")
    return deterministic_config_hash(
        {
            "strategy": build_runtime_config(
                strategy_name,
                config=_live_strategy_config(profile),
            ),
            "entry_filter": {
                "entry_positive_gainer_top_count": entry_positive_gainer_top_count,
                "require_price_above_ema5": require_price_above_ema5,
                "require_price_above_ema10": require_price_above_ema10,
                "entry_policy_enforce": entry_policy_enforce,
            },
            "entry_execution": {
                "order_type": entry_order_type.value,
                "limit_ttl_seconds": entry_limit_ttl_seconds,
            },
        }
    )


@dataclass(frozen=True, slots=True)
class LiveRuntimeDatabases:
    execution_database_url: str
    market_database_url: str
    observability_database_url: str


@dataclass(frozen=True, slots=True)
class LiveRuntimeIdentity:
    account_label: str
    strategy_name: str
    session_id: str
    operator: str
    lease_owner: str
    strategy_config_hash: str
    git_commit_hash: str
    migration_revision: str


@dataclass(frozen=True, slots=True)
class LiveRuntimeMarket:
    market_environment: str
    market_state_source: str
    market_state_hub_url: str
    market_quote_hub_url: str
    market_quote_volume_hub_url: str
    market_websocket_url: str
    account_event_hub_url: str
    risk_control_hub_url: str | None


@dataclass(frozen=True, slots=True)
class LiveRuntimeStrategy:
    profile: LiveOrderFlowImpulseProfile
    entry_positive_gainer_top_count: int | None
    require_price_above_ema5: bool
    require_price_above_ema10: bool
    entry_order_type: EntryType
    entry_limit_ttl_seconds: int
    entry_policy_compare_only: bool
    entry_policy_enforce: bool


@dataclass(frozen=True, slots=True)
class LiveRuntimeExecution:
    hedge_mode: bool
    exit_mode: PositionExitMode
    take_profit_pct: Decimal
    stop_loss_pct: Decimal
    entry_long_only: bool
    entry_leverage: int
    margin_type: str
    candle_grace_bars: int
    candle_grace_decision_profit_pct: Decimal
    candle_grace_profit_pct: Decimal


@dataclass(frozen=True, slots=True)
class LiveRuntimeLifecycle:
    max_runtime_seconds: int
    poll_interval_seconds: float
    checkpoint_every_states: int
    persist_exchange_operations: Collection[str] | None
    acknowledge_missing_shadow_preflight: bool


@dataclass(frozen=True, slots=True)
class LiveRuntimeCredentials:
    base_url: str
    api_key: str
    api_secret: str


@dataclass(frozen=True, slots=True)
class LiveRuntimeConfig:
    """Immutable inputs required to assemble one live daemon runtime.

    The composition root owns CLI and environment resolution.  The runtime
    receives this grouped value so its orchestration interface does not grow
    with every newly supported live option.
    """

    databases: LiveRuntimeDatabases
    identity: LiveRuntimeIdentity
    market: LiveRuntimeMarket
    strategy: LiveRuntimeStrategy
    execution: LiveRuntimeExecution
    lifecycle: LiveRuntimeLifecycle
    credentials: LiveRuntimeCredentials


__all__ = [
    "LiveRuntimeConfig",
    "LiveRuntimeCredentials",
    "LiveRuntimeDatabases",
    "LiveRuntimeExecution",
    "LiveRuntimeIdentity",
    "LiveRuntimeLifecycle",
    "LiveRuntimeMarket",
    "LiveRuntimeStrategy",
]
