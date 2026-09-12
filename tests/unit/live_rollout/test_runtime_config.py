from decimal import Decimal

from crypto_momentum_lab.domain.strategy import EntryType
from crypto_momentum_lab.live_rollout.profile import LiveOrderFlowImpulseProfile
from crypto_momentum_lab.live_rollout.runtime_config import (
    LiveRuntimeConfig,
    LiveRuntimeCredentials,
    LiveRuntimeDatabases,
    LiveRuntimeExecution,
    LiveRuntimeIdentity,
    LiveRuntimeLifecycle,
    LiveRuntimeMarket,
    LiveRuntimeStrategy,
)
from crypto_momentum_lab.strategy_runner.position_exit import PositionExitMode


def test_live_runtime_config_keeps_composition_inputs_grouped() -> None:
    profile = LiveOrderFlowImpulseProfile()
    config = LiveRuntimeConfig(
        databases=LiveRuntimeDatabases(
            execution_database_url="postgresql+asyncpg://execution",
            market_database_url="postgresql+asyncpg://market",
            observability_database_url="postgresql+asyncpg://observability",
        ),
        identity=LiveRuntimeIdentity(
            account_label="account-1",
            strategy_name="orderflow_impulse",
            session_id="session-1",
            operator="operator-1",
            lease_owner="worker-1",
            strategy_config_hash="a" * 64,
            git_commit_hash="b" * 40,
            migration_revision="20260911_0040",
        ),
        market=LiveRuntimeMarket(
            market_environment="live",
            market_state_source="hub",
            market_state_hub_url="ws://market-data:8766",
            market_quote_hub_url="ws://market-data:8767",
            market_quote_volume_hub_url="ws://market-data:8768",
            market_websocket_url="wss://fstream.binance.com/market/ws",
            account_event_hub_url="ws://account-events:8769",
            risk_control_hub_url=None,
        ),
        strategy=LiveRuntimeStrategy(
            profile=profile,
            entry_positive_gainer_top_count=10,
            require_price_above_ema5=False,
            require_price_above_ema10=False,
            entry_order_type=EntryType.LIMIT,
            entry_limit_ttl_seconds=900,
            entry_policy_compare_only=False,
            entry_policy_enforce=False,
        ),
        execution=LiveRuntimeExecution(
            hedge_mode=False,
            exit_mode=PositionExitMode.FIXED,
            take_profit_pct=Decimal("0.03"),
            stop_loss_pct=Decimal("0.015"),
            entry_long_only=True,
            entry_leverage=7,
            margin_type="ISOLATED",
            candle_grace_bars=0,
            candle_grace_decision_profit_pct=Decimal("0.001"),
            candle_grace_profit_pct=Decimal("0"),
        ),
        lifecycle=LiveRuntimeLifecycle(
            max_runtime_seconds=3600,
            poll_interval_seconds=1.0,
            checkpoint_every_states=10,
            persist_exchange_operations=frozenset({"submit", "cancel"}),
            acknowledge_missing_shadow_preflight=False,
        ),
        credentials=LiveRuntimeCredentials(
            base_url="https://fapi.binance.com",
            api_key="key",
            api_secret="secret",
        ),
    )

    assert config.identity.session_id == "session-1"
    assert config.market.market_state_source == "hub"
    assert config.strategy.profile is profile
    assert config.execution.take_profit_pct == Decimal("0.03")
    assert config.lifecycle.persist_exchange_operations == frozenset(
        {"submit", "cancel"}
    )
