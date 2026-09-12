from pathlib import Path

from crypto_momentum_lab.config import (
    BinanceCredentialRole,
    ResolvedBinanceCredentials,
)
from crypto_momentum_lab.domain.strategy import EntryType
from crypto_momentum_lab.live_rollout.runtime_options import (
    LiveRunOptions,
    resolve_live_runtime_config,
)
from crypto_momentum_lab.strategy_runner.position_exit import PositionExitMode


def _credentials() -> ResolvedBinanceCredentials:
    return ResolvedBinanceCredentials(
        role=BinanceCredentialRole.TRADE,
        api_key_env="BINANCE_TRADE_API_KEY",
        api_secret_env="BINANCE_TRADE_API_SECRET",
        api_key="test-key",
        api_secret="test-secret",
    )


def _options(**overrides: object) -> LiveRunOptions:
    values: dict[str, object] = {
        "database_url": "postgresql+asyncpg://runtime-options",
        "account_label": "primary",
        "strategy": "orderflow_impulse",
        "runtime_manifest": None,
        "impulse_window_buckets": None,
        "confirmation_buckets": None,
        "min_return_pct": None,
        "min_imbalance": None,
        "min_intensity": None,
        "min_notional_5m_vs_30m": None,
        "cooldown_buckets": None,
        "market_environment": "research",
        "market_state_source": "hub",
        "market_state_hub_url": "ws://market-data:8766",
        "market_quote_hub_url": "ws://market-data:8768",
        "market_quote_volume_hub_url": "ws://market-data:8768",
        "market_websocket_url": "wss://fstream.binance.com/market/ws",
        "account_event_hub_url": "ws://execution-account-live:8767",
        "risk_control_hub_url": "ws://execution-account-live:8769",
        "session_id": None,
        "operator": "",
        "lease_owner": None,
        "strategy_config_hash": "",
        "git_commit_hash": "",
        "migration_revision": "",
        "max_runtime_seconds": 3600,
        "poll_interval_seconds": 0.25,
        "checkpoint_every_states": 100,
        "hedge_mode": None,
        "exit_mode": None,
        "take_profit_pct": None,
        "stop_loss_pct": None,
        "entry_long_only": None,
        "entry_leverage": None,
        "margin_type": None,
        "entry_positive_gainer_top_count": None,
        "entry_price_above_ema5": None,
        "entry_price_above_ema10": None,
        "entry_order_type": None,
        "entry_limit_ttl_seconds": None,
        "candle_grace_bars": None,
        "candle_grace_decision_profit_pct": None,
        "candle_grace_profit_pct": None,
        "base_url": "https://fapi.binance.com",
        "entry_policy_compare_only": None,
        "entry_policy_enforce": None,
        "acknowledge_missing_shadow_preflight": False,
        "persist_exchange_operations": None,
    }
    values.update(overrides)
    return LiveRunOptions(**values)  # type: ignore[arg-type]


def test_manual_options_resolve_to_grouped_runtime_config() -> None:
    config = resolve_live_runtime_config(
        _options(entry_order_type=EntryType.LIMIT),
        credentials=_credentials(),
    )

    assert config.identity.session_id == "live-manual"
    assert config.identity.lease_owner == "live-worker"
    assert config.strategy.entry_positive_gainer_top_count == 100
    assert config.execution.exit_mode is PositionExitMode.CANDLE_15M
    assert config.lifecycle.persist_exchange_operations == frozenset(
        {"submit", "cancel"}
    )
    assert config.credentials.api_key == "test-key"


def test_manifest_options_resolve_identity_and_derive_strategy_hash() -> None:
    config = resolve_live_runtime_config(
        _options(
            runtime_manifest=Path("deploy/live-runtime.yaml"),
            account_label="primary",
            strategy="orderflow_impulse",
            entry_order_type=None,
        ),
        credentials=_credentials(),
        environment={
            "CML_CODE_COMMIT": "a" * 40,
        },
    )

    assert config.identity.session_id == "live-primary-v1"
    assert config.identity.lease_owner == "live-worker"
    assert config.identity.git_commit_hash == "a" * 40
    assert len(config.identity.strategy_config_hash) == 64
    assert config.execution.entry_leverage == 5


def test_manifest_option_conflict_is_rejected() -> None:
    options = _options(
        runtime_manifest=Path("deploy/live-runtime.yaml"),
        entry_leverage=7,
    )

    try:
        resolve_live_runtime_config(
            options,
            credentials=_credentials(),
            environment={"CML_CODE_COMMIT": "a" * 40},
        )
    except ValueError as error:
        assert "--entry-leverage" in str(error)
    else:
        raise AssertionError("manifest conflict was not rejected")
