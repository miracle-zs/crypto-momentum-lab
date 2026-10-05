from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from crypto_momentum_lab.domain.risk import RiskConfigSnapshot
from crypto_momentum_lab.domain.strategy import EntryType
from crypto_momentum_lab.domain.strategy.position_exit import PositionExitMode
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
from crypto_momentum_lab.live_rollout.runtime_orchestrator import run_live_daemon
from crypto_momentum_lab.live_rollout.startup_resilience import (
    LiveStartupRetryableError,
)


class InjectedAssemblyFailure(Exception):
    pass


class InjectedRetryableRuntimeFailure(ConnectionResetError):
    pass


def _make_test_config() -> LiveRuntimeConfig:
    profile = LiveOrderFlowImpulseProfile(
        impulse_window_buckets=2,
        confirmation_buckets=1,
        min_return_pct=Decimal("0.0075"),
        min_aggressive_imbalance=Decimal("0.30"),
        min_notional_intensity=Decimal("3.0"),
        min_notional_5m_vs_30m=Decimal("1.25"),
        cooldown_buckets=0,
    )
    return LiveRuntimeConfig(
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
            strategy_config_hash="a" * 64,
            git_commit_hash="b" * 40,
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
        ),
        execution=LiveRuntimeExecution(
            target_notional=Decimal("100.00"),
            hedge_mode=False,
            exit_mode=PositionExitMode.CANDLE_15M,
            entry_long_only=True,
            entry_leverage=1,
            margin_type="isolated",
            candle_grace_bars=0,
            candle_grace_decision_profit_pct=Decimal("0.001"),
            candle_grace_profit_pct=Decimal("0"),
        ),
        lifecycle=LiveRuntimeLifecycle(
            max_runtime_seconds=3600,
            poll_interval_seconds=1.0,
            checkpoint_every_states=10,
            persist_exchange_operations=frozenset({"submit", "cancel"}),
        ),
        credentials=LiveRuntimeCredentials(
            base_url="https://fapi.binance.com",
            api_key="mock-key",
            api_secret="mock-secret",
        ),
    )


@pytest.mark.asyncio
async def test_assembly_failure_preserves_original_exception_and_cleans_up() -> None:
    config = _make_test_config()
    cleaned_up: list[str] = []

    def mock_cleanup() -> None:
        cleaned_up.append("mock_resource")

    def failing_assemble_live_persistence(*, ownership_registry, **kwargs):
        ownership_registry.register("mock_db", mock_cleanup)
        raise InjectedAssemblyFailure("simulated failure during assembly")

    with patch(
        "crypto_momentum_lab.live_rollout.runtime_orchestrator.assemble_live_persistence",
        side_effect=failing_assemble_live_persistence,
    ):
        with pytest.raises(
            InjectedAssemblyFailure, match="simulated failure during assembly"
        ):
            await run_live_daemon(config=config)

    assert cleaned_up == ["mock_resource"], (
        "Registered resources must be cleaned up on assembly failure"
    )


@pytest.mark.asyncio
async def test_startup_failure_is_classified_as_startup_retryable_error() -> None:
    config = _make_test_config()

    with patch(
        "crypto_momentum_lab.live_rollout.runtime_orchestrator.assemble_live_persistence",
        side_effect=InjectedRetryableRuntimeFailure("startup network failure"),
    ):
        with pytest.raises(LiveStartupRetryableError):
            await run_live_daemon(config=config)


@pytest.mark.asyncio
async def test_runtime_failure_is_not_classified_as_startup_retryable_error(
    monkeypatch,
) -> None:
    config = _make_test_config()
    config = replace(
        config,
        strategy=replace(config.strategy, entry_positive_gainer_top_count=None),
    )

    mock_persistence = MagicMock()
    mock_persistence.engines.execution_engine = MagicMock()
    mock_persistence.engines.market_engine = MagicMock()
    mock_persistence.engines.observability_engine = MagicMock()
    mock_persistence.engines.checkpoint_engine = MagicMock()
    mock_persistence.engines.heartbeat_engine = MagicMock()

    mock_persistence.factories.execution_factory = MagicMock()
    mock_persistence.factories.market_factory = MagicMock()
    mock_persistence.factories.observability_factory = MagicMock()
    mock_persistence.factories.heartbeat_factory = MagicMock()

    mock_persistence.repositories.telemetry_repository.save_runtime_events = AsyncMock()
    mock_persistence.repositories.signal_repository.save_signals = AsyncMock()
    mock_persistence.repositories.live_repository.get_session = AsyncMock(
        return_value=None
    )
    mock_persistence.repositories.live_repository.save_session = AsyncMock()
    mock_persistence.repositories.order_read_repository = MagicMock()
    mock_persistence.repositories.order_event_repository = MagicMock()
    mock_persistence.repositories.submission_repository = MagicMock()
    mock_persistence.repositories.checkpoint_repository = MagicMock()

    monkeypatch.setattr(
        "crypto_momentum_lab.live_rollout.runtime_orchestrator.assemble_live_persistence",
        lambda **kwargs: mock_persistence,
    )
    monkeypatch.setattr(
        "crypto_momentum_lab.live_rollout.runtime_orchestrator.LiveRuntimeTelemetry.start",
        AsyncMock(),
    )
    monkeypatch.setattr(
        "crypto_momentum_lab.live_rollout.runtime_orchestrator.assemble_live_quote_volume",
        AsyncMock(),
    )
    monkeypatch.setattr(
        "crypto_momentum_lab.live_rollout.runtime_orchestrator.LiveStrategySignalRecorder.start",
        AsyncMock(),
    )
    risk_config = RiskConfigSnapshot(
        environment="live", account_label="account-1",
        max_open_positions=5, max_gross_notional=Decimal("1000"),
        max_order_notional=Decimal("200"), max_daily_loss=Decimal("100"),
        allow_reduce_only_while_draining=True, created_at=datetime.now(UTC),
    )
    monkeypatch.setattr(
        "crypto_momentum_lab.live_rollout.runtime_orchestrator._latest_risk_config",
        AsyncMock(return_value=risk_config),
    )
    monkeypatch.setattr(
        "crypto_momentum_lab.live_rollout.runtime_orchestrator._load_trading_rules",
        AsyncMock(return_value={}),
    )
    mock_client = MagicMock()
    mock_client.fetch_account_config = AsyncMock(
        return_value=MagicMock(hedge_mode=False)
    )
    monkeypatch.setattr(
        "crypto_momentum_lab.live_rollout.runtime_orchestrator.BinanceUsdMTradeClient",
        lambda **kwargs: mock_client,
    )
    candle_sources_mock = MagicMock()
    candle_sources_mock.closed_candle_feed = MagicMock()
    candle_sources_mock.closed_candle_feed.start = AsyncMock()
    candle_sources_mock.candle_source = MagicMock()
    candle_sources_mock.ema_candle_source = MagicMock()
    monkeypatch.setattr(
        "crypto_momentum_lab.live_rollout.runtime_orchestrator.assemble_live_candle_sources",
        lambda **kwargs: candle_sources_mock,
    )
    monkeypatch.setattr(
        "crypto_momentum_lab.live_rollout.runtime_orchestrator.LiveStrategyDaemon.run",
        AsyncMock(),
    )
    monkeypatch.setattr(
        "crypto_momentum_lab.live_rollout.runtime_orchestrator.LiveAccountEventRuntime.run",
        AsyncMock(),
    )
    monkeypatch.setattr(
        "crypto_momentum_lab.live_rollout.runtime_orchestrator.LiveOrderReconciliation.run_requested",
        AsyncMock(),
    )
    monkeypatch.setattr(
        "crypto_momentum_lab.live_rollout.runtime_orchestrator.LiveEntryRuntime.start",
        lambda self: (None, None),
    )
    mock_exec_runtime = MagicMock()
    mock_exec_runtime.book = MagicMock()
    mock_exec_runtime.coordinator = MagicMock()
    mock_exec_runtime.coordinator.aclose = AsyncMock()
    monkeypatch.setattr(
        "crypto_momentum_lab.live_rollout.runtime_orchestrator.build_live_execution_runtime",
        AsyncMock(return_value=mock_exec_runtime),
    )
    monkeypatch.setattr(
        "crypto_momentum_lab.live_rollout.runtime_orchestrator.LiveDecisionFactSource.restore",
        AsyncMock(),
    )
    monkeypatch.setattr(
        "crypto_momentum_lab.live_rollout.runtime_orchestrator.LiveOrderReconciliation.reconcile_all",
        AsyncMock(),
    )
    monkeypatch.setattr(
        "crypto_momentum_lab.live_rollout.runtime_orchestrator.session_state.session_is_draining",
        AsyncMock(return_value=False),
    )
    monkeypatch.setattr(
        "crypto_momentum_lab.live_rollout.runtime_orchestrator.LiveSessionLifecycle.transition",
        AsyncMock(),
    )
    monkeypatch.setattr(
        "crypto_momentum_lab.live_rollout.runtime_orchestrator._wait_for_durable_market_state_cutover",
        AsyncMock(return_value=datetime.now(tz=UTC)),
    )
    mock_persistence.repositories.live_repository.load_active_approval = AsyncMock(
        side_effect=AssertionError("startup must not require approval")
    )
    mock_persistence.repositories.risk_repository.load_active_lease = AsyncMock(
        side_effect=AssertionError("startup must not require lease")
    )
    mock_persistence.repositories.risk_repository.load_active_halts = AsyncMock(
        return_value=()
    )
    mock_persistence.repositories.order_read_repository.load_unresolved_orders = (
        AsyncMock(return_value=())
    )
    mock_persistence.repositories.checkpoint_repository.load_checkpoint = AsyncMock(
        return_value=None
    )
    monkeypatch.setattr(
        "crypto_momentum_lab.live_rollout.runtime_orchestrator._warm_live_strategy",
        AsyncMock(),
    )
    startup_market_mock = MagicMock()
    startup_market_mock.buffer = MagicMock()
    startup_market_mock.buffer.connection_available = True
    startup_market_mock.hub_source = MagicMock()
    startup_market_mock.task = None
    monkeypatch.setattr(
        "crypto_momentum_lab.live_rollout.runtime_orchestrator.assemble_live_startup_market_buffer",
        lambda **kwargs: startup_market_mock,
    )
    monkeypatch.setattr(
        "crypto_momentum_lab.live_rollout.runtime_orchestrator.build_live_market_state_stream",
        lambda **kwargs: MagicMock(),
    )
    channel_sources_mock = MagicMock()
    channel_sources_mock.quote_source = None
    channel_sources_mock.risk_control_source = None
    channel_sources_mock.account_source = MagicMock()
    channel_sources_mock.stop_all = MagicMock()
    monkeypatch.setattr(
        "crypto_momentum_lab.live_rollout.runtime_orchestrator.assemble_live_channel_sources",
        lambda **kwargs: channel_sources_mock,
    )

    def mock_session_factory(*, supervisor, **kwargs):
        mock_session = AsyncMock()
        mock_session.run.side_effect = InjectedRetryableRuntimeFailure(
            "network dropped during runtime"
        )

        async def _close(*args, **kwargs):
            await supervisor.stop()

        mock_session.close.side_effect = _close
        return mock_session

    monkeypatch.setattr(
        "crypto_momentum_lab.live_rollout.runtime_orchestrator.RuntimeSession",
        mock_session_factory,
    )

    # When session.run raises a transient error, it must NOT be
    # wrapped in LiveStartupRetryableError
    with pytest.raises(
        InjectedRetryableRuntimeFailure, match="network dropped during runtime"
    ):
        await run_live_daemon(config=config)
