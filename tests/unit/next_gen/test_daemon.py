from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock

import pytest

from crypto_momentum_lab.next_gen.client import BinanceUserDataStream
from crypto_momentum_lab.next_gen.daemon import NextGenDaemon, run_nextgen_daemon
from crypto_momentum_lab.next_gen.engine import AccountSnapshot, PositionSnapshot
from crypto_momentum_lab.next_gen.models import SystemMode
from crypto_momentum_lab.next_gen.persistence import InMemoryTradingRepository
from tests.unit.next_gen.factories import risk_config
from tests.unit.next_gen.test_strategy_engine import exchange_info


@pytest.fixture(autouse=True)
def fake_market_feed(monkeypatch, tmp_path):
    monkeypatch.setenv("CML_LOCAL_HEALTH_DIR", str(tmp_path / "health"))

    class Feed:
        def __init__(self, *, symbols, on_connection_change, **kwargs):
            self.symbols = symbols
            self.on_connection_change = on_connection_change

        async def run(self, stop_event):
            self.on_connection_change(True)
            await stop_event.wait()

        async def close(self):
            self.on_connection_change(False)

    monkeypatch.setattr("crypto_momentum_lab.next_gen.market_feed.MarketDataFeed", Feed)


@pytest.fixture(autouse=True)
def fake_closed_candle_feed(monkeypatch):
    class Feed:
        def __init__(self, **kwargs):
            pass

        def set_symbols(self, symbols):
            pass

        connection_available = True

        async def start(self):
            pass

        async def stop(self):
            pass

        async def __aiter__(self):
            await asyncio.Event().wait()
            if False:
                yield

    monkeypatch.setattr(
        "crypto_momentum_lab.next_gen.daemon.BinanceClosedCandle15mFeed", Feed
    )


@pytest.fixture
def clean_account_snapshot() -> AccountSnapshot:
    return AccountSnapshot(
        wallet_balances={"USDT": Decimal("10000.00")},
        available_balance=Decimal("10000.00"),
        positions=[
            PositionSnapshot("BTCUSDT", "LONG", Decimal("0.0")),
            PositionSnapshot("ETHUSDT", "SHORT", Decimal("0.0")),
        ],
    )


@pytest.fixture
def mock_rest_client(clean_account_snapshot: AccountSnapshot) -> AsyncMock:
    mock_client = AsyncMock()
    mock_client.fetch_exchange_info.return_value = exchange_info()
    mock_client.fetch_open_orders.return_value = []
    mock_client.fetch_account_information.return_value = clean_account_snapshot
    mock_client.create_listen_key.return_value = "fake_listen_key_123"
    mock_client.keepalive_listen_key.return_value = None
    return mock_client


@pytest.mark.asyncio
async def test_nextgen_daemon_lifecycle_happy_path(
    mock_rest_client: AsyncMock, account_config
) -> None:
    repo = InMemoryTradingRepository()
    repo.risk_configs[("live", "a1")] = risk_config()
    stop_event = asyncio.Event()

    daemon = NextGenDaemon(
        environment="live",
        account_label="a1",
        account_config=account_config,
        db=repo,
        rest_client=mock_rest_client,
        ws_client=BinanceUserDataStream(mock_rest_client, ws_connector=False),
        stop_event=stop_event,
    )

    connected_event = asyncio.Event()
    original_connect = daemon.ws_client.connect

    async def connect_wrapper(*args: Any, **kwargs: Any) -> None:
        await original_connect(*args, **kwargs)
        connected_event.set()

    daemon.ws_client.connect = connect_wrapper  # type: ignore[method-assign]

    async def trigger_stop_soon() -> None:
        await connected_event.wait()
        stop_event.set()

    stop_task = asyncio.create_task(trigger_stop_soon())

    await daemon.run(keepalive_interval_sec=0.05)
    await stop_task

    # 1. 验证守护进程终态
    assert daemon.state.system_mode == SystemMode.STOPPED
    # 2. 验证会话级 Advisory Lock 已干净释放
    assert "live:a1" not in repo._held_locks
    # 3. 验证冷启动检查与 listenKey 创建
    mock_rest_client.fetch_open_orders.assert_awaited_once()
    mock_rest_client.fetch_account_information.assert_awaited_once()
    mock_rest_client.create_listen_key.assert_awaited_once()


@pytest.mark.asyncio
async def test_nextgen_daemon_fails_fast_on_non_flat_baseline(
    account_config,
    mock_rest_client: AsyncMock,
) -> None:
    repo = InMemoryTradingRepository()
    repo.risk_configs[("live", "a1")] = risk_config()
    # 注入存量挂单
    mock_rest_client.fetch_open_orders.return_value = [{"orderId": 9999}]

    stop_event = asyncio.Event()
    daemon = NextGenDaemon(
        environment="live",
        account_label="a1",
        account_config=account_config,
        db=repo,
        rest_client=mock_rest_client,
        ws_client=BinanceUserDataStream(mock_rest_client, ws_connector=False),
        stop_event=stop_event,
    )

    with pytest.raises(ExceptionGroup, match="unhandled errors"):
        await daemon.run()

    # 验证没有进入 RUNNING
    assert daemon.state.system_mode != SystemMode.RUNNING
    # 验证 Advisory Lock 已释放
    assert "live:a1" not in repo._held_locks


@pytest.mark.asyncio
async def test_nextgen_daemon_mutual_exclusion_advisory_lock(
    account_config,
    mock_rest_client: AsyncMock,
) -> None:
    repo = InMemoryTradingRepository()
    repo.risk_configs[("live", "a1")] = risk_config()
    repo._held_locks.add("live:a1")  # 模拟另一实例已持有锁

    stop_event = asyncio.Event()
    daemon = NextGenDaemon(
        environment="live",
        account_label="a1",
        account_config=account_config,
        db=repo,
        rest_client=mock_rest_client,
        ws_client=BinanceUserDataStream(mock_rest_client, ws_connector=False),
        stop_event=stop_event,
    )

    with pytest.raises(RuntimeError, match="已被另一进程持有"):
        await daemon.run()


@pytest.mark.asyncio
async def test_nextgen_daemon_warmup_called_if_provider_present(
    account_config,
    mock_rest_client: AsyncMock,
) -> None:
    repo = InMemoryTradingRepository()
    repo.risk_configs[("live", "a1")] = risk_config()
    stop_event = asyncio.Event()
    mock_market_provider = AsyncMock()

    daemon = NextGenDaemon(
        environment="live",
        account_label="a1",
        account_config=account_config,
        db=repo,
        rest_client=mock_rest_client,
        ws_client=BinanceUserDataStream(mock_rest_client, ws_connector=False),
        market_data_provider=mock_market_provider,
        stop_event=stop_event,
    )

    connected_event = asyncio.Event()
    original_connect = daemon.ws_client.connect

    async def connect_wrapper(*args: Any, **kwargs: Any) -> None:
        await original_connect(*args, **kwargs)
        connected_event.set()

    daemon.ws_client.connect = connect_wrapper  # type: ignore[method-assign]

    async def trigger_stop_soon() -> None:
        await connected_event.wait()
        stop_event.set()

    stop_task = asyncio.create_task(trigger_stop_soon())
    await daemon.run(keepalive_interval_sec=0.05)
    await stop_task

    mock_rest_client.fetch_exchange_info.assert_awaited_once()
    assert daemon.state.system_mode == SystemMode.STOPPED


@pytest.mark.asyncio
async def test_run_nextgen_daemon_convenience_function(
    account_config,
    mock_rest_client: AsyncMock,
) -> None:
    repo = InMemoryTradingRepository()
    repo.risk_configs[("live", "a1")] = risk_config()
    stop_event = asyncio.Event()

    async def stop_daemon_later() -> None:
        await asyncio.sleep(0.02)
        stop_event.set()

    asyncio.create_task(stop_daemon_later())

    daemon = await run_nextgen_daemon(
        environment="live",
        account_label="a1",
        account_config=account_config,
        db=repo,
        rest_client=mock_rest_client,
        ws_client=BinanceUserDataStream(mock_rest_client, ws_connector=False),
        stop_event=stop_event,
        keepalive_interval_sec=0.05,
    )

    assert daemon.state.system_mode == SystemMode.STOPPED
    assert "live:a1" not in repo._held_locks


@pytest.mark.asyncio
async def test_nextgen_daemon_orchestrates_strategy_and_exit_manager(
    account_config,
    mock_rest_client: AsyncMock,
) -> None:
    repo = InMemoryTradingRepository()
    repo.risk_configs[("live", "a1")] = risk_config()
    stop_event = asyncio.Event()

    mock_strategy = AsyncMock()
    mock_strategy.warmup_market_data = AsyncMock()
    strategy_run_started = asyncio.Event()

    async def mock_strat_run(stop_event: asyncio.Event) -> None:
        strategy_run_started.set()
        await stop_event.wait()

    mock_strategy.run = mock_strat_run

    mock_exit_mgr = AsyncMock()
    exit_mgr_started = asyncio.Event()

    async def mock_exit_run(stop_event: asyncio.Event, price_provider=None) -> None:
        exit_mgr_started.set()
        await stop_event.wait()

    mock_exit_mgr.run = mock_exit_run
    mock_exit_mgr.close = AsyncMock()

    daemon = NextGenDaemon(
        environment="live",
        account_label="a1",
        account_config=account_config,
        db=repo,
        rest_client=mock_rest_client,
        ws_client=BinanceUserDataStream(mock_rest_client, ws_connector=False),
        strategy_engine=mock_strategy,
        exit_manager=mock_exit_mgr,
        stop_event=stop_event,
    )

    async def trigger_stop_when_ready() -> None:
        await strategy_run_started.wait()
        await exit_mgr_started.wait()
        stop_event.set()

    stop_task = asyncio.create_task(trigger_stop_when_ready())

    await daemon.run(keepalive_interval_sec=0.05)
    await stop_task

    mock_strategy.warmup_market_data.assert_awaited_once()
    mock_exit_mgr.close.assert_awaited()
    assert daemon.state.system_mode == SystemMode.STOPPED


@pytest.mark.asyncio
async def test_nextgen_daemon_ensures_hedge_mode_and_leverage(
    account_config,
    mock_rest_client: AsyncMock,
) -> None:
    repo = InMemoryTradingRepository()
    repo.risk_configs[("live", "a1")] = risk_config()
    stop_event = asyncio.Event()

    daemon = NextGenDaemon(
        environment="live",
        account_label="a1",
        account_config=account_config,
        db=repo,
        rest_client=mock_rest_client,
        ws_client=BinanceUserDataStream(mock_rest_client, ws_connector=False),
        symbols=["BTCUSDT", "ETHUSDT"],
        stop_event=stop_event,
    )

    async def stop_daemon_soon() -> None:
        await asyncio.sleep(0.02)
        stop_event.set()

    asyncio.create_task(stop_daemon_soon())
    await daemon.run(keepalive_interval_sec=0.05)

    mock_rest_client.set_position_mode.assert_awaited_once_with(dual_side=True)
    assert mock_rest_client.set_margin_type.await_count == 2
    mock_rest_client.set_margin_type.assert_any_await("BTCUSDT", "CROSSED")
    mock_rest_client.set_margin_type.assert_any_await("ETHUSDT", "CROSSED")
    assert mock_rest_client.set_leverage.await_count == 2
    mock_rest_client.set_leverage.assert_any_await("BTCUSDT", leverage=5)
    mock_rest_client.set_leverage.assert_any_await("ETHUSDT", leverage=5)


@pytest.mark.asyncio
async def test_nextgen_daemon_health_heartbeat(
    account_config,
    mock_rest_client: AsyncMock,
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    health_dir = tmp_path / "health"
    monkeypatch.setenv("CML_LOCAL_HEALTH_DIR", str(health_dir))

    repo = InMemoryTradingRepository()
    repo.risk_configs[("live", "a1")] = risk_config()
    stop_event = asyncio.Event()

    daemon = NextGenDaemon(
        environment="live",
        account_label="a1",
        account_config=account_config,
        db=repo,
        rest_client=mock_rest_client,
        ws_client=BinanceUserDataStream(mock_rest_client, ws_connector=False),
        stop_event=stop_event,
    )

    from unittest.mock import PropertyMock, patch

    from crypto_momentum_lab.next_gen.strategy_engine import MomentumStrategyEngine

    ready = patch.object(
        MomentumStrategyEngine, "is_ready", new_callable=PropertyMock, return_value=True
    )
    ready.start()
    heartbeat_checked = asyncio.Event()

    async def check_heartbeat_and_stop() -> None:
        for _ in range(50):
            await asyncio.sleep(0.02)
            account_health_file = health_dir / "nextgen-a1.ok"
            if account_health_file.exists():
                heartbeat_checked.set()
                break
        stop_event.set()

    asyncio.create_task(check_heartbeat_and_stop())
    await daemon.run(keepalive_interval_sec=0.05)

    ready.stop()
    assert heartbeat_checked.is_set()
    # After shutdown, health files should be unlinked
    account_health_file = health_dir / "nextgen-a1.ok"
    assert not account_health_file.exists()
