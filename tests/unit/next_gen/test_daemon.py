from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock

import pytest

from crypto_momentum_lab.next_gen.client import BinanceUserDataStream
from crypto_momentum_lab.next_gen.daemon import NextGenDaemon, run_nextgen_daemon
from crypto_momentum_lab.next_gen.engine import AccountSnapshot, PositionSnapshot
from crypto_momentum_lab.next_gen.models import (
    Order,
    OrderStatus,
    PositionBatch,
    SystemMode,
)
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
            self.symbols = frozenset()

        def set_symbols(self, symbols):
            self.symbols = symbols

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
async def test_exit_candle_subscriptions_follow_restored_and_current_responsibility(
    account_config,
    mock_rest_client: AsyncMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = InMemoryTradingRepository()
    for batch_id in ("restored-btc-1", "restored-btc-2"):
        await repo.async_save_batch(
            PositionBatch(
                "live",
                "a1",
                batch_id,
                "BTCUSDT",
                "LONG",
                Decimal("1"),
                Decimal("100"),
                datetime.now(UTC),
            )
        )
    restored_order = Order(
        "live",
        "a1",
        "e_restored_eth_01",
        "restored-eth",
        "ETHUSDT",
        "BUY",
        "LONG",
        "LIMIT",
        Decimal("1"),
        Decimal("100"),
        status=OrderStatus.OPEN,
    )
    await repo.async_save_order(restored_order)
    daemon = NextGenDaemon(
        environment="live",
        account_label="a1",
        account_config=account_config,
        db=repo,
        rest_client=mock_rest_client,
        symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT", "ADAUSDT"],
    )
    await daemon.order_executor.load_active_state()
    feed = daemon._candle_feed
    started = asyncio.Event()

    async def start_feed() -> None:
        started.set()

    monkeypatch.setattr(feed, "start", start_feed)
    candle_task = asyncio.create_task(daemon._run_exit_candles())

    async def update(order, status, quantity) -> None:
        await daemon.state.on_order_trade_update(
            order.client_order_id,
            order.symbol,
            order.position_side,
            order.side,
            status,
            Decimal(quantity),
            Decimal("100"),
            Decimal("0"),
            Decimal("100"),
            trade_time=datetime.now(UTC),
        )

    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        assert feed.symbols == frozenset({"BTCUSDT", "ETHUSDT"})

        # Canceling a partially filled entry must retain its position's stream.
        await update(
            daemon.state.orders[restored_order.client_order_id],
            OrderStatus.PARTIALLY_FILLED,
            "0.4",
        )
        await update(
            daemon.state.orders[restored_order.client_order_id],
            OrderStatus.CANCELED,
            "0.4",
        )
        assert feed.symbols == frozenset({"BTCUSDT", "ETHUSDT"})
        exit_order = daemon.state.create_exit_order(
            "restored-eth", Decimal("0.4"), None, order_type="MARKET"
        )
        await update(exit_order, OrderStatus.FILLED, "0.4")
        assert feed.symbols == frozenset({"BTCUSDT"})

        # The stream is requested synchronously, before entry submission.
        entry = daemon.state.create_entry_order(
            "SOLUSDT", "LONG", Decimal("1"), Decimal("100"), "new-sol"
        )
        assert feed.symbols == frozenset({"BTCUSDT", "SOLUSDT"})
        daemon.state.rollback_unsubmitted_order(entry)
        assert feed.symbols == frozenset({"BTCUSDT"})
        entry = daemon.state.create_entry_order(
            "ADAUSDT", "LONG", Decimal("1"), Decimal("100"), "new-ada"
        )
        assert feed.symbols == frozenset({"BTCUSDT", "ADAUSDT"})
        daemon.state.mark_order_rejected(entry.client_order_id, "test rejection")
        assert feed.symbols == frozenset({"BTCUSDT"})

        # Closing one batch must not remove the other batch's subscription.
        for batch_id in ("restored-btc-1", "restored-btc-2"):
            exit_order = daemon.state.create_exit_order(
                batch_id, Decimal("1"), None, order_type="MARKET"
            )
            await update(exit_order, OrderStatus.FILLED, "1")
            expected = {"BTCUSDT"} if batch_id == "restored-btc-1" else set()
            assert feed.symbols == frozenset(expected)
    finally:
        candle_task.cancel()
        await asyncio.gather(candle_task, return_exceptions=True)


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
async def test_nextgen_daemon_sets_hedge_mode_without_configuring_all_symbols(
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
    # With no restored responsibility, symbol settings are deferred until the
    # first actual entry; setting every tradable symbol breaks on -4028.
    mock_rest_client.set_margin_type.assert_not_awaited()
    mock_rest_client.set_leverage.assert_not_awaited()


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


@pytest.mark.asyncio
async def test_runtime_status_publish_failure_invalidates_health_and_recovers(
    account_config,
    mock_rest_client: AsyncMock,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    health_dir = tmp_path / "health"
    health_dir.mkdir()
    database_file = health_dir / "database"
    database_file.write_text("previous-success", encoding="utf-8")
    repo = InMemoryTradingRepository()
    repo.risk_configs[("live", "a1")] = risk_config("live", "a1")
    repo.fail_next_n_writes(1)
    mock_rest_client.fetch_account_margin_status.return_value = (
        Decimal("0"),
        Decimal("1000"),
        Decimal("1000"),
        0,
        False,
    )
    daemon = NextGenDaemon(
        environment="live",
        account_label="a1",
        account_config=account_config,
        db=repo,
        rest_client=mock_rest_client,
        ws_client=BinanceUserDataStream(mock_rest_client, ws_connector=False),
    )
    daemon.state.set_system_mode(SystemMode.RUNNING)
    daemon.state.ws_connected = True

    async def run_cycle(*, publish_failed: bool) -> None:
        stop_event = asyncio.Event()

        async def check_markers_after_cycle() -> bool:
            assert (health_dir / "status").read_text(encoding="utf-8") == (
                "not_ready" if publish_failed else "ready"
            )
            assert (health_dir / "nextgen-a1.ok").exists() is not publish_failed
            assert (
                database_file.read_text(encoding="utf-8") == "previous-success"
            ) is publish_failed
            stop_event.set()
            return True

        # The stop wait runs after marker writes, so inspect each completed
        # heartbeat without polling the filesystem or racing another cycle.
        monkeypatch.setattr(stop_event, "wait", check_markers_after_cycle)
        await asyncio.wait_for(daemon._run_health_heartbeat(stop_event), timeout=1)
        assert daemon.state.system_mode is SystemMode.RUNNING
        assert daemon.state.has_unrecoverable_fault is False
        assert daemon.state.can_submit_entry_order() is True
        assert daemon.state.can_submit_exit_order() is True

    await run_cycle(publish_failed=True)
    assert repo.runtime_observations == []
    await run_cycle(publish_failed=False)
    assert len(repo.runtime_observations) == 1


@pytest.mark.asyncio
async def test_transient_risk_config_read_failure_keeps_exits_and_recovers(
    account_config,
    mock_rest_client: AsyncMock,
) -> None:
    repo = InMemoryTradingRepository()
    config = risk_config("live", "a1")
    repo.risk_configs[("live", "a1")] = config
    get_risk_config = repo.async_get_risk_config
    read_count = 0

    async def fail_once(environment: str, account_label: str):
        nonlocal read_count
        read_count += 1
        if read_count == 1:
            raise ConnectionError("temporary database read failure")
        return await get_risk_config(environment, account_label)

    repo.async_get_risk_config = fail_once  # type: ignore[method-assign]
    daemon = NextGenDaemon(
        environment="live",
        account_label="a1",
        account_config=account_config,
        db=repo,
        rest_client=mock_rest_client,
        ws_client=BinanceUserDataStream(mock_rest_client, ws_connector=False),
    )
    daemon.state.risk_config = config
    daemon.state.set_system_mode(SystemMode.RUNNING)
    daemon.state.ws_connected = True
    stop_event = asyncio.Event()
    heartbeat = asyncio.create_task(
        daemon._run_health_heartbeat(stop_event, interval_sec=0.1)
    )

    try:
        for _ in range(100):
            if daemon.state.system_mode == SystemMode.DEGRADED:
                break
            await asyncio.sleep(0.001)

        assert daemon.state.system_mode == SystemMode.DEGRADED
        assert daemon.state.can_submit_entry_order() is False
        assert daemon.state.can_submit_exit_order() is True
        assert daemon.state.has_unrecoverable_fault is False

        for _ in range(200):
            if daemon.state.system_mode == SystemMode.RUNNING:
                break
            await asyncio.sleep(0.001)

        assert daemon.state.system_mode == SystemMode.RUNNING
        assert daemon.state.can_submit_entry_order() is True
    finally:
        stop_event.set()
        await heartbeat
