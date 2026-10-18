from __future__ import annotations

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from crypto_momentum_lab.next_gen.daemon import NextGenDaemon, run_nextgen_daemon
from crypto_momentum_lab.next_gen.engine import AccountSnapshot, PositionSnapshot
from crypto_momentum_lab.next_gen.models import SystemMode
from crypto_momentum_lab.next_gen.persistence import InMemoryTradingRepository


@pytest.fixture
def clean_account_snapshot():
    return AccountSnapshot(
        wallet_balances={"USDT": Decimal("10000.00")},
        available_balance=Decimal("10000.00"),
        positions=[
            PositionSnapshot("BTCUSDT", "LONG", Decimal("0.0")),
            PositionSnapshot("ETHUSDT", "SHORT", Decimal("0.0")),
        ],
    )


@pytest.fixture
def mock_rest_client(clean_account_snapshot):
    mock_client = AsyncMock()
    mock_client.fetch_open_orders.return_value = []
    mock_client.fetch_account_information.return_value = clean_account_snapshot
    mock_client.create_listen_key.return_value = "fake_listen_key_123"
    mock_client.keepalive_listen_key.return_value = None
    return mock_client


@pytest.mark.asyncio
async def test_nextgen_daemon_lifecycle_happy_path(mock_rest_client):
    repo = InMemoryTradingRepository()
    stop_event = asyncio.Event()

    daemon = NextGenDaemon(
        environment="live",
        account_label="a1",
        db=repo,
        rest_client=mock_rest_client,
        stop_event=stop_event,
    )

    connected_event = asyncio.Event()
    original_connect = daemon.ws_client.connect

    async def connect_wrapper(*args, **kwargs):
        await original_connect(*args, **kwargs)
        connected_event.set()

    daemon.ws_client.connect = connect_wrapper  # type: ignore[method-assign]

    async def trigger_stop_soon():
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
async def test_nextgen_daemon_fails_fast_on_non_flat_baseline(mock_rest_client):
    repo = InMemoryTradingRepository()
    # 注入存量挂单
    mock_rest_client.fetch_open_orders.return_value = [{"orderId": 9999}]

    stop_event = asyncio.Event()
    daemon = NextGenDaemon(
        environment="live",
        account_label="a1",
        db=repo,
        rest_client=mock_rest_client,
        stop_event=stop_event,
    )

    with pytest.raises(RuntimeError, match="检测到交易所存在未结挂单"):
        await daemon.run()

    # 验证没有进入 RUNNING
    assert daemon.state.system_mode != SystemMode.RUNNING
    # 验证 Advisory Lock 已释放
    assert "live:a1" not in repo._held_locks


@pytest.mark.asyncio
async def test_nextgen_daemon_mutual_exclusion_advisory_lock(mock_rest_client):
    repo = InMemoryTradingRepository()
    repo._held_locks.add("live:a1")  # 模拟另一实例已持有锁

    stop_event = asyncio.Event()
    daemon = NextGenDaemon(
        environment="live",
        account_label="a1",
        db=repo,
        rest_client=mock_rest_client,
        stop_event=stop_event,
    )

    with pytest.raises(RuntimeError, match="已被另一进程持有"):
        await daemon.run()


@pytest.mark.asyncio
async def test_nextgen_daemon_warmup_called_if_provider_present(mock_rest_client):
    repo = InMemoryTradingRepository()
    stop_event = asyncio.Event()
    mock_market_provider = AsyncMock()

    daemon = NextGenDaemon(
        environment="live",
        account_label="a1",
        db=repo,
        rest_client=mock_rest_client,
        market_data_provider=mock_market_provider,
        stop_event=stop_event,
    )

    connected_event = asyncio.Event()
    original_connect = daemon.ws_client.connect

    async def connect_wrapper(*args, **kwargs):
        await original_connect(*args, **kwargs)
        connected_event.set()

    daemon.ws_client.connect = connect_wrapper  # type: ignore[method-assign]

    async def trigger_stop_soon():
        await connected_event.wait()
        stop_event.set()

    stop_task = asyncio.create_task(trigger_stop_soon())
    await daemon.run(keepalive_interval_sec=0.05)
    await stop_task

    mock_market_provider.warmup.assert_awaited_once()
    assert daemon.state.system_mode == SystemMode.STOPPED


@pytest.mark.asyncio
async def test_run_nextgen_daemon_convenience_function(mock_rest_client):
    repo = InMemoryTradingRepository()
    stop_event = asyncio.Event()

    async def stop_daemon_later():
        await asyncio.sleep(0.02)
        stop_event.set()

    asyncio.create_task(stop_daemon_later())

    daemon = await run_nextgen_daemon(
        environment="live",
        account_label="a1",
        db=repo,
        rest_client=mock_rest_client,
        stop_event=stop_event,
        keepalive_interval_sec=0.05,
    )

    assert daemon.state.system_mode == SystemMode.STOPPED
    assert "live:a1" not in repo._held_locks
