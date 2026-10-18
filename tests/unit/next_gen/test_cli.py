from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest
from typer.testing import CliRunner

from crypto_momentum_lab.apps.next_gen.cli import (
    _resolve_credentials,
    _resolve_repository,
    _run_with_signal_handlers,
    app,
)
from crypto_momentum_lab.next_gen.models import Order, OrderStatus, PositionBatch
from crypto_momentum_lab.next_gen.persistence import InMemoryTradingRepository
from tests.unit.next_gen.test_strategy_engine import exchange_info


@pytest.fixture(autouse=True)
def test_manifest_for_cli(monkeypatch, account_config):
    class Manifest:
        def account(self, label):
            assert label == "a1"
            return account_config

    monkeypatch.setattr(
        "crypto_momentum_lab.apps.next_gen.cli.load_live_runtime_manifest",
        lambda path: Manifest(),
    )


runner = CliRunner()


def test_cli_help() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "run" in result.output
    assert "drain" in result.output
    assert "status" in result.output


def test_cli_status_json() -> None:
    from datetime import UTC, datetime

    repo = InMemoryTradingRepository()
    # 模拟写入 1 个批次和 1 个订单
    batch = PositionBatch(
        environment="live",
        account_label="a1",
        batch_id="b_test_1",
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("0.5"),
        entry_price=Decimal("50000"),
        entry_bar_time=datetime.now(UTC),
    )
    repo.batches[("live", "a1", "b_test_1")] = batch

    order = Order(
        environment="live",
        account_label="a1",
        client_order_id="o_test_1",
        batch_id="b_test_1",
        symbol="BTCUSDT",
        side="BUY",
        position_side="LONG",
        order_type="LIMIT",
        quantity=Decimal("0.5"),
        price=Decimal("50000"),
        status=OrderStatus.FILLED,
        filled_qty=Decimal("0.5"),
        avg_price=Decimal("50000"),
    )
    repo.orders[("live", "a1", "o_test_1")] = order

    with patch(
        "crypto_momentum_lab.apps.next_gen.cli._resolve_repository",
        return_value=(repo, None),
    ):
        result = runner.invoke(app, ["status", "-e", "live", "-a", "a1", "--json"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["environment"] == "live"
        assert data["account_label"] == "a1"
        assert data["active_batches_count"] == 1
        assert data["active_batches"][0]["batch_id"] == "b_test_1"
        assert data["recent_orders_count"] == 1
        assert data["recent_orders"][0]["client_order_id"] == "o_test_1"


def test_cli_status_human_readable() -> None:
    result = runner.invoke(app, ["status", "-e", "testnet", "-a", "a1"])
    assert result.exit_code == 0
    assert "Next-Gen Status [testnet:a1]" in result.output
    assert "Active Batches: 0" in result.output


def test_cli_live_without_db_url_fails_closed() -> None:
    result = runner.invoke(app, ["status", "-e", "live", "-a", "a1"])
    assert result.exit_code != 0
    assert "严禁退回内存仓储" in str(result.exception)


def test_cli_drain_dry_run() -> None:
    result = runner.invoke(app, ["drain", "--dry-run", "-e", "live", "-a", "a1"])
    assert result.exit_code == 0


def test_cli_drain_live_flow_cancels_and_sweeps() -> None:
    from datetime import UTC, datetime

    from crypto_momentum_lab.next_gen.engine import AccountSnapshot, PositionSnapshot

    repo = InMemoryTradingRepository()
    batch = PositionBatch(
        environment="testnet",
        account_label="a1",
        batch_id="b_drain_1",
        symbol="BTCUSDT",
        position_side="LONG",
        quantity=Decimal("0.5"),
        entry_price=Decimal("50000"),
        entry_bar_time=datetime.now(UTC),
    )
    repo.batches[("testnet", "a1", "b_drain_1")] = batch

    mock_rest = AsyncMock()
    old_order = Order(
        environment="testnet",
        account_label="a1",
        client_order_id="fl_b_drain_1_01",
        batch_id=batch.batch_id,
        symbol="BTCUSDT",
        side="SELL",
        position_side="LONG",
        order_type="LIMIT",
        quantity=Decimal("0.5"),
        price=Decimal("50000"),
        status=OrderStatus.OPEN,
    )
    batch.status = "EXITING"
    batch.exiting_quantity = batch.quantity
    repo.orders[("testnet", "a1", old_order.client_order_id)] = old_order
    mock_rest.fetch_open_orders.return_value = []
    mock_rest.cancel_order.return_value = {"orderId": 12345, "status": "CANCELED"}
    mock_rest.fetch_exchange_info.return_value = exchange_info()
    mock_rest.post_order.return_value = {"orderId": 999, "clientOrderId": "sweep_1"}
    mock_ws = AsyncMock()

    async def connect(*, on_event):
        state = on_event.__self__
        state.set_ws_connected(True)

        async def cancel(**params):
            order = state.orders[params["client_order_id"]]
            await state.on_order_trade_update(
                order.client_order_id,
                order.symbol,
                order.position_side,
                order.side,
                OrderStatus.CANCELED,
                Decimal("0"),
                Decimal("0"),
                Decimal("0"),
                Decimal("0"),
            )

        async def fill(**params):
            order = state.orders[params["client_order_id"]]
            await state.on_order_trade_update(
                order.client_order_id,
                order.symbol,
                order.position_side,
                order.side,
                OrderStatus.FILLED,
                order.quantity,
                Decimal("50000"),
                order.quantity,
                Decimal("50000"),
            )

        mock_rest.cancel_order.side_effect = cancel
        mock_rest.post_order.side_effect = fill

    mock_ws.connect.side_effect = connect
    mock_rest.fetch_account_information.side_effect = [
        AccountSnapshot(
            {}, Decimal("1000"), [PositionSnapshot("BTCUSDT", "LONG", Decimal("0.5"))]
        ),
        AccountSnapshot({}, Decimal("1000"), []),
    ]

    with (
        patch(
            "crypto_momentum_lab.apps.next_gen.cli._resolve_credentials",
            return_value=("fake_k", "fake_s"),
        ),
        patch(
            "crypto_momentum_lab.apps.next_gen.cli._resolve_read_credentials",
            return_value=("fake_read_k", "fake_read_s"),
        ),
        patch(
            "crypto_momentum_lab.apps.next_gen.cli._resolve_repository",
            return_value=(repo, None),
        ),
        patch(
            "crypto_momentum_lab.apps.next_gen.cli.BinanceRestClient",
            return_value=mock_rest,
        ),
        patch(
            "crypto_momentum_lab.apps.next_gen.cli.BinanceUserDataStream",
            return_value=mock_ws,
        ),
    ):
        result = runner.invoke(app, ["drain", "-e", "testnet", "-a", "a1"])
        assert result.exit_code == 0
        assert mock_rest.fetch_open_orders.await_count == 1
        mock_rest.query_order.assert_not_awaited()
        mock_rest.cancel_order.assert_awaited_once()
        mock_rest.post_order.assert_awaited_once()
        assert mock_rest.fetch_account_information.await_count == 1


def test_cli_run_dry_run_does_not_connect() -> None:
    with (
        patch("crypto_momentum_lab.apps.next_gen.cli.run_nextgen_daemon") as daemon,
        patch("crypto_momentum_lab.apps.next_gen.cli.BinanceRestClient") as rest,
    ):
        result = runner.invoke(app, ["run", "--dry-run", "-e", "live", "-a", "a1"])
        assert result.exit_code == 0
        daemon.assert_not_called()
        rest.assert_not_called()


def test_cli_testnet_routes_both_clients_to_testnet() -> None:
    rest = AsyncMock()
    daemon = AsyncMock()
    with (
        patch(
            "crypto_momentum_lab.apps.next_gen.cli._resolve_credentials",
            return_value=("key", "secret"),
        ),
        patch(
            "crypto_momentum_lab.apps.next_gen.cli._resolve_read_credentials",
            return_value=("read_key", "read_secret"),
        ),
        patch(
            "crypto_momentum_lab.apps.next_gen.cli.BinanceRestClient", return_value=rest
        ) as factory,
        patch("crypto_momentum_lab.apps.next_gen.cli.run_nextgen_daemon", daemon),
    ):
        result = runner.invoke(app, ["run", "-e", "testnet", "-a", "a1"])
        assert result.exit_code == 0
        assert factory.call_args.kwargs["environment"] == "testnet"
        assert daemon.await_args.kwargs["ws_client"].environment == "testnet"


def test_cli_keeps_read_and_trade_clients_separate() -> None:
    trade_client = AsyncMock()
    read_client = AsyncMock()
    daemon = AsyncMock()
    with (
        patch(
            "crypto_momentum_lab.apps.next_gen.cli._resolve_credentials",
            return_value=("trade_key", "trade_secret"),
        ),
        patch(
            "crypto_momentum_lab.apps.next_gen.cli._resolve_read_credentials",
            return_value=("read_key", "read_secret"),
        ),
        patch(
            "crypto_momentum_lab.apps.next_gen.cli.BinanceRestClient",
            side_effect=[trade_client, read_client],
        ),
        patch("crypto_momentum_lab.apps.next_gen.cli.run_nextgen_daemon", daemon),
    ):
        result = runner.invoke(app, ["run", "-e", "testnet", "-a", "a1"])
        assert result.exit_code == 0
        assert daemon.await_args.kwargs["rest_client"] is trade_client
        assert daemon.await_args.kwargs["read_client"] is read_client
        assert daemon.await_args.kwargs["ws_client"]._rest is read_client


def test_resolve_credentials_fallbacks() -> None:
    # 1. 显式参数优先
    k, s = _resolve_credentials("my_key", "my_secret")
    assert k == "my_key"
    assert s == "my_secret"

    # 2. dry_run 兜底
    k, s = _resolve_credentials(dry_run=True)
    assert k == "mock_api_key"
    assert s == "mock_api_secret"

    # 3. 缺失抛出异常
    with (
        patch.dict("os.environ", {}, clear=True),
        pytest.raises(ValueError, match="Binance API credentials missing"),
    ):
        _resolve_credentials(dry_run=False)


def test_resolve_repository() -> None:
    # dry_run 返回 InMemory
    repo, engine = _resolve_repository(dry_run=True)
    assert isinstance(repo, InMemoryTradingRepository)
    assert engine is None


@pytest.mark.asyncio
async def test_run_with_signal_handlers_sets_stop_event() -> None:
    stop_event_observed = False

    async def worker(stop_event: asyncio.Event) -> None:
        nonlocal stop_event_observed
        # 模拟收到信号
        stop_event.set()
        await asyncio.sleep(0.01)
        stop_event_observed = stop_event.is_set()

    await _run_with_signal_handlers(worker)
    assert stop_event_observed is True
