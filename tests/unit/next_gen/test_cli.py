from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
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
    result = runner.invoke(app, ["status", "-e", "live", "-a", "a1"])
    assert result.exit_code == 0
    assert "Next-Gen Status [live:a1]" in result.output
    assert "Active Batches: 0" in result.output


def test_cli_drain_dry_run() -> None:
    result = runner.invoke(app, ["drain", "--dry-run", "-e", "live", "-a", "a1"])
    assert result.exit_code == 0


def test_cli_run_dry_run_invokes_daemon() -> None:
    mock_run_daemon = AsyncMock()

    with (
        patch(
            "crypto_momentum_lab.apps.next_gen.cli.run_nextgen_daemon",
            mock_run_daemon,
        ),
        patch(
            "crypto_momentum_lab.apps.next_gen.cli._run_with_signal_handlers"
        ) as mock_signal_runner,
    ):
        # 拦截 _run_with_signal_handlers 并直接调用其传入的协程
        async def fake_signal_runner(
            runner_fn: Callable[[asyncio.Event], Awaitable[None]],
        ) -> None:
            stop_event = asyncio.Event()
            await runner_fn(stop_event)

        mock_signal_runner.side_effect = fake_signal_runner

        result = runner.invoke(
            app,
            [
                "run",
                "--dry-run",
                "-e",
                "live",
                "-a",
                "a1",
                "--symbols",
                "BTCUSDT,ETHUSDT",
                "--keepalive-interval",
                "60",
            ],
        )
        assert result.exit_code == 0
        mock_run_daemon.assert_awaited_once()
        _, kwargs = mock_run_daemon.call_args
        assert kwargs["environment"] == "live"
        assert kwargs["account_label"] == "a1"
        assert kwargs["symbols"] == ["BTCUSDT", "ETHUSDT"]
        assert kwargs["keepalive_interval_sec"] == 60.0


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
