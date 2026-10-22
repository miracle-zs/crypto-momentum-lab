"""End-to-end NextGen daemon lifecycle against fakes, without exchange access."""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

from crypto_momentum_lab.domain.market.closed_candle import ClosedCandle15m
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.live_rollout.closed_candle_feed import ClosedCandle15mEvent
from crypto_momentum_lab.live_rollout.runtime_manifest import load_live_runtime_manifest
from crypto_momentum_lab.next_gen.client import (
    BinanceRestClient,
    BinanceUserDataStream,
)
from crypto_momentum_lab.next_gen.daemon import NextGenDaemon
from crypto_momentum_lab.next_gen.engine import AccountSnapshot
from crypto_momentum_lab.next_gen.persistence import InMemoryTradingRepository
from tests.unit.next_gen.factories import risk_config
from tests.unit.next_gen.test_strategy_engine import exchange_info
from tests.unit.strategies.order_flow_impulse.test_order_flow_impulse_runtime import (
    _impulse_states,
)


class _FakePrivateWebSocket:
    def __init__(self) -> None:
        self.messages: asyncio.Queue[str | None] = asyncio.Queue()
        self.closed = False

    async def send(self, message: str) -> None:
        return None

    def __aiter__(self) -> _FakePrivateWebSocket:
        return self

    async def __anext__(self) -> str:
        message = await self.messages.get()
        if message is None:
            raise StopAsyncIteration
        return message

    async def close(self) -> None:
        self.closed = True
        self.messages.put_nowait(None)


class _FakeMarketProvider:
    def __init__(self) -> None:
        self.symbols: list[str] = []
        self.strategy: Any = None
        self.last_state: MarketState15s | None = None

    async def run(self, stop_event: asyncio.Event) -> None:
        await asyncio.sleep(0.05)
        if self.last_state is None or self.strategy is None:
            raise AssertionError("market provider was not wired")
        self.strategy.market_connected = True
        await self.strategy.on_market_state(self.last_state)
        await stop_event.wait()

    async def close(self) -> None:
        return None


class _FakeClosedCandleFeed:
    connection_available = True

    def __init__(self, **kwargs: Any) -> None:
        self.symbols: frozenset[str] = frozenset()
        self.events: asyncio.Queue[ClosedCandle15mEvent | None] = asyncio.Queue()

    def set_symbols(self, symbols: frozenset[str]) -> None:
        self.symbols = symbols

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        self.events.put_nowait(None)

    def __aiter__(self) -> _FakeClosedCandleFeed:
        return self

    async def __anext__(self) -> Any:
        event = await self.events.get()
        if event is None:
            raise StopAsyncIteration
        return event


def _account_config() -> Any:
    manifest = load_live_runtime_manifest(
        Path("deploy/live-runtime.yaml"),
        environment={"CML_CODE_COMMIT": "e2e-code"},
    )
    base = replace(manifest.account("primary"), label="a1")
    profile = replace(
        base.strategy_inputs.profile,
        impulse_window_buckets=3,
        min_return_pct=Decimal("0.01"),
        min_aggressive_imbalance=Decimal("0.5"),
        min_notional_intensity=Decimal("2"),
        min_notional_5m_vs_30m=Decimal("0"),
    )
    return replace(
        base,
        strategy_inputs=replace(base.strategy_inputs, profile=profile),
    )


def _shifted_states() -> tuple[MarketState15s, ...]:
    original = _impulse_states()
    shift = datetime.now(UTC) - original[-1].bucket_end
    return tuple(
        replace(
            state,
            bucket_start=state.bucket_start + shift,
            bucket_end=state.bucket_end + shift,
        )
        for state in original
    )


async def _wait_until(predicate: Any, max_wait: float = 5.0) -> None:
    async def poll() -> None:
        while not predicate():  # noqa: ASYNC110 - bounded test polling
            await asyncio.sleep(0.01)

    await asyncio.wait_for(poll(), max_wait)


@pytest.mark.asyncio
async def test_nextgen_daemon_entry_fill_and_candle_exit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("CML_LOCAL_HEALTH_DIR", str(tmp_path / "health"))
    monkeypatch.setattr(
        "crypto_momentum_lab.next_gen.daemon.BinanceClosedCandle15mFeed",
        _FakeClosedCandleFeed,
    )

    states = _shifted_states()
    last_state = states[-1]
    repository = InMemoryTradingRepository()
    repository.market_states = list(states[:-1])
    repository.entry_symbols = frozenset({"BTCUSDT"})
    repository.risk_configs[("live", "a1")] = risk_config()

    fake_ws = _FakePrivateWebSocket()

    async def connector(url: str) -> _FakePrivateWebSocket:
        return fake_ws

    read_client = AsyncMock(spec=BinanceRestClient)
    read_client.fetch_exchange_info.return_value = exchange_info()
    read_client.fetch_account_information.return_value = AccountSnapshot(
        wallet_balances={"USDT": Decimal("10000")},
        available_balance=Decimal("10000"),
        positions=[],
    )
    read_client.fetch_open_orders.return_value = []
    read_client.create_listen_key.return_value = "fake_listen_key"
    read_client.keepalive_listen_key.return_value = None

    trade_client = AsyncMock(spec=BinanceRestClient)
    trade_client.set_position_mode.return_value = None
    trade_client.set_margin_type.return_value = None
    trade_client.set_leverage.return_value = {}
    trade_client.create_listen_key.return_value = "fake_listen_key"
    trade_client.keepalive_listen_key.return_value = None
    trade_client.fetch_startup_order.return_value = {}
    trade_client.fetch_startup_realized_pnl.return_value = Decimal("0")

    async def post_order(**kwargs: Any) -> dict[str, Any]:
        assert daemon._candle_feed.symbols == frozenset({kwargs["symbol"]})
        price = kwargs["price"] if kwargs["price"] is not None else Decimal("105")
        timestamp = int(datetime.now(UTC).timestamp() * 1000)
        await fake_ws.messages.put(
            json.dumps(
                {
                    "e": "ORDER_TRADE_UPDATE",
                    "o": {
                        "s": kwargs["symbol"],
                        "c": kwargs["client_order_id"],
                        "S": kwargs["side"],
                        "ps": kwargs["position_side"],
                        "X": "FILLED",
                        "z": str(kwargs["quantity"]),
                        "ap": str(price),
                        "l": str(kwargs["quantity"]),
                        "L": str(price),
                        "i": 123456,
                        "T": timestamp,
                        "rp": "0",
                    },
                }
            )
        )
        return {"orderId": 123456, "clientOrderId": kwargs["client_order_id"]}

    trade_client.post_order.side_effect = post_order
    trade_client.cancel_order.return_value = {"status": "CANCELED"}

    market = _FakeMarketProvider()
    market.last_state = last_state
    stop_event = asyncio.Event()
    daemon = NextGenDaemon(
        environment="live",
        account_label="a1",
        account_config=_account_config(),
        db=repository,
        rest_client=trade_client,
        read_client=read_client,
        ws_client=BinanceUserDataStream(
            read_client,
            environment="live",
            ws_connector=connector,
        ),
        market_data_provider=market,
        market_environment="research",
        stop_event=stop_event,
    )
    market.strategy = daemon.strategy_engine

    run_task = asyncio.create_task(daemon.run(keepalive_interval_sec=0.05))
    try:
        await _wait_until(lambda: bool(daemon.state.batches))
        batch = next(iter(daemon.state.batches.values()))
        await _wait_until(lambda: batch.quantity > Decimal("0"))
        entry_price = batch.entry_price
        entry_anchor = batch.entry_bar_time

        candle_start = entry_anchor.replace(
            minute=entry_anchor.minute - entry_anchor.minute % 15,
            second=0,
            microsecond=0,
        ) + timedelta(minutes=15)
        candle = ClosedCandle15m(
            symbol="BTCUSDT",
            candle_start=candle_start,
            candle_end=candle_start + timedelta(minutes=15),
            open_price=entry_price + Decimal("10"),
            close_price=entry_price + Decimal("5"),
        )
        await daemon._candle_feed.events.put(
            ClosedCandle15mEvent(
                candle=candle,
                exchange_event_at=candle.candle_end,
                received_at=candle.candle_end + timedelta(seconds=1),
            )
        )
        await _wait_until(lambda: batch.quantity == Decimal("0"))
        assert batch.status == "CLOSED"
        assert daemon.state.active_exit_orders == {}
        assert daemon._candle_feed.symbols == frozenset()
        assert trade_client.post_order.await_count >= 2
    finally:
        stop_event.set()
        await asyncio.wait_for(run_task, timeout=10)
    assert daemon.state.system_mode.name == "STOPPED"
