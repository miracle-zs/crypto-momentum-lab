import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from crypto_momentum_lab.next_gen.client import (
    BinanceOrderRejectedError,
    BinanceRestClient,
    BinanceUserDataStream,
)
from crypto_momentum_lab.next_gen.models import OrderStatus


@pytest.fixture
def rest_client():
    return BinanceRestClient(
        api_key="test_api_key",
        api_secret="test_api_secret",
        base_url="https://fapi.binance.com",
    )


def test_binance_rest_sign_params(rest_client: BinanceRestClient):
    params = {"symbol": "BTCUSDT", "side": "BUY"}
    signed = rest_client.sign_params(params, timestamp=1700000000000)
    assert "timestamp" in signed
    assert signed["timestamp"] == 1700000000000
    assert "signature" in signed
    assert len(signed["signature"]) == 64  # SHA256 hex string


@pytest.mark.asyncio
async def test_binance_rest_fetch_account_information(
    rest_client: BinanceRestClient,
):
    fake_response = {
        "assets": [
            {
                "asset": "USDT",
                "walletBalance": "10000.50",
                "availableBalance": "9500.00",
            },
            {"asset": "BNB", "walletBalance": "5.0", "availableBalance": "5.0"},
        ],
        "positions": [
            {"symbol": "BTCUSDT", "positionSide": "LONG", "positionAmt": "0.05"},
            {"symbol": "ETHUSDT", "positionSide": "SHORT", "positionAmt": "0.0"},
        ],
        "totalWalletBalance": "10000.50",
        "availableBalance": "9500.00",
    }

    with patch.object(rest_client, "_request", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = fake_response
        snapshot = await rest_client.fetch_account_information()

        assert snapshot.available_balance == Decimal("9500.00")
        assert snapshot.wallet_balances["USDT"] == Decimal("10000.50")
        assert snapshot.wallet_balances["BNB"] == Decimal("5.0")
        assert len(snapshot.positions) == 2
        assert snapshot.positions[0].symbol == "BTCUSDT"
        assert snapshot.positions[0].position_amt == Decimal("0.05")


@pytest.mark.asyncio
async def test_binance_rest_post_order_hedge_mode_no_reduce_only(
    rest_client: BinanceRestClient,
):
    with patch.object(rest_client, "_request", new_callable=AsyncMock) as mock_req:
        mock_req.return_value = {"orderId": 1234567, "clientOrderId": "e_BTC_L_01"}

        res = await rest_client.post_order(
            symbol="BTCUSDT",
            side="BUY",
            position_side="LONG",
            order_type="LIMIT",
            quantity=Decimal("1.5"),
            price=Decimal("65000"),
            client_order_id="e_BTC_L_01",
        )

        assert res["orderId"] == 1234567
        mock_req.assert_called_once()
        call_kwargs = mock_req.call_args.kwargs
        params = call_kwargs["params"]
        assert params["positionSide"] == "LONG"
        assert "reduceOnly" not in params
        assert params["newClientOrderId"] == "e_BTC_L_01"
        assert params["quantity"] == "1.5"
        assert params["price"] == "65000"


@pytest.mark.asyncio
async def test_binance_rest_post_order_rejection_raises_error(
    rest_client: BinanceRestClient,
):
    with patch.object(rest_client, "_request", new_callable=AsyncMock) as mock_req:
        mock_req.side_effect = BinanceOrderRejectedError(
            code=-2010,
            message="Account has insufficient balance for requested action.",
        )

        with pytest.raises(BinanceOrderRejectedError) as exc_info:
            await rest_client.post_order(
                symbol="BTCUSDT",
                side="BUY",
                position_side="LONG",
                order_type="LIMIT",
                quantity=Decimal("100.0"),
                price=Decimal("65000"),
            )
        assert exc_info.value.code == -2010
        assert "insufficient balance" in exc_info.value.message


@pytest.mark.asyncio
async def test_user_data_stream_parse_order_trade_update():
    stream = BinanceUserDataStream(rest_client=AsyncMock())
    mock_order_handler = AsyncMock()

    raw_event = {
        "e": "ORDER_TRADE_UPDATE",
        "E": 1700000000000,
        "T": 1700000000005,
        "o": {
            "s": "BTCUSDT",
            "c": "e_BTC_L_261018_a1_b1_01",
            "S": "BUY",
            "o": "LIMIT",
            "f": "GTC",
            "q": "1.000",
            "p": "65000.00",
            "ap": "65000.00",
            "sp": "0",
            "x": "TRADE",
            "X": "PARTIALLY_FILLED",
            "i": 88888888,
            "l": "0.400",
            "z": "0.400",
            "L": "65000.00",
            "ps": "LONG",
        },
    }

    await stream.dispatch_raw_event(
        raw_event,
        on_order_trade_update=mock_order_handler,
        on_account_update=AsyncMock(),
    )

    mock_order_handler.assert_awaited_once_with(
        client_order_id="e_BTC_L_261018_a1_b1_01",
        symbol="BTCUSDT",
        position_side="LONG",
        side="BUY",
        order_status=OrderStatus.PARTIALLY_FILLED,
        cum_filled_qty=Decimal("0.400"),
        avg_price=Decimal("65000.00"),
        last_filled_qty=Decimal("0.400"),
        last_filled_price=Decimal("65000.00"),
        exchange_order_id=88888888,
        trade_time=datetime.fromtimestamp(1700000000005 / 1000.0, tz=UTC),
        exchange_realized_pnl=None,
    )


@pytest.mark.asyncio
async def test_user_data_stream_parse_account_update():
    stream = BinanceUserDataStream(rest_client=AsyncMock())
    mock_account_handler = AsyncMock()

    raw_event = {
        "e": "ACCOUNT_UPDATE",
        "E": 1700000000000,
        "T": 1700000000005,
        "a": {
            "m": "ORDER",
            "B": [
                {"a": "USDT", "wb": "12500.50", "cw": "12000.00"},
                {"a": "BNB", "wb": "10.00", "cw": "10.00"},
            ],
            "P": [
                {
                    "s": "BTCUSDT",
                    "pa": "0.500",
                    "ep": "65000.00",
                    "cr": "200.0",
                    "ps": "LONG",
                }
            ],
        },
    }

    await stream.dispatch_raw_event(
        raw_event,
        on_order_trade_update=AsyncMock(),
        on_account_update=mock_account_handler,
    )

    mock_account_handler.assert_awaited_once()
    balances = mock_account_handler.await_args.kwargs["balances"]
    assert balances["USDT"] == Decimal("12500.50")
    assert balances["BNB"] == Decimal("10.00")


@pytest.mark.asyncio
async def test_user_data_stream_physical_connect_and_stream_messages():
    mock_rest = AsyncMock()
    mock_rest.create_listen_key.return_value = "fake_listen_key"

    class FakeWebSocket:
        def __init__(self) -> None:
            self.closed = False
            self.messages = [
                '{"e": "ORDER_TRADE_UPDATE", "E": 1000}',
                '{"e": "ACCOUNT_UPDATE", "E": 2000}',
            ]

        def __aiter__(self):
            return self

        async def __anext__(self):
            if not self.messages:
                raise StopAsyncIteration
            return self.messages.pop(0)

        async def close(self) -> None:
            self.closed = True

    fake_ws = FakeWebSocket()
    mock_connector = AsyncMock(return_value=fake_ws)
    status_changes = []

    stream = BinanceUserDataStream(
        rest_client=mock_rest,
        on_status_change=status_changes.append,
        ws_connector=mock_connector,
    )

    received_events = []
    stop_event = asyncio.Event()

    def handle_event(e):
        received_events.append(e)
        if len(received_events) >= 2:
            stop_event.set()

    await stream.connect(on_event=handle_event)
    assert stream.is_connected is True
    assert status_changes == [True]
    mock_connector.assert_awaited_once_with(
        "wss://fstream.binance.com/private/ws/fake_listen_key"
    )

    await stream.run_stream(stop_event=stop_event)

    assert len(received_events) == 2
    assert received_events[0]["e"] == "ORDER_TRADE_UPDATE"
    assert received_events[1]["e"] == "ACCOUNT_UPDATE"

    await stream.close()
    assert fake_ws.closed is True
    assert stream.is_connected is False


@pytest.mark.asyncio
async def test_user_data_stream_connect_network_failure_triggers_degraded():
    mock_rest = AsyncMock()
    mock_rest.create_listen_key.return_value = "fake_listen_key"

    mock_connector = AsyncMock(side_effect=ConnectionRefusedError("Connection refused"))
    status_changes = []

    stream = BinanceUserDataStream(
        rest_client=mock_rest,
        on_status_change=status_changes.append,
        ws_connector=mock_connector,
    )

    with pytest.raises(ConnectionError, match="WebSocket 物理连接失败"):
        await stream.connect()

    assert stream.is_connected is False
    assert status_changes == [False]


def test_get_binance_urls() -> None:
    from crypto_momentum_lab.next_gen.client import get_binance_urls

    rest_live, ws_live, pub_live = get_binance_urls("live")
    assert rest_live == "https://fapi.binance.com"
    assert ws_live == "wss://fstream.binance.com/private/ws"
    assert pub_live == "wss://fstream.binance.com/market/ws"

    rest_test, ws_test, pub_test = get_binance_urls("testnet")
    assert rest_test == "https://testnet.binancefuture.com"
    assert ws_test == "wss://stream.binancefuture.com/private/ws"
    assert pub_test == "wss://stream.binancefuture.com/ws"


@pytest.mark.asyncio
async def test_user_data_stream_auto_reconnects_on_disconnect() -> None:
    mock_rest = AsyncMock()
    mock_rest.create_listen_key.side_effect = ["key_1", "key_2"]

    class MockWS:
        def __init__(self, msgs: list[str]) -> None:
            self.msgs = msgs
            self.closed = False

        def __aiter__(self):
            return self

        async def __anext__(self):
            if not self.msgs:
                raise StopAsyncIteration
            return self.msgs.pop(0)

        async def close(self) -> None:
            self.closed = True

    ws1 = MockWS(['{"e": "ACCOUNT_UPDATE", "E": 1}'])
    ws2 = MockWS(['{"e": "ORDER_TRADE_UPDATE", "E": 2}'])
    mock_connector = AsyncMock(side_effect=[ws1, ws2])

    status_history: list[bool] = []
    stream = BinanceUserDataStream(
        rest_client=mock_rest,
        on_status_change=status_history.append,
        ws_connector=mock_connector,
        initial_backoff=0.01,
    )

    received: list[dict] = []
    stop_event = asyncio.Event()

    def handle_event(e: dict) -> None:
        received.append(e)
        if len(received) >= 2:
            stop_event.set()

    stream._on_event = handle_event

    # 运行流接收循环（自动重连）
    await stream.run_stream(stop_event=stop_event)

    # 确认重连发生了两次，分别收到了两批消息
    assert mock_connector.call_count >= 2
    assert len(received) == 2
    assert received[0]["e"] == "ACCOUNT_UPDATE"
    assert received[1]["e"] == "ORDER_TRADE_UPDATE"


@pytest.mark.asyncio
async def test_set_margin_type_accepts_already_applied(rest_client: BinanceRestClient):
    with patch.object(rest_client, "_request", new_callable=AsyncMock) as mock_req:
        mock_req.side_effect = BinanceOrderRejectedError(
            code=-4046, message="No need to change margin type."
        )
        await rest_client.set_margin_type("BTCUSDT", "CROSSED")

    mock_req.assert_awaited_once()
    params = mock_req.await_args.kwargs["params"]
    assert params == {"symbol": "BTCUSDT", "marginType": "CROSSED"}


@pytest.mark.asyncio
async def test_set_margin_type_propagates_other_rejections(
    rest_client: BinanceRestClient,
):
    with patch.object(rest_client, "_request", new_callable=AsyncMock) as mock_req:
        mock_req.side_effect = BinanceOrderRejectedError(
            code=-4048, message="Invalid margin type."
        )
        with pytest.raises(BinanceOrderRejectedError):
            await rest_client.set_margin_type("BTCUSDT", "CROSSED")
