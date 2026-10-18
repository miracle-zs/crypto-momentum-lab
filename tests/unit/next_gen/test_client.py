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
