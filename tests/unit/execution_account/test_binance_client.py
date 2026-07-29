from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from crypto_momentum_lab.domain.execution import ExchangeOrderState, OrderExecutionPlan
from crypto_momentum_lab.execution_account.binance.client import (
    BinanceUsdMPrivateReadClient,
    BinanceUsdMTradeClient,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    ExchangeOrderQueryUnknownError,
    LiveSubmissionDisabledError,
)


async def test_signed_request_includes_timestamp_and_signature() -> None:
    captured_query = ""

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal captured_query
        captured_query = request.url.query.decode()
        return httpx.Response(200, json=[])

    client = BinanceUsdMPrivateReadClient(
        api_key="key",
        api_secret="secret",
        environment="live",
        account_label="primary",
        base_url="https://fapi.binance.com",
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://fapi.binance.com",
        ),
        clock=lambda: datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
    )

    try:
        await client.fetch_balances()
    finally:
        await client.aclose()

    assert "timestamp=1783123200000" in captured_query
    assert "signature=" in captured_query

async def test_client_fetches_account_snapshot() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/fapi/v3/balance"
        return httpx.Response(
            200,
            json=[
                {
                    "asset": "USDT",
                    "balance": "100.5",
                    "availableBalance": "80.25",
                    "crossUnPnl": "1.5",
                }
            ],
        )

    client = BinanceUsdMPrivateReadClient(
        api_key="key",
        api_secret="secret",
        environment="live",
        account_label="primary",
        base_url="https://fapi.binance.com",
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://fapi.binance.com",
        ),
        clock=lambda: datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
    )

    try:
        balances = await client.fetch_balances()
    finally:
        await client.aclose()

    assert len(balances) == 1
    assert balances[0].asset == "USDT"
    assert balances[0].wallet_balance == Decimal("100.5")
    assert balances[0].available_balance == Decimal("80.25")
    assert balances[0].unrealized_pnl == Decimal("1.5")


async def test_client_fetches_recent_user_trades() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/fapi/v1/userTrades"
        assert request.url.params["symbol"] == "BTCUSDT"
        assert request.url.params["limit"] == "1000"
        return httpx.Response(
            200,
            json=[
                {
                    "symbol": "BTCUSDT",
                    "id": 42,
                    "orderId": 1001,
                    "side": "BUY",
                    "price": "30000.5",
                    "qty": "0.01",
                    "realizedPnl": "-0.25",
                    "commission": "0.12",
                    "commissionAsset": "USDT",
                    "time": 1783123200000,
                }
            ],
        )

    client = BinanceUsdMPrivateReadClient(
        api_key="key",
        api_secret="secret",
        environment="live",
        account_label="primary",
        base_url="https://fapi.binance.com",
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://fapi.binance.com",
        ),
        clock=lambda: datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
    )

    try:
        fills = await client.fetch_recent_fills(("btcusdt",))
    finally:
        await client.aclose()

    assert len(fills) == 1
    assert fills[0].trade_id == "42"
    assert fills[0].order_id == "1001"
    assert fills[0].price == Decimal("30000.5")
    assert fills[0].realized_pnl == Decimal("-0.25")
    assert fills[0].fee == Decimal("0.12")


async def test_order_lookup_timeout_becomes_reconciliation_error() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out", request=request)

    client = BinanceUsdMTradeClient(
        api_key="key",
        api_secret="secret",
        environment="live",
        account_label="primary",
        live_submit_enabled=True,
        base_url="https://fapi.binance.com",
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://fapi.binance.com",
        ),
        clock=lambda: datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
    )

    try:
        with pytest.raises(ExchangeOrderQueryUnknownError):
            await client.query_order_by_client_id("BTCUSDT", "client-1")
    finally:
        await client.aclose()


def test_client_does_not_expose_order_submit_methods() -> None:
    client = BinanceUsdMPrivateReadClient(
        api_key="key",
        api_secret="secret",
        environment="live",
        account_label="primary",
        base_url="https://fapi.binance.com",
        clock=lambda: datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
    )

    assert not hasattr(client, "submit_order")
    assert not hasattr(client, "cancel_order")
    assert not hasattr(client, "flatten_position")


async def test_trade_client_requires_explicit_live_enablement() -> None:
    client = BinanceUsdMTradeClient(
        api_key="key",
        api_secret="secret",
        environment="live",
        account_label="primary",
        live_submit_enabled=False,
        clock=lambda: datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
    )

    try:
        with pytest.raises(LiveSubmissionDisabledError):
            await client.submit_order(_order_plan())
    finally:
        await client.aclose()


async def test_trade_client_submits_signed_binance_order() -> None:
    captured_body = ""

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal captured_body
        captured_body = request.content.decode()
        return httpx.Response(
            200,
            json={
                "clientOrderId": _order_plan().client_order_id,
                "orderId": 12345,
                "status": "FILLED",
                "executedQty": "0.003",
                "avgPrice": "30000",
            },
        )

    client = BinanceUsdMTradeClient(
        api_key="key",
        api_secret="secret",
        environment="live",
        account_label="primary",
        live_submit_enabled=True,
        base_url="https://fapi.binance.com",
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://fapi.binance.com",
        ),
        clock=lambda: datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
    )

    try:
        snapshot = await client.submit_order(_order_plan())
    finally:
        await client.aclose()

    assert "newClientOrderId=" in captured_body
    assert "signature=" in captured_body
    assert snapshot.state is ExchangeOrderState.FILLED


def _order_plan() -> OrderExecutionPlan:
    return OrderExecutionPlan(
        intent_id="candidate-1",
        run_id="run-1",
        client_order_id="cml_12345678901234567890123456789012",
        symbol="BTCUSDT",
        side="BUY",
        order_type="MARKET",
        quantity=Decimal("0.003"),
        price=None,
        reduce_only=False,
        created_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        quantized=True,
    )
