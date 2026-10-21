from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest

from crypto_momentum_lab.domain.execution.exchange_contract import (
    ExchangeCancellationUnknownError,
    ExchangeOrderAlreadyAbsentError,
    ExchangeOrderQueryUnknownError,
    ExchangeSubmissionTimeoutError,
    LiveSubmissionDisabledError,
)
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderPreSubmissionError,
)
from crypto_momentum_lab.execution_account.binance.client import (
    BinanceRateLimitError,
    BinanceUsdMPrivateReadClient,
    BinanceUsdMTradeClient,
)


async def test_private_request_has_finite_timeouts(monkeypatch) -> None:
    sent_timeouts = []
    create_client = httpx.AsyncClient

    def respond(request: httpx.Request) -> httpx.Response:
        sent_timeouts.append(request.extensions["timeout"])
        return httpx.Response(200, json=[])

    def transport_client(*args, **kwargs):
        return create_client(*args, **kwargs, transport=httpx.MockTransport(respond))

    monkeypatch.setattr(httpx, "AsyncClient", transport_client)
    client = BinanceUsdMPrivateReadClient(
        api_key="key",
        api_secret="secret",
        environment="live",
        account_label="primary",
        base_url="https://fapi.binance.com",
    )
    try:
        assert await client.fetch_positions() == ()
        assert sent_timeouts
        assert all(
            value is not None and 0 < value <= 5
            for timeouts in sent_timeouts
            for value in timeouts.values()
        )
    finally:
        await client.aclose()


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
        await client.fetch_positions()
    finally:
        await client.aclose()

    assert "timestamp=1783123200000" in captured_query
    assert "recvWindow=10000" in captured_query
    assert "signature=" in captured_query


async def test_client_exposes_binance_rate_limit_retry_after() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429,
            headers={"Retry-After": "17"},
            json={"code": -1003, "msg": "Too many requests"},
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
        with pytest.raises(BinanceRateLimitError) as error:
            await client.fetch_positions()
    finally:
        await client.aclose()

    assert error.value.retry_after_seconds == 17.0


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


async def test_order_lookup_rate_limit_becomes_reconciliation_error() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429,
            headers={"Retry-After": "3"},
            json={"code": -1003, "msg": "Too many requests"},
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
        with pytest.raises(ExchangeOrderQueryUnknownError) as error:
            await client.query_order_by_client_id("BTCUSDT", "client-1")
    finally:
        await client.aclose()

    assert error.value.retry_after_seconds == 3.0


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


@pytest.mark.parametrize("body", ["not JSON", "null", "{}"])
async def test_order_post_unreadable_response_has_unknown_outcome(body: str) -> None:
    requests: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        return httpx.Response(200, text=body)

    client = BinanceUsdMTradeClient(
        api_key="key",
        api_secret="secret",
        environment="live",
        account_label="primary",
        live_submit_enabled=True,
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler), base_url="https://fapi.binance.com"
        ),
        clock=lambda: datetime(2026, 7, 4, tzinfo=UTC),
    )
    try:
        with pytest.raises(ExchangeSubmissionTimeoutError):
            await client.submit_order(_order_plan())
        assert requests == ["/fapi/v1/order"]
    finally:
        await client.aclose()


async def test_numeric_average_in_submit_response_keeps_unknown_outcome() -> None:
    requests: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        return httpx.Response(
            200,
            json={
                "clientOrderId": _order_plan().client_order_id,
                "orderId": 12345,
                "status": "FILLED",
                "executedQty": "0.003",
                "avgPrice": 30000.0,
                "cumQuote": "90",
            },
        )

    client = BinanceUsdMTradeClient(
        api_key="key",
        api_secret="secret",
        environment="live",
        account_label="primary",
        live_submit_enabled=True,
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://fapi.binance.com",
        ),
        clock=lambda: datetime(2026, 7, 4, tzinfo=UTC),
    )
    try:
        with pytest.raises(
            ExchangeSubmissionTimeoutError,
            match="response parsing failed",
        ):
            await client.submit_order(_order_plan())
        assert requests == ["/fapi/v1/order"]
    finally:
        await client.aclose()


async def test_trade_client_submits_reduce_only_binance_order() -> None:
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
    assert "reduceOnly=true" in captured_body
    assert "positionSide=" not in captured_body
    assert "signature=" in captured_body
    assert snapshot.state is ExchangeOrderState.FILLED


async def test_trade_client_recovers_missing_average_without_reconciliation() -> None:
    requests: list[tuple[str, str]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append((request.method, request.url.path))
        return httpx.Response(
            200,
            json={
                "clientOrderId": _order_plan().client_order_id,
                "orderId": 12345,
                "status": "FILLED",
                "executedQty": "0.003",
                "cumQuote": "90",
            },
        )

    client = BinanceUsdMTradeClient(
        api_key="key",
        api_secret="secret",
        environment="live",
        account_label="primary",
        live_submit_enabled=True,
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://fapi.binance.com",
        ),
        clock=lambda: datetime(2026, 7, 4, tzinfo=UTC),
    )
    try:
        snapshot = await client.submit_order(_order_plan())
    finally:
        await client.aclose()

    assert snapshot.state is ExchangeOrderState.FILLED
    assert snapshot.executed_quantity == Decimal("0.003")
    assert snapshot.average_price == Decimal("30000")
    assert requests == [("POST", "/fapi/v1/order")]


async def test_trade_client_uses_position_side_for_hedge_mode_close() -> None:
    captured_body = ""
    plan = replace(
        _order_plan(),
        side="SELL",
        reduce_only=True,
        position_side=FuturesPositionSide.LONG,
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal captured_body
        captured_body = request.content.decode()
        return httpx.Response(
            200,
            json={
                "clientOrderId": plan.client_order_id,
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
        await client.submit_order(plan)
    finally:
        await client.aclose()

    assert "positionSide=LONG" in captured_body
    assert "reduceOnly=" not in captured_body


async def test_a4a_binance_client_submits_hedge_short_close_no_reduce_only() -> None:
    captured_body = ""
    plan = replace(
        _order_plan(),
        side="BUY",
        reduce_only=True,
        position_side=FuturesPositionSide.SHORT,
    )

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal captured_body
        captured_body = request.content.decode()
        return httpx.Response(
            200,
            json={
                "clientOrderId": plan.client_order_id,
                "orderId": 12346,
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
        await client.submit_order(plan)
    finally:
        await client.aclose()

    assert "positionSide=SHORT" in captured_body
    assert "reduceOnly=" not in captured_body


async def test_trade_client_cancels_one_known_order_without_operator_command() -> None:
    captured_path = ""

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal captured_path
        captured_path = request.url.path
        return httpx.Response(
            200,
            json={
                "clientOrderId": "client-1",
                "orderId": 12345,
                "status": "CANCELED",
                "executedQty": "0",
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
        snapshot = await client.cancel_order_by_client_id("BTCUSDT", "client-1")
    finally:
        await client.aclose()

    assert captured_path == "/fapi/v1/order"
    assert snapshot.state is ExchangeOrderState.CANCELED


async def test_trade_client_treats_malformed_cancel_response_as_unknown_outcome() -> (
    None
):
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "orderId": 12345,
                "status": "CANCELED",
                "executedQty": "0",
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
        with pytest.raises(ExchangeCancellationUnknownError, match="unreadable"):
            await client.cancel_order_by_client_id("BTCUSDT", "client-1")
    finally:
        await client.aclose()


async def test_trade_client_treats_cancel_rate_limit_as_unknown_outcome() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429,
            headers={"Retry-After": "4"},
            json={"code": -1003, "msg": "Too many requests"},
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
        with pytest.raises(ExchangeCancellationUnknownError) as error:
            await client.cancel_order_by_client_id("BTCUSDT", "client-1")
    finally:
        await client.aclose()

    assert error.value.retry_after_seconds == 4.0


async def test_trade_client_treats_cancel_rejection_as_unknown_outcome() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            400,
            request=request,
            json={"code": -1021, "msg": "Timestamp for this request is outside"},
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
        with pytest.raises(ExchangeCancellationUnknownError):
            await client.cancel_order_by_client_id("BTCUSDT", "client-1")
    finally:
        await client.aclose()


async def test_trade_client_treats_connect_error_on_submit_as_unknown_outcome() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection reset", request=request)

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
        with pytest.raises(ExchangeSubmissionTimeoutError, match="unknown outcome"):
            await client.submit_order(_order_plan())
    finally:
        await client.aclose()


async def test_trade_client_classifies_missing_cancel_order_as_absent() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "DELETE":
            return httpx.Response(
                400,
                request=request,
                json={"code": -2011, "msg": "Unknown order sent."},
            )
        assert request.method == "GET"
        if request.url.path == "/fapi/v1/openOrders":
            return httpx.Response(200, request=request, json=[])
        return httpx.Response(
            400,
            request=request,
            json={"code": -2013, "msg": "Order does not exist."},
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
        request_interval_seconds=0.0,
    )

    try:
        with pytest.raises(ExchangeOrderAlreadyAbsentError) as error:
            await client.cancel_order_by_client_id("BTCUSDT", "client-1")
    finally:
        await client.aclose()

    assert error.value.exchange_code == -2011
    assert error.value.exchange_message == "Unknown order sent."


async def test_trade_client_treats_server_error_as_unknown_submit_outcome() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, request=request, json={"msg": "unknown"})

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
        with pytest.raises(ExchangeSubmissionTimeoutError):
            await client.submit_order(_order_plan())
    finally:
        await client.aclose()


def _order_plan() -> OrderExecutionPlan:
    return OrderExecutionPlan(
        client_order_id="cml_12345678901234567890123456789012",
        symbol="BTCUSDT",
        side="BUY",
        quantity=Decimal("0.003"),
        reduce_only=True,
    )


async def test_flat_bootstrap_reads_explicit_zero_position_rows_from_v2() -> None:
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        rows = (
            []
            if request.url.path.endswith("v3/positionRisk")
            else [
                {
                    "symbol": "BTCUSDT",
                    "positionSide": "LONG",
                    "positionAmt": "0",
                    "entryPrice": "0",
                    "markPrice": "30000",
                    "unRealizedProfit": "0",
                    "notional": "0",
                },
                {
                    "symbol": "BTCUSDT",
                    "positionSide": "SHORT",
                    "positionAmt": "0",
                    "entryPrice": "0",
                    "markPrice": "30000",
                    "unRealizedProfit": "0",
                    "notional": "0",
                },
            ]
        )
        return httpx.Response(200, json=rows)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="https://fapi.binance.com"
    ) as http:
        client = BinanceUsdMPrivateReadClient(
            api_key="key",
            api_secret="secret",
            environment="live",
            account_label="primary",
            http_client=http,
        )
        try:
            assert await client.fetch_positions() == ()
            positions = await client.fetch_positions(include_flat=True)
            assert [(p.symbol, p.position_side, p.position_amt) for p in positions] == [
                ("BTCUSDT", "LONG", Decimal("0")),
                ("BTCUSDT", "SHORT", Decimal("0")),
            ]
            assert paths == ["/fapi/v3/positionRisk", "/fapi/v2/positionRisk"]
        finally:
            await client.aclose()


async def test_submit_order_resolves_zero_avg_price_via_cum_quote() -> None:
    plan = _order_plan()

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "clientOrderId": plan.client_order_id,
                "orderId": 99901,
                "status": "FILLED",
                "executedQty": "500",
                "avgPrice": "0.00000",
                "cumQuote": "100.00000",
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
        snapshot = await client.submit_order(plan)
        assert snapshot.executed_quantity == Decimal("500")
        assert snapshot.average_price == Decimal("0.2")
    finally:
        await client.aclose()


async def test_submit_order_retains_missing_price_when_query_remains_zero() -> None:
    plan = _order_plan()

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "clientOrderId": plan.client_order_id,
                "orderId": 99903,
                "status": "FILLED",
                "executedQty": "10",
                "avgPrice": "0.00000",
                "cumQuote": "0",
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
        snapshot = await client.submit_order(plan)
        assert snapshot.executed_quantity == Decimal("10")
        assert snapshot.average_price == Decimal("0")
    finally:
        await client.aclose()


async def test_submit_order_default_does_not_block_on_missing_price() -> None:
    plan = _order_plan()
    requests: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.method)
        return httpx.Response(
            200,
            json={
                "clientOrderId": plan.client_order_id,
                "orderId": 99905,
                "status": "FILLED",
                "executedQty": "500",
                "avgPrice": "0",
                "cumQuote": "0",
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
        snapshot = await client.submit_order(plan)
        assert requests == ["POST"]
        assert snapshot.executed_quantity == Decimal("500")
        assert snapshot.average_price == Decimal("0")
    finally:
        await client.aclose()
