from urllib.parse import parse_qs

import httpx

from crypto_momentum_lab.domain.execution.order_state import ExchangeOrderState
from crypto_momentum_lab.execution_account.binance.client import BinanceUsdMTradeClient
from crypto_momentum_lab.execution_account.orders.state_machine import (
    ExchangeSubmissionTimeoutError,
)
from tests.unit.execution_account.orders.test_state_machine import (
    FakeExchange,
    FakeOrderRepository,
    _machine,
    _plan,
)


async def test_timeout_then_not_found_is_durable_unknown_state() -> None:
    exchange = FakeExchange(
        submit_result=ExchangeSubmissionTimeoutError(),
        query_result=None,
    )
    repository = FakeOrderRepository()

    result = await _machine(exchange, repository).submit(_plan())

    assert exchange.calls == [
        "submit",
        "query",
        "query",
        "query",
        "query",
        "query",
    ]
    assert result.state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
    assert (
        repository.events[-1].state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
    )


async def test_read_timeout_after_acceptance_recovers_without_second_post() -> None:
    plan = _plan()
    calls: list[str] = []
    accepted_ids: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        assert request.url.path == "/fapi/v1/order"
        if request.method == "POST":
            accepted_ids.append(
                parse_qs(request.content.decode())["newClientOrderId"][0]
            )
            raise httpx.ReadTimeout("response lost after acceptance", request=request)
        assert request.method == "GET"
        assert request.url.params["origClientOrderId"] == accepted_ids[0]
        return httpx.Response(
            200,
            json={
                "clientOrderId": accepted_ids[0],
                "orderId": 12345,
                "status": "FILLED",
                "executedQty": "0.003",
                "avgPrice": "30000",
            },
        )

    client = BinanceUsdMTradeClient(
        api_key="test-key",
        api_secret="test-secret",
        environment="live",
        account_label="primary",
        live_submit_enabled=True,
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://fake-exchange.invalid",
        ),
        base_url="https://fake-exchange.invalid",
        clock=lambda: plan.created_at,
    )
    repository = FakeOrderRepository()
    try:
        result = await _machine(client, repository).submit(plan)
        recovered = await _machine(client, repository).reconcile_order(plan)
    finally:
        await client.aclose()

    assert result.state is ExchangeOrderState.FILLED
    assert recovered.state is ExchangeOrderState.FILLED
    assert accepted_ids == [plan.client_order_id]
    assert calls == ["POST", "GET", "GET"]


async def test_recovery_with_no_exchange_record_never_resubmits_or_cancels() -> None:
    exchange = FakeExchange(
        submit_result=AssertionError("recovery must only query"),
        query_result=None,
    )
    repository = FakeOrderRepository()
    result = await _machine(exchange, repository).reconcile_order(_plan())

    assert exchange.calls == ["query"] * 5
    assert result.state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
    assert (
        repository.events[-1].state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
    )
