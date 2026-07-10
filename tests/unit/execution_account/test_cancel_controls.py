from datetime import UTC, datetime

import httpx
import pytest

from crypto_momentum_lab.execution_account.binance.client import BinanceUsdMTradeClient


async def test_cancel_all_requires_operator_command_record() -> None:
    client = BinanceUsdMTradeClient(
        api_key="key",
        api_secret="secret",
        environment="live",
        account_label="primary",
        live_submit_enabled=True,
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(_unexpected)),
        clock=lambda: datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
    )

    try:
        with pytest.raises(PermissionError, match="command is required"):
            await client.cancel_order(
                symbol="BTCUSDT",
                client_order_id="cml_12345678901234567890123456789012",
                command=None,
            )
    finally:
        await client.aclose()


async def _unexpected(request: httpx.Request) -> httpx.Response:
    raise AssertionError("HTTP request must not occur without command authorization")
