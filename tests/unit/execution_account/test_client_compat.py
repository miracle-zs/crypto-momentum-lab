from unittest.mock import patch

import pytest

from crypto_momentum_lab.execution_account.client_compat import (
    fetch_positions_for_reconciliation,
)


@pytest.mark.asyncio
async def test_modern_client_requests_explicit_flat_positions():
    class Client:
        async def fetch_positions(self, *, include_flat=False):
            self.include_flat = include_flat
            return ()

    client = Client()
    assert await fetch_positions_for_reconciliation(client) == ()
    assert client.include_flat is True


@pytest.mark.asyncio
async def test_legacy_client_without_flat_parameter_remains_usable():
    class Client:
        async def fetch_positions(self):
            self.called = True
            return ()

    client = Client()
    assert await fetch_positions_for_reconciliation(client) == ()
    assert client.called is True


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [TypeError, ValueError])
async def test_uninspectable_client_uses_legacy_call(error):
    class Client:
        async def fetch_positions(self, *, include_flat=False):
            self.include_flat = include_flat
            return ()

    client = Client()
    with patch(
        "crypto_momentum_lab.execution_account.client_compat.signature",
        side_effect=error("uninspectable callable"),
    ):
        assert await fetch_positions_for_reconciliation(client) == ()
    assert client.include_flat is False
