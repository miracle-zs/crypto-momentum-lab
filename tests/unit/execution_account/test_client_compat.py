from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from crypto_momentum_lab.execution_account.client_compat import (
    fetch_positions_for_reconciliation,
    incomplete_fill_symbols,
    optional_fill_provenance_fetcher,
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


@pytest.mark.parametrize(
    "client",
    [
        object(),
        type("Client", (), {"fetch_fills_with_provenance": None})(),
        type("Client", (), {"fetch_fills_with_provenance": 42})(),
    ],
)
def test_missing_or_noncallable_provenance_does_not_claim_capability(client):
    assert optional_fill_provenance_fetcher(client) is None


@pytest.mark.asyncio
async def test_provenance_callable_is_used_directly_and_errors_propagate():
    class Client:
        async def fetch_fills_with_provenance(
            self, symbol, *, start_time_ms, checked_through
        ):
            self.request = (symbol, start_time_ms, checked_through)
            raise LookupError("scan failed")

    client = Client()
    fetcher = optional_fill_provenance_fetcher(client)
    assert fetcher == client.fetch_fills_with_provenance
    checked_through = datetime(2026, 10, 1, tzinfo=UTC)
    with pytest.raises(LookupError, match="scan failed"):
        await fetcher("BTCUSDT", start_time_ms=123, checked_through=checked_through)
    assert client.request == ("BTCUSDT", 123, checked_through)


def test_absent_incomplete_marker_is_empty():
    assert tuple(incomplete_fill_symbols(object())) == ()


def test_incomplete_marker_reads_current_values_without_normalizing():
    class Client:
        incomplete_fill_symbols = [" btcusdt ", "BTCUSDT", "BTCUSDT"]

    client = Client()
    assert incomplete_fill_symbols(client) is client.incomplete_fill_symbols
    client.incomplete_fill_symbols = ["ETHUSDT"]
    assert tuple(incomplete_fill_symbols(client)) == ("ETHUSDT",)
