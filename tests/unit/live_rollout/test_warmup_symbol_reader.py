import asyncio
from datetime import UTC, datetime

import pytest

from crypto_momentum_lab.live_rollout.startup_recovery import load_live_warmup_symbols


class Reader:
    async def load_symbols_at(self, *, environment, observed_at):
        assert environment == "research"
        assert observed_at == datetime(2026, 9, 30, tzinfo=UTC)
        return frozenset({"BTCUSDT", "", " "})


async def test_reads_symbols_at_exact_boundary() -> None:
    assert await load_live_warmup_symbols(
        repository=Reader(),
        environment="research",
        observed_at=datetime(2026, 9, 30, tzinfo=UTC),
    ) == frozenset({"BTCUSDT"})


@pytest.mark.parametrize(
    "symbols, expected",
    [((), frozenset()), ((" BTCUSDT ", ""), frozenset({"BTCUSDT"}))],
)
async def test_explicit_symbols_bypass_reader(symbols, expected) -> None:
    assert (
        await load_live_warmup_symbols(
            repository=object(),
            environment="research",
            observed_at=datetime(2026, 9, 30, tzinfo=UTC),
            symbols=symbols,
        )
        == expected
    )


async def test_missing_reader_capability_fails() -> None:
    with pytest.raises(AttributeError):
        await load_live_warmup_symbols(
            repository=object(),
            environment="research",
            observed_at=datetime(2026, 9, 30, tzinfo=UTC),
        )


@pytest.mark.parametrize(
    "error", [ConnectionError("offline"), asyncio.CancelledError()]
)
async def test_reader_failure_propagates(error) -> None:
    class FailingReader:
        async def load_symbols_at(self, **kwargs):
            raise error

    with pytest.raises(type(error)):
        await load_live_warmup_symbols(
            repository=FailingReader(),
            environment="research",
            observed_at=datetime(2026, 9, 30, tzinfo=UTC),
        )
