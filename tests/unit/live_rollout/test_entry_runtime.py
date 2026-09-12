from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from crypto_momentum_lab.live_rollout import entry_runtime as entry_runtime_module
from crypto_momentum_lab.live_rollout.entry_runtime import LiveEntryRuntime


class FakeClient:
    configured_margin_type_count = 2

    def __init__(self) -> None:
        self.margin_symbols: frozenset[str] | None = None
        self.leverage_symbols: frozenset[str] | None = None

    async def warm_entry_margin_type(self, symbols) -> None:
        self.margin_symbols = frozenset(symbols)

    async def warm_entry_leverage(self, symbols) -> None:
        self.leverage_symbols = frozenset(symbols)


class FakeUniverseRepository:
    async def load_snapshot_at(self, _observed_at: datetime):
        return SimpleNamespace(
            ranking=SimpleNamespace(
                gainers=(
                    SimpleNamespace(symbol="BTCUSDT", utc_day_return=0.02),
                    SimpleNamespace(symbol="ETHUSDT", utc_day_return=-0.01),
                    SimpleNamespace(symbol="SOLUSDT", utc_day_return=0.03),
                )
            )
        )


class FakeEmaProvider:
    def load(self, *, symbol: str, observed_at: datetime):
        return SimpleNamespace(
            ema5=101,
            ema10=99,
            observed_at=observed_at,
            snapshot_id=f"snapshot-{symbol}",
            config_hash="ema-config",
        )


@pytest.mark.asyncio
async def test_entry_runtime_warms_exchange_from_positive_gainer_pool(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        entry_runtime_module,
        "PostgresUniverseRepository",
        lambda _factory: FakeUniverseRepository(),
    )
    client = FakeClient()
    runtime = LiveEntryRuntime(
        market_session_factory=object(),  # type: ignore[arg-type]
        client=client,  # type: ignore[arg-type]
        ema_provider=None,
        positive_gainer_top_count=3,
        entry_leverage=7,
        margin_type="ISOLATED",
    )

    await runtime.warm_exchange(datetime(2026, 9, 12, tzinfo=UTC))

    assert client.margin_symbols == frozenset({"BTCUSDT", "SOLUSDT"})
    assert client.leverage_symbols == frozenset({"BTCUSDT", "SOLUSDT"})
    assert runtime.entry_symbol_cache_required
    assert not runtime.entry_filter_cache_required


@pytest.mark.asyncio
async def test_entry_runtime_loads_ema_context_without_pool_cache() -> None:
    observed_at = datetime(2026, 9, 12, tzinfo=UTC)
    runtime = LiveEntryRuntime(
        market_session_factory=object(),  # type: ignore[arg-type]
        client=FakeClient(),  # type: ignore[arg-type]
        ema_provider=FakeEmaProvider(),  # type: ignore[arg-type]
        positive_gainer_top_count=None,
        entry_leverage=None,
        margin_type=None,
    )
    state = SimpleNamespace(
        symbol="BTCUSDT",
        bucket_start=observed_at,
        last_ask_price=102,
        midpoint=None,
        close_price=None,
        mark_price=None,
    )

    loader = runtime.entry_filter_context_loader
    assert loader is not None
    context = await loader(state)

    assert context is not None
    assert context.entry_price == 102
    assert context.ema5 == 101
    assert context.ema10 == 99
