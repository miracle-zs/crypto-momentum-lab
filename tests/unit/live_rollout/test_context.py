from types import SimpleNamespace
from typing import cast

import pytest

from crypto_momentum_lab.live_rollout.context import (
    LiveContextProvider,
    LiveContextRuntime,
    LiveDaemonRuntimeContext,
)


class _Provider:
    def __init__(self) -> None:
        self.current = True
        self.invalidations = 0

    async def __call__(self, _state: object) -> LiveDaemonRuntimeContext:
        raise AssertionError("context loading is outside this unit")

    def is_context_current(self, _context: object) -> bool:
        return self.current

    def invalidate_cache(self) -> None:
        self.invalidations += 1


@pytest.mark.asyncio
async def test_context_runtime_publishes_managed_symbols_through_narrow_callbacks(
) -> None:
    provider = _Provider()
    pending_updates: list[frozenset[str]] = []
    cache_updates: list[tuple[frozenset[str], frozenset[str]]] = []
    published: list[frozenset[str]] = []

    async def on_published(symbols: frozenset[str]) -> None:
        published.append(symbols)

    runtime = LiveContextRuntime(
        run_id="run-1",
        context_provider=cast(LiveContextProvider, provider),
        set_pending_position_symbols=lambda symbols: pending_updates.append(
            frozenset(symbols)
        ),
        update_managed_symbols=lambda positions, orders: cache_updates.append(
            (frozenset(positions), frozenset(orders))
        ),
        on_managed_position_symbols=on_published,
    )
    context = cast(
        LiveDaemonRuntimeContext,
        SimpleNamespace(
            open_position_symbols=frozenset({"BTCUSDT"}),
            unmanaged_position_symbols=frozenset({"ETHUSDT"}),
            pending_position_symbols=frozenset({"SOLUSDT"}),
            unresolved_orders=(
                SimpleNamespace(
                    plan=SimpleNamespace(symbol="adausdt"),
                ),
            ),
        ),
    )

    await runtime.publish_managed_position_symbols(context)

    expected_symbols = frozenset({"BTCUSDT", "ETHUSDT", "SOLUSDT"})
    assert runtime.managed_position_symbols == expected_symbols
    assert pending_updates == [frozenset({"SOLUSDT"})]
    assert cache_updates == [(expected_symbols, frozenset({"ADAUSDT"}))]
    assert published == [expected_symbols]


@pytest.mark.asyncio
async def test_context_runtime_ignores_stale_publication_and_invalidates_provider(
) -> None:
    provider = _Provider()
    cache_updates: list[tuple[frozenset[str], frozenset[str]]] = []
    runtime = LiveContextRuntime(
        run_id="run-1",
        context_provider=cast(LiveContextProvider, provider),
        set_pending_position_symbols=lambda _symbols: None,
        update_managed_symbols=lambda positions, orders: cache_updates.append(
            (frozenset(positions), frozenset(orders))
        ),
    )
    provider.current = False
    context = cast(
        LiveDaemonRuntimeContext,
        SimpleNamespace(
            open_position_symbols=frozenset({"BTCUSDT"}),
            unmanaged_position_symbols=frozenset(),
            pending_position_symbols=frozenset(),
            unresolved_orders=(),
        ),
    )

    await runtime.publish_managed_position_symbols(context)
    runtime.invalidate()

    assert runtime.managed_position_symbols == frozenset()
    assert cache_updates == []
    assert runtime.generation == 1
    assert provider.invalidations == 1
