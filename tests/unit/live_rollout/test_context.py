from datetime import UTC, datetime
from types import SimpleNamespace
from typing import cast

import pytest

from crypto_momentum_lab.live_rollout.context import (
    ContextInvalidation,
    ContextInvalidationReason,
    LiveContextReader,
    LiveContextRuntime,
    LiveDaemonRuntimeContext,
)


class _Provider:
    def __init__(self) -> None:
        self.current = True
        self.invalidations = 0

    async def __call__(self, _state: object) -> LiveDaemonRuntimeContext:
        raise AssertionError("context loading is outside this unit")

    def is_current(self, _context: object) -> bool:
        return self.current

    def invalidate(self, _event: ContextInvalidation | None = None) -> None:
        self.invalidations += 1


@pytest.mark.asyncio
async def test_context_runtime_publishes_managed_symbols_through_narrow_callbacks() -> (
    None
):
    provider = _Provider()
    cache_updates: list[tuple[frozenset[str], frozenset[str]]] = []
    published: list[frozenset[str]] = []

    def on_published(symbols: frozenset[str]) -> None:
        published.append(symbols)

    runtime = LiveContextRuntime(
        run_id="run-1",
        context_provider=cast(LiveContextReader, provider),
        sync_pending_entry_plans=lambda context: None,

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

    runtime.apply_context(context)

    expected_symbols = frozenset({"BTCUSDT", "ETHUSDT", "SOLUSDT"})
    assert runtime.managed_position_symbols == expected_symbols
    assert cache_updates == [(expected_symbols, frozenset({"ADAUSDT"}))]
    assert published == [expected_symbols]


@pytest.mark.asyncio
async def test_context_runtime_ignores_stale_publication_and_invalidates_provider() -> (
    None
):
    provider = _Provider()
    synced = []
    cache_updates: list[tuple[frozenset[str], frozenset[str]]] = []
    runtime = LiveContextRuntime(
        run_id="run-1",
        context_provider=cast(LiveContextReader, provider),
        sync_pending_entry_plans=synced.append,

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

    runtime.apply_context(context)
    runtime.invalidate()

    assert runtime.managed_position_symbols == frozenset()
    assert cache_updates == []
    assert synced == []
    assert runtime.generation == 1
    assert provider.invalidations == 1


def test_context_invalidation_rejects_naive_time() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        ContextInvalidation(
            reason=ContextInvalidationReason.MANUAL,
            occurred_at=datetime(2026, 1, 1, 0, 0, 0),
        )


class _MockReader:
    def __init__(self) -> None:
        self.current = True
        self.last_event: ContextInvalidation | None = None
        self.invalidation_count = 0

    async def __call__(self, _state: object) -> LiveDaemonRuntimeContext:
        raise AssertionError("outside this unit")

    def is_current(self, _context: LiveDaemonRuntimeContext) -> bool:
        return self.current

    def invalidate(self, event: ContextInvalidation | None = None) -> None:
        self.last_event = event
        self.invalidation_count += 1




def test_context_runtime_with_live_context_reader() -> None:
    reader = _MockReader()
    runtime = LiveContextRuntime(
        run_id="run-reader",
        context_provider=reader,
        sync_pending_entry_plans=lambda context: None,

        update_managed_symbols=lambda _p, _o: None,
    )
    dummy_context = cast(LiveDaemonRuntimeContext, SimpleNamespace())

    assert runtime.is_current(dummy_context) is True
    reader.current = False
    assert runtime.is_current(dummy_context) is False

    # Test exception handling in reader.is_current
    def raise_error(_ctx: object) -> bool:
        raise RuntimeError("database connection lost")

    reader.is_current = raise_error  # type: ignore[assignment]
    assert runtime.is_current(dummy_context) is False

    event = ContextInvalidation(
        reason=ContextInvalidationReason.CONTROL_CHANGE,
        occurred_at=datetime.now(tz=UTC),
        details={"operator": "admin"},
    )
    assert runtime.generation == 0
    runtime.invalidate(event)
    assert runtime.generation == 1
    assert reader.invalidation_count == 1
    assert reader.last_event == event


def test_invalidator_internal_type_error_is_not_called_again():
    calls = []

    def invalidate(event):
        calls.append(event)
        raise TypeError("invalid fact")

    runtime = LiveContextRuntime(run_id="run-1",
        context_provider=SimpleNamespace(invalidate=invalidate),
        sync_pending_entry_plans=lambda context: None,

        update_managed_symbols=lambda positions, orders: None)
    runtime.invalidate()
    assert calls == [None]
    assert runtime.generation == 1


@pytest.mark.asyncio
async def test_context_applies_all_memory_views_before_subscription_target_update():
    provider = _Provider()
    order = []
    context = cast(LiveDaemonRuntimeContext, SimpleNamespace(
        open_position_symbols=frozenset({"BTCUSDT"}),
        unmanaged_position_symbols=frozenset(),
        pending_position_symbols=frozenset({"ETHUSDT"}), unresolved_orders=()))

    def notify(symbols):
        assert order == ["entries", "cache"]
        assert runtime.managed_position_symbols == symbols
        provider.current = False
        runtime.apply_context(context)
        assert order == ["entries", "cache"]

    runtime = LiveContextRuntime(run_id="run-1", context_provider=provider,
        sync_pending_entry_plans=lambda context: order.append("entries"),

        update_managed_symbols=lambda positions, orders: order.append("cache"),
        on_managed_position_symbols=notify)
    runtime.apply_context(context)
