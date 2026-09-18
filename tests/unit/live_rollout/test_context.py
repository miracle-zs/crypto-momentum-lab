from datetime import UTC, datetime
from types import SimpleNamespace
from typing import cast

import pytest

from crypto_momentum_lab.live_rollout.context import (
    ContextInvalidation,
    ContextInvalidationReason,
    ContextToken,
    LiveContextProvider,
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


def test_context_invalidation_models() -> None:
    assert ContextInvalidationReason.ACCOUNT_UPDATE == "account_update"
    assert ContextInvalidationReason.LEASE_CHANGE == "lease_change"
    assert ContextInvalidationReason.CONTROL_CHANGE == "control_change"
    assert ContextInvalidationReason.RULES_CHANGE == "rules_change"
    assert ContextInvalidationReason.RECOVERY == "recovery"
    assert ContextInvalidationReason.MANUAL == "manual"

    now = datetime.now(tz=UTC)
    invalidation = ContextInvalidation(
        reason=ContextInvalidationReason.ACCOUNT_UPDATE,
        occurred_at=now,
        details={"seq": 42},
    )
    assert invalidation.reason == ContextInvalidationReason.ACCOUNT_UPDATE
    assert invalidation.occurred_at == now
    assert invalidation.details == {"seq": 42}

    with pytest.raises(ValueError, match="timezone-aware"):
        ContextInvalidation(
            reason=ContextInvalidationReason.MANUAL,
            occurred_at=datetime(2026, 1, 1, 0, 0, 0),
        )

    token = ContextToken(generation=1, context_epoch=5, account_snapshot_version=10)
    assert token.generation == 1
    assert token.context_epoch == 5
    assert token.account_snapshot_version == 10


class _MockReader:
    def __init__(self) -> None:
        self.current = True
        self.last_event: ContextInvalidation | None = None
        self.invalidation_count = 0

    async def for_state(self, _state: object) -> LiveDaemonRuntimeContext:
        raise AssertionError("outside this unit")

    def is_current(self, _context: LiveDaemonRuntimeContext) -> bool:
        return self.current

    def invalidate(self, event: ContextInvalidation | None = None) -> None:
        self.last_event = event
        self.invalidation_count += 1


def test_live_context_reader_protocol_conformance() -> None:
    reader = _MockReader()
    assert isinstance(reader, LiveContextReader)

    class _IncompleteReader:
        def for_state(self, _state: object):
            pass

    assert not isinstance(_IncompleteReader(), LiveContextReader)


def test_context_runtime_with_live_context_reader() -> None:
    reader = _MockReader()
    runtime = LiveContextRuntime(
        run_id="run-reader",
        context_provider=reader,
        set_pending_position_symbols=lambda _s: None,
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

