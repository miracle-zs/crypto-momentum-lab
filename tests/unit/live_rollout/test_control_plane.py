from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from crypto_momentum_lab.domain.account import ExecutionAccountStatus
from crypto_momentum_lab.domain.risk import TradingLease, TradingLeaseState
from crypto_momentum_lab.execution_account.hub import AccountEvent
from crypto_momentum_lab.live_rollout.control_plane import (
    LiveControlPlaneRuntime,
)

NOW = datetime(2026, 9, 11, 6, 0, tzinfo=UTC)


class FakeContextProvider:
    def __init__(self, recovery_context: object | None = None) -> None:
        self.account_updates: list[tuple[object, int, object]] = []
        self.account_invalidations = 0
        self.lease_updates: list[TradingLease] = []
        self.cache_invalidations = 0
        self.recovery_context = recovery_context
        self.loaded_states: list[object] = []

    def update_account_snapshot(
        self,
        snapshot: object,
        *,
        sequence: int,
        account_state: object,
    ) -> None:
        self.account_updates.append((snapshot, sequence, account_state))

    def invalidate_account_snapshot(self) -> None:
        self.account_invalidations += 1

    def update_lease(self, lease: TradingLease) -> None:
        self.lease_updates.append(lease)

    def invalidate_cache(self) -> None:
        self.cache_invalidations += 1

    async def __call__(self, state: object) -> object:
        self.loaded_states.append(state)
        assert self.recovery_context is not None
        return self.recovery_context


class FakeTelemetry:
    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []

    def consumer_health(self, **event: object) -> None:
        self.events.append(event)


def _runtime(
    *,
    provider: FakeContextProvider,
    heartbeat_provider: FakeContextProvider,
    states: tuple[object, ...] = (),
    market_state_available: bool = True,
) -> tuple[
    LiveControlPlaneRuntime,
    list[bool],
    list[bool],
    list[object],
    FakeTelemetry,
    list[str],
]:
    refreshes: list[bool] = []
    database_markers: list[bool] = []
    reacquire_calls: list[object] = []
    market_gap_calls: list[str] = []
    telemetry = FakeTelemetry()

    async def reacquire(gate_context: object) -> TradingLease | None:
        reacquire_calls.append(gate_context)
        return _lease()

    class States:
        def for_symbols(self, _symbols: tuple[str, ...]) -> tuple[object, ...]:
            return states

    runtime = LiveControlPlaneRuntime(
        session_id="live-1",
        context_provider=provider,
        heartbeat_context_provider=heartbeat_provider,
        latest_market_states=States(),
        reacquire_lease=reacquire,
        market_state_available=market_state_available,
        notify_market_state_gap=market_gap_calls.append,
        refresh_entry_gate=lambda: refreshes.append(True),
        mark_database_ok=lambda: database_markers.append(True),
        telemetry=telemetry,
        clock=lambda: NOW,
    )
    return (
        runtime,
        refreshes,
        database_markers,
        reacquire_calls,
        telemetry,
        market_gap_calls,
    )


def _account_event(*, snapshot: object | None, sequence: int = 7) -> AccountEvent:
    return AccountEvent(
        environment="live",
        account_label="primary",
        event_type="ACCOUNT_UPDATE",
        event_id="event-1",
        event_at=NOW,
        received_at=NOW,
        account_state=(
            ExecutionAccountStatus.READY_READONLY
            if snapshot is not None
            else None
        ),
        account_snapshot=snapshot,  # type: ignore[arg-type]
        snapshot_kind="full" if snapshot is not None else "notification",
        sequence=sequence,
    )


def _snapshot() -> object:
    return SimpleNamespace(
        config=SimpleNamespace(environment="live", account_label="primary")
    )


def _lease() -> TradingLease:
    return TradingLease(
        lease_id="lease-1",
        environment="live",
        account_label="primary",
        strategy_name="orderflow_impulse",
        owner="live-worker",
        code_generation="test-generation",
        state=TradingLeaseState.ACTIVE,
        acquired_at=NOW,
        expires_at=NOW + timedelta(minutes=5),
    )


def test_account_snapshot_recovery_fails_closed_until_full_snapshot() -> None:
    provider = FakeContextProvider()
    heartbeat_provider = FakeContextProvider()
    runtime, refreshes, _, _, telemetry, _ = _runtime(
        provider=provider,
        heartbeat_provider=heartbeat_provider,
    )

    runtime.on_account_snapshot(_account_event(snapshot=None))
    assert provider.account_updates == []
    assert runtime.account_snapshot_available is True

    runtime.on_account_snapshot_recovery("account_event_sequence_gap")
    assert runtime.account_snapshot_available is False
    assert provider.account_invalidations == 1
    assert heartbeat_provider.account_invalidations == 1
    assert telemetry.events[-1] == {
        "consumer": "account_event_hub",
        "available": False,
        "occurred_at": NOW,
        "reason": "account_event_sequence_gap",
        "lag": True,
    }

    runtime.on_account_snapshot(_account_event(snapshot=_snapshot()))
    assert runtime.account_snapshot_available is True
    assert len(provider.account_updates) == 1
    assert len(heartbeat_provider.account_updates) == 1
    assert telemetry.events[-1]["recovery"] is True
    assert len(refreshes) == 2


async def test_lease_callbacks_publish_state_and_recover_from_latest_market_state(
) -> None:
    provider = FakeContextProvider()
    gate_context = object()
    heartbeat_provider = FakeContextProvider(
        recovery_context=SimpleNamespace(gate_context=gate_context)
    )
    latest_states = (object(), object())
    runtime, refreshes, database_markers, reacquire_calls, _, _ = _runtime(
        provider=provider,
        heartbeat_provider=heartbeat_provider,
        states=latest_states,
    )

    runtime.on_lease_error(RuntimeError("database unavailable"))
    assert runtime.lease_heartbeat_degraded is True

    lease = _lease()
    runtime.on_lease_renewed(lease)
    assert runtime.lease_heartbeat_degraded is False
    assert provider.lease_updates == [lease]
    assert heartbeat_provider.lease_updates == [lease]
    assert database_markers == [True]

    recovered = await runtime.recover_live_lease()
    assert recovered == lease
    assert heartbeat_provider.loaded_states == [latest_states[-1]]
    assert heartbeat_provider.cache_invalidations == 1
    assert reacquire_calls == [gate_context]
    assert len(refreshes) == 2


def test_market_connection_changes_fail_closed_and_report_sequence_gaps() -> None:
    provider = FakeContextProvider()
    heartbeat_provider = FakeContextProvider()
    runtime, refreshes, _, _, telemetry, gap_calls = _runtime(
        provider=provider,
        heartbeat_provider=heartbeat_provider,
        market_state_available=False,
    )

    assert runtime.market_state_available is False
    assert runtime.market_state_unavailable_reason == (
        "market_state_hub_connecting"
    )

    runtime.on_market_connection_change(
        False,
        "market_state_consumer_lagged",
    )
    assert runtime.market_state_available is False
    assert runtime.market_state_unavailable_reason == (
        "market_state_consumer_lagged"
    )
    assert gap_calls == ["market_state_consumer_lagged"]
    assert telemetry.events[-1]["lag"] is True

    runtime.on_market_connection_change(True, None)
    assert runtime.market_state_available is True
    assert runtime.market_state_unavailable_reason == "market_state_hub_ready"
    assert telemetry.events[-1]["recovery"] is True
    assert len(refreshes) == 2
