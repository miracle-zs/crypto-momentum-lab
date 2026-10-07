from datetime import UTC, datetime
from types import SimpleNamespace

from crypto_momentum_lab.domain.account import ExecutionAccountStatus
from crypto_momentum_lab.execution_account.hub import AccountEvent
from crypto_momentum_lab.live_rollout.control_plane import (
    LiveControlPlaneRuntime,
)

NOW = datetime(2026, 9, 11, 6, 0, tzinfo=UTC)


def test_epoch_reset_keeps_warmup_gate_closed_after_socket_reconnect() -> None:
    gaps = []
    runtime = LiveControlPlaneRuntime(
        session_id="live-reset", context_provider=FakeContextProvider(),
        market_state_available=True, strategy_warmup_ready=True,
        notify_market_state_gap=gaps.append, refresh_entry_gate=lambda: None,
    )
    runtime.on_market_connection_change(False, "market_state_stream_reset")
    assert not runtime.market_state_available
    assert not runtime.strategy_warmup_ready
    assert gaps == ["market_state_stream_reset"]
    runtime.on_market_connection_change(True, None)
    assert runtime.market_state_available
    assert not runtime.strategy_warmup_ready
    runtime.set_strategy_warmup_ready(True, reason="strategy_warmup_ready")
    assert runtime.strategy_warmup_ready


class FakeContextProvider:
    def __init__(self) -> None:
        self.account_updates: list[tuple[object, int, object]] = []
        self.account_invalidations = 0

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



class FakeTelemetry:
    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []

    def consumer_health(self, **event: object) -> None:
        self.events.append(event)


def _runtime(
    *,
    provider: FakeContextProvider,
    market_state_available: bool = True,
) -> tuple[
    LiveControlPlaneRuntime,
    list[bool],
    FakeTelemetry,
    list[str],
]:
    refreshes: list[bool] = []
    market_gap_calls: list[str] = []
    telemetry = FakeTelemetry()

    runtime = LiveControlPlaneRuntime(
        session_id="live-1",
        context_provider=provider,
        market_state_available=market_state_available,
        notify_market_state_gap=market_gap_calls.append,
        refresh_entry_gate=lambda: refreshes.append(True),
        telemetry=telemetry,
        clock=lambda: NOW,
    )
    return (
        runtime,
        refreshes,
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
            ExecutionAccountStatus.READY_READONLY if snapshot is not None else None
        ),
        account_snapshot=snapshot,  # type: ignore[arg-type]
        snapshot_kind="full" if snapshot is not None else "notification",
        sequence=sequence,
    )


def _snapshot() -> object:
    return SimpleNamespace(
        config=SimpleNamespace(environment="live", account_label="primary")
    )




def test_account_snapshot_recovery_fails_closed_until_full_snapshot() -> None:
    provider = FakeContextProvider()
    runtime, refreshes, telemetry, _ = _runtime(
        provider=provider,
    )

    runtime.on_account_snapshot(_account_event(snapshot=None))
    assert provider.account_updates == []
    assert runtime.account_snapshot_available is True

    runtime.on_account_snapshot_recovery("account_event_sequence_gap")
    assert runtime.account_snapshot_available is False
    assert provider.account_invalidations == 1
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
    assert telemetry.events[-1]["recovery"] is True
    assert len(refreshes) == 2




def test_market_connection_changes_fail_closed_and_report_sequence_gaps() -> None:
    provider = FakeContextProvider()
    runtime, refreshes, telemetry, gap_calls = _runtime(
        provider=provider,
        market_state_available=False,
    )

    assert runtime.market_state_available is False
    assert runtime.market_state_unavailable_reason == ("market_state_hub_connecting")

    runtime.on_market_connection_change(
        False,
        "market_state_consumer_lagged",
    )
    assert runtime.market_state_available is False
    assert runtime.market_state_unavailable_reason == ("market_state_consumer_lagged")
    assert gap_calls == ["market_state_consumer_lagged"]
    assert telemetry.events[-1]["lag"] is True

    runtime.on_market_connection_change(True, None)
    assert runtime.market_state_available is True
    assert runtime.market_state_unavailable_reason == "market_state_hub_ready"
    assert telemetry.events[-1]["recovery"] is True
    assert len(refreshes) == 2


def test_strategy_warmup_transition_refreshes_entry_gate() -> None:
    provider = FakeContextProvider()
    runtime, refreshes, _, _ = _runtime(
        provider=provider,
    )

    runtime.set_strategy_warmup_ready(
        True,
        reason="strategy_warmup_ready",
    )

    assert runtime.strategy_warmup_ready is True
    assert runtime.strategy_warmup_reason == "strategy_warmup_ready"
    assert len(refreshes) == 1
