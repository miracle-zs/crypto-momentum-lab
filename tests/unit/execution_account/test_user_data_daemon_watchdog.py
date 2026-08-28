from datetime import UTC, datetime
from types import SimpleNamespace

from crypto_momentum_lab.domain.account import ExecutionAccountStatus
from crypto_momentum_lab.execution_account.daemon import (
    UserDataAccountSyncConfig,
    UserDataAccountSyncDaemon,
)
from crypto_momentum_lab.execution_account.sync import ExecutionAccountSyncResult


class SnapshotService:
    def __init__(self) -> None:
        self.snapshot_times = []

    async def snapshot_once(self, *, observed_at):
        self.snapshot_times.append(observed_at)


class WatchdogStream:
    def __init__(self) -> None:
        self.metrics = SimpleNamespace(
            parsed_event_count=4,
            fill_event_count=1,
            last_event_received_at=None,
        )
        self.reconnect_reasons = []

    async def request_reconnect(self, reason: str) -> None:
        self.reconnect_reasons.append(reason)


def _result(fill_count: int) -> ExecutionAccountSyncResult:
    return ExecutionAccountSyncResult(
        status=ExecutionAccountStatus.READY_READONLY,
        reconciliation_id="reconciliation",
        mismatch_count=0,
        snapshot=SimpleNamespace(),
        fill_count=fill_count,
    )


async def test_daemon_runs_lightweight_snapshot_with_injected_clock() -> None:
    service = SnapshotService()
    daemon = UserDataAccountSyncDaemon(
        service=service,
        stream=WatchdogStream(),
        config=UserDataAccountSyncConfig(snapshot_interval_seconds=15),
        clock=lambda: datetime(2026, 8, 28, 0, 0, 15, tzinfo=UTC),
    )

    await daemon._snapshot()

    assert service.snapshot_times == [
        datetime(2026, 8, 28, 0, 0, 15, tzinfo=UTC)
    ]


async def test_daemon_reconnects_when_rest_finds_unseen_fill_events() -> None:
    stream = WatchdogStream()
    daemon = UserDataAccountSyncDaemon(
        service=SnapshotService(),
        stream=stream,
        config=UserDataAccountSyncConfig(),
    )
    daemon._last_reconciled_fill_count = 10
    daemon._last_stream_fill_event_count = 1

    await daemon._inspect_reconciliation(_result(fill_count=12))

    assert stream.reconnect_reasons == [
        "rest_reconciliation_found_unmatched_fills"
    ]
