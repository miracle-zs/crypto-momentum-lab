from datetime import UTC, datetime, timedelta

from crypto_momentum_lab.domain.account import ExecutionAccountStatus
from crypto_momentum_lab.execution_account.daemon import (
    ContinuousAccountSyncConfig,
    ContinuousAccountSyncDaemon,
)
from crypto_momentum_lab.execution_account.sync import ExecutionAccountSyncResult


async def test_continuous_sync_only_publishes_transient_states_at_startup() -> None:
    service = FakeSyncService()
    sleeps: list[float] = []

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)

    daemon = ContinuousAccountSyncDaemon(
        service=service,
        config=ContinuousAccountSyncConfig(
            interval_seconds=5,
            fill_interval_seconds=60,
        ),
        clock=SequenceClock(),
        sleep=sleep,
    )

    result = await daemon.run(max_cycles=3)

    assert result.cycle_count == 3
    assert result.failure_count == 0
    assert [call[1] for call in service.calls] == [True, False, False]
    assert [call[2] for call in service.calls] == [True, False, False]
    assert sleeps == [5, 5]


async def test_continuous_sync_backs_off_after_rate_limit_failures() -> None:
    service = RateLimitedSyncService()
    sleeps: list[float] = []

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)

    daemon = ContinuousAccountSyncDaemon(
        service=service,
        config=ContinuousAccountSyncConfig(
            interval_seconds=5,
            fill_interval_seconds=60,
            failure_backoff_initial_seconds=10,
            failure_backoff_max_seconds=60,
        ),
        clock=SequenceClock(),
        sleep=sleep,
    )

    result = await daemon.run(max_cycles=3)

    assert result.failure_count == 2
    assert sleeps == [15, 20]


class FakeSyncService:
    def __init__(self) -> None:
        self.calls: list[tuple[datetime, bool, bool]] = []

    async def sync_once(
        self,
        *,
        observed_at: datetime,
        publish_transient_states: bool,
        include_fills: bool,
    ) -> ExecutionAccountSyncResult:
        self.calls.append(
            (observed_at, publish_transient_states, include_fills)
        )
        return ExecutionAccountSyncResult(
            status=ExecutionAccountStatus.READY_READONLY,
            reconciliation_id=f"sync-{len(self.calls)}",
            mismatch_count=0,
        )


class RateLimitedSyncService(FakeSyncService):
    async def sync_once(
        self,
        *,
        observed_at: datetime,
        publish_transient_states: bool,
        include_fills: bool,
    ) -> ExecutionAccountSyncResult:
        self.calls.append(
            (observed_at, publish_transient_states, include_fills)
        )
        if len(self.calls) <= 2:
            error = RuntimeError("429 Too Many Requests")
            error.retry_after_seconds = 15.0
            raise error
        return ExecutionAccountSyncResult(
            status=ExecutionAccountStatus.READY_READONLY,
            reconciliation_id=f"sync-{len(self.calls)}",
            mismatch_count=0,
        )


class SequenceClock:
    def __init__(self) -> None:
        self._value = datetime(2026, 8, 4, 0, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        value = self._value
        self._value += timedelta(seconds=5)
        return value
