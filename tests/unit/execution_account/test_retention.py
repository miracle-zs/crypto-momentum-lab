from datetime import UTC, datetime

import pytest

from crypto_momentum_lab.execution_account.retention import (
    AccountSnapshotRetentionConfig,
    prune_account_snapshots_once,
    run_account_snapshot_retention,
)


class FakeRetentionRepository:
    def __init__(self) -> None:
        self.call: dict[str, object] | None = None

    async def prune_account_snapshots(
        self,
        *,
        environment: str,
        account_label: str,
        before: datetime,
        equity_before: datetime,
        batch_size: int,
        max_rows_per_table: int,
        consumer_requirements: tuple[object, ...] = (),
    ) -> dict[str, int]:
        self.call = {
            "environment": environment,
            "account_label": account_label,
            "before": before,
            "equity_before": equity_before,
            "batch_size": batch_size,
            "max_rows_per_table": max_rows_per_table,
            "consumer_requirements": consumer_requirements,
        }
        return {"account_balance_snapshots": 3}


def test_retention_config_rejects_fast_schedule() -> None:
    with pytest.raises(ValueError, match="at least 300"):
        AccountSnapshotRetentionConfig(interval_seconds=299)


def test_retention_config_default_covers_one_hour_of_live_snapshots() -> None:
    assert AccountSnapshotRetentionConfig().max_rows_per_table == 5_000


def test_retention_config_keeps_equity_at_least_as_long_as_operations() -> None:
    with pytest.raises(ValueError, match="at least retention_days"):
        AccountSnapshotRetentionConfig(
            retention_days=30,
            equity_retention_days=7,
        )


@pytest.mark.asyncio
async def test_prune_once_passes_the_configured_horizon() -> None:
    repository = FakeRetentionRepository()
    now = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)
    config = AccountSnapshotRetentionConfig(
        retention_days=7,
        equity_retention_days=370,
        interval_seconds=900,
        batch_size=250,
        max_rows_per_table=2_000,
    )

    deleted = await prune_account_snapshots_once(
        repository=repository,
        environment="live",
        account_label="primary",
        config=config,
        now=now,
    )

    assert deleted == {"account_balance_snapshots": 3}
    assert repository.call == {
        "environment": "live",
        "account_label": "primary",
        "before": datetime(2026, 8, 16, 12, 0, tzinfo=UTC),
        "equity_before": datetime(2025, 8, 18, 12, 0, tzinfo=UTC),
        "batch_size": 250,
        "max_rows_per_table": 2_000,
        "consumer_requirements": (),
    }


@pytest.mark.asyncio
async def test_retention_loop_fails_closed_when_provider_raises() -> None:
    repository = FakeRetentionRepository()
    config = AccountSnapshotRetentionConfig(
        interval_seconds=300,
    )
    errors: list[Exception] = []

    async def _failing_provider():
        raise RuntimeError("database connection down")

    call_count = 0

    async def _fake_sleep(_seconds: float) -> None:
        nonlocal call_count
        call_count += 1
        if call_count > 1:
            await asyncio.sleep(100)
        else:
            await asyncio.sleep(0)

    import asyncio

    task = asyncio.create_task(
        run_account_snapshot_retention(
            repository=repository,
            environment="live",
            account_label="primary",
            config=config,
            consumer_requirements_provider=_failing_provider,
            on_error=lambda err: errors.append(err),
            sleep=_fake_sleep,
        )
    )

    # Let the loop execute 1 iteration
    await asyncio.sleep(0.01)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # The failing provider was called and logged
    assert len(errors) > 0
    assert isinstance(errors[0], RuntimeError)
    # CRITICAL: Fail-closed verification! Repository prune was NEVER called!
    assert repository.call is None
