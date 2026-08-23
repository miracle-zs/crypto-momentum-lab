from datetime import UTC, datetime

import pytest

from crypto_momentum_lab.execution_account.retention import (
    AccountSnapshotRetentionConfig,
    prune_account_snapshots_once,
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
        batch_size: int,
        max_rows_per_table: int,
    ) -> dict[str, int]:
        self.call = {
            "environment": environment,
            "account_label": account_label,
            "before": before,
            "batch_size": batch_size,
            "max_rows_per_table": max_rows_per_table,
        }
        return {"account_balance_snapshots": 3}


def test_retention_config_rejects_fast_schedule() -> None:
    with pytest.raises(ValueError, match="at least 300"):
        AccountSnapshotRetentionConfig(interval_seconds=299)


@pytest.mark.asyncio
async def test_prune_once_passes_the_configured_horizon() -> None:
    repository = FakeRetentionRepository()
    now = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)
    config = AccountSnapshotRetentionConfig(
        retention_days=7,
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
        "batch_size": 250,
        "max_rows_per_table": 2_000,
    }
