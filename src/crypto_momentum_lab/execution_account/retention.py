"""Background retention for execution-account operational snapshots."""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from crypto_momentum_lab.domain.operational.retention_contract import (
    RetentionConsumerRequirement,
)


class AccountSnapshotRetentionRepository(Protocol):
    async def prune_account_snapshots(
        self,
        *,
        environment: str,
        account_label: str,
        before: datetime,
        equity_before: datetime,
        batch_size: int,
        max_rows_per_table: int,
        consumer_requirements: tuple[RetentionConsumerRequirement, ...] = (),
    ) -> dict[str, int]: ...


@dataclass(frozen=True, slots=True)
class AccountSnapshotRetentionConfig:
    retention_days: int = 7
    equity_retention_days: int = 370
    interval_seconds: float = 3_600.0
    # Keep each transaction small enough that retention cannot compete with
    # order/account writes for the PostgreSQL memory budget.
    batch_size: int = 250
    # The live balance stream produces a little over 3,000 rows per hour;
    # allow one cycle to drain the normal hourly volume while retaining the
    # per-batch and per-cycle runtime bounds below.
    max_rows_per_table: int = 5_000
    max_runtime_seconds: float = 45.0

    def __post_init__(self) -> None:
        if self.retention_days <= 0:
            raise ValueError("retention_days must be positive")
        if self.equity_retention_days < self.retention_days:
            raise ValueError("equity_retention_days must be at least retention_days")
        if self.interval_seconds < 300:
            raise ValueError("interval_seconds must be at least 300 seconds")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if self.max_rows_per_table <= 0:
            raise ValueError("max_rows_per_table must be positive")
        if self.max_runtime_seconds < 5:
            raise ValueError("max_runtime_seconds must be at least 5 seconds")


async def prune_account_snapshots_once(
    *,
    repository: AccountSnapshotRetentionRepository,
    environment: str,
    account_label: str,
    config: AccountSnapshotRetentionConfig,
    now: datetime | None = None,
    consumer_requirements: tuple[RetentionConsumerRequirement, ...] = (),
) -> dict[str, int]:
    observed_at = now or datetime.now(tz=UTC)
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    return await repository.prune_account_snapshots(
        environment=environment,
        account_label=account_label,
        before=observed_at - timedelta(days=config.retention_days),
        equity_before=observed_at - timedelta(days=config.equity_retention_days),
        batch_size=config.batch_size,
        max_rows_per_table=config.max_rows_per_table,
        consumer_requirements=consumer_requirements,
    )


async def run_account_snapshot_retention(
    *,
    repository: AccountSnapshotRetentionRepository,
    environment: str,
    account_label: str,
    config: AccountSnapshotRetentionConfig,
    consumer_requirements_provider: (
        Callable[[], Awaitable[tuple[RetentionConsumerRequirement, ...]]] | None
    ) = None,
    on_error: Callable[[Exception], None] | None = None,
    on_pruned: Callable[[dict[str, int]], None] | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    """Run low-rate, bounded retention without touching the sync fast path."""
    while True:
        await sleep(config.interval_seconds)
        try:
            async with asyncio.timeout(config.max_runtime_seconds):
                reqs: tuple[RetentionConsumerRequirement, ...] = ()
                if consumer_requirements_provider is not None:
                    try:
                        reqs = await consumer_requirements_provider()
                    except Exception as req_err:
                        if on_error is not None:
                            on_error(req_err)
                        continue
                deleted = await prune_account_snapshots_once(
                    repository=repository,
                    environment=environment,
                    account_label=account_label,
                    config=config,
                    consumer_requirements=reqs,
                )
        except Exception as error:
            if on_error is not None:
                on_error(error)
            continue
        if on_pruned is not None and any(deleted.values()):
            on_pruned(deleted)
