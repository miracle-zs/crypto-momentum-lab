"""Bounded retention for high-churn operational tables.

These tables are serving the live control plane, not acting as an archive.  A
bounded, small-batch delete keeps the database's working set finite and avoids
one large transaction holding locks or generating a second memory spike.
"""

import logging
from datetime import UTC, datetime
from typing import Any, cast

from sqlalchemy import text
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.sql.elements import TextClause

from crypto_momentum_lab.domain.operational.retention_contract import (
    RetentionConsumerRequirement,
    RetentionWatermarkEvaluator,
)
from crypto_momentum_lab.domain.operational.retention_models import (
    resolve_dataset_scope,
)
from crypto_momentum_lab.persistence.postgres.runtime_state_partitions import (
    RUNTIME_STATE_PARTITION_LOOKAHEAD,
    drop_expired_runtime_state_partitions,
    ensure_runtime_state_partitions,
)
from crypto_momentum_lab.persistence.postgres.strategy_runtime_event_partitions import (
    EVENT_PARTITION_LOOKAHEAD,
    ensure_event_partitions,
)

_logger = logging.getLogger(__name__)


class PostgresOperationalRetentionRepository:
    """Prune data that can be rebuilt from the market-data archive."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._session_factory = session_factory

    async def prune_contract_metadata(
        self,
        *,
        before: datetime,
        batch_size: int = 1_000,
        consumer_requirements: tuple[RetentionConsumerRequirement, ...] = (),
    ) -> int:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        effective_before = before
        if consumer_requirements:
            gating = RetentionWatermarkEvaluator.evaluate_cutoff(
                requested_cutoff=before,
                requirements=consumer_requirements,
            )
            effective_before = gating.effective_cutoff

        # Keep the newest snapshot for every symbol even when it is older than
        # the retention horizon.  The live runner needs one usable rule row;
        # deleting the only row makes an unchanged in-memory metadata cache
        # look valid while the database is empty.
        statement = text(
            "WITH doomed AS ("
            "SELECT metadata.ctid "
            "FROM contract_metadata AS metadata "
            "WHERE metadata.effective_at < :before "
            "AND EXISTS ("
            "SELECT 1 FROM contract_metadata AS newer "
            "WHERE newer.symbol = metadata.symbol "
            "AND newer.effective_at > metadata.effective_at"
            ") "
            "ORDER BY metadata.effective_at "
            "LIMIT :batch_size"
            ") "
            "DELETE FROM contract_metadata AS metadata "
            "USING doomed "
            "WHERE metadata.ctid = doomed.ctid"
        )
        return await self._execute_delete(
            statement,
            before=effective_before,
            batch_size=batch_size,
            dataset_name="contract_metadata",
        )

    async def prune_runtime_market_states(
        self,
        *,
        before: datetime,
        batch_size: int = 1_000,
        consumer_requirements: tuple[RetentionConsumerRequirement, ...] = (),
    ) -> int:
        effective_before = before
        if consumer_requirements:
            gating = RetentionWatermarkEvaluator.evaluate_cutoff(
                requested_cutoff=before,
                requirements=consumer_requirements,
            )
            effective_before = gating.effective_cutoff

        await ensure_runtime_state_partitions(
            self._session_factory,
            through=datetime.now(UTC) + RUNTIME_STATE_PARTITION_LOOKAHEAD,
        )
        return await drop_expired_runtime_state_partitions(
            self._session_factory,
            before=effective_before,
        )

    async def ensure_strategy_runtime_event_partitions(self) -> int:
        """Create missing daily event partitions."""
        try:
            observed_at = datetime.now(UTC)
            return await ensure_event_partitions(
                self._session_factory,
                through=observed_at + EVENT_PARTITION_LOOKAHEAD,
            )
        except Exception:
            _logger.exception(
                "Failed to ensure strategy runtime event partitions; continuing"
            )
            return 0

    async def _execute_delete(
        self,
        statement: TextClause,
        *,
        before: datetime,
        batch_size: int,
        dataset_name: str | None = None,
    ) -> int:
        async with self._session_factory() as session:
            async with session.begin():
                if dataset_name:
                    scope = resolve_dataset_scope(dataset_name)
                    for lock_key in scope.advisory_lock_keys:
                        await session.execute(
                            text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"),
                            {"lock_key": lock_key},
                        )
                result = cast(
                    CursorResult[Any],
                    await session.execute(
                        statement,
                        {"before": before, "batch_size": batch_size},
                    ),
                )
                return max(result.rowcount or 0, 0)
