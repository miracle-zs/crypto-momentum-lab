from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock

import pytest

import crypto_momentum_lab.persistence.postgres.operational_retention as retention
from crypto_momentum_lab.persistence.postgres.operational_retention import (
    PostgresOperationalRetentionRepository,
)


async def test_partition_failure_does_not_switch_to_row_deletion(monkeypatch) -> None:
    monkeypatch.setattr(
        retention,
        "ensure_runtime_state_partitions",
        AsyncMock(side_effect=RuntimeError("partition DDL failed")),
    )
    sessions = Mock(side_effect=AssertionError("unexpected database session"))
    repository = PostgresOperationalRetentionRepository(sessions)
    with pytest.raises(RuntimeError, match="partition DDL failed"):
        await repository.prune_runtime_market_states(
            before=datetime(2026, 10, 1, tzinfo=UTC)
        )
    sessions.assert_not_called()
