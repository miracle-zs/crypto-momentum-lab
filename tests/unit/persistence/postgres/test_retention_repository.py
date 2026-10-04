from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock

import pytest

from crypto_momentum_lab.domain.operational.retention_models import (
    ConsumerDependency,
    RecoverySpec,
)
from crypto_momentum_lab.persistence.postgres.retention_repository import (
    AsyncPostgresRetentionRepository,
)


@pytest.mark.parametrize("operation", ["save", "delete", "save_in_session"])
async def test_lock_failure_does_not_write_recovery_dependency(operation):
    session = AsyncMock()
    session.__aenter__.return_value = session
    session.execute.side_effect = RuntimeError("database lock unavailable")
    repository = AsyncPostgresRetentionRepository(Mock(return_value=session))
    dependency = ConsumerDependency(
        consumer_id="decision-replay",
        dataset_name="runtime_market_states_15s",
        generation=1,
        recovery_spec=RecoverySpec(
            source_dataset="runtime_market_states_15s",
            earliest_needed_watermark=datetime(2026, 10, 4, tzinfo=UTC),
        ),
        dependency_version="version-1",
    )

    with pytest.raises(RuntimeError, match="database lock unavailable"):
        if operation == "save":
            await repository.save_dependency(dependency)
        elif operation == "delete":
            await repository.delete_dependency(
                dependency.consumer_id, dependency.dataset_name
            )
        else:
            await repository.save_dependency_in_session(session, dependency)

    assert session.execute.await_count == 1
    session.merge.assert_not_awaited()
    session.get.assert_not_awaited()
    session.add.assert_not_called()
    session.commit.assert_not_awaited()
