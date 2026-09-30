import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.live_rollout.postgres_runtime import (
    PostgresLiveContextProvider,
)


@pytest.mark.parametrize("strategy_name", ["orderflow_impulse", "another_strategy"])
def test_live_context_constructs_real_position_repair_with_strategy_scope(
    strategy_name,
):
    provider = PostgresLiveContextProvider(
        session_factory=async_sessionmaker(),
        account_label="primary",
        run_id="live-test",
        strategy_name=strategy_name,
        strategy_config_hash="config",
        git_commit_hash="commit",
        migration_revision="revision",
        lease_owner="owner",
        approval_id="approval",
    )
    reservations = provider._position_repair_uow._execution._reservation_repository
    assert reservations._strategy_name == strategy_name
