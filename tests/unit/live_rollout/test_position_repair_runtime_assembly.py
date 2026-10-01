import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.live_rollout.position_self_healing import (
    LiveUnmanagedPositionRepair,
)
from crypto_momentum_lab.live_rollout.postgres_runtime import (
    PostgresLiveContextProvider,
)
from crypto_momentum_lab.persistence.postgres.position_repair import (
    PostgresPositionRepairUnitOfWork,
)


@pytest.mark.parametrize("strategy_name", ["orderflow_impulse", "another_strategy"])
def test_repair_module_owns_real_transaction_with_strategy_scope(
    strategy_name,
):
    sessions = async_sessionmaker()
    repair = LiveUnmanagedPositionRepair(
        account_label="primary", run_id="live-test", book=ExecutionBook(),
        uow=PostgresPositionRepairUnitOfWork(sessions, strategy_name=strategy_name),
        context_is_current=lambda context: provider.is_current(context),
        invalidate_context=lambda: provider.invalidate(), request_recovery=lambda: None,
    )
    provider = PostgresLiveContextProvider(
        session_factory=sessions,
        account_label="primary",
        run_id="live-test",
        strategy_name=strategy_name,
        strategy_config_hash="config",
        git_commit_hash="commit",
        migration_revision="revision",
        lease_owner="owner",
        approval_id="approval",
        request_position_repair=repair.request,
    )
    assert not hasattr(provider, "_position_repair_uow")
    reservations = repair._uow._execution._reservation_repository
    assert reservations._strategy_name == strategy_name
