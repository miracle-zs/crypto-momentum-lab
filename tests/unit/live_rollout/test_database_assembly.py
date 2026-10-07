"""Owned database resources close on success and partial startup failure."""

from types import SimpleNamespace

import pytest
from sqlalchemy.pool import NullPool

from crypto_momentum_lab.live_rollout.runtime_session import ResourceOwnershipRegistry
from crypto_momentum_lab.persistence.postgres import session as session_module
from crypto_momentum_lab.persistence.postgres.live_runtime_assembly import (
    assemble_live_persistence,
)


class Engine:
    def __init__(self):
        self.disposed = False
        self.sync_engine = SimpleNamespace(pool=NullPool(lambda: None))

    async def dispose(self):
        self.disposed = True


@pytest.mark.parametrize("failing_database", (None, "market", "obs"))
async def test_teardown_closes_every_created_database_resource(
    monkeypatch,
    failing_database,
):
    created = []

    def create(database_url, **kwargs):
        if failing_database and database_url.endswith("/" + failing_database):
            raise RuntimeError("database unavailable")
        engine = Engine()
        created.append(engine)
        return engine

    monkeypatch.setattr(session_module, "create_async_engine", create)
    ownership = ResourceOwnershipRegistry(run_id="assembly-test")

    def assemble():
        return assemble_live_persistence(
            execution_database_url="postgresql+asyncpg://host/exec",
            market_database_url="postgresql+asyncpg://host/market",
            observability_database_url="postgresql+asyncpg://host/obs",
            account_label="test_acc", strategy_name="test_strat",
            ownership_registry=ownership,
        )

    if failing_database:
        with pytest.raises(RuntimeError, match="database unavailable"):
            assemble()
    else:
        assemble()
    assert created
    assert all(not engine.disposed for engine in created)
    await ownership.teardown_all()
    assert all(engine.disposed for engine in created)
    await ownership.teardown_all()
