from __future__ import annotations

import pytest

from crypto_momentum_lab.live_rollout.database_assembly import (
    LivePersistenceAssembly,
    assemble_live_persistence,
)
from crypto_momentum_lab.live_rollout.runtime_session import (
    ResourceOwnershipRegistry,
)
from crypto_momentum_lab.persistence.postgres import session as session_module


class _FakeEngine:
    def __init__(self, name: str) -> None:
        self.name = name
        self.disposed = False

    async def dispose(self) -> None:
        self.disposed = True


@pytest.mark.asyncio
async def test_assemble_live_persistence_creates_and_registers_all(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created_engines: list[tuple[str, dict[str, object]]] = []

    def fake_create_async_engine(database_url: str, **kwargs: object) -> _FakeEngine:
        created_engines.append((database_url, dict(kwargs)))
        return _FakeEngine(database_url)

    monkeypatch.setattr(
        session_module, "create_async_engine", fake_create_async_engine
    )

    ownership = ResourceOwnershipRegistry(run_id="run-assembly-test")
    assembly = assemble_live_persistence(
        execution_database_url="postgresql+asyncpg://host/exec",
        market_database_url="postgresql+asyncpg://host/market",
        observability_database_url="postgresql+asyncpg://host/obs",
        account_label="test_acc",
        strategy_name="test_strat",
        ownership_registry=ownership,
    )

    assert isinstance(assembly, LivePersistenceAssembly)

    # 5 engines created
    assert len(created_engines) == 5

    # Heartbeat engine has dedicated single-connection pool parameters
    heartbeat_call = created_engines[4]
    assert heartbeat_call[0] == "postgresql+asyncpg://host/exec"
    assert heartbeat_call[1]["pool_size"] == 1
    assert heartbeat_call[1]["max_overflow"] == 0
    assert heartbeat_call[1]["pool_timeout"] == 3.0

    # 5 engines registered in ownership_registry
    registered_names = [res.name for res in ownership._resources]
    assert registered_names == [
        "execution_engine",
        "market_engine",
        "observability_engine",
        "checkpoint_engine",
        "heartbeat_engine",
    ]

    # Teardown properly disposes all engines in reverse order
    await ownership.teardown_all()
    assert assembly.engines.heartbeat_engine.disposed  # type: ignore[attr-defined]
    assert assembly.engines.checkpoint_engine.disposed  # type: ignore[attr-defined]
    assert assembly.engines.observability_engine.disposed  # type: ignore[attr-defined]
    assert assembly.engines.market_engine.disposed  # type: ignore[attr-defined]
    assert assembly.engines.execution_engine.disposed  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_assemble_live_persistence_teardown_on_engine_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    call_count = 0

    def fake_create_async_engine(database_url: str, **kwargs: object) -> _FakeEngine:
        nonlocal call_count
        call_count += 1
        if call_count == 3:
            raise RuntimeError("Observability engine connection failure")
        return _FakeEngine(database_url)

    monkeypatch.setattr(
        session_module, "create_async_engine", fake_create_async_engine
    )

    ownership = ResourceOwnershipRegistry(run_id="run-fail-test")

    with pytest.raises(RuntimeError, match="Observability engine connection failure"):
        assemble_live_persistence(
            execution_database_url="postgresql+asyncpg://host/exec",
            market_database_url="postgresql+asyncpg://host/market",
            observability_database_url="postgresql+asyncpg://host/obs",
            account_label="test_acc",
            strategy_name="test_strat",
            ownership_registry=ownership,
        )

    # First two engines were registered before the failure
    registered_names = [res.name for res in ownership._resources]
    assert registered_names == ["execution_engine", "market_engine"]

    # Verify teardown_all cleanly disposes the engines created before failure
    await ownership.teardown_all()
    assert len(ownership._resources) == 0
