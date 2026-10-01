import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from crypto_momentum_lab.domain.account import ExecutionAccountStatus
from crypto_momentum_lab.domain.execution.order_state import ExchangeOrderState
from crypto_momentum_lab.live_rollout import postgres_runtime


def setup_reader(monkeypatch):
    trade_pool = object()
    heartbeat_pool = object()
    provider = postgres_runtime.PostgresLiveContextProvider(
        session_factory=trade_pool, account_label="primary", run_id="run",
        strategy_name="strategy", strategy_config_hash="config", git_commit_hash="commit",
        migration_revision="migration", lease_owner="owner", approval_id="approval",
    )
    live = SimpleNamespace(load_active_approval=AsyncMock(return_value=None))
    risk = SimpleNamespace(load_active_lease=AsyncMock(return_value=None),
                           load_active_halts=AsyncMock(return_value=()))
    orders = SimpleNamespace(load_unresolved_orders=AsyncMock(return_value=(
        SimpleNamespace(state=ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION),)))
    pools = []
    for name, repository in (("PostgresLiveRolloutRepository", live),
                             ("PostgresRiskRepository", risk),
                             ("PostgresOrderReadRepository", orders)):
        def factory(pool, repository=repository):
            pools.append(pool)
            return repository
        monkeypatch.setattr(postgres_runtime, name, factory)
    config = object()
    monkeypatch.setattr(postgres_runtime, "_latest_risk_config", AsyncMock(return_value=config))
    monkeypatch.setattr(postgres_runtime, "_latest_account_state", AsyncMock(return_value=ExecutionAccountStatus.READY_READONLY))
    return provider, heartbeat_pool, pools, live, risk, orders, config


async def test_gate_reader_uses_isolated_pool_without_full_context_or_market(monkeypatch):
    provider, pool, pools, live, risk, orders, config = setup_reader(monkeypatch)
    # A stuck transaction/context read must not own lease recovery.
    await provider._context_load_lock.acquire()
    provider._load_context_once = AsyncMock(side_effect=AssertionError("full context read"))
    provider._account_position_view = AsyncMock(side_effect=AssertionError("position read"))
    provider._load_symbol_rules = AsyncMock(side_effect=AssertionError("market rules read"))
    try:
        gate = await asyncio.wait_for(provider.load_lease_gate(pool), 1)
    finally:
        provider._context_load_lock.release()
    assert pools == [pool, pool, pool]
    assert gate.risk_config is config
    assert gate.active_lease is None
    assert gate.account_state is ExecutionAccountStatus.READY_READONLY
    assert gate.unresolved_order_states == (ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,)
    orders.load_unresolved_orders.assert_awaited_once_with("run")
    assert provider.cached_context is None


async def test_gate_reader_rejects_control_facts_changed_during_read(monkeypatch):
    provider, pool, *_ = setup_reader(monkeypatch)
    started = asyncio.Event()
    release = asyncio.Event()

    async def config(*args):
        started.set()
        await release.wait()
        return object()

    monkeypatch.setattr(postgres_runtime, "_latest_risk_config", config)
    task = asyncio.create_task(provider.load_lease_gate(pool))
    try:
        await asyncio.wait_for(started.wait(), 1)
        provider.invalidate_account_snapshot()
        release.set()
        with pytest.raises(RuntimeError, match="control facts changed"):
            await task
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_gate_reader_uses_shared_ws_readiness_and_pinned_approval(monkeypatch):
    provider, pool, pools, live, *_ = setup_reader(monkeypatch)
    provider._realtime_account_state = ExecutionAccountStatus.SYNCING
    live.load_active_approval.return_value = SimpleNamespace(approval_id="different")
    gate = await provider.load_lease_gate(pool)
    assert gate.account_state is ExecutionAccountStatus.SYNCING
    assert gate.approval is None
    postgres_runtime._latest_account_state.assert_not_awaited()
