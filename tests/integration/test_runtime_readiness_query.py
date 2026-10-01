"""Published gates retain identity and freshness checks on real PostgreSQL."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.operator_dashboard.overview_queries import OverviewQueries
from crypto_momentum_lab.persistence.postgres.models import (
    ExecutionAccountProcessStateRow,
    StrategyRuntimeEventRow,
    TradingLeaseRow,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)


@pytest.mark.parametrize(
    "condition",
    [
        "valid",
        "wrong_commit",
        "wrong_session",
        "stale",
        "future",
        "missing_lease",
        "invalid_bool",
    ],
)
async def test_published_gate_is_account_scoped_and_requires_current_proof(
    async_database_url, tmp_path, condition
):
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    prefix = "gate-" + uuid4().hex[:12]
    accounts = (prefix + "-a", prefix + "-b")
    run_id = prefix + "-run"
    rows = []
    for index, account in enumerate(accounts):
        rows.append(
            ExecutionAccountProcessStateRow(
                state_id=uuid4(),
                environment="live",
                account_label=account,
                state="ready_readonly",
                occurred_at=now,
                reason=None,
            )
        )
        if index == 0 or condition != "missing_lease":
            rows.append(
                TradingLeaseRow(
                    lease_id=account,
                    environment="live",
                    account_label=account,
                    strategy_name="momentum",
                    owner=run_id,
                    code_generation="commit",
                    state="active",
                    acquired_at=now - timedelta(minutes=1),
                    expires_at=now + timedelta(minutes=1),
                )
            )
        payload = {
            "schema_version": 1,
            "account_label": account,
            "session_id": run_id,
            "code_commit": "commit",
            "tradeability": {
                "mode": "FULLY_TRADEABLE" if index == 0 else "EXIT_ONLY",
                "entry_gate_open": index == 0,
                "entry_gate_reason": "ready"
                if index == 0
                else "strategy_warmup_incomplete",
                "exit_gate_open": True,
                "exit_gate_reason": "ready",
                "unmanaged_risk_clear": True,
                "halt_active": False,
            },
        }
        observed = now
        if index == 1:
            if condition == "wrong_commit":
                payload["code_commit"] = "previous"
            elif condition == "wrong_session":
                payload["session_id"] = "other"
            elif condition == "stale":
                observed -= timedelta(seconds=181)
            elif condition == "future":
                observed += timedelta(seconds=1)
            elif condition == "invalid_bool":
                payload["tradeability"]["entry_gate_open"] = "false"
        rows.append(
            StrategyRuntimeEventRow(
                event_id=account,
                run_id=run_id,
                event_type="runtime_readiness",
                occurred_at=observed,
                symbol=None,
                bucket_start=None,
                details=payload,
            )
        )
    try:
        async with factory() as session, session.begin():
            session.add_all(rows)
        queries = OverviewQueries(
            factory,
            clock=lambda: now,
            stale_after_seconds=180,
            research_collector_root=tmp_path,
        )
        result = await queries.live_accounts()
        by_account = {account.account_label: account for account in result.accounts}
        assert by_account[accounts[0]].runtime_tradeability.entry_gate_open
        other = by_account[accounts[1]]
        if condition == "valid":
            assert not other.runtime_tradeability.entry_gate_open
            assert (
                other.runtime_tradeability.entry_gate_reason
                == "strategy_warmup_incomplete"
            )
            assert other.runtime_observed_at == now
        else:
            assert other.runtime_tradeability is None
            assert other.runtime_observed_at is None
    finally:
        async with factory() as session, session.begin():
            await session.execute(
                delete(StrategyRuntimeEventRow).where(
                    StrategyRuntimeEventRow.run_id == run_id
                )
            )
            await session.execute(
                delete(TradingLeaseRow).where(
                    TradingLeaseRow.account_label.in_(accounts)
                )
            )
            await session.execute(
                delete(ExecutionAccountProcessStateRow).where(
                    ExecutionAccountProcessStateRow.account_label.in_(accounts)
                )
            )
        await engine.dispose()
