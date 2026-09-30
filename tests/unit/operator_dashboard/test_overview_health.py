from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from crypto_momentum_lab.operator_dashboard.overview_queries import OverviewQueries
from crypto_momentum_lab.operator_dashboard.schemas import (
    LiveAccountsResponse,
    LiveAccountSummaryResponse,
    ServiceStatusResponse,
    SystemOverviewResponse,
)
from crypto_momentum_lab.operator_dashboard.status import OperationalStatus

NOW = datetime(2026, 9, 30, 2, tzinfo=UTC)


class Session:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        pass

    async def scalars(self, _statement):
        head = SimpleNamespace(
            account_label="primary",
            status="ready",
            mismatch_count=0,
        )
        return SimpleNamespace(all=lambda: [head])


class Queries(OverviewQueries):
    def __init__(self, *, strategy_state, status=OperationalStatus.READY):
        super().__init__(
            Session,
            clock=lambda: NOW,
            stale_after_seconds=180,
            research_collector_root="/nonexistent",
        )
        self.account = LiveAccountSummaryResponse(
            account_label="primary",
            environment="live",
            status=status,
            readiness="ready_readonly"
            if status == OperationalStatus.READY
            else "syncing",
            observed_at=NOW,
            strategy_name="orderflow_impulse",
            strategy_state=strategy_state,
            lease_expires_at=NOW + timedelta(days=1),
        )

    async def health(self):
        return {"app_status": "UP", "database_status": "UP"}

    async def live_accounts(self):
        return LiveAccountsResponse(status=self.account.status, accounts=[self.account])

    async def overview(self):
        return SystemOverviewResponse(
            generated_at=NOW,
            database_status=OperationalStatus.READY,
            services=[
                ServiceStatusResponse(
                    name="strategy-runner",
                    status=OperationalStatus.FRESH,
                    observed_at=NOW,
                    age_seconds=0,
                )
            ],
            active_halt_count=0,
            active_lease=None,
        )


@pytest.mark.parametrize(
    "state, reason",
    [
        (None, "strategy_state_unconfirmed"),
        ("halted", "strategy_not_active"),
    ],
)
async def test_valid_lease_is_not_blamed_for_missing_or_inactive_strategy(
    state, reason
):
    queries = Queries(strategy_state=state)
    view = await queries.operational_health()
    cap = next(d for d in view["dimensions"] if d["name"] == "executable_capability")
    assert reason in cap["details"]
    assert "lease_expired" not in cap["details"]
    readiness = await queries.readiness()
    assert readiness.tradeability.entry_gate_open is False
    assert readiness.tradeability.entry_gate_reason == reason


@pytest.mark.parametrize(
    "status", [OperationalStatus.READY, OperationalStatus.DEGRADED]
)
async def test_process_state_does_not_invent_a_fact_gap_count(status):
    view = await Queries(strategy_state="running", status=status).operational_health()
    fact = next(d for d in view["dimensions"] if d["name"] == "fact_integrity")
    assert fact["status"] == "unknown"
    assert fact["metric_value"] is None
    assert "unconfirmed" in fact["details"]
