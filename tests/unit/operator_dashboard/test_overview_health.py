from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from crypto_momentum_lab.operator_dashboard.overview_queries import OverviewQueries
from crypto_momentum_lab.operator_dashboard.schemas import (
    LiveAccountsResponse,
    LiveAccountSummaryResponse,
    ServiceStatusResponse,
    SystemOverviewResponse,
    TradeabilityDetailResponse,
)
from crypto_momentum_lab.operator_dashboard.status import OperationalStatus

NOW = datetime(2026, 9, 30, 2, tzinfo=UTC)


@pytest.mark.parametrize("durable_halt", [False, True])
async def test_action_halt_preserves_published_running_mode(durable_halt):
    queries = Queries(strategy_state="active")
    queries.account.runtime_observed_at = NOW
    queries.account.runtime_tradeability = TradeabilityDetailResponse(
        mode="RUNNING",
        entry_gate_open=False,
        entry_gate_reason="halt_active",
        exit_gate_open=False,
        exit_gate_reason="operator_paused_exit",
        unmanaged_risk_clear=True,
        halt_active=True,
    )
    overview = await queries.overview()
    overview.active_halt_count = int(durable_halt)

    async def current_overview():
        return overview

    queries.overview = current_overview
    result = await queries.readiness()
    assert result.tradeability.mode == "RUNNING"
    assert result.tradeability.halt_active
    assert not result.tradeability.entry_gate_open
    assert not result.tradeability.exit_gate_open
    assert result.status is OperationalStatus.HALTED


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
    assert "runtime_readiness_missing_or_stale" in cap["details"]
    assert "lease_expired" not in cap["details"]
    readiness = await queries.readiness()
    assert readiness.tradeability.entry_gate_open is False
    assert (
        readiness.tradeability.entry_gate_reason == "runtime_readiness_missing_or_stale"
    )


@pytest.mark.parametrize(
    "status", [OperationalStatus.READY, OperationalStatus.DEGRADED]
)
async def test_process_state_does_not_invent_a_fact_gap_count(status):
    view = await Queries(strategy_state="running", status=status).operational_health()
    fact = next(d for d in view["dimensions"] if d["name"] == "fact_integrity")
    assert fact["status"] == "unknown"
    assert fact["metric_value"] is None
    assert "unconfirmed" in fact["details"]


async def test_syncing_account_reports_syncing_not_readonly_permission():
    queries = Queries(strategy_state="active", status=OperationalStatus.DEGRADED)
    readiness = await queries.readiness()
    assert readiness.tradeability.mode == "UNKNOWN"
    assert (
        readiness.tradeability.entry_gate_reason == "runtime_readiness_missing_or_stale"
    )
    assert not readiness.tradeability.entry_gate_open
    assert not readiness.tradeability.exit_gate_open


async def test_ready_readonly_observer_can_support_live_strategy():
    queries = Queries(strategy_state="active")
    queries.account.runtime_observed_at = NOW
    queries.account.runtime_tradeability = TradeabilityDetailResponse(
        mode="FULLY_TRADEABLE",
        entry_gate_open=True,
        entry_gate_reason="live_entry_prerequisites_ready",
        exit_gate_open=True,
        exit_gate_reason="normal",
        unmanaged_risk_clear=True,
        halt_active=False,
    )
    readiness = await queries.readiness()
    assert readiness.tradeability.mode == "FULLY_TRADEABLE"
    assert readiness.tradeability.entry_gate_reason == "live_entry_prerequisites_ready"


@pytest.mark.parametrize("age", [181, -1])
async def test_old_or_future_runtime_result_cannot_authorize_display(age):
    queries = Queries(strategy_state="active")
    queries.account.runtime_observed_at = NOW - timedelta(seconds=age)
    queries.account.runtime_tradeability = TradeabilityDetailResponse(
        mode="FULLY_TRADEABLE",
        entry_gate_open=True,
        entry_gate_reason="ready",
        exit_gate_open=True,
        exit_gate_reason="normal",
        unmanaged_risk_clear=True,
        halt_active=False,
    )
    assert (await queries.readiness()).tradeability.mode == "UNKNOWN"


async def test_runtime_block_reason_survives_green_account_and_lease():
    queries = Queries(strategy_state="active")
    queries.account.runtime_observed_at = NOW
    queries.account.runtime_tradeability = TradeabilityDetailResponse(
        mode="EXIT_ONLY",
        entry_gate_open=False,
        entry_gate_reason="pending_live_positions:BTCUSDT",
        exit_gate_open=True,
        exit_gate_reason="normal",
        unmanaged_risk_clear=False,
        halt_active=False,
    )
    result = await queries.readiness()
    assert result.tradeability.entry_gate_reason == "pending_live_positions:BTCUSDT"
    assert result.tradeability.exit_gate_open
    view = await queries.operational_health()
    assert (
        "pending_live_positions:BTCUSDT"
        in next(d for d in view["dimensions"] if d["name"] == "executable_capability")[
            "details"
        ]
    )


async def test_fleet_block_does_not_rewrite_another_accounts_runtime_gate():
    queries = Queries(strategy_state="active")
    queries.account.runtime_observed_at = NOW
    queries.account.runtime_tradeability = TradeabilityDetailResponse(
        mode="FULLY_TRADEABLE",
        entry_gate_open=True,
        entry_gate_reason="ready",
        exit_gate_open=True,
        exit_gate_reason="normal",
        unmanaged_risk_clear=True,
        halt_active=False,
    )
    other = queries.account.model_copy(deep=True)
    other.account_label = "account-2"
    other.runtime_tradeability.entry_gate_open = False
    other.runtime_tradeability.entry_gate_reason = "strategy_warmup_incomplete"

    async def accounts():
        return LiveAccountsResponse(
            status=OperationalStatus.READY, accounts=[queries.account, other]
        )

    queries.live_accounts = accounts
    result = await queries.readiness()
    assert not result.tradeability.entry_gate_open
    assert result.tradeability.entry_gate_reason == "strategy_warmup_incomplete"
    assert result.accounts[0].runtime_tradeability.entry_gate_open
    assert not result.accounts[1].runtime_tradeability.entry_gate_open


@pytest.mark.parametrize("invalid", [None, "session", "boolean", "schema", "account"])
async def test_live_accounts_accepts_only_valid_runtime_identity(invalid):
    payload = {
        "schema_version": 1,
        "account_label": "primary",
        "code_commit": "commit",
        "session_id": "run",
        "tradeability": {
            "mode": "EXIT_ONLY",
            "entry_gate_open": False,
            "entry_gate_reason": "strategy_warmup_incomplete",
            "exit_gate_open": True,
            "exit_gate_reason": "normal",
            "unmanaged_risk_clear": True,
            "halt_active": False,
        },
    }
    if invalid == "account":
        payload["account_label"] = "other"
    elif invalid == "session":
        payload["session_id"] = "other"
    elif invalid == "boolean":
        payload["tradeability"]["entry_gate_open"] = "true"
    elif invalid == "schema":
        payload["schema_version"] = 2
    process = SimpleNamespace(
        account_label="primary",
        environment="live",
        state="ready_readonly",
        occurred_at=NOW,
    )
    strategy = SimpleNamespace(
        account_label="primary",
        strategy_name="strategy",
        state="active",
        changed_at=NOW,
    )
    lease = SimpleNamespace(
        account_label="primary",
        strategy_name="strategy",
        expires_at=NOW - timedelta(minutes=5),
        code_generation="old",
    )
    data = iter(([process], [strategy], [lease]))

    class DataSession(Session):
        async def scalars(self, statement):
            rows = next(data)
            return SimpleNamespace(all=lambda: rows)

        async def scalar(self, statement):
            return SimpleNamespace(details=payload, run_id="run", occurred_at=NOW)

    queries = OverviewQueries(
        DataSession,
        clock=lambda: NOW,
        stale_after_seconds=180,
        research_collector_root="/nonexistent",
    )
    result = await queries.live_accounts()
    account = result.accounts[0]
    if invalid is None:
        assert account.runtime_observed_at == NOW
        assert (
            account.runtime_tradeability.entry_gate_reason
            == "strategy_warmup_incomplete"
        )
    else:
        assert account.runtime_tradeability is None


def test_live_account_status_accepts_running():
    from crypto_momentum_lab.operator_dashboard.overview_queries import (
        live_account_status,
    )

    assert (
        live_account_status("running", observed_at=NOW, now=NOW)
        == OperationalStatus.READY
    )
    assert (
        live_account_status("ready_readonly", observed_at=NOW, now=NOW)
        == OperationalStatus.READY
    )
    assert (
        live_account_status("syncing", observed_at=NOW, now=NOW)
        == OperationalStatus.DEGRADED
    )
    assert (
        live_account_status("stopped", observed_at=NOW, now=NOW)
        == OperationalStatus.HALTED
    )


async def test_running_tradeability_reports_ready():
    queries = Queries(strategy_state="active")
    queries.account.runtime_observed_at = NOW
    queries.account.runtime_tradeability = TradeabilityDetailResponse(
        mode="RUNNING",
        entry_gate_open=True,
        entry_gate_reason="live_entry_prerequisites_ready",
        exit_gate_open=True,
        exit_gate_reason="normal",
        unmanaged_risk_clear=True,
        halt_active=False,
    )
    readiness = await queries.readiness()
    assert readiness.status == OperationalStatus.READY
    assert readiness.tradeability.mode == "RUNNING"
    assert readiness.tradeability.entry_gate_open is True
    assert readiness.tradeability.entry_gate_reason == "live_entry_prerequisites_ready"
