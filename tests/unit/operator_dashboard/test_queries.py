from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

from crypto_momentum_lab.operator_dashboard.overview_queries import (
    universe_membership as _universe_membership,
)
from crypto_momentum_lab.operator_dashboard.queries import DashboardQueries
from crypto_momentum_lab.operator_dashboard.status import OperationalStatus
from crypto_momentum_lab.operator_dashboard.telemetry_queries import (
    DecisionSLOQueries,
    _decision_slo_response,
)


async def test_universe_dashboard_hides_loser_ranking() -> None:
    observed_at = datetime(2026, 9, 14, 0, 0, tzinfo=UTC)
    snapshot = SimpleNamespace(snapshot_id="snapshot", observed_at=observed_at)
    entries = (
        SimpleNamespace(
            symbol="GAINERUSDT",
            gainer_rank=1,
            loser_rank=None,
            utc_day_return=Decimal("0.1"),
            current_price=Decimal("1.1"),
        ),
        SimpleNamespace(
            symbol="LOSERUSDT",
            gainer_rank=None,
            loser_rank=1,
            utc_day_return=Decimal("-0.1"),
            current_price=Decimal("0.9"),
        ),
    )

    class Session:
        def __init__(self) -> None:
            self.scalars_calls = 0

        async def scalar(self, _statement):
            return snapshot

        async def scalars(self, _statement):
            self.scalars_calls += 1
            return SimpleNamespace(
                all=lambda: entries if self.scalars_calls == 1 else ()
            )

    class SessionContext:
        def __init__(self, session: Session) -> None:
            self.session = session

        async def __aenter__(self) -> Session:
            return self.session

        async def __aexit__(self, *_args) -> None:
            return None

    class SessionFactory:
        def __init__(self, session: Session) -> None:
            self.session = session

        def __call__(self) -> SessionContext:
            return SessionContext(self.session)

    result = await DashboardQueries(
        SessionFactory(Session()),
        clock=lambda: observed_at,
        nextgen_accounts=(),
    ).universe()

    assert [row["symbol"] for row in result.gainers] == ["GAINERUSDT"]
    assert result.losers == []

def test_decision_slo_response_aggregates_latency_health_and_reasons() -> None:
    start = datetime(2026, 9, 11, tzinfo=UTC)
    rows = [
        SimpleNamespace(
            event_type="candidate_accepted",
            occurred_at=start + timedelta(milliseconds=300),
            details={
                "decision_slo_latency_ms": {
                    "market_state_received->context_ready": 100.0,
                    "context_ready->candidate_accepted": 200.0,
                }
            },
        ),
        SimpleNamespace(
            event_type="intent_saved",
            occurred_at=start + timedelta(milliseconds=500),
            details={
                "decision_slo_latency_ms": {
                    "candidate_accepted->intent_saved": 200.0,
                }
            },
        ),
        SimpleNamespace(
            event_type="exchange_request_started",
            occurred_at=start + timedelta(milliseconds=700),
            details={
                "decision_slo_latency_ms": {
                    "intent_saved->exchange_request_started": 200.0,
                }
            },
        ),
        SimpleNamespace(
            event_type="terminal_reason",
            occurred_at=start + timedelta(seconds=1),
            details={
                "lane": "entry",
                "trigger_source": "market",
                "reason": "risk_block",
            },
        ),
        SimpleNamespace(
            event_type="consumer_health",
            occurred_at=start + timedelta(seconds=2),
            details={
                "consumer": "market_state_hub",
                "available": False,
                "recovery": False,
                "lag": True,
                "reason": "market_state_consumer_lagged",
            },
        ),
        SimpleNamespace(
            event_type="consumer_health",
            occurred_at=start + timedelta(seconds=3),
            details={
                "consumer": "market_state_hub",
                "available": True,
                "recovery": True,
                "lag": False,
                "reason": "reconnected",
            },
        ),
    ]

    response = _decision_slo_response(
        rows,
        window="24h",
        window_start=start,
        window_end=start + timedelta(days=1),
        truncated=False,
    )

    assert response.status is OperationalStatus.READY
    assert response.persisted_event_count == len(rows)
    assert (
        response.phase_latency["market_state_received->context_ready"].p95_ms == 100.0
    )
    assert (
        response.phase_latency["intent_saved->exchange_request_started"].max_ms == 200.0
    )
    assert response.terminal_reasons == {"entry": {"market": {"risk_block": 1}}}
    assert response.consumers[0].consumer == "market_state_hub"
    assert response.consumers[0].recovery_count == 1
    assert response.consumers[0].unavailable_event_count == 1
    assert response.consumers[0].lag_event_count == 1
    assert response.consumers[0].last_available is True

def test_decision_slo_response_preserves_true_unfiltered_latencies() -> None:
    start = datetime(2026, 9, 11, tzinfo=UTC)
    rows = [
        SimpleNamespace(
            event_type="risk_approved",
            occurred_at=start + timedelta(milliseconds=10),
            details={
                "previous_phase": "candidate_accepted",
                "latency_ms_from_previous": 0.5,
            },
        ),
        SimpleNamespace(
            event_type="risk_approved",
            occurred_at=start + timedelta(seconds=54),
            details={
                "previous_phase": "candidate_accepted",
                "latency_ms_from_previous": 54000.0,
            },
        ),
    ]

    response = _decision_slo_response(
        rows,
        window="24h",
        window_start=start,
        window_end=start + timedelta(days=1),
        truncated=False,
    )
    samples = response.phase_latency["candidate_accepted->risk_approved"]
    assert samples.sample_count == 2
    assert samples.max_ms == 54000.0

async def test_decision_slo_query_uses_bounded_historical_window() -> None:
    window_end = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)
    event = SimpleNamespace(
        event_type="consumer_health",
        occurred_at=window_end,
        details={
            "consumer": "account_event_hub",
            "available": True,
            "recovery": False,
            "lag": False,
        },
    )

    class Session:
        statement = None

        async def scalars(self, statement):
            self.statement = statement
            return SimpleNamespace(all=lambda: (event,))

        async def execute(self, statement):
            self.statement = statement
            return SimpleNamespace(all=lambda: (event,))

    class SessionContext:
        def __init__(self, session: Session) -> None:
            self.session = session

        async def __aenter__(self) -> Session:
            return self.session

        async def __aexit__(self, *_args) -> None:
            return None

    class SessionFactory:
        def __init__(self, session: Session) -> None:
            self.session = session

        def __call__(self) -> SessionContext:
            return SessionContext(self.session)

    session = Session()
    queries = DecisionSLOQueries(
        SessionFactory(session),
        clock=lambda: window_end,
    )

    response = await queries.decision_slo("6h")

    assert response.window == "6h"
    assert response.window_start == window_end - timedelta(hours=6)
    assert response.window_end == window_end
    assert response.persisted_event_count == 1
    assert session.statement is not None

def test_universe_membership_carries_rank_and_market_fields() -> None:
    membership = SimpleNamespace(
        symbol="AAAUSDT",
        status="retained",
        side="gainer",
    )
    entry = SimpleNamespace(
        symbol="AAAUSDT",
        gainer_rank=25,
        loser_rank=None,
        utc_day_return=Decimal("0.0123"),
        current_price=Decimal("10.50"),
    )

    result = _universe_membership(membership, entry)

    assert result == {
        "symbol": "AAAUSDT",
        "status": "retained",
        "side": "gainer",
        "rank": 25,
        "utc_day_return": "0.0123",
        "current_price": "10.50",
    }
