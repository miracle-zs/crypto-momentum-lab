from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

from sqlalchemy.dialects.postgresql import dialect as postgresql_dialect

from crypto_momentum_lab.operator_dashboard.common_equity import (
    EquityObservation as _EquityObservation,
)
from crypto_momentum_lab.operator_dashboard.common_equity import (
    build_common_equity_curve as _build_common_equity_curve,
)
from crypto_momentum_lab.operator_dashboard.common_equity import (
    common_equity_interval_seconds as _common_equity_interval_seconds,
)
from crypto_momentum_lab.operator_dashboard.common_equity import (
    live_account_equity_point as _live_account_equity_point,
)
from crypto_momentum_lab.operator_dashboard.common_equity import (
    live_aggregated_equity_observations as _live_aggregated_equity_observations,
)
from crypto_momentum_lab.operator_dashboard.overview_queries import (
    universe_membership as _universe_membership,
)
from crypto_momentum_lab.operator_dashboard.paper_account_queries import (
    _downsample_equity_snapshots,
    _is_dashboard_paper_run,
    _paper_account_summary,
    _paper_exit_label,
)
from crypto_momentum_lab.operator_dashboard.paper_equity_queries import (
    _live_common_equity_statement,
    _paper_common_equity_statement,
    _paper_equity_statement,
    _paper_first_equity_statement,
    _paper_latest_equity_statement,
)
from crypto_momentum_lab.operator_dashboard.queries import (
    FIXED_COMMON_EQUITY_START_AT,
    DashboardQueries,
    parse_common_equity_start_at,
    parse_live_cash_flow_adjustments,
)
from crypto_momentum_lab.operator_dashboard.status import OperationalStatus
from crypto_momentum_lab.operator_dashboard.telemetry_queries import (
    _decision_slo_response,
)
from crypto_momentum_lab.persistence.postgres.models import PaperEquitySnapshotRow

def test_downsample_equity_snapshots_keeps_latest_row_in_each_utc_bucket() -> None:
    start = datetime(2026, 7, 28, tzinfo=UTC)
    rows = [
        _snapshot("first", start + timedelta(seconds=10), "1000"),
        _snapshot("latest-in-bucket", start + timedelta(minutes=5, seconds=59), "1001"),
        _snapshot("next-bucket", start + timedelta(minutes=6), "1002"),
    ]

    sampled = _downsample_equity_snapshots(rows)

    assert [row.snapshot_id for row in sampled] == [
        "latest-in-bucket",
        "next-bucket",
    ]

async def test_recent_paper_history_keeps_open_rows_and_caps_closed_query() -> None:
    opened_at = datetime(2026, 8, 1, tzinfo=UTC)
    open_row = _paper_position_row(
        position_id="open",
        status="open",
        opened_at=opened_at,
    )
    closed_row = _paper_position_row(
        position_id="closed",
        status="closed",
        opened_at=opened_at - timedelta(days=1),
        closed_at=opened_at,
    )

    class Session:
        def __init__(self) -> None:
            self.scalar_calls = 0
            self.scalars_statements = []

        async def scalar(self, _statement):
            self.scalar_calls += 1
            return "paper-run" if self.scalar_calls == 1 else 501

        async def scalars(self, statement):
            self.scalars_statements.append(statement)
            return SimpleNamespace(
                all=lambda: (
                    (open_row,) if len(self.scalars_statements) == 1 else (closed_row,)
                )
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

    session = Session()
    queries = DashboardQueries(SessionFactory(session), nextgen_accounts=())

    result = await queries.paper_history("paper-run")

    assert result.closed_trade_count == 501
    assert result.history_complete is False
    assert [item["position_id"] for item in result.closed_trades] == ["closed"]
    assert len(result.trade_events) == 3

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

def test_downsample_equity_snapshots_caps_result_to_latest_240_buckets() -> None:
    start = datetime(2026, 7, 27, tzinfo=UTC)
    rows = [
        _snapshot(str(index), start + timedelta(minutes=6 * index), str(1000 + index))
        for index in range(242)
    ]

    sampled = _downsample_equity_snapshots(rows)

    assert len(sampled) == 240
    assert sampled[0].snapshot_id == "2"
    assert sampled[-1].snapshot_id == "241"

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
    queries = DashboardQueries(
        SessionFactory(session),
        clock=lambda: window_end,
        nextgen_accounts=(),
    )

    response = await queries.decision_slo("6h")

    assert response.window == "6h"
    assert response.window_start == window_end - timedelta(hours=6)
    assert response.window_end == window_end
    assert response.persisted_event_count == 1
    assert session.statement is not None

def test_common_equity_query_uses_indexable_lateral_bucket_lookups() -> None:
    statement = _paper_common_equity_statement(
        ["paper-a", "paper-b"],
        datetime(2026, 8, 21, 2, 45, tzinfo=UTC),
        datetime(2026, 8, 27, 17, 12, tzinfo=UTC),
    )

    sql = str(statement.compile(dialect=postgresql_dialect()))

    assert "generate_series" in sql
    assert "AS equity_buckets(bucket)" in sql
    assert "LATERAL" in sql
    assert "DISTINCT ON" not in sql

def test_common_equity_interval_keeps_long_history_bounded() -> None:
    start = datetime(2026, 8, 1, 2, 45, tzinfo=UTC)
    end = start + timedelta(days=365)

    interval_seconds = _common_equity_interval_seconds(
        start,
        end,
        max_points=240,
    )

    bucket_count = int((end - start).total_seconds()) // interval_seconds + 1

    assert interval_seconds >= 15 * 60
    assert interval_seconds % (15 * 60) == 0
    assert bucket_count <= 240

def test_common_equity_curve_caps_points_for_an_unbounded_caller() -> None:
    start = datetime(2026, 8, 1, 2, 45, tzinfo=UTC)
    interval_seconds = _common_equity_interval_seconds(
        start,
        start + timedelta(days=10),
        max_points=8,
    )
    observations = [
        _EquityObservation(
            observed_at=start + timedelta(minutes=15 * index),
            equity=Decimal(str(1000 + index)),
            source_observed_at=start + timedelta(minutes=15 * index),
        )
        for index in range(1_000)
    ]

    points, baseline = _build_common_equity_curve(
        observations,
        common_start_at=start,
        end_at=start + timedelta(days=10),
        interval_seconds=interval_seconds,
        max_points=8,
    )

    assert baseline is not None
    assert len(points) <= 8
    assert points[0]["delta"] == "0"

def test_paper_equity_query_is_bounded_and_narrow() -> None:
    statement = _paper_equity_statement(
        ["paper-a", "paper-b"],
        datetime(2026, 8, 1, tzinfo=UTC),
        datetime(2026, 8, 31, tzinfo=UTC),
    )

    sql = str(statement.compile(dialect=postgresql_dialect()))

    assert "generate_series" in sql
    assert "LATERAL" in sql
    assert "raw_payload" not in sql
    assert "DISTINCT ON" not in sql

def test_paper_latest_equity_query_uses_one_narrow_probe_per_run() -> None:
    statement = _paper_latest_equity_statement(["paper-a", "paper-b"])

    sql = str(statement.compile(dialect=postgresql_dialect()))

    assert "LATERAL" in sql
    assert "ORDER BY paper_equity_snapshots_1.observed_at DESC" in sql
    assert "raw_payload" not in sql
    assert "max(" not in sql.lower()

def test_live_common_equity_query_aggregates_latest_timestamp_per_bucket() -> None:
    statement = _live_common_equity_statement(
        environment="live",
        account_label="primary",
        window_start=datetime(2026, 8, 1, tzinfo=UTC),
        window_end=datetime(2026, 8, 2, tzinfo=UTC),
    )

    sql = str(statement.compile(dialect=postgresql_dialect()))

    assert "generate_series" in sql
    assert "LATERAL" in sql
    assert "GROUP BY" in sql
    assert "raw_payload" not in sql

def test_first_equity_query_uses_one_index_lookup_per_run() -> None:
    statement = _paper_first_equity_statement(["paper-a", "paper-b"])

    sql = str(statement.compile(dialect=postgresql_dialect()))

    assert "LATERAL" in sql
    assert "min(" not in sql.lower()


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

def test_candle_exit_label_includes_entry_filter_variants() -> None:
    portfolio = {
        "exit_mode": "candle_15m",
        "candle_confirmation_count": 1,
        "candle_minimum_holding_buckets": 0,
    }

    assert (
        _paper_exit_label(
            "candle_15m",
            portfolio,
            {"allow_long": True, "allow_short": False},
        )
        == "15M 收线退出 · 仅多头"
    )
    assert (
        _paper_exit_label(
            "candle_15m",
            portfolio,
            {
                "allow_long": True,
                "allow_short": False,
                "max_abs_aggressive_imbalance": "0.7113",
            },
        )
        == "15M 收线退出 · 仅多头 · 主动不平衡 ≤ 71.13%"
    )
    assert (
        _paper_exit_label(
            "candle_15m",
            portfolio,
            {
                "allow_long": True,
                "allow_short": False,
                "require_price_above_ema5": True,
                "require_price_above_ema10": True,
            },
        )
        == "15M 收线退出 · 仅多头 · 价格 > 15M EMA5 · 价格 > 15M EMA10"
    )

def test_candle_exit_label_includes_grace_recovery_threshold() -> None:
    assert (
        _paper_exit_label(
            "candle_15m",
            {
                "candle_grace_bars": 8,
                "candle_grace_profit_pct": "0.0058",
            },
        )
        == "反向后宽限 8 根 15M · 回收 +0.58%"
    )

def test_dashboard_excludes_fixed_exit_paper_accounts() -> None:
    assert not _is_dashboard_paper_run(
        SimpleNamespace(execution_config={"portfolio": {"exit_mode": "fixed"}})
    )
    assert _is_dashboard_paper_run(
        SimpleNamespace(execution_config={"portfolio": {"exit_mode": "candle_15m"}})
    )

def test_dashboard_marks_paper_account_stale_when_checkpoint_is_old() -> None:
    now = datetime(2026, 8, 21, 5, 15, tzinfo=UTC)
    summary = _paper_account_summary(
        SimpleNamespace(
            run_id="paper-account-old",
            strategy_name="orderflow_impulse",
            config_hash="config-hash",
            execution_config={"portfolio": {"exit_mode": "candle_15m"}},
        ),
        now=now,
        stale_after_seconds=120,
        checkpoint_at=now - timedelta(hours=2),
        open_position_count=0,
        closed_trade_count=0,
        winning_trade_count=0,
        latest_equity=None,
    )

    assert summary.status is OperationalStatus.STALE

def test_live_account_balance_becomes_equity_point() -> None:
    point = _live_account_equity_point(
        SimpleNamespace(
            observed_at=datetime(2026, 8, 16, 2, 0, tzinfo=UTC),
            wallet_balance=Decimal("282.28"),
            unrealized_pnl=Decimal("-5.48"),
        )
    )

    assert point["equity"] == "276.80"
    assert point["balance"] == "282.28"
    assert point["unrealized_pnl"] == "-5.48"

def test_common_equity_curve_starts_at_zero_and_carries_latest_15m_bucket() -> None:
    common_start = datetime(2026, 8, 21, 2, 45, tzinfo=UTC)
    points, baseline = _build_common_equity_curve(
        [
            _EquityObservation(
                observed_at=common_start + timedelta(minutes=2),
                equity=Decimal("1000"),
                source_observed_at=common_start + timedelta(minutes=2),
            ),
            _EquityObservation(
                observed_at=common_start + timedelta(minutes=17),
                equity=Decimal("1002"),
                source_observed_at=common_start + timedelta(minutes=17),
            ),
            _EquityObservation(
                observed_at=common_start + timedelta(minutes=31),
                equity=Decimal("1001"),
                source_observed_at=common_start + timedelta(minutes=31),
            ),
        ],
        common_start_at=common_start,
        end_at=common_start + timedelta(minutes=45),
    )

    assert baseline == Decimal("1000")
    assert [point["delta"] for point in points] == [
        "0",
        "2",
        "1",
        "1",
    ]
    assert all(point["return_pct"] is not None for point in points)

def test_common_equity_curve_respects_shared_source_watermark() -> None:
    common_start = datetime(2026, 8, 21, 2, 45, tzinfo=UTC)
    points, baseline = _build_common_equity_curve(
        [
            _EquityObservation(
                observed_at=common_start + timedelta(minutes=2),
                equity=Decimal("1000"),
                source_observed_at=common_start + timedelta(minutes=2),
            ),
            _EquityObservation(
                observed_at=common_start + timedelta(minutes=17),
                equity=Decimal("1002"),
                source_observed_at=common_start + timedelta(minutes=17),
            ),
            _EquityObservation(
                observed_at=common_start + timedelta(minutes=31),
                equity=Decimal("1001"),
                source_observed_at=common_start + timedelta(minutes=31),
            ),
        ],
        common_start_at=common_start,
        end_at=common_start + timedelta(minutes=45),
        source_end_at=common_start + timedelta(minutes=20),
    )

    assert baseline == Decimal("1000")
    assert [point["delta"] for point in points] == [
        "0",
        "2",
        "2",
        "2",
    ]
    assert all(
        point["source_observed_at"]
        <= (common_start + timedelta(minutes=17)).isoformat()
        for point in points
    )

def test_live_common_equity_removes_configured_external_deposit() -> None:
    observed_at = datetime(2026, 8, 21, 9, 45, tzinfo=UTC)
    observations = _live_aggregated_equity_observations(
        [
            (datetime(2026, 8, 21, 9, 30, tzinfo=UTC), Decimal("1000")),
            (observed_at, Decimal("1202")),
        ],
        account_label="primary",
        cash_flow_adjustments=parse_live_cash_flow_adjustments(
            '[{"account_label":"primary","effective_at":"2026-08-21T09:41:00Z","amount":"200"}]'
        ),
    )

    assert [observation.equity for observation in observations] == [
        Decimal("1000"),
        Decimal("1002"),
    ]

def test_live_cash_flow_parser_allows_explicit_empty_configuration() -> None:
    assert parse_live_cash_flow_adjustments("[]") == ()
    default = parse_live_cash_flow_adjustments("")
    assert len(default) == 0
    assert default == ()

def test_common_equity_start_is_fixed_to_august_21_anchor() -> None:
    assert parse_common_equity_start_at("") == FIXED_COMMON_EQUITY_START_AT
    assert (
        parse_common_equity_start_at("2026-08-21T10:45:00+08:00")
        == FIXED_COMMON_EQUITY_START_AT
    )

def test_common_equity_start_rejects_moving_the_production_anchor() -> None:
    try:
        parse_common_equity_start_at("2026-08-22T00:00:00Z")
    except ValueError as error:
        assert "fixed" in str(error)
    else:
        raise AssertionError("expected fixed common-equity anchor validation")

def _snapshot(
    snapshot_id: str,
    observed_at: datetime,
    equity: str,
) -> PaperEquitySnapshotRow:
    value = Decimal(equity)
    return PaperEquitySnapshotRow(
        snapshot_id=snapshot_id,
        run_id="paper-account-test",
        observed_at=observed_at,
        balance=value,
        equity=value,
        realized_pnl=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        total_fees=Decimal("0"),
        open_position_count=0,
    )

def _paper_position_row(
    *,
    position_id: str,
    status: str,
    opened_at: datetime,
    closed_at: datetime | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        position_id=position_id,
        symbol="BTCUSDT",
        side="long",
        status=status,
        opened_at=opened_at,
        closed_at=closed_at,
        entry_price=Decimal("100"),
        exit_price=None if closed_at is None else Decimal("101"),
        last_mark_price=Decimal("100"),
        quantity=Decimal("1"),
        entry_notional=Decimal("100"),
        unrealized_pnl=Decimal("0"),
        realized_pnl=None if closed_at is None else Decimal("1"),
        return_pct=None if closed_at is None else Decimal("0.01"),
        entry_fee=Decimal("0"),
        exit_fee=Decimal("0"),
        close_reason=None if closed_at is None else "take_profit",
    )


async def test_dashboard_queries_account_performance_and_cash_flow_seeding() -> None:
    """Verifies DashboardQueries computes authoritative metrics via
    AccountPerformanceCalculator and seeds cash flows.
    """
    from decimal import Decimal
    from unittest.mock import AsyncMock, MagicMock

    from crypto_momentum_lab.operator_dashboard.queries import DashboardQueries
    from crypto_momentum_lab.persistence.postgres.models import (
        AccountBalanceSnapshotRow,
    )

    now = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
    mock_session = AsyncMock()

    from uuid import uuid4

    snap1 = AccountBalanceSnapshotRow(
        snapshot_id=uuid4(),
        environment="live",
        account_label="primary",
        asset="USDT",
        wallet_balance=Decimal("200.00"),
        available_balance=Decimal("200.00"),
        unrealized_pnl=Decimal("0.00"),
        observed_at=now - timedelta(hours=24),
        raw_payload={},
    )
    snap2 = AccountBalanceSnapshotRow(
        snapshot_id=uuid4(),
        environment="live",
        account_label="primary",
        asset="USDT",
        wallet_balance=Decimal("205.00"),
        available_balance=Decimal("205.00"),
        unrealized_pnl=Decimal("0.00"),
        observed_at=now,
        raw_payload={},
    )

    scalars_mock = MagicMock()
    # 1. snaps call returns [snap1, snap2]
    # 2. cf_rows call returns empty list
    scalars_mock.all.side_effect = [
        [snap1, snap2],
        [],
    ]
    mock_session.scalars.return_value = scalars_mock

    mock_factory = MagicMock()
    mock_factory.return_value.__aenter__.return_value = mock_session

    dashboard = DashboardQueries(
        session_factory=mock_factory,
        clock=lambda: now,
        nextgen_accounts=(),
    )

    perf = await dashboard.account_performance("primary", window_hours=24)
    assert perf["status"] == "insufficient_coverage"
    assert perf["account_label"] == "primary"
    assert perf["start_equity"] == "200.00"
    assert perf["end_equity"] == "205.00"
    # Uncertified windows must not publish return figures.
    assert perf["net_equity_delta"] is None
    assert perf["cash_flow_adjusted_pnl"] is None
    assert perf["coverage_status"] == "uncertified"
    assert perf["is_certified"] is False
    assert perf["cash_flow_coverage_proof"] == "uncertified_zero_cash_flow_facts"
    assert perf["twr"] is None

    # Now test with certified cash flows
    from crypto_momentum_lab.operator_dashboard.performance_builder import (
        compute_cash_flow_evidence_hash,
    )
    from crypto_momentum_lab.persistence.postgres.models import CashFlowCorrectionRow

    eff_at = now - timedelta(hours=10)
    cf_row = CashFlowCorrectionRow(
        correction_id="cf_001",
        account_label="primary",
        amount=Decimal("10.00"),
        cash_flow_type="deposit",
        effective_at=eff_at,
        reason="audit_deposit",
        approval_ref="appr_001",
        evidence_hash=compute_cash_flow_evidence_hash(
            correction_id="cf_001",
            account_label="primary",
            amount=Decimal("10.00"),
            cash_flow_type="deposit",
            effective_at=eff_at,
            reason="audit_deposit",
            approval_ref="appr_001",
        ),
        created_at=eff_at,
    )
    scalars_mock.all.side_effect = [
        [snap1, snap2],
        [cf_row],
    ]
    perf_certified = await dashboard.account_performance("primary", window_hours=24)
    assert perf_certified["coverage_status"] == "confirmed"
    assert perf_certified["is_certified"] is True
    assert (
        perf_certified["cash_flow_coverage_proof"]
        == "audited_records_count_1_with_verified_evidence"
    )
    assert perf_certified["cash_flow_corrections_count"] == 1

    # Test proven empty cash flows
    scalars_mock.all.side_effect = [
        [snap1, snap2],
        [],
    ]
    perf_proven_empty = await dashboard.account_performance(
        "primary", window_hours=24, is_empty_proven=True
    )
    assert perf_proven_empty["coverage_status"] == "confirmed"
    assert perf_proven_empty["is_certified"] is True
    assert perf_proven_empty["cash_flow_coverage_proof"] == "proven_zero_cash_flows"
    assert perf_proven_empty["net_equity_delta"] is not None
