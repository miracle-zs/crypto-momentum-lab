from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from sqlalchemy.dialects.postgresql import dialect as postgresql_dialect

from crypto_momentum_lab.operator_dashboard.queries import (
    FIXED_COMMON_EQUITY_START_AT,
    DashboardQueries,
    _account_equity_statement,
    _account_margin_statement,
    _aggregate_account_fills,
    _build_common_equity_curve,
    _checkpoint_times_statement,
    _common_equity_interval_seconds,
    _decision_slo_response,
    _downsample_equity_snapshots,
    _EquityObservation,
    _is_dashboard_paper_run,
    _latest_checkpoint_at_statement,
    _latest_live_account_process_statement,
    _live_account_equity_point,
    _live_account_metric_points,
    _live_account_metrics_window_start,
    _live_common_equity_statement,
    _live_equity_observations,
    _live_observation,
    _live_strategy_signal,
    _paper_account_summary,
    _paper_common_equity_statement,
    _paper_equity_statement,
    _paper_exit_label,
    _paper_first_equity_statement,
    _paper_latest_equity_statement,
    _split_exchange_orders,
    _universe_membership,
    parse_common_equity_start_at,
    parse_live_cash_flow_adjustments,
)
from crypto_momentum_lab.operator_dashboard.status import OperationalStatus
from crypto_momentum_lab.persistence.postgres.models import PaperEquitySnapshotRow


def test_live_account_summary_keeps_four_account_order_and_state_join() -> None:
    observed_at = datetime(2026, 9, 6, 0, 0, tzinfo=UTC)
    processes = [
        SimpleNamespace(
            account_label=label,
            environment="live",
            state="ready_readonly" if label != "account-3" else "halted",
            occurred_at=observed_at,
        )
        for label in ("account-4", "account-3", "account-2", "primary")
    ]
    strategy_states = [
        SimpleNamespace(
            account_label=label,
            strategy_name="orderflow_impulse",
            state="running",
            changed_at=observed_at,
        )
        for label in ("primary", "account-2", "account-3", "account-4")
    ]
    leases = [
        SimpleNamespace(
            account_label=label,
            expires_at=observed_at + timedelta(minutes=10),
        )
        for label in ("primary", "account-2", "account-3", "account-4")
    ]

    summaries = DashboardQueries._live_account_summaries(
        processes,
        strategy_states,
        leases,
    )

    assert [summary.account_label for summary in summaries] == [
        "primary",
        "account-2",
        "account-3",
        "account-4",
    ]
    assert [summary.status for summary in summaries] == [
        OperationalStatus.READY,
        OperationalStatus.READY,
        OperationalStatus.HALTED,
        OperationalStatus.READY,
    ]
    assert all(summary.lease_expires_at is not None for summary in summaries)


def test_live_account_summary_uses_active_lease_when_strategy_state_missing() -> None:
    observed_at = datetime(2026, 9, 6, tzinfo=UTC)
    lease_expires_at = observed_at + timedelta(minutes=10)

    summaries = DashboardQueries._live_account_summaries(
        [
            SimpleNamespace(
                account_label="primary",
                environment="live",
                state="ready_readonly",
                occurred_at=observed_at,
            )
        ],
        [],
        [
            SimpleNamespace(
                account_label="primary",
                strategy_name="orderflow_impulse",
                expires_at=lease_expires_at,
            )
        ],
    )

    assert summaries[0].strategy_name == "orderflow_impulse"
    assert summaries[0].strategy_state is None


def test_latest_live_account_process_query_excludes_non_live_states() -> None:
    statement = _latest_live_account_process_statement()
    sql = str(
        statement.compile(
            dialect=postgresql_dialect(),
            compile_kwargs={"literal_binds": True},
        )
    ).lower()

    assert "environment = 'live'" in sql
    assert "group by execution_account_process_states.account_label" in sql


def test_live_account_metric_points_derive_equity_margin_and_drawdown_ratios() -> None:
    start = datetime(2026, 9, 6, 0, 0, tzinfo=UTC)
    equity_rows = [
        SimpleNamespace(
            observed_at=start,
            wallet_balance=Decimal("1000"),
            unrealized_pnl=Decimal("0"),
        ),
        SimpleNamespace(
            observed_at=start + timedelta(minutes=6),
            wallet_balance=Decimal("1010"),
            unrealized_pnl=Decimal("0"),
        ),
        SimpleNamespace(
            observed_at=start + timedelta(minutes=12),
            wallet_balance=Decimal("990"),
            unrealized_pnl=Decimal("0"),
        ),
    ]
    points = _live_account_metric_points(
        equity_rows,
        [
            (start, Decimal("100")),
            (start + timedelta(minutes=6), Decimal("120")),
            (start + timedelta(minutes=12), Decimal("80")),
        ],
        interval_seconds=6 * 60,
    )

    assert [point.equity for point in points] == ["1000", "1010", "990"]
    assert [point.equity_change_ratio for point in points] == [
        "0",
        "0.01",
        "-0.01",
    ]
    assert [point.margin_used for point in points] == ["100", "120", "80"]
    assert points[1].margin_occupancy_ratio == str(Decimal("120") / Decimal("1010"))
    assert [point.drawdown for point in points] == ["0", "0", "-20"]
    assert points[2].drawdown_ratio == "-0.01980198019801980198019801980"


def test_live_account_metrics_window_starts_at_daily_0800_utc_plus_8() -> None:
    window_end = datetime(2026, 9, 9, 1, 0, tzinfo=UTC)

    assert _live_account_metrics_window_start(
        window_end,
        timedelta(hours=24),
    ) == datetime(2026, 9, 9, 0, 0, tzinfo=UTC)

    assert _live_account_metrics_window_start(
        datetime(2026, 9, 8, 23, 0, tzinfo=UTC),
        timedelta(hours=24),
    ) == datetime(2026, 9, 8, 0, 0, tzinfo=UTC)


def test_live_account_margin_query_uses_account_level_rest_margin() -> None:
    statement = _account_margin_statement(
        environment="live",
        account_label="account-2",
        window_start=datetime(2026, 9, 5, tzinfo=UTC),
        window_end=datetime(2026, 9, 6, tzinfo=UTC),
        interval_seconds=6 * 60,
    )
    sql = str(
        statement.compile(
            dialect=postgresql_dialect(),
            compile_kwargs={"literal_binds": True},
        )
    ).lower()

    assert "account_config_snapshots" in sql
    assert "account_reconciliation_runs" in sql
    assert "totalinitialmargin" in sql
    assert "rest_reconciliation" in sql
    assert "account_position_snapshots" not in sql
    assert "notional" not in sql


def test_live_account_margin_query_excludes_websocket_config_observations() -> None:
    statement = _account_margin_statement(
        environment="live",
        account_label="account-2",
        window_start=datetime(2026, 9, 5, tzinfo=UTC),
        window_end=datetime(2026, 9, 6, tzinfo=UTC),
        interval_seconds=6 * 60,
    )
    sql = str(
        statement.compile(
            dialect=postgresql_dialect(),
            compile_kwargs={"literal_binds": True},
        )
    ).lower()

    assert "raw_payload" in sql
    assert "totalinitialmargin" in sql
    assert "user_data_stream" not in sql


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
                    (open_row,)
                    if len(self.scalars_statements) == 1
                    else (closed_row,)
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
    queries = DashboardQueries(SessionFactory(session))

    result = await queries.paper_history("paper-run")

    assert result.closed_trade_count == 501
    assert result.history_complete is False
    assert [item["position_id"] for item in result.closed_trades] == ["closed"]
    assert len(result.trade_events) == 3
    assert session.scalars_statements[1]._limit_clause.value == 500


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


def test_dashboard_uses_latest_checkpoint_without_append_only_events() -> None:
    source = Path(
        "src/crypto_momentum_lab/operator_dashboard/queries.py"
    ).read_text(encoding="utf-8")

    assert "StrategyRuntimeCheckpointRow" in source
    assert "StrategyRuntimeEventRow" in source


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
    assert response.phase_latency[
        "market_state_received->context_ready"
    ].p95_ms == 100.0
    assert response.phase_latency[
        "intent_saved->exchange_request_started"
    ].max_ms == 200.0
    assert response.terminal_reasons == {
        "entry": {"market": {"risk_block": 1}}
    }
    assert response.consumers[0].consumer == "market_state_hub"
    assert response.consumers[0].recovery_count == 1
    assert response.consumers[0].unavailable_event_count == 1
    assert response.consumers[0].lag_event_count == 1
    assert response.consumers[0].last_available is True


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
    )

    response = await queries.decision_slo("6h")

    assert response.window == "6h"
    assert response.window_start == window_end - timedelta(hours=6)
    assert response.window_end == window_end
    assert response.persisted_event_count == 1
    assert session.statement is not None
    assert session.statement._limit_clause.value == 50_001


def test_latest_checkpoint_query_selects_only_timestamp() -> None:
    statement = _latest_checkpoint_at_statement()

    assert [column.key for column in statement.selected_columns] == ["saved_at"]


def test_paper_checkpoint_query_selects_only_identity_and_timestamp() -> None:
    statement = _checkpoint_times_statement(["paper-a", "paper-b"])

    assert [column.key for column in statement.selected_columns] == [
        "run_id",
        "saved_at",
    ]


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


def test_account_equity_query_uses_narrow_lateral_bucket_lookups() -> None:
    statement = _account_equity_statement(
        environment="live",
        account_label="primary",
        asset="USDT",
        window_start=datetime(2026, 8, 1, tzinfo=UTC),
        window_end=datetime(2026, 8, 31, tzinfo=UTC),
        interval_seconds=3 * 60 * 60,
    )

    sql = str(statement.compile(dialect=postgresql_dialect()))

    assert "generate_series" in sql
    assert "LATERAL" in sql
    assert "wallet_balance" in sql
    assert "unrealized_pnl" in sql
    assert "raw_payload" not in sql
    assert "DISTINCT ON" not in sql


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


def test_live_preflight_uses_current_transition_instead_of_old_checkpoint() -> None:
    transition_at = datetime(2026, 8, 21, 0, 40, tzinfo=UTC)
    old_checkpoint_at = transition_at - timedelta(hours=6)

    observed_at, heartbeat_source = _live_observation(
        state="preflight",
        runtime_checkpoint_at=old_checkpoint_at,
        transition_at=transition_at,
    )

    assert observed_at == transition_at
    assert heartbeat_source == "state_transition"


def test_live_enabled_uses_runtime_checkpoint() -> None:
    transition_at = datetime(2026, 8, 21, 0, 40, tzinfo=UTC)
    checkpoint_at = transition_at - timedelta(seconds=3)

    observed_at, heartbeat_source = _live_observation(
        state="live_enabled",
        runtime_checkpoint_at=checkpoint_at,
        transition_at=transition_at,
    )

    assert observed_at == checkpoint_at
    assert heartbeat_source == "runtime_checkpoint"


def test_dashboard_separates_confirmed_open_orders_from_uncertain_orders() -> None:
    rows = [
        SimpleNamespace(state="acknowledged"),
        SimpleNamespace(state="partially_filled"),
        SimpleNamespace(state="submitting"),
        SimpleNamespace(state="mystery"),
        SimpleNamespace(state="filled"),
    ]

    pending, ambiguous = _split_exchange_orders(rows)

    assert [row.state for row in pending] == [
        "acknowledged",
        "partially_filled",
    ]
    assert [row.state for row in ambiguous] == ["submitting", "mystery"]


def test_universe_membership_carries_rank_and_market_fields() -> None:
    membership = SimpleNamespace(
        symbol="AAAUSDT",
        status="retained",
        side="gainer",
        left_target_at=datetime(2026, 8, 18, 4, 0, tzinfo=UTC),
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

    assert _paper_exit_label(
        "candle_15m",
        portfolio,
        {"allow_long": True, "allow_short": False},
    ) == "15M 收线退出 · 仅多头"
    assert _paper_exit_label(
        "candle_15m",
        portfolio,
        {
            "allow_long": True,
            "allow_short": False,
            "max_abs_aggressive_imbalance": "0.7113",
        },
    ) == "15M 收线退出 · 仅多头 · 主动不平衡 ≤ 71.13%"
    assert _paper_exit_label(
        "candle_15m",
        portfolio,
        {
            "allow_long": True,
            "allow_short": False,
            "require_price_above_ema5": True,
            "require_price_above_ema10": True,
        },
    ) == "15M 收线退出 · 仅多头 · 价格 > 15M EMA5 · 价格 > 15M EMA10"


def test_candle_exit_label_includes_grace_recovery_threshold() -> None:
    assert _paper_exit_label(
        "candle_15m",
        {
            "candle_grace_bars": 8,
            "candle_grace_profit_pct": "0.0058",
        },
    ) == "反向后宽限 8 根 15M · 回收 +0.58%"


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


def test_live_strategy_signal_serializes_context_and_volume() -> None:
    detected_at = datetime(2026, 8, 28, 11, 0, tzinfo=UTC)
    recorded_at = datetime(2026, 8, 28, 11, 0, 1, tzinfo=UTC)
    signal = _live_strategy_signal(
        SimpleNamespace(
            observation_id="live-observation",
            signal_id="signal-1",
            candidate_id=None,
            run_id="live-run",
            account_label="primary",
            strategy_name="orderflow_impulse",
            strategy_version="v0",
            config_hash="config-hash",
            code_commit="commit-hash",
            signal_kind="strategy_signal",
            symbol="BTCUSDT",
            side="long",
            detected_at=detected_at,
            source_state_at=detected_at,
            recorded_at=recorded_at,
            reason="orderflow_impulse",
            schema_version=1,
            quote_volume_24h=Decimal("1234.50"),
            quote_volume_24h_quote_asset="USDT",
            quote_volume_24h_source="ticker",
            quote_volume_24h_source_at=detected_at,
            quote_volume_24h_fetched_at=recorded_at,
            quote_volume_24h_age_ms=1000,
            features={"impulse_return_pct": "0.01"},
            reference_prices={"midpoint": "100"},
            market_context={"spread": "0.001"},
            filter_context={"entry_enabled": True},
            candidate_context={"candidate_count": 1},
            account_context={"gross_exposure": "10"},
        )
    )

    assert signal["detected_at"] == detected_at.isoformat()
    assert signal["quote_volume_24h"] == "1234.50"
    assert signal["features"] == {"impulse_return_pct": "0.01"}
    assert signal["filter_context"] == {"entry_enabled": True}


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
    observations = _live_equity_observations(
        [
            SimpleNamespace(
                account_label="primary",
                observed_at=datetime(2026, 8, 21, 9, 30, tzinfo=UTC),
                wallet_balance=Decimal("1000"),
                unrealized_pnl=Decimal("0"),
            ),
            SimpleNamespace(
                account_label="primary",
                observed_at=observed_at,
                wallet_balance=Decimal("1202"),
                unrealized_pnl=Decimal("0"),
            ),
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
    assert len(default) == 1
    assert default[0].amount == Decimal("200")


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


def test_account_fills_are_aggregated_to_one_row_per_order() -> None:
    rows = [
        SimpleNamespace(
            order_id="order-1",
            symbol="TUTUSDT",
            side="SELL",
            fee_asset="BNB",
            price=Decimal("0.03158"),
            quantity=Decimal("736"),
            realized_pnl=Decimal("-1.27"),
            fee=Decimal("0.0116"),
            trade_at=datetime(2026, 8, 16, 1, 40, 13, tzinfo=UTC),
        ),
        SimpleNamespace(
            order_id="order-1",
            symbol="TUTUSDT",
            side="SELL",
            fee_asset="USDT",
            price=Decimal("0.03159"),
            quantity=Decimal("163"),
            realized_pnl=Decimal("-0.28"),
            fee=Decimal("0.0026"),
            trade_at=datetime(2026, 8, 16, 1, 40, 13, tzinfo=UTC),
        ),
    ]

    aggregated = _aggregate_account_fills(
        rows,
        {"order-1": "orderflow_impulse"},
    )

    assert len(aggregated) == 1
    assert aggregated[0]["order_id"] == "order-1"
    assert aggregated[0]["quantity"] == "899"
    assert aggregated[0]["realized_pnl"] == "-1.55"
    assert aggregated[0]["fee"] == "0.0142"
    assert aggregated[0]["fee_asset"] == "BNB / USDT"
    assert aggregated[0]["fill_count"] == 2
    assert aggregated[0]["strategy_name"] == "orderflow_impulse"
    assert aggregated[0]["reduce_only"] is False
    assert aggregated[0]["close_reason"] is None
    assert (
        Decimal(str(aggregated[0]["price"])).quantize(Decimal("0.00001"))
        == Decimal("0.03158")
    )


def test_account_fills_preserve_live_exit_reason_metadata() -> None:
    row = SimpleNamespace(
        order_id="exit-order-1",
        symbol="ONUSDT",
        side="SELL",
        fee_asset="USDT",
        price=Decimal("0.05000"),
        quantity=Decimal("10"),
        realized_pnl=Decimal("1.20"),
        fee=Decimal("0.001"),
        trade_at=datetime(2026, 8, 16, 2, 0, tzinfo=UTC),
    )

    aggregated = _aggregate_account_fills(
        [row],
        {"exit-order-1": "orderflow_impulse"},
        {
            "exit-order-1": {
                "reduce_only": True,
                "close_reason": "candle_15m_grace_timeout_1",
            }
        },
    )

    assert aggregated[0]["reduce_only"] is True
    assert aggregated[0]["close_reason"] == "candle_15m_grace_timeout_1"


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
