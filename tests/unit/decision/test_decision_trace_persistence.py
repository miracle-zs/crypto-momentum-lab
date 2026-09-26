"""Tests for PostgresDecisionTraceRepository and live trace recording wiring (R2)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy.dialects import postgresql

from crypto_momentum_lab.domain.decision.decision_engine import (
    FrozenDecisionInputs,
    PolicyState,
    create_authoritative_decision_filter,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    FactCoverageInterval,
    FactCoverageStatus,
    PositionHealthStatus,
    PositionKey,
    PositionLedgerBatch,
    PositionView,
)
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.market.revision_models import (
    DecisionTrace,
    MarketRevisionRef,
    MarketVisibilityMode,
)
from crypto_momentum_lab.domain.strategy.models import StrategyDecision
from crypto_momentum_lab.live_rollout.decision_facts import LiveDecisionFactSource
from crypto_momentum_lab.persistence.postgres.decision_trace_repository import (
    PostgresDecisionTraceRepository,
)
from crypto_momentum_lab.persistence.postgres.models import (
    DecisionTraceRow,
    MarketRevisionRefRow,
)
from crypto_momentum_lab.tools.reproduce_decision import audit_decision_trace


def _make_market_state(symbol: str = "BTCUSDT") -> MarketState15s:
    start = datetime(2026, 9, 25, 8, 0, tzinfo=UTC)
    return MarketState15s(
        schema_version=1,
        exchange="binance",
        environment="live",
        symbol=symbol,
        bucket_start=start,
        bucket_end=start + timedelta(seconds=15),
        open_price=Decimal("100"),
        high_price=Decimal("105"),
        low_price=Decimal("98"),
        close_price=Decimal("102"),
        trade_count=10,
        trade_notional=Decimal("1000"),
        aggressive_buy_notional=Decimal("600"),
        aggressive_sell_notional=Decimal("400"),
        last_bid_price=Decimal("102"),
        last_ask_price=Decimal("102.1"),
        spread=Decimal("0.1"),
        midpoint=Decimal("102.05"),
        liquidation_count=0,
        liquidation_notional=Decimal("0"),
        mark_price=Decimal("102"),
        closed_kline_count=0,
        source_event_count=10,
        first_received_at=start,
        last_received_at=start + timedelta(seconds=14),
    )


class _AsyncContext:
    def __init__(self, value: Any) -> None:
        self._value = value

    async def __aenter__(self) -> Any:
        return self._value

    async def __aexit__(self, *args: Any) -> None:
        pass


class _FakeQueryResult:
    def __init__(
        self, scalar_val: Any = None, scalars_list: list[Any] | None = None
    ) -> None:
        self._scalar = scalar_val
        self._scalars = scalars_list or []

    def scalar_one_or_none(self) -> Any:
        return self._scalar

    def scalar(self) -> Any:
        return self._scalar

    def scalars(self) -> _FakeQueryResult:
        return self

    def all(self) -> list[Any]:
        return self._scalars


class _FakeAsyncSession:
    def __init__(self) -> None:
        self.statements: list[Any] = []
        self.trace_rows: dict[str, DecisionTraceRow] = {}
        self.rev_rows: dict[str, MarketRevisionRefRow] = {}

    def begin(self) -> _AsyncContext:
        return _AsyncContext(self)

    async def execute(self, statement: Any) -> _FakeQueryResult:
        self.statements.append(statement)
        sql_text = str(getattr(statement, "text", ""))

        # Dialect compile check
        compiled_str = ""
        try:
            compiled_str = str(
                statement.compile(dialect=postgresql.dialect())  # type: ignore[no-untyped-call]
            )
        except Exception:
            compiled_str = str(statement)

        if (
            "SET LOCAL synchronous_commit" in sql_text
            or "SET LOCAL synchronous_commit" in compiled_str
        ):
            return _FakeQueryResult()

        if "INSERT INTO market_revision_refs" in compiled_str:
            return _FakeQueryResult()

        if "INSERT INTO decision_traces" in compiled_str:
            return _FakeQueryResult()

        if "SELECT count(decision_traces.decision_id)" in compiled_str:
            return _FakeQueryResult(scalar_val=len(self.trace_rows))

        if "FROM decision_traces" in compiled_str:
            # Return first or matching trace row
            row = next(iter(self.trace_rows.values()), None)
            return _FakeQueryResult(scalar_val=row)

        if "FROM market_revision_refs" in compiled_str:
            return _FakeQueryResult(scalars_list=list(self.rev_rows.values()))

        return _FakeQueryResult()


class _FakeSessionFactory:
    def __init__(self, session: _FakeAsyncSession) -> None:
        self.session = session

    def __call__(self) -> _AsyncContext:
        return _AsyncContext(self.session)


@pytest.mark.asyncio
async def test_postgres_decision_trace_repository_saves_with_non_durable_commit() -> (
    None
):
    session = _FakeAsyncSession()
    repo = PostgresDecisionTraceRepository(_FakeSessionFactory(session))  # type: ignore[arg-type]
    t0 = datetime(2026, 9, 25, 8, 0, tzinfo=UTC)

    ref = MarketRevisionRef(
        scope="live",
        symbol="BTCUSDT",
        interval="15s",
        bucket_start=t0,
        bucket_end=t0 + timedelta(seconds=15),
        revision_id="live:BTCUSDT:15s:1790323200:abc1234567",
        content_hash="abc1234567890",
        published_at=t0 + timedelta(seconds=14),
        source_epoch="ep_live",
        visibility_mode=MarketVisibilityMode.DECISION_VISIBLE,
        observed_at=t0,
    )

    trace = DecisionTrace(
        decision_id="trace_test_001",
        strategy_name="orderflow_impulse",
        account_label="primary",
        decision_time=t0 + timedelta(seconds=15),
        evaluated_market_refs=(ref,),
        intent_produced=False,
        intent_id=None,
        rejection_reason="cooldown_active",
        input_hash="hash_input_123",
        frame_digest="frame_digest_456",
        trace_payload={
            "next_policy_state": {"policy_version": 2},
            "market_state": {"symbol": "BTCUSDT", "close_price": "102"},
        },
    )

    await repo.save_decision_trace(trace)

    # 1. Non-durable commit policy executed
    assert session.statements[0].text == "SET LOCAL synchronous_commit = OFF"

    # 2. Both market_revision_refs and decision_traces upserted
    compiled_rev = str(
        session.statements[1].compile(dialect=postgresql.dialect())  # type: ignore[no-untyped-call]
    )
    assert "INSERT INTO market_revision_refs" in compiled_rev
    assert "ON CONFLICT (revision_id) DO NOTHING" in compiled_rev

    compiled_trace = str(
        session.statements[2].compile(dialect=postgresql.dialect())  # type: ignore[no-untyped-call]
    )
    assert "INSERT INTO decision_traces" in compiled_trace
    assert "ON CONFLICT (decision_id) DO UPDATE" in compiled_trace


@pytest.mark.asyncio
async def test_postgres_decision_trace_repository_load() -> None:
    session = _FakeAsyncSession()
    t0 = datetime(2026, 9, 25, 8, 0, tzinfo=UTC)
    trace_row = DecisionTraceRow(
        decision_id="trace_test_002",
        strategy_name="orderflow_impulse",
        account_label="primary",
        decision_time=t0,
        intent_produced=True,
        intent_id="cand_1",
        rejection_reason=None,
        evaluated_revision_ids=["live:BTCUSDT:15s:1790323200:ref1"],
        trace_payload={"input_hash": "h1", "frame_digest": "f1"},
        created_at=t0,
    )
    rev_row = MarketRevisionRefRow(
        revision_id="live:BTCUSDT:15s:1790323200:ref1",
        scope="live",
        symbol="BTCUSDT",
        interval="15s",
        bucket_start=t0,
        bucket_end=t0 + timedelta(seconds=15),
        content_hash="h_content",
        published_at=t0,
        source_epoch="ep1",
        visibility_mode="decision_visible",
        is_canonical=False,
        payload={},
        lineage={},
    )
    session.trace_rows["trace_test_002"] = trace_row
    session.rev_rows["live:BTCUSDT:15s:1790323200:ref1"] = rev_row

    repo = PostgresDecisionTraceRepository(_FakeSessionFactory(session))  # type: ignore[arg-type]
    loaded = await repo.load_decision_trace("trace_test_002")

    assert loaded is not None
    assert loaded.decision_id == "trace_test_002"
    assert loaded.intent_produced is True
    assert loaded.intent_id == "cand_1"
    assert len(loaded.evaluated_market_refs) == 1
    assert (
        loaded.evaluated_market_refs[0].revision_id
        == "live:BTCUSDT:15s:1790323200:ref1"
    )


@pytest.mark.asyncio
async def test_live_decision_fact_source_records_trace_on_decision() -> None:
    session = _FakeAsyncSession()
    repo = PostgresDecisionTraceRepository(_FakeSessionFactory(session))  # type: ignore[arg-type]
    fact_source = LiveDecisionFactSource(
        account_label="primary",
        trace_repository=repo,
        strategy_name="orderflow_impulse",
    )

    state = _make_market_state()
    key = PositionKey(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )
    opened_at = datetime(2026, 9, 25, 6, 0, tzinfo=UTC)
    batch = PositionLedgerBatch(
        batch_id="b_01",
        episode_id="ep_01",
        quantity=Decimal("1.0"),
        original_quantity=Decimal("1.0"),
        entry_price=Decimal("100.00"),
        opened_at=opened_at,
    )
    view = PositionView(
        key=key,
        projection_version="pv1",
        input_revision=1,
        event_cut=state.bucket_end,
        policy_version="v1",
        schema_version="v1",
        coverage=FactCoverageInterval(
            start_at=opened_at,
            end_at=state.bucket_end,
            status=FactCoverageStatus.CONFIRMED,
        ),
        active_episode=None,
        batches=(batch,),
        unallocated_quantity=Decimal("0"),
        reconciliation_gap=Decimal("0"),
        health_status=PositionHealthStatus.READY,
    )
    frozen = FrozenDecisionInputs(
        position_view=view,
        cash_balance=Decimal("1000"),
        policy_state=PolicyState(policy_version=1),
        universe_version="univ_v1",
        risk_config_version="risk_v1",
    )

    traces_received: list[DecisionTrace] = []
    filt = create_authoritative_decision_filter(
        "orderflow_impulse",
        target_notional=Decimal("500"),
        fact_provider=lambda s: frozen,
        on_decision_result=fact_source.on_decision_result,
        trace_recorder=traces_received.append,
    )

    dec = StrategyDecision(signals=(), candidates=(), rejections=())
    filt(dec, state)

    assert len(traces_received) == 1
    trace = traces_received[0]
    assert trace.account_label == "primary"
    assert trace.strategy_name == "orderflow_impulse"
    assert "market_state" in trace.trace_payload

    # Background async task execution
    await asyncio.sleep(0.01)
    assert len(session.statements) >= 3


@pytest.mark.asyncio
async def test_audit_decision_trace_reproducibility(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _FakeAsyncSession()
    t0 = datetime(2026, 9, 25, 8, 0, tzinfo=UTC)
    trace_row = DecisionTraceRow(
        decision_id="trace_audit_001",
        strategy_name="orderflow_impulse",
        account_label="primary",
        decision_time=t0,
        intent_produced=False,
        intent_id=None,
        rejection_reason="holding_position_no_exit",
        evaluated_revision_ids=["live:BTCUSDT:15s:1790323200:testref"],
        trace_payload={
            "input_hash": "input_hash_xyz",
            "frame_digest": "frame_digest_xyz",
            "output_intent": None,
            "next_policy_state": {"policy_version": 3},
        },
        created_at=t0,
    )
    rev_row = MarketRevisionRefRow(
        revision_id="live:BTCUSDT:15s:1790323200:testref",
        scope="live",
        symbol="BTCUSDT",
        interval="15s",
        bucket_start=t0,
        bucket_end=t0 + timedelta(seconds=15),
        content_hash="testrefhash",
        published_at=t0,
        source_epoch="ep_live",
        visibility_mode="decision_visible",
        is_canonical=False,
        payload={},
        lineage={},
    )
    session.trace_rows["trace_audit_001"] = trace_row
    session.rev_rows["live:BTCUSDT:15s:1790323200:testref"] = rev_row

    from crypto_momentum_lab.tools import reproduce_decision

    class FakeEngine:
        async def dispose(self) -> None:
            pass

    monkeypatch.setattr(
        reproduce_decision,
        "create_async_database_engine",
        lambda url, **kwargs: FakeEngine(),
    )
    monkeypatch.setattr(
        reproduce_decision,
        "async_sessionmaker",
        lambda engine, **kwargs: _FakeSessionFactory(session),
    )

    audit_res = await audit_decision_trace(
        "trace_audit_001", database_url="postgresql+asyncpg://cml:pwd@localhost/cml"
    )
    assert audit_res["status"] == "VERIFIED_REPRODUCIBLE"
    assert audit_res["reproduced"] is True
    assert audit_res["decision_id"] == "trace_audit_001"
    assert audit_res["strategy_name"] == "orderflow_impulse"
    assert audit_res["evaluated_revisions_count"] == 1
    assert (
        audit_res["evaluated_revisions"][0]["revision_id"]
        == "live:BTCUSDT:15s:1790323200:testref"
    )
    assert audit_res["next_policy_state_version"] == 3
