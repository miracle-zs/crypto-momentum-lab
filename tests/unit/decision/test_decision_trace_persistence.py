"""Tests for PostgresDecisionTraceRepository and live trace recording wiring (R2)."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy.dialects import postgresql

from crypto_momentum_lab.domain.decision.decision_engine import (
    FrozenDecisionInputs,
    PolicyState,
    create_authoritative_async_decision_filter,
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
            self._persist_insert(statement, MarketRevisionRefRow, self.rev_rows)
            return _FakeQueryResult()

        if "INSERT INTO decision_traces" in compiled_str:
            self._persist_insert(statement, DecisionTraceRow, self.trace_rows)
            return _FakeQueryResult()

        if "SELECT count(decision_traces.decision_id)" in compiled_str:
            return _FakeQueryResult(scalar_val=len(self.trace_rows))

        if "FROM decision_traces" in compiled_str:
            rows = self._matching_rows(statement, self.trace_rows.values())
            row = rows[0] if rows else None
            return _FakeQueryResult(scalar_val=row, scalars_list=rows)

        if "FROM market_revision_refs" in compiled_str:
            return _FakeQueryResult(
                scalars_list=self._matching_rows(statement, self.rev_rows.values())
            )

        return _FakeQueryResult()

    @staticmethod
    def _insert_values(statement: Any) -> list[dict[str, Any]]:
        multi_values = getattr(statement, "_multi_values", ())
        if multi_values:
            rows = multi_values[0]
            return [
                {
                    getattr(column, "name", str(column)): value
                    for column, value in row.items()
                }
                for row in rows
            ]
        values = getattr(statement, "_values", None) or {}
        return [
            {
                getattr(column, "name", str(column)): getattr(value, "value", value)
                for column, value in values.items()
            }
        ]

    @classmethod
    def _persist_insert(
        cls,
        statement: Any,
        row_type: type[Any],
        rows_by_id: dict[str, Any],
    ) -> None:
        for values in cls._insert_values(statement):
            row = row_type(**values)
            identity = row.decision_id if isinstance(row, DecisionTraceRow) else row.revision_id
            rows_by_id.setdefault(identity, row)

    @staticmethod
    def _matching_rows(statement: Any, rows: Any) -> list[Any]:
        where = getattr(statement, "whereclause", None)
        if where is None:
            return list(rows)
        predicates = list(getattr(where, "clauses", (where,)))
        matched = []
        for row in rows:
            valid = True
            for predicate in predicates:
                column = getattr(predicate, "left", None)
                right = getattr(predicate, "right", None)
                field_name = getattr(column, "key", None)
                expected = getattr(right, "value", None)
                if field_name is None:
                    continue
                actual = getattr(row, field_name, None)
                if isinstance(expected, (list, tuple, set, frozenset)):
                    valid = actual in expected
                else:
                    valid = actual == expected
                if not valid:
                    break
            if valid:
                matched.append(row)
        return matched


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

    # Both reference and trace rows use immutable conflict handling.
    sql_texts = [
        str(stmt.compile(dialect=postgresql.dialect()))  # type: ignore[no-untyped-call]
        for stmt in session.statements[1:]
    ]
    assert any(
        "INSERT INTO market_revision_refs" in s
        and "ON CONFLICT (revision_id) DO NOTHING" in s
        for s in sql_texts
    )
    assert any(
        "INSERT INTO decision_traces" in s
        and "ON CONFLICT (decision_id) DO NOTHING" in s
        for s in sql_texts
    )


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
async def test_async_decision_filter_awaits_durable_commit_callback() -> None:

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

    commits: list[tuple[DecisionTrace, object, object]] = []
    callback_finished = False

    async def provide_facts(_state: MarketState15s, _side: object) -> FrozenDecisionInputs:
        return frozen

    async def durable_commit(trace: DecisionTrace, result: object, inp: object) -> None:
        nonlocal callback_finished
        await asyncio.sleep(0)
        commits.append((trace, result, inp))
        callback_finished = True

    filt = create_authoritative_async_decision_filter(
        "orderflow_impulse",
        fact_provider=provide_facts,
        durable_decision_commit=durable_commit,
        target_notional=Decimal("500"),
    )

    from crypto_momentum_lab.domain.strategy.models import (
        EntryType,
        OrderIntentCandidate,
        StrategySide,
        StrategySignal,
    )

    signal = StrategySignal(
        signal_id="sig_trace_1",
        run_id="run_trace_1",
        strategy_name="orderflow_impulse",
        strategy_version="v1",
        config_hash="cfg_trace_1",
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        detected_at=state.bucket_end,
        source_state_at=state.bucket_end,
        reason="trace_test",
        features={},
        reference_prices={},
    )
    candidate = OrderIntentCandidate(
        candidate_id="cand_trace_1",
        signal_id="sig_trace_1",
        run_id="run_trace_1",
        strategy_name="orderflow_impulse",
        strategy_version="v1",
        config_hash="cfg_trace_1",
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        entry_type=EntryType.MARKET,
        limit_price=Decimal("102"),
        desired_notional=Decimal("500"),
        reduce_only=False,
        expires_at=state.bucket_end + timedelta(minutes=1),
        created_at=state.bucket_end,
        reason="trace_test",
        features={},
    )
    dec = StrategyDecision(signals=(signal,), candidates=(candidate,), rejections=())
    await filt(dec, state)

    assert len(commits) == 1
    assert callback_finished
    trace = commits[0][0]
    assert trace.account_label == "primary"
    assert trace.strategy_name == "orderflow_impulse"
    assert "market_state" in trace.trace_payload
    assert "policy_parameters" in trace.trace_payload
    assert "prior_policy_state" in trace.trace_payload
    assert "output_exit_command" in trace.trace_payload



@pytest.mark.asyncio
async def test_audit_decision_trace_reproducibility(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = _FakeAsyncSession()
    t0 = datetime(2026, 9, 25, 8, 0, tzinfo=UTC)

    from crypto_momentum_lab.domain.decision.decision_engine import (
        ClockEvent,
        DecisionInput,
        EffectivePolicy,
        PolicyState,
        decide,
        decision_trace_from_result,
    )
    from crypto_momentum_lab.domain.decision.decision_frame import DecisionFrame
    from crypto_momentum_lab.domain.decision.policy_transition import (
        compute_policy_parameters_digest,
        compute_policy_state_digest,
    )
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        PositionHealthStatus,
        PositionKey,
        PositionView,
    )
    from tests.unit.decision.test_decision_engine import _make_market_envelope

    ref, env = _make_market_envelope("BTCUSDT", t0, Decimal("65500.00"))
    pos_view = PositionView(
        key=PositionKey("live", "primary", "BTCUSDT"),
        projection_version="pv_replay_0",
        input_revision=1,
        event_cut=None,
        policy_version="v1",
        schema_version="v1",
        coverage=None,
        active_episode=None,
        batches=(),
        unallocated_quantity=Decimal("0"),
        reconciliation_gap=Decimal("0"),
        health_status=PositionHealthStatus.READY,
    )
    inp = DecisionInput(
        symbol="BTCUSDT",
        market_ref=ref,
        market_envelope=env,
        position_view=pos_view,
        universe_version="u1",
        clock_event=ClockEvent(sequence=1, timestamp=t0),
        cash_balance=Decimal("10000.00"),
        risk_config_version="risk_v1",
    )
    policy = EffectivePolicy(
        policy_id="pol-1",
        strategy_name="orderflow_impulse",
        policy_version=1,
        entry_threshold=Decimal("65000.00"),
        target_notional=Decimal("1000.00"),
    )
    prior_state = PolicyState(policy_version=1)
    ref2 = replace(ref, revision_id=f"{ref.revision_id}:derived")
    inp = replace(
        inp,
        frame=DecisionFrame(
            scope="live",
            symbol="BTCUSDT",
            market_refs=(ref, ref2),
            position_view_token=pos_view.projection_version,
            clock_event=inp.clock_event,
            universe_version=inp.universe_version,
            risk_config_version=inp.risk_config_version,
            policy_code_digest=f"policy_{policy.strategy_name}_{prior_state.policy_version}",
            policy_parameters_digest=compute_policy_parameters_digest(policy),
            policy_state_digest=compute_policy_state_digest(prior_state),
            risk_plan_digest=inp.risk_config_version,
            cash_balance=inp.cash_balance,
        ),
    )
    res = decide(inp, prior_state, policy)
    real_trace = decision_trace_from_result(
        res,
        inp,
        "orderflow_impulse",
        "primary",
        prior_policy_state=prior_state,
        policy=policy,
    )

    trace_id = real_trace.decision_id
    trace_row = DecisionTraceRow(
        decision_id=trace_id,
        strategy_name=real_trace.strategy_name,
        account_label=real_trace.account_label,
        decision_time=t0,
        intent_produced=real_trace.intent_produced,
        intent_id=real_trace.intent_id,
        rejection_reason=real_trace.rejection_reason,
        evaluated_revision_ids=[ref.revision_id, ref2.revision_id],
        trace_payload=real_trace.trace_payload,
        created_at=t0,
    )
    rev_row = MarketRevisionRefRow(
        revision_id=ref.revision_id,
        scope="live",
        symbol="BTCUSDT",
        interval="15s",
        bucket_start=t0,
        bucket_end=t0 + timedelta(seconds=15),
        content_hash=ref.content_hash,
        published_at=ref.published_at,
        source_epoch="ep_live",
        visibility_mode="decision_visible",
        is_canonical=False,
        payload={},
        lineage={},
    )
    session.trace_rows[trace_id] = trace_row
    session.rev_rows[ref.revision_id] = rev_row
    session.rev_rows[ref2.revision_id] = MarketRevisionRefRow(
        revision_id=ref2.revision_id,
        scope=ref2.scope,
        symbol=ref2.symbol,
        interval=ref2.interval,
        bucket_start=ref2.bucket_start,
        bucket_end=ref2.bucket_end,
        content_hash=ref2.content_hash,
        published_at=ref2.published_at,
        source_epoch=ref2.source_epoch,
        visibility_mode=ref2.visibility_mode.value,
        is_canonical=False,
        payload={},
        lineage={},
    )

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
        trace_id, database_url="postgresql+asyncpg://cml:pwd@localhost/cml"
    )
    assert audit_res["status"] == "VERIFIED_REPRODUCIBLE", audit_res
    assert audit_res["reproduced"] is True
    assert audit_res["decision_id"] == trace_id
    assert audit_res["strategy_name"] == "orderflow_impulse"
    assert audit_res["evaluated_revisions_count"] == 2
    assert audit_res["evaluated_revisions"][0]["revision_id"] == ref.revision_id
    assert audit_res["evaluated_revisions"][1]["revision_id"] == ref2.revision_id

    # Reconstructed policy/state must match the digests frozen in the frame.
    tampered_policy_payload = dict(real_trace.trace_payload)
    tampered_policy_payload["policy_parameters"] = dict(
        tampered_policy_payload["policy_parameters"]
    )
    tampered_policy_payload["policy_parameters"]["target_notional"] = "999"
    tampered_policy = replace(real_trace, trace_payload=tampered_policy_payload)
    policy_audit = await audit_decision_trace(trace_id, trace_override=tampered_policy)
    assert policy_audit["status"] == "UNREPRODUCIBLE"
    assert policy_audit["error"] == (
        "Reconstructed policy parameters differ from the DecisionFrame digest"
    )

    tampered_state_payload = dict(real_trace.trace_payload)
    tampered_state_payload["prior_policy_state"] = dict(
        tampered_state_payload["prior_policy_state"]
    )
    tampered_state_payload["prior_policy_state"]["warmup_status"] = {"BTCUSDT": True}
    tampered_state = replace(real_trace, trace_payload=tampered_state_payload)
    state_audit = await audit_decision_trace(trace_id, trace_override=tampered_state)
    assert state_audit["status"] == "UNREPRODUCIBLE"
    assert state_audit["error"] == (
        "Reconstructed prior policy state differs from the DecisionFrame digest"
    )

    tampered_refs_payload = dict(real_trace.trace_payload)
    tampered_refs_payload["decision_frame"] = dict(
        tampered_refs_payload["decision_frame"]
    )
    tampered_refs_payload["decision_frame"]["market_revision_ids"] = [ref.revision_id]
    tampered_refs = replace(real_trace, trace_payload=tampered_refs_payload)
    refs_audit = await audit_decision_trace(trace_id, trace_override=tampered_refs)
    assert refs_audit["status"] == "UNREPRODUCIBLE"
    assert refs_audit["error"] == (
        "DecisionFrame market revisions differ from the trace revisions"
    )
    assert audit_res["next_policy_state_version"] == 2


@pytest.mark.asyncio
async def test_decision_trace_repository_blocks_conflicting_overwrite() -> None:
    """Reject attempts to overwrite a trace with conflicting content."""
    session = _FakeAsyncSession()
    repo = PostgresDecisionTraceRepository(_FakeSessionFactory(session))

    t0 = datetime(2026, 9, 27, 5, 0, tzinfo=UTC)
    ref = MarketRevisionRef(
        scope="live",
        symbol="BTCUSDT",
        interval="15s",
        bucket_start=t0,
        bucket_end=t0 + timedelta(seconds=15),
        revision_id="live:BTCUSDT:15s:1:ref1",
        content_hash="content_hash_1",
        published_at=t0,
        source_epoch="ep_live",
        visibility_mode=MarketVisibilityMode.DECISION_VISIBLE,
    )
    trace_1 = DecisionTrace(
        decision_id="dec_btc_001",
        strategy_name="orderflow_impulse",
        account_label="primary",
        decision_time=t0,
        evaluated_market_refs=(ref,),
        intent_produced=True,
        intent_id="cand-1",
        rejection_reason=None,
        input_hash="hash_orig_1111",
        frame_digest="frame_orig_1111",
        trace_payload={
            "intent": "buy",
            "input_hash": "hash_orig_1111",
            "frame_digest": "frame_orig_1111",
        },
    )
    # Seed the session with existing trace row
    session.trace_rows["dec_btc_001"] = DecisionTraceRow(
        decision_id="dec_btc_001",
        strategy_name="orderflow_impulse",
        account_label="primary",
        decision_time=t0,
        intent_produced=True,
        intent_id="cand-1",
        rejection_reason=None,
        evaluated_revision_ids=["live:BTCUSDT:15s:1:ref1"],
        trace_payload={
            "intent": "buy",
            "input_hash": "hash_orig_1111",
            "frame_digest": "frame_orig_1111",
        },
        created_at=t0,
    )

    # 1. Saving the exact same trace again is idempotent and succeeds
    await repo.save_decision_traces([trace_1])

    # Reject a conflicting input hash or intent under the same ID.
    conflicting_trace = DecisionTrace(
        decision_id="dec_btc_001",
        strategy_name="orderflow_impulse",
        account_label="primary",
        decision_time=t0,
        evaluated_market_refs=(ref,),
        intent_produced=False,
        intent_id=None,
        rejection_reason="below_entry_threshold",
        input_hash="hash_conflict_2222",
        frame_digest="frame_conflict_2222",
        trace_payload={"intent": None},
    )
    with pytest.raises(ValueError, match="Immutable audit conflict"):
        await repo.save_decision_traces([conflicting_trace])


@pytest.mark.asyncio
async def test_decision_trace_accepts_existing_market_revision_with_canonical_and_authoritative_payload() -> None:
    """Decision traces must not conflict with market revisions previously persisted by the market feed.
    
    The market feed saves authoritative MarketRevisionRefRows with is_canonical=True,
    detailed exchange payload, and source watermark lineage. When a strategy executes
    and persists its DecisionTrace referencing that market revision, it must accept
    the existing row as long as the core identity (scope, symbol, interval, bucket,
    content_hash) matches.
    """
    session = _FakeAsyncSession()
    repo = PostgresDecisionTraceRepository(_FakeSessionFactory(session))

    t0 = datetime(2026, 9, 27, 5, 0, tzinfo=UTC)
    rev_id = "research:XPINUSDT:15s:1790604135:f2b278dd33"
    content_hash = "f2b278dd3384162deaf57f0a529148985f132587e5a20bd61b6f713a5c382232"

    # 1. Seed existing MarketRevisionRefRow as saved by MarketBookRepository
    session.rev_rows[rev_id] = MarketRevisionRefRow(
        revision_id=rev_id,
        scope="research",
        symbol="XPINUSDT",
        interval="15s",
        bucket_start=t0,
        bucket_end=t0 + timedelta(seconds=15),
        content_hash=content_hash,
        published_at=t0,
        source_epoch="seq_5284",
        visibility_mode="decision_visible",
        is_canonical=True,  # Promoted by canonical promoter
        payload={"spread": "0.0000010", "symbol": "XPINUSDT", "exchange": "binance-usdm"},
        lineage={"source_watermark_at": "2026-09-28T14:02:30.006000+00:00"},
    )

    # 2. Strategy produces a DecisionTrace referencing this market revision
    ref = MarketRevisionRef(
        scope="research",
        symbol="XPINUSDT",
        interval="15s",
        bucket_start=t0,
        bucket_end=t0 + timedelta(seconds=15),
        revision_id=rev_id,
        content_hash=content_hash,
        published_at=t0,
        source_epoch="seq_5284",
        visibility_mode=MarketVisibilityMode.DECISION_VISIBLE,
    )
    trace = DecisionTrace(
        decision_id="dec_xpin_entry_001",
        strategy_name="orderflow_impulse",
        account_label="account-4",
        decision_time=t0,
        evaluated_market_refs=(ref,),
        intent_produced=True,
        intent_id="entry-xpin-1",
        rejection_reason=None,
        input_hash="hash_xpin_entry_1",
        frame_digest="frame_xpin_entry_1",
        trace_payload={
            "market_state": {"symbol": "XPINUSDT", "close_price": "0.0008975"},
            "intent": "buy",
        },
    )

    # 3. Saving the trace must succeed without raising Immutable audit conflict
    await repo.save_decision_traces([trace])
    assert "dec_xpin_entry_001" in session.trace_rows
    # Existing authoritative market book payload and is_canonical flag must remain unchanged
    assert session.rev_rows[rev_id].is_canonical is True
    assert session.rev_rows[rev_id].payload["exchange"] == "binance-usdm"

    # 4. A genuine market revision conflict (e.g. mismatched content hash) must still be rejected
    conflicting_ref = MarketRevisionRef(
        scope="research",
        symbol="XPINUSDT",
        interval="15s",
        bucket_start=t0,
        bucket_end=t0 + timedelta(seconds=15),
        revision_id=rev_id,
        content_hash="mismatched_content_hash_xxxx",
        published_at=t0,
        source_epoch="seq_5284",
        visibility_mode=MarketVisibilityMode.DECISION_VISIBLE,
    )
    conflicting_trace = DecisionTrace(
        decision_id="dec_xpin_entry_002",
        strategy_name="orderflow_impulse",
        account_label="account-4",
        decision_time=t0,
        evaluated_market_refs=(conflicting_ref,),
        intent_produced=True,
        intent_id="entry-xpin-2",
        rejection_reason=None,
        input_hash="hash_xpin_entry_2",
        frame_digest="frame_xpin_entry_2",
        trace_payload={"intent": "buy"},
    )
    with pytest.raises(ValueError, match="Immutable audit conflict: MarketRevisionRef"):
        await repo.save_decision_traces([conflicting_trace])

