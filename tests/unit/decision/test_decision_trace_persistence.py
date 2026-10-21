"""Tests for historical decision trace storage and replay."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy.dialects import postgresql

from crypto_momentum_lab.domain.decision.trace_audit import verify_decision_trace
from crypto_momentum_lab.domain.market.market_book import UnreproducibleError
from crypto_momentum_lab.domain.market.revision_models import (
    MarketRevisionRef,
    MarketVisibilityMode,
)
from crypto_momentum_lab.persistence.postgres.decision_trace_repository import (
    PostgresDecisionTraceRepository,
)
from crypto_momentum_lab.persistence.postgres.decision_trace_storage import (
    share_policy_states,
)
from crypto_momentum_lab.persistence.postgres.models import (
    DecisionPolicyEvidenceRow,
    DecisionTraceRow,
    MarketRevisionRefRow,
)
from crypto_momentum_lab.tools.reproduce_decision import audit_decision_trace
from tests.fixtures.symbol_rules import btc_lot_rules


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

    def scalars(self) -> _FakeQueryResult:
        return self

    def all(self) -> list[Any]:
        return self._scalars

class _FakeAsyncSession:
    def __init__(self) -> None:
        self.trace_rows: dict[str, DecisionTraceRow] = {}
        self.rev_rows: dict[str, MarketRevisionRefRow] = {}
        self.policy_rows: dict[str, DecisionPolicyEvidenceRow] = {}

    async def execute(self, statement: Any) -> _FakeQueryResult:
        compiled_str = ""
        try:
            compiled_str = str(
                statement.compile(dialect=postgresql.dialect())  # type: ignore[no-untyped-call]
            )
        except Exception:
            compiled_str = str(statement)

        if "FROM decision_policy_evidence" in compiled_str:
            return _FakeQueryResult(
                scalars_list=self._matching_rows(statement, self.policy_rows.values())
            )

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
async def test_postgres_repository_loads_summary_market_refs() -> None:
    session = _FakeAsyncSession()
    t0 = datetime(2026, 10, 7, 8, 0, tzinfo=UTC)
    ref = MarketRevisionRef(
        scope="live",
        symbol="BTCUSDT",
        interval="15s",
        bucket_start=t0,
        bucket_end=t0 + timedelta(seconds=15),
        revision_id="live:BTCUSDT:15s:normal-hold",
        content_hash="content_hash_normal_hold",
        published_at=t0,
        source_epoch="ep_live",
        visibility_mode=MarketVisibilityMode.DECISION_VISIBLE,
    )
    decision_id = "normal-hold-001"
    session.trace_rows[decision_id] = DecisionTraceRow(
        decision_id=decision_id,
        strategy_name="orderflow_impulse",
        account_label="primary",
        decision_time=t0,
        intent_produced=False,
        intent_id=None,
        rejection_reason="holding_position_no_exit",
        evaluated_revision_ids=[ref.revision_id],
        trace_payload={
            "evidence_level": "summary",
            "summary_schema_version": 1,
            "frame_digest": "frame-digest",
            "input_hash": "input-hash",
            "original_payload_sha256": "0" * 64,
            "outcome": "holding_position_no_exit",
            "market_refs": [
                {
                    "scope": "live",
                    "symbol": "BTCUSDT",
                    "interval": "15s",
                    "bucket_start": t0.isoformat(),
                    "bucket_end": (t0 + timedelta(seconds=15)).isoformat(),
                    "revision_id": ref.revision_id,
                    "content_hash": ref.content_hash,
                    "published_at": t0.isoformat(),
                    "source_epoch": "ep_live",
                    "visibility_mode": "decision_visible",
                    "observed_at": "",
                }
            ],
        },
        created_at=t0,
    )
    repo = PostgresDecisionTraceRepository(_FakeSessionFactory(session))  # type: ignore[arg-type]

    loaded = await repo.load_decision_trace(decision_id)

    assert loaded is not None
    assert loaded.evaluated_market_refs == (ref,)
    assert verify_decision_trace(loaded, decision_id)["status"] == "SUMMARY_ONLY"

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

@pytest.mark.parametrize("sizing_kind", [None, "fixed", "equity", "large-state"])
async def test_audit_decision_trace_reproducibility(
    monkeypatch: pytest.MonkeyPatch,
    sizing_kind: str | None,
    tmp_path,
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
    if sizing_kind not in (None, "large-state"):
        from crypto_momentum_lab.domain.strategy.sizing import (
            EquityFractionSizingModel,
            FixedNotionalSizingModel,
        )

        if sizing_kind == "fixed":
            sizing_model = FixedNotionalSizingModel(
                target_notional=policy.target_notional,
                max_leverage=Decimal("5"),
                max_slippage_budget_bps=Decimal("10"),
                resize_tolerance=Decimal("0.05"),
            )
        else:
            sizing_model = EquityFractionSizingModel(
                fraction_of_equity=Decimal("0.10"),
                min_notional_floor=Decimal("10"),
                max_notional_cap=Decimal("1000.00"),
                max_leverage=Decimal("5"),
                max_slippage_budget_bps=Decimal("10"),
                resize_tolerance=Decimal("0.05"),
            )
        policy = replace(
            policy,
            sizing_model=sizing_model,
            symbol_lot_rules=btc_lot_rules(),
        )
    prior_state = PolicyState(policy_version=1)
    if sizing_kind == "large-state":
        from tests.unit.decision.test_decision_state_storage import large_payload

        prior_state = replace(
            prior_state,
            sizing_state_by_symbol=large_payload()["prior_policy_state"][
                "sizing_state"
            ],
        )
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
    archived_policy_states = {}
    if sizing_kind == "large-state":
        trace_row.trace_payload, archived_policy_states = share_policy_states(
            trace_row.trace_payload
        )
        session.policy_rows.update(
            {
                digest: DecisionPolicyEvidenceRow(
                    state_digest=digest, state_payload=state, created_at=t0
                )
                for digest, state in archived_policy_states.items()
            }
        )
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

    # Replay remains available after both the hot trace and revision metadata
    # have been removed: the verified archive carries its own exact identities.
    import hashlib

    import zstandard

    from crypto_momentum_lab.tools.reproduce_decision import (
        load_archived_decision_trace,
    )

    cold_row = {
        "decision_id": trace_id,
        "strategy_name": real_trace.strategy_name,
        "account_label": real_trace.account_label,
        "decision_time": t0.isoformat(),
        "intent_produced": real_trace.intent_produced,
        "intent_id": real_trace.intent_id,
        "rejection_reason": real_trace.rejection_reason,
        "evaluated_revision_ids": [ref.revision_id, ref2.revision_id],
        "trace_payload": trace_row.trace_payload,
        "archived_policy_states": archived_policy_states,
        "archived_market_refs": [
            {
                "scope": r.scope,
                "symbol": r.symbol,
                "interval": r.interval,
                "bucket_start": r.bucket_start.isoformat(),
                "bucket_end": r.bucket_end.isoformat(),
                "revision_id": r.revision_id,
                "content_hash": r.content_hash,
                "published_at": r.published_at.isoformat(),
                "source_epoch": r.source_epoch,
                "visibility_mode": r.visibility_mode.value,
                "lineage": {},
            }
            for r in (ref, ref2)
        ],
    }
    archive = tmp_path / "traces.jsonl.zst"
    archive.write_bytes(
        zstandard.ZstdCompressor().compress(json.dumps(cold_row).encode() + b"\n")
    )
    manifest = tmp_path / "traces.manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "table": "decision_traces",
                "format": "jsonl",
                "file": archive.name,
                "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
            }
        )
    )
    cold = load_archived_decision_trace(manifest, trace_id)
    assert verify_decision_trace(cold, trace_id)["status"] == "VERIFIED_REPRODUCIBLE"
    archive.write_bytes(archive.read_bytes() + b"corrupted")
    with pytest.raises(UnreproducibleError, match="SHA-256"):
        load_archived_decision_trace(manifest, trace_id)

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
    assert audit_res["next_policy_state_version"] == (
        res.next_policy_state.policy_version
    )
