"""Integration tests for DecisionTrace, multi-mode replay, and retention gating.

Obeys Astra Architecture Blueprint 2026-09-25:
- Decision-visible replay reproduces exact live decision;
- Canonical replay explains divergence due to late data repairs;
- Pruned original revisions fail closed with UnreproducibleError;
- Decision traces register active recovery dependencies with RetentionAuthority.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.decision import (
    ClockEvent,
    DecisionFrame,
    DecisionInput,
    EffectivePolicy,
    PolicyState,
    decide,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    PositionHealthStatus,
    PositionKey,
    PositionView,
)
from crypto_momentum_lab.domain.market import (
    DatasetManifest,
    RunManifest,
)
from crypto_momentum_lab.domain.market.decision_trace_service import (
    DecisionTraceService,
)
from crypto_momentum_lab.domain.market.market_book import (
    InMemoryMarketBookRepository,
    MarketBook,
    UnreproducibleError,
)
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.market.revision_models import (
    MarketEnvelope,
    MarketVisibilityMode,
)
from crypto_momentum_lab.domain.operational.retention_authority import (
    InMemoryRetentionRepository,
    RetentionAuthority,
)
from crypto_momentum_lab.market_data.hub import MarketStateBatch


def _make_state(
    *,
    symbol: str = "BTCUSDT",
    bucket_start: datetime,
    close_price: Decimal,
    missing_count: int = 0,
    data_complete: bool = True,
) -> MarketState15s:
    bucket_end = bucket_start + timedelta(seconds=15)
    return MarketState15s(
        schema_version=1,
        environment="live",
        exchange="binance",
        symbol=symbol,
        bucket_start=bucket_start,
        bucket_end=bucket_end,
        open_price=Decimal("64990.00"),
        high_price=Decimal("65010.00"),
        low_price=Decimal("64980.00"),
        close_price=close_price,
        trade_count=100,
        trade_notional=Decimal("1000000.00"),
        aggressive_buy_notional=Decimal("500000.00"),
        aggressive_sell_notional=Decimal("500000.00"),
        last_bid_price=Decimal("64999.00"),
        last_ask_price=Decimal("65001.00"),
        spread=Decimal("2.00"),
        midpoint=Decimal("65000.00"),
        liquidation_count=0,
        liquidation_notional=Decimal("0.00"),
        mark_price=Decimal("65000.00"),
        closed_kline_count=0,
        source_event_count=100,
        first_received_at=bucket_start + timedelta(milliseconds=100),
        last_received_at=bucket_end - timedelta(milliseconds=50),
        data_complete=data_complete,
        missing_agg_trade_count=missing_count,
        is_backfill=not data_complete,
    )


def test_decision_trace_replay_divergence_and_reproducibility() -> None:
    repo = InMemoryMarketBookRepository()
    book = MarketBook(repo)
    trace_service = DecisionTraceService(book)

    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)

    # 1. Live observation v1: missing late trades, close=64995.00
    s1 = _make_state(
        bucket_start=t0,
        close_price=Decimal("64995.00"),
        missing_count=3,
        data_complete=False,
    )
    ref1 = book.publish(
        s1,
        visibility_mode=MarketVisibilityMode.DECISION_VISIBLE,
        is_canonical=False,
    )

    # Simple breakout policy: trigger if close > 65000.00
    def breakout_policy(
        envelopes: tuple[MarketEnvelope, ...],
    ) -> tuple[bool, str | None]:
        env = envelopes[0]
        if (env.state.close_price or Decimal("0")) > Decimal("65000.00"):
            return True, None
        return False, "no_breakout"

    intent_produced, reason = breakout_policy((book.read(ref1),))
    assert intent_produced is False
    assert reason == "no_breakout"

    # Record live decision trace
    trace = trace_service.record_decision(
        decision_id="dec_001",
        strategy_name="breakout_momentum",
        account_label="live-1",
        decision_time=t0 + timedelta(seconds=15, milliseconds=50),
        evaluated_market_refs=(ref1,),
        intent_produced=intent_produced,
        rejection_reason=reason,
    )
    assert trace.decision_id == "dec_001"

    # 2. Canonical repair v2: late trades arrived, repaired close=65015.00
    s2 = _make_state(
        bucket_start=t0,
        close_price=Decimal("65015.00"),
        missing_count=0,
        data_complete=True,
    )
    ref2 = book.publish(
        s2,
        visibility_mode=MarketVisibilityMode.CANONICAL,
        is_canonical=True,
    )
    assert ref2.revision_id != ref1.revision_id

    # 3. Decision-Visible Replay: MUST faithfully reproduce original live behavior
    res_visible = trace_service.replay_decision(
        "dec_001",
        replay_mode=MarketVisibilityMode.DECISION_VISIBLE,
        policy_evaluator=breakout_policy,
    )
    assert res_visible.reproduced is True
    assert res_visible.replayed_intent_produced is False
    assert res_visible.replayed_rejection_reason == "no_breakout"
    assert res_visible.divergence_explanation is None

    # 4. Canonical Replay: Evaluates counterfactual reality with repaired data
    res_canonical = trace_service.replay_decision(
        "dec_001",
        replay_mode=MarketVisibilityMode.CANONICAL,
        policy_evaluator=breakout_policy,
    )
    assert res_canonical.reproduced is False
    assert res_canonical.replayed_intent_produced is True
    assert res_canonical.replayed_rejection_reason is None
    assert "Divergence in canonical replay" in (
        res_canonical.divergence_explanation or ""
    )


def test_missing_decision_visible_fails_closed_without_faking() -> None:
    repo = InMemoryMarketBookRepository()
    book = MarketBook(repo)
    trace_service = DecisionTraceService(book)

    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
    s1 = _make_state(bucket_start=t0, close_price=Decimal("64990.00"))
    ref1 = book.publish(s1)

    trace_service.record_decision(
        decision_id="dec_lost_history",
        strategy_name="breakout_momentum",
        account_label="live-1",
        decision_time=t0 + timedelta(seconds=15),
        evaluated_market_refs=(ref1,),
        intent_produced=False,
    )

    # Also publish a canonical v2
    s2 = _make_state(bucket_start=t0, close_price=Decimal("65010.00"))
    book.publish(s2, is_canonical=True)

    # Simulate v1 being deleted/missing from disk
    repo.envelopes.pop(ref1.revision_id)

    # Must fail closed with UnreproducibleError rather than silently substituting v2
    with pytest.raises(UnreproducibleError, match="Cannot reproduce decision"):
        trace_service.replay_decision(
            "dec_lost_history",
            replay_mode=MarketVisibilityMode.DECISION_VISIBLE,
            policy_evaluator=lambda envs: (True, None),
        )


def test_decision_trace_registers_retention_dependency() -> None:
    book_repo = InMemoryMarketBookRepository()
    ret_repo = InMemoryRetentionRepository()
    book = MarketBook(book_repo)
    authority = RetentionAuthority(ret_repo)
    trace_service = DecisionTraceService(book, authority)

    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
    s1 = _make_state(bucket_start=t0, close_price=Decimal("65000.00"))
    ref1 = book.publish(s1)

    trace_service.record_decision(
        decision_id="dec_dep_test",
        strategy_name="orderflow_impulse",
        account_label="primary",
        decision_time=t0 + timedelta(seconds=15),
        evaluated_market_refs=(ref1,),
        intent_produced=True,
    )

    # Authority must have an active consumer dependency protecting t0
    deps = ret_repo.get_dependencies("market_revisions")
    assert len(deps) == 1
    assert deps[0].consumer_id == "decision_dec_dep_test"
    assert deps[0].recovery_spec.earliest_needed_watermark == t0

    # Prune requested after t0 must be bounded by t0
    t_future = t0 + timedelta(days=1)
    plan = authority.plan_prune(
        dataset_name="market_revisions", requested_cutoff=t_future
    )
    assert plan.is_constrained is True
    assert plan.effective_cutoff == t0


def _make_flat_position_view(symbol: str) -> PositionView:
    pos_key = PositionKey(
        environment="live",
        account_label="primary",
        symbol=symbol,
    )
    return PositionView(
        key=pos_key,
        projection_version="pv_flat_0",
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


def test_vertical_slice_publisher_runner_trace_and_replay() -> None:
    """Astra Blueprint Phase 2 Vertical Slice Acceptance Test:

    1. Real publisher generates authentic MarketRevisionRef with timestamps.
    2. Runner executes DecisionEngine with DecisionFrame binding multi-window refs.
    3. Immutable DecisionTrace records decision with frame_digest and input_hash.
    4. Replay in DECISION_VISIBLE mode reproduces exact live decision.
    5. Canonical repair update does NOT perturb DECISION_VISIBLE replay.
    6. Pruned/missing original revision fails closed with UnreproducibleError.
    7. Authoritative DatasetManifest and RunManifest compute deterministic hashes.
    """
    repo = InMemoryMarketBookRepository()
    book = MarketBook(repo)
    trace_service = DecisionTraceService(book)

    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)

    # --- 1. Real publisher publishes batch with authentic MarketRevisionRef ---
    s1 = _make_state(
        symbol="BTCUSDT",
        bucket_start=t0,
        close_price=Decimal("65500.00"),
        missing_count=2,
        data_complete=False,
    )
    batch = MarketStateBatch(
        sequence=1,
        published_at=t0 + timedelta(seconds=15),
        environment="live",
        states=(s1,),
        stream_id="epoch-20260925",
    )
    assert len(batch.revision_refs) == 1
    pub_ref = batch.get_revision("BTCUSDT")
    assert pub_ref is not None
    assert pub_ref.scope == "live"
    assert pub_ref.symbol == "BTCUSDT"
    assert pub_ref.interval == "15s"
    assert pub_ref.source_epoch == "epoch-20260925"

    env1 = MarketEnvelope(
        ref=pub_ref,
        state=s1,
        lineage={"publisher": "market_state_hub", "batch_epoch": "epoch-20260925"},
    )
    repo.save_envelope(env1)
    repo.set_canonical_ref("live", "BTCUSDT", "15s", t0, pub_ref)

    # --- 2. Runner executes pure DecisionEngine with DecisionFrame ---
    pview = _make_flat_position_view("BTCUSDT")
    clock = ClockEvent(timestamp=t0 + timedelta(seconds=15), sequence=1)

    frame = DecisionFrame(
        scope="live",
        symbol="BTCUSDT",
        market_refs=(pub_ref,),
        position_view_token=pview.projection_version,
        universe_version="univ_20260925",
        risk_config_version="risk_v1",
        policy_code_digest="sha256_code_v1",
        policy_parameters_digest="sha256_params_v1",
        policy_state_digest="sha256_state_v1",
        clock_event=clock,
        cash_balance=Decimal("10000.00"),
    )
    assert len(frame.frame_digest) == 64

    d_input = DecisionInput(
        symbol="BTCUSDT",
        market_ref=pub_ref,
        market_envelope=env1,
        position_view=pview,
        universe_version="univ_20260925",
        clock_event=clock,
        cash_balance=Decimal("10000.00"),
        risk_config_version="risk_v1",
        frame=frame,
    )
    assert d_input.frame_digest == frame.frame_digest

    policy = EffectivePolicy(
        policy_id="breakout_live_v1",
        strategy_name="orderflow_impulse",
        entry_threshold=Decimal("65000.00"),
    )
    policy_state = PolicyState()
    result = decide(d_input, policy_state, policy)
    assert result.intent is not None
    assert result.rejection_reason is None
    assert result.frame_digest == frame.frame_digest
    assert len(result.input_hash) == 64

    # --- 3. Trace Service records DecisionTrace ---
    trace = trace_service.record_decision(
        decision_id="dec_slice_001",
        strategy_name="orderflow_impulse",
        account_label="primary",
        decision_time=t0 + timedelta(seconds=15, milliseconds=20),
        evaluated_market_refs=(pub_ref,),
        intent_produced=True,
        intent_id=result.intent.candidate_id,
        rejection_reason=None,
        input_hash=result.input_hash,
        frame_digest=result.frame_digest,
    )
    assert trace.frame_digest == frame.frame_digest
    assert trace.input_hash == result.input_hash

    # Verify loaded trace round-trip
    loaded_trace = repo.load_decision_trace("dec_slice_001")
    assert loaded_trace is not None
    assert loaded_trace.frame_digest == frame.frame_digest
    assert loaded_trace.input_hash == result.input_hash

    # --- 4. DECISION_VISIBLE mode reproduces exact decision ---
    def replay_evaluator(
        envelopes: tuple[MarketEnvelope, ...],
    ) -> tuple[bool, str | None]:
        env = envelopes[0]
        inp = DecisionInput(
            symbol="BTCUSDT",
            market_ref=env.ref,
            market_envelope=env,
            position_view=pview,
            universe_version="univ_20260925",
            clock_event=clock,
            cash_balance=Decimal("10000.00"),
            risk_config_version="risk_v1",
        )
        res = decide(inp, policy_state, policy)
        return res.intent is not None, res.rejection_reason

    replay_vis = trace_service.replay_decision(
        "dec_slice_001",
        replay_mode=MarketVisibilityMode.DECISION_VISIBLE,
        policy_evaluator=replay_evaluator,
    )
    assert replay_vis.reproduced is True
    assert replay_vis.replayed_intent_produced is True
    assert replay_vis.replayed_rejection_reason is None
    assert replay_vis.divergence_explanation is None

    # --- 5. Canonical repair update: repaired close=64000 (no entry) ---
    s2 = _make_state(
        symbol="BTCUSDT",
        bucket_start=t0,
        close_price=Decimal("64000.00"),
        missing_count=0,
        data_complete=True,
    )
    ref2 = book.publish(
        s2,
        visibility_mode=MarketVisibilityMode.CANONICAL,
        is_canonical=True,
    )
    assert ref2.revision_id != pub_ref.revision_id

    # Canonical repair does NOT affect DECISION_VISIBLE replay
    replay_vis2 = trace_service.replay_decision(
        "dec_slice_001",
        replay_mode=MarketVisibilityMode.DECISION_VISIBLE,
        policy_evaluator=replay_evaluator,
    )
    assert replay_vis2.reproduced is True
    assert replay_vis2.replayed_intent_produced is True

    # CANONICAL replay reflects late repairs and flags divergence
    replay_canon = trace_service.replay_decision(
        "dec_slice_001",
        replay_mode=MarketVisibilityMode.CANONICAL,
        policy_evaluator=replay_evaluator,
    )
    assert replay_canon.reproduced is False
    assert replay_canon.replayed_intent_produced is False
    assert "Divergence in canonical replay" in (
        replay_canon.divergence_explanation or ""
    )

    # --- 6. Deleting original revision fails closed with UnreproducibleError ---
    repo.envelopes.pop(pub_ref.revision_id)
    with pytest.raises(UnreproducibleError, match="Cannot reproduce decision"):
        trace_service.replay_decision(
            "dec_slice_001",
            replay_mode=MarketVisibilityMode.DECISION_VISIBLE,
            policy_evaluator=replay_evaluator,
        )

    # --- 7. DatasetManifest & RunManifest deterministic hashing ---
    d_manifest = DatasetManifest(
        manifest_id="manifest_slice_01",
        scope="live",
        symbols=("BTCUSDT",),
        interval="15s",
        start_time=t0,
        end_time=t0 + timedelta(minutes=15),
        visibility_mode=MarketVisibilityMode.CANONICAL,
        revision_refs=(ref2,),
    )
    assert len(d_manifest.manifest_hash) == 64

    r_manifest = RunManifest(
        run_id="run_slice_01",
        strategy_name="orderflow_impulse",
        strategy_policy_version="breakout_live_v1",
        dataset_manifest_id=d_manifest.manifest_id,
        policy_code_digest="sha256_code_v1",
        policy_parameters_digest="sha256_params_v1",
        risk_plan_digest="sha256_risk_v1",
    )
    assert len(r_manifest.run_manifest_hash) == 64

