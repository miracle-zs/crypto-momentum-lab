"""Unit tests for pure DecisionEngine (R3).

Tests:
1. Determinism and zero side effects (pure function reproducibility);
2. Entry eligibility and OrderIntentCandidate generation;
3. Position holding and exit command generation with batch allocation;
4. State transition and cooldown enforcement via versioned PolicyState.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.decision.decision_engine import (
    ClockEvent,
    DecisionInput,
    EffectivePolicy,
    PolicyState,
    decide,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    PositionHealthStatus,
    PositionKey,
    PositionLedgerBatch,
    PositionView,
)
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.market.revision_models import (
    MarketEnvelope,
    MarketRevisionRef,
    MarketVisibilityMode,
)


def _make_market_envelope(
    symbol: str,
    bucket_start: datetime,
    close_price: Decimal,
) -> tuple[MarketRevisionRef, MarketEnvelope]:
    bucket_end = bucket_start + timedelta(seconds=15)
    state = MarketState15s(
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
        data_complete=True,
        missing_agg_trade_count=0,
        is_backfill=False,
    )
    ref = MarketRevisionRef(
        scope="live",
        symbol=symbol,
        interval="15s",
        bucket_start=bucket_start,
        bucket_end=bucket_end,
        revision_id=f"rev_{symbol}_{int(bucket_start.timestamp())}",
        content_hash="content_hash_123",
        published_at=bucket_end,
        source_epoch="ep1",
        visibility_mode=MarketVisibilityMode.DECISION_VISIBLE,
    )
    return ref, MarketEnvelope(ref=ref, state=state)


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


def test_decision_engine_pure_determinism() -> None:
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
    mref, menv = _make_market_envelope("BTCUSDT", t0, Decimal("65500.00"))
    pview = _make_flat_position_view("BTCUSDT")

    inp = DecisionInput(
        symbol="BTCUSDT",
        market_ref=mref,
        market_envelope=menv,
        position_view=pview,
        universe_version="univ_v1",
        clock_event=ClockEvent(timestamp=t0 + timedelta(seconds=15), sequence=1),
        cash_balance=Decimal("10000.00"),
        risk_config_version="risk_v1",
    )
    state = PolicyState()
    policy = EffectivePolicy(
        policy_id="pol_test_01",
        strategy_name="orderflow_impulse",
        entry_threshold=Decimal("65000.00"),
    )

    # Calling twice with same inputs must produce byte-for-byte identical results
    res1 = decide(inp, state, policy)
    res2 = decide(inp, state, policy)

    assert res1.decision_id == res2.decision_id
    assert res1.input_hash == res2.input_hash
    assert res1.intent is not None
    assert res2.intent is not None
    assert res1.intent.candidate_id == res2.intent.candidate_id
    assert res1.intent.desired_notional == policy.target_notional


def test_decision_engine_cooldown_enforcement() -> None:
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
    mref, menv = _make_market_envelope("BTCUSDT", t0, Decimal("65500.00"))
    pview = _make_flat_position_view("BTCUSDT")

    clock_time = t0 + timedelta(seconds=15)
    inp = DecisionInput(
        symbol="BTCUSDT",
        market_ref=mref,
        market_envelope=menv,
        position_view=pview,
        universe_version="univ_v1",
        clock_event=ClockEvent(timestamp=clock_time, sequence=1),
        cash_balance=Decimal("10000.00"),
        risk_config_version="risk_v1",
    )

    # Active cooldown for next 10 minutes
    state = PolicyState().with_cooldown("BTCUSDT", clock_time + timedelta(minutes=10))
    policy = EffectivePolicy(
        policy_id="pol_test_01",
        strategy_name="orderflow_impulse",
        entry_threshold=Decimal("65000.00"),
    )

    res = decide(inp, state, policy)
    assert res.intent is None
    assert res.rejection_reason == "cooldown_active"


def test_decision_engine_holding_position_exit_evaluation() -> None:
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
    mref, menv = _make_market_envelope("BTCUSDT", t0, Decimal("64000.00"))

    pos_key = PositionKey(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
    )
    batch = PositionLedgerBatch(
        batch_id="batch_001",
        episode_id="ep_001",
        quantity=Decimal("1.5"),
        original_quantity=Decimal("1.5"),
        entry_price=Decimal("65000.00"),
        opened_at=t0 - timedelta(hours=2),  # Opened 2 hours ago (> max_holding_seconds)
    )
    open_pview = PositionView(
        key=pos_key,
        projection_version="pv_open_1",
        input_revision=2,
        event_cut=t0,
        policy_version="v1",
        schema_version="v1",
        coverage=None,
        active_episode=None,
        batches=(batch,),
        unallocated_quantity=Decimal("0"),
        reconciliation_gap=Decimal("0"),
        health_status=PositionHealthStatus.READY,
    )

    clock_time = t0 + timedelta(seconds=15)
    inp = DecisionInput(
        symbol="BTCUSDT",
        market_ref=mref,
        market_envelope=menv,
        position_view=open_pview,
        universe_version="univ_v1",
        clock_event=ClockEvent(timestamp=clock_time, sequence=2),
        cash_balance=Decimal("10000.00"),
        risk_config_version="risk_v1",
    )
    state = PolicyState()
    policy = EffectivePolicy(
        policy_id="pol_test_01",
        strategy_name="orderflow_impulse",
    )

    res = decide(inp, state, policy)
    assert res.exit_command is not None
    assert res.exit_command.requested_quantity == Decimal("1.5")
    assert res.exit_command.allocation_plan is not None
    assert len(res.exit_command.allocation_plan.allocations) == 1
    assert res.exit_command.allocation_plan.allocations[0].batch_id == "batch_001"
    assert res.exit_command.allocation_plan.allocations[
        0
    ].allocated_quantity == Decimal("1.5")

    # Next policy state must transition into cooldown
    assert res.next_policy_state.is_in_cooldown("BTCUSDT", clock_time)


def test_decision_input_rejects_mismatched_market_envelope_ref() -> None:
    """Regression test: DecisionInput must enforce market_envelope.ref == market_ref."""
    import pytest

    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
    mref1, _ = _make_market_envelope("BTCUSDT", t0, Decimal("65500.00"))
    _, menv2 = _make_market_envelope(
        "BTCUSDT", t0 + timedelta(seconds=15), Decimal("65600.00")
    )
    pview = _make_flat_position_view("BTCUSDT")

    with pytest.raises(
        ValueError, match="must match market_ref"
    ):
        DecisionInput(
            symbol="BTCUSDT",
            market_ref=mref1,
            market_envelope=menv2,
            position_view=pview,
            universe_version="univ_v1",
            clock_event=ClockEvent(timestamp=t0 + timedelta(seconds=15), sequence=1),
            cash_balance=Decimal("10000.00"),
            risk_config_version="risk_v1",
        )

