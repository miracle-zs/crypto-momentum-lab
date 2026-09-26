"""Unit tests for Phase 4 Sizing, Lot Partitioning, and Margin Capacity (R3).

Obeys Astra Architecture Blueprint 2026-09-25:
- Pure mathematical quantization: downward truncation and remainder.
- Fail-closed admission: rejects orders breaching limits or margin.
- Integration with execute_policy_transition for entry paths.
- Exit allocation invariants: sum(allocations) == total <= requested.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.decision import (
    ClockEvent,
    DecisionFrame,
    EffectivePolicy,
    EquityFractionSizingModel,
    FixedNotionalSizingModel,
    PolicyState,
    SizingPlan,
    SizingRejection,
    StrategyPositionMode,
    SymbolLotRules,
    default_symbol_lot_rules,
    execute_policy_transition,
    quantize_lot_quantity,
)
from crypto_momentum_lab.domain.execution import (
    ExitAllocation,
    ExitAllocationPlan,
    ExitPolicyMode,
    PositionHealthStatus,
    PositionKey,
    PositionLedgerBatch,
    PositionView,
    validate_exit_allocation_plan,
)
from crypto_momentum_lab.domain.market.market_book import compute_market_state_hash
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.market.revision_models import (
    MarketEnvelope,
    MarketRevisionRef,
    MarketVisibilityMode,
)
from crypto_momentum_lab.domain.strategy.models import (
    EntryType,
    OrderIntentCandidate,
    StrategySide,
)


def _make_15s_state(
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
        open_price=close_price,
        high_price=close_price + Decimal("50.00"),
        low_price=close_price - Decimal("50.00"),
        close_price=close_price,
        trade_count=100,
        trade_notional=Decimal("1000000.00"),
        aggressive_buy_notional=Decimal("500000.00"),
        aggressive_sell_notional=Decimal("500000.00"),
        last_bid_price=close_price - Decimal("0.50"),
        last_ask_price=close_price + Decimal("0.50"),
        spread=Decimal("1.00"),
        midpoint=close_price,
        liquidation_count=0,
        liquidation_notional=Decimal("0.00"),
        mark_price=close_price,
        closed_kline_count=0,
        source_event_count=100,
        first_received_at=bucket_start,
        last_received_at=bucket_end,
        data_complete=True,
        missing_agg_trade_count=0,
        is_backfill=False,
    )
    content_hash = compute_market_state_hash(state)
    ref = MarketRevisionRef(
        scope="live",
        symbol=symbol,
        interval="15s",
        bucket_start=bucket_start,
        bucket_end=bucket_end,
        revision_id=f"rev_{symbol}_{int(bucket_start.timestamp())}",
        content_hash=content_hash,
        published_at=bucket_end,
        source_epoch="ep_test",
        visibility_mode=MarketVisibilityMode.DECISION_VISIBLE,
    )
    return ref, MarketEnvelope(ref=ref, state=state)


def _make_position_view(symbol: str) -> PositionView:
    pos_key = PositionKey(
        environment="live",
        account_label="test_acc",
        symbol=symbol,
    )
    return PositionView(
        key=pos_key,
        projection_version="pv_test_01",
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


def test_quantize_lot_quantity_downward_truncation() -> None:
    """Lot quantity must strictly truncate downward without negative remainder."""
    raw = Decimal("0.01578")
    step = Decimal("0.001")
    quantized, remainder = quantize_lot_quantity(raw, step)
    assert quantized == Decimal("0.015")
    assert remainder == Decimal("0.00078")
    assert quantized + remainder == raw

    # Zero or negative inputs return (0, 0)
    assert quantize_lot_quantity(Decimal("0"), step) == (Decimal("0"), Decimal("0"))
    assert quantize_lot_quantity(Decimal("-5"), step) == (Decimal("0"), Decimal("0"))

    # Invalid step size raises ValueError
    with pytest.raises(ValueError, match="step_size must be positive"):
        quantize_lot_quantity(Decimal("10"), Decimal("0"))


def test_symbol_lot_rules_validation() -> None:
    """SymbolLotRules must enforce positive bounds and step ordering."""
    rules = default_symbol_lot_rules("BTCUSDT")
    assert rules.symbol == "BTCUSDT"
    assert rules.step_size == Decimal("0.001")
    assert rules.min_quantity == Decimal("0.001")
    assert rules.min_notional == Decimal("5.00")

    # Rejection of invalid rules
    with pytest.raises(ValueError, match="symbol must not be empty"):
        SymbolLotRules(
            symbol="",
            tick_size=Decimal("0.1"),
            step_size=Decimal("0.001"),
            min_quantity=Decimal("0.001"),
            max_quantity=Decimal("100"),
            min_notional=Decimal("5"),
        )
    with pytest.raises(
        ValueError, match="max_quantity cannot be less than min_quantity"
    ):
        SymbolLotRules(
            symbol="BTCUSDT",
            tick_size=Decimal("0.1"),
            step_size=Decimal("0.001"),
            min_quantity=Decimal("10"),
            max_quantity=Decimal("1"),
            min_notional=Decimal("5"),
        )


def test_fixed_notional_sizing_success() -> None:
    """FixedNotionalSizingModel produces valid SizingPlan with quantized lot."""
    model = FixedNotionalSizingModel(
        target_notional=Decimal("650.00"),
        max_leverage=Decimal("5.0"),
        resize_tolerance=Decimal("0.05"),
    )
    lot_rules = default_symbol_lot_rules("BTCUSDT")
    now = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)

    # Price = 65000.00 -> raw_qty = 0.01 BTC -> notional = 650.00
    plan = model.compute_plan(
        symbol="BTCUSDT",
        reference_price=Decimal("65000.00"),
        cash_balance=Decimal("1000.00"),
        lot_rules=lot_rules,
        as_of=now,
    )
    assert isinstance(plan, SizingPlan)
    assert plan.symbol == "BTCUSDT"
    assert plan.quantized_quantity == Decimal("0.010")
    assert plan.actual_notional == Decimal("650.00")
    assert plan.margin_required == Decimal("130.00")  # 650 / 5
    assert plan.lot_remainder == Decimal("0")


def test_fixed_notional_sizing_fail_closed_rejections() -> None:
    """FixedNotionalSizingModel rejects bad prices, limits, and margin exhaustion."""
    model = FixedNotionalSizingModel(
        target_notional=Decimal("100.00"),
        max_leverage=Decimal("5.0"),
        resize_tolerance=Decimal("0.05"),
    )
    lot_rules = default_symbol_lot_rules("BTCUSDT")
    now = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)

    # 1. Invalid price
    rej1 = model.compute_plan(
        symbol="BTCUSDT",
        reference_price=Decimal("0"),
        cash_balance=Decimal("1000.00"),
        lot_rules=lot_rules,
        as_of=now,
    )
    assert isinstance(rej1, SizingRejection)
    assert rej1.reason == "invalid_reference_price"

    # 2. Below min_quantity: price = 200,000, notional = 100 -> raw_qty = 0.0005 < 0.001
    rej2 = model.compute_plan(
        symbol="BTCUSDT",
        reference_price=Decimal("200000.00"),
        cash_balance=Decimal("1000.00"),
        lot_rules=lot_rules,
        as_of=now,
    )
    assert isinstance(rej2, SizingRejection)
    assert rej2.reason == "below_min_quantity"

    # 3. Insufficient margin: cash = 10, margin_req = 100 / 5 = 20 -> fails closed
    rej3 = model.compute_plan(
        symbol="BTCUSDT",
        reference_price=Decimal("50000.00"),
        cash_balance=Decimal("10.00"),
        lot_rules=lot_rules,
        as_of=now,
    )
    assert isinstance(rej3, SizingRejection)
    assert rej3.reason == "insufficient_margin"

    # 4. Resize beyond tolerance: raw_qty = 0.0028, truncated to 0.002 -> notional = 70
    rej4 = model.compute_plan(
        symbol="BTCUSDT",
        reference_price=Decimal("35000.00"),
        cash_balance=Decimal("1000.00"),
        lot_rules=lot_rules,
        as_of=now,
    )
    assert isinstance(rej4, SizingRejection)
    assert rej4.reason == "resize_beyond_tolerance"


def test_equity_fraction_sizing_dynamic_compounding() -> None:
    """EquityFractionSizingModel scales target notional with available equity."""
    model = EquityFractionSizingModel(
        fraction_of_equity=Decimal("0.10"),  # 10%
        min_notional_floor=Decimal("10.00"),
        max_notional_cap=Decimal("2000.00"),
        max_leverage=Decimal("5.0"),
        resize_tolerance=Decimal("0.10"),
    )
    lot_rules = default_symbol_lot_rules("BTCUSDT")
    now = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)

    # Cash = 2000, 10% fraction * 5x leverage -> 1000 notional
    # Price = 50000 -> 0.020 BTC -> 1000 actual notional -> 200 margin
    plan = model.compute_plan(
        symbol="BTCUSDT",
        reference_price=Decimal("50000.00"),
        cash_balance=Decimal("2000.00"),
        lot_rules=lot_rules,
        as_of=now,
    )
    assert isinstance(plan, SizingPlan)
    assert plan.target_notional == Decimal("1000.00")
    assert plan.quantized_quantity == Decimal("0.020")
    assert plan.actual_notional == Decimal("1000.00")
    assert plan.margin_required == Decimal("200.00")


def test_execute_policy_transition_with_sizing_model() -> None:
    """Policy transition attaches SizingPlan to candidate and preserves sizing state."""
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
    mref, menv = _make_15s_state("BTCUSDT", t0, Decimal("66000.00"))
    pview = _make_position_view("BTCUSDT")

    clock = ClockEvent(timestamp=t0 + timedelta(seconds=15), sequence=1)
    frame = DecisionFrame(
        scope="test",
        symbol="BTCUSDT",
        clock_event=clock,
        market_refs=(mref,),
        position_view_token=pview.projection_version,
        universe_version="univ_v1",
        risk_config_version="risk_v1",
        policy_code_digest="code_01",
        policy_parameters_digest="params_01",
        policy_state_digest="state_01",
        cash_balance=Decimal("5000.00"),
    )
    state = PolicyState()
    sizing_model = FixedNotionalSizingModel(
        target_notional=Decimal("660.00"),
        max_leverage=Decimal("5.0"),
        resize_tolerance=Decimal("0.05"),
    )
    policy = EffectivePolicy(
        policy_id="breakout_v1",
        strategy_name="breakout",
        entry_threshold=Decimal("65000.00"),
        position_mode=StrategyPositionMode.LONG_ONLY,
        sizing_model=sizing_model,
    )

    transition = execute_policy_transition(
        frame=frame,
        prior_state=state,
        policy_artifact=policy,
        market_envelope=menv,
        position_view=pview,
    )

    assert transition.rejection_reason is None
    assert transition.entry_candidate is not None
    cand = transition.entry_candidate
    assert cand.desired_notional == Decimal("660.00")
    assert Decimal(str(cand.features["quantized_quantity"])) == Decimal("0.010")
    assert cand.features["sizing_model"] == "fixed_notional"

    # State records sizing plan
    assert "BTCUSDT" in transition.next_state.sizing_state_by_symbol
    st = transition.next_state.sizing_state_by_symbol["BTCUSDT"]
    assert st["quantized_quantity"] == Decimal("0.010")


def test_execute_policy_transition_fail_closed_on_sizing_rejection() -> None:
    """When margin is insufficient, transition fails closed and rejects candidate."""
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
    mref, menv = _make_15s_state("BTCUSDT", t0, Decimal("66000.00"))
    pview = _make_position_view("BTCUSDT")

    clock = ClockEvent(timestamp=t0 + timedelta(seconds=15), sequence=1)
    # Available cash is zero
    frame = DecisionFrame(
        scope="test",
        symbol="BTCUSDT",
        clock_event=clock,
        market_refs=(mref,),
        position_view_token=pview.projection_version,
        universe_version="univ_v1",
        risk_config_version="risk_v1",
        policy_code_digest="code_01",
        policy_parameters_digest="params_01",
        policy_state_digest="state_01",
        cash_balance=Decimal("0.00"),
    )
    state = PolicyState()
    sizing_model = FixedNotionalSizingModel(
        target_notional=Decimal("660.00"),
        max_leverage=Decimal("5.0"),
    )
    policy = EffectivePolicy(
        policy_id="breakout_v1",
        strategy_name="breakout",
        entry_threshold=Decimal("65000.00"),
        position_mode=StrategyPositionMode.LONG_ONLY,
        sizing_model=sizing_model,
    )

    transition = execute_policy_transition(
        frame=frame,
        prior_state=state,
        policy_artifact=policy,
        market_envelope=menv,
        position_view=pview,
    )

    assert transition.entry_candidate is None
    assert transition.rejection_reason == "sizing_insufficient_margin"
    assert transition.next_state.policy_version == state.policy_version


def test_execute_policy_transition_generator_with_dynamic_sizing() -> None:
    """Candidate generator path evaluates sizing and modifies intent correctly."""
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
    mref, menv = _make_15s_state("BTCUSDT", t0, Decimal("50000.00"))
    pview = _make_position_view("BTCUSDT")

    clock = ClockEvent(timestamp=t0 + timedelta(seconds=15), sequence=1)
    frame = DecisionFrame(
        scope="test",
        symbol="BTCUSDT",
        clock_event=clock,
        market_refs=(mref,),
        position_view_token=pview.projection_version,
        universe_version="univ_v1",
        risk_config_version="risk_v1",
        policy_code_digest="code_01",
        policy_parameters_digest="params_01",
        policy_state_digest="state_01",
        cash_balance=Decimal("10000.00"),
    )
    state = PolicyState()

    def generator(arg0: object, st: object) -> OrderIntentCandidate:
        return OrderIntentCandidate(
            candidate_id="custom_intent_01",
            signal_id="sig_01",
            run_id="run_01",
            strategy_name="custom_strat",
            strategy_version="v1",
            config_hash="conf_01",
            symbol="BTCUSDT",
            side=StrategySide.LONG,
            entry_type=EntryType.MARKET,
            limit_price=None,
            desired_notional=Decimal("100.00"),
            reduce_only=False,
            expires_at=t0 + timedelta(minutes=5),
            created_at=t0,
            reason="custom_signal",
            features={"custom_feat": 1},
        )

    sizing_model = EquityFractionSizingModel(
        fraction_of_equity=Decimal("0.05"),  # 5% of 10000 * 5x = 2500 target notional
        min_notional_floor=Decimal("10.00"),
        max_notional_cap=Decimal("5000.00"),
        max_leverage=Decimal("5.0"),
    )
    policy = EffectivePolicy(
        policy_id="custom_v1",
        strategy_name="custom_strat",
        candidate_generator=generator,
        sizing_model=sizing_model,
    )

    transition = execute_policy_transition(
        frame=frame,
        prior_state=state,
        policy_artifact=policy,
        market_envelope=menv,
        position_view=pview,
    )

    assert transition.entry_candidate is not None
    cand = transition.entry_candidate
    assert cand.desired_notional == Decimal("2500.00")
    assert cand.features["quantized_quantity"] == "0.050"
    assert cand.features["custom_feat"] == 1


def test_validate_exit_allocation_plan_invariants() -> None:
    """ExitAllocationPlan strictly verifies Astra Section 7 allocation invariants."""
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
    b1 = PositionLedgerBatch(
        batch_id="b1",
        episode_id="ep1",
        quantity=Decimal("1.5"),
        original_quantity=Decimal("1.5"),
        entry_price=Decimal("65000"),
        opened_at=t0,
    )
    b2 = PositionLedgerBatch(
        batch_id="b2",
        episode_id="ep1",
        quantity=Decimal("1.0"),
        original_quantity=Decimal("1.0"),
        entry_price=Decimal("66000"),
        opened_at=t0,
    )

    pos_key = PositionKey(
        environment="live",
        account_label="test_acc",
        symbol="BTCUSDT",
    )

    # 1. Valid allocation plan: requesting 2.0, allocating 1.5 from b1 and 0.5 from b2
    valid_plan = ExitAllocationPlan(
        position_key=pos_key,
        allocations=(
            ExitAllocation(batch_id="b1", allocated_quantity=Decimal("1.5")),
            ExitAllocation(batch_id="b2", allocated_quantity=Decimal("0.5")),
        ),
        total_allocated_quantity=Decimal("2.0"),
        policy=ExitPolicyMode.TARGET_BATCHES_ONLY,
    )
    validate_exit_allocation_plan(
        valid_plan, (b1, b2), requested_quantity=Decimal("2.0")
    )
    assert valid_plan.total_allocated_quantity == Decimal("2.0")

    # 2. Over-requested: allocated 2.5 > requested 2.0
    bad_plan_over = ExitAllocationPlan(
        position_key=pos_key,
        allocations=(
            ExitAllocation(batch_id="b1", allocated_quantity=Decimal("1.5")),
            ExitAllocation(batch_id="b2", allocated_quantity=Decimal("1.0")),
        ),
        total_allocated_quantity=Decimal("2.5"),
        policy=ExitPolicyMode.TARGET_BATCHES_ONLY,
    )
    with pytest.raises(ValueError, match="exceeds requested_quantity"):
        validate_exit_allocation_plan(
            bad_plan_over, (b1, b2), requested_quantity=Decimal("2.0")
        )

    # 3. Sum mismatch: sum(allocations) != total rejected in __post_init__
    with pytest.raises(ValueError, match="sum of allocations"):
        ExitAllocationPlan(
            position_key=pos_key,
            allocations=(
                ExitAllocation(batch_id="b1", allocated_quantity=Decimal("1.0")),
            ),
            total_allocated_quantity=Decimal("1.5"),
            policy=ExitPolicyMode.TARGET_BATCHES_ONLY,
        )

    # 4. Batch capacity exceeded: allocating 2.0 from b1 when b1 only has 1.5
    bad_plan_batch = ExitAllocationPlan(
        position_key=pos_key,
        allocations=(ExitAllocation(batch_id="b1", allocated_quantity=Decimal("2.0")),),
        total_allocated_quantity=Decimal("2.0"),
        policy=ExitPolicyMode.TARGET_BATCHES_ONLY,
    )
    with pytest.raises(ValueError, match="exceeds batch b1 capacity"):
        validate_exit_allocation_plan(bad_plan_batch, (b1, b2))

    # 5. Unknown batch allocated
    bad_plan_unknown = ExitAllocationPlan(
        position_key=pos_key,
        allocations=(
            ExitAllocation(batch_id="unknown_b", allocated_quantity=Decimal("1.0")),
        ),
        total_allocated_quantity=Decimal("1.0"),
        policy=ExitPolicyMode.TARGET_BATCHES_ONLY,
    )
    with pytest.raises(ValueError, match="references non-existent batch"):
        validate_exit_allocation_plan(bad_plan_unknown, (b1, b2))
