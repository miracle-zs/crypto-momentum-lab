"""Unit tests for PolicyTransition and StrategyPolicy contracts (R3).

Obeys Astra Architecture Blueprint 2026-09-25:
1. Pure transition: transition(frame, prior_state, policy) -> PolicyTransition;
2. Identical results across Live dry-run, Paper, and Research;
3. 15m closed candle validation (rejects invalid spans, non-positive prices);
4. Explicit StrategyPositionMode enforcement (LONG_ONLY, SHORT_ONLY, BOTH);
5. Clocks drive holding exits and timers even in absence of entry signals;
6. TimerRequest emission (cooldown_expiry, grace_period_expiry, max_holding);
7. PolicyState versioning and immutability.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.decision import (
    ClockEvent,
    DecisionFrame,
    EffectivePolicy,
    PolicyState,
    StrategyPositionMode,
    TimerRequest,
    execute_policy_transition,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    PositionEpisode,
    PositionHealthStatus,
    PositionKey,
    PositionLedgerBatch,
    PositionView,
)
from crypto_momentum_lab.domain.execution.trade_command import TradeCommandType
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
from crypto_momentum_lab.domain.strategy.position_exit import (
    ClosedCandle15m,
    PositionExitPolicy,
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
        open_price=Decimal("65000.00"),
        high_price=Decimal("65100.00"),
        low_price=Decimal("64900.00"),
        close_price=close_price,
        trade_count=50,
        trade_notional=Decimal("500000.00"),
        aggressive_buy_notional=Decimal("250000.00"),
        aggressive_sell_notional=Decimal("250000.00"),
        last_bid_price=close_price - Decimal("1.00"),
        last_ask_price=close_price + Decimal("1.00"),
        spread=Decimal("2.00"),
        midpoint=close_price,
        liquidation_count=0,
        liquidation_notional=Decimal("0.00"),
        mark_price=close_price,
        closed_kline_count=0,
        source_event_count=50,
        first_received_at=bucket_start + timedelta(milliseconds=50),
        last_received_at=bucket_end - timedelta(milliseconds=50),
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


def _make_position_view(
    symbol: str,
    quantity: Decimal = Decimal("0"),
    opened_at: datetime | None = None,
    side: StrategySide = StrategySide.LONG,
) -> PositionView:
    pos_side = (
        FuturesPositionSide.SHORT
        if side == StrategySide.SHORT
        else FuturesPositionSide.LONG
    )
    pos_key = PositionKey(
        environment="live",
        account_label="test_acc",
        symbol=symbol,
        position_side=pos_side,
    )
    batches: tuple[PositionLedgerBatch, ...] = ()
    active_episode: PositionEpisode | None = None
    if quantity > Decimal("0"):
        batch = PositionLedgerBatch(
            batch_id=f"batch_{symbol}_01",
            episode_id="ep_01",
            quantity=quantity,
            original_quantity=quantity,
            entry_price=Decimal("65000.00"),
            opened_at=opened_at or datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC),
        )
        batches = (batch,)
        active_episode = PositionEpisode(
            episode_id="ep_01",
            position_key=pos_key,
            side=side,
            opened_at=opened_at or datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC),
            batches=batches,
        )
    return PositionView(
        key=pos_key,
        projection_version="pv_test_01",
        input_revision=1,
        event_cut=None,
        policy_version="v1",
        schema_version="v1",
        coverage=None,
        active_episode=active_episode,
        batches=batches,
        unallocated_quantity=Decimal("0"),
        reconciliation_gap=Decimal("0"),
        health_status=PositionHealthStatus.READY,
    )


def test_closed_candle_15m_strict_validation() -> None:
    """ClosedCandle15m must reject invalid spans, naive times, and bad prices."""
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)

    # Valid 15-minute candle
    candle = ClosedCandle15m(
        symbol="BTCUSDT",
        candle_start=t0,
        candle_end=t0 + timedelta(minutes=15),
        open_price=Decimal("65000.00"),
        close_price=Decimal("65200.00"),
    )
    assert candle.duration == timedelta(minutes=15)

    # Reject duration != 15m (e.g. 15 seconds)
    with pytest.raises(ValueError, match="duration must be exactly 15 minutes"):
        ClosedCandle15m(
            symbol="BTCUSDT",
            candle_start=t0,
            candle_end=t0 + timedelta(seconds=15),
            open_price=Decimal("65000.00"),
            close_price=Decimal("65200.00"),
        )

    # Reject naive datetime
    with pytest.raises(ValueError, match="must be timezone-aware"):
        ClosedCandle15m(
            symbol="BTCUSDT",
            candle_start=datetime(2026, 9, 25, 12, 0, 0),
            candle_end=datetime(2026, 9, 25, 12, 15, 0),
            open_price=Decimal("65000.00"),
            close_price=Decimal("65200.00"),
        )

    # Reject non-positive open price
    with pytest.raises(ValueError, match="open_price and close_price must be positive"):
        ClosedCandle15m(
            symbol="BTCUSDT",
            candle_start=t0,
            candle_end=t0 + timedelta(minutes=15),
            open_price=Decimal("0.00"),
            close_price=Decimal("65200.00"),
        )

    # Reject empty symbol
    with pytest.raises(ValueError, match="symbol must not be empty"):
        ClosedCandle15m(
            symbol="",
            candle_start=t0,
            candle_end=t0 + timedelta(minutes=15),
            open_price=Decimal("65000.00"),
            close_price=Decimal("65200.00"),
        )


def test_timer_request_validation() -> None:
    """TimerRequest enforces non-empty fields and timezone-aware timestamps."""
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)

    timer = TimerRequest(
        timer_id="tm_01",
        timer_type="cooldown_expiry",
        symbol="BTCUSDT",
        due_at=t0 + timedelta(minutes=15),
    )
    assert timer.timer_id == "tm_01"

    # Reject naive due_at
    with pytest.raises(ValueError, match="must be timezone-aware"):
        TimerRequest(
            timer_id="tm_02",
            timer_type="cooldown_expiry",
            symbol="BTCUSDT",
            due_at=datetime(2026, 9, 25, 12, 15, 0),
        )


def test_policy_transition_pure_determinism() -> None:
    """Calling transition twice with same inputs yields identical outputs."""
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
    mref, menv = _make_15s_state("BTCUSDT", t0, Decimal("66000.00"))
    pview = _make_position_view("BTCUSDT", Decimal("0"))

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
    )
    state = PolicyState()
    policy = EffectivePolicy(
        policy_id="breakout_v1",
        strategy_name="breakout",
        entry_threshold=Decimal("65000.00"),
        position_mode=StrategyPositionMode.LONG_ONLY,
    )

    t1 = execute_policy_transition(
        frame=frame,
        prior_state=state,
        policy_artifact=policy,
        market_envelope=menv,
        position_view=pview,
    )
    t2 = execute_policy_transition(
        frame=frame,
        prior_state=state,
        policy_artifact=policy,
        market_envelope=menv,
        position_view=pview,
    )

    assert t1.decision_id == t2.decision_id
    assert t1.input_hash == t2.input_hash
    assert t1.frame_digest == t2.frame_digest
    assert t1.entry_candidate is not None
    assert t2.entry_candidate is not None
    assert t1.entry_candidate.candidate_id == t2.entry_candidate.candidate_id
    assert t1.next_state == t2.next_state
    assert t1.transition_time == clock.timestamp


def test_explicit_position_mode_enforcement() -> None:
    """StrategyPositionMode must strictly filter candidates and reject violations."""
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
    mref, menv = _make_15s_state("BTCUSDT", t0, Decimal("66000.00"))
    pview = _make_position_view("BTCUSDT", Decimal("0"))
    clock = ClockEvent(timestamp=t0 + timedelta(seconds=15), sequence=1)
    frame = DecisionFrame(
        scope="test",
        symbol="BTCUSDT",
        clock_event=clock,
        market_refs=(mref,),
        position_view_token=pview.projection_version,
        universe_version="univ_v1",
    )
    state = PolicyState()

    # Built-in breakout generates LONG: SHORT_ONLY policy must reject it
    policy_short_only = EffectivePolicy(
        policy_id="short_only_policy",
        strategy_name="breakout",
        entry_threshold=Decimal("65000.00"),
        position_mode=StrategyPositionMode.SHORT_ONLY,
    )
    t_short = execute_policy_transition(
        frame=frame,
        prior_state=state,
        policy_artifact=policy_short_only,
        market_envelope=menv,
        position_view=pview,
    )
    assert t_short.entry_candidate is None
    assert t_short.rejection_reason == "direction_not_permitted_by_position_mode"

    # Generator emitting SHORT candidate: LONG_ONLY policy must reject it
    def short_generator(
        env: MarketEnvelope, st: PolicyState
    ) -> OrderIntentCandidate:
        return OrderIntentCandidate(
            candidate_id="cand_short_01",
            signal_id="sig_short_01",
            run_id="run_01",
            strategy_name="mean_revert",
            strategy_version="v1",
            config_hash="conf_01",
            symbol="BTCUSDT",
            side=StrategySide.SHORT,
            entry_type=EntryType.MARKET,
            limit_price=None,
            desired_notional=Decimal("500.00"),
            reduce_only=False,
            expires_at=clock.timestamp + timedelta(minutes=5),
            created_at=clock.timestamp,
            reason="overbought",
            features={},
        )

    policy_long_only = EffectivePolicy(
        policy_id="long_only_policy",
        strategy_name="mean_revert",
        position_mode=StrategyPositionMode.LONG_ONLY,
        candidate_generator=short_generator,
    )
    t_long = execute_policy_transition(
        frame=frame,
        prior_state=state,
        policy_artifact=policy_long_only,
        market_envelope=menv,
        position_view=pview,
    )
    assert t_long.entry_candidate is None
    assert t_long.rejection_reason == "direction_not_permitted_by_position_mode"

    # BOTH allows SHORT candidate
    policy_both = EffectivePolicy(
        policy_id="both_policy",
        strategy_name="mean_revert",
        position_mode=StrategyPositionMode.BOTH,
        candidate_generator=short_generator,
    )
    t_both = execute_policy_transition(
        frame=frame,
        prior_state=state,
        policy_artifact=policy_both,
        market_envelope=menv,
        position_view=pview,
    )
    assert t_both.entry_candidate is not None
    assert t_both.entry_candidate.side == StrategySide.SHORT


def test_clock_driven_holding_exit_and_cooldown_timer() -> None:
    """Clock drives holding exit and emits cooldown timer even without entry signal."""
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
    mref, menv = _make_15s_state("BTCUSDT", t0, Decimal("64000.00"))

    # Position opened 2 hours ago (> 1 hour max holding)
    opened_at = t0 - timedelta(hours=2)
    pview = _make_position_view(
        "BTCUSDT", quantity=Decimal("2.0"), opened_at=opened_at
    )

    clock = ClockEvent(timestamp=t0 + timedelta(seconds=15), sequence=2)
    frame = DecisionFrame(
        scope="test",
        symbol="BTCUSDT",
        clock_event=clock,
        market_refs=(mref,),
        position_view_token=pview.projection_version,
        universe_version="univ_v1",
    )
    state = PolicyState()
    policy = EffectivePolicy(
        policy_id="breakout_v1",
        strategy_name="breakout",
        cooldown_duration=timedelta(minutes=15),
        exit_policy=PositionExitPolicy(max_holding_seconds=3600),
    )

    t = execute_policy_transition(
        frame=frame,
        prior_state=state,
        policy_artifact=policy,
        market_envelope=menv,
        position_view=pview,
    )

    assert t.exit_command is not None
    assert t.exit_command.command_type == TradeCommandType.EXIT
    assert t.exit_command.requested_quantity == Decimal("2.0")
    assert t.exit_command.allocation_plan is not None

    # Next state must transition into cooldown
    assert t.next_state.is_in_cooldown("BTCUSDT", clock.timestamp)

    # Cooldown timer request emitted
    assert len(t.timer_requests) == 1
    cd_timer = t.timer_requests[0]
    assert cd_timer.timer_type == "cooldown_expiry"
    assert cd_timer.symbol == "BTCUSDT"
    assert cd_timer.due_at == clock.timestamp + timedelta(minutes=15)


def test_clock_driven_holding_timer_when_not_exiting() -> None:
    """When position is held and not exiting, max_holding_expiry timer is scheduled."""
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
    mref, menv = _make_15s_state("BTCUSDT", t0, Decimal("65500.00"))

    # Position opened 10 minutes ago (< 1 hour max holding)
    opened_at = t0 - timedelta(minutes=10)
    pview = _make_position_view(
        "BTCUSDT", quantity=Decimal("1.0"), opened_at=opened_at
    )

    clock = ClockEvent(timestamp=t0 + timedelta(seconds=15), sequence=2)
    frame = DecisionFrame(
        scope="test",
        symbol="BTCUSDT",
        clock_event=clock,
        market_refs=(mref,),
        position_view_token=pview.projection_version,
        universe_version="univ_v1",
    )
    state = PolicyState()
    policy = EffectivePolicy(
        policy_id="breakout_v1",
        strategy_name="breakout",
        exit_policy=PositionExitPolicy(max_holding_seconds=3600),
    )

    t = execute_policy_transition(
        frame=frame,
        prior_state=state,
        policy_artifact=policy,
        market_envelope=menv,
        position_view=pview,
    )

    assert t.exit_command is None
    assert t.entry_candidate is None
    assert t.rejection_reason == "holding_position_no_exit"

    # Scheduled max holding timer
    assert len(t.timer_requests) == 1
    hold_timer = t.timer_requests[0]
    assert hold_timer.timer_type == "max_holding_expiry"
    assert hold_timer.symbol == "BTCUSDT"
    assert hold_timer.due_at == opened_at + timedelta(seconds=3600)


def test_grace_period_timer_emission_on_entry() -> None:
    """When policy has grace_period > 0, grace_period timer is emitted on entry."""
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
    mref, menv = _make_15s_state("BTCUSDT", t0, Decimal("66000.00"))
    pview = _make_position_view("BTCUSDT", Decimal("0"))

    clock = ClockEvent(timestamp=t0 + timedelta(seconds=15), sequence=1)
    frame = DecisionFrame(
        scope="test",
        symbol="BTCUSDT",
        clock_event=clock,
        market_refs=(mref,),
        position_view_token=pview.projection_version,
        universe_version="univ_v1",
    )

    cand = OrderIntentCandidate(
        candidate_id="cand_grace_01",
        signal_id="sig_grace_01",
        run_id="run_01",
        strategy_name="breakout",
        strategy_version="v1",
        config_hash="conf_01",
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        entry_type=EntryType.MARKET,
        limit_price=None,
        desired_notional=Decimal("500.00"),
        reduce_only=False,
        expires_at=clock.timestamp + timedelta(minutes=5),
        created_at=clock.timestamp,
        reason="breakout",
        features={},
    )

    policy = EffectivePolicy(
        policy_id="grace_policy",
        strategy_name="breakout",
        grace_period=timedelta(seconds=45),
        candidate_generator=lambda env, st: cand,
    )
    state = PolicyState()

    t = execute_policy_transition(
        frame=frame,
        prior_state=state,
        policy_artifact=policy,
        market_envelope=menv,
        position_view=pview,
    )

    assert t.entry_candidate is not None
    assert len(t.timer_requests) == 1
    grace_timer = t.timer_requests[0]
    assert grace_timer.timer_type == "grace_period_expiry"
    assert grace_timer.symbol == "BTCUSDT"
    assert grace_timer.due_at == clock.timestamp + timedelta(seconds=45)


def test_policy_state_immutability_and_versioning() -> None:
    """PolicyState methods return new instances with incremented version."""
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
    s0 = PolicyState(policy_version=1)

    s1 = s0.with_cooldown("BTCUSDT", t0 + timedelta(minutes=15))
    assert s1.policy_version == 2
    assert s0.policy_version == 1
    assert s1.is_in_cooldown("BTCUSDT", t0) is True
    assert s0.is_in_cooldown("BTCUSDT", t0) is False

    s2 = s1.with_anchor_and_intent("BTCUSDT", Decimal("65000.00"), "cand_01")
    assert s2.policy_version == 3
    assert s2.anchor_prices_by_symbol["BTCUSDT"] == Decimal("65000.00")
    assert s2.active_intent_ids_by_symbol["BTCUSDT"] == "cand_01"
    # Cooldown preserved
    assert s2.is_in_cooldown("BTCUSDT", t0) is True

    s3 = s2.with_holding_deadline("BTCUSDT", t0 + timedelta(hours=1))
    assert s3.policy_version == 4
    assert s3.holding_deadline_by_symbol["BTCUSDT"] == t0 + timedelta(hours=1)

    s4 = s3.with_signal_memory("ema_fast", Decimal("65100.00"))
    assert s4.policy_version == 5
    assert s4.signal_memory["ema_fast"] == Decimal("65100.00")

    s5 = s4.with_cleared_symbol("BTCUSDT")
    assert s5.policy_version == 6
    assert "BTCUSDT" not in s5.cooldown_until_by_symbol
    assert "BTCUSDT" not in s5.anchor_prices_by_symbol
    assert "BTCUSDT" not in s5.active_intent_ids_by_symbol
    assert "BTCUSDT" not in s5.holding_deadline_by_symbol
    # Global signal memory preserved
    assert s5.signal_memory["ema_fast"] == Decimal("65100.00")


def test_short_position_holding_exit_and_reversal_command() -> None:
    """Short position candle exit generates LONG reduce-only command
    and with_exit state.
    """
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
    mref, menv = _make_15s_state("BTCUSDT", t0, Decimal("64500.00"))

    # Short position opened 30 minutes ago
    opened_at = t0 - timedelta(minutes=30)
    pview = _make_position_view(
        "BTCUSDT",
        quantity=Decimal("1.5"),
        opened_at=opened_at,
        side=StrategySide.SHORT,
    )

    # Bullish candle (close > open) closing for BTCUSDT
    candle_start = t0 - timedelta(minutes=15)
    closed_candle = ClosedCandle15m(
        symbol="BTCUSDT",
        candle_start=candle_start,
        candle_end=t0,
        open_price=Decimal("64000.00"),
        close_price=Decimal("64500.00"),
    )

    clock = ClockEvent(timestamp=t0 + timedelta(seconds=15), sequence=5)
    frame = DecisionFrame(
        scope="test",
        symbol="BTCUSDT",
        clock_event=clock,
        market_refs=(mref,),
        position_view_token=pview.projection_version,
        universe_version="univ_v1",
    )
    # State with active anchor and deadline
    state = (
        PolicyState()
        .with_anchor_and_intent("BTCUSDT", Decimal("65000.00"), "cand_prev")
        .with_holding_deadline("BTCUSDT", opened_at + timedelta(hours=2))
    )
    policy = EffectivePolicy(
        policy_id="short_exit_policy",
        strategy_name="breakout",
        cooldown_duration=timedelta(minutes=15),
        exit_policy=PositionExitPolicy(max_holding_seconds=7200),
    )

    t = execute_policy_transition(
        frame=frame,
        prior_state=state,
        policy_artifact=policy,
        market_envelope=menv,
        position_view=pview,
        closed_candles=(closed_candle,),
    )

    assert t.exit_command is not None
    assert t.exit_command.command_type == TradeCommandType.EXIT
    # Closing a short position requires a LONG order
    assert t.exit_command.side == StrategySide.LONG
    assert t.exit_command.reduce_only is True
    assert t.exit_command.requested_quantity == Decimal("1.5")
    assert t.exit_command.reason == "candle_15m_bullish"

    # with_exit: Cooldown active and symbol anchor/deadline cleared
    assert t.next_state.is_in_cooldown("BTCUSDT", clock.timestamp)
    assert "BTCUSDT" not in t.next_state.anchor_prices_by_symbol
    assert "BTCUSDT" not in t.next_state.holding_deadline_by_symbol

    # Cooldown timer request emitted
    assert len(t.timer_requests) == 1
    assert t.timer_requests[0].timer_type == "cooldown_expiry"
    assert t.timer_requests[0].symbol == "BTCUSDT"


def test_short_position_max_holding_exit() -> None:
    """Short position held past max_holding_seconds produces LONG exit command."""
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
    mref, menv = _make_15s_state("BTCUSDT", t0, Decimal("64000.00"))

    # Opened 2 hours ago (> 1 hour max holding)
    opened_at = t0 - timedelta(hours=2)
    pview = _make_position_view(
        "BTCUSDT",
        quantity=Decimal("2.0"),
        opened_at=opened_at,
        side=StrategySide.SHORT,
    )

    clock = ClockEvent(timestamp=t0 + timedelta(seconds=15), sequence=6)
    frame = DecisionFrame(
        scope="test",
        symbol="BTCUSDT",
        clock_event=clock,
        market_refs=(mref,),
        position_view_token=pview.projection_version,
        universe_version="univ_v1",
    )
    state = PolicyState()
    policy = EffectivePolicy(
        policy_id="short_max_hold_policy",
        strategy_name="breakout",
        exit_policy=PositionExitPolicy(max_holding_seconds=3600),
    )

    t = execute_policy_transition(
        frame=frame,
        prior_state=state,
        policy_artifact=policy,
        market_envelope=menv,
        position_view=pview,
    )

    assert t.exit_command is not None
    assert t.exit_command.side == StrategySide.LONG
    assert t.exit_command.requested_quantity == Decimal("2.0")
    assert t.exit_command.reason == "max_holding_period"
    assert t.next_state.is_in_cooldown("BTCUSDT", clock.timestamp)


def test_short_breakout_entry_generation() -> None:
    """Breakout below short_entry_threshold produces SHORT candidate
    when mode allows.
    """
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
    # Price drops to 62500, below short_entry_threshold 63000
    mref, menv = _make_15s_state("BTCUSDT", t0, Decimal("62500.00"))
    pview = _make_position_view("BTCUSDT", Decimal("0"))
    clock = ClockEvent(timestamp=t0 + timedelta(seconds=15), sequence=7)
    frame = DecisionFrame(
        scope="test",
        symbol="BTCUSDT",
        clock_event=clock,
        market_refs=(mref,),
        position_view_token=pview.projection_version,
        universe_version="univ_v1",
    )
    state = PolicyState()

    # 1. Mode BOTH: Short candidate generated
    policy_both = EffectivePolicy(
        policy_id="both_breakout_policy",
        strategy_name="breakout",
        entry_threshold=Decimal("65000.00"),
        short_entry_threshold=Decimal("63000.00"),
        position_mode=StrategyPositionMode.BOTH,
        grace_period=timedelta(seconds=30),
    )
    t_both = execute_policy_transition(
        frame=frame,
        prior_state=state,
        policy_artifact=policy_both,
        market_envelope=menv,
        position_view=pview,
    )
    assert t_both.entry_candidate is not None
    assert t_both.entry_candidate.side == StrategySide.SHORT
    assert t_both.entry_candidate.reason == "breakout_below_short_threshold"
    assert t_both.next_state.anchor_prices_by_symbol["BTCUSDT"] == Decimal("62500.00")
    assert "BTCUSDT" in t_both.next_state.grace_until_by_symbol
    assert len(t_both.timer_requests) == 1
    assert t_both.timer_requests[0].timer_type == "grace_period_expiry"

    # 2. Mode LONG_ONLY: Short candidate rejected
    policy_long_only = EffectivePolicy(
        policy_id="long_only_policy",
        strategy_name="breakout",
        entry_threshold=Decimal("65000.00"),
        short_entry_threshold=Decimal("63000.00"),
        position_mode=StrategyPositionMode.LONG_ONLY,
    )
    t_long = execute_policy_transition(
        frame=frame,
        prior_state=state,
        policy_artifact=policy_long_only,
        market_envelope=menv,
        position_view=pview,
    )
    assert t_long.entry_candidate is None
    assert t_long.rejection_reason == "direction_not_permitted_by_position_mode"

    # 3. Mode SHORT_ONLY: Short candidate accepted
    policy_short_only = EffectivePolicy(
        policy_id="short_only_policy",
        strategy_name="breakout",
        entry_threshold=Decimal("65000.00"),
        short_entry_threshold=Decimal("63000.00"),
        position_mode=StrategyPositionMode.SHORT_ONLY,
    )
    t_short = execute_policy_transition(
        frame=frame,
        prior_state=state,
        policy_artifact=policy_short_only,
        market_envelope=menv,
        position_view=pview,
    )
    assert t_short.entry_candidate is not None
    assert t_short.entry_candidate.side == StrategySide.SHORT


def test_walk_forward_split_state_reset_and_carry() -> None:
    """PolicyState.reset_for_split cleans symbol state and optionally carries sizing."""
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
    s0 = (
        PolicyState(policy_version=10)
        .with_cooldown("BTCUSDT", t0 + timedelta(minutes=15))
        .with_anchor_and_intent("BTCUSDT", Decimal("65000.00"), "cand_01")
        .with_grace_until("BTCUSDT", t0 + timedelta(minutes=5))
        .with_holding_deadline("BTCUSDT", t0 + timedelta(hours=1))
        .with_sizing_state("BTCUSDT", {"target_quantity": "0.1"})
        .with_signal_memory("ema_diff", Decimal("12.5"))
    )

    # 1. Clean slate split (carry_sizing=False)
    s_clean = s0.reset_for_split(carry_sizing=False)
    assert s_clean.policy_version == s0.policy_version + 1
    assert s_clean.cooldown_until_by_symbol == {}
    assert s_clean.anchor_prices_by_symbol == {}
    assert s_clean.active_intent_ids_by_symbol == {}
    assert s_clean.grace_until_by_symbol == {}
    assert s_clean.holding_deadline_by_symbol == {}
    assert s_clean.sizing_state_by_symbol == {}
    assert s_clean.signal_memory == {}

    # 2. Split with sizing carry (carry_sizing=True)
    s_carry = s0.reset_for_split(carry_sizing=True)
    assert s_carry.policy_version == s0.policy_version + 1
    assert s_carry.cooldown_until_by_symbol == {}
    assert s_carry.anchor_prices_by_symbol == {}
    assert s_carry.active_intent_ids_by_symbol == {}
    assert s_carry.grace_until_by_symbol == {}
    assert s_carry.holding_deadline_by_symbol == {}
    assert s_carry.sizing_state_by_symbol == {"BTCUSDT": {"target_quantity": "0.1"}}
    assert s_carry.signal_memory == {}


def test_policy_state_with_exit_atomic_purge() -> None:
    """with_exit atomically sets cooldown and purges symbol state
    while preserving other symbols.
    """
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
    cd_due = t0 + timedelta(minutes=15)
    s0 = (
        PolicyState(policy_version=1)
        .with_anchor_and_intent("BTCUSDT", Decimal("65000.00"), "cand_btc")
        .with_grace_until("BTCUSDT", t0 + timedelta(minutes=5))
        .with_holding_deadline("BTCUSDT", t0 + timedelta(hours=1))
        .with_sizing_state("BTCUSDT", {"qty": "1.0"})
        .with_anchor_and_intent("ETHUSDT", Decimal("3500.00"), "cand_eth")
        .with_signal_memory("ema_trend", Decimal("1.0"))
    )

    s1 = s0.with_exit("BTCUSDT", cd_due)
    assert s1.policy_version == s0.policy_version + 1
    assert s1.cooldown_until_by_symbol["BTCUSDT"] == cd_due
    assert "BTCUSDT" not in s1.anchor_prices_by_symbol
    assert "BTCUSDT" not in s1.active_intent_ids_by_symbol
    assert "BTCUSDT" not in s1.grace_until_by_symbol
    assert "BTCUSDT" not in s1.holding_deadline_by_symbol
    assert "BTCUSDT" not in s1.sizing_state_by_symbol

    # ETHUSDT and global signal memory must be completely untouched
    assert s1.anchor_prices_by_symbol["ETHUSDT"] == Decimal("3500.00")
    assert s1.active_intent_ids_by_symbol["ETHUSDT"] == "cand_eth"
    assert s1.signal_memory["ema_trend"] == Decimal("1.0")

