"""Decision filter must not invent READY positions or empty PolicyState."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

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
from crypto_momentum_lab.domain.strategy.models import StrategyDecision


def _state() -> MarketState15s:
    start = datetime(2026, 9, 25, 8, 0, tzinfo=UTC)
    return MarketState15s(
        schema_version=1,
        exchange="binance",
        environment="live",
        symbol="BTCUSDT",
        bucket_start=start,
        bucket_end=datetime(2026, 9, 25, 8, 0, 15, tzinfo=UTC),
        open_price=Decimal("100"),
        high_price=Decimal("101"),
        low_price=Decimal("99"),
        close_price=Decimal("100"),
        trade_count=1,
        trade_notional=Decimal("1"),
        aggressive_buy_notional=Decimal("1"),
        aggressive_sell_notional=Decimal("0"),
        last_bid_price=Decimal("100"),
        last_ask_price=Decimal("100"),
        spread=Decimal("0"),
        midpoint=Decimal("100"),
        liquidation_count=0,
        liquidation_notional=Decimal("0"),
        mark_price=Decimal("100"),
        closed_kline_count=0,
        source_event_count=1,
        first_received_at=start,
        last_received_at=start,
    )


def _decision_with_candidate() -> StrategyDecision:
    return StrategyDecision(signals=(), candidates=(), rejections=())


def test_filter_without_provider_rejects_all_candidates() -> None:
    filt = create_authoritative_decision_filter("strat")
    out = filt(_decision_with_candidate(), _state())
    # No fabricated READY path: empty candidates is fine when none offered.
    assert out.candidates == ()


def test_filter_rejects_when_frozen_inputs_missing() -> None:
    filt = create_authoritative_decision_filter(
        "strat",
        fact_provider=lambda state: None,
    )
    out = filt(_decision_with_candidate(), _state())
    assert out.candidates == ()


def test_filter_rejects_non_ready_position_health() -> None:
    key = PositionKey(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )
    view = PositionView(
        key=key,
        projection_version="pv1",
        input_revision=1,
        event_cut=datetime(2026, 9, 25, 8, 0, tzinfo=UTC),
        policy_version="v1",
        schema_version="v1",
        coverage=FactCoverageInterval(
            start_at=datetime(2026, 9, 25, 7, 0, tzinfo=UTC),
            end_at=datetime(2026, 9, 25, 8, 0, tzinfo=UTC),
            status=FactCoverageStatus.PENDING,
        ),
        active_episode=None,
        batches=(),
        unallocated_quantity=Decimal("0"),
        reconciliation_gap=Decimal("0"),
        health_status=PositionHealthStatus.CATCHING_UP,
    )
    frozen = FrozenDecisionInputs(
        position_view=view,
        cash_balance=Decimal("100"),
        policy_state=PolicyState(),
        universe_version="univ_v1",
        risk_config_version="risk_v1",
    )
    filt = create_authoritative_decision_filter(
        "strat",
        fact_provider=lambda state: frozen,
    )
    out = filt(_decision_with_candidate(), _state())
    assert out.candidates == ()


def test_filter_rejects_symbol_mismatch() -> None:
    key = PositionKey(
        environment="live",
        account_label="primary",
        symbol="ETHUSDT",
        position_side=FuturesPositionSide.BOTH,
    )
    view = PositionView(
        key=key,
        projection_version="pv1",
        input_revision=1,
        event_cut=datetime(2026, 9, 25, 8, 0, tzinfo=UTC),
        policy_version="v1",
        schema_version="v1",
        coverage=None,
        active_episode=None,
        batches=(),
        unallocated_quantity=Decimal("0"),
        reconciliation_gap=Decimal("0"),
        health_status=PositionHealthStatus.READY,
    )
    frozen = FrozenDecisionInputs(
        position_view=view,
        cash_balance=Decimal("100"),
        policy_state=PolicyState(),
        universe_version="univ_v1",
        risk_config_version="risk_v1",
    )
    filt = create_authoritative_decision_filter(
        "strat",
        fact_provider=lambda state: frozen,
    )
    out = filt(_decision_with_candidate(), _state())
    assert out.candidates == ()


def test_frozen_inputs_reject_negative_cash() -> None:
    key = PositionKey(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )
    view = PositionView(
        key=key,
        projection_version="pv1",
        input_revision=1,
        event_cut=datetime(2026, 9, 25, 8, 0, tzinfo=UTC),
        policy_version="v1",
        schema_version="v1",
        coverage=None,
        active_episode=None,
        batches=(),
        unallocated_quantity=Decimal("0"),
        reconciliation_gap=Decimal("0"),
        health_status=PositionHealthStatus.READY,
    )
    try:
        FrozenDecisionInputs(
            position_view=view,
            cash_balance=Decimal("-1"),
            policy_state=PolicyState(),
            universe_version="univ_v1",
            risk_config_version="risk_v1",
        )
    except ValueError as exc:
        assert "cash_balance" in str(exc)
    else:
        raise AssertionError("negative cash must be rejected")


def test_filter_invokes_on_decision_result_callback() -> None:
    from crypto_momentum_lab.domain.decision.decision_engine import DecisionResult
    from crypto_momentum_lab.domain.strategy.models import (
        EntryType,
        OrderIntentCandidate,
        StrategySide,
    )

    key = PositionKey(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )
    view = PositionView(
        key=key,
        projection_version="pv1",
        input_revision=1,
        event_cut=datetime(2026, 9, 25, 8, 0, tzinfo=UTC),
        policy_version="v1",
        schema_version="v1",
        coverage=FactCoverageInterval(
            start_at=datetime(2026, 9, 25, 7, 0, tzinfo=UTC),
            end_at=datetime(2026, 9, 25, 8, 0, tzinfo=UTC),
            status=FactCoverageStatus.CONFIRMED,
        ),
        active_episode=None,
        batches=(),
        unallocated_quantity=Decimal("0"),
        reconciliation_gap=Decimal("0"),
        health_status=PositionHealthStatus.READY,
    )
    frozen = FrozenDecisionInputs(
        position_view=view,
        cash_balance=Decimal("1000"),
        policy_state=PolicyState(policy_version=3),
        universe_version="univ_v1",
        risk_config_version="risk_v1",
    )

    results_captured: list[DecisionResult] = []
    filt = create_authoritative_decision_filter(
        "orderflow_impulse",
        target_notional=Decimal("500"),
        fact_provider=lambda state: frozen,
        on_decision_result=results_captured.append,
    )

    from crypto_momentum_lab.domain.strategy.models import (
        StrategySignal,
    )

    sig = StrategySignal(
        signal_id="sig_1",
        run_id="run_1",
        strategy_name="orderflow_impulse",
        strategy_version="v1",
        config_hash="cfg_hash",
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        detected_at=datetime(2026, 9, 25, 8, 0, tzinfo=UTC),
        source_state_at=datetime(2026, 9, 25, 8, 0, tzinfo=UTC),
        reason="momentum_signal",
        features={},
        reference_prices={},
    )
    cand = OrderIntentCandidate(
        candidate_id="cand_1",
        signal_id="sig_1",
        run_id="run_1",
        strategy_name="orderflow_impulse",
        strategy_version="v1",
        config_hash="cfg_hash",
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        entry_type=EntryType.MARKET,
        limit_price=Decimal("100"),
        desired_notional=Decimal("500"),
        reduce_only=False,
        expires_at=datetime(2026, 9, 25, 8, 1, tzinfo=UTC),
        created_at=datetime(2026, 9, 25, 8, 0, tzinfo=UTC),
        reason="momentum_entry",
        features={},
    )
    dec = StrategyDecision(signals=(sig,), candidates=(cand,), rejections=())
    out = filt(dec, _state())

    assert len(results_captured) == 1
    assert results_captured[0].next_policy_state.policy_version > 3
    assert len(out.candidates) == 1


def test_filter_evaluates_open_position_exit_when_candidates_empty() -> None:
    """Empty candidates with an open position must trigger holding evaluation
    and on_decision_result.
    """
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
        event_cut=datetime(2026, 9, 25, 8, 0, tzinfo=UTC),
        policy_version="v1",
        schema_version="v1",
        coverage=FactCoverageInterval(
            start_at=datetime(2026, 9, 25, 6, 0, tzinfo=UTC),
            end_at=datetime(2026, 9, 25, 8, 0, tzinfo=UTC),
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

    results_captured = []
    filt = create_authoritative_decision_filter(
        "orderflow_impulse",
        target_notional=Decimal("500"),
        fact_provider=lambda state: frozen,
        on_decision_result=results_captured.append,
    )

    # Empty candidates decision (clock tick only)
    dec = StrategyDecision(signals=(), candidates=(), rejections=())
    out = filt(dec, _state())

    assert out.candidates == ()
    # Holding evaluation must have executed and recorded result
    assert len(results_captured) == 1
    res = results_captured[0]
    assert res.transition.exit_command is not None
    assert res.next_policy_state.is_in_cooldown(
        "BTCUSDT", datetime(2026, 9, 25, 8, 0, tzinfo=UTC)
    )


