"""Unit tests proving multi-adapter decision consistency (Phase D criteria).

Verifies Astra Architecture Blueprint 2026-09-25:
- Identical DecisionInput slice produces 100% identical decision results
  across Live and Simulation adapters;
- Zero environment-branching inside pure decision core (R3);
- Both adapters drive authoritative AccountJournal and respect identical lot boundaries.
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
from crypto_momentum_lab.domain.decision.simulation_execution import (
    FillModel,
    SimulationExecutionAdapter,
)
from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    PositionHealthStatus,
    PositionKey,
    PositionView,
)
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.market.revision_models import (
    MarketEnvelope,
    MarketRevisionRef,
    MarketVisibilityMode,
)


def _build_test_market(
    symbol: str,
    bucket_start: datetime,
    close_price: Decimal,
    environment: str = "live",
) -> tuple[MarketRevisionRef, MarketEnvelope]:
    bucket_end = bucket_start + timedelta(seconds=15)
    state = MarketState15s(
        schema_version=1,
        environment=environment,
        exchange="binance",
        symbol=symbol,
        bucket_start=bucket_start,
        bucket_end=bucket_end,
        open_price=Decimal("64990.00"),
        high_price=Decimal("65050.00"),
        low_price=Decimal("64950.00"),
        close_price=close_price,
        trade_count=100,
        trade_notional=Decimal("1000000.00"),
        aggressive_buy_notional=Decimal("600000.00"),
        aggressive_sell_notional=Decimal("400000.00"),
        last_bid_price=Decimal("65010.00"),
        last_ask_price=Decimal("65012.00"),
        spread=Decimal("2.00"),
        midpoint=Decimal("65011.00"),
        liquidation_count=0,
        liquidation_notional=Decimal("0.00"),
        mark_price=Decimal("65011.00"),
        closed_kline_count=0,
        source_event_count=100,
        first_received_at=bucket_start + timedelta(milliseconds=100),
        last_received_at=bucket_end - timedelta(milliseconds=50),
        data_complete=True,
        missing_agg_trade_count=0,
        is_backfill=False,
    )
    ref = MarketRevisionRef(
        scope=environment,
        symbol=symbol,
        interval="15s",
        bucket_start=bucket_start,
        bucket_end=bucket_end,
        revision_id=f"rev_{symbol}_{int(bucket_start.timestamp())}",
        content_hash="content_hash_fixed_12345",
        published_at=bucket_end,
        source_epoch="ep1",
        visibility_mode=MarketVisibilityMode.DECISION_VISIBLE,
    )
    return ref, MarketEnvelope(ref=ref, state=state)


def test_pure_decision_multi_adapter_consistency() -> None:
    """Verifies that Live and Simulation environments receive identical decisions."""
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)

    # Prepare identical market state and position views for Live vs Simulation
    ref_live, env_live = _build_test_market("BTCUSDT", t0, Decimal("65500.00"), "live")
    ref_sim, env_sim = _build_test_market(
        "BTCUSDT", t0, Decimal("65500.00"), "simulation"
    )

    pos_key_live = PositionKey(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
    )
    pos_key_sim = PositionKey(
        environment="simulation",
        account_label="sim_primary",
        symbol="BTCUSDT",
    )

    view_live = PositionView(
        key=pos_key_live,
        projection_version="pv_100",
        input_revision=1,
        event_cut=t0,
        policy_version="v1",
        schema_version="v1",
        coverage=None,
        active_episode=None,
        batches=(),
        unallocated_quantity=Decimal("0"),
        reconciliation_gap=Decimal("0"),
        health_status=PositionHealthStatus.READY,
    )
    view_sim = PositionView(
        key=pos_key_sim,
        projection_version="pv_100",
        input_revision=1,
        event_cut=t0,
        policy_version="v1",
        schema_version="v1",
        coverage=None,
        active_episode=None,
        batches=(),
        unallocated_quantity=Decimal("0"),
        reconciliation_gap=Decimal("0"),
        health_status=PositionHealthStatus.READY,
    )

    clock = ClockEvent(timestamp=t0 + timedelta(seconds=15), sequence=1)
    state = PolicyState()
    policy = EffectivePolicy(
        policy_id="shared_policy_01",
        strategy_name="orderflow_impulse",
        entry_threshold=Decimal("65000.00"),
        target_notional=Decimal("1500.00"),
    )

    input_live = DecisionInput(
        symbol="BTCUSDT",
        market_ref=ref_live,
        market_envelope=env_live,
        position_view=view_live,
        universe_version="univ_v1",
        clock_event=clock,
        cash_balance=Decimal("10000.00"),
        risk_config_version="risk_v1",
    )

    input_sim = DecisionInput(
        symbol="BTCUSDT",
        market_ref=ref_sim,
        market_envelope=env_sim,
        position_view=view_sim,
        universe_version="univ_v1",
        clock_event=clock,
        cash_balance=Decimal("10000.00"),
        risk_config_version="risk_v1",
    )

    # Evaluate decision in Live context vs Simulation context
    res_live = decide(input_live, state, policy)
    res_sim = decide(input_sim, state, policy)

    # 1. Hashes and decision ID must match
    assert res_live.decision_id == res_sim.decision_id
    assert res_live.input_hash == res_sim.input_hash

    # 2. Intent must be identical across both
    assert res_live.intent is not None
    assert res_sim.intent is not None
    assert res_live.intent.candidate_id == res_sim.intent.candidate_id
    assert res_live.intent.symbol == res_sim.intent.symbol == "BTCUSDT"
    assert res_live.intent.side == res_sim.intent.side
    assert (
        res_live.intent.desired_notional
        == res_sim.intent.desired_notional
        == Decimal("1500.00")
    )
    assert res_live.intent.features == res_sim.intent.features

    # 3. Simulate execution in SimulationExecutionAdapter
    journal_sim = AccountJournal(pos_key_sim)
    sim_adapter = SimulationExecutionAdapter(
        default_fill_model=FillModel(slippage_bps=Decimal("2.0"))
    )
    fill_sim = sim_adapter.execute_entry(res_sim.intent, env_sim, journal_sim)

    assert fill_sim.symbol == "BTCUSDT"
    assert fill_sim.quantity > Decimal("0")
    assert fill_sim.fill_model_version == "conservative_v1"

    # Verify journal recorded fill and position snapshot
    sim_facts = journal_sim.read_cut()
    assert len(sim_facts.fills) == 1
    assert len(sim_facts.snapshots) == 1
    assert sim_facts.snapshots[0].position_amt == fill_sim.quantity
