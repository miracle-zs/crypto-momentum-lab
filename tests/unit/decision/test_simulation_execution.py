"""Unit tests for SimulationExecution adapter (R3).

Tests:
1. Versioned FillModel validation and slippage calculations (BUY vs SELL);
2. Entry execution creating canonical AccountFillEvent and AccountPositionSnapshot
   in AccountJournal;
3. Exit execution against reserved lots and coordinator fill reconciliation;
4. Enforcement of invariants (e.g. non-exit commands rejected).
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.decision.simulation_execution import (
    FillModel,
    SimulationExecutionAdapter,
)
from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
from crypto_momentum_lab.domain.execution.execution_coordinator import (
    ExecutionCoordinator,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    PositionHealthStatus,
    PositionKey,
    PositionLedgerBatch,
    PositionView,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    ExitAllocation,
    ExitAllocationPlan,
    ExitPolicyMode,
    TradeCommand,
    TradeCommandType,
)
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


def _make_market_envelope(
    symbol: str,
    bucket_start: datetime,
    close_price: Decimal,
) -> tuple[MarketRevisionRef, MarketEnvelope]:
    bucket_end = bucket_start + timedelta(seconds=15)
    state = MarketState15s(
        schema_version=1,
        environment="paper",
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
        scope="paper",
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


def test_fill_model_slippage_calculation() -> None:
    model = FillModel(
        model_version="test_v1",
        slippage_bps=Decimal("10.0"),  # 10 bps = 0.1%
        fee_rate=Decimal("0.0005"),
    )
    base = Decimal("50000.00")

    # BUY executed price must be higher than base price
    buy_exec, buy_slip = model.compute_executed_price(base, "BUY")
    assert buy_exec == Decimal("50050.00")
    assert buy_slip == Decimal("50.00")

    # SELL executed price must be lower than base price
    sell_exec, sell_slip = model.compute_executed_price(base, "SELL")
    assert sell_exec == Decimal("49950.00")
    assert sell_slip == Decimal("50.00")


def test_simulation_execution_entry_flow() -> None:
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
    _, menv = _make_market_envelope("BTCUSDT", t0, Decimal("65000.00"))

    pos_key = PositionKey(
        environment="paper",
        account_label="paper_account",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )
    journal = AccountJournal(pos_key)
    adapter = SimulationExecutionAdapter()

    intent = OrderIntentCandidate(
        candidate_id="cand_test_01",
        signal_id="sig_test_01",
        run_id="run_sim_01",
        strategy_name="orderflow_impulse",
        strategy_version="v1",
        config_hash="conf_hash_01",
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        entry_type=EntryType.MARKET,
        limit_price=None,
        desired_notional=Decimal("13000.00"),
        reduce_only=False,
        expires_at=t0 + timedelta(minutes=5),
        created_at=t0,
        reason="test_entry",
        features={"score": 1.0},
    )

    fill_result = adapter.execute_entry(intent, menv, journal)

    assert fill_result.symbol == "BTCUSDT"
    assert fill_result.side == "BUY"
    assert fill_result.quantity > Decimal("0")
    assert fill_result.price > Decimal("65000.00")  # slippage added
    assert fill_result.fee > Decimal("0")

    # Verify journal facts
    facts = journal.read_cut()
    assert len(facts.fills) == 1
    assert facts.fills[0].trade_id == fill_result.fill_id
    assert facts.fills[0].quantity == fill_result.quantity
    assert len(facts.snapshots) == 1
    assert facts.snapshots[0].position_amt == fill_result.quantity


def test_simulation_execution_exit_flow_with_reservation() -> None:
    t0 = datetime(2026, 9, 25, 12, 0, 0, tzinfo=UTC)
    _, menv = _make_market_envelope("BTCUSDT", t0, Decimal("64000.00"))

    pos_key = PositionKey(
        environment="paper",
        account_label="paper_account",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )
    journal = AccountJournal(pos_key)
    coordinator = ExecutionCoordinator()
    adapter = SimulationExecutionAdapter()

    batch = PositionLedgerBatch(
        batch_id="batch_sim_100",
        episode_id="ep_sim_01",
        quantity=Decimal("0.5"),
        original_quantity=Decimal("0.5"),
        entry_price=Decimal("65000.00"),
        opened_at=t0 - timedelta(hours=1),
    )
    pview = PositionView(
        key=pos_key,
        projection_version="pv_sim_open",
        input_revision=1,
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

    alloc_plan = ExitAllocationPlan(
        position_key=pos_key,
        allocations=(
            ExitAllocation(
                batch_id="batch_sim_100",
                allocated_quantity=Decimal("0.5"),
                entry_price=Decimal("65000.00"),
            ),
        ),
        total_allocated_quantity=Decimal("0.5"),
        policy=ExitPolicyMode.FULL_POSITION_CLOSE,
        reason="stop_loss",
    )
    cmd = TradeCommand(
        command_id="cmd_exit_sim_01",
        position_key=pos_key,
        command_type=TradeCommandType.EXIT,
        side=StrategySide.SHORT,
        order_type=EntryType.MARKET,
        requested_quantity=Decimal("0.5"),
        reduce_only=True,
        allocation_plan=alloc_plan,
        reason="stop_loss",
        created_at=t0,
    )

    # 1. Coordinator reserves the lot
    reservations = coordinator.reserve_exit(cmd, pview)
    assert len(reservations) == 1
    assert reservations[0].active_quantity == Decimal("0.5")

    # 2. Adapter executes the exit
    fill_result = adapter.execute_exit(
        cmd,
        menv,
        journal,
        coordinator,
        reservation_id=reservations[0].reservation_id,
    )

    assert fill_result.symbol == "BTCUSDT"
    assert fill_result.side == "SELL"
    assert fill_result.quantity == Decimal("0.5")
    expected_pnl = (fill_result.price - Decimal("65000.00")) * Decimal("0.5")
    assert fill_result.realized_pnl == expected_pnl
    assert fill_result.realized_pnl < Decimal("0")
    assert journal.read_cut().fills[0].realized_pnl == expected_pnl

    # 3. Reservation should now be fully consumed (active_quantity == 0)
    active_res = coordinator.get_active_reservations(pos_key)
    assert len(active_res) == 0

    # 4. Invariant: non-exit command is rejected
    non_exit_cmd = TradeCommand(
        command_id="cmd_entry_sim_02",
        position_key=pos_key,
        command_type=TradeCommandType.ENTRY,
        side=StrategySide.LONG,
        order_type=EntryType.MARKET,
        requested_quantity=Decimal("0.5"),
        created_at=t0,
    )
    with pytest.raises(ValueError, match="execute_exit requires EXIT command_type"):
        adapter.execute_exit(non_exit_cmd, menv, journal, coordinator)

