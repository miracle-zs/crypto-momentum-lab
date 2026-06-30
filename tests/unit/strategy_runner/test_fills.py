from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    RunMode,
    StrategyRunIdentity,
    StrategySide,
    StrategySignal,
)
from crypto_momentum_lab.strategy_runner.fills import (
    ReplayExecutionConfig,
    SimulatedFill,
    SimulatedFillStatus,
    deterministic_fill_id,
    fill_summary,
    pending_candidate_fill,
    simulate_candidate_fill,
)


def test_simulate_candidate_fill_uses_latency_fee_spread_and_fill_id() -> None:
    identity = _identity()
    signal = _signal(identity)
    candidate = _candidate(identity=identity, signal_id=signal.signal_id)
    state = _state(5, close=Decimal("101.4"))

    fill = simulate_candidate_fill(
        candidate=candidate,
        states=(state,),
        execution=ReplayExecutionConfig(
            latency_buckets=1,
            taker_fee_rate=Decimal("0.0004"),
            slippage_bps=Decimal("1"),
        ),
    )

    assert fill.fill_id.startswith("fill_")
    assert fill.fill_id == deterministic_fill_id(candidate_id=candidate.candidate_id)
    assert fill.status is SimulatedFillStatus.FILLED
    assert fill.target_fill_at == datetime(2026, 6, 22, 0, 1, 15, tzinfo=UTC)
    assert fill.filled_at == datetime(2026, 6, 22, 0, 1, 15, tzinfo=UTC)
    assert fill.fill_price == Decimal("101.420141")
    assert fill.fee == Decimal("0.0400")
    assert fill.total_cost > fill.fee


def test_pending_fill_records_source_ended_before_fill() -> None:
    identity = _identity()
    candidate = _candidate(
        identity=identity,
        signal_id="sig_1",
        created_at=datetime(2026, 6, 22, 0, 1, tzinfo=UTC),
    )

    fill = pending_candidate_fill(
        candidate=candidate,
        execution=ReplayExecutionConfig(latency_buckets=2),
        reason="source_ended_before_fill",
    )

    assert fill.fill_id == deterministic_fill_id(candidate_id=candidate.candidate_id)
    assert fill.status is SimulatedFillStatus.PENDING
    assert fill.filled_at is None
    assert fill.reason == "source_ended_before_fill"


def test_fill_summary_counts_status_and_costs() -> None:
    filled = _filled_fill(symbol="BTCUSDT", notional=Decimal("100"))
    pending = _pending_fill(symbol="BTCUSDT")

    summary = fill_summary((filled, pending))

    assert summary["fills_by_status"] == {"filled": 1, "pending": 1}
    assert summary["filled_notional_by_symbol"] == {"BTCUSDT": Decimal("100")}


def _identity() -> StrategyRunIdentity:
    return StrategyRunIdentity(
        run_id="run-1",
        strategy_name="compression_breakout",
        strategy_version="v0",
        config_hash="abc",
        run_mode=RunMode.PAPER,
        code_commit="unknown",
        created_at=datetime(2026, 6, 22, 0, 0, tzinfo=UTC),
        source_paths=("memory",),
    )


def _signal(identity: StrategyRunIdentity) -> StrategySignal:
    detected_at = datetime(2026, 6, 22, 0, 1, tzinfo=UTC)
    return StrategySignal(
        signal_id="sig_1",
        run_id=identity.run_id,
        strategy_name=identity.strategy_name,
        strategy_version=identity.strategy_version,
        config_hash=identity.config_hash,
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        detected_at=detected_at,
        source_state_at=detected_at,
        reason="compression_breakout",
        features={"breakout_price": "101.2"},
        reference_prices={"midpoint": "101.2"},
    )


def _candidate(
    *,
    identity: StrategyRunIdentity,
    signal_id: str,
    created_at: datetime = datetime(2026, 6, 22, 0, 1, tzinfo=UTC),
) -> OrderIntentCandidate:
    return OrderIntentCandidate(
        candidate_id="cand_1",
        signal_id=signal_id,
        run_id=identity.run_id,
        strategy_name=identity.strategy_name,
        strategy_version=identity.strategy_version,
        config_hash=identity.config_hash,
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        entry_type=EntryType.MARKET,
        limit_price=None,
        desired_notional=Decimal("100"),
        reduce_only=False,
        expires_at=created_at + timedelta(seconds=30),
        created_at=created_at,
        reason="compression_breakout",
        features={"breakout_price": "101.2"},
    )


def _state(bucket_index: int, *, close: Decimal) -> MarketState15s:
    bucket_start = datetime(2026, 6, 22, 0, 0, tzinfo=UTC) + timedelta(
        seconds=15 * bucket_index
    )
    bucket_end = bucket_start + timedelta(seconds=15)
    return MarketState15s(
        schema_version=1,
        exchange="binance-usdm",
        environment="research",
        symbol="BTCUSDT",
        bucket_start=bucket_start,
        bucket_end=bucket_end,
        open_price=close,
        high_price=close,
        low_price=close,
        close_price=close,
        trade_count=10,
        trade_notional=Decimal("1000"),
        aggressive_buy_notional=Decimal("600"),
        aggressive_sell_notional=Decimal("400"),
        last_bid_price=close - Decimal("0.01"),
        last_ask_price=close + Decimal("0.01"),
        spread=Decimal("0.02"),
        midpoint=close,
        liquidation_count=0,
        liquidation_notional=Decimal("0"),
        mark_price=close,
        closed_kline_count=0,
        source_event_count=10,
        first_received_at=bucket_start,
        last_received_at=bucket_end,
    )


def _filled_fill(*, symbol: str, notional: Decimal) -> SimulatedFill:
    filled_at = datetime(2026, 6, 22, 0, 1, 15, tzinfo=UTC)
    return SimulatedFill(
        fill_id="fill_1",
        candidate_id="cand_1",
        signal_id="sig_1",
        symbol=symbol,
        side=StrategySide.LONG,
        status=SimulatedFillStatus.FILLED,
        target_fill_at=filled_at,
        filled_at=filled_at,
        requested_notional=notional,
        filled_notional=notional,
        quantity=Decimal("1"),
        reference_midpoint=Decimal("100"),
        spread=Decimal("0.02"),
        fill_price=Decimal("100.01"),
        fee=Decimal("0.04"),
        total_cost=Decimal("0.05"),
        cost_bps=Decimal("5"),
        reason="filled",
    )


def _pending_fill(*, symbol: str) -> SimulatedFill:
    target_fill_at = datetime(2026, 6, 22, 0, 1, 30, tzinfo=UTC)
    return SimulatedFill(
        fill_id="fill_2",
        candidate_id="cand_2",
        signal_id="sig_2",
        symbol=symbol,
        side=StrategySide.LONG,
        status=SimulatedFillStatus.PENDING,
        target_fill_at=target_fill_at,
        filled_at=None,
        requested_notional=Decimal("100"),
        filled_notional=None,
        quantity=None,
        reference_midpoint=None,
        spread=None,
        fill_price=None,
        fee=Decimal("0"),
        total_cost=Decimal("0"),
        cost_bps=None,
        reason="source_ended_before_fill",
    )
