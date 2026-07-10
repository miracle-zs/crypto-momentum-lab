from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.account import ExecutionAccountStatus
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.risk.models import (
    RiskDecision,
    StrategyLiveState,
    TradingLease,
    TradingLeaseState,
)
from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    StrategySide,
)
from crypto_momentum_lab.risk.gateway import RiskContext, RiskGateway
from tests.unit.domain.risk.test_models import _risk_config


def test_gateway_rejects_missing_active_lease() -> None:
    evaluation = RiskGateway().evaluate(_intent(), _context(active_lease=None))

    assert evaluation.decision is RiskDecision.REJECTED
    assert evaluation.reason == "missing_active_lease"


def test_gateway_rejects_stale_market_state() -> None:
    context = _context(
        now=datetime(2026, 7, 4, 0, 2, tzinfo=UTC),
        market_state=_market_state(0),
    )

    evaluation = RiskGateway().evaluate(_intent(), context)

    assert evaluation.decision is RiskDecision.REJECTED
    assert evaluation.reason == "stale_market_state"


def test_gateway_rejects_account_not_ready() -> None:
    evaluation = RiskGateway().evaluate(
        _intent(),
        _context(account_state=ExecutionAccountStatus.DEGRADED),
    )

    assert evaluation.decision is RiskDecision.REJECTED
    assert evaluation.reason == "account_not_ready"


def test_gateway_approves_small_entry_when_all_limits_pass() -> None:
    evaluation = RiskGateway().evaluate(_intent(), _context())

    assert evaluation.decision is RiskDecision.APPROVED
    assert evaluation.reason == "approved"


def test_gateway_allows_reduce_only_while_draining() -> None:
    evaluation = RiskGateway().evaluate(
        _intent(reduce_only=True),
        _context(strategy_state=StrategyLiveState.DRAINING),
    )

    assert evaluation.decision is RiskDecision.APPROVED
    assert evaluation.reason == "reduce_only_draining"


def _context(
    *,
    active_lease: TradingLease | None | object = "default",
    market_state=None,
    account_state: ExecutionAccountStatus = ExecutionAccountStatus.READY_READONLY,
    strategy_state: StrategyLiveState = StrategyLiveState.ACTIVE,
    now: datetime = datetime(2026, 7, 4, 0, 0, 20, tzinfo=UTC),
) -> RiskContext:
    lease = _lease() if active_lease == "default" else active_lease
    return RiskContext(
        now=now,
        active_lease=lease,
        latest_market_state=market_state or _market_state(0),
        account_state=account_state,
        open_position_symbols=frozenset(),
        active_halts=(),
        risk_config=_risk_config(max_order_notional=Decimal("100")),
        strategy_state=strategy_state,
    )


def _lease() -> TradingLease:
    return TradingLease(
        lease_id="lease-1",
        environment="live",
        account_label="primary",
        strategy_name="compression_breakout",
        owner="worker-1",
        state=TradingLeaseState.ACTIVE,
        acquired_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        expires_at=datetime(2026, 7, 4, 0, 5, tzinfo=UTC),
    )


def _intent(reduce_only: bool = False) -> OrderIntentCandidate:
    now = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)
    return OrderIntentCandidate(
        candidate_id="candidate-1",
        signal_id="signal-1",
        run_id="run-1",
        strategy_name="compression_breakout",
        strategy_version="v0",
        config_hash="a" * 64,
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        entry_type=EntryType.MARKET,
        limit_price=None,
        desired_notional=Decimal("50"),
        reduce_only=reduce_only,
        expires_at=now + timedelta(seconds=30),
        created_at=now,
        reason="test",
        features={},
    )


def _market_state(bucket_index: int) -> MarketState15s:
    bucket_start = datetime(2026, 7, 4, 0, 0, tzinfo=UTC) + timedelta(
        seconds=15 * bucket_index
    )
    return MarketState15s(
        schema_version=1,
        exchange="binance-usdm",
        environment="live",
        symbol="BTCUSDT",
        bucket_start=bucket_start,
        bucket_end=bucket_start + timedelta(seconds=15),
        open_price=Decimal("100"),
        high_price=Decimal("100"),
        low_price=Decimal("100"),
        close_price=Decimal("100"),
        trade_count=1,
        trade_notional=Decimal("100"),
        aggressive_buy_notional=Decimal("60"),
        aggressive_sell_notional=Decimal("40"),
        last_bid_price=Decimal("99.99"),
        last_ask_price=Decimal("100.01"),
        spread=Decimal("0.02"),
        midpoint=Decimal("100"),
        liquidation_count=0,
        liquidation_notional=Decimal("0"),
        mark_price=Decimal("100"),
        closed_kline_count=0,
        source_event_count=1,
        first_received_at=bucket_start,
        last_received_at=bucket_start + timedelta(seconds=15),
    )
