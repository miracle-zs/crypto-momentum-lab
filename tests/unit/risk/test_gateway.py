from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account import ExecutionAccountStatus
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.risk.limits import FixedLiveLimits, LiveLimitContext
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
    evaluation = (
        RiskGateway().evaluate(_intent(), _context(active_lease=None)).evaluation
    )

    assert evaluation.decision is RiskDecision.REJECTED
    assert evaluation.reason == "missing_active_lease"


def test_gateway_rejects_lease_fencing_mismatch() -> None:
    evaluation = (
        RiskGateway()
        .evaluate(
            _intent(),
            replace(
                _context(),
                required_lease_owner="another-worker",
                required_lease_id="lease-2",
                required_account_label="primary",
                required_strategy_name="compression_breakout",
            ),
        )
        .evaluation
    )

    assert evaluation.decision is RiskDecision.REJECTED
    assert evaluation.reason == "lease_owner_mismatch"


def test_gateway_rejects_stale_market_state() -> None:
    context = _context(
        now=datetime(2026, 7, 4, 0, 2, tzinfo=UTC),
        market_state=_market_state(0),
    )

    evaluation = RiskGateway().evaluate(_intent(), context).evaluation

    assert evaluation.decision is RiskDecision.REJECTED
    assert evaluation.reason == "stale_market_state"


def test_gateway_rejects_account_not_started() -> None:
    evaluation = (
        RiskGateway()
        .evaluate(
            _intent(),
            _context(account_state=ExecutionAccountStatus.STARTING),
        )
        .evaluation
    )

    assert evaluation.decision is RiskDecision.REJECTED
    assert evaluation.reason == "account_not_ready"


def test_gateway_approves_when_account_state_is_running() -> None:
    evaluation = (
        RiskGateway()
        .evaluate(
            _intent(),
            _context(account_state=ExecutionAccountStatus.RUNNING),
        )
        .evaluation
    )

    assert evaluation.decision is RiskDecision.APPROVED


def test_gateway_approves_small_entry_when_all_limits_pass() -> None:
    evaluation = RiskGateway().evaluate(_intent(), _context()).evaluation

    assert evaluation.decision is RiskDecision.APPROVED
    assert evaluation.reason == "approved"


def test_gateway_rejects_entry_with_unbounded_capacity_limits() -> None:
    context = replace(
        _context(),
        open_position_symbols=frozenset({"ETHUSDT", "SOLUSDT"}),
        risk_config=replace(
            _risk_config(max_order_notional=None),
            max_gross_notional=None,
            max_open_positions=None,
        ),
    )

    evaluation = (
        RiskGateway()
        .evaluate(
            _intent(desired_notional=Decimal("100")),
            context,
        )
        .evaluation
    )

    assert evaluation.decision is RiskDecision.REJECTED
    assert evaluation.reason == "missing_max_order_notional_limit"


def test_gateway_allows_reduce_only_while_draining() -> None:
    evaluation = (
        RiskGateway()
        .evaluate(
            _intent(reduce_only=True),
            _context(strategy_state=StrategyLiveState.DRAINING),
        )
        .evaluation
    )

    assert evaluation.decision is RiskDecision.APPROVED
    assert evaluation.reason == "reduce_only_draining"


def test_gateway_does_not_cap_reduce_only_exit_notional() -> None:
    evaluation = (
        RiskGateway()
        .evaluate(
            _intent(reduce_only=True, desired_notional=Decimal("500")),
            _context(),
        )
        .evaluation
    )

    assert evaluation.decision is RiskDecision.APPROVED
    assert evaluation.reason == "reduce_only"


def test_gateway_allows_reduce_only_when_account_syncing() -> None:
    evaluation = (
        RiskGateway()
        .evaluate(
            _intent(reduce_only=True),
            _context(account_state=ExecutionAccountStatus.SYNCING),
        )
        .evaluation
    )

    assert evaluation.decision is RiskDecision.APPROVED
    assert evaluation.reason == "reduce_only"


def test_gateway_rejects_reduce_only_when_account_stopped() -> None:
    evaluation = (
        RiskGateway()
        .evaluate(
            _intent(reduce_only=True),
            _context(account_state=ExecutionAccountStatus.STOPPED),
        )
        .evaluation
    )

    assert evaluation.decision is RiskDecision.REJECTED
    assert evaluation.reason == "account_stopped"


def test_gateway_blocks_entries_when_strategy_is_halted() -> None:
    evaluation = (
        RiskGateway()
        .evaluate(
            _intent(),
            _context(strategy_state=StrategyLiveState.HALTED),
        )
        .evaluation
    )

    assert evaluation.decision is RiskDecision.HALTED
    assert evaluation.reason == "strategy_halted"


def _entry_limit_context() -> LiveLimitContext:
    return LiveLimitContext(
        symbol="BTCUSDT",
        requested_notional=Decimal("50"),
        open_position_symbols=frozenset(),
        realized_pnl=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        gross_exposure=Decimal("0"),
        min_notional=Decimal("5"),
        has_unresolved_order=False,
    )


def _fixed_limits() -> FixedLiveLimits:
    return FixedLiveLimits(
        notional_cap=Decimal("25"),
        max_open_positions=2,
        max_daily_loss=Decimal("10"),
        max_gross_exposure=Decimal("100"),
        max_concurrency_per_symbol=2,
    )


def test_gateway_applies_notional_cap_before_order_limit() -> None:
    context = replace(
        _context(),
        risk_config=_risk_config(max_order_notional=Decimal("30")),
    )
    result = RiskGateway(limits=_fixed_limits()).evaluate(
        _intent(),
        context,
        limit_context=_entry_limit_context(),
    )
    assert result.evaluation.decision is RiskDecision.APPROVED
    assert result.candidate is not None
    assert result.candidate.desired_notional == Decimal("25")
    assert result.evaluation.details["desired_notional"] == "25"


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"gross_exposure": None}, "missing_gross_exposure"),
        ({"gross_exposure": Decimal("100")}, "max_gross_exposure_reached"),
        ({"realized_pnl": Decimal("-10")}, "max_daily_loss_reached"),
        ({"has_unresolved_order": True}, "unresolved_order_uncertainty"),
        ({"symbol_concurrency": 2}, "max_concurrency_per_symbol_exceeded"),
    ],
)
def test_gateway_rejects_fixed_limit_failures(change, reason) -> None:
    result = RiskGateway(limits=_fixed_limits()).evaluate(
        _intent(),
        _context(),
        limit_context=replace(_entry_limit_context(), **change),
    )
    assert result.candidate is None
    assert result.evaluation.decision is RiskDecision.REJECTED
    assert result.evaluation.reason == reason


def test_gateway_preserves_authority_rejection_after_limit_approval() -> None:
    result = RiskGateway(limits=_fixed_limits()).evaluate(
        _intent(),
        _context(active_lease=None),
        limit_context=_entry_limit_context(),
    )
    assert result.candidate is not None
    assert result.evaluation.reason == "missing_active_lease"


def test_gateway_reduce_only_bypasses_entry_limits() -> None:
    intent = _intent(reduce_only=True, desired_notional=Decimal("500"))
    result = RiskGateway(limits=_fixed_limits()).evaluate(intent, _context())
    assert result.candidate is intent
    assert result.evaluation.decision is RiskDecision.APPROVED


def test_gateway_requires_limit_facts_for_configured_entry() -> None:
    with pytest.raises(ValueError, match="entry limit context is required"):
        RiskGateway(limits=_fixed_limits()).evaluate(_intent(), _context())


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
        code_generation="test-generation",
        state=TradingLeaseState.ACTIVE,
        acquired_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
        expires_at=datetime(2026, 7, 4, 0, 5, tzinfo=UTC),
    )


def _intent(
    reduce_only: bool = False,
    desired_notional: Decimal = Decimal("50"),
) -> OrderIntentCandidate:
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
        desired_notional=desired_notional,
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
