from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.account import ExecutionAccountStatus
from crypto_momentum_lab.domain.execution import ExchangeOrderState
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.risk import (
    RiskConfigSnapshot,
    RiskEvaluation,
    StrategyLiveState,
    TradingLease,
    TradingLeaseState,
)
from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    StrategyCheckpoint,
    StrategyDecision,
    StrategySide,
    StrategySignal,
)
from crypto_momentum_lab.execution_account.orders.quantization import (
    SymbolTradingRules,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionStateMachine,
    SubmitPolicy,
)
from crypto_momentum_lab.risk.gateway import RiskGateway
from crypto_momentum_lab.shadow_operation.models import (
    ShadowDecisionMetric,
    ShadowOrderPlan,
    ShadowSession,
)
from crypto_momentum_lab.shadow_operation.service import (
    ShadowOperationConfig,
    ShadowOperationContext,
    ShadowOperationService,
)
from tests.unit.execution_account.orders.test_state_machine import (
    FakeExchange,
    FakeOrderRepository,
    _snapshot,
)

NOW = datetime(2026, 7, 4, 0, 0, 20, tzinfo=UTC)


async def test_shadow_service_requires_active_strategy_lease() -> None:
    service, shadow_repository, _ = _service()

    result = await service.run((_state(),), _context(active_lease=None))

    assert result.halt_reason == "missing_active_lease"
    assert shadow_repository.sessions[0].state == "halted"


async def test_shadow_service_requires_ready_account_sync() -> None:
    service, _, _ = _service()

    result = await service.run(
        (_state(),),
        _context(account_state=ExecutionAccountStatus.DEGRADED),
    )

    assert result.halt_reason == "account_not_ready"
    assert result.processed_state_count == 0


async def test_shadow_service_persists_suppression_for_approved_intent() -> None:
    service, shadow_repository, order_state_repository = _service()

    result = await service.run((_state(),), _context())

    assert result.approved_intent_count == 1
    assert result.suppression_count == 1
    assert len(shadow_repository.plans) == 1
    assert len(order_state_repository.suppressions) == 1


async def test_shadow_service_halts_on_market_staleness() -> None:
    service, shadow_repository, _ = _service()
    stale_now = NOW + timedelta(minutes=2)

    result = await service.run((_state(),), _context(now=stale_now))

    assert result.halt_reason == "stale_market_state"
    assert shadow_repository.metrics[-1].category == "stale_data_block"


def _service() -> tuple[
    ShadowOperationService,
    "FakeShadowRepository",
    FakeOrderRepository,
]:
    shadow_repository = FakeShadowRepository()
    approved_repository = FakeApprovedIntentRepository()
    order_state_repository = FakeOrderRepository()
    exchange = FakeExchange(
        submit_result=_snapshot(ExchangeOrderState.ACKNOWLEDGED)
    )
    machine = OrderExecutionStateMachine(
        exchange=exchange,
        repository=order_state_repository,
        submit_policy=SubmitPolicy.SHADOW_SUPPRESS,
        live_submit_enabled=False,
        clock=lambda: NOW,
    )
    return (
        ShadowOperationService(
            strategy=FakeStrategy(),
            risk_gateway=RiskGateway(),
            shadow_repository=shadow_repository,
            approved_intent_repository=approved_repository,
            state_machine=machine,
            config=ShadowOperationConfig(
                run_id="shadow-1",
                account_label="primary",
                strategy_name="compression_breakout",
                strategy_config_hash="a" * 64,
                lease_owner="shadow-worker",
                max_market_state_age_seconds=30,
                resize_tolerance=Decimal("0.20"),
            ),
        ),
        shadow_repository,
        order_state_repository,
    )


class FakeStrategy:
    def on_market_state(self, state: MarketState15s) -> StrategyDecision:
        return StrategyDecision(
            signals=(_signal(),),
            candidates=(_intent(),),
            rejections=(),
            checkpoint=StrategyCheckpoint({}, {}, {}, {}),
        )


class FakeApprovedIntentRepository:
    async def save_approved_intent(
        self,
        intent: OrderIntentCandidate,
        evaluation: RiskEvaluation,
    ) -> None:
        pass


class FakeShadowRepository:
    def __init__(self) -> None:
        self.sessions: list[ShadowSession] = []
        self.plans: list[ShadowOrderPlan] = []
        self.metrics: list[ShadowDecisionMetric] = []

    async def start_session(self, session_record: ShadowSession) -> None:
        self.sessions.append(session_record)

    async def end_session(
        self,
        run_id: str,
        *,
        state: str,
        ended_at: datetime,
    ) -> None:
        pass

    async def save_order_plan(self, plan: ShadowOrderPlan) -> None:
        self.plans.append(plan)

    async def save_metric(self, metric: ShadowDecisionMetric) -> None:
        self.metrics.append(metric)


def _context(
    *,
    active_lease: TradingLease | None | object = "default",
    account_state: ExecutionAccountStatus = ExecutionAccountStatus.READY_READONLY,
    now: datetime = NOW,
) -> ShadowOperationContext:
    lease = _lease() if active_lease == "default" else active_lease
    return ShadowOperationContext(
        now=now,
        active_lease=lease,
        account_state=account_state,
        open_position_symbols=frozenset(),
        active_halts=(),
        risk_config=RiskConfigSnapshot(
            environment="live",
            account_label="primary",
            max_order_notional=Decimal("100"),
            max_gross_notional=Decimal("500"),
            max_daily_loss=Decimal("25"),
            max_open_positions=1,
            max_market_state_age_seconds=30,
            max_account_state_age_seconds=30,
            allow_reduce_only_while_draining=True,
            created_at=NOW,
        ),
        strategy_state=StrategyLiveState.ACTIVE,
        trading_rules={
            "BTCUSDT": SymbolTradingRules(
                symbol="BTCUSDT",
                tick_size=Decimal("0.1"),
                step_size=Decimal("0.001"),
                min_quantity=Decimal("0.001"),
                max_quantity=Decimal("100"),
                min_notional=Decimal("5"),
            )
        },
    )


def _lease() -> TradingLease:
    return TradingLease(
        lease_id="lease-1",
        environment="live",
        account_label="primary",
        strategy_name="compression_breakout",
        owner="shadow-worker",
        state=TradingLeaseState.ACTIVE,
        acquired_at=NOW - timedelta(minutes=1),
        expires_at=NOW + timedelta(minutes=5),
    )


def _intent() -> OrderIntentCandidate:
    return OrderIntentCandidate(
        candidate_id="candidate-1",
        signal_id="signal-1",
        run_id="run-1",
        strategy_name="compression_breakout",
        strategy_version="v1",
        config_hash="a" * 64,
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        entry_type=EntryType.MARKET,
        limit_price=None,
        desired_notional=Decimal("100"),
        reduce_only=False,
        expires_at=NOW + timedelta(seconds=30),
        created_at=NOW,
        reason="test",
        features={},
    )


def _signal() -> StrategySignal:
    return StrategySignal(
        signal_id="signal-1",
        run_id="run-1",
        strategy_name="compression_breakout",
        strategy_version="v1",
        config_hash="a" * 64,
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        detected_at=NOW,
        source_state_at=NOW,
        reason="test",
        features={},
        reference_prices={},
    )


def _state() -> MarketState15s:
    start = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)
    return MarketState15s(
        schema_version=1,
        exchange="binance-usdm",
        environment="live",
        symbol="BTCUSDT",
        bucket_start=start,
        bucket_end=start + timedelta(seconds=15),
        open_price=Decimal("30000"),
        high_price=Decimal("30000"),
        low_price=Decimal("30000"),
        close_price=Decimal("30000"),
        trade_count=1,
        trade_notional=Decimal("100"),
        aggressive_buy_notional=Decimal("60"),
        aggressive_sell_notional=Decimal("40"),
        last_bid_price=Decimal("29999"),
        last_ask_price=Decimal("30001"),
        spread=Decimal("2"),
        midpoint=Decimal("30000"),
        liquidation_count=0,
        liquidation_notional=Decimal("0"),
        mark_price=Decimal("30000"),
        closed_kline_count=0,
        source_event_count=1,
        first_received_at=start,
        last_received_at=start + timedelta(seconds=15),
    )
