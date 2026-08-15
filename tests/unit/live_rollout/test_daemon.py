from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

from crypto_momentum_lab.domain.execution import (
    ExchangeOrderSnapshot,
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.risk import RiskEvaluation
from crypto_momentum_lab.domain.strategy import (
    OrderIntentCandidate,
    StrategyCheckpoint,
    StrategyDecision,
)
from crypto_momentum_lab.execution_account.orders.quantization import (
    SymbolTradingRules,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionStateMachine,
    SubmitPolicy,
)
from crypto_momentum_lab.live_rollout.daemon import (
    LiveDaemonConfig,
    LiveDaemonRuntimeContext,
    LiveStrategyDaemon,
)
from crypto_momentum_lab.live_rollout.exits import (
    LiveExitConfig,
    LiveExitManager,
    ManagedLivePosition,
)
from crypto_momentum_lab.live_rollout.limits import FixedLiveLimits
from crypto_momentum_lab.risk.gateway import RiskGateway
from crypto_momentum_lab.strategy_runner.position_exit import PositionExitPolicy
from tests.unit.execution_account.orders.test_state_machine import (
    FakeExchange,
    FakeOrderRepository,
    _snapshot,
)
from tests.unit.live_rollout.test_gates import _context as gate_context
from tests.unit.shadow_operation.test_service import (
    FakeStrategy,
    _state,
)
from tests.unit.shadow_operation.test_service import (
    _context as shadow_context,
)

NOW = datetime(2026, 7, 4, 0, 0, 20, tzinfo=UTC)


async def test_live_daemon_submits_strategy_candidate_after_all_gates() -> None:
    exchange = PlanAwareExchange()
    daemon = _daemon(exchange=exchange)

    result = await daemon.run(_states())

    assert result.processed_state_count == 1
    assert result.approved_intent_count == 1
    assert result.submitted_order_count == 1
    assert result.halt_reason is None
    assert exchange.calls == ["submit"]


async def test_live_daemon_allows_entry_when_same_symbol_is_already_open() -> None:
    exchange = PlanAwareExchange()

    async def position_context(state: object) -> LiveDaemonRuntimeContext:
        del state
        return replace(
            _runtime_context(),
            open_position_symbols=frozenset({"BTCUSDT"}),
        )

    daemon = _daemon(
        exchange=exchange,
        context_provider=position_context,
    )

    result = await daemon.run(_states())

    assert result.approved_intent_count == 1
    assert result.submitted_order_count == 1
    assert result.halt_reason is None
    assert exchange.calls == ["submit"]


async def test_live_daemon_allows_entry_with_confirmed_resting_order() -> None:
    exchange = PlanAwareExchange()

    async def context_with_resting_order(
        state: object,
    ) -> LiveDaemonRuntimeContext:
        del state
        resting_order = ExchangeOrderState.ACKNOWLEDGED
        return replace(
            _runtime_context(),
            gate_context=replace(
                gate_context(),
                unresolved_order_states=(resting_order,),
            ),
            unresolved_order_states=(resting_order,),
        )

    daemon = _daemon(
        exchange=exchange,
        context_provider=context_with_resting_order,
    )

    result = await daemon.run(_states())

    assert result.approved_intent_count == 1
    assert result.submitted_order_count == 1
    assert result.halt_reason is None
    assert exchange.calls == ["submit"]


async def test_live_daemon_blocks_before_submit_when_gate_changes() -> None:
    exchange = FakeExchange(
        submit_result=_snapshot(ExchangeOrderState.ACKNOWLEDGED)
    )

    async def blocked_context(state: object) -> LiveDaemonRuntimeContext:
        del state
        return replace(_runtime_context(), active_halts=(_halt(),))

    daemon = _daemon(exchange=exchange, context_provider=blocked_context)

    result = await daemon.run(_states())

    assert result.halt_reason is not None
    assert result.halt_reason.startswith("live_gate:")
    assert exchange.calls == []


async def test_live_daemon_processes_delayed_startup_state_without_age_gate() -> None:
    exchange = PlanAwareExchange()
    stale = replace(
        _state(),
        bucket_start=NOW - timedelta(minutes=2),
        bucket_end=NOW - timedelta(minutes=2) + timedelta(seconds=15),
    )

    async def states() -> AsyncIterator:
        yield stale
        yield _state()

    daemon = _daemon(
        exchange=exchange,
    )

    result = await daemon.run(states())

    assert result.processed_state_count == 2
    assert result.approved_intent_count == 2
    assert result.halt_reason is None
    assert exchange.calls == ["submit", "submit"]


async def test_live_daemon_resets_strategy_after_market_state_gap() -> None:
    exchange = PlanAwareExchange()
    strategy = GapAwareFakeStrategy()

    async def states() -> AsyncIterator:
        first = _state()
        yield first
        yield replace(
            first,
            bucket_start=first.bucket_start + timedelta(minutes=5),
            bucket_end=first.bucket_end + timedelta(minutes=5),
        )

    daemon = _daemon(exchange=exchange, strategy=strategy)

    result = await daemon.run(states())

    assert result.halt_reason is None
    assert strategy.reset_symbols == ["BTCUSDT"]
    assert strategy.reset_counts_at_decision == [0, 1]


async def test_live_daemon_saves_final_checkpoint_before_normal_exit() -> None:
    exchange = PlanAwareExchange()
    repository = FakeLiveRepository()
    daemon = _daemon(
        exchange=exchange,
        repository=repository,
        checkpoint_every_states=100,
    )

    await daemon.run(_states())

    assert repository.saved_checkpoint_run_ids == ["run-1"]


async def test_live_daemon_submits_hedge_mode_reduce_only_exit() -> None:
    exchange = PlanAwareExchange()
    position = ManagedLivePosition(
        symbol="BTCUSDT",
        side="long",
        position_side=FuturesPositionSide.LONG,
        quantity=Decimal("0.001"),
        entry_price=Decimal("31000"),
        opened_at=datetime(2026, 7, 3, 23, 59, tzinfo=UTC),
    )

    async def position_context(state: object) -> LiveDaemonRuntimeContext:
        del state
        return replace(
            _runtime_context(),
            open_position_symbols=frozenset({"BTCUSDT"}),
            managed_positions=(position,),
        )

    daemon = _daemon(
        exchange=exchange,
        context_provider=position_context,
        exit_manager=LiveExitManager(
            config=LiveExitConfig(
                run_id="run-1",
                strategy_name="compression_breakout",
                strategy_version="v0",
                strategy_config_hash="a" * 64,
                policy=PositionExitPolicy(),
            )
        ),
        hedge_mode=True,
    )

    result = await daemon.run(_states())

    assert result.approved_intent_count == 2
    assert len(exchange.plans) == 2
    assert exchange.plans[0].reduce_only is True
    assert exchange.plans[0].side == "SELL"
    assert exchange.plans[0].position_side is FuturesPositionSide.LONG
    assert exchange.plans[1].reduce_only is False


async def test_live_daemon_halts_on_unmanaged_account_position() -> None:
    exchange = PlanAwareExchange()

    async def position_context(state: object) -> LiveDaemonRuntimeContext:
        del state
        return replace(
            _runtime_context(),
            open_position_symbols=frozenset({"ETHUSDT"}),
            unmanaged_position_symbols=frozenset({"ETHUSDT"}),
        )

    daemon = _daemon(exchange=exchange, context_provider=position_context)

    result = await daemon.run(_states())

    assert result.halt_reason == "unmanaged_live_positions:ETHUSDT"
    assert exchange.calls == []


class FakeLiveRepository:
    def __init__(self) -> None:
        self.saved_checkpoint_run_ids: list[str] = []

    async def save_approved_intent(
        self,
        intent: OrderIntentCandidate,
        evaluation: RiskEvaluation,
    ) -> None:
        pass

    async def save_checkpoint(
        self,
        run_id: str,
        checkpoint: StrategyCheckpoint,
        saved_at: datetime,
    ) -> None:
        self.saved_checkpoint_run_ids.append(run_id)


def _daemon(
    *,
    exchange,
    strategy=None,
    context_provider=None,
    repository: FakeLiveRepository | None = None,
    checkpoint_every_states: int = 1,
    exit_manager: LiveExitManager | None = None,
    hedge_mode: bool = False,
) -> LiveStrategyDaemon:
    order_repository = FakeOrderRepository()
    machine = OrderExecutionStateMachine(
        exchange=exchange,
        repository=order_repository,
        submit_policy=SubmitPolicy.LIVE_SUBMIT,
        live_submit_enabled=True,
        clock=lambda: NOW,
    )

    async def default_context(state: object) -> LiveDaemonRuntimeContext:
        del state
        return _runtime_context()

    return LiveStrategyDaemon(
        strategy=strategy or FakeStrategy(),
        risk_gateway=RiskGateway(),
        limits=FixedLiveLimits(
            notional_cap=Decimal("25"),
            max_open_positions=1,
            max_daily_loss=Decimal("10"),
            max_gross_exposure=Decimal("25"),
        ),
        repository=repository or FakeLiveRepository(),
        state_machine=machine,
        context_provider=context_provider or default_context,
        config=LiveDaemonConfig(
            run_id="run-1",
            resize_tolerance=Decimal("0.20"),
            checkpoint_every_states=checkpoint_every_states,
            hedge_mode=hedge_mode,
        ),
        exit_manager=exit_manager,
    )


class PlanAwareExchange:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.plans: list[OrderExecutionPlan] = []

    async def submit_order(self, plan: OrderExecutionPlan) -> ExchangeOrderSnapshot:
        self.calls.append("submit")
        self.plans.append(plan)
        return ExchangeOrderSnapshot(
            client_order_id=plan.client_order_id,
            exchange_order_id="exchange-1",
            state=ExchangeOrderState.ACKNOWLEDGED,
            observed_at=NOW,
            executed_quantity=Decimal("0"),
            average_price=Decimal("0"),
        )

    async def query_order_by_client_id(
        self,
        symbol: str,
        client_order_id: str,
    ) -> ExchangeOrderSnapshot | None:
        self.calls.append("query")
        return None


class GapAwareFakeStrategy(FakeStrategy):
    def __init__(self) -> None:
        self.reset_symbols: list[str] = []
        self.reset_counts_at_decision: list[int] = []

    def required_data(self):
        return SimpleNamespace(max_gap_seconds=30)

    def reset_symbol(self, symbol: str) -> None:
        self.reset_symbols.append(symbol)

    def on_market_state(self, state: MarketState15s) -> StrategyDecision:
        self.reset_counts_at_decision.append(len(self.reset_symbols))
        return super().on_market_state(state)


def _runtime_context() -> LiveDaemonRuntimeContext:
    shadow = shadow_context(now=NOW)
    gate = gate_context()
    return LiveDaemonRuntimeContext(
        now=NOW,
        gate_context=gate,
        active_lease=gate.active_lease,
        account_state=gate.account_state,
        account_observed_at=NOW,
        open_position_symbols=frozenset(),
        realized_pnl=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        gross_exposure=Decimal("0"),
        active_halts=(),
        unresolved_order_states=(),
        risk_config=gate.risk_config,
        strategy_state=shadow.strategy_state,
        trading_rules={
            "BTCUSDT": SymbolTradingRules(
                symbol="BTCUSDT",
                tick_size=Decimal("0.1"),
                step_size=Decimal("0.0001"),
                min_quantity=Decimal("0.0001"),
                max_quantity=Decimal("100"),
                min_notional=Decimal("5"),
            )
        },
    )


def _halt():
    from crypto_momentum_lab.domain.risk import RiskHalt

    return RiskHalt(
        halt_id="halt-1",
        environment="live",
        account_label="primary",
        reason="operator_stop",
        active=True,
        created_at=NOW,
        details={},
    )


async def _states() -> AsyncIterator:
    yield _state()
