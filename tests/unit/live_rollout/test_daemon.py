import asyncio
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from sqlalchemy.exc import OperationalError

from crypto_momentum_lab.domain.account import (
    AccountPositionSnapshot,
    ExecutionAccountStatus,
)
from crypto_momentum_lab.domain.execution.exchange_contract import (
    ExchangeCancellationUnknownError,
    ExchangeOrderAlreadyAbsentError,
    ExchangeSubmissionTimeoutError,
)
from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.order_read_models import (
    PersistedExchangeOrder,
)
from crypto_momentum_lab.domain.execution.order_rules import SymbolTradingRules
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderSnapshot,
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.order_submission import (
    PreparedOrderSubmission,
)
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.risk import RiskEvaluation
from crypto_momentum_lab.domain.risk.limits import FixedLiveLimits
from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    StrategyCheckpoint,
    StrategyDecision,
    StrategySide,
    universe_snapshot_for_symbols,
)
from crypto_momentum_lab.domain.market.closed_candle import ClosedCandle15m
from crypto_momentum_lab.domain.strategy.position_exit import (
    PositionExitMode,
    PositionExitPolicy,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionStateMachine,
)
from crypto_momentum_lab.live_rollout.closed_candle_feed import (
    ClosedCandle15mEvent,
)
from crypto_momentum_lab.live_rollout.context import LiveEntryFilterContext
from crypto_momentum_lab.live_rollout.daemon import (
    LiveDaemonConfig,
    LiveDaemonRuntimeContext,
    LiveStrategyDaemon,
)
from crypto_momentum_lab.live_rollout.exits import (
    LiveExitCancellationRequest,
    LiveExitConfig,
    LiveExitManager,
    LiveExitOrderRequest,
    ManagedLivePosition,
)
from crypto_momentum_lab.live_rollout.scheduled_risk_window import (
    ScheduledRiskWindowConfig,
)
from crypto_momentum_lab.domain.risk import RiskGateway
from tests.fixtures.live_context_reader import context_reader
from tests.fixtures.live_market import (
    FakeStrategy,
    _intent,
    _state,
)
from tests.fixtures.live_market import (
    _context as shadow_context,
)
from tests.unit.execution_account.orders.test_coordinator import (
    OrderExecutionCoordinator,
)
from tests.unit.execution_account.orders.test_state_machine import (
    FakeExchange,
    FakeOrderRepository,
    _snapshot,
)
from tests.unit.live_rollout.test_gates import _context as gate_context
from tests.unit.live_rollout.test_gates import _risk_config as gate_risk_config

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


async def test_live_daemon_bypasses_backfill_states_without_submitting_orders() -> None:
    from dataclasses import replace

    exchange = PlanAwareExchange()
    daemon = _daemon(exchange=exchange)

    async def _backfill_states():
        yield replace(_state(), is_backfill=True)

    result = await daemon.run(_backfill_states())

    assert result.processed_state_count == 1
    assert result.submitted_order_count == 0
    assert result.approved_intent_count == 0
    assert exchange.calls == []


async def test_live_daemon_uses_lower_of_ask_and_close_gtd_limit_for_entries() -> None:
    exchange = PlanAwareExchange()
    daemon = _daemon(exchange=exchange, entry_order_type=EntryType.LIMIT)

    result = await daemon.run(_states())

    assert result.halt_reason is None
    assert result.submitted_order_count == 1
    assert len(exchange.plans) == 1
    plan = exchange.plans[0]
    assert plan.order_type == "LIMIT"
    assert plan.price == Decimal("30000")
    assert plan.time_in_force == "GTD"
    assert plan.expires_at == NOW + timedelta(minutes=15)


async def test_live_daemon_caps_entry_limit_at_lower_ask_when_ask_is_below_close() -> (
    None
):
    exchange = PlanAwareExchange()
    state = replace(
        _state(),
        close_price=Decimal("30000"),
        last_ask_price=Decimal("29950"),
        midpoint=Decimal("29949.5"),
    )

    async def states() -> AsyncIterator[MarketState15s]:
        yield state

    daemon = _daemon(exchange=exchange, entry_order_type=EntryType.LIMIT)

    result = await daemon.run(states())

    assert result.halt_reason is None
    assert result.submitted_order_count == 1
    assert exchange.plans[0].order_type == "LIMIT"
    assert exchange.plans[0].price == Decimal("29950")
    assert exchange.plans[0].expires_at == NOW + timedelta(minutes=15)


async def test_live_signal_record_includes_universe_and_effective_entry_context() -> (
    None
):
    recorder = RecordingSignalRecorder()

    def universe_context(
        symbol: str,
        observed_at: datetime,
    ) -> dict[str, object]:
        return {
            "symbol": symbol,
            "snapshot_observed_at": observed_at - timedelta(minutes=1),
            "utc_day_return": Decimal("0.123"),
            "gainer_rank": 7,
            "in_entry_pool": True,
        }

    daemon = _daemon(
        exchange=PlanAwareExchange(),
        signal_recorder=recorder,
        entry_universe_context_provider=universe_context,
    )

    result = await daemon.run(_states())

    assert result.halt_reason is None
    assert recorder.decision_filter_context is not None
    universe = recorder.decision_filter_context["universe"]
    assert universe["gainer_rank"] == 7
    effective = recorder.decision_filter_context["effective_entry_candidates"][
        "candidate-1"
    ]
    assert effective["original_entry_type"] == "market"
    assert effective["effective_entry_type"] == "limit"
    assert effective["effective_limit_price"] == Decimal("30000")
    assert (
        effective["effective_limit_price_source"]
        == "min(state.last_ask_price,state.close_price)"
    )
    assert effective["effective_expires_at"] == NOW + timedelta(minutes=15)


async def test_live_policy_enforce_uses_policy_eligible_candidate_for_submission() -> (
    None
):
    recorder = RecordingSignalRecorder()
    exchange = PlanAwareExchange()
    daemon = _daemon(
        exchange=exchange,
        signal_recorder=recorder,
    )

    result = await daemon.run(_states())

    assert result.halt_reason is None
    assert result.submitted_order_count == 1
    assert exchange.calls == ["submit"]
    assert recorder.decision_filter_context is not None
    assert recorder.decision_filter_context["entry_policy_mode"] == "enforce"


async def test_live_policy_enforce_blocks_policy_ineligible_candidate() -> None:
    recorder = RecordingSignalRecorder()
    exchange = PlanAwareExchange()

    async def load_symbols(observed_at: datetime) -> frozenset[str]:
        del observed_at
        return frozenset({"BTCUSDT"})

    def empty_universe(observed_at: datetime):
        return universe_snapshot_for_symbols(
            frozenset(),
            observed_at=observed_at,
        )

    daemon = _daemon(
        exchange=exchange,
        signal_recorder=recorder,
        entry_symbol_loader=load_symbols,
        entry_universe_snapshot_provider=empty_universe,
    )

    result = await daemon.run(_states())

    assert result.halt_reason is None
    assert result.submitted_order_count == 0
    assert exchange.calls == []
    assert recorder.decision_filter_context is not None
    decisions = recorder.decision_filter_context["entry_policy_decisions"]
    assert len(decisions) == 1
    assert decisions[0]["policy_eligible"] is False
    assert decisions[0]["policy_rejection_reasons"] == ["outside_entry_universe"]


async def test_live_policy_enforce_fails_closed_on_universe_snapshot_error() -> None:
    recorder = RecordingSignalRecorder()
    exchange = PlanAwareExchange()

    async def load_symbols(observed_at: datetime) -> frozenset[str]:
        del observed_at
        return frozenset({"BTCUSDT"})

    def broken_universe(observed_at: datetime):
        del observed_at
        raise RuntimeError("universe unavailable")

    daemon = _daemon(
        exchange=exchange,
        signal_recorder=recorder,
        entry_symbol_loader=load_symbols,
        entry_universe_snapshot_provider=broken_universe,
    )

    result = await daemon.run(_states())

    assert result.halt_reason is None
    assert result.submitted_order_count == 0
    assert exchange.calls == []
    assert recorder.decision_filter_context is not None
    assert (
        recorder.decision_filter_context["entry_policy_universe_snapshot_error"]
        == "RuntimeError"
    )
    assert (
        recorder.decision_filter_context["entry_policy_skip_reason"]
        == "universe_snapshot_error"
    )


@pytest.mark.parametrize("reduce_only", [False, True])
async def test_live_daemon_does_not_submit_expired_entry_candidate(reduce_only) -> None:
    exchange = PlanAwareExchange()

    class ExpiredCandidateStrategy(FakeStrategy):
        def on_market_state(self, state: MarketState15s) -> StrategyDecision:
            decision = super().on_market_state(state)
            return replace(
                decision,
                candidates=(
                    replace(
                        decision.candidates[0],
                        expires_at=NOW + timedelta(seconds=1),
                        reduce_only=reduce_only,
                    ),
                ),
            )

    daemon = _daemon(
        exchange=exchange,
        strategy=ExpiredCandidateStrategy(),
        clock=lambda: NOW + timedelta(seconds=2),
    )

    result = await daemon.run(_states())

    assert result.halt_reason is None
    assert result.approved_intent_count == 0
    assert result.submitted_order_count == 0
    assert exchange.calls == []


async def test_duplicate_prepared_intent_never_reaches_exchange() -> None:
    class AlreadySubmittedRepository(FakeLiveRepository):
        async def prepare_submission_in_session(self, session, **kwargs):
            return None

    exchange = PlanAwareExchange()
    daemon = _daemon(exchange=exchange, repository=AlreadySubmittedRepository())
    result = await daemon.run(_states())
    assert result.halt_reason is None
    assert exchange.calls == []


def test_live_daemon_tracks_entry_lane_reason_even_when_state_is_unchanged() -> None:
    daemon = _daemon(exchange=PlanAwareExchange())

    daemon.set_entry_enabled(True, reason="prerequisites_ready")
    assert daemon.entry_enabled_reason == "prerequisites_ready"

    daemon.set_entry_enabled(False, reason="market_state_consumer_lagged")
    assert daemon.entry_enabled is False
    assert daemon.entry_enabled_reason == "market_state_consumer_lagged"


async def test_live_daemon_prefetches_next_context_while_exchange_is_busy() -> None:
    exchange = BlockingPlanAwareExchange()
    second_context_started = asyncio.Event()
    context_calls = 0

    async def context_provider(state: object) -> LiveDaemonRuntimeContext:
        nonlocal context_calls
        del state
        context_calls += 1
        if context_calls == 2:
            second_context_started.set()
        return _runtime_context()

    async def states() -> AsyncIterator:
        first = _state()
        yield first
        yield replace(
            first,
            bucket_start=first.bucket_start + timedelta(seconds=15),
            bucket_end=first.bucket_end + timedelta(seconds=15),
        )

    daemon = _daemon(
        exchange=exchange,
        context_provider=context_provider,
    )
    run = asyncio.create_task(daemon.run(states()))
    await exchange.submit_started.wait()
    await asyncio.wait_for(second_context_started.wait(), timeout=0.03)

    exchange.release_submit.set()
    await run

    assert context_calls >= 2


async def test_live_daemon_starts_later_context_while_first_context_waits() -> None:
    first_context_started = asyncio.Event()
    release_first_context = asyncio.Event()
    second_context_started = asyncio.Event()
    context_calls = 0

    async def context_provider(state: object) -> LiveDaemonRuntimeContext:
        nonlocal context_calls
        del state
        context_calls += 1
        if context_calls == 1:
            first_context_started.set()
            await release_first_context.wait()
        elif context_calls == 2:
            second_context_started.set()
        return _runtime_context()

    async def states() -> AsyncIterator:
        first = _state()
        yield first
        yield replace(
            first,
            bucket_start=first.bucket_start + timedelta(seconds=15),
            bucket_end=first.bucket_end + timedelta(seconds=15),
        )

    daemon = _daemon(
        exchange=PlanAwareExchange(),
        context_provider=context_provider,
    )
    run = asyncio.create_task(daemon.run(states()))
    await first_context_started.wait()
    await asyncio.wait_for(second_context_started.wait(), timeout=0.03)

    release_first_context.set()
    await run


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


async def test_live_daemon_allows_multiple_same_symbol_limit_entries() -> None:
    exchange = PlanAwareExchange()

    daemon = _daemon(
        exchange=exchange,
        strategy=TwoCandidateStrategy(),
        max_gross_exposure=Decimal("100"),
    )

    result = await daemon.run(_states())

    assert result.approved_intent_count == 2
    assert result.submitted_order_count == 2
    assert exchange.calls == ["submit", "submit"]


async def test_live_daemon_reserves_unfilled_limit_entry_notional() -> None:
    exchange = PlanAwareExchange()

    daemon = _daemon(
        exchange=exchange,
        strategy=TwoCandidateStrategy(),
    )

    result = await daemon.run(_states())

    assert result.approved_intent_count == 1
    assert result.submitted_order_count == 1
    assert exchange.calls == ["submit"]


async def test_terminal_entry_event_releases_in_memory_reservation() -> None:
    exchange = PlanAwareExchange()
    daemon = _daemon(exchange=exchange)

    await daemon.run(_states())
    plan = exchange.plans[0]
    assert daemon._pending_entries.snapshot()

    daemon.pending_entries.observe_order_event(
        plan,
        ExchangeOrderEvent(
            event_id="filled-event",
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.FILLED,
            occurred_at=NOW,
            exchange_order_id="exchange-1",
            details={"executed_quantity": str(plan.quantity)},
        ),
    )

    assert daemon._pending_entries.snapshot() == ()


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


async def test_live_daemon_filters_new_entries_by_pool_and_closed_ema() -> None:
    exchange = PlanAwareExchange()
    symbol_loader_calls: list[datetime] = []
    context_loader_calls: list[str] = []

    async def load_symbols(observed_at: datetime) -> frozenset[str]:
        symbol_loader_calls.append(observed_at)
        return frozenset()

    async def load_context(state: MarketState15s) -> LiveEntryFilterContext:
        context_loader_calls.append(state.symbol)
        return LiveEntryFilterContext(
            entry_price=Decimal("30001"),
            ema5=Decimal("30000"),
            ema10=Decimal("30000"),
        )

    daemon = _daemon(
        exchange=exchange,
        entry_symbol_loader=load_symbols,
        entry_filter_context_loader=load_context,
        require_price_above_ema5=True,
        require_price_above_ema10=True,
    )

    result = await daemon.run(_states())

    assert result.halt_reason is None
    assert result.approved_intent_count == 0
    assert result.submitted_order_count == 0
    assert len(symbol_loader_calls) == 1
    assert context_loader_calls == ["BTCUSDT"]
    assert exchange.calls == []


@pytest.mark.parametrize(("entry_price", "submitted"), [("30001", 1), ("30000", 0)])
async def test_live_daemon_accepts_entry_inside_top100_and_above_both_emas(
    entry_price, submitted
) -> None:
    exchange = PlanAwareExchange()

    async def load_symbols(_observed_at: datetime) -> frozenset[str]:
        return frozenset({"BTCUSDT"})

    async def load_context(_state: MarketState15s) -> LiveEntryFilterContext:
        return LiveEntryFilterContext(
            entry_price=Decimal(entry_price),
            ema5=Decimal("30000"),
            ema10=Decimal("29999"),
            ema_observed_at=NOW,
        )

    daemon = _daemon(
        exchange=exchange,
        entry_symbol_loader=load_symbols,
        entry_filter_context_loader=load_context,
        require_price_above_ema5=True,
        require_price_above_ema10=True,
    )

    result = await daemon.run(_states())

    assert result.halt_reason is None
    assert result.approved_intent_count == submitted
    assert result.submitted_order_count == submitted
    assert exchange.calls == (["submit"] if submitted else [])


async def test_live_daemon_continues_running_when_candidate_risk_rejects() -> None:
    exchange = FakeExchange(submit_result=_snapshot(ExchangeOrderState.ACKNOWLEDGED))

    async def blocked_context(state: object) -> LiveDaemonRuntimeContext:
        del state
        return replace(_runtime_context(), active_halts=(_halt(),))

    daemon = _daemon(exchange=exchange, context_provider=blocked_context)

    result = await daemon.run(_states())

    assert result.halt_reason is None
    assert result.submitted_order_count == 0
    assert exchange.calls == []


async def test_live_daemon_keeps_running_while_reconciliation_is_pending() -> None:
    exchange = PlanAwareExchange()
    calls = 0

    async def flaky_context(state: object) -> LiveDaemonRuntimeContext:
        nonlocal calls
        del state
        calls += 1
        if calls == 1:
            uncertain = ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
            return replace(
                _runtime_context(),
                gate_context=replace(
                    gate_context(),
                ),
                unresolved_order_states=(uncertain,),
            )
        return _runtime_context()

    async def states() -> AsyncIterator:
        first = _state()
        yield first
        yield replace(
            first,
            bucket_start=first.bucket_start + timedelta(seconds=15),
            bucket_end=first.bucket_end + timedelta(seconds=15),
        )

    daemon = _daemon(exchange=exchange, context_provider=flaky_context)

    result = await daemon.run(states())

    assert result.halt_reason is None
    assert result.processed_state_count == 2
    assert result.submitted_order_count == 1
    assert exchange.calls == ["submit"]


@pytest.mark.parametrize(
    "unknown_symbol, price, gross_cap, expected_posts",
    [
        ("ETHUSDT", Decimal("10"), Decimal("100"), 1),
        ("BTCUSDT", Decimal("10"), Decimal("100"), 0),
        ("ETHUSDT", None, Decimal("100"), 0),
        ("ETHUSDT", Decimal("100"), Decimal("25"), 0),
    ],
)
async def test_unknown_order_scope_through_market_loop_and_submission(
    unknown_symbol,
    price,
    gross_cap,
    expected_posts,
) -> None:
    exchange = PlanAwareExchange()
    unknown = PersistedExchangeOrder(
        plan=OrderExecutionPlan(
            client_order_id="existing-unknown",
            intent_id="old-intent",
            run_id="run-1",
            symbol=unknown_symbol,
            side="BUY",
            order_type="LIMIT",
            quantity=Decimal("1"),
            price=price,
            reduce_only=False,
            created_at=NOW,
        ),
        state=ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
        exchange_order_id=None,
        updated_at=NOW,
    )
    context = _runtime_context()
    config = replace(
        context.risk_config, max_open_positions=2, max_gross_notional=gross_cap
    )
    context = replace(
        context,
        risk_config=config,
        gate_context=replace(
            context.gate_context,
        ),
        unresolved_orders=(unknown,),
        unresolved_order_states=(unknown.state,),
    )

    async def provider(state):
        return context

    async def states():
        yield _state()

    daemon = _daemon(
        exchange=exchange,
        context_provider=provider,
        max_gross_exposure=gross_cap,
        max_open_positions=2,
    )
    result = await daemon.run(states())
    assert result.halt_reason is None
    assert result.processed_state_count == 1
    assert result.submitted_order_count == expected_posts
    assert exchange.calls == ["submit"] * expected_posts
    assert all(
        plan.client_order_id != unknown.plan.client_order_id for plan in exchange.plans
    )


async def test_live_daemon_checkpoints_while_reconciliation_is_pending() -> None:
    exchange = PlanAwareExchange()
    repository = SignalingLiveRepository()
    release_stream = asyncio.Event()
    uncertain = ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION

    async def blocked_context(state: object) -> LiveDaemonRuntimeContext:
        del state
        return replace(
            _runtime_context(),
            gate_context=replace(
                gate_context(),
            ),
            unresolved_order_states=(uncertain,),
        )

    async def states() -> AsyncIterator:
        yield _state()
        await release_stream.wait()

    daemon = _daemon(
        exchange=exchange,
        context_provider=blocked_context,
        repository=repository,
        checkpoint_every_states=1,
    )
    run = asyncio.create_task(daemon.run(states()))
    try:
        await asyncio.wait_for(repository.checkpoint_saved.wait(), timeout=0.1)
    finally:
        release_stream.set()
        await run

    assert repository.saved_checkpoint_run_ids == ["run-1"]


async def test_live_daemon_does_not_halt_after_order_outcome_stays_unknown() -> None:
    exchange = FakeExchange(
        submit_result=ExchangeSubmissionTimeoutError(),
        query_result=None,
    )
    daemon = _daemon(exchange=exchange)

    result = await daemon.run(_states())

    assert result.halt_reason is None
    assert result.approved_intent_count == 1
    assert exchange.calls == ["submit", "query", "query", "query", "query", "query"]


async def test_unknown_reduce_only_exit_submits_recovery_for_current_position() -> None:
    exchange = TimeoutThenAcknowledgedExitExchange()
    recovery = CurrentPositionExitRecovery()
    position = ManagedLivePosition(
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.LONG,
        quantity=Decimal("0.001"),
        entry_price=Decimal("31000"),
        opened_at=NOW - timedelta(minutes=1),
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
        exit_recovery_client=recovery,
        exit_manager=LiveExitManager(
            config=LiveExitConfig(
                account_label="primary",
                candle_grace_decision_profit_pct=Decimal("0"),
                run_id="run-1",
                strategy_name="compression_breakout",
                strategy_version="v0",
                strategy_config_hash="a" * 64,
                policy=PositionExitPolicy(max_holding_seconds=30),
            )
        ),
    )

    failure = await daemon.process_account_event(_state())

    assert failure == "pending_exit_order_recovery:BTCUSDT"
    assert not recovery.plans
    await daemon.recover_requested_exits()
    assert len(exchange.plans) == 2
    assert exchange.plans[0].reduce_only is True
    assert exchange.plans[1].reduce_only is True
    assert exchange.plans[1].client_order_id != exchange.plans[0].client_order_id
    assert exchange.plans[1].quantity == Decimal("0.0007")
    assert recovery.plans == [exchange.plans[0]]


async def test_candle_account_event_recovers_existing_unknown_exit() -> None:
    exchange = PlanAwareExchange()
    recovery = CurrentPositionExitRecovery()
    position = ManagedLivePosition(
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.LONG,
        quantity=Decimal("0.001"),
        entry_price=Decimal("31000"),
        opened_at=NOW - timedelta(minutes=1),
    )
    pending_plan = OrderExecutionPlan(
        intent_id="pending-exit",
        run_id="run-1",
        client_order_id="cml_pending_exit_123456789012345678",
        symbol="BTCUSDT",
        side="SELL",
        order_type="MARKET",
        price=None,
        reduce_only=True,
        created_at=NOW,
        position_side=FuturesPositionSide.LONG,
        quantity=Decimal("0.001"),
        quantized=True,
    )
    pending = PersistedExchangeOrder(
        plan=pending_plan,
        state=ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
        exchange_order_id=None,
        updated_at=NOW,
    )

    async def position_context(state: object) -> LiveDaemonRuntimeContext:
        del state
        return replace(
            _runtime_context(),
            open_position_symbols=frozenset({"BTCUSDT"}),
            managed_positions=(position,),
            unresolved_orders=(pending,),
            unresolved_order_states=(
                ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
            ),
        )

    daemon = _daemon(
        exchange=exchange,
        context_provider=position_context,
        exit_recovery_client=recovery,
        exit_manager=LiveExitManager(
            config=LiveExitConfig(
                account_label="primary",
                candle_grace_decision_profit_pct=Decimal("0"),
                run_id="run-1",
                strategy_name="compression_breakout",
                strategy_version="v0",
                strategy_config_hash="a" * 64,
                policy=PositionExitPolicy(mode=PositionExitMode.CANDLE_15M),
            )
        ),
    )

    failure = await daemon.process_account_event(_state())

    assert failure is None
    assert not recovery.plans
    await daemon.recover_requested_exits()
    assert len(exchange.plans) == 1
    assert exchange.plans[0].reduce_only is True
    assert exchange.plans[0].quantity == Decimal("0.0007")
    assert recovery.plans == [pending_plan]


async def test_unknown_reduce_only_limit_exit_reuses_limit_recovery_type() -> None:
    exchange = TimeoutThenAcknowledgedExitExchange()
    recovery = CurrentPositionExitRecovery()
    candidate = replace(
        _intent(),
        candidate_id="limit-exit",
        entry_type=EntryType.LIMIT,
        limit_price=Decimal("30000"),
        desired_notional=Decimal("30"),
        reduce_only=True,
        expires_at=NOW + timedelta(minutes=1),
    )
    context = _runtime_context()
    daemon = _daemon(
        exchange=exchange,
        exit_recovery_client=recovery,
        hedge_mode=True,
    )

    approved, submitted, failure = await daemon._exit_processor.process_requests(
        (
            LiveExitOrderRequest(
                candidate=candidate,
                quantity=Decimal("0.001"),
            ),
        ),
        state=_state(),
        context=context,
    )

    assert failure == "pending_exit_order_recovery:BTCUSDT"
    assert not recovery.plans
    await daemon.recover_requested_exits()
    assert approved == 1
    assert submitted == 1
    assert exchange.plans[0].order_type == "LIMIT"
    assert exchange.plans[1].order_type == "LIMIT"
    assert exchange.plans[1].price == Decimal("30000")
    assert exchange.plans[1].quantity == Decimal("0.0007")


async def test_unknown_reduce_only_exit_does_not_duplicate_active_exit() -> None:
    exchange = TimeoutThenAcknowledgedExitExchange()
    recovery = ActiveExitRecovery()
    candidate = replace(
        _intent(),
        candidate_id="active-exit",
        reduce_only=True,
        desired_notional=Decimal("30"),
    )
    daemon = _daemon(
        exchange=exchange,
        exit_recovery_client=recovery,
        hedge_mode=True,
    )

    approved, submitted, failure = await daemon._exit_processor.process_requests(
        (
            LiveExitOrderRequest(
                candidate=candidate,
                quantity=Decimal("0.001"),
            ),
        ),
        state=_state(),
        context=_runtime_context(),
    )

    assert failure == "pending_exit_order_recovery:BTCUSDT"
    assert not recovery.plans
    await daemon.recover_requested_exits()
    assert approved == 1
    assert submitted == 1
    assert len(exchange.plans) == 1
    assert recovery.plans == [exchange.plans[0]]


async def test_unknown_grace_limit_cancel_falls_back_to_market() -> None:
    exchange = UnknownCancelExchange()
    recovery = CurrentPositionExitRecovery()
    cancel_plan = OrderExecutionPlan(
        intent_id="grace-limit",
        run_id="run-1",
        client_order_id="cml_grace_limit_123456789012345678",
        symbol="BTCUSDT",
        side="SELL",
        order_type="LIMIT",
        quantity=Decimal("0.001"),
        price=Decimal("30000"),
        reduce_only=True,
        created_at=NOW - timedelta(minutes=15),
        position_side=FuturesPositionSide.LONG,
        quantized=True,
    )
    fallback_candidate = replace(
        _intent(),
        candidate_id="market-fallback",
        entry_type=EntryType.MARKET,
        limit_price=None,
        desired_notional=Decimal("30"),
        reduce_only=True,
    )
    daemon = _daemon(
        exchange=exchange,
        exit_recovery_client=recovery,
        hedge_mode=True,
    )

    approved, submitted, failure = await daemon._exit_processor.process_requests(
        (
            LiveExitCancellationRequest(
                cancel_plan=cancel_plan,
                fallback_candidate=fallback_candidate,
                fallback_quantity=Decimal("0.001"),
            ),
        ),
        state=_state(),
        context=_runtime_context(),
    )

    assert failure == "pending_exit_order_recovery:BTCUSDT"
    assert not recovery.plans
    await daemon.recover_requested_exits()
    assert approved == 0
    assert submitted == 0
    assert len(exchange.plans) == 1
    assert exchange.plans[0].order_type == "MARKET"


async def test_grace_timeout_resizes_intent_after_cancel_fill() -> None:
    class PartiallyFilledCancelExchange(PlanAwareExchange):
        async def cancel_order_by_client_id(
            self,
            symbol: str,
            client_order_id: str,
        ) -> ExchangeOrderSnapshot:
            del symbol
            self.calls.append("cancel")
            return ExchangeOrderSnapshot(
                client_order_id=client_order_id,
                exchange_order_id="exchange-cancel",
                state=ExchangeOrderState.CANCELED,
                observed_at=NOW,
                executed_quantity=Decimal("0.0003"),
                average_price=Decimal("30000"),
            )

    class RecordingRepository(FakeLiveRepository):
        def __init__(self) -> None:
            super().__init__()
            self.intents: list[OrderIntentCandidate] = []

        async def save_approved_intent(
            self,
            intent: OrderIntentCandidate,
            evaluation: RiskEvaluation,
        ) -> None:
            del evaluation
            self.intents.append(intent)

    exchange = PartiallyFilledCancelExchange()
    repository = RecordingRepository()
    cancel_plan = OrderExecutionPlan(
        intent_id="grace-limit",
        run_id="run-1",
        client_order_id="cml_grace_limit_123456789012345678",
        symbol="BTCUSDT",
        side="SELL",
        order_type="LIMIT",
        quantity=Decimal("0.001"),
        price=Decimal("30000"),
        reduce_only=True,
        created_at=NOW - timedelta(minutes=15),
        position_side=FuturesPositionSide.LONG,
        quantized=True,
    )
    fallback_candidate = replace(
        _intent(),
        candidate_id="market-fallback",
        entry_type=EntryType.MARKET,
        limit_price=None,
        desired_notional=Decimal("30"),
        reduce_only=True,
        features={
            "quantity": "0.001",
            "reference_price": "30000",
        },
    )
    position = ManagedLivePosition(
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.LONG,
        quantity=Decimal("0.001"),
        entry_price=Decimal("30000"),
        opened_at=NOW - timedelta(minutes=30),
    )
    context = replace(
        _runtime_context(),
        open_position_symbols=frozenset({"BTCUSDT"}),
        managed_positions=(position,),
    )

    async def current_position_context(state: object) -> LiveDaemonRuntimeContext:
        del state
        return context

    daemon = _daemon(
        exchange=exchange,
        repository=repository,
        hedge_mode=True,
        context_provider=current_position_context,
    )

    approved, submitted, failure = await daemon._exit_processor.process_requests(
        (
            LiveExitCancellationRequest(
                cancel_plan=cancel_plan,
                fallback_candidate=fallback_candidate,
                fallback_quantity=Decimal("0.001"),
            ),
        ),
        state=_state(),
        context=context,
    )

    assert failure is None
    assert approved == 1
    assert submitted == 1
    assert exchange.plans[-1].quantity == Decimal("0.0007")
    assert repository.intents[-1].features["quantity"] == "0.0007"
    assert repository.intents[-1].desired_notional == Decimal("21.0000")


async def test_live_daemon_keeps_running_through_transient_database_error() -> None:
    exchange = PlanAwareExchange()
    calls = 0

    async def flaky_context(state: object) -> LiveDaemonRuntimeContext:
        nonlocal calls
        del state
        calls += 1
        if calls == 1:
            raise OperationalError("context", {}, RuntimeError("recovery"))
        return _runtime_context()

    async def states() -> AsyncIterator:
        yield _state()
        next_state = _state()
        yield replace(
            next_state,
            bucket_start=next_state.bucket_start + timedelta(seconds=15),
            bucket_end=next_state.bucket_end + timedelta(seconds=15),
        )

    daemon = _daemon(exchange=exchange, context_provider=flaky_context)

    result = await daemon.run(states())

    assert result.halt_reason is None
    assert result.processed_state_count == 2
    assert result.submitted_order_count == 1


async def test_market_unavailable_blocks_entries_but_keeps_exit_lane_enabled() -> None:
    exchange = PlanAwareExchange()
    position = ManagedLivePosition(
        symbol="BTCUSDT",
        side=StrategySide.LONG,
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
                account_label="primary",
                candle_grace_decision_profit_pct=Decimal("0"),
                run_id="run-1",
                strategy_name="compression_breakout",
                strategy_version="v0",
                strategy_config_hash="a" * 64,
                policy=PositionExitPolicy(max_holding_seconds=30),
            )
        ),
    )
    daemon.set_entry_enabled(False, reason="market_state_hub_unavailable")

    result = await daemon.run(_states())
    assert result.halt_reason is None
    assert result.approved_intent_count == 1
    assert result.submitted_order_count == 1
    assert exchange.plans[0].reduce_only is True

    failure = await daemon.process_account_event(_state())
    assert failure is None
    assert len(exchange.plans) == 2
    assert exchange.plans[1].reduce_only is True


async def test_absent_recovery_order_falls_back_to_market_exit() -> None:
    exchange = AbsentRecoveryExchange()
    recovery_created_at = NOW - timedelta(minutes=15)
    recovery_plan = OrderExecutionPlan(
        intent_id="recovery-intent",
        run_id="run-1",
        client_order_id="recovery-client",
        symbol="BTCUSDT",
        side="SELL",
        order_type="LIMIT",
        quantity=Decimal("0.001"),
        price=Decimal("30000"),
        reduce_only=True,
        created_at=recovery_created_at,
        position_side=FuturesPositionSide.LONG,
        quantized=True,
    )
    position = ManagedLivePosition(
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.LONG,
        quantity=Decimal("0.001"),
        entry_price=Decimal("30000"),
        opened_at=NOW - timedelta(minutes=30),
        recovery_order_client_id=recovery_plan.client_order_id,
        recovery_exit_started_at=recovery_created_at,
        recovery_order_created_at=recovery_created_at,
        recovery_order_plan=recovery_plan,
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
                account_label="primary",
                candle_grace_decision_profit_pct=Decimal("0.0088"),
                run_id="run-1",
                strategy_name="compression_breakout",
                strategy_version="v0",
                strategy_config_hash="a" * 64,
                policy=PositionExitPolicy(mode=PositionExitMode.CANDLE_15M),
                candle_grace_bars=1,
                candle_grace_profit_pct=Decimal("0.0088"),
            )
        ),
    )

    failure = await daemon.process_grace_timeout(_state(), now=NOW)

    assert failure is None
    assert exchange.calls[0] == "cancel"
    assert exchange.calls[-1] == "submit"
    assert exchange.plans[-1].order_type == "MARKET"
    assert exchange.plans[-1].reduce_only is True
    assert exchange.plans[-1].quantity == Decimal("0.001")


async def test_closed_candle_exit_uses_direct_event_without_rest_loader() -> None:
    exchange = PlanAwareExchange()
    position = ManagedLivePosition(
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.LONG,
        quantity=Decimal("0.001"),
        entry_price=Decimal("31000"),
        opened_at=datetime(2026, 7, 4, 0, 1, tzinfo=UTC),
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
                account_label="primary",
                candle_grace_decision_profit_pct=Decimal("0"),
                run_id="run-1",
                strategy_name="compression_breakout",
                strategy_version="v0",
                strategy_config_hash="a" * 64,
                policy=PositionExitPolicy(mode=PositionExitMode.CANDLE_15M),
            )
        ),
    )
    event_at = datetime(2026, 7, 4, 0, 30, 0, 100000, tzinfo=UTC)
    event = ClosedCandle15mEvent(
        candle=ClosedCandle15m(
            symbol="BTCUSDT",
            candle_start=datetime(2026, 7, 4, 0, 15, tzinfo=UTC),
            candle_end=datetime(2026, 7, 4, 0, 30, tzinfo=UTC),
            open_price=Decimal("31000"),
            close_price=Decimal("30000"),
        ),
        exchange_event_at=datetime(2026, 7, 4, 0, 30, tzinfo=UTC),
        received_at=event_at,
    )

    failure = await daemon.process_closed_candle(event)

    assert failure is None
    assert exchange.calls == ["submit"]
    assert exchange.plans[0].symbol == "BTCUSDT"
    assert exchange.plans[0].reduce_only is True


async def test_live_daemon_resets_only_symbol_when_states_skip_buckets() -> None:
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
        max_gross_exposure=Decimal("100"),
    )

    result = await daemon.run(states())

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


async def test_live_daemon_resets_each_symbol_after_explicit_market_gap() -> None:
    exchange = PlanAwareExchange()
    strategy = GapAwareFakeStrategy()
    daemon = _daemon(exchange=exchange, strategy=strategy)

    daemon.notify_market_state_gap(reason="market_state_consumer_lagged")
    result = await daemon.run(_states())

    assert result.halt_reason is None
    assert strategy.reset_symbols == ["BTCUSDT"]
    assert strategy.reset_counts_at_decision == [1]


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


async def test_live_daemon_does_not_halt_on_periodic_checkpoint_timeout() -> None:
    exchange = PlanAwareExchange()
    repository = FakeLiveRepository()
    repository.checkpoint_failures_remaining = 1
    daemon = _daemon(
        exchange=exchange,
        repository=repository,
        checkpoint_every_states=1,
    )

    result = await daemon.run(_states())

    assert result.halt_reason is None
    assert exchange.calls == ["submit"]


async def test_live_daemon_submits_hedge_mode_reduce_only_exit() -> None:
    exchange = PlanAwareExchange()
    position = ManagedLivePosition(
        symbol="BTCUSDT",
        side=StrategySide.LONG,
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
                account_label="primary",
                candle_grace_decision_profit_pct=Decimal("0"),
                run_id="run-1",
                strategy_name="compression_breakout",
                strategy_version="v0",
                strategy_config_hash="a" * 64,
                policy=PositionExitPolicy(max_holding_seconds=30),
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


@pytest.mark.parametrize("capacity", [1, 2])
async def test_live_daemon_continues_with_unmanaged_account_position(capacity) -> None:
    exchange = PlanAwareExchange()

    async def position_context(state: object) -> LiveDaemonRuntimeContext:
        del state
        return replace(
            _runtime_context(),
            risk_config=replace(
                _runtime_context().risk_config, max_open_positions=capacity
            ),
            open_position_symbols=frozenset({"ETHUSDT"}),
            unmanaged_position_symbols=frozenset({"ETHUSDT"}),
        )

    daemon = _daemon(
        exchange=exchange,
        context_provider=position_context,
        max_open_positions=capacity,
    )

    result = await daemon.run(_states())

    assert result.halt_reason is None
    assert result.processed_state_count == 1
    assert exchange.calls == (["submit"] if capacity == 2 else [])


async def test_pending_account_position_does_not_block_entry_lane() -> None:
    exchange = PlanAwareExchange()

    async def position_context(state: object) -> LiveDaemonRuntimeContext:
        del state
        return replace(
            _runtime_context(),
            open_position_symbols=frozenset({"ETHUSDT"}),
            pending_position_symbols=frozenset({"ETHUSDT"}),
        )

    daemon = _daemon(
        exchange=exchange,
        context_provider=position_context,
        exit_manager=LiveExitManager(
            config=LiveExitConfig(
                account_label="primary",
                candle_grace_decision_profit_pct=Decimal("0"),
                run_id="run-1",
                strategy_name="compression_breakout",
                strategy_version="v0",
                strategy_config_hash="a" * 64,
                policy=PositionExitPolicy(),
            )
        ),
    )

    failure = await daemon.process_account_event(replace(_state(), symbol="ETHUSDT"))

    assert failure is None
    # Account-wide entry lane is not closed for other symbols
    assert daemon.entry_enabled is True
    # Reconciliation remains diagnostic
    assert daemon.is_symbol_entry_allowed("ETHUSDT") == (True, "entry_allowed")
    # Other symbols (e.g. BTCUSDT) remain allowed
    assert daemon.is_symbol_entry_allowed("BTCUSDT") == (True, "entry_allowed")
    assert exchange.calls == []


async def test_scheduled_risk_window_late_start_after_reopen_is_noop(
    monkeypatch,
) -> None:
    exchange = PlanAwareExchange()
    reopen_time = datetime(2026, 7, 4, 2, 0, tzinfo=UTC)
    position = ManagedLivePosition(
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.LONG,
        quantity=Decimal("0.001"),
        entry_price=Decimal("30000"),
        opened_at=reopen_time - timedelta(minutes=1),
    )
    cancellation_calls: list[tuple[OrderExecutionPlan, ...]] = []

    async def cancel_entries(
        plans: tuple[OrderExecutionPlan, ...],
    ) -> int:
        cancellation_calls.append(plans)
        return 0

    async def fetch_positions() -> tuple[AccountPositionSnapshot, ...]:
        return ()

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
                account_label="primary",
                candle_grace_decision_profit_pct=Decimal("0"),
                run_id="run-1",
                strategy_name="compression_breakout",
                strategy_version="v0",
                strategy_config_hash="a" * 64,
                policy=PositionExitPolicy(),
            ),
        ),
        clock=lambda: reopen_time,
        scheduled_risk_window=ScheduledRiskWindowConfig(),
        cancel_unfilled_entry_orders=cancel_entries,
        fetch_exchange_positions=fetch_positions,
    )
    daemon._scheduled_controller.observe_state(_state())

    gate_calls: list[tuple[bool, str]] = []
    original_set_gate = daemon.set_scheduled_entry_blocked

    def record_gate(blocked: bool, *, reason: str) -> None:
        gate_calls.append((blocked, reason))
        original_set_gate(blocked, reason=reason)

    monkeypatch.setattr(daemon, "set_scheduled_entry_blocked", record_gate)

    failure = await daemon.process_scheduled_risk_window(now=reopen_time)

    assert failure is None
    assert exchange.plans == []
    assert cancellation_calls == []
    assert daemon.entry_enabled is True
    assert all(not blocked for blocked, _reason in gate_calls)
    assert all(
        reason != "scheduled_risk_window_complete" for _blocked, reason in gate_calls
    )


async def test_risk_control_cancel_all_open_entries_uses_injected_coordinator() -> None:
    exchange = PlanAwareExchange()
    cancellation_calls: list[tuple[OrderExecutionPlan, ...]] = []

    async def cancel_entries(
        plans: tuple[OrderExecutionPlan, ...],
    ) -> int:
        cancellation_calls.append(plans)
        return 0

    daemon = _daemon(
        exchange=exchange,
        cancel_unfilled_entry_orders=cancel_entries,
    )

    failure = await daemon.cancel_all_open_entries()

    assert failure is None
    assert cancellation_calls == [()]
    assert exchange.calls == []


async def test_risk_control_flatten_reuses_reduce_only_exit_processor() -> None:
    exchange = PlanAwareExchange()
    position = ManagedLivePosition(
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.LONG,
        quantity=Decimal("0.001"),
        entry_price=Decimal("30000"),
        opened_at=NOW - timedelta(minutes=1),
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
                account_label="primary",
                candle_grace_decision_profit_pct=Decimal("0"),
                run_id="run-1",
                strategy_name="compression_breakout",
                strategy_version="v0",
                strategy_config_hash="a" * 64,
                policy=PositionExitPolicy(),
            )
        ),
    )
    daemon._scheduled_controller.observe_state(_state())

    failure = await daemon.request_flatten()

    assert failure is None
    assert len(exchange.plans) == 1
    assert exchange.plans[0].reduce_only is True
    assert exchange.plans[0].order_type == "MARKET"


async def test_scheduled_flatten_targets_exchange_position_before_market_state_arrives() -> (
    None
):
    exchange = PlanAwareExchange()
    position = ManagedLivePosition(
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.LONG,
        quantity=Decimal("0.001"),
        entry_price=Decimal("30000"),
        opened_at=NOW - timedelta(minutes=1),
    )

    async def position_context(state: object) -> LiveDaemonRuntimeContext:
        del state
        return replace(
            _runtime_context(),
            open_position_symbols=frozenset({"BTCUSDT"}),
            managed_positions=(position,),
        )

    async def fetch_positions() -> tuple[AccountPositionSnapshot, ...]:
        return (
            AccountPositionSnapshot(
                environment="live",
                account_label="primary",
                symbol="BTCUSDT",
                position_side="LONG",
                position_amt=Decimal("0.001"),
                entry_price=Decimal("30000"),
                mark_price=Decimal("30000"),
                unrealized_pnl=Decimal("0"),
                notional=Decimal("30"),
                leverage=10,
                margin_type="CROSSED",
                observed_at=NOW,
                raw_payload={},
            ),
        )

    daemon = _daemon(
        exchange=exchange,
        context_provider=position_context,
        exit_manager=LiveExitManager(
            config=LiveExitConfig(
                account_label="primary",
                candle_grace_decision_profit_pct=Decimal("0"),
                run_id="run-1",
                strategy_name="compression_breakout",
                strategy_version="v0",
                strategy_config_hash="a" * 64,
                policy=PositionExitPolicy(),
            )
        ),
        clock=lambda: NOW,
        fetch_exchange_positions=fetch_positions,
    )
    # The market loop has only delivered another symbol so far.  The
    # exchange position must still be flattened immediately.
    daemon._scheduled_controller.observe_state(replace(_state(), symbol="ETHUSDT"))

    failure = await daemon.request_flatten(now=NOW)

    assert failure is None
    assert len(exchange.plans) == 1
    assert exchange.plans[0].symbol == "BTCUSDT"
    assert exchange.plans[0].reduce_only is True
    assert exchange.plans[0].order_type == "MARKET"


@pytest.mark.parametrize("unmanaged_symbols", [frozenset(), frozenset({"ETHUSDT"})])
@pytest.mark.parametrize("cancel_failed", [False, True])
@pytest.mark.parametrize("wait_for_exit", [False, True])
@pytest.mark.parametrize("start_minute", [45, 55])
async def test_scheduled_risk_window_flattens_verifies_and_reopens_entries(
    unmanaged_symbols,
    cancel_failed,
    wait_for_exit,
    start_minute,
) -> None:
    exit_sent = asyncio.Event()

    class SignallingExchange(PlanAwareExchange):
        async def submit_order(self, plan):
            result = await super().submit_order(plan)
            exit_sent.set()
            return result

    exchange = SignallingExchange()
    scheduled_now = datetime(2026, 7, 3, 23, start_minute, tzinfo=UTC)
    current_time = [scheduled_now]
    position = ManagedLivePosition(
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.LONG,
        quantity=Decimal("0.001"),
        entry_price=Decimal("30000"),
        opened_at=scheduled_now - timedelta(minutes=1),
    )
    cancellation_calls: list[tuple[OrderExecutionPlan, ...]] = []

    async def cancel_entries(
        plans: tuple[OrderExecutionPlan, ...],
    ) -> int:
        cancellation_calls.append(plans)
        if wait_for_exit:
            await asyncio.wait_for(exit_sent.wait(), timeout=1)
        if cancel_failed:
            raise OSError("cancel unavailable")
        return 0

    async def fetch_positions() -> tuple[AccountPositionSnapshot, ...]:
        return (
            AccountPositionSnapshot(
                environment="live",
                account_label="primary",
                symbol="BTCUSDT",
                position_side="LONG",
                position_amt=Decimal("0") if exit_sent.is_set() else position.quantity,
                entry_price=Decimal("30000"),
                mark_price=Decimal("30000"),
                unrealized_pnl=Decimal("0"),
                notional=Decimal("0")
                if exit_sent.is_set()
                else position.quantity * position.entry_price,
                leverage=10,
                margin_type="CROSSED",
                observed_at=current_time[0],
                raw_payload={},
            ),
        )

    async def position_context(state: object) -> LiveDaemonRuntimeContext:
        del state
        return replace(
            _runtime_context(),
            open_position_symbols=frozenset({"BTCUSDT"}),
            managed_positions=(position,),
            unmanaged_position_symbols=unmanaged_symbols,
        )

    daemon = _daemon(
        exchange=exchange,
        context_provider=position_context,
        exit_manager=LiveExitManager(
            config=LiveExitConfig(
                account_label="primary",
                candle_grace_decision_profit_pct=Decimal("0"),
                run_id="run-1",
                strategy_name="compression_breakout",
                strategy_version="v0",
                strategy_config_hash="a" * 64,
                policy=PositionExitPolicy(),
            )
        ),
        clock=lambda: current_time[0],
        scheduled_risk_window=ScheduledRiskWindowConfig(),
        cancel_unfilled_entry_orders=cancel_entries,
        fetch_exchange_positions=fetch_positions,
    )
    daemon._scheduled_controller.observe_state(_state())

    failure = await asyncio.wait_for(
        daemon.process_scheduled_risk_window(now=scheduled_now), timeout=2
    )

    assert failure == (
        "scheduled_entry_order_cancel_failed:OSError" if cancel_failed else None
    )
    assert daemon.entry_enabled is False
    assert cancellation_calls == [()]
    assert len(exchange.plans) == 1
    assert exchange.plans[0].reduce_only is True
    assert exchange.plans[0].order_type == "MARKET"

    cancel_failed = False
    verification_time = datetime(2026, 7, 3, 23, 58, tzinfo=UTC)
    current_time[0] = verification_time
    failure = await daemon.process_scheduled_risk_window(now=verification_time)

    assert failure is None
    assert daemon.entry_enabled is False

    reopen_time = datetime(2026, 7, 4, 2, 0, tzinfo=UTC)
    current_time[0] = reopen_time
    failure = await daemon.process_scheduled_risk_window(now=reopen_time)

    assert failure is None
    assert daemon.entry_enabled is True


class FakeLiveRepository:
    def __init__(self) -> None:
        self.saved_checkpoint_run_ids: list[str] = []
        self.checkpoint_failures_remaining = 0

    async def save_approved_intent(
        self,
        intent: OrderIntentCandidate,
        evaluation: RiskEvaluation,
    ) -> None:
        pass

    async def prepare_submission_in_session(
        self, session, *, intent, evaluation, plan, prepared_at, **kwargs
    ):
        await self.save_approved_intent(intent, evaluation)
        return PreparedOrderSubmission(
            plan=plan,
            submitting_event=ExchangeOrderEvent(
                event_id=f"prepared:{plan.client_order_id}",
                client_order_id=plan.client_order_id,
                state=ExchangeOrderState.SUBMITTING,
                occurred_at=prepared_at,
                exchange_order_id=None,
                details={},
            ),
        )

    async def save_checkpoint(
        self,
        run_id: str,
        checkpoint: StrategyCheckpoint,
        saved_at: datetime,
    ) -> None:
        if self.checkpoint_failures_remaining > 0:
            self.checkpoint_failures_remaining -= 1
            raise TimeoutError("checkpoint timeout")
        self.saved_checkpoint_run_ids.append(run_id)


class SignalingLiveRepository(FakeLiveRepository):
    def __init__(self) -> None:
        super().__init__()
        self.checkpoint_saved = asyncio.Event()

    async def save_checkpoint(
        self,
        run_id: str,
        checkpoint: StrategyCheckpoint,
        saved_at: datetime,
    ) -> None:
        await super().save_checkpoint(run_id, checkpoint, saved_at)
        self.checkpoint_saved.set()


_created_coordinators: list[OrderExecutionCoordinator] = []


def _daemon(
    *,
    exchange,
    request_order_cleanup=lambda plans: None,
    strategy=None,
    context_provider=None,
    repository: FakeLiveRepository | None = None,
    checkpoint_repository=None,
    checkpoint_every_states: int = 1,
    exit_manager: LiveExitManager | None = None,
    hedge_mode: bool = False,
    entry_symbol_loader=None,
    entry_filter_context_loader=None,
    entry_universe_context_provider=None,
    entry_universe_snapshot_provider=None,
    signal_recorder=None,
    require_price_above_ema5: bool = False,
    require_price_above_ema10: bool = False,
    entry_order_type: EntryType = EntryType.LIMIT,
    max_gross_exposure: Decimal = Decimal("25"),
    max_open_positions: int = 1,
    exit_recovery_client=None,
    clock=None,
    scheduled_risk_window: ScheduledRiskWindowConfig | None = None,
    cancel_unfilled_entry_orders=None,
    fetch_exchange_positions=None,
) -> LiveStrategyDaemon:
    order_repository = FakeOrderRepository()
    machine = OrderExecutionStateMachine(
        exchange=exchange,
        event_repository=order_repository,
        live_submit_enabled=True,
        clock=lambda: NOW,
        reconciliation_retry_delays=(0.0, 0.0, 0.0, 0.0),
    )

    async def default_context(state: object) -> LiveDaemonRuntimeContext:
        del state
        context = _runtime_context()
        config = replace(context.risk_config, max_gross_notional=max_gross_exposure)
        return replace(
            context,
            risk_config=config,
            gate_context=replace(
                context.gate_context,
            ),
        )

    coordinator = OrderExecutionCoordinator(
        backend=machine,
        account_label="test_account",
        environment="live",
        execution_book=ExecutionBook(),
    )
    _created_coordinators.append(coordinator)
    submission_repository = repository or FakeLiveRepository()
    checkpoint_repository = checkpoint_repository or submission_repository
    return LiveStrategyDaemon(
        request_order_cleanup=request_order_cleanup,
        strategy=strategy or FakeStrategy(),
        risk_gateway=RiskGateway(
            limits=FixedLiveLimits(
                notional_cap=Decimal("25"),
                max_open_positions=max_open_positions,
                max_daily_loss=Decimal("10"),
                max_gross_exposure=max_gross_exposure,
            ),
        ),
        submission_repository=submission_repository,
        persist_checkpoint=checkpoint_repository.save_checkpoint,
        state_machine=coordinator,
        context_provider=context_reader(context_provider or default_context),
        signal_recorder=signal_recorder,
        config=LiveDaemonConfig(
            run_id="run-1",
            account_label="test_account",
            resize_tolerance=Decimal("0.20"),
            checkpoint_every_states=checkpoint_every_states,
            hedge_mode=hedge_mode,
            entry_symbol_loader=entry_symbol_loader,
            require_price_above_ema5=require_price_above_ema5,
            require_price_above_ema10=require_price_above_ema10,
            entry_filter_context_loader=entry_filter_context_loader,
            entry_universe_context_provider=entry_universe_context_provider,
            entry_universe_snapshot_provider=entry_universe_snapshot_provider,
            entry_order_type=entry_order_type,
            scheduled_risk_window=scheduled_risk_window,
        ),
        exit_manager=exit_manager,
        exit_recovery_client=exit_recovery_client,
        clock=clock or (lambda: NOW),
        cancel_unfilled_entry_orders=cancel_unfilled_entry_orders,
        fetch_exchange_positions=fetch_exchange_positions,
        request_exit_recovery=Mock(),
    )


class RecordingSignalRecorder:
    def __init__(self) -> None:
        self.decision_filter_context: dict[str, object] | None = None

    def record_decision(
        self,
        *,
        decision,
        state,
        recorded_at,
        account_context,
        filter_context,
    ) -> None:
        del decision, state, recorded_at, account_context
        self.decision_filter_context = filter_context

    def record_candidate(
        self,
        *,
        candidate,
        state,
        recorded_at,
        account_context,
        filter_context,
    ) -> None:
        del candidate, state, recorded_at, account_context, filter_context


class PlanAwareExchange(FakeExchange):
    def __init__(self) -> None:
        self.on_request = None
        self.on_response = None
        self.calls: list[str] = []
        self.plans: list[OrderExecutionPlan] = []

    async def submit_order(self, plan: OrderExecutionPlan) -> ExchangeOrderSnapshot:
        await self.emit_submit_boundary(plan, is_request=True)
        try:
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
        finally:
            await self.emit_submit_boundary(plan, is_request=False)

    async def query_order_by_client_id(
        self,
        symbol: str,
        client_order_id: str,
    ) -> ExchangeOrderSnapshot | None:
        self.calls.append("query")
        return None


class AbsentRecoveryExchange(PlanAwareExchange):
    async def cancel_order_by_client_id(
        self,
        symbol: str,
        client_order_id: str,
    ) -> ExchangeOrderSnapshot:
        del symbol, client_order_id
        self.calls.append("cancel")
        raise ExchangeOrderAlreadyAbsentError(
            "Unknown order sent.",
            exchange_code=-2011,
            exchange_message="Unknown order sent.",
            http_status=400,
        )


class TimeoutThenAcknowledgedExitExchange(PlanAwareExchange):
    async def submit_order(self, plan: OrderExecutionPlan) -> ExchangeOrderSnapshot:
        await self.emit_submit_boundary(plan, is_request=True)
        try:
            self.calls.append("submit")
            self.plans.append(plan)
            if len(self.plans) == 1:
                raise ExchangeSubmissionTimeoutError("submit timed out")
            return ExchangeOrderSnapshot(
                client_order_id=plan.client_order_id,
                exchange_order_id="exchange-recovery",
                state=ExchangeOrderState.ACKNOWLEDGED,
                observed_at=NOW,
                executed_quantity=Decimal("0"),
                average_price=Decimal("0"),
            )
        finally:
            await self.emit_submit_boundary(plan, is_request=False)


class UnknownCancelExchange(TimeoutThenAcknowledgedExitExchange):
    async def cancel_order_by_client_id(
        self,
        symbol: str,
        client_order_id: str,
    ) -> ExchangeOrderSnapshot:
        del symbol, client_order_id
        self.calls.append("cancel")
        raise ExchangeCancellationUnknownError("cancel timed out")


class CurrentPositionExitRecovery:
    def __init__(self) -> None:
        self.plans: list[OrderExecutionPlan] = []

    async def inspect_exit_order(
        self,
        plan: OrderExecutionPlan,
    ) -> SimpleNamespace:
        self.plans.append(plan)
        return SimpleNamespace(
            order=None,
            position_quantity=Decimal("0.0007"),
            active_exit_order_client_ids=(),
            observed_at=NOW,
        )


class ActiveExitRecovery(CurrentPositionExitRecovery):
    async def inspect_exit_order(
        self,
        plan: OrderExecutionPlan,
    ) -> SimpleNamespace:
        self.plans.append(plan)
        return SimpleNamespace(
            order=ExchangeOrderSnapshot(
                client_order_id=plan.client_order_id,
                exchange_order_id="exchange-active",
                state=ExchangeOrderState.ACKNOWLEDGED,
                observed_at=NOW,
                executed_quantity=Decimal("0"),
                average_price=Decimal("0"),
            ),
            position_quantity=Decimal("0.0007"),
            active_exit_order_client_ids=(plan.client_order_id,),
            observed_at=NOW,
        )


class TwoCandidateStrategy(FakeStrategy):
    def on_market_state(self, state: MarketState15s) -> StrategyDecision:
        decision = super().on_market_state(state)
        second = replace(
            decision.candidates[0],
            candidate_id="candidate-2",
        )
        return replace(decision, candidates=(decision.candidates[0], second))


class BlockingPlanAwareExchange(PlanAwareExchange):
    def __init__(self) -> None:
        super().__init__()
        self.submit_started = asyncio.Event()
        self.release_submit = asyncio.Event()

    async def submit_order(self, plan: OrderExecutionPlan) -> ExchangeOrderSnapshot:
        self.submit_started.set()
        await self.release_submit.wait()
        return await super().submit_order(plan)


class GapAwareFakeStrategy(FakeStrategy):
    def required_data(self):
        from crypto_momentum_lab.domain.strategy import StrategyDataRequirement

        return StrategyDataRequirement(15, 1, ("close_price",), 30, False)

    def __init__(self) -> None:
        self.reset_symbols: list[str] = []
        self.reset_counts_at_decision: list[int] = []

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
        account_state=ExecutionAccountStatus.RUNNING,
        account_observed_at=NOW,
        open_position_symbols=frozenset(),
        realized_pnl=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        gross_exposure=Decimal("0"),
        active_halts=(),
        unresolved_order_states=(),
        risk_config=gate_risk_config(),
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


async def test_history_replay_reaches_live_decision_without_loading_live_context():
    exchange = PlanAwareExchange()
    calls = []

    async def context(state):
        assert not state.is_backfill, "replay must not query or repair live exposure"
        calls.append(state)
        return _runtime_context()

    async def states():
        for index in range(64):
            yield replace(
                _state(),
                is_backfill=True,
                bucket_start=_state().bucket_start
                - timedelta(seconds=15 * (64 - index)),
                bucket_end=_state().bucket_end - timedelta(seconds=15 * (64 - index)),
            )
        yield _state()

    daemon = _daemon(exchange=exchange, context_provider=context)
    result = await daemon.run(states())
    assert result.processed_state_count == 65
    assert result.halt_reason is None
    assert calls == [_state()]
    assert exchange.calls == ["submit"]


async def test_submission_and_checkpoint_persistence_are_independent():
    class SubmissionOnly(FakeLiveRepository):
        def __init__(self):
            self.approved = []

        async def save_approved_intent(self, intent, evaluation):
            self.approved.append(intent)

    saved = []

    async def persist_checkpoint(run_id, checkpoint, saved_at):
        saved.append((run_id, checkpoint, saved_at))

    submissions = SubmissionOnly()
    checkpoints = SimpleNamespace(save_checkpoint=persist_checkpoint)
    daemon = _daemon(
        exchange=PlanAwareExchange(),
        repository=submissions,
        checkpoint_repository=checkpoints,
    )
    result = await daemon.run(_states())
    assert result.submitted_order_count == 1
    assert len(submissions.approved) == 1
    assert saved
    assert all(item[0] == "run-1" for item in saved)


@pytest.mark.parametrize("waiting_on", ["cancellation", "verification"])
async def test_market_loop_keeps_processing_while_scheduled_io_waits(waiting_on):
    minute = 45 if waiting_on == "cancellation" else 58
    now = datetime(2026, 7, 3, 23, minute, tzinfo=UTC)
    started = asyncio.Event()
    release = asyncio.Event()

    async def cancel(plans):
        if waiting_on == "cancellation":
            started.set()
            await release.wait()
        return 0

    async def verify():
        started.set()
        await release.wait()
        return ()

    daemon = _daemon(
        exchange=PlanAwareExchange(),
        clock=lambda: now,
        scheduled_risk_window=ScheduledRiskWindowConfig(),
        cancel_unfilled_entry_orders=cancel,
        fetch_exchange_positions=verify,
    )

    async def states():
        await started.wait()
        yield _state()
        state = _state()
        yield replace(
            state,
            bucket_start=state.bucket_start + timedelta(seconds=15),
            bucket_end=state.bucket_end + timedelta(seconds=15),
        )

    task = asyncio.create_task(daemon.run(states()))
    try:
        result = await asyncio.wait_for(asyncio.shield(task), 0.5)
        assert result.processed_state_count == 2
        assert result.submitted_order_count == 0
        assert result.halt_reason is None
        assert not daemon.entry_enabled
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def test_schedule_gate_closes_at_boundary_without_waiting_for_timer():
    current = [datetime(2026, 7, 3, 23, 44, 59, tzinfo=UTC)]
    daemon = _daemon(
        exchange=PlanAwareExchange(),
        clock=lambda: current[0],
        scheduled_risk_window=ScheduledRiskWindowConfig(),
    )
    assert daemon.entry_enabled
    current[0] += timedelta(seconds=1)
    assert not daemon.entry_enabled
    assert daemon.entry_enabled_reason == "scheduled_risk_window"
    # A controller awaiting evidence must remain closed after the wall-clock window.
    daemon.set_scheduled_entry_blocked(True, reason="scheduled_positions_unverified")
    current[0] = datetime(2026, 7, 4, 1, 0, tzinfo=UTC)
    assert not daemon.entry_enabled
    assert daemon.entry_enabled_reason == "scheduled_positions_unverified"


async def test_market_invalidation_fences_prefetch_and_uses_shared_provider_capability():
    from crypto_momentum_lab.live_rollout.context_prefetch import PrefetchedContext

    calls = []
    fresh = _runtime_context()

    class Provider:
        generation = 0

        def is_current(self, context):
            return True

        async def __call__(self, state):
            calls.append("load")
            return fresh

        def invalidate(self, event=None):
            self.generation += 1
            calls.append("invalidate")

    daemon = _daemon(exchange=PlanAwareExchange(), context_provider=Provider())
    generation = daemon._context_runtime.generation
    state = await anext(_states())
    prefetched = PrefetchedContext(
        state=state,
        generation=generation,
        received_at=fresh.now,
        context=_runtime_context(),
        error=None,
    )
    daemon._context_runtime.invalidate()
    assert daemon._context_runtime.generation == generation + 1
    admitted = await daemon._context_runtime.prepare(prefetched)
    assert admitted.error is None
    assert admitted.context is fresh
    assert calls == ["invalidate", "load"]


def test_operator_can_pause_and_resume_exits():
    daemon = _daemon(exchange=PlanAwareExchange())
    assert daemon.exit_enabled
    daemon.set_exit_enabled(False, reason="operator_pause")
    assert not daemon.exit_enabled
    daemon.set_exit_enabled(True, reason="operator_resume")
    assert daemon.exit_enabled
    with pytest.raises(TypeError, match="enabled must be a bool"):
        daemon.set_exit_enabled("yes", reason="invalid")


async def test_exit_exception_does_not_stop_later_market_states():
    failed = asyncio.Event()

    class ExitManager(LiveExitManager):
        calls = 0

        async def requests_for_state(self, state, positions):
            self.calls += 1
            if self.calls == 1:
                failed.set()
                raise RuntimeError("one exit observation failed")
            return ()

    manager = ExitManager(
        config=LiveExitConfig(
            account_label="primary",
            candle_grace_decision_profit_pct=Decimal("0"),
            run_id="run-1",
            strategy_name="compression_breakout",
            strategy_version="v0",
            strategy_config_hash="a" * 64,
            policy=PositionExitPolicy(max_holding_seconds=30),
        )
    )
    exchange = PlanAwareExchange()
    daemon = _daemon(exchange=exchange, exit_manager=manager)

    async def states():
        first = _state()
        yield first
        await asyncio.wait_for(failed.wait(), timeout=1)
        yield replace(
            first,
            bucket_start=first.bucket_start + timedelta(seconds=15),
            bucket_end=first.bucket_end + timedelta(seconds=15),
        )

    result = await daemon.run(states())
    assert result.processed_state_count == 2
    assert result.halt_reason is None
    assert manager.calls == 2


async def test_orphan_exit_cleanup_is_enqueued_without_foreground_cancel():
    from crypto_momentum_lab.domain.execution.order_read_models import (
        PersistedExchangeOrder,
    )
    from tests.unit.execution_account.orders.test_state_machine import _plan

    plan = replace(
        _plan(),
        reduce_only=True,
        side="SELL",
        order_type="LIMIT",
        price=Decimal("30000"),
    )
    requested = []
    exchange = PlanAwareExchange()

    async def context(state):
        return replace(
            _runtime_context(),
            unresolved_orders=(
                PersistedExchangeOrder(
                    plan,
                    ExchangeOrderState.ACKNOWLEDGED,
                    "123",
                    NOW,
                ),
            ),
        )

    daemon = _daemon(
        exchange=exchange,
        context_provider=context,
        request_order_cleanup=requested.append,
    )
    result = await daemon.run(_states())
    assert result.halt_reason is None
    assert requested == [(plan,)]
    assert "cancel" not in exchange.calls
