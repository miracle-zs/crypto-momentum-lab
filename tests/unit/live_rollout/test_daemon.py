import asyncio
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

from sqlalchemy.exc import OperationalError

from crypto_momentum_lab.domain.execution import (
    ExchangeOrderEvent,
    ExchangeOrderSnapshot,
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.risk import RiskEvaluation
from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    StrategyCheckpoint,
    StrategyDecision,
)
from crypto_momentum_lab.execution_account.orders.quantization import (
    SymbolTradingRules,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    ExchangeCancellationUnknownError,
    ExchangeOrderAlreadyAbsentError,
    ExchangeSubmissionTimeoutError,
    OrderExecutionStateMachine,
    SubmitPolicy,
)
from crypto_momentum_lab.live_rollout.closed_candle_feed import (
    ClosedCandle15mEvent,
)
from crypto_momentum_lab.live_rollout.daemon import (
    LiveDaemonConfig,
    LiveDaemonRuntimeContext,
    LiveEntryFilterContext,
    LiveStrategyDaemon,
    _is_transient_live_gate,
    _live_entry_candidate_passes,
)
from crypto_momentum_lab.live_rollout.exits import (
    LiveExitCancellationRequest,
    LiveExitConfig,
    LiveExitManager,
    LiveExitOrderRequest,
    ManagedLivePosition,
)
from crypto_momentum_lab.live_rollout.limits import FixedLiveLimits
from crypto_momentum_lab.persistence.postgres.order_repository import (
    PersistedExchangeOrder,
)
from crypto_momentum_lab.risk.gateway import RiskGateway
from crypto_momentum_lab.strategy_runner.position_exit import (
    ClosedCandle15m,
    PositionExitMode,
    PositionExitPolicy,
)
from tests.unit.execution_account.orders.test_state_machine import (
    FakeExchange,
    FakeOrderRepository,
    _snapshot,
)
from tests.unit.live_rollout.test_gates import _context as gate_context
from tests.unit.shadow_operation.test_service import (
    FakeStrategy,
    _intent,
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


async def test_live_daemon_caps_entry_limit_at_lower_ask_when_ask_is_below_close(
) -> None:
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


async def test_live_signal_record_includes_universe_and_effective_entry_context(
) -> None:
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
    effective = recorder.decision_filter_context[
        "effective_entry_candidates"
    ]["candidate-1"]
    assert effective["original_entry_type"] == "market"
    assert effective["effective_entry_type"] == "limit"
    assert effective["effective_limit_price"] == Decimal("30000")
    assert (
        effective["effective_limit_price_source"]
        == "min(state.last_ask_price,state.close_price)"
    )
    assert effective["effective_expires_at"] == NOW + timedelta(minutes=15)


async def test_live_signal_record_can_compare_entry_policy_without_submitting_change(
) -> None:
    recorder = RecordingSignalRecorder()
    exchange = PlanAwareExchange()
    daemon = _daemon(
        exchange=exchange,
        signal_recorder=recorder,
        entry_policy_compare_only=True,
    )

    result = await daemon.run(_states())

    assert result.halt_reason is None
    assert result.submitted_order_count == 1
    assert exchange.calls == ["submit"]
    assert recorder.decision_filter_context is not None
    assert recorder.decision_filter_context["entry_policy_compare_only"] is True
    comparisons = recorder.decision_filter_context[
        "entry_policy_comparisons"
    ]
    assert len(comparisons) == 1
    assert comparisons[0]["matched"] is True
    assert recorder.decision_filter_context[
        "entry_policy_comparison_summary"
    ] == {
        "candidates": 1,
        "matched": 1,
        "mismatched": 0,
        "legacy_eligible": 1,
        "policy_eligible": 1,
        "reduce_only_skipped": 0,
        "policy_reasons": {},
        "mismatch_reasons": {},
    }


async def test_live_daemon_does_not_submit_expired_entry_candidate() -> None:
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
    assert daemon._pending_entry_plans

    daemon.observe_entry_order_event(
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

    assert daemon._pending_entry_plans == {}


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


async def test_live_daemon_accepts_entry_inside_top100_and_above_both_emas() -> None:
    exchange = PlanAwareExchange()

    async def load_symbols(_observed_at: datetime) -> frozenset[str]:
        return frozenset({"BTCUSDT"})

    async def load_context(_state: MarketState15s) -> LiveEntryFilterContext:
        return LiveEntryFilterContext(
            entry_price=Decimal("30001"),
            ema5=Decimal("30000"),
            ema10=Decimal("29999"),
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
    assert result.approved_intent_count == 1
    assert result.submitted_order_count == 1
    assert exchange.calls == ["submit"]


def test_live_entry_ema_filter_is_strictly_above() -> None:
    candidate = _intent()

    assert not _live_entry_candidate_passes(
        candidate,
        context=LiveEntryFilterContext(
            entry_price=Decimal("30000"),
            ema5=Decimal("30000"),
            ema10=Decimal("29999"),
        ),
        require_price_above_ema5=True,
        require_price_above_ema10=True,
    )


async def test_live_daemon_blocks_before_submit_when_gate_changes() -> None:
    exchange = FakeExchange(submit_result=_snapshot(ExchangeOrderState.ACKNOWLEDGED))

    async def blocked_context(state: object) -> LiveDaemonRuntimeContext:
        del state
        return replace(_runtime_context(), active_halts=(_halt(),))

    daemon = _daemon(exchange=exchange, context_provider=blocked_context)

    result = await daemon.run(_states())

    assert result.halt_reason is not None
    assert result.halt_reason.startswith("live_gate:")
    assert exchange.calls == []


async def test_live_daemon_survives_temporary_lease_gate_block() -> None:
    exchange = PlanAwareExchange()
    calls = 0

    async def flaky_context(state: object) -> LiveDaemonRuntimeContext:
        nonlocal calls
        del state
        calls += 1
        if calls == 1:
            return replace(
                _runtime_context(),
                active_lease=None,
                gate_context=replace(gate_context(), active_lease=None),
            )
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
    assert exchange.calls == ["submit"]


def test_unresolved_order_gate_is_transient_until_reconciliation_finishes() -> None:
    assert _is_transient_live_gate(("unresolved_order_uncertainty",))
    assert not _is_transient_live_gate(("active_risk_halt",))


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
                    unresolved_order_states=(uncertain,),
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
                unresolved_order_states=(uncertain,),
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
        side="long",
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
                run_id="run-1",
                strategy_name="compression_breakout",
                strategy_version="v0",
                strategy_config_hash="a" * 64,
                policy=PositionExitPolicy(),
            )
        ),
    )

    failure = await daemon.process_account_event(_state())

    assert failure is None
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
        side="long",
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

    approved, submitted, failure = await daemon._process_exit_requests(
        (
            LiveExitOrderRequest(
                candidate=candidate,
                quantity=Decimal("0.001"),
            ),
        ),
        state=_state(),
        context=context,
    )

    assert failure is None
    assert approved == 2
    assert submitted == 2
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

    approved, submitted, failure = await daemon._process_exit_requests(
        (
            LiveExitOrderRequest(
                candidate=candidate,
                quantity=Decimal("0.001"),
            ),
        ),
        state=_state(),
        context=_runtime_context(),
    )

    assert failure is None
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

    approved, submitted, failure = await daemon._process_exit_requests(
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

    assert failure is None
    assert approved == 1
    assert submitted == 1
    assert len(exchange.plans) == 1
    assert exchange.plans[0].order_type == "MARKET"


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
        side="long",
        position_side=FuturesPositionSide.LONG,
        quantity=Decimal("0.001"),
        entry_price=Decimal("30000"),
        opened_at=NOW - timedelta(minutes=30),
        recovery_order_client_id=recovery_plan.client_order_id,
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
        side="long",
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
        max_gross_exposure=Decimal("100"),
    )

    result = await daemon.run(states())

    assert result.processed_state_count == 2
    assert result.approved_intent_count == 2
    assert result.halt_reason is None
    assert exchange.calls == ["submit", "submit"]


async def test_live_daemon_reconciles_once_per_market_bucket() -> None:
    exchange = PlanAwareExchange()
    reconcile_calls = 0

    async def reconcile() -> None:
        nonlocal reconcile_calls
        reconcile_calls += 1

    async def states() -> AsyncIterator:
        first = _state()
        yield first
        yield replace(
            first,
            bucket_end=first.bucket_end,
        )

    daemon = _daemon(exchange=exchange, reconcile_orders=reconcile)

    result = await daemon.run(states())

    assert result.halt_reason is None
    assert reconcile_calls == 1


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


async def test_account_event_lane_submits_exit_without_entry_decision() -> None:
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
    )

    failure = await daemon.process_account_event(_state())

    assert failure is None
    assert exchange.calls == ["submit"]
    assert exchange.plans[0].reduce_only is True


async def test_account_event_exit_does_not_wait_for_slow_market_exit() -> None:
    exchange = PlanAwareExchange()
    market_exit_started = asyncio.Event()
    release_market_exit = asyncio.Event()
    position_opened_at = datetime(2026, 7, 3, 23, 59, tzinfo=UTC)
    positions = (
        ManagedLivePosition(
            symbol="ETHUSDT",
            side="long",
            position_side=FuturesPositionSide.LONG,
            quantity=Decimal("0.001"),
            entry_price=Decimal("31000"),
            opened_at=position_opened_at,
        ),
        ManagedLivePosition(
            symbol="BTCUSDT",
            side="long",
            position_side=FuturesPositionSide.LONG,
            quantity=Decimal("0.001"),
            entry_price=Decimal("31000"),
            opened_at=position_opened_at,
        ),
    )

    class SlowCandleLoader:
        async def load_closed_candles(
            self,
            *,
            symbol: str,
            start: datetime,
            end: datetime,
        ) -> tuple[ClosedCandle15m, ...]:
            del start, end
            if symbol == "ETHUSDT":
                market_exit_started.set()
                await release_market_exit.wait()
                return ()
            return (
                ClosedCandle15m(
                    symbol="BTCUSDT",
                    candle_start=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
                    candle_end=datetime(2026, 7, 4, 0, 15, tzinfo=UTC),
                    open_price=Decimal("31000"),
                    close_price=Decimal("30000"),
                ),
            )

    async def position_context(state: object) -> LiveDaemonRuntimeContext:
        del state
        return replace(
            _runtime_context(),
            open_position_symbols=frozenset({"BTCUSDT", "ETHUSDT"}),
            managed_positions=positions,
        )

    base_state = replace(
        _state(),
        bucket_end=datetime(2026, 7, 4, 0, 15, 15, tzinfo=UTC),
    )
    market_state = replace(base_state, symbol="ETHUSDT")
    account_state = replace(base_state, symbol="BTCUSDT")
    daemon = _daemon(
        exchange=exchange,
        context_provider=position_context,
        exit_manager=LiveExitManager(
            config=LiveExitConfig(
                run_id="run-1",
                strategy_name="compression_breakout",
                strategy_version="v0",
                strategy_config_hash="a" * 64,
                policy=PositionExitPolicy(mode=PositionExitMode.CANDLE_15M),
            ),
            candle_loader=SlowCandleLoader(),
        ),
    )
    daemon.set_entry_enabled(False, reason="test_only")

    async def states() -> AsyncIterator[MarketState15s]:
        yield market_state

    run_task = asyncio.create_task(daemon.run(states()))
    await asyncio.wait_for(market_exit_started.wait(), timeout=1)
    account_task = asyncio.create_task(daemon.process_account_event(account_state))
    try:
        account_failure = await asyncio.wait_for(
            asyncio.shield(account_task),
            timeout=0.05,
        )
    finally:
        release_market_exit.set()

    assert account_failure is None
    assert await account_task is None
    await run_task
    assert exchange.calls == ["submit"]
    assert exchange.plans[0].symbol == "BTCUSDT"
    assert exchange.plans[0].reduce_only is True


async def test_market_exit_workers_do_not_wait_for_another_symbol() -> None:
    btc_exit_submitted = asyncio.Event()
    market_exit_started = asyncio.Event()
    release_market_exit = asyncio.Event()
    position_opened_at = datetime(2026, 7, 3, 23, 59, tzinfo=UTC)
    positions = (
        ManagedLivePosition(
            symbol="ETHUSDT",
            side="long",
            position_side=FuturesPositionSide.LONG,
            quantity=Decimal("0.001"),
            entry_price=Decimal("31000"),
            opened_at=position_opened_at,
        ),
        ManagedLivePosition(
            symbol="BTCUSDT",
            side="long",
            position_side=FuturesPositionSide.LONG,
            quantity=Decimal("0.001"),
            entry_price=Decimal("31000"),
            opened_at=position_opened_at,
        ),
    )

    class RecordingExchange(PlanAwareExchange):
        async def submit_order(
            self,
            plan: OrderExecutionPlan,
        ) -> ExchangeOrderSnapshot:
            result = await super().submit_order(plan)
            if plan.symbol == "BTCUSDT":
                btc_exit_submitted.set()
            return result

    class SlowCandleLoader:
        async def load_closed_candles(
            self,
            *,
            symbol: str,
            start: datetime,
            end: datetime,
        ) -> tuple[ClosedCandle15m, ...]:
            del start, end
            if symbol == "ETHUSDT":
                market_exit_started.set()
                await release_market_exit.wait()
                return ()
            return (
                ClosedCandle15m(
                    symbol="BTCUSDT",
                    candle_start=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
                    candle_end=datetime(2026, 7, 4, 0, 15, tzinfo=UTC),
                    open_price=Decimal("31000"),
                    close_price=Decimal("30000"),
                ),
            )

    exchange = RecordingExchange()

    async def position_context(state: object) -> LiveDaemonRuntimeContext:
        del state
        return replace(
            _runtime_context(),
            open_position_symbols=frozenset({"BTCUSDT", "ETHUSDT"}),
            managed_positions=positions,
        )

    base_state = replace(
        _state(),
        bucket_end=datetime(2026, 7, 4, 0, 15, 15, tzinfo=UTC),
    )
    market_state = replace(base_state, symbol="ETHUSDT")
    second_market_state = replace(base_state, symbol="BTCUSDT")
    daemon = _daemon(
        exchange=exchange,
        context_provider=position_context,
        exit_manager=LiveExitManager(
            config=LiveExitConfig(
                run_id="run-1",
                strategy_name="compression_breakout",
                strategy_version="v0",
                strategy_config_hash="a" * 64,
                policy=PositionExitPolicy(mode=PositionExitMode.CANDLE_15M),
            ),
            candle_loader=SlowCandleLoader(),
        ),
    )
    daemon.set_entry_enabled(False, reason="test_only")

    async def states() -> AsyncIterator[MarketState15s]:
        yield market_state
        yield second_market_state

    run_task = asyncio.create_task(daemon.run(states()))
    await asyncio.wait_for(market_exit_started.wait(), timeout=1)
    try:
        await asyncio.wait_for(btc_exit_submitted.wait(), timeout=0.2)
    finally:
        release_market_exit.set()
    result = await run_task

    assert result.halt_reason is None
    assert exchange.calls == ["submit"]
    assert exchange.plans[0].symbol == "BTCUSDT"


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
        self.checkpoint_failures_remaining = 0

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


def _daemon(
    *,
    exchange,
    strategy=None,
    context_provider=None,
    repository: FakeLiveRepository | None = None,
    checkpoint_every_states: int = 1,
    exit_manager: LiveExitManager | None = None,
    hedge_mode: bool = False,
    entry_symbol_loader=None,
    entry_filter_context_loader=None,
    entry_universe_context_provider=None,
    entry_policy_compare_only: bool = False,
    signal_recorder=None,
    require_price_above_ema5: bool = False,
    require_price_above_ema10: bool = False,
    entry_order_type: EntryType = EntryType.LIMIT,
    max_gross_exposure: Decimal = Decimal("25"),
    reconcile_orders=None,
    exit_recovery_client=None,
    clock=None,
) -> LiveStrategyDaemon:
    order_repository = FakeOrderRepository()
    machine = OrderExecutionStateMachine(
        exchange=exchange,
        repository=order_repository,
        submit_policy=SubmitPolicy.LIVE_SUBMIT,
        live_submit_enabled=True,
        clock=lambda: NOW,
        reconciliation_retry_delays=(0.0, 0.0, 0.0, 0.0),
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
            max_gross_exposure=max_gross_exposure,
        ),
        repository=repository or FakeLiveRepository(),
        state_machine=machine,
        context_provider=context_provider or default_context,
        signal_recorder=signal_recorder,
        config=LiveDaemonConfig(
            run_id="run-1",
            resize_tolerance=Decimal("0.20"),
            checkpoint_every_states=checkpoint_every_states,
            hedge_mode=hedge_mode,
            entry_symbol_loader=entry_symbol_loader,
            require_price_above_ema5=require_price_above_ema5,
            require_price_above_ema10=require_price_above_ema10,
            entry_filter_context_loader=entry_filter_context_loader,
            entry_universe_context_provider=entry_universe_context_provider,
            entry_policy_compare_only=entry_policy_compare_only,
            entry_order_type=entry_order_type,
        ),
        exit_manager=exit_manager,
        exit_recovery_client=exit_recovery_client,
        reconcile_orders=reconcile_orders,
        clock=clock or (lambda: NOW),
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
