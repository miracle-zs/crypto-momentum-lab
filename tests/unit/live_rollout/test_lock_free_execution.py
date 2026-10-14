from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from crypto_momentum_lab.domain.account import AccountFillEvent, AccountPositionSnapshot
from crypto_momentum_lab.domain.decision.decision_engine import PolicyState
from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
from crypto_momentum_lab.domain.execution.exchange_contract import (
    ExchangeSubmissionTimeoutError,
)
from crypto_momentum_lab.domain.execution.execution_action_models import (
    Accepted,
    Blocked,
    ExecutionRequest,
    StaleView,
)
from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.order_result import OrderExecutionResult
from crypto_momentum_lab.domain.execution.order_rules import SymbolTradingRules
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderSnapshot,
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderSubmissionPreparation,
    PreparedOrderSubmission,
)
from crypto_momentum_lab.domain.execution.ports import (
    DurableExecutionPositionState,
    ExecutionHeadSnapshot,
    ExecutionTransactionPort,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    AccountFillLoadProvenance,
    FactCoverageInterval,
    FactCoverageStatus,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.position_repair_models import (
    PositionRepairFacts,
    PositionRepairReceipt,
    PositionRepairRequest,
)
from crypto_momentum_lab.domain.execution.recovery_models import (
    AccountFacts,
    DurableJournalCut,
)
from crypto_momentum_lab.domain.execution.snapshot_encoding import (
    stable_snapshot_anchor_id,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.market.revision_models import (
    MarketRevisionRef,
    MarketVisibilityMode,
)
from crypto_momentum_lab.domain.risk import RiskDecision, RiskEvaluation, RiskGateway
from crypto_momentum_lab.domain.risk.limits import FixedLiveLimits
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide
from crypto_momentum_lab.execution_account.binance.client import (
    BinanceUsdMTradeClient,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionStateMachine,
)
from crypto_momentum_lab.live_rollout.decision_facts import (
    DecisionCommit,
    DecisionCommitReceipt,
    DecisionInput,
    DecisionResult,
    DecisionTrace,
    LiveDecisionFactSource,
)
from crypto_momentum_lab.live_rollout.exit_processor import (
    ExitProcessorConfig,
    LiveExitProcessor,
)
from crypto_momentum_lab.live_rollout.exits import (
    LiveExitOrderRequest,
    ManagedLivePosition,
    ManagedLivePositionBatch,
)
from crypto_momentum_lab.live_rollout.pending_entries import LivePendingEntryRegistry
from crypto_momentum_lab.live_rollout.position_lifecycle import (
    PositionLifecycleLocks,
    live_symbol_position_key,
)
from crypto_momentum_lab.live_rollout.submission import (
    LiveCandidateSubmission,
    LiveSubmissionConfig,
)
from tests.fixtures.live_market import _intent, _state
from tests.unit.execution_account.orders.test_coordinator import (
    OrderExecutionCoordinator,
)
from tests.unit.live_rollout.test_daemon import _runtime_context

NOW = datetime(2026, 7, 4, 0, 0, 20, tzinfo=UTC)


def _trading_rules(symbol: str = "BTCUSDT") -> SymbolTradingRules:
    return SymbolTradingRules(
        symbol=symbol,
        tick_size=Decimal("1"),
        step_size=Decimal("0.0001"),
        min_quantity=Decimal("0.0001"),
        max_quantity=Decimal("100"),
        min_notional=Decimal("5"),
    )


def _limits() -> FixedLiveLimits:
    return FixedLiveLimits(
        notional_cap=Decimal("100000"),
        max_open_positions=10,
        max_daily_loss=Decimal("10000"),
        max_gross_exposure=Decimal("200000"),
    )


class MemoryEventRepo:
    def __init__(self) -> None:
        self.events: list[ExchangeOrderEvent] = []

    async def record_order_observation(
        self, event: ExchangeOrderEvent, fills: tuple[Any, ...] = ()
    ) -> bool:
        self.events.append(event)
        return True


class RecordingPreparedRepo:
    def __init__(self) -> None:
        self.prepare_calls: list[dict[str, Any]] = []

    async def save_approved_intent(self, intent: Any, evaluation: Any) -> None:
        pass

    async def prepare_submission_in_session(
        self, session: Any, **kwargs: Any
    ) -> PreparedOrderSubmission:
        self.prepare_calls.append(kwargs)
        plan = cast(OrderExecutionPlan, kwargs["plan"])
        return PreparedOrderSubmission(
            plan=plan,
            submitting_event=ExchangeOrderEvent(
                event_id=f"evt-{plan.client_order_id}",
                client_order_id=plan.client_order_id,
                state=ExchangeOrderState.SUBMITTING,
                occurred_at=NOW,
                exchange_order_id=None,
                details={},
            ),
        )


class A3aExchange:
    def __init__(
        self, recovery_gate: asyncio.Event, exit_reached: asyncio.Event
    ) -> None:
        self.recovery_gate = recovery_gate
        self.exit_reached = exit_reached
        self.recovery_started = asyncio.Event()
        self.entry_posts = 0
        self.exit_posts = 0
        self.calls: list[str] = []

    def set_exchange_boundary_callbacks(
        self, on_request: Any = None, on_response: Any = None
    ) -> None:
        pass

    async def submit_order(self, plan: OrderExecutionPlan) -> ExchangeOrderSnapshot:
        if not plan.reduce_only:
            self.entry_posts += 1
            self.calls.append(f"submit_entry:{plan.client_order_id}")
            raise ExchangeSubmissionTimeoutError("entry POST timeout")
        self.exit_posts += 1
        self.calls.append(f"submit_exit:{plan.client_order_id}")
        self.exit_reached.set()
        return ExchangeOrderSnapshot(
            client_order_id=plan.client_order_id,
            exchange_order_id="ex-exit-1",
            state=ExchangeOrderState.ACKNOWLEDGED,
            observed_at=NOW,
            executed_quantity=Decimal("0"),
            average_price=Decimal("0"),
        )

    async def query_order_by_client_id(
        self, symbol: str, client_order_id: str
    ) -> ExchangeOrderSnapshot | None:
        self.calls.append(f"query_order:{client_order_id}")
        self.recovery_started.set()
        await asyncio.wait_for(self.recovery_gate.wait(), timeout=5.0)
        return None


@pytest.mark.asyncio
async def test_a3a_entry_timeout_releases_key_worker_and_exit_posts_before_get_releases() -> (
    None
):
    recovery_gate = asyncio.Event()
    exit_reached = asyncio.Event()
    exchange = A3aExchange(recovery_gate, exit_reached)
    event_repo = MemoryEventRepo()
    prep_repo = RecordingPreparedRepo()

    state_machine = OrderExecutionStateMachine(
        exchange=exchange,
        event_repository=event_repo,
        live_submit_enabled=True,
        clock=lambda: NOW,
        retry_lookup_on_submit_timeout=False,
        reconciliation_retry_delays=(0.01, 0.01),
    )
    coordinator = OrderExecutionCoordinator(
        environment="live",
        backend=state_machine,
        account_label="primary",
        execution_book=ExecutionBook(),
    )
    coordinator.configure_submission(prep_repo, clock=lambda: NOW)

    pending_registry = LivePendingEntryRegistry(clock=lambda: NOW)
    position_locks = PositionLifecycleLocks()

    config = LiveSubmissionConfig(
        run_id="run-1",
        account_label="primary",
        resize_tolerance=Decimal("0.05"),
        hedge_mode=False,
        entry_order_type=EntryType.MARKET,
        entry_limit_ttl_seconds=605,
    )
    submission = LiveCandidateSubmission(
        risk_gateway=RiskGateway(limits=_limits()),
        state_machine=coordinator,
        config=config,
        clock=lambda: NOW,
        pending_entry_reservation=pending_registry.reservation,
        pending_entry_plans=pending_registry.snapshot,
        pending_entry_uncertainty=pending_registry.has_uncertain_entry,
        remember_pending_entry=pending_registry.remember,
        record_signal_candidate=lambda *args, **kwargs: None,
        position_locks=position_locks,
    )

    pos_key = PositionKey("live", "primary", "BTCUSDT", FuturesPositionSide.LONG)
    batch = ManagedLivePositionBatch(
        batch_id="batch-1",
        quantity=Decimal("0.001"),
        entry_price=Decimal("50000"),
        opened_at=NOW,
    )
    managed_pos = ManagedLivePosition(
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.LONG,
        quantity=Decimal("0.001"),
        entry_price=Decimal("50000"),
        opened_at=NOW,
        batches=(batch,),
    )
    context = replace(
        _runtime_context(),
        managed_positions=(managed_pos,),
        risk_config=replace(
            _runtime_context().risk_config,
            max_open_positions=10,
            max_order_notional=Decimal("10000"),
            max_gross_notional=Decimal("100000"),
        ),
        trading_rules={"BTCUSDT": _trading_rules()},
    )

    # 1. Submit entry for BTCUSDT LONG -> POST times out.
    entry_cand = replace(
        _intent(),
        candidate_id="entry-cand-1",
        symbol="BTCUSDT",
        reduce_only=False,
        features={"position_side": "LONG"},
    )
    entry_result = await submission.execute(
        entry_cand,
        requested_quantity=Decimal("0.0005"),
        state=_state(),
        context=context,
        reference_price=Decimal("50000"),
    )
    assert entry_result is not None
    assert entry_result.state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
    assert exchange.entry_posts == 1
    assert pending_registry.has_uncertain_entry("BTCUSDT")
    notional, symbols = pending_registry.reservation(())
    assert notional > Decimal("0")
    assert "BTCUSDT" in symbols

    # 2. Key worker is freed immediately; recovery GET starts in background.
    recovery_task = asyncio.create_task(
        state_machine.reconcile_order(entry_result.plan)
    )
    await asyncio.wait_for(exchange.recovery_started.wait(), timeout=2.0)

    # 3. Verify application lock is acquirable (not locked).
    assert not position_locks.is_locked(pos_key)

    # 4. Submit exit for SAME symbol and position side while recovery GET hangs.
    exit_cand = replace(
        _intent(),
        candidate_id="exit-cand-1",
        symbol="BTCUSDT",
        reduce_only=True,
        entry_type=EntryType.MARKET,
        features={"position_side": "LONG"},
    )
    exit_result = await submission.execute(
        exit_cand,
        requested_quantity=Decimal("0.001"),
        state=_state(),
        context=context,
        reference_price=Decimal("50000"),
    )

    # 5. Assert exit POST reached mock BEFORE recovery GET releases!
    assert exit_reached.is_set()
    assert exchange.exit_posts == 1
    assert exit_result is not None
    assert exit_result.state is ExchangeOrderState.ACKNOWLEDGED
    assert exit_result.plan.quantity <= managed_pos.quantity

    # Release recovery GET.
    recovery_gate.set()
    await recovery_task
    await coordinator.aclose()


@pytest.mark.asyncio
async def test_a3b_all_io_entrypoints_assert_no_application_locks_held() -> None:
    position_locks = PositionLifecycleLocks()
    pos_key = PositionKey("live", "primary", "BTCUSDT", FuturesPositionSide.LONG)

    class LockCheckingExchange:
        def __init__(self) -> None:
            self.post_count = 0

        def set_exchange_boundary_callbacks(
            self, on_request: Any = None, on_response: Any = None
        ) -> None:
            pass

        async def submit_order(self, plan: OrderExecutionPlan) -> ExchangeOrderSnapshot:
            assert not position_locks.is_locked(pos_key)
            self.post_count += 1
            return ExchangeOrderSnapshot(
                client_order_id=plan.client_order_id,
                exchange_order_id="ex-1",
                state=ExchangeOrderState.ACKNOWLEDGED,
                observed_at=NOW,
                executed_quantity=Decimal("0"),
                average_price=Decimal("0"),
            )

        async def query_order_by_client_id(
            self, symbol: str, client_order_id: str
        ) -> ExchangeOrderSnapshot | None:
            assert not position_locks.is_locked(pos_key)
            return None

    exchange = LockCheckingExchange()
    event_repo = MemoryEventRepo()
    prep_repo = RecordingPreparedRepo()

    state_machine = OrderExecutionStateMachine(
        exchange=exchange,
        event_repository=event_repo,
        live_submit_enabled=True,
        clock=lambda: NOW,
    )
    coordinator = OrderExecutionCoordinator(
        environment="live",
        backend=state_machine,
        account_label="primary",
        execution_book=ExecutionBook(),
    )
    coordinator.configure_submission(prep_repo, clock=lambda: NOW)

    config = LiveSubmissionConfig(
        run_id="run-1",
        account_label="primary",
        resize_tolerance=Decimal("0.05"),
        hedge_mode=False,
        entry_order_type=EntryType.MARKET,
        entry_limit_ttl_seconds=605,
    )
    submission = LiveCandidateSubmission(
        risk_gateway=RiskGateway(limits=_limits()),
        state_machine=coordinator,
        config=config,
        clock=lambda: NOW,
        pending_entry_reservation=lambda *a, **kw: (Decimal("0"), frozenset()),
        pending_entry_plans=lambda: (),
        pending_entry_uncertainty=lambda cid: False,
        remember_pending_entry=lambda plan, res: None,
        record_signal_candidate=lambda *args, **kwargs: None,
        position_locks=position_locks,
    )

    managed_pos = ManagedLivePosition(
        symbol="BTCUSDT",
        side=StrategySide.LONG,
        position_side=FuturesPositionSide.LONG,
        quantity=Decimal("0.001"),
        entry_price=Decimal("50000"),
        opened_at=NOW,
        batches=(
            ManagedLivePositionBatch(
                batch_id="b-1",
                quantity=Decimal("0.001"),
                entry_price=Decimal("50000"),
                opened_at=NOW,
            ),
        ),
    )
    context = replace(
        _runtime_context(),
        managed_positions=(managed_pos,),
        risk_config=replace(
            _runtime_context().risk_config,
            max_open_positions=10,
            max_order_notional=Decimal("10000"),
            max_gross_notional=Decimal("100000"),
        ),
        trading_rules={"BTCUSDT": _trading_rules()},
    )

    # 1. Entry execution checks position_locks is NOT held during HTTP POST.
    entry_cand = replace(
        _intent(),
        candidate_id="cand-entry",
        symbol="BTCUSDT",
        reduce_only=False,
        features={"position_side": "LONG"},
    )
    await submission.execute(
        entry_cand,
        requested_quantity=Decimal("0.0005"),
        state=_state(),
        context=context,
        reference_price=Decimal("50000"),
    )
    assert exchange.post_count == 1

    # 2. Exit execution checks position_locks is NOT held during HTTP POST.
    exit_cand = replace(
        _intent(),
        candidate_id="cand-exit",
        symbol="BTCUSDT",
        reduce_only=True,
        entry_type=EntryType.MARKET,
        features={"position_side": "LONG"},
    )
    await submission.execute(
        exit_cand,
        requested_quantity=Decimal("0.001"),
        state=_state(),
        context=context,
        reference_price=Decimal("50000"),
    )
    assert exchange.post_count == 2

    # 3. Decision exit: verify UoW commit runs without commit_lock.
    class LockCheckingUoW:
        def __init__(self, fact_source: LiveDecisionFactSource) -> None:
            self._source = fact_source

        async def commit_decision(
            self, commit: DecisionCommit
        ) -> DecisionCommitReceipt:
            assert not self._source._commit_lock.locked()
            return DecisionCommitReceipt(
                decision_id=commit.trace.decision_id,
                policy_key=commit.policy_key,
                prior_state_digest="prior-digest",
                next_state_digest="new-digest-32bytes-padding-ok!!",
                policy_revision=commit.expected_policy_revision + 1,
                durable_at=NOW,
                is_replay=False,
            )

        async def mark_exit_dispatched(self, decision_id: str, command_id: str) -> bool:
            assert not self._source._commit_lock.locked()
            return True

    fact_source = LiveDecisionFactSource(
        account_label="primary",
        strategy_name="orderflow_impulse",
        decision_unit_of_work=None,
        request_exit_recovery=lambda: None,
    )
    mock_uow = LockCheckingUoW(fact_source)
    fact_source._decision_uow = mock_uow  # type: ignore[assignment]

    exit_handler_called = False

    async def mock_decision_exit_handler(
        command: TradeCommand,
    ) -> OrderExecutionResult:
        nonlocal exit_handler_called
        exit_handler_called = True
        assert not fact_source._commit_lock.locked()
        return OrderExecutionResult(
            client_order_id="exit-test-client-id",
            state=ExchangeOrderState.ACKNOWLEDGED,
            exchange_order_id="ex-1",
        )

    fact_source.set_exit_handler(mock_decision_exit_handler)
    fact_source._exit_matches_current_book = AsyncMock(return_value=True)  # type: ignore[method-assign]

    market_ref = MarketRevisionRef(
        scope="live",
        symbol="BTCUSDT",
        interval="15s",
        bucket_start=NOW,
        bucket_end=NOW + timedelta(seconds=15),
        revision_id="rev-1",
        content_hash="hash-1",
        published_at=NOW + timedelta(seconds=15),
        source_epoch="epoch-1",
        visibility_mode=MarketVisibilityMode.DECISION_VISIBLE,
        observed_at=NOW,
    )
    trace = DecisionTrace(
        decision_id="dec-1",
        strategy_name="orderflow_impulse",
        account_label="primary",
        decision_time=NOW,
        evaluated_market_refs=(market_ref,),
        intent_produced=False,
        frame_digest="digest-1234",
    )
    exit_cmd = TradeCommand(
        command_id="cmd-exit-decision-check",
        position_key=pos_key,
        command_type=TradeCommandType.EXIT,
        side=StrategySide.LONG,
        order_type=EntryType.MARKET,
        requested_quantity=Decimal("0.001"),
        reduce_only=True,
        created_at=NOW,
        expected_projection_version="1",
    )
    result = DecisionResult(
        decision_id="dec-1",
        input_hash="hash-1",
        intent=None,
        exit_command=exit_cmd,
        next_policy_state=PolicyState(),
        rejection_reason=None,
        evaluated_at=NOW,
    )
    receipt = await fact_source.commit_decision(
        trace, result, cast(DecisionInput, None)
    )
    assert receipt.policy_revision == 1
    assert exit_handler_called

    # 3b. Actual Decision Exit: verify position_locks is NOT held during exit execution
    lock_key = live_symbol_position_key("primary", "BTCUSDT")

    class MockDecisionExitManager:
        uses_market_state_exit = True

        async def requests_for_state(
            self, _state: Any, _positions: Any
        ) -> tuple[LiveExitOrderRequest, ...]:
            assert position_locks.is_locked(lock_key)
            return (
                LiveExitOrderRequest(
                    candidate=exit_cand,
                    quantity=Decimal("0.001"),
                ),
            )

    class LockCheckingExitSubmission:
        def __init__(self, locks: PositionLifecycleLocks) -> None:
            self.locks = locks
            self.executed = False

        async def execute(self, *args: Any, **kwargs: Any) -> Any:
            assert not self.locks.is_locked(lock_key)
            self.executed = True
            return OrderExecutionResult(
                client_order_id="exit-test-req",
                state=ExchangeOrderState.ACKNOWLEDGED,
                exchange_order_id="ex-1",
            )

    exit_submission = LockCheckingExitSubmission(position_locks)
    exit_processor = LiveExitProcessor(
        config=ExitProcessorConfig(run_id="run-1"),
        exit_manager=MockDecisionExitManager(),  # type: ignore[arg-type]
        exit_recovery_client=None,
        state_machine=state_machine,
        submission=exit_submission,  # type: ignore[arg-type]
        telemetry=None,
        clock=lambda: NOW,
        is_exit_enabled=lambda: True,
        context_provider=lambda: context,
        apply_context=lambda ctx: None,
        invalidate_context_cache=lambda: None,
        position_locks=position_locks,
        account_label="primary",
        request_recovery=lambda: None,
    )
    outcome = await exit_processor.handle_trigger("state", _state(), context)
    assert exit_submission.executed
    assert outcome.submitted_order_count == 1

    # 4. Binance margin type: verify warmup and confirmation without holding lock
    class MockClient(BinanceUsdMTradeClient):
        def __init__(self) -> None:
            super().__init__(
                api_key="k",
                api_secret="s",
                environment="test",
                account_label="primary",
                margin_type="ISOLATED",
                live_submit_enabled=True,
            )
            self.fetch_called = 0
            self.post_called = 0
            self.current_remote = "CROSSED"

        async def fetch_symbol_margin_type(self, symbol: str) -> str | None:
            assert not self._margin_type_lock.locked()
            self.fetch_called += 1
            return self.current_remote

        async def _signed_post(
            self, path: str, payload: dict[str, Any], priority: int = 1
        ) -> dict[str, Any]:
            assert not self._margin_type_lock.locked()
            self.post_called += 1
            return {}

    client = MockClient()
    # 4a. Confirmation write: remote is CROSSED, desired is ISOLATED
    margin_res = await client._ensure_entry_margin_type("BTCUSDT")
    assert margin_res == "ISOLATED"
    assert client.fetch_called == 1
    assert client.post_called == 1

    # 4b. Pre-warmed cache hit: returns cached margin type without lock/I/O
    margin_res_cached = await client._ensure_entry_margin_type("BTCUSDT")
    assert margin_res_cached == "ISOLATED"
    assert client.fetch_called == 1
    assert client.post_called == 1

    # 5. ExecutionBook UoW database transaction:
    # verify _mutation_lock is NOT held during I/O
    class LockCheckingDurableUoW:
        def __init__(self, book: ExecutionBook) -> None:
            self._book = book
            self.tx_count = 0
            self.load_position_called = False
            self.persisted_heads: dict[str, Any] = {}

        @asynccontextmanager
        async def transaction(self, key: PositionKey, account_scope: str | None = None):
            # Assert application mutation lock is NOT held during database transaction!
            assert not self._book._mutation_lock(key).locked()
            self.tx_count += 1
            tx = MagicMock(spec=ExecutionTransactionPort)
            canon = key.canonical_id

            async def _load_head(key: Any = None, *args: Any, **kwargs: Any) -> Any:
                return self.persisted_heads.get(canon)

            async def _persist_head(key: Any = None, *args: Any, **kwargs: Any) -> int:
                curr = self.persisted_heads.get(canon)
                rev = (curr.revision + 1) if curr else 1
                scope = self._book._stream_scopes.get(canon)
                stream_id = scope.stream_id if scope else "stream-1"
                stream_epoch = scope.stream_epoch if scope else "epoch-1"
                head_mock = MagicMock(
                    revision=rev,
                    stream_id=stream_id,
                    stream_epoch=stream_epoch,
                )
                self.persisted_heads[canon] = head_mock
                return rev

            tx.load_head = AsyncMock(side_effect=_load_head)
            tx.save_reservations = AsyncMock()
            tx.persist_watermark = AsyncMock()
            tx.persist_head = AsyncMock(side_effect=_persist_head)
            tx.record_evidence = AsyncMock(
                return_value=MagicMock(is_replay=False, is_conflict=False)
            )
            tx.record_trade = AsyncMock(return_value=True)
            yield tx

        async def load_position(self, key: PositionKey, as_of: Any = None):
            assert not self._book._mutation_lock(key).locked()
            self.load_position_called = True
            return self.position_state

    uow_checker_book = ExecutionBook()
    uow_checker = LockCheckingDurableUoW(uow_checker_book)
    uow_checker.position_state = None
    uow_checker_book._execution_unit_of_work = uow_checker
    uow_checker_book._stream_scopes[pos_key.canonical_id] = (
        AccountFactStreamScope.for_position_key(
            pos_key, stream_id="stream-1", stream_epoch="epoch-1"
        )
    )
    uow_checker_book._head_revisions[pos_key.canonical_id] = 0

    init_view = uow_checker_book._ensure_book(pos_key).get_view()
    req = ExecutionRequest(
        request_id="req-durable-lock-check",
        scope=ExecutionScope("live", "primary", "BTCUSDT", FuturesPositionSide.LONG),
        strategy_name="test",
        run_id="run-1",
        decision_ref="tp",
        expected_view_token=init_view.projection_version,
        action=TradeCommandType.ENTRY,
        requested_quantity=Decimal("0.001"),
        side=StrategySide.LONG,
    )
    res = await uow_checker_book.act(req)
    assert isinstance(res, Accepted)
    assert uow_checker.tx_count == 1

    # 6. Durable observe & reload_position: verify _mutation_lock not held during I/O
    obs_fill = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id="trade-obs-lock-check",
        order_id="order-obs-lock-check",
        side="BUY",
        price=Decimal("50000"),
        quantity=Decimal("0.001"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=NOW,
        raw_payload={"positionSide": "LONG"},
    )
    obs_evidence = ExecutionEvidence(
        evidence_id="ev-obs-lock-check",
        scope=ExecutionScope("live", "primary", "BTCUSDT", FuturesPositionSide.LONG),
        stream_id="stream-1",
        stream_epoch="epoch-1",
        sequence=1,
        observed_at=NOW,
        fills=(obs_fill,),
    )
    obs_res = await uow_checker_book.observe(obs_evidence)
    assert obs_res is not None
    assert uow_checker.tx_count == 2

    await uow_checker_book.reload_position(pos_key)
    assert uow_checker.load_position_called

    # Reconcile reservation divergence:
    # verify _mutation_lock not held during durable load
    uow_checker_book._recovery_required_commands.add(
        f"reservation_divergence:{pos_key.canonical_id}"
    )
    uow_checker_book._head_expected_reservation_ids[pos_key.canonical_id] = set()
    uow_checker.load_position_called = False
    await uow_checker_book.reconcile_reservation_divergence(pos_key)
    assert uow_checker.load_position_called

    # Repair position:
    # verify _mutation_lock not held during fact load, persist and reload
    class LockCheckingRepairUow:
        def __init__(self, book: ExecutionBook) -> None:
            self._book = book
            self.load_facts_called = False
            self.persist_repair_called = False

        @asynccontextmanager
        async def transaction(self, key: PositionKey):
            assert not self._book._mutation_lock(key).locked()
            yield self

        async def load_repair_facts(
            self, request: PositionRepairRequest
        ) -> PositionRepairFacts:
            assert not self._book._mutation_lock(request.key).locked()
            self.load_facts_called = True
            scope = request.scope
            fill = AccountFillEvent(
                environment=request.key.environment,
                account_label=request.key.account_label,
                symbol=request.key.symbol,
                trade_id="fill-repair-lock",
                order_id="order-repair-lock",
                side="BUY",
                price=Decimal("50000"),
                quantity=request.expected_quantity,
                realized_pnl=Decimal("0"),
                fee=Decimal("0"),
                fee_asset="USDT",
                trade_at=NOW,
                raw_payload={"positionSide": "LONG"},
            )
            snap_time = NOW - timedelta(hours=1)
            zero_snap = AccountPositionSnapshot(
                environment=request.key.environment,
                account_label=request.key.account_label,
                symbol=request.key.symbol,
                position_side=request.key.position_side.value,
                position_amt=Decimal("0"),
                entry_price=Decimal("0"),
                unrealized_pnl=Decimal("0"),
                mark_price=Decimal("50000"),
                notional=Decimal("0"),
                leverage=5,
                margin_type="cross",
                observed_at=snap_time,
                raw_payload={},
            )
            current_snap = AccountPositionSnapshot(
                environment=request.key.environment,
                account_label=request.key.account_label,
                symbol=request.key.symbol,
                position_side=request.key.position_side.value,
                position_amt=request.expected_quantity,
                entry_price=Decimal("50000"),
                unrealized_pnl=Decimal("0"),
                mark_price=Decimal("50000"),
                notional=Decimal("50000") * request.expected_quantity,
                leverage=5,
                margin_type="cross",
                observed_at=NOW,
                raw_payload={},
            )
            anchor_id = stable_snapshot_anchor_id(zero_snap)
            prov = AccountFillLoadProvenance(
                stream_scope=scope,
                load_id="scan-1",
                scan_origin_from_id=None,
                scan_origin_start_time_ms=1000,
                request_from_id=None,
                next_from_id=None,
                page_count=1,
                page_exhausted=True,
                truncated=False,
                checked_through=NOW,
                observed_at=NOW,
                source_anchor_id=anchor_id,
                source_anchor_event_cut=snap_time,
                source_anchor_kind="zero_snapshot",
            )
            cov = FactCoverageInterval(
                start_at=snap_time,
                end_at=NOW,
                source_cursor="scan-1",
                status=FactCoverageStatus.CONFIRMED,
                stream_scope=scope,
                evidence_observed_at=NOW,
                load_provenance=prov,
            )
            cut = DurableJournalCut(
                scope=scope,
                facts=AccountFacts(
                    position_key=request.key,
                    stream_scope=scope,
                    coverage=cov,
                    snapshots=(zero_snap, current_snap),
                    fill_load_provenance=prov,
                ),
                revision=0,
                as_of=NOW,
            )
            return PositionRepairFacts(
                cut, None, (fill,), frozenset({"order-repair-lock"})
            )

        async def persist_repair(self, repair: Any) -> PositionRepairReceipt:
            assert not self._book._mutation_lock(repair.request.key).locked()
            self.persist_repair_called = True
            receipt = PositionRepairReceipt(
                scope=repair.request.scope,
                head_revision=repair.expected_head_revision + int(repair.needs_write),
                projection_version=repair.projection_version,
                changed=repair.needs_write,
            )
            head = ExecutionHeadSnapshot(
                receipt.head_revision,
                repair.request.scope.stream_id,
                repair.request.scope.stream_epoch,
                repair.projection_version,
                repair.head_payload,
            )
            from crypto_momentum_lab.domain.execution.execution_head import (
                decode_execution_head_state,
            )

            current_book = decode_execution_head_state(
                repair.head_payload,
                key=repair.request.key,
                scope=repair.request.scope,
            )
            uow_checker.position_state = DurableExecutionPositionState(
                repair.request.scope,
                DurableJournalCut(
                    scope=repair.request.scope,
                    facts=current_book.current_facts,
                    revision=receipt.head_revision,
                    as_of=NOW,
                ),
                head,
                ("fill-repair-lock",),
                (),
                (),
            )
            return receipt

    repair_uow = LockCheckingRepairUow(uow_checker_book)
    repair_req = PositionRepairRequest(
        key=pos_key,
        run_id="run-1",
        scope=AccountFactStreamScope.for_position_key(
            pos_key, stream_id="stream-1", stream_epoch="epoch-1"
        ),
        expected_quantity=Decimal("0.005"),
        observed_at=NOW,
    )
    published_repair = await uow_checker_book.repair_position(
        repair_req, uow=repair_uow
    )
    assert repair_uow.load_facts_called
    assert repair_uow.persist_repair_called
    assert published_repair.view is not None
    assert published_repair.view.total_quantity == Decimal("0.005")

    await coordinator.aclose()


@pytest.mark.asyncio
async def test_a3c_concurrent_exits_atomic_reservation_allows_at_most_one_post() -> (
    None
):
    # 1. Concurrent exits: only one can atomically reserve batch quantity 40.7
    book = ExecutionBook()
    scope = ExecutionScope("live", "primary", "ORCAUSDT", FuturesPositionSide.LONG)
    t0 = datetime(2026, 10, 9, 3, 29, 59, tzinfo=UTC)
    fill = AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="ORCAUSDT",
        trade_id="open-1",
        order_id="order-1",
        side="BUY",
        price=Decimal("2.455"),
        quantity=Decimal("40.7"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=t0,
        raw_payload={"positionSide": "LONG", "is_system": True},
    )
    snapshot = AccountPositionSnapshot(
        environment="live",
        account_label="primary",
        symbol="ORCAUSDT",
        position_side="LONG",
        position_amt=Decimal("40.7"),
        entry_price=Decimal("2.455"),
        mark_price=Decimal("2.47965848"),
        unrealized_pnl=Decimal("1.00360013"),
        notional=Decimal("100.92210013"),
        leverage=5,
        margin_type="cross",
        observed_at=t0,
        raw_payload={},
    )
    await book.observe(
        ExecutionEvidence(
            evidence_id="init-fill",
            scope=scope,
            observed_at=t0,
            fill=fill,
            snapshot=snapshot,
            coverage=FactCoverageInterval(
                start_at=t0, end_at=t0, status=FactCoverageStatus.CONFIRMED
            ),
        )
    )
    pos_view = await book.read(scope)
    v1_token = pos_view.projection_version

    post_count = 0

    class A3cExchange:
        def set_exchange_boundary_callbacks(
            self, on_request: Any = None, on_response: Any = None
        ) -> None:
            pass

        async def submit_order(self, plan: OrderExecutionPlan) -> ExchangeOrderSnapshot:
            nonlocal post_count
            post_count += 1
            return ExchangeOrderSnapshot(
                client_order_id=plan.client_order_id,
                exchange_order_id=f"ex-a3c-{post_count}",
                state=ExchangeOrderState.ACKNOWLEDGED,
                observed_at=NOW,
                executed_quantity=Decimal("0"),
                average_price=Decimal("0"),
            )

    a3c_coord = OrderExecutionCoordinator(
        environment="live",
        backend=OrderExecutionStateMachine(
            exchange=A3cExchange(),
            event_repository=MemoryEventRepo(),
            live_submit_enabled=True,
            clock=lambda: NOW,
        ),
        account_label="primary",
        execution_book=book,
    )
    a3c_coord.configure_submission(RecordingPreparedRepo(), clock=lambda: NOW)

    async def execute_competing_exit(req_id: str, decision_ref: str) -> Any:
        act_res = await book.act(
            ExecutionRequest(
                request_id=req_id,
                scope=scope,
                strategy_name="test",
                run_id="run-1",
                decision_ref=decision_ref,
                expected_view_token=v1_token,
                action=TradeCommandType.EXIT,
                requested_quantity=Decimal("40.7"),
                side=StrategySide.LONG,
            )
        )
        if isinstance(act_res, Accepted):
            plan_acc = OrderExecutionPlan(
                intent_id=req_id,
                run_id="run-1",
                client_order_id=f"cml_a3c_{req_id}_12345678",
                symbol="ORCAUSDT",
                side="SELL",
                position_side=FuturesPositionSide.LONG,
                order_type="MARKET",
                quantity=Decimal("40.7"),
                price=None,
                reduce_only=True,
                quantized=True,
                created_at=NOW,
            )
            prep_acc = OrderSubmissionPreparation(
                intent=replace(_intent(), candidate_id=req_id, reduce_only=True),
                evaluation=RiskEvaluation(
                    evaluation_id=f"ev-a3c-{req_id}",
                    candidate_id=req_id,
                    decision=RiskDecision.APPROVED,
                    reason="ok",
                    evaluated_at=NOW,
                    details={},
                ),
            )
            return await a3c_coord.prepare_and_execute(plan_acc, preparation=prep_acc)
        return act_res

    res1, res2 = await asyncio.gather(
        execute_competing_exit("exit-1", "tp"),
        execute_competing_exit("exit-2", "sl"),
    )
    await a3c_coord.aclose()

    # Invariant: exactly 1 exit POSTed, exactly 1 blocked by atomic reservation conflict
    assert post_count == 1
    results = [res1, res2]
    assert any(isinstance(r, Blocked) for r in results)
    assert any(r is not None and not isinstance(r, Blocked) for r in results)
    assert len(book.coordinator.get_active_reservations(scope.to_position_key())) == 1

    # 3. Stale view: v1 plan cannot overwrite v2 state after lock-free read
    newer_fill = replace(
        fill,
        trade_id="open-2",
        order_id="order-2",
        quantity=Decimal("1"),
        trade_at=t0 + timedelta(seconds=5),
    )
    await book.observe(
        ExecutionEvidence(
            evidence_id="new-fill",
            scope=scope,
            observed_at=newer_fill.trade_at,
            fill=newer_fill,
            snapshot=replace(
                snapshot,
                position_amt=Decimal("41.7"),
                observed_at=newer_fill.trade_at,
            ),
        )
    )
    stale_req = ExecutionRequest(
        request_id="exit-stale",
        scope=scope,
        strategy_name="test",
        run_id="run-1",
        decision_ref="stale",
        expected_view_token=v1_token,
        action=TradeCommandType.EXIT,
        requested_quantity=Decimal("40.7"),
        side=StrategySide.LONG,
    )
    stale_res = await book.act(stale_req)
    assert isinstance(stale_res, StaleView)
    assert stale_res.expected_token == v1_token


@pytest.mark.asyncio
async def test_a3d_caller_cancellation_and_missing_fill_price() -> None:
    # 1. Caller cancellation after POST started preserves POST count == 1 and state.
    post_started = asyncio.Event()
    post_release = asyncio.Event()
    post_finished = asyncio.Event()
    posts = 0

    class CancelGuardedBackend:
        async def submit(
            self, plan: OrderExecutionPlan, **kwargs: Any
        ) -> OrderExecutionResult:
            nonlocal posts
            posts += 1
            post_started.set()
            try:
                await asyncio.wait_for(post_release.wait(), timeout=5.0)
                return OrderExecutionResult(
                    client_order_id=plan.client_order_id,
                    state=ExchangeOrderState.ACKNOWLEDGED,
                    exchange_order_id="ex-1",
                    plan=plan,
                )
            finally:
                post_finished.set()

    prep_repo = RecordingPreparedRepo()
    coordinator = OrderExecutionCoordinator(
        environment="live",
        backend=CancelGuardedBackend(),
        account_label="primary",
        execution_book=ExecutionBook(),
    )
    coordinator.configure_submission(prep_repo, clock=lambda: NOW)

    plan = OrderExecutionPlan(
        intent_id="cand-cancel",
        run_id="run-1",
        client_order_id="cml_cancel_test_1234567890123456",
        symbol="BTCUSDT",
        side="BUY",
        position_side=FuturesPositionSide.LONG,
        order_type="MARKET",
        quantity=Decimal("0.001"),
        price=None,
        reduce_only=False,
        created_at=NOW,
    )
    prep = OrderSubmissionPreparation(
        intent=replace(_intent(), candidate_id="cand-cancel"),
        evaluation=RiskEvaluation(
            evaluation_id="ev-c",
            candidate_id="cand-cancel",
            decision=RiskDecision.APPROVED,
            reason="ok",
            evaluated_at=NOW,
            details={},
        ),
    )

    task = asyncio.create_task(coordinator.prepare_and_execute(plan, preparation=prep))
    await asyncio.wait_for(post_started.wait(), timeout=5.0)

    # Cancel the caller task.
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Release backend POST and deterministically wait for completion.
    post_release.set()
    await asyncio.wait_for(post_finished.wait(), timeout=5.0)

    # POST total count remains exactly 1!
    assert posts == 1
    await coordinator.aclose()

    # 2. Filled qty > 0 missing price returns UNKNOWN without synthetic price
    # while background GET hangs on Event.
    reconciliation_gate = asyncio.Event()
    reconciliation_started = asyncio.Event()

    class MissingPriceExchange:
        def __init__(self) -> None:
            self.post_count = 0

        def set_exchange_boundary_callbacks(
            self, on_request: Any = None, on_response: Any = None
        ) -> None:
            pass

        async def submit_order(self, plan: OrderExecutionPlan) -> ExchangeOrderSnapshot:
            self.post_count += 1
            # Exchange POST returns filled quantity 0.5 but price 0
            return ExchangeOrderSnapshot(
                client_order_id=plan.client_order_id,
                exchange_order_id="ex-missing-price",
                state=ExchangeOrderState.FILLED,
                observed_at=NOW,
                executed_quantity=Decimal("0.5"),
                average_price=Decimal("0"),
            )

        async def query_order_by_client_id(
            self, symbol: str, client_order_id: str
        ) -> ExchangeOrderSnapshot | None:
            reconciliation_started.set()
            await asyncio.wait_for(reconciliation_gate.wait(), timeout=5.0)
            return None

    ex_missing = MissingPriceExchange()
    sm_missing = OrderExecutionStateMachine(
        exchange=ex_missing,
        event_repository=MemoryEventRepo(),
        live_submit_enabled=True,
        clock=lambda: NOW,
        retry_lookup_on_submit_timeout=False,
        reconciliation_retry_delays=(0.001, 0.001),
    )
    plan_with_limit = replace(
        plan,
        client_order_id="cml_no_synth_price_12345",
        price=Decimal("60000"),
        quantized=True,
    )
    prep_sub_missing = PreparedOrderSubmission(
        plan=plan_with_limit,
        submitting_event=ExchangeOrderEvent(
            event_id="evt-missing-price",
            client_order_id=plan_with_limit.client_order_id,
            state=ExchangeOrderState.SUBMITTING,
            occurred_at=NOW,
            exchange_order_id=None,
            details={},
        ),
    )
    submit_result = await sm_missing.submit(
        plan_with_limit, prepared_submission=prep_sub_missing
    )

    # Invariant: Does NOT synthesize price from plan limit (60000);
    # preserves 0 and retains unknown/uncertain state
    assert submit_result.average_price == Decimal("0")
    assert submit_result.executed_quantity == Decimal("0.5")
    assert submit_result.state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION
    assert ex_missing.post_count == 1

    # Background recovery GET hangs on Event without blocking the completed
    # submit result
    rec_task = asyncio.create_task(sm_missing.reconcile_order(plan_with_limit))
    await asyncio.wait_for(reconciliation_started.wait(), timeout=2.0)
    reconciliation_gate.set()
    await rec_task
