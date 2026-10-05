import asyncio
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

import pytest

from crypto_momentum_lab.domain.execution.order_execution_port import OrderExecutionPort
from crypto_momentum_lab.domain.execution.order_result import OrderExecutionResult
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    FuturesPositionSide,
)
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import StrategySide
from crypto_momentum_lab.live_rollout.context import (
    LiveContextProvider,
    LiveDaemonRuntimeContext,
)
from crypto_momentum_lab.live_rollout.exit_processor import (
    ExitProcessorConfig,
    LiveExitProcessor,
)
from crypto_momentum_lab.live_rollout.exits import (
    LiveExitOrderRequest,
    ManagedLivePosition,
)
from crypto_momentum_lab.live_rollout.order_identity_errors import (
    is_durable_order_identity_conflict,
)
from crypto_momentum_lab.live_rollout.position_lifecycle import PositionLifecycleLocks
from crypto_momentum_lab.live_rollout.submission import LiveCandidateSubmission
from tests.fixtures.async_reservations import (
    InMemoryPositionReservationRepository as MemoryReservations,
)
from tests.fixtures.live_market import _intent, _state
from tests.fixtures.prepared_submission import submit_prepared

NOW = datetime(2026, 7, 4, 0, 0, 20, tzinfo=UTC)


async def test_exit_recovery_keeps_inspecting_after_previous_replacement_attempts():
    from unittest.mock import AsyncMock

    from crypto_momentum_lab.domain.execution.order_state import OrderExecutionPlan

    submission = RecordingSubmission(_acknowledged_result())
    processor = _processor(submission)
    plan = OrderExecutionPlan(
        "exit",
        "run-1",
        "original-exit",
        "BTCUSDT",
        "SELL",
        "MARKET",
        Decimal("0.001"),
        None,
        True,
        NOW,
    )
    inspector = AsyncMock(side_effect=TimeoutError("exchange still unavailable"))
    processor._exit_recovery_client = SimpleNamespace(inspect_exit_order=inspector)
    processor._exit_recovery_attempts[plan.client_order_id] = 3
    result = await processor._recover_unknown_exit(
        plan=plan,
        known_executed_quantity=Decimal("0"),
        state=_state(),
    )
    assert result is None
    inspector.assert_awaited_once_with(plan)
    assert submission.calls == []


@pytest.mark.parametrize(
    "cancel_state",
    [
        ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
        ExchangeOrderState.CANCELING,
        ExchangeOrderState.REJECTED,
    ],
)
async def test_one_batch_cancel_failure_does_not_stop_another_batch_exit(cancel_state):
    from unittest.mock import AsyncMock

    from crypto_momentum_lab.domain.execution.order_state import OrderExecutionPlan
    from crypto_momentum_lab.live_rollout.exits import LiveExitCancellationRequest

    submission = RecordingSubmission(_acknowledged_result())
    processor = _processor(submission)
    plan = OrderExecutionPlan(
        "exit-first",
        "run-1",
        "cancel-first",
        "BTCUSDT",
        "SELL",
        "LIMIT",
        Decimal("0.001"),
        Decimal("100"),
        True,
        NOW,
    )
    processor._state_machine = SimpleNamespace(
        cancel_order=AsyncMock(
            return_value=OrderExecutionResult(
                plan.client_order_id, cancel_state, None, plan=plan
            ),
        )
    )
    first = replace(_intent(), candidate_id="first-batch", reduce_only=True)
    second = replace(_intent(), candidate_id="second-batch", reduce_only=True)
    result = await processor.process_requests(
        (
            LiveExitCancellationRequest(
                cancel_plan=plan,
                fallback_candidate=first,
                fallback_quantity=Decimal("0.001"),
            ),
            LiveExitOrderRequest(candidate=second, quantity=Decimal("0.001")),
        ),
        state=_state(),
        context=_context(),
    )
    assert result[:2] == (1, 1)
    assert result[2] is not None
    assert [candidate.candidate_id for candidate, _, _ in submission.calls] == [
        "second-batch"
    ]


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ("client order ID is already bound to a different order", True),
        ("execution command cml_1 is in non-dispatchable state terminal", True),
        ("active lease disappeared", False),
        ("exit order was allocated from stale position", False),
    ],
)
def test_only_durable_order_identity_errors_are_classified_as_conflicts(
    message: str,
    expected: bool,
) -> None:
    assert is_durable_order_identity_conflict(RuntimeError(message)) is expected


class RecordingSubmission:
    def __init__(self, result: OrderExecutionResult | None) -> None:
        self.result = result
        self.calls: list[tuple[object, Decimal | None, MarketState15s]] = []

    async def execute(
        self,
        candidate,
        *,
        requested_quantity,
        state,
        context,
        reference_price=None,
    ):
        del context, reference_price
        self.calls.append((candidate, requested_quantity, state))
        return self.result


class BlockingSubmission(RecordingSubmission):
    def __init__(self) -> None:
        super().__init__(_acknowledged_result())
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.active = 0
        self.max_active = 0

    async def execute(self, *args, **kwargs):
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self.started.set()
        await self.release.wait()
        self.active -= 1
        return await super().execute(*args, **kwargs)


def _acknowledged_result() -> OrderExecutionResult:
    return OrderExecutionResult(
        client_order_id="cml_exit_1",
        state=ExchangeOrderState.ACKNOWLEDGED,
        exchange_order_id="exchange-1",
    )


def _context() -> LiveDaemonRuntimeContext:
    return cast(
        LiveDaemonRuntimeContext,
        SimpleNamespace(
            pending_position_symbols=frozenset(),
            unmanaged_position_symbols=frozenset(),
            managed_positions=(
                ManagedLivePosition(
                    symbol=_intent().symbol,
                    side=StrategySide.LONG,
                    position_side=FuturesPositionSide.LONG,
                    quantity=Decimal("1"),
                    entry_price=Decimal("100"),
                    opened_at=NOW,
                    account_label="primary",
                ),
            ),
        ),
    )


def _processor(
    submission: object,
) -> LiveExitProcessor:
    async def provide_context(_state: MarketState15s) -> LiveDaemonRuntimeContext:
        return _context()

    def publish_positions(_context: LiveDaemonRuntimeContext) -> None:
        return None

    return LiveExitProcessor(
        position_locks=PositionLifecycleLocks(),
        account_label="primary",
        config=ExitProcessorConfig(run_id="run-1"),
        exit_manager=None,
        exit_recovery_client=None,
        state_machine=cast(OrderExecutionPort, object()),
        submission=cast(LiveCandidateSubmission, submission),
        telemetry=None,
        clock=lambda: NOW,
        is_exit_enabled=lambda: True,
        context_provider=cast(LiveContextProvider, provide_context),
        apply_context=publish_positions,
        invalidate_context_cache=lambda: None,
        request_recovery=Mock(),
    )


def test_exit_processor_config_rejects_blank_run_id() -> None:
    with pytest.raises(ValueError, match="run_id must not be empty"):
        ExitProcessorConfig(run_id=" ")


@pytest.mark.asyncio
async def test_process_requests_delegates_one_exit_and_reports_submission_counts() -> (
    None
):
    submission = RecordingSubmission(_acknowledged_result())
    processor = _processor(submission)
    candidate = replace(_intent(), candidate_id="exit-1", reduce_only=True)
    state = _state()
    context = _context()

    approved, submitted, failure = await processor.process_requests(
        (LiveExitOrderRequest(candidate=candidate, quantity=Decimal("0.001")),),
        state=state,
        context=context,
    )

    assert (approved, submitted, failure) == (1, 1, None)
    assert len(submission.calls) == 1
    submitted_candidate, quantity, submitted_state = submission.calls[0]
    assert submitted_candidate.candidate_id == candidate.candidate_id
    assert submitted_candidate.reduce_only
    assert quantity == Decimal("0.001")
    assert submitted_state == state


@pytest.mark.asyncio
async def test_missing_durable_position_facts_degrades_exit_without_crashing() -> None:
    class MissingFactsSubmission:
        async def execute(self, *_args, **_kwargs):
            raise RuntimeError(
                "Failed to create position reservation: "
                "Position facts are not durably restored"
            )

    processor = _processor(MissingFactsSubmission())
    candidate = replace(_intent(), candidate_id="exit-missing-facts", reduce_only=True)

    result = await processor.process_requests(
        (LiveExitOrderRequest(candidate=candidate, quantity=Decimal("0.001")),),
        state=_state(),
        context=_context(),
    )

    assert result == (0, 0, "position_facts_not_restored")


@pytest.mark.asyncio
async def test_process_requests_serializes_same_symbol_batches() -> None:
    submission = BlockingSubmission()
    processor = _processor(submission)
    state = _state()
    context = _context()

    first = asyncio.create_task(
        processor.process_requests(
            (
                LiveExitOrderRequest(
                    candidate=replace(
                        _intent(), candidate_id="exit-1", reduce_only=True
                    ),
                    quantity=Decimal("0.001"),
                ),
            ),
            state=state,
            context=context,
        )
    )
    await submission.started.wait()
    second = asyncio.create_task(
        processor.process_requests(
            (
                LiveExitOrderRequest(
                    candidate=replace(
                        _intent(), candidate_id="exit-2", reduce_only=True
                    ),
                    quantity=Decimal("0.001"),
                ),
            ),
            state=state,
            context=context,
        )
    )
    await asyncio.sleep(0)
    assert len(submission.calls) == 0
    assert submission.max_active == 1

    submission.release.set()
    assert await first == (1, 1, None)
    assert await second == (1, 1, None)
    assert len(submission.calls) == 2


@pytest.mark.asyncio
async def test_position_readiness_guard_defers_exit_instead_of_killing_daemon():
    from crypto_momentum_lab.domain.execution.order_submission import (
        OrderPreSubmissionError,
    )
    from crypto_momentum_lab.domain.execution.reservation_registry import (
        ExecutionReadinessError,
    )

    class ConflictSubmission:
        async def execute(self, *_args, **_kwargs):
            try:
                raise ExecutionReadinessError(
                    "PositionView for live:account-4:XVSUSDT:LONG "
                    "is not ready for trade (health=CONFLICT)"
                )
            except ExecutionReadinessError as cause:
                raise OrderPreSubmissionError(
                    "Failed to create position reservation"
                ) from cause

    processor = _processor(ConflictSubmission())
    candidate = replace(_intent(), candidate_id="exit-conflict", reduce_only=True)
    result = await processor.process_requests(
        (LiveExitOrderRequest(candidate=candidate, quantity=Decimal("0.001")),),
        state=_state(),
        context=_context(),
    )
    assert result == (0, 0, "position_not_ready")


@pytest.mark.asyncio
async def test_recovery_guard_preserves_receipt_and_post_attempts():
    from unittest.mock import AsyncMock

    from crypto_momentum_lab.domain.execution.order_state import OrderExecutionPlan
    from crypto_momentum_lab.domain.execution.order_submission import (
        OrderPreSubmissionError,
    )
    from crypto_momentum_lab.domain.execution.reservation_registry import (
        ExecutionReadinessError,
    )

    class ConflictSubmission:
        async def execute(self, *_args, **_kwargs):
            try:
                raise ExecutionReadinessError("PositionView not ready for trade")
            except ExecutionReadinessError as cause:
                raise OrderPreSubmissionError(
                    "Failed to create position reservation"
                ) from cause

    processor = _processor(ConflictSubmission())
    recovery = SimpleNamespace(
        inspect_exit_order=AsyncMock(
            return_value=SimpleNamespace(
                order=None,
                position_quantity=Decimal("0.001"),
                active_exit_order_client_ids=(),
                observed_at=NOW,
            )
        )
    )
    processor._exit_recovery_client = recovery
    original_receipt = OrderExecutionResult(
        "original-client-id", ExchangeOrderState.ABSENT_RECONCILED, None
    )
    processor._state_machine = SimpleNamespace(
        mark_absent_reconciled=AsyncMock(return_value=original_receipt)
    )
    plan = OrderExecutionPlan(
        "original-exit",
        "run-1",
        "original-client-id",
        "BTCUSDT",
        "SELL",
        "MARKET",
        Decimal("0.001"),
        None,
        True,
        NOW,
    )
    result = await processor._recover_unknown_exit(
        plan=plan,
        known_executed_quantity=Decimal("0"),
        state=_state(),
        source_candidate=replace(_intent(), reduce_only=True),
    )
    assert result is None
    await processor._recover_unknown_exit(
        plan=plan,
        known_executed_quantity=Decimal("0"),
        state=_state(),
    )
    recovery.inspect_exit_order.assert_awaited_once()


@pytest.mark.asyncio
async def test_unrelated_submission_corruption_still_propagates():
    class CorruptSubmission:
        async def execute(self, *_args, **_kwargs):
            raise ValueError("invalid durable payload")

    processor = _processor(CorruptSubmission())
    candidate = replace(_intent(), reduce_only=True)
    with pytest.raises(ValueError, match="invalid durable payload"):
        await processor.process_requests(
            (LiveExitOrderRequest(candidate=candidate, quantity=Decimal("0.001")),),
            state=_state(),
            context=_context(),
        )


async def test_stale_exit_projection_defers_before_post_and_invalidates_context():
    from unittest.mock import AsyncMock

    from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
    from crypto_momentum_lab.domain.execution.order_state import (
        FuturesPositionSide,
        OrderExecutionPlan,
    )
    from tests.unit.execution_account.orders.test_coordinator import (
        OrderExecutionCoordinator,
        _submission_preparation,
    )

    book = ExecutionBook(reservation_repository=MemoryReservations())
    backend, repository = AsyncMock(), AsyncMock()
    coordinator = OrderExecutionCoordinator(
        backend=backend,
        environment="live",
        account_label="primary",
        execution_book=book,
    )
    coordinator.configure_submission(repository)
    plan = OrderExecutionPlan(
        intent_id="exit",
        run_id="run-1",
        client_order_id="stale-exit",
        symbol="BTCUSDT",
        side="SELL",
        order_type="MARKET",
        quantity=Decimal("1"),
        price=None,
        reduce_only=True,
        position_side=FuturesPositionSide.LONG,
        created_at=NOW,
        batch_id="real-batch",
        projection_version="pv_outdated",
    )

    class QueuedSubmission:
        async def execute(self, *_args, **_kwargs):
            return await coordinator.prepare_and_execute(
                plan,
                preparation=_submission_preparation(plan),
            )

    processor = _processor(QueuedSubmission())
    invalidations = []
    processor._invalidate_context_cache = lambda: invalidations.append(True)
    try:
        for _ in range(2):
            result = await processor.process_requests(
                (
                    LiveExitOrderRequest(
                        candidate=replace(_intent(), reduce_only=True),
                        quantity=Decimal("1"),
                    ),
                ),
                state=_state(),
                context=_context(),
            )
            assert result == (0, 0, "pending_live_context:BTCUSDT")
        assert invalidations == [True, True]
        backend.submit.assert_not_awaited()
        repository.prepare_submission_in_session.assert_not_awaited()
        assert book.get_outbox(plan.client_order_id) is None
    finally:
        await coordinator.aclose()


def test_recovery_rebuilds_current_target_batch_without_closing_addons():
    from crypto_momentum_lab.domain.execution.order_state import (
        FuturesPositionSide,
        OrderExecutionPlan,
    )
    from crypto_momentum_lab.domain.strategy import StrategySide
    from crypto_momentum_lab.live_rollout.exit_processor import (
        _build_exit_recovery_candidate,
    )
    from crypto_momentum_lab.live_rollout.exits import ManagedLivePosition

    position = ManagedLivePosition(
        "BTCUSDT",
        StrategySide.LONG,
        FuturesPositionSide.LONG,
        Decimal("0.4"),
        Decimal("100"),
        NOW,
        batch_id="target",
        projection_version="pv_current",
    )
    context = SimpleNamespace(
        managed_positions=(
            position,
            replace(position, batch_id="new-addon", quantity=Decimal("2")),
        )
    )
    plan = OrderExecutionPlan(
        "exit",
        "run-1",
        "original",
        "BTCUSDT",
        "SELL",
        "MARKET",
        Decimal("1"),
        None,
        True,
        NOW,
        position_side=FuturesPositionSide.LONG,
        batch_id="target",
        projection_version="pv_old",
    )
    source = replace(
        _intent(),
        reduce_only=True,
        features={
            "batch_id": "target",
            "projection_version": "pv_old",
        },
    )
    candidate = _build_exit_recovery_candidate(
        plan=plan,
        source_candidate=source,
        context=context,
        state=_state(),
        now=NOW,
        reference_price=Decimal("100"),
        root_client_order_id="original",
        attempt=1,
        quantity=Decimal("2.4"),
    )
    assert candidate.features["quantity"] == "0.4"
    assert candidate.features["projection_version"] == "pv_current"
    assert candidate.features["batch_id"] == "target"
    assert candidate.desired_notional == Decimal("40")
    context.managed_positions = context.managed_positions[1:]
    assert (
        _build_exit_recovery_candidate(
            plan=plan,
            source_candidate=source,
            context=context,
            state=_state(),
            now=NOW,
            reference_price=Decimal("100"),
            root_client_order_id="original",
            attempt=1,
            quantity=Decimal("2"),
        )
        is None
    )


async def test_real_book_degraded_view_does_not_block_exit():
    from unittest.mock import AsyncMock, MagicMock

    from crypto_momentum_lab.domain.account.models import (
        AccountFillEvent,
        AccountPositionSnapshot,
    )
    from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
    from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
    from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
    from crypto_momentum_lab.domain.execution.order_state import (
        FuturesPositionSide,
        OrderExecutionPlan,
    )
    from tests.unit.execution_account.orders.test_coordinator import (
        OrderExecutionCoordinator,
    )

    scope = ExecutionScope("live", "primary", "BTCUSDT", FuturesPositionSide.LONG)
    book = ExecutionBook(reservation_repository=MemoryReservations())
    fill = AccountFillEvent(
        "live",
        "primary",
        "BTCUSDT",
        "entry",
        "order",
        "BUY",
        Decimal("100"),
        Decimal("1"),
        Decimal("0"),
        Decimal("0"),
        "USDT",
        NOW,
        {"positionSide": "LONG"},
    )
    snap = AccountPositionSnapshot(
        "live",
        "primary",
        "BTCUSDT",
        "LONG",
        Decimal("1"),
        Decimal("100"),
        Decimal("100"),
        Decimal("0"),
        Decimal("100"),
        5,
        "cross",
        NOW,
        {},
    )
    await book.observe(ExecutionEvidence("open", scope, NOW, fill=fill, snapshot=snap))
    view = await book.read(scope)
    assert not view.is_ready_for_trade and view.batches
    backend = MagicMock()
    backend.submit = AsyncMock(
        return_value=OrderExecutionResult(
            "exit-guard", ExchangeOrderState.ACKNOWLEDGED, "123"
        )
    )
    coordinator = OrderExecutionCoordinator(
        backend=backend,
        environment="live",
        account_label="primary",
        execution_book=book,
    )
    plan = OrderExecutionPlan(
        intent_id="exit",
        run_id="live",
        client_order_id="exit-guard",
        symbol="BTCUSDT",
        side="SELL",
        order_type="MARKET",
        quantity=Decimal("1"),
        price=None,
        reduce_only=True,
        position_side=FuturesPositionSide.LONG,
        created_at=NOW,
        batch_id=view.batches[0].batch_id,
        projection_version=view.projection_version,
    )

    class GuardedSubmission:
        async def execute(self, *_args, **_kwargs):
            return await submit_prepared(coordinator, plan)

    processor = _processor(GuardedSubmission())
    result = await processor.process_requests(
        (
            LiveExitOrderRequest(
                candidate=replace(_intent(), reduce_only=True), quantity=Decimal("1")
            ),
        ),
        state=_state(),
        context=_context(),
    )
    assert result == (1, 1, None)
    backend.submit.assert_awaited_once()
    assert book.get_active_reservations(scope.to_position_key())
    await coordinator.aclose()


async def test_remote_recovery_does_not_hold_quote_decision_lock_or_repeat_query():
    started = asyncio.Event()
    release = asyncio.Event()
    calls = []
    processor = _processor(RecordingSubmission(_acknowledged_result()))
    processor._exit_manager = SimpleNamespace(
        uses_market_state_exit=False, requests_for_quote=lambda *_: _provide(())
    )
    state = _state()
    plan = SimpleNamespace(
        symbol=state.symbol,
        reduce_only=True,
        client_order_id="unknown-exit",
        intent_id="exit",
        position_side=SimpleNamespace(value="LONG"),
    )
    context = SimpleNamespace(
        managed_positions=(),
        unresolved_orders=(
            SimpleNamespace(
                plan=plan,
                state=ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
                executed_quantity=Decimal("0"),
                updated_at=NOW,
            ),
        ),
    )

    async def inspect(plan):
        calls.append(plan.client_order_id)
        started.set()
        await release.wait()
        raise TimeoutError("unknown exchange receipt")

    processor._exit_recovery_client = SimpleNamespace(inspect_exit_order=inspect)
    processor._context_provider = lambda state: _provide(context)
    first_outcome = await processor.handle_trigger("state", state, context)
    assert first_outcome.failure is None
    assert not calls
    worker = asyncio.create_task(processor.recover_requested_exits())
    try:
        await asyncio.wait_for(started.wait(), 1)
        outcome = await asyncio.wait_for(
            processor.handle_trigger(
                "quote", state, context, quote=SimpleNamespace(symbol=state.symbol)
            ),
            0.5,
        )
        assert outcome.failure is None
        assert calls == ["unknown-exit"]
        release.set()
        await asyncio.wait_for(worker, 1)
        await processor.recover_requested_exits()
        assert calls == ["unknown-exit"]
    finally:
        release.set()
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


async def _provide(context):
    return context


async def test_unknown_order_does_not_block_candle_evaluation_for_other_batches():
    from unittest.mock import AsyncMock

    processor = _processor(RecordingSubmission(_acknowledged_result()))
    processor._exit_manager = SimpleNamespace(
        requests_for_closed_candle=AsyncMock(return_value=())
    )
    state = _state()
    plan = SimpleNamespace(
        symbol=state.symbol,
        reduce_only=True,
        client_order_id="unknown-exit",
        intent_id="exit",
        position_side=SimpleNamespace(value="LONG"),
    )
    context = SimpleNamespace(
        managed_positions=(),
        unresolved_orders=(
            SimpleNamespace(
                plan=plan,
                state=ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
                executed_quantity=Decimal("0"),
                updated_at=NOW,
            ),
        ),
    )
    processor._exit_recovery_client = SimpleNamespace(
        inspect_exit_order=AsyncMock(side_effect=TimeoutError("unknown receipt"))
    )
    event = SimpleNamespace(
        candle=SimpleNamespace(symbol=state.symbol), received_at=NOW
    )
    outcome = await processor.handle_trigger(
        "closed_candle", state, context, event=event
    )
    assert outcome.failure is None
    processor._exit_manager.requests_for_closed_candle.assert_awaited_once()


async def test_cancel_fallback_continues_when_context_read_temporarily_fails():
    from unittest.mock import AsyncMock

    from crypto_momentum_lab.domain.execution.order_state import OrderExecutionPlan
    from crypto_momentum_lab.live_rollout.exits import LiveExitCancellationRequest

    submission = RecordingSubmission(_acknowledged_result())
    processor = _processor(submission)
    processor._state_machine = SimpleNamespace(
        cancel_order=AsyncMock(
            return_value=OrderExecutionResult(
                "original", ExchangeOrderState.CANCELED, None
            )
        )
    )

    async def load(state):
        raise OSError("context storage temporarily unavailable")

    processor._context_provider = load
    plan = OrderExecutionPlan(
        "exit",
        "run-1",
        "original",
        "BTCUSDT",
        "SELL",
        "LIMIT",
        Decimal("0.001"),
        Decimal("100"),
        True,
        NOW,
    )
    request = LiveExitCancellationRequest(
        cancel_plan=plan,
        fallback_candidate=replace(_intent(), reduce_only=True),
        fallback_quantity=Decimal("0.001"),
        fallback_to_current_position=True,
    )
    context = SimpleNamespace(
        managed_positions=(
            SimpleNamespace(
                symbol=plan.symbol,
                position_side=plan.position_side,
                quantity=plan.quantity,
                side=StrategySide.LONG,
                batches=(),
                projection_version=None,
            ),
        )
    )
    result = await processor.process_requests(
        (request,), state=_state(), context=context
    )
    assert result == (1, 1, None)
    processor._state_machine.cancel_order.assert_awaited_once()
    assert len(submission.calls) == 1


async def test_grace_cancel_rebuilds_fallback_from_latest_position():
    from unittest.mock import AsyncMock

    from crypto_momentum_lab.domain.execution.order_state import (
        FuturesPositionSide,
        OrderExecutionPlan,
    )
    from crypto_momentum_lab.domain.strategy import StrategySide
    from crypto_momentum_lab.live_rollout.exits import (
        LiveExitCancellationRequest,
        ManagedLivePosition,
    )

    is_current = [True]
    old_position = ManagedLivePosition(
        "BTCUSDT",
        StrategySide.LONG,
        FuturesPositionSide.LONG,
        Decimal("1.0"),
        Decimal("100"),
        NOW,
        batch_id="batch-1",
        projection_version="old",
    )
    fresh_position = replace(
        old_position,
        quantity=Decimal("0.6"),
        projection_version="new",
    )
    fresh_context = SimpleNamespace(
        pending_position_symbols=frozenset(),
        unmanaged_position_symbols=frozenset(),
        managed_positions=(fresh_position,),
    )
    submission = RecordingSubmission(_acknowledged_result())
    processor = _processor(submission)

    async def refresh_context(_state):
        is_current[0] = True
        return fresh_context

    processor._context_provider = AsyncMock(side_effect=refresh_context)
    processor._apply_context = lambda _: None
    processor._state_machine = SimpleNamespace(
        cancel_order=AsyncMock(
            side_effect=lambda _plan: (
                is_current.__setitem__(0, False)
                or OrderExecutionResult("recovery", ExchangeOrderState.CANCELED, None)
            )
        )
    )
    candidate = replace(
        _intent(),
        candidate_id="old-fallback",
        reduce_only=True,
        desired_notional=Decimal("100"),
        features={
            "quantity": "1.0",
            "reference_price": "100",
            "exit_allocations": [{"batch_id": "batch-1", "quantity": "1.0"}],
        },
    )
    cancel_plan = OrderExecutionPlan(
        "recovery",
        "run-1",
        "recovery",
        "BTCUSDT",
        "SELL",
        "LIMIT",
        Decimal("1.0"),
        Decimal("99"),
        True,
        NOW,
        position_side=FuturesPositionSide.LONG,
    )
    request = LiveExitCancellationRequest(
        cancel_plan=cancel_plan,
        fallback_candidate=candidate,
        fallback_quantity=Decimal("1.0"),
    )

    result = await processor.process_requests(
        (request,),
        state=_state(),
        context=SimpleNamespace(
            pending_position_symbols=frozenset(),
            unmanaged_position_symbols=frozenset(),
            managed_positions=(old_position,),
        ),
    )

    assert result == (1, 1, None)
    submitted_candidate, submitted_quantity, _ = submission.calls[0]
    assert submitted_quantity == Decimal("0.6")
    assert submitted_candidate.candidate_id != candidate.candidate_id
    assert submitted_candidate.features["quantity"] == "0.6"
    assert submitted_candidate.features["exit_allocations"] == [
        {"batch_id": "batch-1", "quantity": "0.6"}
    ]


@pytest.mark.parametrize("blocked_by", ["projection", "readiness", "admission"])
async def test_blocked_replacement_keeps_recovery_work_after_old_receipt_is_terminal(
    blocked_by,
):
    from unittest.mock import AsyncMock

    from crypto_momentum_lab.domain.execution.order_state import (
        FuturesPositionSide,
        OrderExecutionPlan,
    )
    from crypto_momentum_lab.domain.execution.order_submission import (
        OrderProjectionConflictError,
    )
    from crypto_momentum_lab.domain.execution.reservation_registry import (
        ExecutionReadinessError,
    )
    from crypto_momentum_lab.domain.strategy import StrategySide
    from crypto_momentum_lab.live_rollout.exits import ManagedLivePosition

    error = {
        "projection": OrderProjectionConflictError("facts advanced"),
        "readiness": ExecutionReadinessError("PositionView not ready for trade"),
        "admission": None,
    }[blocked_by]
    submission = SimpleNamespace(
        execute=AsyncMock(side_effect=error, return_value=None)
    )
    processor = _processor(submission)
    plan = OrderExecutionPlan(
        "exit",
        "run-1",
        "original",
        "BTCUSDT",
        "SELL",
        "MARKET",
        Decimal("1"),
        None,
        True,
        NOW,
        position_side=FuturesPositionSide.LONG,
        batch_id="target",
        projection_version="pv_old",
    )
    position = ManagedLivePosition(
        "BTCUSDT",
        StrategySide.LONG,
        FuturesPositionSide.LONG,
        Decimal("0.4"),
        Decimal("100"),
        NOW,
        batch_id="target",
        projection_version="pv_current",
    )
    context = SimpleNamespace(
        managed_positions=(position,),
        pending_position_symbols=frozenset(),
        unmanaged_position_symbols=frozenset(),
    )
    processor._context_provider = lambda state: _provide(context)
    processor._state_machine = SimpleNamespace(
        mark_absent_reconciled=AsyncMock(
            return_value=OrderExecutionResult(
                "original",
                ExchangeOrderState.ABSENT_RECONCILED,
                None,
            )
        )
    )
    processor._exit_recovery_client = SimpleNamespace(
        inspect_exit_order=AsyncMock(
            return_value=SimpleNamespace(
                order=None,
                position_quantity=Decimal("0.4"),
                active_exit_order_client_ids=(),
                observed_at=NOW,
            )
        )
    )
    processor.request_exit_recovery(
        plan=plan,
        known_executed_quantity=Decimal("0"),
        state=_state(),
        source_candidate=replace(_intent(), reduce_only=True),
    )
    assert await processor.recover_requested_exits() == ()
    assert processor.has_pending_recovery
    assert submission.execute.call_args.kwargs["requested_quantity"] == Decimal("0.4")
    assert await processor.recover_requested_exits() == ()
    submission.execute.assert_awaited_once()
    processor._clock = lambda: processor._exit_recovery_next_attempt_at["original"]
    submission.execute.side_effect = None
    submission.execute.return_value = OrderExecutionResult(
        "replacement",
        ExchangeOrderState.FILLED,
        "exchange-replacement",
    )
    outcomes = await processor.recover_requested_exits()
    assert len(outcomes) == 1
    assert outcomes[0][1].submitted_order_count == 1
    assert not processor.has_pending_recovery
    assert submission.execute.await_count == 2


async def test_recovery_batch_rotates_failed_reads_without_starving_later_orders():
    processor = _processor(RecordingSubmission(None))
    reads = []

    async def inspect(plan):
        reads.append(plan.symbol)
        raise OSError("exchange inspection temporarily unavailable")

    processor._exit_recovery_client = SimpleNamespace(inspect_exit_order=inspect)
    for i in range(6):
        state = replace(_state(), symbol=f"SYMBOL{i}USDT")
        processor.request_exit_recovery(
            plan=SimpleNamespace(
                symbol=state.symbol,
                reduce_only=True,
                client_order_id=f"exit-{i}",
                intent_id=f"exit-{i}",
            ),
            known_executed_quantity=Decimal("0"),
            state=state,
        )
    await processor.recover_requested_exits()
    await processor.recover_requested_exits()
    assert reads[:5] == [f"SYMBOL{i}USDT" for i in range(5)]
    assert reads[5] == "SYMBOL5USDT"


async def test_latest_trigger_during_inspection_does_not_repeat_completed_recovery():
    from unittest.mock import AsyncMock

    processor = _processor(RecordingSubmission(None))
    processor._exit_recovery_client = object()
    state = _state()
    plan = SimpleNamespace(
        symbol=state.symbol, client_order_id="exit", intent_id="exit"
    )
    processor.request_exit_recovery(
        plan=plan, known_executed_quantity=Decimal("0"), state=state
    )

    async def recover(**kwargs):
        processor.request_exit_recovery(
            plan=plan, known_executed_quantity=Decimal("0"), state=state
        )
        return OrderExecutionResult("exit", ExchangeOrderState.ABSENT_RECONCILED, None)

    processor._recover_unknown_exit = AsyncMock(side_effect=recover)
    assert len(await processor.recover_requested_exits()) == 1
    assert await processor.recover_requested_exits() == ()
    processor._recover_unknown_exit.assert_awaited_once()


@pytest.mark.parametrize("attempt", [3, 4])
async def test_durable_recovery_attempt_does_not_stop_inspection_after_restart(attempt):
    from unittest.mock import AsyncMock

    processor = _processor(RecordingSubmission(None))  # No in-memory attempt history.
    inspector = AsyncMock(side_effect=TimeoutError("exchange unavailable"))
    processor._exit_recovery_client = SimpleNamespace(inspect_exit_order=inspector)
    plan = SimpleNamespace(
        symbol="BTCUSDT",
        reduce_only=True,
        client_order_id="persisted-recovery",
        intent_id=f"live-exit-recovery-original-{attempt}",
        position_side=SimpleNamespace(value="LONG"),
    )
    result = await processor._recover_unknown_exit(
        plan=plan,
        known_executed_quantity=Decimal("0.2"),
        state=_state(),
    )
    assert result is None
    inspector.assert_awaited_once_with(plan)
    assert not processor._submission.calls


async def test_committed_flat_receipt_invalidates_context_without_replacement():
    from unittest.mock import AsyncMock, Mock

    processor = _processor(RecordingSubmission(None))
    invalidate = Mock()
    processor._invalidate_context_cache = invalidate
    result = OrderExecutionResult("exit", ExchangeOrderState.ABSENT_RECONCILED, None)
    processor._state_machine = SimpleNamespace(
        mark_absent_reconciled=AsyncMock(return_value=result)
    )
    processor._exit_recovery_client = SimpleNamespace(
        inspect_exit_order=AsyncMock(
            return_value=SimpleNamespace(
                order=None,
                position_quantity=Decimal("0"),
                active_exit_order_client_ids=(),
                observed_at=NOW,
            )
        )
    )
    processor._context_provider = lambda state: _provide(_context())
    plan = SimpleNamespace(
        symbol="BTCUSDT",
        reduce_only=True,
        client_order_id="exit",
        intent_id="exit",
        position_side=SimpleNamespace(value="LONG"),
    )
    observed = await processor._recover_unknown_exit(
        plan=plan,
        known_executed_quantity=Decimal("0"),
        state=_state(),
    )
    assert observed == result
    invalidate.assert_called_once()
    assert not processor._submission.calls


async def test_incomplete_terminal_fill_retains_recovery_without_replacement_post():
    from unittest.mock import AsyncMock

    from crypto_momentum_lab.domain.execution.order_state import (
        ExchangeOrderSnapshot,
        FuturesPositionSide,
        OrderExecutionPlan,
    )
    from crypto_momentum_lab.domain.strategy import StrategySide
    from crypto_momentum_lab.live_rollout.exits import ManagedLivePosition

    submission = SimpleNamespace(execute=AsyncMock(return_value=None))
    processor = _processor(submission)
    plan = OrderExecutionPlan(
        "exit",
        "run-1",
        "incomplete-original",
        "BTCUSDT",
        "SELL",
        "MARKET",
        Decimal("1"),
        None,
        True,
        NOW,
        position_side=FuturesPositionSide.LONG,
        batch_id="target",
    )
    position = ManagedLivePosition(
        "BTCUSDT",
        StrategySide.LONG,
        FuturesPositionSide.LONG,
        Decimal("1"),
        Decimal("100"),
        NOW,
        batch_id="target",
        projection_version="pv_current",
    )
    context = SimpleNamespace(
        managed_positions=(position,),
        pending_position_symbols=frozenset(),
        unmanaged_position_symbols=frozenset(),
    )
    processor._context_provider = lambda state: _provide(context)
    pending = OrderExecutionResult(
        plan.client_order_id,
        ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
        "exchange-original",
        executed_quantity=Decimal("1"),
        plan=plan,
    )
    processor._state_machine = SimpleNamespace(
        apply_observed_snapshot=AsyncMock(return_value=pending),
    )
    observation = SimpleNamespace(
        order=ExchangeOrderSnapshot(
            plan.client_order_id,
            "exchange-original",
            ExchangeOrderState.FILLED,
            NOW,
            Decimal("1"),
            Decimal("0"),
        ),
        # The account position can still lag behind the already reported fill.
        position_quantity=Decimal("1"),
        active_exit_order_client_ids=(),
        observed_at=NOW,
    )
    processor._exit_recovery_client = SimpleNamespace(
        inspect_exit_order=AsyncMock(return_value=observation),
    )
    processor.request_exit_recovery(
        plan=plan,
        known_executed_quantity=Decimal("1"),
        state=_state(),
        source_candidate=replace(_intent(), reduce_only=True),
    )
    await processor.recover_requested_exits()
    assert processor.has_pending_recovery
    submission.execute.assert_not_awaited()
    processor._clock = lambda: processor._exit_recovery_next_attempt_at[
        plan.client_order_id
    ]
    observation.position_quantity = Decimal("0")
    processor._state_machine.apply_observed_snapshot.return_value = replace(
        pending,
        state=ExchangeOrderState.FILLED,
        average_price=Decimal("100"),
    )
    await processor.recover_requested_exits()
    assert not processor.has_pending_recovery
    submission.execute.assert_not_awaited()
