import asyncio
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import cast

import pytest

from crypto_momentum_lab.domain.execution.order_state import ExchangeOrderState
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionPort,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionResult,
)
from crypto_momentum_lab.live_rollout.context import (
    LiveContextProvider,
    LiveDaemonRuntimeContext,
)
from crypto_momentum_lab.live_rollout.exit_processor import (
    ExitProcessorConfig,
    LiveExitProcessor,
)
from crypto_momentum_lab.live_rollout.exits import LiveExitOrderRequest
from crypto_momentum_lab.live_rollout.order_identity_errors import (
    is_durable_order_identity_conflict,
)
from crypto_momentum_lab.live_rollout.submission import LiveCandidateSubmission
from tests.unit.shadow_operation.test_service import _intent, _state

NOW = datetime(2026, 7, 4, 0, 0, 20, tzinfo=UTC)


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ("client order ID is already bound to a different order", True),
        ("order already exists in terminal status", True),
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


def _acknowledged_result(*, suppressed: bool = False) -> OrderExecutionResult:
    return OrderExecutionResult(
        client_order_id="cml_exit_1",
        state=ExchangeOrderState.ACKNOWLEDGED,
        exchange_order_id="exchange-1",
        suppressed=suppressed,
    )


def _context() -> LiveDaemonRuntimeContext:
    return cast(LiveDaemonRuntimeContext, SimpleNamespace())


def _processor(
    submission: object,
    *,
    context_is_current: Callable[[LiveDaemonRuntimeContext], bool] = lambda _: True,
) -> LiveExitProcessor:
    async def provide_context(_state: MarketState15s) -> LiveDaemonRuntimeContext:
        return _context()

    async def publish_positions(_context: LiveDaemonRuntimeContext) -> None:
        return None

    return LiveExitProcessor(
        config=ExitProcessorConfig(run_id="run-1"),
        exit_manager=None,
        exit_recovery_client=None,
        state_machine=cast(OrderExecutionPort, object()),
        submission=cast(LiveCandidateSubmission, submission),
        telemetry=None,
        clock=lambda: NOW,
        is_exit_enabled=lambda: True,
        context_provider=cast(LiveContextProvider, provide_context),
        sync_pending_entry_plans=lambda _context: None,
        publish_managed_position_symbols=publish_positions,
        invalidate_context_cache=lambda: None,
        context_is_current=context_is_current,
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
    assert submission.calls == [(candidate, Decimal("0.001"), state)]


@pytest.mark.asyncio
async def test_process_requests_counts_suppressed_exit_without_exchange_submission() -> (
    None
):
    submission = RecordingSubmission(_acknowledged_result(suppressed=True))
    processor = _processor(submission)
    candidate = replace(_intent(), candidate_id="exit-suppressed", reduce_only=True)

    approved, submitted, failure = await processor.process_requests(
        (LiveExitOrderRequest(candidate=candidate, quantity=Decimal("0.001")),),
        state=_state(),
        context=_context(),
    )

    assert (approved, submitted, failure) == (1, 0, None)


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
async def test_process_requests_retries_when_context_is_fenced_during_submission() -> (
    None
):
    current = [True]

    class StaleOnceSubmission(RecordingSubmission):
        async def execute(self, *args, **kwargs):
            if not self.calls:
                current[0] = False
                self.calls.append(
                    (args[0], kwargs["requested_quantity"], kwargs["state"])
                )
                return None
            return await super().execute(*args, **kwargs)

    submission = StaleOnceSubmission(_acknowledged_result())
    context_loads = 0

    async def provide_context(_state: MarketState15s) -> LiveDaemonRuntimeContext:
        nonlocal context_loads
        context_loads += 1
        current[0] = True
        return _context()

    async def publish_positions(_context: LiveDaemonRuntimeContext) -> None:
        return None

    processor = LiveExitProcessor(
        config=ExitProcessorConfig(run_id="run-1"),
        exit_manager=None,
        exit_recovery_client=None,
        state_machine=cast(OrderExecutionPort, object()),
        submission=cast(LiveCandidateSubmission, submission),
        telemetry=None,
        clock=lambda: NOW,
        is_exit_enabled=lambda: True,
        context_provider=cast(LiveContextProvider, provide_context),
        sync_pending_entry_plans=lambda _context: None,
        publish_managed_position_symbols=publish_positions,
        invalidate_context_cache=lambda: None,
        context_is_current=lambda _context: current[0],
    )
    candidate = replace(_intent(), candidate_id="exit-stale", reduce_only=True)

    approved, submitted, failure = await processor.process_requests(
        (LiveExitOrderRequest(candidate=candidate, quantity=Decimal("0.001")),),
        state=_state(),
        context=_context(),
    )

    assert (approved, submitted, failure) == (1, 1, None)
    assert len(submission.calls) == 2
    assert context_loads == 1


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
    from crypto_momentum_lab.domain.execution.execution_coordinator import (
        ExecutionReadinessError,
    )
    from crypto_momentum_lab.domain.execution.order_submission import (
        OrderPreSubmissionError,
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

    from crypto_momentum_lab.domain.execution.execution_coordinator import (
        ExecutionReadinessError,
    )
    from crypto_momentum_lab.domain.execution.order_state import OrderExecutionPlan
    from crypto_momentum_lab.domain.execution.order_submission import (
        OrderPreSubmissionError,
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
        context=_context(),
        source_candidate=replace(_intent(), reduce_only=True),
    )
    assert result is original_receipt
    assert processor._exit_recovery_attempts["original-client-id"] == 0
    assert processor._exit_recovery_next_attempt_at["original-client-id"] > NOW
    await processor._recover_unknown_exit(
        plan=plan,
        known_executed_quantity=Decimal("0"),
        state=_state(),
        context=_context(),
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


async def test_real_book_blocked_exit_preserves_readiness_type_through_coordinator():
    from unittest.mock import MagicMock

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
    from crypto_momentum_lab.execution_account.orders.coordinator import (
        OrderExecutionCoordinator,
    )

    scope = ExecutionScope("live", "primary", "BTCUSDT", FuturesPositionSide.LONG)
    book = ExecutionBook()
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
    coordinator = OrderExecutionCoordinator(
        backend=backend,
        environment="live",
        account_label="primary",
        execution_book=book,
        reservation_repository=MagicMock(),
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
            await coordinator._ensure_reservation(plan)
            raise AssertionError("unready exit was permitted")

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
    assert result == (0, 0, "position_not_ready")
    assert not book.get_active_reservations(scope.to_position_key())
    backend.submit.assert_not_called()
