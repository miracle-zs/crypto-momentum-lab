from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderAlreadyPreparedError,
    OrderPreSubmissionError,
    OrderRejectionOutcome,
)
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionCoordinator,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work import (
    ExecutionTransaction,
)
from crypto_momentum_lab.persistence.postgres.models import (
    ExchangeOrderRow,
)
from crypto_momentum_lab.persistence.postgres.order_submission_repository import (
    PostgresOrderSubmissionRepository,
)

NOW = datetime(2026, 10, 11, 12, 0, 0, tzinfo=UTC)


def _make_plan(
    client_order_id: str = "order-1", intent_id: str = "intent-1"
) -> OrderExecutionPlan:
    return OrderExecutionPlan(
        intent_id=intent_id,
        run_id="run-1",
        client_order_id=client_order_id,
        symbol="BTCUSDT",
        side="BUY",
        order_type="MARKET",
        quantity=Decimal("1.0"),
        price=None,
        reduce_only=False,
        position_side=FuturesPositionSide.BOTH,
        created_at=NOW,
        quantized=True,
    )


@pytest.mark.asyncio
async def test_mark_order_rejected_cascades_cleanup() -> None:
    session = AsyncMock()
    order_row = ExchangeOrderRow(
        client_order_id="order-1",
        state=ExchangeOrderState.SUBMITTING.value,
        intent_id="intent-1",
    )
    session.scalar.return_value = order_row

    tx = ExecutionTransaction(
        session,
        journal_store=AsyncMock(),
        command_repository=AsyncMock(),
        reservation_repository=AsyncMock(),
    )

    outcome = await tx.mark_order_rejected(
        "order-1",
        reason="conflict rejected",
        occurred_at=NOW,
        intent_id="intent-1",
    )

    assert outcome is OrderRejectionOutcome.REJECTED
    assert order_row.state == ExchangeOrderState.REJECTED.value
    assert order_row.updated_at == NOW
    # 4 writes: event insert, intent update, reservation update, claim update
    assert session.execute.await_count == 4


@pytest.mark.asyncio
async def test_mark_order_rejected_reports_absent_and_terminal_rows() -> None:
    session = AsyncMock()
    tx = ExecutionTransaction(
        session,
        journal_store=AsyncMock(),
        command_repository=AsyncMock(),
        reservation_repository=AsyncMock(),
    )

    # No durable order row: a prepared-but-never-dispatched command.
    session.scalar.return_value = None
    assert (
        await tx.mark_order_rejected("order-1", reason="never sent")
        is OrderRejectionOutcome.ABSENT
    )
    session.execute.assert_not_awaited()

    # A committed terminal order must not be rewritten by a late rejection.
    terminal_row = ExchangeOrderRow(
        client_order_id="order-1",
        state=ExchangeOrderState.FILLED.value,
        intent_id="intent-1",
    )
    session.scalar.return_value = terminal_row
    assert (
        await tx.mark_order_rejected(
            "order-1", reason="conflict rejected", intent_id="intent-1"
        )
        is OrderRejectionOutcome.TERMINAL
    )
    assert terminal_row.state == ExchangeOrderState.FILLED.value
    session.execute.assert_not_awaited()

    # A row bound to another intent is an identity conflict, not a rejection.
    mismatch_row = ExchangeOrderRow(
        client_order_id="order-1",
        state=ExchangeOrderState.SUBMITTING.value,
        intent_id="other-intent",
    )
    session.scalar.return_value = mismatch_row
    assert (
        await tx.mark_order_rejected(
            "order-1", reason="conflict rejected", intent_id="intent-1"
        )
        is OrderRejectionOutcome.INTENT_MISMATCH
    )
    assert mismatch_row.state == ExchangeOrderState.SUBMITTING.value
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_mark_submission_rejected_cascades_cleanup() -> None:
    session = AsyncMock()
    session.__aenter__.return_value = session
    transaction = AsyncMock()
    session.begin = Mock(return_value=transaction)

    order_row = ExchangeOrderRow(
        client_order_id="order-2",
        state=ExchangeOrderState.SUBMITTING.value,
        intent_id="intent-2",
    )
    session.scalar.return_value = order_row
    factory = Mock(return_value=session)

    repo = PostgresOrderSubmissionRepository(factory)
    outcome = await repo.mark_submission_rejected(
        "order-2",
        reason="rejected by pre-check",
        occurred_at=NOW,
        intent_id="intent-2",
    )

    assert outcome is OrderRejectionOutcome.REJECTED
    assert order_row.state == ExchangeOrderState.REJECTED.value
    assert order_row.updated_at == NOW
    assert session.execute.await_count == 4


@pytest.mark.asyncio
async def test_mark_submission_rejected_absent_row_reports_absent() -> None:
    session = AsyncMock()
    session.__aenter__.return_value = session
    transaction = AsyncMock()
    session.begin = Mock(return_value=transaction)
    session.scalar.return_value = None
    factory = Mock(return_value=session)

    repo = PostgresOrderSubmissionRepository(factory)
    outcome = await repo.mark_submission_rejected(
        "order-2",
        reason="rejected by pre-check",
        occurred_at=NOW,
        intent_id="wrong-intent",
    )

    assert outcome is OrderRejectionOutcome.ABSENT
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_coordinator_preserves_order_identity_on_prepare_collision() -> None:
    sub_repo = AsyncMock()
    book = SimpleNamespace(
        get_outbox=Mock(return_value=None),
        has_command_repository=False,
        has_reservation_repository=False,
    )
    coord = OrderExecutionCoordinator(
        backend=AsyncMock(),
        environment="live",
        account_label="primary",
        execution_book=book,
    )
    coord.configure_submission(sub_repo, clock=lambda: NOW)

    plan = _make_plan("order-1", "intent-1")

    # 1. OrderAlreadyPreparedError must NOT reject the existing order
    await coord._record_submission_failure(
        plan,
        OrderAlreadyPreparedError(),
        before_exchange_post=True,
    )
    sub_repo.mark_submission_rejected.assert_not_awaited()

    # 2. Client ID collision ValueError must NOT reject the existing order
    await coord._record_submission_failure(
        plan,
        ValueError("client order ID is already bound to another order"),
        before_exchange_post=True,
    )
    sub_repo.mark_submission_rejected.assert_not_awaited()

    # 3. Regular pre-submission error DOES reject with matching intent_id
    await coord._record_submission_failure(
        plan,
        RuntimeError("pre-submission check failed"),
        before_exchange_post=True,
    )
    sub_repo.mark_submission_rejected.assert_awaited_once_with(
        "order-1",
        reason="pre-submission check failed",
        occurred_at=NOW,
        intent_id="intent-1",
    )

    # 4. Wrapped OrderPreSubmissionError with ValueError cause must NOT reject
    wrapped_err = OrderPreSubmissionError(
        "Failed to create position entry for order-1: client order ID is already bound"
    )
    wrapped_err.__cause__ = ValueError(
        "client order ID is already bound to a different order"
    )
    await coord._record_submission_failure(
        plan,
        wrapped_err,
        before_exchange_post=True,
    )
    # Still only awaited once from step 3!
    assert sub_repo.mark_submission_rejected.await_count == 1


class _RestoredOutboxBook:
    """Storage boundary that answers from a real restored command state."""

    has_execution_unit_of_work = True
    has_command_repository = True
    has_reservation_repository = True

    def __init__(self, state) -> None:
        self._state = state
        self.mark_rejected = AsyncMock()

    def get_outbox(self, client_order_id: str):
        return self._state.outbox_by_command_id.get(client_order_id)

    async def act(self, request, prepare_submission=None):
        from crypto_momentum_lab.domain.execution.execution_action_processor import (
            accept_execution_command,
        )

        dependencies = SimpleNamespace(
            persistence_failed=lambda: False,
            book_for_key=lambda key: SimpleNamespace(
                get_view=lambda: SimpleNamespace(projection_version="pv_1")
            ),
            coordinator=None,
            reservation_repository=None,
            mark_persistence_failed=lambda: None,
            persist_outbox=AsyncMock(),
            advance_context_revision=lambda: None,
        )
        return await accept_execution_command(
            self._state,
            dependencies,
            request,
            stream_scopes_present=False,
        )


def _restored_command_state(client_order_id: str, state_value) -> SimpleNamespace:
    from crypto_momentum_lab.domain.execution.execution_action_state import (
        ExecutionActionState,
    )

    entry = SimpleNamespace(state=state_value)
    return ExecutionActionState(
        requests_by_id={},
        receipts_by_id={},
        outbox_by_command_id={client_order_id: entry},
        command_reservations={},
        recovery_required_commands=set(),
        dispatch_reconciliation_required_commands=set(),
    )


def _replay_preparation(plan):
    from dataclasses import replace as _replace

    from crypto_momentum_lab.domain.execution.order_submission import (
        OrderSubmissionPreparation,
    )
    from crypto_momentum_lab.domain.risk import RiskDecision, RiskEvaluation
    from tests.fixtures.live_market import _intent

    intent = _replace(_intent(), candidate_id=plan.intent_id, run_id=plan.run_id)
    return OrderSubmissionPreparation(
        intent=intent,
        evaluation=RiskEvaluation(
            evaluation_id="evaluation",
            candidate_id=plan.intent_id,
            decision=RiskDecision.APPROVED,
            reason="test",
            evaluated_at=NOW,
            details={},
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("reduce_only", [False, True])
async def test_restored_outbox_command_is_not_rejected_on_replay(
    reduce_only: bool,
) -> None:
    """A replay of an already-owned command must never reject or release it.

    Recovery restores ``acknowledged``/``prepared`` commands without marking
    them for dispatch reconciliation, so a same-identity resubmission reaches
    the processor's "command already exists" branch. That branch must be a
    recovery condition: neither the outbox command nor its reservation may be
    terminated by pre-submission cleanup.
    """
    from crypto_momentum_lab.domain.execution.command_models import DispatchState

    plan = OrderExecutionPlan(
        intent_id="intent-restored",
        run_id="run-1",
        client_order_id="order-restored",
        symbol="BTCUSDT",
        side="SELL" if reduce_only else "BUY",
        order_type="MARKET",
        quantity=Decimal("1.0"),
        price=None,
        reduce_only=reduce_only,
        position_side=FuturesPositionSide.BOTH,
        created_at=NOW,
        quantized=True,
        projection_version="pv_1",
        batch_id="lot-1" if reduce_only else None,
    )
    state = _restored_command_state(plan.client_order_id, DispatchState.ACKNOWLEDGED)
    book = _RestoredOutboxBook(state)
    sub_repo = AsyncMock()
    coordinator = OrderExecutionCoordinator(
        backend=AsyncMock(),
        environment="live",
        account_label="primary",
        execution_book=book,
    )
    coordinator.configure_submission(sub_repo, clock=lambda: NOW)

    try:
        result = await coordinator.prepare_and_execute(
            plan,
            preparation=_replay_preparation(plan),
        )
    finally:
        await coordinator.aclose()

    assert result is None
    # The restored identity is untouched: no rejection write, no release.
    book.mark_rejected.assert_not_awaited()
    sub_repo.mark_submission_rejected.assert_not_awaited()
    assert (
        state.outbox_by_command_id[plan.client_order_id].state
        is DispatchState.ACKNOWLEDGED
    )


@pytest.mark.asyncio
async def test_conflicting_retry_does_not_reject_the_owning_command() -> None:
    """Only the retry is refused; the accepted command keeps its identity.

    A different payload for an already accepted ``request_id`` reaches the
    processor's ``CommandConflict`` branch. Treating that as a pre-submission
    failure would ``mark_rejected`` the command that already owns the identity
    and release its reservation.
    """
    from dataclasses import replace

    from crypto_momentum_lab.domain.execution.command_models import DispatchState
    from crypto_momentum_lab.domain.execution.order_submission import (
        OrderCommandConflictError,
    )
    from tests.fixtures.live_market import _intent

    original = OrderExecutionPlan(
        intent_id="intent-conflict",
        run_id="run-1",
        client_order_id="order-conflict",
        symbol="BTCUSDT",
        side="BUY",
        order_type="MARKET",
        quantity=Decimal("1.0"),
        price=None,
        reduce_only=False,
        position_side=FuturesPositionSide.BOTH,
        created_at=NOW,
        quantized=True,
        projection_version="pv_1",
    )
    conflicting = replace(original, quantity=Decimal("2.0"))

    plan_client_id = "order-conflict"
    state = _restored_command_state(plan_client_id, DispatchState.ACKNOWLEDGED)
    book = _RestoredOutboxBook(state)
    sub_repo = AsyncMock()
    coordinator = OrderExecutionCoordinator(
        backend=AsyncMock(),
        environment="live",
        account_label="primary",
        execution_book=book,
    )
    coordinator.configure_submission(sub_repo, clock=lambda: NOW)
    # The owning command was accepted earlier with its own payload.
    state.requests_by_id[plan_client_id] = coordinator._execution_request(
        original, strategy_name=_intent().strategy_name
    )

    try:
        with pytest.raises(OrderCommandConflictError):
            await coordinator.prepare_and_execute(
                conflicting,
                preparation=_replay_preparation(conflicting),
            )
    finally:
        await coordinator.aclose()

    book.mark_rejected.assert_not_awaited()
    sub_repo.mark_submission_rejected.assert_not_awaited()
    assert (
        state.outbox_by_command_id[plan_client_id].state
        is DispatchState.ACKNOWLEDGED
    )


@pytest.mark.asyncio
async def test_coordinator_rejection_passes_intent_id_to_execution_book() -> None:
    sub_repo = AsyncMock()
    book = SimpleNamespace(
        get_outbox=Mock(return_value=Mock()),
        mark_rejected=AsyncMock(),
    )
    coord = OrderExecutionCoordinator(
        backend=AsyncMock(),
        environment="live",
        account_label="primary",
        execution_book=book,
    )
    coord.configure_submission(sub_repo, clock=lambda: NOW)

    plan = _make_plan("order-9", "intent-9")
    await coord._record_submission_failure(
        plan,
        RuntimeError("submission rejected"),
        before_exchange_post=True,
    )
    book.mark_rejected.assert_awaited_once_with(
        "order-9",
        reason="submission rejected",
        intent_id="intent-9",
    )


class _CompensationBook:
    """Book boundary that refuses rejection and fails its own cleanup."""

    has_execution_unit_of_work = True
    has_command_repository = True
    has_reservation_repository = True

    def __init__(self, *, act_error: BaseException) -> None:
        self._act_error = act_error
        self.mark_rejected = AsyncMock(side_effect=RuntimeError("db unavailable"))
        self.require_command_recovery = Mock()

    def get_outbox(self, client_order_id: str):
        return Mock()

    async def act(self, request, prepare_submission=None):
        raise self._act_error


@pytest.mark.asyncio
async def test_compensation_failure_keeps_the_original_submission_error() -> None:
    """Cleanup must never replace the failure the caller classifies.

    Production restart chain: projection conflict -> cleanup tried to reject an
    already terminal order -> the terminal-protection error escaped and killed
    the worker. The original conflict must survive, and the command must end up
    gated behind recovery instead of dispatchable.
    """
    from crypto_momentum_lab.domain.execution.order_submission import (
        OrderProjectionConflictError,
    )

    plan = _make_plan("order-conflict-chain", "intent-conflict-chain")
    plan = OrderExecutionPlan(
        intent_id=plan.intent_id,
        run_id=plan.run_id,
        client_order_id=plan.client_order_id,
        symbol=plan.symbol,
        side=plan.side,
        order_type=plan.order_type,
        quantity=plan.quantity,
        price=None,
        reduce_only=False,
        position_side=FuturesPositionSide.BOTH,
        created_at=NOW,
        quantized=True,
        projection_version="pv_1",
    )
    book = _CompensationBook(
        act_error=OrderProjectionConflictError("position projection moved")
    )
    coordinator = OrderExecutionCoordinator(
        backend=AsyncMock(),
        environment="live",
        account_label="primary",
        execution_book=book,
    )
    coordinator.configure_submission(AsyncMock(), clock=lambda: NOW)

    try:
        with pytest.raises(OrderProjectionConflictError):
            await coordinator.prepare_and_execute(
                plan,
                preparation=_replay_preparation(plan),
            )
    finally:
        await coordinator.aclose()

    # The refusal did not escape, and the command stays recoverable.
    book.require_command_recovery.assert_called_once_with(plan.client_order_id)


@pytest.mark.asyncio
async def test_compensation_helper_swallows_cleanup_failure() -> None:
    from crypto_momentum_lab.domain.execution.order_submission import (
        OrderProjectionConflictError,
    )

    plan = _make_plan("order-compensation", "intent-compensation")
    book = _CompensationBook(act_error=RuntimeError("unused"))
    coordinator = OrderExecutionCoordinator(
        backend=AsyncMock(),
        environment="live",
        account_label="primary",
        execution_book=book,
    )
    coordinator.configure_submission(AsyncMock(), clock=lambda: NOW)
    try:
        original = OrderProjectionConflictError("projection moved")
        # Must not raise, must not mask, must keep the command recoverable.
        await coordinator._compensate_submission_failure(
            plan, original, before_exchange_post=True
        )
    finally:
        await coordinator.aclose()

    book.mark_rejected.assert_awaited()
    book.require_command_recovery.assert_called_once_with(plan.client_order_id)


@pytest.mark.asyncio
async def test_terminal_first_projection_conflict_cleanup_interleaving() -> None:
    """Phase-A interleaving: WS terminal first, conflict, then cleanup.

    One order's identity must survive the whole sequence: the terminal fact
    never regresses, the higher REST quantity is counted once, the refused
    rejection neither releases the reservation nor kills the worker, and the
    command stays gated behind recovery.
    """
    from crypto_momentum_lab.domain.execution.order_result import (
        OrderExecutionResult,
    )
    from crypto_momentum_lab.domain.execution.order_state import (
        ExchangeOrderEvent,
        OrderExecutionPlan,
    )
    from crypto_momentum_lab.domain.execution.order_submission import (
        OrderProjectionConflictError,
    )
    from crypto_momentum_lab.live_rollout.pending_entries import (
        LivePendingEntryRegistry,
    )

    client_order_id = "order-interleaved"
    request_plan = OrderExecutionPlan(
        intent_id="intent-interleaved",
        run_id="run-1",
        client_order_id=client_order_id,
        symbol="BTCUSDT",
        side="BUY",
        order_type="MARKET",
        quantity=Decimal("10"),
        price=None,
        reduce_only=False,
        position_side=FuturesPositionSide.BOTH,
        created_at=NOW,
        quantized=True,
        projection_version="pv_1",
        reference_price=Decimal("100"),
    )
    registry = LivePendingEntryRegistry(clock=lambda: NOW)
    registry.remember(
        request_plan,
        OrderExecutionResult(
            client_order_id=client_order_id,
            state=ExchangeOrderState.PARTIALLY_FILLED,
            exchange_order_id="ex-interleaved",
            executed_quantity=Decimal("3"),
            plan=request_plan,
        ),
    )

    # 1. The WS terminal fact arrives first with the partial quantity.
    registry.observe_order_event(
        request_plan,
        ExchangeOrderEvent(
            event_id="evt-terminal",
            client_order_id=client_order_id,
            state=ExchangeOrderState.CANCELED,
            occurred_at=NOW,
            exchange_order_id="ex-interleaved",
            details={"executed_quantity": "3"},
        ),
    )
    # 2. The later REST result carries more executed quantity: merged once.
    registry.remember(
        request_plan,
        OrderExecutionResult(
            client_order_id=client_order_id,
            state=ExchangeOrderState.CANCELED,
            exchange_order_id="ex-interleaved",
            executed_quantity=Decimal("5"),
            plan=request_plan,
        ),
    )
    notional, symbols = registry.reservation(())
    assert notional == Decimal("500")  # 5 x 100
    assert symbols == frozenset({"BTCUSDT"})
    assert registry.snapshot() == ()

    # 3. A projection conflict on the next attempt reaches cleanup, which tries
    #    to reject the already terminal command and is refused.
    book = _CompensationBook(
        act_error=OrderProjectionConflictError("projection moved")
    )
    coordinator = OrderExecutionCoordinator(
        backend=AsyncMock(),
        environment="live",
        account_label="primary",
        execution_book=book,
    )
    coordinator.configure_submission(AsyncMock(), clock=lambda: NOW)
    try:
        with pytest.raises(OrderProjectionConflictError):
            await coordinator.prepare_and_execute(
                request_plan,
                preparation=_replay_preparation(request_plan),
            )
    finally:
        await coordinator.aclose()

    # The worker survived, the command stayed recoverable, and the terminal
    # settlement was neither released twice nor regressed.
    book.mark_rejected.assert_awaited()
    book.require_command_recovery.assert_called_once_with(client_order_id)
    notional_after, symbols_after = registry.reservation(())
    assert notional_after == notional
    assert symbols_after == symbols
