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

    success = await tx.mark_order_rejected(
        "order-1",
        reason="conflict rejected",
        occurred_at=NOW,
        intent_id="intent-1",
    )

    assert success is True
    assert order_row.state == ExchangeOrderState.REJECTED.value
    assert order_row.updated_at == NOW
    # 4 writes: event insert, intent update, reservation update, claim update
    assert session.execute.await_count == 4


@pytest.mark.asyncio
async def test_mark_order_rejected_mismatched_intent_returns_false() -> None:
    session = AsyncMock()
    # If intent mismatches, the DB query where-clause filters it out
    session.scalar.return_value = None

    tx = ExecutionTransaction(
        session,
        journal_store=AsyncMock(),
        command_repository=AsyncMock(),
        reservation_repository=AsyncMock(),
    )

    success = await tx.mark_order_rejected(
        "order-1",
        reason="conflict rejected",
        occurred_at=NOW,
        intent_id="other-intent",
    )

    assert success is False
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
    success = await repo.mark_submission_rejected(
        "order-2",
        reason="rejected by pre-check",
        occurred_at=NOW,
        intent_id="intent-2",
    )

    assert success is True
    assert order_row.state == ExchangeOrderState.REJECTED.value
    assert order_row.updated_at == NOW
    assert session.execute.await_count == 4


@pytest.mark.asyncio
async def test_mark_submission_rejected_not_found_returns_false() -> None:
    session = AsyncMock()
    session.__aenter__.return_value = session
    transaction = AsyncMock()
    session.begin = Mock(return_value=transaction)
    session.scalar.return_value = None
    factory = Mock(return_value=session)

    repo = PostgresOrderSubmissionRepository(factory)
    success = await repo.mark_submission_rejected(
        "order-2",
        reason="rejected by pre-check",
        occurred_at=NOW,
        intent_id="wrong-intent",
    )

    assert success is False
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
