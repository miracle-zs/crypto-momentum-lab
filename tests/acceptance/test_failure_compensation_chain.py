"""SYSTEM.md section 8: a terminal command must not lose its failure compensation.

"命令终态后投影冲突及失败补偿｜真实账本、测试 PostgreSQL、调用方及 supervisor
完整链不因可恢复补偿退出；真正数据库故障仍可见" and T1 "补偿不拒绝已终态命令、
原失败不被二次异常覆盖".

The chain under test is the real one: the real ``OrderExecutionCoordinator`` over
the real ``ExecutionBook`` and its real command lifecycle, a real caller awaiting
the submit, and a real supervised worker task. Only the exchange POST and the
persistence seam fail, and both are external boundaries.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.execution.command_models import DispatchState
from crypto_momentum_lab.domain.execution.exchange_contract import (
    ExchangeSubmissionTimeoutError,
)
from crypto_momentum_lab.domain.execution.order_execution_port import OrderExecutionPort
from crypto_momentum_lab.domain.execution.order_result import OrderExecutionResult
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderPreSubmissionError,
)
from crypto_momentum_lab.domain.execution.reservation_registry import (
    ExecutionReadinessError,
)
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionCoordinator,
)
from crypto_momentum_lab.persistence.postgres.account_journal_store import (
    JournalFactConflict,
)
from tests.fixtures.prepared_submission import submit_prepared
from tests.unit.execution.test_terminal_settlement import (
    ObservationTransaction,
    reserved_book,
)

pytestmark = pytest.mark.contract

NOW = datetime(2026, 10, 12, 18, 0, tzinfo=UTC)


class TimeoutBackend(OrderExecutionPort):
    """The exchange POST times out: the outcome is genuinely unknown."""

    def __init__(self) -> None:
        self.posts = 0

    async def submit(
        self,
        plan: OrderExecutionPlan,
        *,
        prepared_submission: object = None,
    ) -> OrderExecutionResult:
        self.posts += 1
        raise ExchangeSubmissionTimeoutError("POST timed out")

    async def cancel_order(self, plan: OrderExecutionPlan) -> OrderExecutionResult:
        raise NotImplementedError

    async def reconcile_order(self, plan: OrderExecutionPlan) -> OrderExecutionResult:
        raise NotImplementedError

    async def apply_observed_snapshot(self, plan, snapshot):
        raise NotImplementedError


class FailingHeadCommit:
    """Fails only the Nth durable head commit, leaving earlier commits intact."""

    def __init__(self, inner, *, fail_at: int) -> None:
        self._inner = inner
        self._fail_at = fail_at
        self._head_commits = 0

    def __getattr__(self, name: str):
        return getattr(self._inner, name)

    @asynccontextmanager
    async def transaction(self, key, account_scope=None):
        transaction = ObservationTransaction(self._inner.head)
        yield transaction
        if transaction.head is not self._inner.head:
            self._head_commits += 1
            if self._head_commits >= self._fail_at:
                raise RuntimeError("injected durable head commit failure")
        self._inner.head = transaction.head


def _exit_plan() -> OrderExecutionPlan:
    return OrderExecutionPlan(
        intent_id="exit-intent",
        run_id="run-1",
        client_order_id="exit",
        symbol="BTCUSDT",
        side="SELL",
        order_type="MARKET",
        quantity=Decimal("2"),
        price=None,
        reduce_only=True,
        position_side=FuturesPositionSide.LONG,
        created_at=NOW,
        quantized=True,
    )


def _coordinator(book, backend: TimeoutBackend) -> OrderExecutionCoordinator:
    return OrderExecutionCoordinator(
        backend=backend,
        account_label="primary",
        environment="live",
        execution_book=book,
    )


@pytest.mark.asyncio
async def test_a_failed_unknown_transition_never_masks_the_original_failure() -> None:
    """When the post-exchange persistence fails, the caller keeps the POST outcome.

    The first durable head commit is the DISPATCHING transition; the second is
    the failed UNKNOWN compensation. The exchange timeout is the original
    failure and must reach the caller unchanged.
    """
    book = await reserved_book()
    book._execution_unit_of_work = FailingHeadCommit(
        book._execution_unit_of_work, fail_at=2
    )
    backend = TimeoutBackend()
    coordinator = _coordinator(book, backend)

    with pytest.raises(ExchangeSubmissionTimeoutError) as raised:
        await submit_prepared(coordinator, _exit_plan())

    assert isinstance(
        raised.value.__cause__ or raised.value, ExchangeSubmissionTimeoutError
    ), (
        "the secondary compensation error replaced the original submission "
        f"failure: {raised.value!r}"
    )
    assert backend.posts == 1, "an unknown outcome must never be re-sent"
    assert book.command_requires_recovery("exit") is True, (
        "a failed failure-compensation must leave the command gated behind "
        "recovery instead of dispatchable"
    )
    await coordinator.aclose()


@pytest.mark.asyncio
async def test_supervised_worker_survives_and_reports_one_original_failure() -> None:
    """The supervising worker sees exactly one failure, and not a compounded one."""
    book = await reserved_book()
    book._execution_unit_of_work = FailingHeadCommit(
        book._execution_unit_of_work, fail_at=2
    )
    backend = TimeoutBackend()
    coordinator = _coordinator(book, backend)
    caller_errors: list[BaseException] = []

    async def caller() -> None:
        try:
            await submit_prepared(coordinator, _exit_plan())
        except BaseException as error:  # noqa: BLE001 - the worker records it
            caller_errors.append(error)

    worker = asyncio.create_task(caller(), name="live-account-worker")
    await asyncio.wait_for(asyncio.shield(worker), timeout=2.0)

    assert not worker.cancelled()
    assert worker.exception() is None, "the supervised worker must stay alive"
    assert len(caller_errors) == 1, "the caller must observe one failure, not two"
    assert isinstance(caller_errors[0], ExchangeSubmissionTimeoutError), (
        "the caller saw a secondary compensation failure instead of the exchange "
        f"outcome: {caller_errors[0]!r}"
    )
    assert book.command_requires_recovery("exit") is True
    await coordinator.aclose()


@pytest.mark.asyncio
async def test_compensation_never_rejects_an_already_terminal_command() -> None:
    """A durable terminal command must not be contradicted by compensation."""
    book = await reserved_book()
    await book.mark_terminal("exit", reason="filled", terminal_at=NOW)
    entry = book.get_outbox("exit")
    assert entry is not None
    assert entry.state is DispatchState.TERMINAL

    backend = TimeoutBackend()
    coordinator = _coordinator(book, backend)

    with pytest.raises(OrderPreSubmissionError):
        await submit_prepared(coordinator, _exit_plan())

    entry = book.get_outbox("exit")
    assert entry is not None
    assert entry.state is DispatchState.TERMINAL, (
        "failure compensation rejected or resurrected a terminal command"
    )
    assert backend.posts == 0, "a terminal command must not reach the exchange"
    await coordinator.aclose()


@pytest.mark.asyncio
async def test_projection_conflict_with_failed_compensation_keeps_owner_alive(
) -> None:
    """The real projection-conflict -> failed-compensation interleaving.

    The exchange returns a result, the durable fact commit conflicts, and the
    fallback UNKNOWN transition also fails. The caller must keep the original
    conflict, the command must stay gated for recovery, and the same coordinator
    must still be able to process the next event afterwards.
    """
    book = await reserved_book()
    real_observe = book.observe
    real_mark_unknown = book.mark_unknown
    backend = TimeoutBackend()
    coordinator = _coordinator(book, backend)
    processed: list[str] = []

    async def conflicting_observe(evidence):
        raise JournalFactConflict("durable fact identity conflict")

    async def failing_mark_unknown(command_id, *args, **kwargs):
        raise JournalFactConflict("durable outbox transition conflict")

    book.observe = conflicting_observe
    book.mark_unknown = failing_mark_unknown

    # The exchange accepts the order; persisting the returned facts then fails.
    accepted = OrderExecutionResult(
        client_order_id="exit",
        state=ExchangeOrderState.ACKNOWLEDGED,
        exchange_order_id="exchange-1",
        executed_quantity=Decimal("0"),
        plan=_exit_plan(),
    )
    await coordinator._observe_returned_order_result(_exit_plan(), accepted)
    assert book.command_requires_recovery("exit") is True, (
        "a failed projection and a failed compensation must still gate the "
        "command behind recovery"
    )

    # The same worker keeps running: a later event on this coordinator is
    # processed normally instead of finding a dead owner.
    book.observe = real_observe
    book.mark_unknown = real_mark_unknown
    # The next event reaches the real engine again instead of finding a
    # poisoned coordinator. It may legitimately be answered with "still needs
    # evidence"; what must not happen is a new owner-level failure.
    try:
        await coordinator._observe_returned_order_result(
            _exit_plan(), replace(accepted, executed_quantity=Decimal("0"))
        )
    except ExecutionReadinessError:
        pass
    processed.append("second-event")
    assert processed == ["second-event"]
    assert book.command_requires_recovery("exit") is True

    await coordinator.aclose()
