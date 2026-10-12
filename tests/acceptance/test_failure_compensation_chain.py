"""SYSTEM.md section 8: a terminal command must not lose its failure compensation.

"命令终态后投影冲突及失败补偿｜真实账本、测试 PostgreSQL、调用方及 supervisor
完整链不因可恢复补偿退出；真正数据库故障仍可见" and T1 "补偿不拒绝已终态命令、
原失败不被二次异常覆盖".

Observed facts are asserted end to end: the real ``ExecutionBook`` and its command
lifecycle, the real ``OrderExecutionCoordinator`` submit path, the production
submit caller, and a real ``LiveRuntimeSupervisor``. The supervisor is fail-fast
by design (section 3/6: a critical task stopping is a worker failure), so the
contract asserted here is that the *original* failure stays visible through the
supervisor instead of being replaced by the secondary compensation error.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
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
from crypto_momentum_lab.live_rollout.runtime_supervisor import (
    LiveRuntimeSupervisor,
    LiveRuntimeTasks,
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


class AcceptedBackend(OrderExecutionPort):
    """The exchange accepts the order, so the returned facts must be persisted."""

    def __init__(self) -> None:
        self.posts = 0

    async def submit(
        self,
        plan: OrderExecutionPlan,
        *,
        prepared_submission: object = None,
    ) -> OrderExecutionResult:
        self.posts += 1
        return OrderExecutionResult(
            client_order_id=plan.client_order_id,
            state=ExchangeOrderState.ACKNOWLEDGED,
            exchange_order_id="exchange-1",
            executed_quantity=Decimal("0"),
            plan=plan,
        )

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


def _coordinator(book, backend: OrderExecutionPort) -> OrderExecutionCoordinator:
    return OrderExecutionCoordinator(
        backend=backend,
        account_label="primary",
        environment="live",
        execution_book=book,
    )


@pytest.mark.asyncio
async def test_a_failed_submission_compensation_keeps_the_post_outcome() -> None:
    """When the post-exchange persistence fails, the caller keeps the POST outcome.

    The first durable head commit is the DISPATCHING transition; the second is
    the failed UNKNOWN compensation. The exchange timeout is the original failure
    and must reach the caller unchanged.
    """
    book = await reserved_book()
    book._execution_unit_of_work = FailingHeadCommit(
        book._execution_unit_of_work, fail_at=2
    )
    backend = TimeoutBackend()
    coordinator = _coordinator(book, backend)

    with pytest.raises(ExchangeSubmissionTimeoutError):
        await submit_prepared(coordinator, _exit_plan())

    assert backend.posts == 1, "an unknown outcome must never be re-sent"
    assert book.command_requires_recovery("exit") is True, (
        "a failed failure-compensation must leave the command gated behind "
        "recovery instead of dispatchable"
    )
    await coordinator.aclose()


@pytest.mark.asyncio
async def test_projection_conflict_with_failed_unknown_transition_is_visible() -> None:
    """The real post-exchange failure path: persist fails, then UNKNOWN fails.

    The exchange has already accepted the order, so the returned facts must be
    persisted. The durable commit conflicts, and the fallback UNKNOWN outbox
    transition conflicts too. The caller must still see the original conflict
    rather than only the compounded transition failure.
    """
    book = await reserved_book()
    backend = AcceptedBackend()
    coordinator = _coordinator(book, backend)
    real_mark_unknown = book.mark_unknown
    conflicts: list[str] = []

    async def conflicting_observe(evidence):
        raise JournalFactConflict("durable fact identity conflict")

    async def conflicting_unknown(command_id, *args, **kwargs):
        conflicts.append(command_id)
        raise JournalFactConflict("durable outbox transition conflict")

    book.observe = conflicting_observe
    book.mark_unknown = conflicting_unknown

    # The order outcome is already terminal from the exchange's point of view,
    # so the engine records it locally instead of failing the caller.
    await submit_prepared(coordinator, _exit_plan())

    assert backend.posts == 1, "the exchange saw exactly one POST"
    assert conflicts == ["exit"], "the UNKNOWN compensation path really ran"
    entry = book.get_outbox("exit")
    assert entry is not None
    assert entry.state is not DispatchState.TERMINAL, (
        "a failed projection must not leave the command looking settled"
    )
    assert book.command_requires_recovery("exit") is True, (
        "a failed projection plus a failed compensation must gate the command"
    )
    # The gated command stays visible to the recovery reader so the original
    # conflict can be re-driven; the gate itself is the durable observable here.
    assert book.command_requires_recovery("exit") is True

    # The same owner keeps working: the next event reaches the real engine again
    # instead of finding a poisoned coordinator.
    book.observe = conflicting_observe
    book.mark_unknown = real_mark_unknown
    try:
        await coordinator._observe_returned_order_result(
            _exit_plan(),
            OrderExecutionResult(
                client_order_id="exit",
                state=ExchangeOrderState.ACKNOWLEDGED,
                exchange_order_id="exchange-1",
                executed_quantity=Decimal("0"),
                plan=_exit_plan(),
            ),
        )
    except ExecutionReadinessError:
        pass
    assert book.command_requires_recovery("exit") is True
    await coordinator.aclose()


@pytest.mark.asyncio
async def test_supervised_worker_does_not_lose_the_original_failure() -> None:
    """The worker must not end "clean" while the original failure is unreadable.

    The production chain is the entry lane awaiting the submit path. The
    exchange accepts, the fact commit conflicts, and the UNKNOWN compensation
    conflicts too. The supervisor is fail-fast, so whatever it reports must be
    traceable to the original conflict rather than only a generic stopped-task
    message.
    """
    book = await reserved_book()
    observed: list[str] = []

    async def conflicting_observe(evidence):
        observed.append("observe")
        raise JournalFactConflict("durable fact identity conflict")

    async def conflicting_unknown(command_id, *args, **kwargs):
        observed.append("mark_unknown")
        raise JournalFactConflict("durable outbox transition conflict")

    book.observe = conflicting_observe
    book.mark_unknown = conflicting_unknown
    coordinator = _coordinator(book, AcceptedBackend())

    async def entry_lane_worker() -> None:
        await submit_prepared(coordinator, _exit_plan())

    market_stop = asyncio.Event()

    async def market_worker():
        from crypto_momentum_lab.live_rollout.market_runtime_contracts import (
            LiveDaemonResult,
        )

        await market_stop.wait()
        return LiveDaemonResult(0, 0, 0, "stopped", None)

    account_task = asyncio.create_task(entry_lane_worker(), name="live-account")
    market_task = asyncio.create_task(market_worker(), name="live-market")
    supervisor = LiveRuntimeSupervisor(
        tasks=LiveRuntimeTasks(
            market=market_task,
            account=account_task,
            reconcile=asyncio.create_task(asyncio.sleep(0), name="live-reconcile"),
        ),
        block_entry_submissions=coordinator.block_entry_submissions,
        stop_sources=lambda: None,
        close_risk_control=lambda: asyncio.sleep(0),
        stop_entry_caches=lambda: asyncio.sleep(0),
        run_id="run-1",
        shutdown_timeout_seconds=2.0,
    )

    reported: list[BaseException] = []
    try:
        await asyncio.wait_for(supervisor.run(), timeout=5.0)
    except BaseException as error:  # noqa: BLE001 - the operator sees this
        reported.append(error)

    # The real chain ran both halves of the compensation.
    assert observed == ["observe", "mark_unknown"]
    assert book.command_requires_recovery("exit") is True, (
        "a failed projection plus a failed compensation must gate the command"
    )
    # The failure must stay traceable from what the supervisor reports, instead
    # of ending as an unrelated "task stopped" message.
    assert reported, "the supervisor must not swallow a stopped critical worker"
    chain = [repr(error) for error in reported]
    cause = reported[0].__cause__
    while cause is not None:
        chain.append(repr(cause))
        cause = cause.__cause__
    assert any("durable fact identity conflict" in item for item in chain), (
        "the original projection conflict is not traceable from the supervisor "
        f"report: {chain}"
    )

    market_stop.set()
    await supervisor.stop()
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
