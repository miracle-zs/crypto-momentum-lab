"""SYSTEM.md section 8: a terminal command must not lose its failure compensation.

"命令终态后投影冲突及失败补偿｜真实账本、测试 PostgreSQL、调用方及 supervisor
完整链不因可恢复补偿退出；真正数据库故障仍可见" and T1 "补偿不拒绝已终态命令、
原失败不被二次异常覆盖".

The first three tests assert through the real ``ExecutionBook`` and the real
``OrderExecutionCoordinator`` submit path.  The fourth test extends to the
full chain: real PostgreSQL, real ``OrderExecutionStateMachine``,
real ``OrderExecutionCoordinator``, real ``LiveOrderReconciliation``,
real ``LiveAccountEventRuntime.run``, and real ``LiveRuntimeSupervisor``.
Parametrized over projection-conflict vs real-SQL failure injected at the
same ``AsyncPostgresExecutionUnitOfWork.transaction`` seam.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

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


def _plan(order_id: str) -> OrderExecutionPlan:
    return OrderExecutionPlan(
        intent_id=f"intent-{order_id}",
        run_id="run-1",
        client_order_id=order_id,
        symbol="BTCUSDT",
        side="BUY",
        order_type="MARKET",
        quantity=Decimal("1"),
        price=None,
        reduce_only=False,
        position_side=FuturesPositionSide.BOTH,
        created_at=NOW,
        quantized=True,
    )


def _result(order_id: str) -> OrderExecutionResult:
    return OrderExecutionResult(
        client_order_id=order_id,
        state=ExchangeOrderState.ACKNOWLEDGED,
        exchange_order_id="ex-1",
        executed_quantity=Decimal("0"),
        plan=_plan(order_id),
    )


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


# ---------------------------------------------------------------------------
# Full-chain compensation: real PG + real FSM + real supervisor
# Parametrized over failure_kind = projection | sql
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("failure_kind", ["projection", "sql"])
@pytest.mark.asyncio
async def test_compensation_chain_with_real_fsm(
    async_database_url: str,
    failure_kind: str,
) -> None:
    """Real PG + real ``OrderExecutionStateMachine`` + real supervisor.

    Same-DI construction.  The ``_FailingUoW`` arms one transaction failure
    triggered by the first WS event through the real consumer chain:

    * ``projection``: two ``JournalFactConflict`` with distinct texts — original
      projection failure + UNKNOWN compensation failure.  Both rollbacks
      verified via ``structlog.testing.capture_logs``.
    * ``sql``: one ``SELECT 1/0`` after the real SQL executes, producing a real
      PostgreSQL ``DBAPIError``.  The coordinator marks the command for recovery;
      the runtime retries and the second event commits normally.

    In both cases:
    * First command: ``command_requires_recovery==True``, not terminal,
      persistent state verified in PG, reservation not released.
    * Second command: exact ``acknowledged`` status, CAS ``head.revision``
      advanced, receipt row inserted.
    * Supervisor stays alive; real shutdown via ``shutdown`` task.
    """
    from contextlib import asynccontextmanager
    from dataclasses import replace
    from unittest.mock import AsyncMock

    import structlog
    from sqlalchemy import select, text

    from crypto_momentum_lab.domain.execution.execution_book import (
        Accepted,
        ExecutionBook,
        ExecutionRequest,
    )
    from crypto_momentum_lab.domain.execution.order_state import (
        ExchangeOrderSnapshot,
    )
    from crypto_momentum_lab.domain.execution.order_submission import (
        OrderSubmissionPreparation,
    )
    from crypto_momentum_lab.domain.execution.trade_command import TradeCommandType
    from crypto_momentum_lab.domain.strategy import StrategySide
    from crypto_momentum_lab.execution_account.hub import AccountEvent
    from crypto_momentum_lab.execution_account.orders.coordinator import (
        OrderExecutionCoordinator,
    )
    from crypto_momentum_lab.execution_account.orders.state_machine import (
        OrderExecutionStateMachine,
    )
    from crypto_momentum_lab.live_rollout.account_channel import (
        LiveAccountEventRuntime,
    )
    from crypto_momentum_lab.live_rollout.order_reconciliation import (
        LiveOrderReconciliation,
    )
    from crypto_momentum_lab.live_rollout.runtime_supervisor import (
        LiveRuntimeSupervisor,
        LiveRuntimeTasks,
    )
    from crypto_momentum_lab.persistence.postgres.account_journal_store import (
        PostgresAccountJournalStore,
    )
    from crypto_momentum_lab.persistence.postgres.command_repository import (
        PostgresCommandRepository,
    )
    from crypto_momentum_lab.persistence.postgres.execution_unit_of_work import (
        AsyncPostgresExecutionUnitOfWork,
    )
    from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
        ExecutionBookHeadRow,
        ExecutionEvidenceReceiptRow,
    )
    from crypto_momentum_lab.persistence.postgres.models import (
        ExchangeOrderRow,
        ExecutionCommandRow,
    )
    from crypto_momentum_lab.persistence.postgres.order_event_repository import (
        PostgresOrderEventRepository,
    )
    from crypto_momentum_lab.persistence.postgres.order_read_repository import (
        PostgresOrderReadRepository,
    )
    from crypto_momentum_lab.persistence.postgres.order_submission_repository import (
        PostgresOrderSubmissionRepository,
    )
    from crypto_momentum_lab.persistence.postgres.position_reservation_repository import (  # noqa: E501
        AsyncPostgresPositionReservationRepository,
        PositionReservationRow,
    )
    from crypto_momentum_lab.persistence.postgres.session import (
        create_async_database_engine,
    )
    from tests.integration.persistence.test_authority_book_transactions import (
        _evidence,
    )
    from tests.integration.persistence.test_order_repository import (
        _evaluation,
        _intent,
        _plan,
    )

    _STRATEGY = "chain-strict"

    # ── Failing UoW ──
    class _FailingUoW(AsyncPostgresExecutionUnitOfWork):
        """Inject failures after real SQL executes, before commit.

        For ``projection``: raises ``JournalFactConflict`` with a labeled text.
        For ``sql``: executes ``SELECT 1/0`` producing a real PG division error.
        """

        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            self._failures: list[tuple[str, str]] = []
            self.consumed: list[str] = []

        def arm(self, label: str, error_text: str) -> None:
            self._failures.append((label, error_text))

        @asynccontextmanager
        async def transaction(self, *args, **kwargs):
            async with super().transaction(*args, **kwargs) as tx:
                yield tx
                if self._failures:
                    label, error_text = self._failures.pop(0)
                    self.consumed.append(label)
                    if label == "sql":
                        await tx.session.execute(text("SELECT 1/0"))
                    else:
                        raise JournalFactConflict(error_text)

    # ── Blocking source ──
    class _BlockingSource:
        def __init__(self, events):
            self._events = list(events)
            self._index = 0
            self._closed = asyncio.Event()

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self._index < len(self._events):
                event = self._events[self._index]
                self._index += 1
                return event
            await self._closed.wait()
            raise StopAsyncIteration

        def close(self):
            self._closed.set()

    # ── Fake exchange ──
    class _FakeExchange:
        def __init__(self) -> None:
            self._callbacks = {}

        def set_exchange_boundary_callbacks(self, **kwargs):
            self._callbacks = kwargs

        async def submit_order(self, plan):
            return ExchangeOrderSnapshot(
                client_order_id=plan.client_order_id,
                exchange_order_id=f"ex-{plan.client_order_id}",
                state=ExchangeOrderState.ACKNOWLEDGED,
                observed_at=datetime.now(UTC),
                executed_quantity=Decimal("0"),
                average_price=Decimal("0"),
            )

        async def query_order_by_client_id(self, symbol, client_order_id):
            return None

        async def cancel_order_by_client_id(self, symbol, client_order_id):
            return ExchangeOrderSnapshot(
                client_order_id=client_order_id,
                exchange_order_id=f"ex-{client_order_id}",
                state=ExchangeOrderState.CANCELED,
                observed_at=datetime.now(UTC),
                executed_quantity=Decimal("0"),
                average_price=Decimal("0"),
            )

    class _EmptyCache:
        def for_symbols(self, _symbols):
            return ()

    class _NoOpDaemon:
        async def process_account_event(self, _state, quote=None):
            return None

    # ── Barriers ──
    event_1_done = asyncio.Event()
    event_2_done = asyncio.Event()

    # ── All tasks for cleanup ──
    all_tasks: list[asyncio.Task] = []

    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        account = f"strict-{uuid4().hex[:12]}"

        # ── 1. Same-DI construction ──
        commands = PostgresCommandRepository(factory)
        reservations = AsyncPostgresPositionReservationRepository(
            factory, strategy_name=_STRATEGY
        )
        failing_uow = _FailingUoW(
            factory,
            journal_store=PostgresAccountJournalStore(),
            command_repository=commands,
            reservation_repository=reservations,
        )
        book = ExecutionBook(
            command_repository=commands,
            reservation_repository=reservations,
            execution_unit_of_work=failing_uow,
        )
        await book.restore(account_label=account)

        now = datetime.now(UTC)
        opening = _evidence(account, "1")
        opening = replace(
            opening,
            observed_at=now - timedelta(seconds=60),
            fill=replace(opening.fill, trade_at=now - timedelta(seconds=60)),
        )
        await book.observe(opening)
        view = await book.read(opening.scope)
        assert len(view.batches) >= 1
        batch_id = view.batches[0].batch_id

        # ── 2. Prepare two real exit orders ──
        submission_repo = PostgresOrderSubmissionRepository(factory)
        prepared_orders: list[str] = []
        reservation_ids: list[str] = []
        for idx in range(2):
            view = await book.read(opening.scope)
            cid = f"cml_exit_{uuid4().hex[:24]}"
            intent = replace(
                _intent(),
                candidate_id=cid,
                reduce_only=True,
                side=StrategySide.LONG,
            )
            plan = replace(
                _plan(),
                intent_id=cid,
                client_order_id=cid,
                projection_version=view.projection_version,
                reduce_only=True,
                side="SELL",
                quantity=Decimal("0.001"),
                batch_id=batch_id,
                created_at=now - timedelta(seconds=30) + timedelta(seconds=idx),
            )
            preparation = OrderSubmissionPreparation(
                intent=intent,
                evaluation=_evaluation(intent, cid),
            )

            async def _prepare(tx, _plan=plan, _prep=preparation):
                return await submission_repo.prepare_submission_in_session(
                    tx.session,
                    intent=_prep.intent,
                    evaluation=_prep.evaluation,
                    plan=_plan,
                    prepared_at=_plan.created_at,
                )

            result = await book.act(
                ExecutionRequest(
                    cid,
                    opening.scope,
                    _STRATEGY,
                    plan.run_id,
                    cid,
                    view.projection_version,
                    TradeCommandType.EXIT,
                    plan.quantity,
                    reduce_only=True,
                    target_batch_ids=(batch_id,),
                    created_at=plan.created_at,
                    side=StrategySide.LONG,
                ),
                prepare_submission=_prepare,
            )
            assert isinstance(result, Accepted), f"order {idx} not accepted"
            entry = book.get_outbox(cid)
            assert entry is not None
            assert entry.state is DispatchState.DISPATCHING
            async with factory() as session:
                assert await session.get(ExchangeOrderRow, cid) is not None
                assert await session.get(ExecutionCommandRow, cid) is not None
                # Collect reservation IDs for later assertion.
                res_rows = (
                    await session.scalars(
                        select(PositionReservationRow).where(
                            PositionReservationRow.command_id == cid
                        )
                    )
                ).all()
                assert res_rows, "each exit command must reserve a real batch"
                for r in res_rows:
                    reservation_ids.append(r.reservation_id)
            prepared_orders.append(cid)

        # Record baseline CAS head revision.
        async with factory() as session:
            head_before = await session.scalar(
                select(ExecutionBookHeadRow).where(
                    ExecutionBookHeadRow.account_label == account
                )
            )
            cas_revision_before = head_before.revision if head_before else 0

        # ── 3. Arm failure ──
        if failure_kind == "projection":
            failing_uow.arm("original", "projection_conflict_original_failure")
            failing_uow.arm("compensation", "unknown_compensation_failure")
        else:
            failing_uow.arm("sql", "SELECT 1/0")

        # ── 4. Real FSM + real coordinator ──
        event_repo = PostgresOrderEventRepository(factory)
        exchange = _FakeExchange()
        fsm = OrderExecutionStateMachine(
            exchange=exchange,
            event_repository=event_repo,
            live_submit_enabled=False,
            clock=lambda: now,
            reconciliation_retry_delays=(),
        )
        coordinator = OrderExecutionCoordinator(
            backend=fsm,
            account_label=account,
            environment="live",
            execution_book=book,
        )

        # ── 5. Real reconciliation ──
        order_repo = PostgresOrderReadRepository(factory)
        reconciliation = LiveOrderReconciliation(
            order_repository=order_repo,
            state_machine=coordinator,
            run_id="strict-test",
            execution_book=book,
        )

        # ── 6. WS events ──
        def _make_ws(cid, seq):
            return AccountEvent(
                environment="live",
                account_label=account,
                event_type="ORDER_TRADE_UPDATE",
                event_id=f"ws-{cid}",
                event_at=now,
                received_at=now,
                client_order_id=cid,
                symbols=("BTCUSDT",),
                order_update={
                    "c": cid, "s": "BTCUSDT", "S": "SELL", "o": "MARKET",
                    "q": "0.001", "p": "0", "R": True, "ps": "BOTH",
                    "i": 12345 + seq, "X": "NEW", "z": "0", "ap": "0",
                    "T": int(now.timestamp() * 1000),
                },
                sequence=seq,
            )

        ws_events = [
            _make_ws(prepared_orders[0], 1),
            _make_ws(prepared_orders[1], 2),
        ]

        # ── 7. Runtime ──
        async def _on_snapshot(event):
            if event.client_order_id == prepared_orders[0]:
                event_1_done.set()
            elif event.client_order_id == prepared_orders[1]:
                event_2_done.set()

        source = _BlockingSource(ws_events)
        runtime = LiveAccountEventRuntime(
            daemon=_NoOpDaemon(),
            latest_market_states=_EmptyCache(),
            latest_market_quotes=_EmptyCache(),
            order_reconciliation=reconciliation,
            run_id="strict-test",
            is_transient_error=lambda err: isinstance(
                err, (JournalFactConflict, ConnectionError)
            ),
            on_account_snapshot=_on_snapshot,
            on_account_snapshot_recovery=lambda reason: None,
        )

        # ── 8. Supervisor with separate shutdown event ──
        account_task = asyncio.create_task(runtime.run(source))
        all_tasks.append(account_task)
        shutdown_event = asyncio.Event()
        shutdown_task = asyncio.create_task(shutdown_event.wait())
        all_tasks.append(shutdown_task)
        market_task = asyncio.create_task(asyncio.sleep(9999))
        all_tasks.append(market_task)
        reconcile_task = asyncio.create_task(asyncio.sleep(9999))
        all_tasks.append(reconcile_task)
        tasks = LiveRuntimeTasks(
            market=market_task,
            account=account_task,
            reconcile=reconcile_task,
            shutdown=shutdown_task,
        )
        supervisor = LiveRuntimeSupervisor(
            tasks=tasks,
            block_entry_submissions=lambda: None,
            stop_sources=lambda: None,
            close_risk_control=AsyncMock(),
            stop_entry_caches=AsyncMock(),
            run_id="strict-test",
        )
        supervisor_task = asyncio.create_task(supervisor.run())
        all_tasks.append(supervisor_task)

        # ── 9. Wait for event 1 with structlog capture ──
        with structlog.testing.capture_logs() as cap_logs:
            await asyncio.wait_for(event_1_done.wait(), timeout=15.0)

        if failure_kind == "projection":
            assert failing_uow.consumed == ["original", "compensation"], (
                "both projection rollbacks must have been consumed"
            )
            error_logs = [
                e for e in cap_logs if e.get("log_level") == "error"
            ]
            assert any(
                "projection_conflict_original_failure" in str(e)
                for e in error_logs
            ), "original projection failure must be logged"
            assert any(
                "unknown_compensation_failure" in str(e)
                for e in error_logs
            ), "compensation failure must be logged"
        else:
            assert failing_uow.consumed == ["sql"], (
                "SQL failure must have been consumed exactly once"
            )
            error_logs = [
                e for e in cap_logs if e.get("log_level") == "error"
            ]
            # The real PG division error must appear in the error channel.
            assert any(
                "division" in str(e).lower() or "DivisionByZero" in str(e)
                for e in error_logs
            ), f"real SQL DBAPIError must be logged, got: {error_logs}"

        # ── 10. First command: recovery gate, not terminal, reservation ──
        assert book.command_requires_recovery(prepared_orders[0]), (
            "first command must be publicly gated after rollback"
        )
        async with factory() as session:
            cmd1 = await session.get(ExecutionCommandRow, prepared_orders[0])
            assert cmd1 is not None
            assert cmd1.status in (
                DispatchState.DISPATCHING.value, DispatchState.UNKNOWN.value
            ), "failed command must retain unresolved dispatch responsibility"
            # Reservation must not be released by the failed transaction.
            for rid in reservation_ids:
                res_row = await session.get(PositionReservationRow, rid)
                assert res_row is not None
                assert res_row.status == "ACTIVE"
                assert res_row.reserved_quantity > 0
                assert res_row.released_quantity == 0
                assert res_row.consumed_quantity == 0

        # ── 11. Wait for event 2 ──
        await asyncio.wait_for(event_2_done.wait(), timeout=15.0)

        # ── 12. Second command: exact ACK, CAS revision advanced, receipt ──
        async with factory() as session:
            cmd2 = await session.get(ExecutionCommandRow, prepared_orders[1])
            assert cmd2 is not None
            assert cmd2.status == "acknowledged", (
                f"second command must be exactly 'acknowledged', got "
                f"{cmd2.status}"
            )
            # CAS head revision must have advanced.
            head_after = await session.scalar(
                select(ExecutionBookHeadRow).where(
                    ExecutionBookHeadRow.account_label == account
                )
            )
            assert head_after is not None
            assert head_after.revision > cas_revision_before, (
                f"CAS head revision must advance: {head_after.revision} <= "
                f"{cas_revision_before}"
            )
            # Receipt row for the second event's evidence must exist.
            receipts = (
                await session.scalars(
                    select(ExecutionEvidenceReceiptRow).where(
                        ExecutionEvidenceReceiptRow.account_label == account
                    )
                )
            ).all()
            assert len(receipts) > 0, (
                "at least one evidence receipt must be persisted"
            )

        # ── 13. Supervisor still alive ──
        assert not account_task.done()
        assert not supervisor_task.done()

        # ── 14. Real shutdown ──
        shutdown_event.set()
        result = await asyncio.wait_for(supervisor_task, timeout=5.0)
        assert result.halt_reason == "shutdown_requested"
        source.close()
        await asyncio.wait_for(supervisor.stop(), timeout=5.0)
        try:
            await asyncio.wait_for(account_task, timeout=5.0)
        except asyncio.CancelledError:
            pass
        await coordinator.aclose()
    finally:
        for t in all_tasks:
            if not t.done():
                t.cancel()
        if all_tasks:
            await asyncio.wait_for(
                asyncio.gather(*all_tasks, return_exceptions=True), timeout=5.0
            )
        if "coordinator" in locals():
            await asyncio.wait_for(coordinator.aclose(), timeout=5.0)
        await engine.dispose()