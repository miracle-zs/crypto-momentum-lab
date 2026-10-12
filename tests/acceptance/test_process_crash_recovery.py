"""SYSTEM.md section 8 acceptance: stepwise real child-process crash recovery.

Row "准备后/发送后/事实提交后/发布前逐点崩溃".

The uncovered item this file closes is that a restart which only builds a new
:class:`ExecutionBook` inside the same interpreter does not prove *process*
behaviour.  SYSTEM.md requires that process exit and restart acceptance
include real process behaviour ("进程退出和重启相关验收必须包含真实进程行为，不能全部用
同进程新建对象代替").  Every case here runs one real child interpreter
(``sys.executable -c``) against the real local PostgreSQL test database,
terminates that child with ``os._exit`` at a chosen point -- so no ``finally``,
atexit hook or garbage collection can flush anything -- and then restores the
durable state from PostgreSQL in a second, independent run.

Crash points and the contract each one proves:

* ``prepared`` -- the command and its real reservation were committed but no
  exchange POST was ever prepared.  The restart must classify the command as
  NOT sent (``PREPARED``), restore the reservation, and refuse to accept the
  same durable identity a second time.
* ``send_stage`` -- the send stage was durably recorded (command
  ``dispatching`` plus a ``SUBMITTING`` exchange order) and the child died
  inside the POST.  The restart must classify it as ``UNKNOWN``, never as
  unsent, and must keep both the reservation and the lookup responsibility.
* ``facts`` -- real fills were committed.  The restart must restore quantity,
  batch identity, weighted cost basis and trade/order identity, and an
  already-seen trade must stay counted exactly once.
* ``before_publication`` -- the accepted command transaction committed but the
  staged candidate was never published.  The restart must load everything from
  PostgreSQL (including the exit boundary committed in that same transaction)
  and must not re-synthesize facts or re-send the order.

Only the exchange network and the clock are substituted.  Persistence, the
domain engine, trade parsing, reservation accounting and the command lifecycle
are the production implementations.

Expectations are derived from an independent business example:

    1.5 BTCUSDT @ 100  -> 150 quote
    0.5 BTCUSDT @ 120  ->  60 quote
    ------------------------------------
    2.0 BTCUSDT            210 quote -> weighted entry price 105
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.domain.execution.command_models import (
    CommandExecutionState,
    DispatchState,
    ExecutionScope,
)
from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
from crypto_momentum_lab.domain.execution.execution_action_models import (
    Accepted,
    ExecutionRecoveryPending,
    ExecutionRequest,
)
from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.observation_models import (
    Applied,
    Duplicate,
)
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderSubmissionPreparation,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import PositionKey
from crypto_momentum_lab.domain.execution.trade_command import TradeCommandType
from crypto_momentum_lab.domain.trading import TradeSide
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionCoordinator,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work import (
    AsyncPostgresExecutionUnitOfWork,
    ExecutionTransaction,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
    ExecutionTradeIdentityRow,
)
from crypto_momentum_lab.persistence.postgres.models import (
    ExchangeOrderEventRow,
    ExchangeOrderRow,
    ExecutionCommandRow,
    PositionReservationRow,
)
from crypto_momentum_lab.persistence.postgres.order_submission_repository import (
    PostgresOrderSubmissionRepository,
)
from crypto_momentum_lab.persistence.postgres.position_fact_journal_models import (
    PositionFactJournalEventRow,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)
from tests.integration.persistence.test_authority_book_transactions import _book
from tests.integration.persistence.test_order_repository import (
    _evaluation,
    _intent,
    _plan,
)

pytestmark = pytest.mark.contract

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CHILD_TIMEOUT_SECONDS = 90
_SUMMARY_PREFIX = "CML_CRASH_SUMMARY "
_STRATEGY_NAME = "compression_breakout"
_RUN_ID = "process-crash-recovery"

# One distinct exit code per crash point.  The parent asserts the exact code,
# so a child that quietly completes (or fails an assertion) cannot pass.
_EXIT_PREPARED = 91
_EXIT_SEND_STAGE = 92
_EXIT_FACTS = 93
_EXIT_BEFORE_PUBLICATION = 94

_CHILD_CODE = "\n".join(
    (
        "import os, sys",
        "sys.path.insert(0, os.environ['CML_CRASH_ROOT'])",
        "from tests.acceptance.test_process_crash_recovery import run_child_process",
        "run_child_process()",
    )
)


class _CrashAfterCommitUnitOfWork(AsyncPostgresExecutionUnitOfWork):
    """Real PostgreSQL unit of work that kills the process just after COMMIT.

    Only the process-lifetime seam is added: every statement, transaction
    boundary, advisory lock and CAS check is the production implementation.
    ``os._exit`` runs once the ``session.begin()`` block has returned (the
    commit is durable) and before the caller may publish its staged candidate,
    so no ``finally`` or GC path can flush anything.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._crash_exit_code: int | None = None

    def arm(self, exit_code: int) -> None:
        self._crash_exit_code = exit_code

    @asynccontextmanager
    async def transaction(
        self,
        key: PositionKey,
        *,
        account_scope: str | None = None,
    ) -> AsyncIterator[ExecutionTransaction]:
        async with super().transaction(key, account_scope=account_scope) as tx:
            yield tx
        if self._crash_exit_code is not None:
            os._exit(self._crash_exit_code)


class _CrashingExchangeBackend:
    """Exchange network double: the POST kills the process instead of replying."""

    def __init__(self, exit_code: int) -> None:
        self._exit_code = exit_code

    async def submit(
        self,
        plan: OrderExecutionPlan,
        *,
        prepared_submission,
    ):
        os._exit(self._exit_code)


def _crash_unit_of_work(book: ExecutionBook) -> _CrashAfterCommitUnitOfWork:
    unit_of_work = book._execution_unit_of_work
    if not isinstance(unit_of_work, _CrashAfterCommitUnitOfWork):
        raise AssertionError("this crash scenario requires the crashing unit of work")
    return unit_of_work


def _scope_for(account: str) -> ExecutionScope:
    return ExecutionScope(
        environment="live",
        account_label=account,
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )


def _params(scenario: str, account: str) -> dict:
    """Deterministic inputs shared by the child and the parent assertions."""
    base = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
    fills = [
        {
            "evidence_id": f"{scenario}-ev-1",
            "trade_id": f"{scenario}-trade-1",
            "order_id": f"ord-{scenario}",
            "side": "BUY",
            "price": "100",
            "quantity": "1.5",
            "sequence": 1,
            "at": base.isoformat(),
        }
    ]
    if scenario == "facts":
        # Independent business example: 1.5@100 plus 0.5@120 is 2.0 at 105.
        fills = [
            {
                "evidence_id": "facts-ev-1",
                "trade_id": "facts-trade-1",
                "order_id": "ord-facts",
                "side": "BUY",
                "price": "100",
                "quantity": "1.5",
                "sequence": 1,
                "at": base.isoformat(),
            },
            {
                "evidence_id": "facts-ev-2",
                "trade_id": "facts-trade-2",
                "order_id": "ord-facts",
                "side": "BUY",
                "price": "120",
                "quantity": "0.5",
                "sequence": 2,
                "at": (base + timedelta(seconds=1)).isoformat(),
            },
        ]
    else:
        fills[0]["quantity"] = "2"
    return {
        "scenario": scenario,
        "account": account,
        "run_id": _RUN_ID,
        "exit_command_id": f"cml_{uuid4().hex}",
        "exit_order_created_at": (base + timedelta(seconds=30)).isoformat(),
        "fills": fills,
    }


def _fill_evidence(params: dict, index: int) -> ExecutionEvidence:
    spec = params["fills"][index]
    scope = _scope_for(params["account"])
    fill = AccountFillEvent(
        environment=scope.environment,
        account_label=scope.account_label,
        symbol=scope.symbol,
        trade_id=spec["trade_id"],
        order_id=spec["order_id"],
        side=spec["side"],
        price=Decimal(spec["price"]),
        quantity=Decimal(spec["quantity"]),
        realized_pnl=Decimal(0),
        fee=Decimal(0),
        fee_asset="USDT",
        trade_at=datetime.fromisoformat(spec["at"]),
        raw_payload={"positionSide": "BOTH"},
    )
    return ExecutionEvidence(
        evidence_id=spec["evidence_id"],
        scope=scope,
        observed_at=fill.trade_at,
        fill=fill,
        stream_id="trades",
        stream_epoch="one",
        sequence=spec["sequence"],
    )


def _exit_request(
    params: dict,
    *,
    projection_version: str,
    batch_id: str,
    quantity: str = "1",
) -> ExecutionRequest:
    return ExecutionRequest(
        request_id=params["exit_command_id"],
        scope=_scope_for(params["account"]),
        strategy_name=_STRATEGY_NAME,
        run_id=params["run_id"],
        decision_ref=params["exit_command_id"],
        expected_view_token=projection_version,
        action=TradeCommandType.EXIT,
        requested_quantity=Decimal(quantity),
        side=TradeSide.SHORT,
        order_type="MARKET",
        reduce_only=True,
        target_batch_ids=(batch_id,),
        batch_quantities={batch_id: Decimal(quantity)},
        created_at=datetime.fromisoformat(params["exit_order_created_at"]),
    )


def _submission_plan(
    params: dict,
    *,
    projection_version: str,
    batch_id: str,
    quantity: str = "1",
) -> OrderExecutionPlan:
    identity = params["exit_command_id"]
    return replace(
        _plan(),
        intent_id=identity,
        run_id=params["run_id"],
        client_order_id=identity,
        quantity=Decimal(quantity),
        side="SELL",
        reduce_only=True,
        batch_id=batch_id,
        created_at=datetime.fromisoformat(params["exit_order_created_at"]),
        projection_version=projection_version,
    )


def _preparation(params: dict, plan: OrderExecutionPlan) -> OrderSubmissionPreparation:
    identity = params["exit_command_id"]
    intent = replace(_intent(), candidate_id=identity, run_id=params["run_id"])
    return OrderSubmissionPreparation(
        intent=intent, evaluation=_evaluation(intent, identity)
    )


def _emit_summary(values: dict) -> None:
    print(_SUMMARY_PREFIX + json.dumps(values, sort_keys=True), flush=True)


def _read_summary(stdout: str) -> dict:
    for line in reversed(stdout.splitlines()):
        if line.startswith(_SUMMARY_PREFIX):
            return json.loads(line[len(_SUMMARY_PREFIX) :])
    raise AssertionError(f"child produced no summary line; stdout={stdout!r}")


def _child_database_url() -> str:
    return os.environ["CML_TEST_ASYNC_DATABASE_URL"]


async def _child_prepared_not_sent(params: dict) -> None:
    """Commit the prepared command and its reservation, then die unpublished."""
    engine = create_async_database_engine(_child_database_url())
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = params["account"]
    book = _book(factory, _CrashAfterCommitUnitOfWork)
    await book.restore(account_label=account)
    assert isinstance(await book.observe(_fill_evidence(params, 0)), Applied)
    view = await book.read(_scope_for(account))
    request = _exit_request(
        params,
        projection_version=view.projection_version,
        batch_id=view.batches[0].batch_id,
    )
    _crash_unit_of_work(book).arm(_EXIT_PREPARED)
    await book.act(request)
    raise AssertionError("the prepared-command crash point was never reached")


async def _child_send_stage(params: dict) -> None:
    """Record the send stage durably, then die inside the POST."""
    engine = create_async_database_engine(_child_database_url())
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = params["account"]
    book = _book(factory)
    await book.restore(account_label=account)
    assert isinstance(await book.observe(_fill_evidence(params, 0)), Applied)
    view = await book.read(_scope_for(account))
    plan = _submission_plan(
        params,
        projection_version=view.projection_version,
        batch_id=view.batches[0].batch_id,
    )
    coordinator = OrderExecutionCoordinator(
        backend=_CrashingExchangeBackend(_EXIT_SEND_STAGE),
        environment="live",
        account_label=account,
        execution_book=book,
        submission_repository=PostgresOrderSubmissionRepository(factory),
    )
    await coordinator.prepare_and_execute(plan, preparation=_preparation(params, plan))
    raise AssertionError("the send-stage crash point was never reached")


async def _child_facts_committed(params: dict) -> None:
    """Commit two real fills, report the committed projection, then die."""
    engine = create_async_database_engine(_child_database_url())
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = params["account"]
    book = _book(factory)
    await book.restore(account_label=account)
    assert isinstance(await book.observe(_fill_evidence(params, 0)), Applied)
    assert isinstance(await book.observe(_fill_evidence(params, 1)), Applied)
    view = await book.read(_scope_for(account))
    batch = view.batches[0]
    _emit_summary(
        {
            "projection_version": view.projection_version,
            "batch_id": batch.batch_id,
            "quantity": str(batch.quantity),
            "entry_price": str(batch.entry_price),
        }
    )
    os._exit(_EXIT_FACTS)


async def _child_before_publication(params: dict) -> None:
    """Commit the accepted exit command, then die before publication."""
    engine = create_async_database_engine(_child_database_url())
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = params["account"]
    book = _book(factory, _CrashAfterCommitUnitOfWork)
    await book.restore(account_label=account)
    assert isinstance(await book.observe(_fill_evidence(params, 0)), Applied)
    view = await book.read(_scope_for(account))
    batch_id = view.batches[0].batch_id
    plan = _submission_plan(
        params, projection_version=view.projection_version, batch_id=batch_id
    )
    preparation = _preparation(params, plan)
    request = _exit_request(
        params, projection_version=view.projection_version, batch_id=batch_id
    )
    repository = PostgresOrderSubmissionRepository(factory)

    async def prepare(tx) -> object:
        prepared = await repository.prepare_submission_in_session(
            tx.session,
            intent=preparation.intent,
            evaluation=preparation.evaluation,
            plan=plan,
            prepared_at=plan.created_at,
        )
        assert prepared is not None
        return prepared

    _crash_unit_of_work(book).arm(_EXIT_BEFORE_PUBLICATION)
    await book.act(request, prepare_submission=prepare)
    raise AssertionError("the pre-publication crash point was never reached")


_SCENARIOS = {
    "prepared": _child_prepared_not_sent,
    "send_stage": _child_send_stage,
    "facts": _child_facts_committed,
    "before_publication": _child_before_publication,
}


def run_child_process() -> None:
    """Entry point executed by the real child interpreter."""
    scenario = os.environ["CML_CRASH_SCENARIO"]
    params = json.loads(os.environ["CML_CRASH_PARAMS"])
    asyncio.run(_SCENARIOS[scenario](params))


async def _run_crash_child(
    scenario: str,
    params: dict,
    expected_exit_code: int,
    database_url: str,
) -> str:
    environment = dict(os.environ)
    environment["CML_CRASH_ROOT"] = str(_REPO_ROOT)
    environment["CML_CRASH_SCENARIO"] = scenario
    environment["CML_CRASH_PARAMS"] = json.dumps(params)
    environment["CML_TEST_ASYNC_DATABASE_URL"] = database_url
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        _CHILD_CODE,
        cwd=str(_REPO_ROOT),
        env=environment,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            child.communicate(), timeout=_CHILD_TIMEOUT_SECONDS
        )
    except TimeoutError:
        child.kill()
        await child.wait()
        raise AssertionError(
            f"child scenario {scenario!r} did not reach its crash point within "
            f"{_CHILD_TIMEOUT_SECONDS}s"
        ) from None
    assert child.returncode == expected_exit_code, (
        f"child scenario {scenario!r} exited with {child.returncode}, expected "
        f"{expected_exit_code}\nstdout={stdout.decode()}\nstderr={stderr.decode()}"
    )
    return stdout.decode()


async def _restore_book(factory, account: str) -> ExecutionBook:
    book = _book(factory)
    await book.restore(account_label=account)
    return book


async def _command_row(factory, command_id: str) -> ExecutionCommandRow:
    async with factory() as session:
        row = await session.get(ExecutionCommandRow, command_id)
    assert row is not None, "the crashed command must be durable"
    return row


async def _row_count(factory, model, *conditions) -> int:
    async with factory() as session:
        value = await session.scalar(
            select(func.count()).select_from(model).where(*conditions)
        )
    return int(value or 0)


async def test_prepared_command_restarts_as_not_sent_with_its_reservation(
    async_database_url: str,
) -> None:
    """Crash after prepare, before any POST: restart says NOT sent."""
    account = f"crash-prepared-{uuid4().hex[:12]}"
    params = _params("prepared", account)
    command_id = params["exit_command_id"]
    await _run_crash_child("prepared", params, _EXIT_PREPARED, async_database_url)

    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        # Durable ground truth before any in-memory decoding: prepared, and no
        # exchange order row exists because no POST was ever prepared.
        row = await _command_row(factory, command_id)
        assert row.status == DispatchState.PREPARED.value
        order_rows = await _row_count(
            factory, ExchangeOrderRow, ExchangeOrderRow.client_order_id == command_id
        )
        assert order_rows == 0

        book = await _restore_book(factory, account)
        entry = book.get_outbox(command_id)
        assert entry is not None
        assert entry.state is DispatchState.PREPARED
        assert entry.execution_state is CommandExecutionState.PREPARED
        assert entry.attempt_count == 0
        assert not book.command_requires_recovery(command_id)

        reservations = book.get_active_reservations()
        assert len(reservations) == 1
        reservation = reservations[0]
        assert reservation.active_quantity == Decimal("1")
        assert reservation.consumed_quantity == Decimal("0")
        assert reservation.released_quantity == Decimal("0")
        assert reservation.command_id == command_id

        scope = _scope_for(account)
        view = await book.read(scope)
        assert view.total_quantity == Decimal("2")
        assert len(view.batches) == 1
        assert reservation.batch_id == view.batches[0].batch_id

        # The same durable identity must never be accepted for a new POST.
        retry = await book.act(
            _exit_request(
                params,
                projection_version=view.projection_version,
                batch_id=view.batches[0].batch_id,
            )
        )
        assert not isinstance(retry, Accepted)
        assert isinstance(retry, ExecutionRecoveryPending)
    finally:
        await engine.dispose()


async def test_send_stage_crash_restarts_as_unknown_with_reservation(
    async_database_url: str,
) -> None:
    """Crash inside the POST: restart says UNKNOWN, keeps reservation + lookup."""
    account = f"crash-send-{uuid4().hex[:12]}"
    params = _params("send_stage", account)
    command_id = params["exit_command_id"]
    await _run_crash_child("send_stage", params, _EXIT_SEND_STAGE, async_database_url)

    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        # The send stage really was recorded before the process died.
        row = await _command_row(factory, command_id)
        assert row.status == DispatchState.DISPATCHING.value
        async with factory() as session:
            order = await session.get(ExchangeOrderRow, command_id)
        assert order is not None
        assert order.state == ExchangeOrderState.SUBMITTING.value
        assert order.executed_quantity == Decimal("0")

        book = await _restore_book(factory, account)
        entry = book.get_outbox(command_id)
        assert entry is not None
        assert entry.state is DispatchState.UNKNOWN
        assert entry.execution_state is CommandExecutionState.UNKNOWN
        assert entry.attempt_count == 1
        assert book.command_requires_recovery(command_id)

        reservations = book.get_active_reservations()
        assert len(reservations) == 1
        assert reservations[0].active_quantity == Decimal("1")
        assert reservations[0].consumed_quantity == Decimal("0")
        assert reservations[0].released_quantity == Decimal("0")

        scope = _scope_for(account)
        view = await book.read(scope)
        assert view.total_quantity == Decimal("2")
        assert len(view.batches) == 1
        assert reservations[0].batch_id == view.batches[0].batch_id

        # Never assume unsent: the durable identity must not be re-prepared.
        calls: list[object] = []

        async def forbidden_prepare(tx) -> object:
            calls.append(tx)
            raise AssertionError("an unknown command must not be re-prepared")

        retry = await book.act(
            _exit_request(
                params,
                projection_version=view.projection_version,
                batch_id=view.batches[0].batch_id,
            ),
            prepare_submission=forbidden_prepare,
        )
        assert calls == []
        assert isinstance(retry, ExecutionRecoveryPending)
        assert (
            await _row_count(
                factory,
                ExchangeOrderEventRow,
                ExchangeOrderEventRow.client_order_id == command_id,
            )
            == 1
        )
    finally:
        await engine.dispose()


async def test_committed_facts_restore_quantity_cost_and_deduplication(
    async_database_url: str,
) -> None:
    """Crash after real fills: restart restores facts exactly once."""
    account = f"crash-facts-{uuid4().hex[:12]}"
    params = _params("facts", account)
    stdout = await _run_crash_child("facts", params, _EXIT_FACTS, async_database_url)
    summary = _read_summary(stdout)

    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        book = await _restore_book(factory, account)
        scope = _scope_for(account)
        view = await book.read(scope)

        # Independent business example: 1.5@100 + 0.5@120 -> 2.0 at 105.
        assert view.total_quantity == Decimal("2")
        assert len(view.batches) == 1
        batch = view.batches[0]
        assert batch.quantity == Decimal("2")
        assert batch.entry_price == Decimal("105")
        assert batch.batch_id == summary["batch_id"]
        assert batch.quantity == Decimal(summary["quantity"])
        assert batch.entry_price == Decimal(summary["entry_price"])
        assert view.projection_version == summary["projection_version"]

        async with factory() as session:
            rows = (
                await session.scalars(
                    select(ExecutionTradeIdentityRow).where(
                        ExecutionTradeIdentityRow.account_label == account
                    )
                )
            ).all()
        assert {
            (row.trade_id, row.order_id, row.quantity, row.price) for row in rows
        } == {
            ("facts-trade-1", "ord-facts", Decimal("1.5"), Decimal("100")),
            ("facts-trade-2", "ord-facts", Decimal("0.5"), Decimal("120")),
        }
        # Two real fills were committed; a restart must not synthesize a third.
        assert (
            await _row_count(
                factory,
                PositionFactJournalEventRow,
                PositionFactJournalEventRow.account_label == account,
                PositionFactJournalEventRow.event_kind == "fill",
            )
            == 2
        )

        # An already-seen trade stays counted exactly once after the restart.
        for index in (0, 1):
            replay = await book.observe(_fill_evidence(params, index))
            assert isinstance(replay, Duplicate), replay
        after = await book.read(scope)
        assert after.total_quantity == Decimal("2")
        assert len(after.batches) == 1
        assert after.batches[0].quantity == Decimal("2")
        assert after.projection_version == view.projection_version
    finally:
        await engine.dispose()


async def test_commit_before_publication_restores_from_postgres_without_resend(
    async_database_url: str,
) -> None:
    """Commit succeeded, publication lost: restart loads PG, never re-sends."""
    account = f"crash-unpublished-{uuid4().hex[:12]}"
    params = _params("before_publication", account)
    command_id = params["exit_command_id"]
    await _run_crash_child(
        "before_publication", params, _EXIT_BEFORE_PUBLICATION, async_database_url
    )

    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        # The committed-but-unpublished transaction must be fully durable.
        row = await _command_row(factory, command_id)
        assert row.status == DispatchState.DISPATCHING.value
        async with factory() as session:
            order = await session.get(ExchangeOrderRow, command_id)
        assert order is not None
        assert order.state == ExchangeOrderState.SUBMITTING.value
        assert (
            await _row_count(
                factory,
                ExchangeOrderEventRow,
                ExchangeOrderEventRow.client_order_id == command_id,
            )
            == 1
        )

        book = await _restore_book(factory, account)
        scope = _scope_for(account)
        view = await book.read(scope)
        assert view.total_quantity == Decimal("2")
        assert len(view.batches) == 1
        assert view.batches[0].quantity == Decimal("2")
        assert view.batches[0].entry_price == Decimal("100")

        # The exit boundary committed in the same lost-publication transaction
        # must survive, because publication, not persistence, was lost.
        assert view.batches[0].exit_order_submitted_at == datetime.fromisoformat(
            params["exit_order_created_at"]
        )

        entry = book.get_outbox(command_id)
        assert entry is not None
        assert entry.state is DispatchState.UNKNOWN
        assert book.command_requires_recovery(command_id)

        reservations = book.get_active_reservations()
        assert len(reservations) == 1
        assert reservations[0].active_quantity == Decimal("1")
        assert reservations[0].batch_id == view.batches[0].batch_id

        # No re-synthesis: exactly the real trade identities and fact rows that
        # the lost-publication transaction committed.
        async with factory() as session:
            identity_rows = (
                await session.scalars(
                    select(ExecutionTradeIdentityRow).where(
                        ExecutionTradeIdentityRow.account_label == account
                    )
                )
            ).all()
        assert len(identity_rows) == 1
        assert identity_rows[0].trade_id == "before_publication-trade-1"
        assert (
            await _row_count(
                factory,
                PositionFactJournalEventRow,
                PositionFactJournalEventRow.account_label == account,
                PositionFactJournalEventRow.event_kind == "fill",
            )
            == 1
        )
        # The exit boundary was committed in that same unpublished transaction.
        assert (
            await _row_count(
                factory,
                PositionFactJournalEventRow,
                PositionFactJournalEventRow.account_label == account,
                PositionFactJournalEventRow.event_kind == "boundary",
            )
            == 1
        )

        # No re-send: the durable unknown identity must never be re-prepared.
        calls: list[object] = []

        async def forbidden_prepare(tx) -> object:
            calls.append(tx)
            raise AssertionError("an unknown command must not be re-prepared")

        retry = await book.act(
            _exit_request(
                params,
                projection_version=view.projection_version,
                batch_id=view.batches[0].batch_id,
            ),
            prepare_submission=forbidden_prepare,
        )
        assert calls == []
        assert isinstance(retry, ExecutionRecoveryPending)
        assert (
            await _row_count(
                factory,
                PositionReservationRow,
                PositionReservationRow.account_label == account,
            )
            == 1
        )
    finally:
        await engine.dispose()
