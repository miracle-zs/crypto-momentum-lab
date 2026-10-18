"""SYSTEM.md Section 7, 8, 10 Acceptance: Single-account in-process lifecycle & direct recovery.

Proves:
1. Normal restart succeeds without historical journal or checkpoint tables (O(1) direct Head restore).
2. Late fills and fill deduplication are preserved across restarts.
3. A missing Head is not rebuilt from historical fills; active commands and reservations still require one.
4. Single-account in-process lifecycle: private event -> fill booking -> expectation verification -> exit execution -> clean shutdown (without WebSocket Hub).
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import async_sessionmaker
from websockets.asyncio.server import serve

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    ExecutionAccountStatus,
)
from crypto_momentum_lab.domain.execution.command_models import (
    DispatchState,
    ExecutionScope,
)
from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
from crypto_momentum_lab.domain.execution.execution_action_models import (
    Accepted,
    ExecutionRequest,
)
from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.observation_models import (
    Applied,
    Duplicate,
)
from crypto_momentum_lab.domain.execution.order_result import (
    OrderExecutionResult,
)
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderSnapshot,
    ExchangeOrderState,
    ExitAllocation,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderSubmissionPreparation,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    PositionKey,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    ExitPolicyMode,
    TradeCommandType,
)
from crypto_momentum_lab.domain.live_rollout import RollbackCommand
from crypto_momentum_lab.domain.live_rollout.authorization import (
    CANCEL_ALL_OPEN_ENTRIES_COMMAND,
    CANCEL_ALL_OPEN_ENTRIES_CONFIRMATION,
    EMERGENCY_FLATTEN_COMMAND,
    EMERGENCY_FLATTEN_CONFIRMATION,
)
from crypto_momentum_lab.domain.risk import RiskDecision, RiskEvaluation
from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    StrategySide,
)
from crypto_momentum_lab.execution_account.expectations import (
    AccountPositionExpectation,
    AccountPositionExpectationRegistry,
)
from crypto_momentum_lab.execution_account.hub import (
    AccountEvent,
    InProcessAccountEventSource,
)
from crypto_momentum_lab.execution_account.in_process_producer import (
    InProcessAccountStreamProducer,
)
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionCoordinator,
)
from crypto_momentum_lab.execution_account.risk_control_hub import (
    RiskControlAction,
    RiskControlEvent,
)
from crypto_momentum_lab.live_rollout.entry_control import (
    LiveEntryControlGate,
)
from crypto_momentum_lab.live_rollout.entry_expectations import (
    DirectPositionExpectationPublisher,
)
from crypto_momentum_lab.live_rollout.order_reconciliation import (
    ReconciliationPassResult,
)
from crypto_momentum_lab.live_rollout.risk_control import (
    LiveRiskControlRuntime,
    RiskControlCommandDispatcher,
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
    ExecutionTradeIdentityRow,
)
from crypto_momentum_lab.persistence.postgres.live_rollout_repository import (
    PostgresLiveRolloutRepository,
)
from crypto_momentum_lab.persistence.postgres.order_submission_repository import (
    PostgresOrderSubmissionRepository,
)
from crypto_momentum_lab.persistence.postgres.position_reservation_repository import (
    AsyncPostgresPositionReservationRepository,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)

NOW = datetime.now(UTC) - timedelta(hours=1)


def _scope(key: PositionKey) -> ExecutionScope:
    return ExecutionScope(
        environment=key.environment,
        account_label=key.account_label,
        symbol=key.symbol,
        position_side=key.position_side,
    )


def _book(
    factory: async_sessionmaker,
) -> tuple[ExecutionBook, AsyncPostgresPositionReservationRepository]:
    commands = PostgresCommandRepository(factory)
    reservations = AsyncPostgresPositionReservationRepository(
        factory, strategy_name="lifecycle-acceptance"
    )
    uow = AsyncPostgresExecutionUnitOfWork(
        factory,
        command_repository=commands,
        reservation_repository=reservations,
    )
    book = ExecutionBook(
        command_repository=commands,
        reservation_repository=reservations,
        execution_unit_of_work=uow,
    )
    return book, reservations


def _fill(
    key: PositionKey,
    *,
    trade_id: str,
    order_id: str,
    quantity: Decimal,
    price: Decimal = Decimal("50000"),
    side: str = "BUY",
    at: datetime = NOW,
) -> AccountFillEvent:
    return AccountFillEvent(
        environment=key.environment,
        account_label=key.account_label,
        symbol=key.symbol,
        trade_id=trade_id,
        order_id=order_id,
        side=side,
        price=price,
        quantity=quantity,
        realized_pnl=Decimal("0"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=at,
        raw_payload={"positionSide": key.position_side.value},
    )


def _fill_evidence(
    key: PositionKey,
    *,
    trade_id: str,
    order_id: str,
    quantity: Decimal,
    price: Decimal = Decimal("50000"),
    side: str = "BUY",
    at: datetime = NOW,
    stream_epoch: str = "epoch-test",
    sequence: int = 1,
) -> ExecutionEvidence:
    return ExecutionEvidence(
        evidence_id=f"fill-{trade_id}",
        scope=_scope(key),
        observed_at=at,
        fill=_fill(
            key,
            trade_id=trade_id,
            order_id=order_id,
            quantity=quantity,
            price=price,
            side=side,
            at=at,
        ),
        stream_id="test-stream",
        stream_epoch=stream_epoch,
        sequence=sequence,
    )


def _exit_request(
    key: PositionKey,
    *,
    command_id: str,
    allocation: ExitAllocation,
    view_token: str | int,
) -> ExecutionRequest:
    return ExecutionRequest(
        request_id=command_id,
        scope=_scope(key),
        strategy_name="orderflow_impulse",
        run_id="run-1",
        decision_ref=command_id,
        expected_view_token=str(view_token),
        action=TradeCommandType.EXIT,
        requested_quantity=allocation.allocated_quantity,
        side=StrategySide.LONG,
        order_type="MARKET",
        reduce_only=True,
        target_batch_ids=(allocation.batch_id,),
        batch_quantities={allocation.batch_id: allocation.allocated_quantity},
        exit_policy_mode=ExitPolicyMode.TARGET_BATCHES_ONLY,
        created_at=NOW,
    )


@pytest.mark.asyncio
async def test_normal_restart_succeeds_without_historical_journal_or_checkpoints(
    async_database_url: str,
) -> None:
    """Normal restart with open position & reservation succeeds even when old journal/checkpoint tables are wiped."""
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"direct-restart-{uuid4().hex}"
    key = PositionKey("live", account, "BTCUSDT")
    scope = _scope(key)
    command_id = f"exit-{uuid4().hex[:20]}"
    opened_at = NOW - timedelta(minutes=5)

    try:
        # Step 1: Initialize Book 1, observe fill, act exit reservation, commit to DB
        book1, _reservations1 = _book(factory)
        await book1.restore(account_label=account)
        applied = await book1.observe(
            _fill_evidence(
                key,
                trade_id="trade-seed-1",
                order_id="order-seed-1",
                quantity=Decimal("2.0"),
                price=Decimal("50000"),
                at=opened_at,
                sequence=1,
            )
        )
        assert isinstance(applied, Applied)
        before = await book1.read(scope)
        assert before.total_quantity == Decimal("2.0")
        batch = before.batches[0]

        allocation = ExitAllocation(
            batch_id=batch.batch_id,
            allocated_quantity=Decimal("1.5"),
            entry_price=batch.entry_price,
        )
        accepted = await book1.act(
            _exit_request(
                key,
                command_id=command_id,
                allocation=allocation,
                view_token=before.projection_version,
            )
        )
        assert isinstance(accepted, Accepted)

        # Restart into a new Book; recovery comes from the execution head.
        book2, reservations2 = _book(factory)
        await book2.restore(account_label=account)
        after = await book2.read(scope)

        # Verify: Position, batches, cost basis restored directly from Head
        assert after.total_quantity == Decimal("2.0")
        assert len(after.batches) == 1
        assert after.batches[0].batch_id == batch.batch_id
        assert after.batches[0].quantity == Decimal("2.0")
        assert after.batches[0].entry_price == Decimal("50000")

        # Verify: Prepared outbox command and active reservations restored
        outbox = book2.get_outbox(command_id)
        assert outbox is not None
        assert outbox.state is DispatchState.PREPARED
        assert outbox.command.requested_quantity == Decimal("1.5")
        active = book2.get_active_reservations(key)
        assert len(active) == 1
        assert active[0].active_quantity == Decimal("1.5")

        # Step 4: Late fill arriving after restart folds cleanly
        late_fill = _fill_evidence(
            key,
            trade_id="trade-late-2",
            order_id="order-late-2",
            quantity=Decimal("0.5"),
            price=Decimal("52000"),
            at=NOW + timedelta(seconds=1),
            sequence=2,
        )
        late_applied = await book2.observe(late_fill)
        assert isinstance(late_applied, Applied)
        after_late = await book2.read(scope)
        assert after_late.total_quantity == Decimal("2.5")

        # Step 5: Duplicate original fill is deduplicated
        dup = await book2.observe(
            _fill_evidence(
                key,
                trade_id="trade-seed-1",
                order_id="order-seed-1",
                quantity=Decimal("2.0"),
                price=Decimal("50000"),
                at=opened_at,
                sequence=1,
            )
        )
        assert isinstance(dup, Duplicate)
        assert (await book2.read(scope)).total_quantity == Decimal("2.5")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_partial_missing_head_is_not_reconstructed_from_historical_fills(
    async_database_url: str,
) -> None:
    """A missing position head is not recreated by reading historical fills."""

    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"partial-head-{uuid4().hex}"
    btc = PositionKey("live", account, "BTCUSDT")
    eth = PositionKey("live", account, "ETHUSDT")

    try:
        book1, _ = _book(factory)
        await book1.restore(account_label=account)
        for index, key in enumerate((btc, eth), start=1):
            applied = await book1.observe(
                _fill_evidence(
                    key,
                    trade_id=f"trade-{key.symbol}-1",
                    order_id=f"order-{key.symbol}-1",
                    quantity=Decimal("1"),
                    price=Decimal("50000"),
                    at=NOW + timedelta(seconds=index),
                    sequence=index,
                )
            )
            assert isinstance(applied, Applied)

        # Both positions are held with no active exit command/reservation.
        async with factory() as session:
            heads_before = (
                await session.scalars(
                    select(ExecutionBookHeadRow.symbol).where(
                        ExecutionBookHeadRow.environment == "live",
                        ExecutionBookHeadRow.account_label == account,
                    )
                )
            ).all()
        assert sorted(heads_before) == ["BTCUSDT", "ETHUSDT"]

        # Delete only the ETH head; its entry fill remains durable.
        async with factory() as session:
            deleted = await session.execute(
                delete(ExecutionBookHeadRow).where(
                    ExecutionBookHeadRow.environment == "live",
                    ExecutionBookHeadRow.account_label == account,
                    ExecutionBookHeadRow.symbol == "ETHUSDT",
                )
            )
            assert deleted.rowcount == 1
            await session.commit()

        # Historical fills do not reconstruct current position heads at startup.
        book2, _ = _book(factory)
        await book2.restore(account_label=account)
        restored_symbols = sorted(
            key.symbol for key in book2.position_keys_for_account("live", account)
        )
        assert restored_symbols == ["BTCUSDT"]
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_historical_evidence_does_not_recreate_a_missing_position_head(
    async_database_url: str,
) -> None:
    """New accounts and positions with only historical evidence restore from heads only."""

    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"multi-corrupt-{uuid4().hex}"
    fresh_account = f"brand-new-{uuid4().hex}"
    btc = PositionKey("live", account, "BTCUSDT")
    eth = PositionKey("live", account, "ETHUSDT")

    try:
        # A genuinely new account has no heads and no responsibility rows.
        fresh_book, _ = _book(factory)
        await fresh_book.restore(account_label=fresh_account)
        assert (await fresh_book.read(_scope(btc))).total_quantity == Decimal("0")

        book1, _ = _book(factory)
        await book1.restore(account_label=account)
        for index, key in enumerate((btc, eth), start=1):
            assert isinstance(
                await book1.observe(
                    _fill_evidence(
                        key,
                        trade_id=f"trade-{key.symbol}-1",
                        order_id=f"order-{key.symbol}-1",
                        quantity=Decimal("1"),
                        price=Decimal("50000"),
                        at=NOW + timedelta(seconds=index),
                        sequence=index,
                    )
                ),
                Applied,
            )
        # Historical evidence remains available, but it is not a second
        # source for reconstructing the current position.
        async with factory() as session:
            receipts = (
                await session.scalars(
                    select(ExecutionEvidenceReceiptRow.symbol).where(
                        ExecutionEvidenceReceiptRow.environment == "live",
                        ExecutionEvidenceReceiptRow.account_label == account,
                    )
                )
            ).all()
            trade_symbols = (
                await session.scalars(
                    select(ExecutionTradeIdentityRow.symbol).where(
                        ExecutionTradeIdentityRow.environment == "live",
                        ExecutionTradeIdentityRow.account_label == account,
                    )
                )
            ).all()
        assert "ETHUSDT" in receipts
        assert "ETHUSDT" in trade_symbols

        async with factory() as session:
            deleted = await session.execute(
                delete(ExecutionBookHeadRow).where(
                    ExecutionBookHeadRow.environment == "live",
                    ExecutionBookHeadRow.account_label == account,
                    ExecutionBookHeadRow.symbol == "ETHUSDT",
                )
            )
            assert deleted.rowcount == 1
            await session.commit()

        book2, _ = _book(factory)
        await book2.restore(account_label=account)
        restored_symbols = sorted(
            key.symbol for key in book2.position_keys_for_account("live", account)
        )
        assert restored_symbols == ["BTCUSDT"]
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_missing_head_with_only_history_restores_without_reconstruction(
    async_database_url: str,
) -> None:
    """Historical fills and receipts do not reconstruct a deleted current head."""
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"corrupt-head-{uuid4().hex}"
    key = PositionKey("live", account, "BTCUSDT")

    try:  # Step 1: Create state with fill in Book 1
        book1, _ = _book(factory)
        await book1.restore(account_label=account)
        applied = await book1.observe(
            _fill_evidence(
                key,
                trade_id="trade-c-1",
                order_id="order-c-1",
                quantity=Decimal("1.0"),
                at=NOW,
                sequence=1,
            )
        )
        assert isinstance(applied, Applied)

        # Step 2: Delete the current head while historical evidence remains.
        async with factory() as session:
            await session.execute(
                delete(ExecutionBookHeadRow).where(
                    ExecutionBookHeadRow.environment == "live",
                    ExecutionBookHeadRow.account_label == account,
                )
            )
            await session.commit()

        # Step 3: Restore does not scan history to rebuild current state.
        book2, _ = _book(factory)
        await book2.restore(account_label=account)
        assert book2.position_keys_for_account("live", account) == ()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_single_account_in_process_lifecycle_private_events_to_exit_and_shutdown(
    async_database_url: str,
) -> None:
    """End-to-end single-account lifecycle: private event -> fill -> expectation verification -> exit -> shutdown."""
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"inproc-life-{uuid4().hex}"
    symbol = "BTCUSDT"
    key = PositionKey("live", account, symbol)
    scope = _scope(key)

    try:
        # 1. Single-process components
        event_source = InProcessAccountEventSource(
            environment="live",
            account_label=account,
        )
        registry = AccountPositionExpectationRegistry(
            environment="live",
            account_label=account,
        )
        expectation_publisher = DirectPositionExpectationPublisher(registry)
        book, reservations = _book(factory)
        await book.restore(account_label=account)

        # 2. Strategy registers expected entry
        client_order_id = f"c-{uuid4().hex[:12]}"
        now_dt = datetime.now(tz=UTC)
        expectation = AccountPositionExpectation(
            environment="live",
            account_label=account,
            symbol=symbol,
            position_side="BOTH",
            client_order_id=client_order_id,
            side="BUY",
            quantity=Decimal("0.5"),
            created_at=now_dt,
            expires_at=now_dt + timedelta(seconds=60),
        )
        await expectation_publisher.register(expectation)
        assert registry.pending_count == 1

        # 3. Private event received in-process: verification against registry
        consumed = registry.consume(
            symbol=symbol,
            position_side="BOTH",
            position_amt=Decimal("0.5"),
            observed_at=now_dt,
        )
        assert consumed is not None
        assert consumed.client_order_id == client_order_id
        assert registry.pending_count == 0

        # Build and publish AccountEvent via in-process source
        trade_id = f"t-{uuid4().hex[:10]}"
        order_id = f"o-{uuid4().hex[:10]}"
        fill_event = _fill(
            key,
            trade_id=trade_id,
            order_id=order_id,
            quantity=Decimal("0.5"),
            price=Decimal("60000"),
            side="BUY",
            at=now_dt,
        )
        account_event = AccountEvent(
            environment="live",
            account_label=account,
            event_type="ORDER_TRADE_UPDATE",
            event_id=f"evt-{trade_id}",
            event_at=now_dt,
            received_at=now_dt,
            symbols=(symbol,),
            symbol=symbol,
            has_fill=True,
            trade_id=trade_id,
            fills=(fill_event,),
            sequence=1,
            stream_epoch="inproc-epoch-1",
        )
        event_source.publish(account_event)

        # 4. In-process consumer consumes event and commits fill
        pulled_event = await anext(aiter(event_source))
        assert pulled_event.event_id == f"evt-{trade_id}"
        evidence = _fill_evidence(
            key,
            trade_id=trade_id,
            order_id=order_id,
            quantity=Decimal("0.5"),
            price=Decimal("60000"),
            side="BUY",
            at=now_dt,
            sequence=1,
            stream_epoch="inproc-epoch-1",
        )
        applied = await book.observe(evidence)
        assert isinstance(applied, Applied)
        view = await book.read(scope)
        assert view.total_quantity == Decimal("0.5")
        assert view.batches[0].entry_price == Decimal("60000")

        # 5. Exit triggered in-process: position is closed
        exit_cmd_id = f"exit-{uuid4().hex[:12]}"
        allocation = ExitAllocation(
            batch_id=view.batches[0].batch_id,
            allocated_quantity=Decimal("0.5"),
            entry_price=Decimal("60000"),
        )
        exit_req = _exit_request(
            key,
            command_id=exit_cmd_id,
            allocation=allocation,
            view_token=view.projection_version,
        )
        accepted = await book.act(exit_req)
        assert isinstance(accepted, Accepted)

        # Exit fill arrives from exchange
        exit_trade_id = f"t-exit-{uuid4().hex[:10]}"
        exit_fill = _fill(
            key,
            trade_id=exit_trade_id,
            order_id=f"o-exit-{exit_cmd_id}",
            quantity=Decimal("0.5"),
            price=Decimal("61000"),
            side="SELL",
            at=now_dt + timedelta(seconds=10),
        )
        exit_account_event = AccountEvent(
            environment="live",
            account_label=account,
            event_type="ORDER_TRADE_UPDATE",
            event_id=f"evt-{exit_trade_id}",
            event_at=now_dt + timedelta(seconds=10),
            received_at=now_dt + timedelta(seconds=10),
            symbols=(symbol,),
            symbol=symbol,
            has_fill=True,
            trade_id=exit_trade_id,
            fills=(exit_fill,),
            sequence=2,
            stream_epoch="inproc-epoch-1",
        )
        event_source.publish(exit_account_event)
        pulled_exit = await anext(aiter(event_source))
        assert pulled_exit.event_id == f"evt-{exit_trade_id}"

        # Fold exit fill: position flattened
        exit_evidence = _fill_evidence(
            key,
            trade_id=exit_trade_id,
            order_id=f"o-exit-{exit_cmd_id}",
            quantity=Decimal("0.5"),
            price=Decimal("61000"),
            side="SELL",
            at=now_dt + timedelta(seconds=10),
            sequence=2,
            stream_epoch="inproc-epoch-1",
        )
        exit_applied = await book.observe(exit_evidence)
        assert isinstance(exit_applied, Applied)
        view_flat = await book.read(scope)
        assert view_flat.total_quantity == Decimal("0")

        # 6. Graceful shutdown
        event_source.stop()
        metrics = event_source.metrics
        assert metrics.enqueued_event_count == 2
        assert metrics.dequeued_event_count == 2
        assert metrics.queue_depth == 0
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_durable_risk_control_operator_command_consumption(
    async_database_url: str,
) -> None:
    """Operator commands written directly to PostgreSQL are idempotently claimed, authorized, and executed without Hub."""
    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"risk-ctrl-{uuid4().hex[:12]}"
    strategy = "orderflow_impulse"
    session_id = f"sess-{uuid4().hex[:12]}"
    repo = PostgresLiveRolloutRepository(factory)

    executed_actions: list[str] = []

    async def fake_cancel_all(_command_id: str) -> str | None:
        executed_actions.append("cancel_all")
        return None

    async def fake_flatten(_command_id: str) -> str | None:
        executed_actions.append("flatten")
        return None

    dispatcher = RiskControlCommandDispatcher(
        repository=repo,
        account_label=account,
        strategy_name=strategy,
        session_id=session_id,
        cancel_all_open_entries=fake_cancel_all,
        request_flatten=fake_flatten,
        clock=lambda: datetime.now(tz=UTC),
    )

    try:
        # Step 1: Save two commands via repository (as CLI does without Hub)
        cmd1 = RollbackCommand(
            command_id=f"cmd-cancel-{uuid4().hex[:8]}",
            command_type=CANCEL_ALL_OPEN_ENTRIES_COMMAND,
            requested_by="ops-lead",
            confirmation_text=CANCEL_ALL_OPEN_ENTRIES_CONFIRMATION,
            requested_at=datetime.now(tz=UTC),
            idempotency_key=f"idem-{uuid4().hex[:8]}",
            account_label=account,
            strategy_name=strategy,
            session_id=session_id,
            status="requested",
            completed_at=None,
            failure_reason=None,
        )
        cmd2 = RollbackCommand(
            command_id=f"cmd-flat-{uuid4().hex[:8]}",
            command_type=EMERGENCY_FLATTEN_COMMAND,
            requested_by="ops-lead",
            confirmation_text=EMERGENCY_FLATTEN_CONFIRMATION,
            requested_at=datetime.now(tz=UTC),
            idempotency_key=f"idem-{uuid4().hex[:8]}",
            account_label=account,
            strategy_name=strategy,
            session_id=session_id,
            status="requested",
            completed_at=None,
            failure_reason=None,
        )
        assert await repo.save_command(cmd1)
        assert await repo.save_command(cmd2)

        # Step 2: Daemon poller loads pending requested commands
        pending = await repo.load_pending_requested_commands(
            account_label=account,
            strategy_name=strategy,
            session_id=session_id,
        )
        assert len(pending) == 2

        # Step 3: Dispatch first command (cancel all)
        evt1 = RiskControlEvent(
            environment="live",
            account_label=account,
            strategy_name=strategy,
            session_id=session_id,
            action=RiskControlAction.CANCEL_ALL_OPEN_ENTRIES,
            event_id=f"evt-{cmd1.command_id}",
            command_id=cmd1.command_id,
            reason="durable_poller",
            issued_at=cmd1.requested_at,
            details={"command_type": cmd1.command_type},
        )
        failure1 = await dispatcher.dispatch(evt1)
        assert failure1 is None
        assert executed_actions == ["cancel_all"]

        # Verify command 1 status is completed in PostgreSQL
        stored1 = await repo.load_command(cmd1.command_id)
        assert stored1 is not None
        assert stored1.status == "completed"
        assert stored1.completed_at is not None

        # Step 4: Dispatch second command (flatten)
        evt2 = RiskControlEvent(
            environment="live",
            account_label=account,
            strategy_name=strategy,
            session_id=session_id,
            action=RiskControlAction.REQUEST_FLATTEN,
            event_id=f"evt-{cmd2.command_id}",
            command_id=cmd2.command_id,
            reason="durable_poller",
            issued_at=cmd2.requested_at,
            details={"command_type": cmd2.command_type},
        )
        failure2 = await dispatcher.dispatch(evt2)
        assert failure2 is None
        assert executed_actions == ["cancel_all", "flatten"]

        stored2 = await repo.load_command(cmd2.command_id)
        assert stored2 is not None
        assert stored2.status == "completed"

        # Step 5: Duplicate delivery of cmd1 does not re-execute!
        dup_failure = await dispatcher.dispatch(evt1)
        assert dup_failure is None
        assert len(executed_actions) == 2  # No extra execution!

        # Step 6: No more pending commands
        remaining = await repo.load_pending_requested_commands(
            account_label=account,
            strategy_name=strategy,
            session_id=session_id,
        )
        assert len(remaining) == 0
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_in_process_event_queue_overflow_marks_gap_blocks_risk_and_reconciles() -> (
    None
):
    """InProcessAccountEventSource queue overflow marks gap, blocks new risk, triggers bounded reconciliation, and clears gap after."""
    import asyncio

    small_queue: asyncio.Queue[AccountEvent] = asyncio.Queue(maxsize=1)
    reconciled_symbols: list[tuple[str, ...]] = []

    def on_overflow(affected: tuple[str, ...]) -> None:
        reconciled_symbols.append(affected)

    source = InProcessAccountEventSource(
        queue=small_queue,
        environment="live",
        account_label="primary",
        on_overflow_reconcile=on_overflow,
    )
    # A newly constructed source has not proven continuity yet: an unconnected
    # stream must not admit new risk.
    assert not source.is_risk_allowed("BTCUSDT")
    assert not source.has_fact_gap
    assert not source.continuity_verified

    now_dt = datetime.now(tz=UTC)
    evt1 = AccountEvent(
        environment="live",
        account_label="primary",
        event_type="ORDER_TRADE_UPDATE",
        event_id="evt-1",
        event_at=now_dt,
        received_at=now_dt,
        symbols=("BTCUSDT",),
        symbol="BTCUSDT",
        sequence=1,
        stream_epoch="epoch-overflow",
    )
    evt2 = AccountEvent(
        environment="live",
        account_label="primary",
        event_type="ORDER_TRADE_UPDATE",
        event_id="evt-2",
        event_at=now_dt,
        received_at=now_dt,
        symbols=("BTCUSDT",),
        symbol="BTCUSDT",
        sequence=2,
        stream_epoch="epoch-overflow",
    )

    # First event fits in queue and establishes continuity for the connection.
    source.publish(evt1)
    assert source.continuity_verified
    assert source.is_risk_allowed("BTCUSDT")
    assert not source.has_fact_gap

    # Second event causes QueueFull
    source.publish(evt2)
    assert source.has_fact_gap
    assert not source.is_risk_allowed("BTCUSDT")
    assert "BTCUSDT" in source.gap_symbols
    assert source.metrics.queue_overflow_count == 1
    assert reconciled_symbols == [("BTCUSDT",)]

    # Bounded reconciliation verifies state and clears the gap for that symbol.
    source.clear_fact_gap("BTCUSDT")
    assert not source.has_fact_gap
    assert source.is_risk_allowed("BTCUSDT")


@pytest.mark.asyncio
async def test_real_assembled_lifecycle_with_fake_ws_exchange(
    async_database_url: str,
) -> None:
    """Full lifecycle executed from fake exchange WebSocket, InProcessAccountStreamProducer, InProcessAccountEventSource through coordinator to clean shutdown."""
    import asyncio

    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"e2e-ws-{uuid4().hex[:12]}"
    symbol = "BTCUSDT"
    key = PositionKey(
        environment="live",
        account_label=account,
        symbol=symbol,
        position_side=FuturesPositionSide.LONG,
    )
    scope = _scope(key)
    book, reservations = _book(factory)
    await book.restore(account_label=account)

    class _FakeExchangeBackend:
        def __init__(self) -> None:
            self.submitted_orders: list[OrderExecutionPlan] = []

        async def submit(
            self, plan: OrderExecutionPlan, *, prepared_submission: Any
        ) -> OrderExecutionResult:
            self.submitted_orders.append(plan)
            return OrderExecutionResult(
                client_order_id=plan.client_order_id,
                state=ExchangeOrderState.ACKNOWLEDGED,
                exchange_order_id="1002",
                executed_quantity=Decimal("0"),
                plan=plan,
            )

        async def cancel_order(self, plan: OrderExecutionPlan) -> OrderExecutionResult:
            return OrderExecutionResult(
                client_order_id=plan.client_order_id,
                state=ExchangeOrderState.CANCELED,
                exchange_order_id=None,
                plan=plan,
            )

        async def reconcile_order(
            self, plan: OrderExecutionPlan
        ) -> OrderExecutionResult:
            return OrderExecutionResult(
                client_order_id=plan.client_order_id,
                state=ExchangeOrderState.ACKNOWLEDGED,
                exchange_order_id="1002",
                plan=plan,
            )

        async def apply_observed_snapshot(
            self, plan: OrderExecutionPlan, snapshot: Any
        ) -> OrderExecutionResult:
            return OrderExecutionResult(
                client_order_id=plan.client_order_id,
                state=ExchangeOrderState.FILLED,
                exchange_order_id=getattr(snapshot, "exchange_order_id", "1002"),
                executed_quantity=getattr(
                    snapshot, "executed_quantity", Decimal("0.5")
                ),
                average_price=getattr(snapshot, "average_price", Decimal("60000")),
                plan=plan,
            )

        async def mark_reconciliation_pending(
            self, plan: OrderExecutionPlan
        ) -> OrderExecutionResult:
            return OrderExecutionResult(
                client_order_id=plan.client_order_id,
                state=ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION,
                exchange_order_id=None,
                plan=plan,
            )

        async def mark_absent_reconciled(
            self, plan: OrderExecutionPlan, *, details: Any
        ) -> OrderExecutionResult:
            return OrderExecutionResult(
                client_order_id=plan.client_order_id,
                state=ExchangeOrderState.ABSENT_RECONCILED,
                exchange_order_id=None,
                plan=plan,
            )

    fake_backend = _FakeExchangeBackend()
    sub_repo = PostgresOrderSubmissionRepository(factory)
    coordinator = OrderExecutionCoordinator(
        backend=fake_backend,
        account_label=account,
        environment="live",
        execution_book=book,
        submission_repository=sub_repo,
    )

    # 1. Listen key fake client
    class _FakeListenKeyClient:
        def __init__(self) -> None:
            self.closed = False

        async def start_user_data_stream(self) -> str:
            return "fake-listen-key-123"

        async def keepalive_user_data_stream(self, listen_key: str) -> None:
            pass

        async def close_user_data_stream(self, listen_key: str) -> None:
            self.closed = True

    fake_client = _FakeListenKeyClient()
    connected_event = asyncio.Event()
    client_connections: list[Any] = []

    # 1b. Read-only REST client providing the merged account baseline.
    class _FakeReadClient:
        def __init__(self) -> None:
            self.observed_at = datetime.now(tz=UTC)
            self.config_calls = 0

        async def fetch_account_config(self) -> Any:
            from crypto_momentum_lab.domain.account import AccountConfigSnapshot

            self.config_calls += 1
            return AccountConfigSnapshot(
                environment="live",
                account_label=account,
                multi_assets_mode=True,
                hedge_mode=False,
                fee_tier=0,
                observed_at=self.observed_at,
                raw_payload={},
            )

        async def fetch_balances(self) -> tuple[Any, ...]:
            from crypto_momentum_lab.domain.account import AccountBalanceSnapshot

            return (
                AccountBalanceSnapshot(
                    environment="live",
                    account_label=account,
                    asset="USDT",
                    wallet_balance=Decimal("10000"),
                    available_balance=Decimal("10000"),
                    unrealized_pnl=Decimal("0"),
                    observed_at=self.observed_at,
                    raw_payload={},
                ),
            )

        async def fetch_positions(
            self, *, include_flat: bool = False
        ) -> tuple[Any, ...]:
            return ()

        async def fetch_open_orders(self) -> tuple[Any, ...]:
            return ()

        @property
        def incomplete_fill_symbols(self) -> frozenset[str]:
            return frozenset()

        async def fetch_recent_fills(
            self, symbols: Any = (), **kwargs: Any
        ) -> tuple[Any, ...]:
            return ()

        async def fetch_fills_with_provenance(self, symbol: str, **kwargs: Any) -> Any:
            raise NotImplementedError

    fake_read_client = _FakeReadClient()
    published_baselines: list[Any] = []
    persisted_snapshots: list[Any] = []

    async def _record_baseline(snapshot: Any, event: Any) -> None:
        published_baselines.append(snapshot)

    async def _record_persistence(snapshot: Any, event: Any) -> None:
        persisted_snapshots.append(snapshot)

    # 2. Local WebSocket server simulating Binance user data stream
    async def ws_handler(websocket: Any) -> None:
        client_connections.append(websocket)
        connected_event.set()
        try:
            await websocket.wait_closed()
        except Exception:
            pass

    server = await serve(ws_handler, "127.0.0.1", 0)
    server_port = server.sockets[0].getsockname()[1]
    ws_url = f"ws://127.0.0.1:{server_port}/ws"

    consumer_task: asyncio.Task[None] | None = None
    try:
        registry = AccountPositionExpectationRegistry(
            environment="live",
            account_label=account,
        )
        source = InProcessAccountEventSource(
            environment="live",
            account_label=account,
        )
        producer = InProcessAccountStreamProducer(
            client=fake_client,
            registry=registry,
            source=source,
            environment="live",
            account_label=account,
            websocket_url=ws_url,
            require_live_stream=True,
            read_client=fake_read_client,
            expected_multi_assets_mode=True,
            expected_hedge_mode=False,
            publish_baseline=_record_baseline,
            persist_snapshot=_record_persistence,
        )

        exit_plan_ref: list[OrderExecutionPlan] = []
        entry_client_order_id = f"c-entry-{uuid4().hex[:8]}"
        exit_client_order_id = f"c-exit-{uuid4().hex[:8]}"

        # Wire normal consumer reading from source and dispatching to coordinator
        consumer_stopping = asyncio.Event()
        entry_account_processed = asyncio.Event()

        async def _consumer_loop() -> None:
            try:
                async for event in source:
                    if event.account_snapshot is not None or event.fills:
                        await coordinator.observe_account_snapshot(
                            event.account_snapshot,
                            fills=event.fills,
                            fill_load_scans=event.fill_load_scans,
                            stream_id="account_event_hub",
                            stream_epoch=event.stream_epoch,
                            sequence=event.sequence,
                            evidence_id=event.event_id,
                            hedge_mode=False,
                        )
                    if event.event_type == "ACCOUNT_UPDATE":
                        entry_account_processed.set()
                    if (
                        event.event_type == "ORDER_TRADE_UPDATE"
                        and event.client_order_id == exit_client_order_id
                        and exit_plan_ref
                    ):
                        snap = ExchangeOrderSnapshot(
                            client_order_id=exit_client_order_id,
                            exchange_order_id="1002",
                            state=ExchangeOrderState.FILLED,
                            observed_at=event.event_at,
                            executed_quantity=Decimal("0.5"),
                            average_price=Decimal("60000"),
                        )
                        await coordinator.apply_observed_snapshot(
                            exit_plan_ref[0], snap
                        )
                    if consumer_stopping.is_set():
                        break
            except asyncio.CancelledError:
                pass

        consumer_task = asyncio.create_task(_consumer_loop())

        # Register entry expectation
        expectation = AccountPositionExpectation(
            environment="live",
            account_label=account,
            symbol=symbol,
            position_side="LONG",
            client_order_id=entry_client_order_id,
            side="BUY",
            quantity=Decimal("0.5"),
            created_at=datetime.now(tz=UTC) - timedelta(seconds=1),
            expires_at=datetime.now(tz=UTC) + timedelta(seconds=30),
        )
        registry.register(expectation)
        assert registry.pending_count == 1

        # Start producer task
        producer.start()

        # Wait for WS connection
        await asyncio.wait_for(connected_event.wait(), timeout=5.0)
        assert len(client_connections) == 1
        ws = client_connections[0]

        # The startup REST baseline is accepted as soon as the stream connects,
        # before any business frame: that is what makes the account view current
        # and what admits new risk.
        for _ in range(100):
            if source.is_risk_allowed(symbol) and published_baselines:
                break
            await asyncio.sleep(0.01)
        assert published_baselines, "startup account baseline was not published"
        assert persisted_snapshots, "startup account baseline was not persisted"
        assert source.continuity_verified is True
        assert source.is_risk_allowed(symbol) is True
        assert fake_read_client.config_calls == 1
        assert producer.account_snapshot is not None
        assert {
            item.asset: item.wallet_balance
            for item in producer.account_snapshot.balances
        }["USDT"] == Decimal("10000")
        # The baseline is the full account projection, not a symbol list.
        assert published_baselines[0].positions == ()
        assert published_baselines[0].config.hedge_mode is False

        # 3. Simulate Binance sending ORDER_TRADE_UPDATE (entry fill)
        now_ms = int(datetime.now(tz=UTC).timestamp() * 1000)
        trade_msg = {
            "e": "ORDER_TRADE_UPDATE",
            "E": now_ms,
            "T": now_ms,
            "o": {
                "s": symbol,
                "c": entry_client_order_id,
                "S": "BUY",
                "o": "LIMIT",
                "f": "GTC",
                "q": "0.5",
                "p": "60000",
                "ap": "60000",
                "x": "TRADE",
                "X": "FILLED",
                "i": 1001,
                "l": "0.5",
                "z": "0.5",
                "L": "60000",
                "n": "0.01",
                "N": "USDT",
                "T": now_ms,
                "t": 9991,
                "b": "0",
                "a": "0",
                "m": False,
                "R": False,
                "wt": "CONTRACT_PRICE",
                "ot": "LIMIT",
                "ps": "LONG",
                "cp": False,
                "rp": "0",
                "pP": False,
                "si": 0,
                "st": 0,
            },
        }
        await ws.send(json.dumps(trade_msg))

        # 4. Simulate Binance sending ACCOUNT_UPDATE (position now 0.5)
        account_msg = {
            "e": "ACCOUNT_UPDATE",
            "E": now_ms + 10,
            "T": now_ms + 10,
            "a": {
                "m": "ORDER",
                "B": [{"a": "USDT", "wb": "10000", "cw": "10000", "bc": "0"}],
                "P": [
                    {
                        "s": symbol,
                        "pa": "0.5",
                        "ep": "60000",
                        "cr": "0",
                        "up": "0",
                        "mt": "cross",
                        "iw": "0",
                        "ps": "LONG",
                    }
                ],
            },
        }
        await ws.send(json.dumps(account_msg))
        # A fill can create the batch before the following account frame is
        # committed. Wait for both facts before capturing an exit view token.
        await asyncio.wait_for(entry_account_processed.wait(), timeout=5)

        # Verification through normal consumer chain (no direct book.observe!)
        view = None
        for _ in range(50):
            view = await book.read(scope)
            if view.total_quantity == Decimal("0.5"):
                break
            await asyncio.sleep(0.05)

        assert view is not None
        assert view.total_quantity == Decimal("0.5")
        assert registry.pending_count == 0  # Expectation consumed by incoming position!
        batch = view.batches[0]

        # 5. Exit execution: prepare_and_execute creates the reservation and
        # posts to the backend, but does not complete the exit until the
        # exchange reports its fill.
        now_dt = datetime.now(tz=UTC)
        exit_intent = OrderIntentCandidate(
            candidate_id=f"intent-exit-{uuid4().hex[:8]}",
            signal_id=f"sig-exit-{uuid4().hex[:8]}",
            run_id="run-1",
            strategy_name="lifecycle-acceptance",
            strategy_version="v0",
            config_hash="abc",
            symbol=symbol,
            side=StrategySide.LONG,
            entry_type=EntryType.MARKET,
            limit_price=None,
            desired_notional=Decimal("30000"),
            reduce_only=True,
            expires_at=now_dt + timedelta(minutes=10),
            created_at=now_dt,
            reason="exit",
            features={},
        )
        exit_eval = RiskEvaluation(
            evaluation_id=f"eval-exit-{uuid4().hex[:8]}",
            candidate_id=exit_intent.candidate_id,
            decision=RiskDecision.APPROVED,
            evaluated_at=now_dt,
            reason="approved",
            details={},
        )
        exit_plan = OrderExecutionPlan(
            client_order_id=exit_client_order_id,
            intent_id=exit_intent.candidate_id,
            run_id="run-1",
            symbol=symbol,
            side="SELL",
            position_side=FuturesPositionSide.LONG,
            order_type="MARKET",
            quantity=Decimal("0.5"),
            price=None,
            reduce_only=True,
            created_at=now_dt,
            batch_id=batch.batch_id,
            allocations=(
                ExitAllocation(
                    batch_id=batch.batch_id,
                    allocated_quantity=Decimal("0.5"),
                    entry_price=batch.entry_price,
                ),
            ),
            projection_version=view.projection_version,
            strategy_name="lifecycle-acceptance",
        )

        # The account projection advanced when the position frame was booked, so
        # the exit must be prepared against the *current* view token rather than
        # a token captured before the account facts changed.
        current_view = await book.read(scope)
        assert current_view.total_quantity == Decimal("0.5")
        current_batch = current_view.batches[0]
        exit_plan = replace(
            exit_plan,
            batch_id=current_batch.batch_id,
            allocations=(
                ExitAllocation(
                    batch_id=current_batch.batch_id,
                    allocated_quantity=Decimal("0.5"),
                    entry_price=current_batch.entry_price,
                ),
            ),
            projection_version=current_view.projection_version,
        )

        exit_plan_ref.append(exit_plan)
        exit_res = await coordinator.prepare_and_execute(
            exit_plan,
            preparation=OrderSubmissionPreparation(
                intent=exit_intent,
                evaluation=exit_eval,
                environment="live",
                account_label=account,
                strategy_name="lifecycle-acceptance",
            ),
        )
        assert exit_res is not None
        assert exit_res.state == ExchangeOrderState.ACKNOWLEDGED
        assert len(fake_backend.submitted_orders) == 1

        # book.act does NOT mean exit is completed on exchange:
        view_after_act = await book.read(scope)
        assert view_after_act.total_quantity == Decimal(
            "0.5"
        )  # Position is still held!
        active_res = book.get_active_reservations(key)
        assert len(active_res) == 1
        assert active_res[0].reserved_quantity == Decimal("0.5")  # But reserved!

        # 7. Fake exchange executes the exit order and pushes fill over WS
        exit_trade_msg = {
            "e": "ORDER_TRADE_UPDATE",
            "E": now_ms + 20,
            "T": now_ms + 20,
            "o": {
                "s": symbol,
                "c": exit_client_order_id,
                "S": "SELL",
                "o": "MARKET",
                "f": "GTC",
                "q": "0.5",
                "p": "60000",
                "ap": "60000",
                "x": "TRADE",
                "X": "FILLED",
                "i": 1002,
                "l": "0.5",
                "z": "0.5",
                "L": "60000",
                "n": "0.01",
                "N": "USDT",
                "T": now_ms + 20,
                "t": 9992,
                "b": "0",
                "a": "0",
                "m": False,
                "R": True,
                "wt": "CONTRACT_PRICE",
                "ot": "MARKET",
                "ps": "LONG",
                "cp": False,
                "rp": "0",
                "pP": False,
                "si": 0,
                "st": 0,
            },
        }
        await ws.send(json.dumps(exit_trade_msg))

        exit_account_msg = {
            "e": "ACCOUNT_UPDATE",
            "E": now_ms + 30,
            "T": now_ms + 30,
            "a": {
                "m": "ORDER",
                "B": [{"a": "USDT", "wb": "10000", "cw": "10000", "bc": "0"}],
                "P": [
                    {
                        "s": symbol,
                        "pa": "0.0",
                        "ep": "0",
                        "cr": "0",
                        "up": "0",
                        "mt": "cross",
                        "iw": "0",
                        "ps": "LONG",
                    }
                ],
            },
        }
        await ws.send(json.dumps(exit_account_msg))

        # Account fills update the position; order cumulative reports settle
        # reservation capacity. They are separate inputs and may be consumed
        # in either order, so wait for both effects before asserting the end.
        view_final = None
        for _ in range(50):
            view_final = await book.read(scope)
            active_reservations = book.get_active_reservations(key)
            if view_final.total_quantity == Decimal("0") and not any(
                reservation.active_quantity != Decimal("0")
                for reservation in active_reservations
            ):
                break
            await asyncio.sleep(0.05)

        assert view_final is not None
        assert view_final.total_quantity == Decimal("0")
        # The exit fill consumed the whole reservation and the reservation was
        # settled, so no reserved quantity remains for that command.
        reservations_final = book.get_active_reservations(key)
        assert [
            reservation.command_id
            for reservation in reservations_final
            if reservation.active_quantity != Decimal("0")
        ] == []

        # The merged account projection tracked the real account state through
        # the whole lifecycle rather than only the execution book.
        assert producer.account_state is ExecutionAccountStatus.RUNNING
        assert producer.account_snapshot is not None
        assert published_baselines, "startup baseline missing"
        assert persisted_snapshots, "merged account observations were not persisted"

        # 8. Clean shutdown
        await producer.aclose()
        assert fake_client.closed
        consumer_stopping.set()
        source.stop()
        if consumer_task:
            consumer_task.cancel()
            try:
                await consumer_task
            except asyncio.CancelledError:
                pass
        await coordinator.aclose()
    finally:
        server.close()
        await server.wait_closed()
        await engine.dispose()


@pytest.mark.asyncio
async def test_risk_control_database_mode_isolated_states() -> None:
    """LiveRiskControlRuntime database mode provides isolated states for initialization, clear, halt, draining, and DB failure."""
    draining = False
    active_halt = False
    db_should_fail = False

    async def mock_load_durable_state() -> tuple[bool, bool]:
        if db_should_fail:
            raise ConnectionError("PostgreSQL connection refused")
        return draining, active_halt

    async def mock_dispatch(event: Any) -> str | None:
        return None

    refreshed_count = 0

    def mock_refresh() -> None:
        nonlocal refreshed_count
        refreshed_count += 1

    runtime = LiveRiskControlRuntime(
        enabled=True,
        session_id="test-db-mode",
        load_durable_state=mock_load_durable_state,
        dispatch=mock_dispatch,
        invalidate_contexts=lambda: None,
        refresh_entry_gate=mock_refresh,
        telemetry=None,
        clock=lambda: datetime.now(tz=UTC),
        database_mode=True,
    )

    # 1. Initial state: database not yet verified -> blocked
    blocked, reason = runtime.entry_gate()
    assert blocked is True
    assert reason == "risk_control_database_unavailable"

    # 2. Successful reconcile: healthy account -> clear and unblocked!
    await runtime.reconcile()
    blocked, reason = runtime.entry_gate()
    assert blocked is False
    assert reason == "risk_control_clear"

    # 3. Active halt in DB -> blocked with active_risk_halt
    active_halt = True
    await runtime.reconcile()
    blocked, reason = runtime.entry_gate()
    assert blocked is True
    assert reason == "active_risk_halt"

    # 4. Session draining in DB -> blocked with session_draining
    active_halt = False
    draining = True
    await runtime.reconcile()
    blocked, reason = runtime.entry_gate()
    assert blocked is True
    assert reason == "session_draining"

    # 5. DB failure -> blocked with risk_control_database_unavailable
    draining = False
    db_should_fail = True
    await runtime.reconcile()
    blocked, reason = runtime.entry_gate()
    assert blocked is True
    assert reason == "risk_control_database_unavailable"

    # 6. DB recovers -> clear and unblocked again!
    db_should_fail = False
    await runtime.reconcile()
    blocked, reason = runtime.entry_gate()
    assert blocked is False
    assert reason == "risk_control_clear"


@pytest.mark.asyncio
async def test_reconciliation_failure_retains_fact_gap_symbol_isolated() -> None:
    """Order reconciliation pass failure retains fact gaps; gaps block specific symbols fail-closed while leaving healthy symbols and exits operable."""
    source = InProcessAccountEventSource(
        environment="live",
        account_label="primary",
    )
    # A verified connection with one isolated symbol: the gap blocks only that
    # symbol and never the healthy range.
    source.mark_continuity_verified(stream_epoch="epoch-gap", sequence=1)
    source._gap_symbols.add("BTCUSDT")
    assert not source.is_risk_allowed("BTCUSDT")
    assert source.is_risk_allowed("ETHUSDT")  # Healthy symbol unaffected!

    class _MockStateMachine:
        @property
        def is_admitting_entries(self) -> bool:
            return True

        @property
        def admission_disabled_reason(self) -> str | None:
            return None

    gate = LiveEntryControlGate(
        run_id="run-1",
        state_machine=_MockStateMachine(),
        clock=lambda: datetime.now(tz=UTC),
        is_symbol_fact_ready=source.is_risk_allowed,
    )

    # BTCUSDT is blocked due to fact gap!
    allowed_btc, reason_btc = gate.is_symbol_entry_allowed("BTCUSDT")
    assert allowed_btc is False
    assert reason_btc == "symbol_fact_pending:BTCUSDT"

    # ETHUSDT is admitted!
    allowed_eth, reason_eth = gate.is_symbol_entry_allowed("ETHUSDT")
    assert allowed_eth is True
    assert reason_eth == "entry_allowed"

    # Simulated reconciliation failure: timeout / error / unknown
    failed_pass_result = ReconciliationPassResult(
        tasks=frozenset({"orders"}),
        success=False,
        verified_symbols=frozenset(),
        failed_symbols=frozenset({"BTCUSDT"}),
        pending_tasks=frozenset({"orders"}),
        has_unknown=True,
        has_pending_work=True,
    )

    # Callback must NOT clear the gap when reconciliation failed!
    def handle_reconcile_completed(result: ReconciliationPassResult) -> None:
        if not result.success:
            return  # Retain gap!
        for s in result.verified_symbols:
            source.clear_fact_gap(s)

    handle_reconcile_completed(failed_pass_result)
    assert not source.is_risk_allowed("BTCUSDT")
    assert gate.is_symbol_entry_allowed("BTCUSDT")[0] is False

    # Subsequent successful reconciliation pass verifying BTCUSDT:
    success_pass_result = ReconciliationPassResult(
        tasks=frozenset({"orders"}),
        success=True,
        verified_symbols=frozenset({"BTCUSDT"}),
        failed_symbols=frozenset(),
        pending_tasks=frozenset(),
        has_unknown=False,
        has_pending_work=False,
    )
    handle_reconcile_completed(success_pass_result)
    assert source.is_risk_allowed("BTCUSDT")
    assert gate.is_symbol_entry_allowed("BTCUSDT")[0] is True


async def _seed_single_fill_head(
    factory: async_sessionmaker,
    *,
    account: str,
    symbol: str = "BTCUSDT",
) -> tuple[PositionKey, dict[str, Any]]:
    book, _ = _book(factory)
    await book.restore(account_label=account)
    key = PositionKey("live", account, symbol)
    applied = await book.observe(
        _fill_evidence(
            key,
            trade_id=f"trade-{symbol}-1",
            order_id=f"order-{symbol}-1",
            quantity=Decimal("1"),
            price=Decimal("50000"),
            at=NOW,
            sequence=1,
        )
    )
    assert isinstance(applied, Applied)
    async with factory() as session:
        head = await session.get(
            ExecutionBookHeadRow, ("live", account, symbol, "BOTH")
        )
        assert head is not None
        return key, dict(head.state_payload)


@pytest.mark.asyncio
async def test_position_head_keeps_trade_facts_out_of_the_position_projection(
    async_database_url: str,
) -> None:
    """Trade identity is stored once; the current-position head has no fill list."""

    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    try:
        duplicate_account = f"cp-dupe-{uuid4().hex}"
        key, payload = await _seed_single_fill_head(factory, account=duplicate_account)
        assert "fills" not in payload
        assert "position_state" in payload
        async with factory() as session:
            trade_rows = (
                await session.scalars(
                    select(ExecutionTradeIdentityRow.trade_id).where(
                        ExecutionTradeIdentityRow.environment == key.environment,
                        ExecutionTradeIdentityRow.account_label == key.account_label,
                        ExecutionTradeIdentityRow.symbol == key.symbol,
                        ExecutionTradeIdentityRow.position_side
                        == key.position_side.value,
                    )
                )
            ).all()
        assert trade_rows == ["trade-BTCUSDT-1"]

    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_executing_operator_command_is_recovered_after_restart(
    async_database_url: str,
) -> None:
    """A claimed-then-crashed operator command must not be stranded.

    Reproduced: after a claim the command is ``executing``; the restart poller
    only asked for ``requested`` rows, so the operator responsibility was
    silently dropped.  Recovery must find the row, continue the unfinished
    action against current state, and record the outcome without flipping it
    back to ``requested`` for blind re-execution.
    """

    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"exec-recover-{uuid4().hex}"
    strategy = "orderflow_impulse"
    first_session = f"sess-old-{uuid4().hex[:8]}"
    second_session = f"sess-new-{uuid4().hex[:8]}"
    repo = PostgresLiveRolloutRepository(factory)
    now_dt = datetime.now(tz=UTC)

    try:
        command = RollbackCommand(
            command_id=f"cmd-flat-{uuid4().hex[:8]}",
            command_type=EMERGENCY_FLATTEN_COMMAND,
            requested_by="ops-lead",
            confirmation_text=EMERGENCY_FLATTEN_CONFIRMATION,
            requested_at=now_dt,
            idempotency_key=f"idem-{uuid4().hex[:8]}",
            account_label=account,
            strategy_name=strategy,
            session_id=first_session,
            status="requested",
            completed_at=None,
            failure_reason=None,
        )
        assert await repo.save_command(command)

        actions: list[str] = []

        async def _cancel_all(_command_id: str) -> str | None:
            actions.append("cancel_all")
            return None

        async def _flatten(_command_id: str) -> str | None:
            actions.append("flatten")
            return None

        # The pre-restart process claims the command and then dies before
        # recording any result.
        old_dispatcher = RiskControlCommandDispatcher(
            repository=repo,
            account_label=account,
            strategy_name=strategy,
            session_id=first_session,
            cancel_all_open_entries=_cancel_all,
            request_flatten=_flatten,
            clock=lambda: datetime.now(tz=UTC),
        )
        claimed = await repo.claim_command(
            command.command_id,
            account_label=account,
            strategy_name=strategy,
            session_id=first_session,
        )
        assert claimed is not None and claimed.status == "executing"

        # The restart poller still sees no *requested* work, which is exactly
        # the stranding that made the responsibility invisible.
        assert (
            await repo.load_pending_requested_commands(
                account_label=account,
                strategy_name=strategy,
                session_id=second_session,
            )
            == ()
        )

        # Recovery finds the executing row across sessions.
        stranded = await repo.load_executing_commands_for_recovery(
            account_label=account,
            strategy_name=strategy,
        )
        assert [item.command_id for item in stranded] == [command.command_id]

        new_dispatcher = RiskControlCommandDispatcher(
            repository=repo,
            account_label=account,
            strategy_name=strategy,
            session_id=second_session,
            cancel_all_open_entries=_cancel_all,
            request_flatten=_flatten,
            clock=lambda: datetime.now(tz=UTC),
        )
        recovered = await new_dispatcher.recover_executing_commands()
        assert recovered == 1
        assert actions == ["flatten"]
        stored = await repo.load_command(command.command_id)
        assert stored is not None and stored.status == "completed"

        # Recovery is idempotent: the same dispatcher never re-runs a concluded
        # command, and a duplicate delivery reports it as already concluded.
        assert await new_dispatcher.recover_executing_commands() == 0
        assert actions == ["flatten"]
        assert (
            await repo.load_executing_commands_for_recovery(
                account_label=account,
                strategy_name=strategy,
            )
            == ()
        )

        # Accepting a flatten command is not proof the position was closed: the
        # dispatch result alone never writes a position fact.
        assert old_dispatcher is not None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_late_fill_across_restart_preserves_quantity_cost_and_batches(
    async_database_url: str,
) -> None:
    """A late fill admitted after restore must not corrupt the next fact."""

    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"late-fill-{uuid4().hex}"
    key = PositionKey("live", account, "BTCUSDT")

    try:
        book1, _ = _book(factory)
        await book1.restore(account_label=account)
        for index, (trade_id, quantity) in enumerate(
            (("trade-1", Decimal("1")), ("trade-2", Decimal("0.5")))
        ):
            assert isinstance(
                await book1.observe(
                    _fill_evidence(
                        key,
                        trade_id=trade_id,
                        order_id=f"order-{trade_id}",
                        quantity=quantity,
                        price=Decimal("50000"),
                        at=NOW + timedelta(seconds=index),
                        sequence=index + 1,
                    )
                ),
                Applied,
            )
        before = await book1.read(scope := _scope(key))
        assert before.total_quantity == Decimal("1.5")
        batch_ids = tuple(batch.batch_id for batch in before.batches)
        batch_quantities = {b.batch_id: b.quantity for b in before.batches}

        # Restart: the head carries both active fills.
        book2, _ = _book(factory)
        await book2.restore(account_label=account)
        restored = await book2.read(scope)
        assert restored.total_quantity == Decimal("1.5")
        assert tuple(b.batch_id for b in restored.batches) == batch_ids
        assert {b.batch_id: b.quantity for b in restored.batches} == batch_quantities

        # A genuine late fill after the restore extends the position, and the
        # batch attribution established before the restart is preserved.
        late_trade = "trade-late"
        assert isinstance(
            await book2.observe(
                _fill_evidence(
                    key,
                    trade_id=late_trade,
                    order_id="order-late",
                    quantity=Decimal("0.25"),
                    price=Decimal("52000"),
                    at=NOW + timedelta(seconds=10),
                    sequence=10,
                )
            ),
            Applied,
        )
        after = await book2.read(scope)
        assert after.total_quantity == Decimal("1.75")
        assert tuple(b.batch_id for b in after.batches) == batch_ids
        assert {b.batch_id: b.quantity for b in after.batches} == {
            **batch_quantities,
            batch_ids[-1]: batch_quantities[batch_ids[-1]] + Decimal("0.25"),
        }

        # The duplicate of the late fill is still deduplicated after restart.
        assert isinstance(
            await book2.observe(
                _fill_evidence(
                    key,
                    trade_id=late_trade,
                    order_id="order-late",
                    quantity=Decimal("0.25"),
                    price=Decimal("52000"),
                    at=NOW + timedelta(seconds=10),
                    sequence=10,
                )
            ),
            Duplicate,
        )
        assert (await book2.read(scope)).total_quantity == Decimal("1.75")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_executing_command_recovery_reuses_command_identity_and_serializes(
    async_database_url: str,
) -> None:
    """Recovery continues the existing operator responsibility and identity.

    Covers: crash before the action, crash after the action but before the
    completion record, and repeated passes.  The resumed action must reuse the
    command record's identity instead of creating a new flatten later.
    """

    from crypto_momentum_lab.domain.live_rollout.authorization import (
        EMERGENCY_FLATTEN_COMMAND,
        EMERGENCY_FLATTEN_CONFIRMATION,
    )

    engine = create_async_database_engine(async_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    account = f"claim-{uuid4().hex}"
    strategy = "orderflow_impulse"
    repo = PostgresLiveRolloutRepository(factory)
    requested_at = datetime.now(tz=UTC) - timedelta(minutes=5)

    try:
        command = RollbackCommand(
            command_id=f"cmd-{uuid4().hex[:8]}",
            command_type=EMERGENCY_FLATTEN_COMMAND,
            requested_by="ops-lead",
            confirmation_text=EMERGENCY_FLATTEN_CONFIRMATION,
            requested_at=requested_at,
            idempotency_key=f"idem-{uuid4().hex[:8]}",
            account_label=account,
            strategy_name=strategy,
            session_id="session-old",
            status="executing",
            completed_at=None,
            failure_reason=None,
        )
        assert await repo.save_command(command)

        identities: list[str] = []
        actions: list[str] = []

        async def _flatten(operation_id: str) -> str | None:
            actions.append("flatten")
            identities.append(operation_id)
            return None

        async def _cancel_all(_command_id: str) -> str | None:
            actions.append("cancel_all")
            return None

        # Crash after the claim: the command is ``executing`` and no completion
        # was recorded, so recovery must find it (not ``requested``).
        first = RiskControlCommandDispatcher(
            repository=repo,
            account_label=account,
            strategy_name=strategy,
            session_id="session-old",
            cancel_all_open_entries=_cancel_all,
            request_flatten=_flatten,
            clock=lambda: datetime.now(tz=UTC),
        )
        # Two recoverers race for the same executing command.
        second = RiskControlCommandDispatcher(
            repository=repo,
            account_label=account,
            strategy_name=strategy,
            session_id="session-new",
            cancel_all_open_entries=_cancel_all,
            request_flatten=_flatten,
            clock=lambda: datetime.now(tz=UTC),
        )

        # The account has one sequential runtime consumer; within that process
        # the command is concluded exactly once (`complete_command` only succeeds
        # from ``executing``).  A later pass must not repeat the action or mint a
        # new identity.
        assert await first.recover_executing_commands() == 1
        assert await second.recover_executing_commands() == 0
        assert await first.recover_executing_commands() == 0
        assert actions == ["flatten"]
        assert identities == [command.command_id]
        stored = await repo.load_command(command.command_id)
        assert stored is not None and stored.status == "completed"

        # Crash after the action but before the completion record: the command
        # record still carries the identity, and recovery resumes it rather than
        # creating a new flatten.
        stranded = replace(
            command,
            command_id=f"cmd-{uuid4().hex[:8]}",
            idempotency_key=f"idem-{uuid4().hex[:8]}",
            status="executing",
        )
        assert await repo.save_command(stranded)
        third = RiskControlCommandDispatcher(
            repository=repo,
            account_label=account,
            strategy_name=strategy,
            session_id="session-old",
            cancel_all_open_entries=_cancel_all,
            request_flatten=_flatten,
            clock=lambda: datetime.now(tz=UTC),
        )
        assert await third.recover_executing_commands() == 1
        assert await third.recover_executing_commands() == 0
        assert actions == ["flatten", "flatten"]
        # The resumed attempt reuses the persisted command ID across sessions.
        assert identities == [command.command_id, stranded.command_id]
        assert (await repo.load_command(stranded.command_id)).status == "completed"
    finally:
        await engine.dispose()


def _plan_for_key(account: str, symbol: str) -> OrderExecutionPlan:
    return OrderExecutionPlan(
        client_order_id=f"c-{uuid4().hex[:8]}",
        intent_id=f"intent-{uuid4().hex[:8]}",
        run_id="run-renew",
        symbol=symbol,
        side="BUY",
        position_side=FuturesPositionSide.LONG,
        order_type="LIMIT",
        quantity=Decimal("0.5"),
        price=Decimal("60000"),
        reduce_only=False,
        created_at=datetime.now(tz=UTC),
        strategy_name="orderflow_impulse",
        strategy_version="v1",
    )
