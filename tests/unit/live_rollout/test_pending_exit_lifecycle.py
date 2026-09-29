"""Tests for the finite state lifecycle and convergence of pending exit commands."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, create_autospec

import pytest

from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.order_state import (
    ExitAllocation,
    FuturesPositionSide,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    ExitAllocationPlan,
    ExitPolicyMode,
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide
from crypto_momentum_lab.live_rollout.decision_facts import LiveDecisionFactSource
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work import (
    AsyncPostgresDecisionUnitOfWork,
)


def _setup_pending_exit_test():
    key = PositionKey("live", "primary", "GRASSUSDT", FuturesPositionSide.LONG)
    scope = AccountFactStreamScope.for_position_key(
        key,
        stream_id="account_event_hub",
        stream_epoch="epoch-active",
    )
    plan = ExitAllocationPlan(
        position_key=key,
        allocations=(ExitAllocation("batch-1", Decimal("137.6")),),
        total_allocated_quantity=Decimal("137.6"),
        policy=ExitPolicyMode.FULL_POSITION_CLOSE,
        projection_version="pv_active",
    )
    command = TradeCommand(
        command_id="cmd_exit_test_123",
        position_key=key,
        command_type=TradeCommandType.EXIT,
        side=StrategySide.LONG,
        order_type=EntryType.MARKET,
        requested_quantity=Decimal("137.6"),
        reduce_only=True,
        allocation_plan=plan,
        expected_projection_version="pv_active",
        created_at=datetime(2026, 9, 29, 11, 52, tzinfo=UTC),
    )
    uow = create_autospec(AsyncPostgresDecisionUnitOfWork, instance=True, spec_set=True)
    uow.load_pending_exits.return_value = (("dec_test_123", command),)
    uow.mark_exit_dispatched.return_value = True
    uow.mark_exit_superseded.return_value = True

    book = create_autospec(ExecutionBook, instance=True, spec_set=True)
    view = SimpleNamespace(
        stream_scope=scope,
        is_ready_for_trade=True,
        projection_version="pv_active",
        total_quantity=Decimal("137.6"),
    )
    book.read.return_value = view

    source = LiveDecisionFactSource(
        "primary",
        decision_unit_of_work=uow,
        execution_book=book,
    )
    handler = AsyncMock(return_value=SimpleNamespace(state="submitted"))
    source.set_exit_handler(handler)
    source.bind_account_stream(
        stream_id="account_event_hub",
        stream_epoch="epoch-active",
        sequence=1,
    )
    return source, uow, book, view, handler, command


@pytest.mark.asyncio
async def test_pending_exit_dispatches_when_book_matches_active_epoch() -> None:
    """When the Book's scope matches the active stream and the projection version

    matches the command, the pending exit must be dispatched immediately.
    """
    source, uow, book, view, handler, command = _setup_pending_exit_test()

    await source.recover_pending_exits()

    handler.assert_awaited_once_with(command)
    uow.mark_exit_dispatched.assert_awaited_once_with(
        "dec_test_123", command.command_id
    )
    uow.mark_exit_superseded.assert_not_awaited()


@pytest.mark.asyncio
async def test_pending_exit_superseded_when_position_confirmed_flat_on_exchange() -> None:
    """If exchange reconciliation proves the physical position has already been closed

    (total_quantity == 0), the pending exit in outbox must be safely marked SUPERSEDED
    instead of staying permanently PENDING and logging deferred warnings forever.
    """
    source, uow, book, view, handler, command = _setup_pending_exit_test()

    # Position is flat in the current book
    view.total_quantity = Decimal("0")

    await source.recover_pending_exits()

    # Invariant: Must NOT dispatch a phantom order on a flat position
    handler.assert_not_awaited()
    uow.mark_exit_dispatched.assert_not_awaited()

    # Invariant: Must transition to SUPERSEDED to close the finite state machine loop
    uow.mark_exit_superseded.assert_awaited_once_with(
        "dec_test_123", command.command_id, "position_already_flat"
    )


def test_exit_command_client_order_id_bounded() -> None:
    """Binance and PostgreSQL strictly limit client_order_id to 36 chars.
    A 39-char command_id must be deterministically hashed to <= 36 chars.
    """
    from crypto_momentum_lab.domain.execution.order_state import (
        deterministic_client_order_id,
    )

    long_cmd_id = "cmd_exit_dec_GRASSUSDT_b213547b8a82344f"  # 39 chars
    assert len(long_cmd_id) == 39

    session_id = "live-b1-long-100u-5x-v1"
    exit_client_order_id = (
        long_cmd_id
        if len(long_cmd_id) <= 36
        else deterministic_client_order_id(session_id, long_cmd_id)
    )
    assert len(exit_client_order_id) <= 36
    assert exit_client_order_id.startswith("cml_")
