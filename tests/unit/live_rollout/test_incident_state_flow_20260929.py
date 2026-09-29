"""Production incident contracts: durable repair must reach the live reader.

Synthetic identities only; these tests never connect to an exchange or database.
"""

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, create_autospec

import pytest

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.order_state import ExitAllocation
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFacts,
    AccountFactStreamScope,
    FuturesPositionSide,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.recovery_models import DurableJournalCut
from crypto_momentum_lab.domain.execution.trade_command import (
    ExitAllocationPlan,
    ExitPolicyMode,
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide
from crypto_momentum_lab.live_rollout.decision_facts import LiveDecisionFactSource
from crypto_momentum_lab.live_rollout.position_self_healing import (
    auto_heal_unmanaged_position,
)
from crypto_momentum_lab.persistence.postgres.account_journal_store import (
    PostgresAccountJournalStore,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work import (
    AsyncPostgresDecisionUnitOfWork,
    AsyncPostgresExecutionUnitOfWork,
    DurableExecutionPositionState,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
    ExecutionBookHeadRow,
)


@pytest.mark.parametrize("has_position", [False, True])
async def test_repaired_position_reload_uses_the_real_uow_contract(
    has_position: bool,
) -> None:
    # An unrestricted AsyncMock silently invents load_execution_positions,
    # hiding the exact AttributeError recorded on the production host.
    uow = create_autospec(
        AsyncPostgresExecutionUnitOfWork, instance=True, spec_set=True
    )
    book = ExecutionBook(execution_unit_of_work=uow)
    key = PositionKey("live", "incident-account", "TESTUSDT", FuturesPositionSide.LONG)
    now = datetime(2026, 9, 29, 10, tzinfo=UTC)

    scope = AccountFactStreamScope.for_position_key(
        key,
        stream_id="account_event_hub",
        stream_epoch="current-epoch",
    )
    cut = DurableJournalCut(
        scope=scope,
        facts=AccountFacts(position_key=key, stream_scope=scope),
        revision=0,
        as_of=now,
    )
    state = DurableExecutionPositionState(
        scope=scope,
        cut=cut,
        head=None,
        trade_ids=(),
        evidence_ids=(),
        watermarks=(),
    )
    uow.load_positions.return_value = (state,) if has_position else ()
    result = await book.reload_position(key, as_of=now)
    if has_position:
        assert result is not None
        assert result.stream_scope == scope
        assert result.total_quantity == Decimal("0")
    else:
        assert result is None
    uow.load_positions.assert_awaited_once_with(
        environment="live",
        account_label="incident-account",
        as_of=now,
    )


def test_reconnect_selects_latest_registered_epoch_for_each_account() -> None:
    book = ExecutionBook()
    book.register_active_stream(
        environment="live",
        account_label="other-account",
        stream_id="account_event_hub",
        stream_epoch="other-epoch",
    )
    for index in range(16):
        epoch = f"epoch-{index}"
        book.register_active_stream(
            environment="live",
            account_label="incident-account",
            stream_id="account_event_hub",
            stream_epoch=epoch,
        )
        assert book.get_active_stream("live", "incident-account") == (
            "account_event_hub",
            epoch,
        ), "a reconnect must not route new observations back to an obsolete epoch"
    assert book.get_active_stream("live", "other-account") == (
        "account_event_hub",
        "other-epoch",
    )


@pytest.mark.parametrize(
    "previous_epoch",
    [
        "current-epoch",
        "obsolete-epoch",
    ],
)
async def test_self_heal_sequence_watermark_is_scoped_to_epoch(
    previous_epoch: str,
) -> None:
    now = datetime(2026, 9, 29, 10, tzinfo=UTC)
    key = PositionKey("live", "incident-account", "TESTUSDT", FuturesPositionSide.LONG)
    scope = AccountFactStreamScope.for_position_key(
        key,
        stream_id="account_event_hub",
        stream_epoch="current-epoch",
    )
    fill = AccountFillEvent(
        environment="live",
        account_label=key.account_label,
        symbol=key.symbol,
        trade_id="test-trade",
        order_id="test-order",
        side="BUY",
        quantity=Decimal("1"),
        price=Decimal("10"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=now,
        raw_payload={"positionSide": "LONG"},
    )
    cut = DurableJournalCut(
        scope=scope,
        facts=AccountFacts(position_key=key, stream_scope=scope, fills=(fill,)),
        revision=1,
        as_of=now,
    )
    head = ExecutionBookHeadRow(
        environment="live",
        account_label=key.account_label,
        symbol=key.symbol,
        position_side="LONG",
        stream_id="account_event_hub",
        stream_epoch=previous_epoch,
        revision=10,
        projection_version="pv_previous",
        state_payload={"last_sequence": 461, "active_reservation_ids": []},
        updated_at=now,
    )
    session = MagicMock()
    session.scalar = AsyncMock(side_effect=[1, None, None, head])
    session.scalars = AsyncMock(
        side_effect=[
            SimpleNamespace(all=lambda: [fill]),
            SimpleNamespace(all=lambda: []),
        ]
    )
    session.flush = AsyncMock()
    session.commit = AsyncMock()
    store = create_autospec(PostgresAccountJournalStore, instance=True, spec_set=True)
    store.load_recovery_in_session.return_value = cut

    assert await auto_heal_unmanaged_position(
        session=session,
        journal_store=store,
        environment="live",
        account_label=key.account_label,
        symbol=key.symbol,
        active_stream_epoch="current-epoch",
    )
    session.commit.assert_awaited_once()
    assert head.stream_epoch == "current-epoch"
    assert head.state_payload["last_sequence"] == (
        461 if previous_epoch == "current-epoch" else None
    ), "the new stream starts its own sequence; carrying 461 rejects fresh events"


def _pending_exit_case():
    key = PositionKey("live", "incident-account", "TESTUSDT", FuturesPositionSide.LONG)
    scope = AccountFactStreamScope.for_position_key(
        key,
        stream_id="account_event_hub",
        stream_epoch="current-epoch",
    )
    plan = ExitAllocationPlan(
        position_key=key,
        allocations=(ExitAllocation("batch-1", Decimal("1")),),
        total_allocated_quantity=Decimal("1"),
        policy=ExitPolicyMode.FULL_POSITION_CLOSE,
        projection_version="pv_expected",
    )
    command = TradeCommand(
        command_id="incident-exit",
        position_key=key,
        command_type=TradeCommandType.EXIT,
        side=StrategySide.LONG,
        order_type=EntryType.MARKET,
        requested_quantity=Decimal("1"),
        reduce_only=True,
        allocation_plan=plan,
        expected_projection_version="pv_expected",
        created_at=datetime(2026, 9, 29, 10, tzinfo=UTC),
    )
    uow = create_autospec(AsyncPostgresDecisionUnitOfWork, instance=True, spec_set=True)
    uow.load_pending_exits.return_value = (("incident-decision", command),)
    uow.mark_exit_dispatched.return_value = True
    book = create_autospec(ExecutionBook, instance=True, spec_set=True)
    view = SimpleNamespace(
        stream_scope=scope,
        is_ready_for_trade=True,
        projection_version="pv_expected",
    )
    book.read.return_value = view
    source = LiveDecisionFactSource(
        "incident-account",
        decision_unit_of_work=uow,
        execution_book=book,
    )
    handler = AsyncMock(return_value=SimpleNamespace(state="submitted"))
    source.set_exit_handler(handler)
    source.bind_account_stream(
        stream_id="account_event_hub", stream_epoch="current-epoch", sequence=1
    )
    return source, uow, book, view, handler, command


async def test_pending_exit_recovers_after_book_becomes_ready() -> None:
    source, uow, book, _, handler, command = _pending_exit_case()
    book.read.side_effect = ValueError(
        "requested account stream does not match the restored position"
    )
    await source.recover_pending_exits()
    handler.assert_not_awaited()
    uow.mark_exit_dispatched.assert_not_awaited()

    book.read.side_effect = None
    await source.recover_pending_exits()
    handler.assert_awaited_once_with(command)
    uow.mark_exit_dispatched.assert_awaited_once_with(
        "incident-decision", command.command_id
    )


@pytest.mark.parametrize("mismatch", ["epoch", "projection", "allocation", "not_ready"])
async def test_stale_pending_exit_is_never_dispatched_or_silently_acknowledged(
    mismatch: str,
) -> None:
    source, uow, _, view, handler, command = _pending_exit_case()
    if mismatch == "epoch":
        view.stream_scope = replace(view.stream_scope, stream_epoch="obsolete-epoch")
    elif mismatch == "projection":
        view.projection_version = "pv_newer"
    elif mismatch == "allocation":
        command = replace(
            command,
            allocation_plan=replace(
                command.allocation_plan, projection_version="pv_other"
            ),
        )
        uow.load_pending_exits.return_value = (("incident-decision", command),)
    else:
        view.is_ready_for_trade = False
    for _ in range(3):
        await source.recover_pending_exits()
    handler.assert_not_awaited()
    uow.mark_exit_dispatched.assert_not_awaited()
    # Characterization: there is no exchange reconciliation at this seam that
    # would justify declaring an unknown submission superseded.
    uow.mark_exit_superseded.assert_not_awaited()


@pytest.mark.parametrize("result_state", ["rejected", "unknown"])
async def test_unconfirmed_exit_does_not_advance_durable_status(
    result_state: str,
) -> None:
    source, uow, _, _, handler, _ = _pending_exit_case()
    handler.return_value = SimpleNamespace(state=result_state)
    await source.recover_pending_exits()
    handler.assert_awaited_once()
    uow.mark_exit_dispatched.assert_not_awaited()


async def test_unrelated_book_corruption_is_not_swallowed_as_epoch_recovery() -> None:
    source, _, book, _, handler, _ = _pending_exit_case()
    book.read.side_effect = ValueError("invalid recovery payload")
    with pytest.raises(ValueError, match="invalid recovery payload"):
        await source.recover_pending_exits()
    handler.assert_not_awaited()
