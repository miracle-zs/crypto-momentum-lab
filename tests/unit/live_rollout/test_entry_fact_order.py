"""Real Book/context/exit scheduling regressions for the MAGIC entry race."""

from dataclasses import replace
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.execution.position_ledger import PositionLedger
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFacts,
    AccountFactStreamScope,
    FactCoverageInterval,
    PositionHealthStatus,
)
from scripts.diagnostics.cml_entry_fact_order_20261002 import (
    probe_context,
    probe_quote_wakeup,
)
from tests.unit.execution.test_position_batch_consistency import _snapshot
from tests.unit.execution.test_terminal_settlement import SCOPE


@pytest.mark.parametrize(
    "case",
    [
        "receipt_before_trade",
        "snapshot_before_trade",
        "external_position",
        "trade_applied",
    ],
)
async def test_real_entry_fact_interleavings_preserve_sync_state(case):
    result = await probe_context(case)
    assert result["passed"], result
    if case == "snapshot_before_trade":
        assert result["view_health"] != PositionHealthStatus.CONFLICT.value


async def test_new_committed_facts_bypass_quote_sync_backoff():
    result = await probe_quote_wakeup()
    assert result["passed"], result


def test_snapshot_without_complete_cut_does_not_prove_quantity_conflict():
    key = SCOPE.to_position_key()
    scope = AccountFactStreamScope.for_position_key(
        key,
        stream_id="hub",
        stream_epoch="epoch",
    )
    snap = replace(
        _snapshot("2", "20.594640", symbol=key.symbol), account_label=key.account_label
    )
    result = PositionLedger(key).project(
        AccountFacts(
            key,
            snapshots=(snap,),
            stream_scope=scope,
        )
    )
    assert result.health_status == PositionHealthStatus.CATCHING_UP
    assert not result.is_comparable
    assert result.discrepancy is None


def test_verified_complete_cut_still_detects_real_quantity_conflict():
    # Unscoped complete coverage is an explicit offline input contract; scoped
    # live coverage additionally requires source-anchored load provenance.
    key = SCOPE.to_position_key()
    snap = replace(
        _snapshot("2", "20.594640", symbol=key.symbol), account_label=key.account_label
    )
    result = PositionLedger(key).project(
        AccountFacts(
            key,
            snapshots=(snap,),
            coverage=FactCoverageInterval(
                start_at=snap.observed_at,
                end_at=snap.observed_at,
            ),
        )
    )
    assert result.health_status == PositionHealthStatus.CONFLICT
    assert result.is_comparable
    assert result.reconciliation_gap == Decimal("2")


@pytest.mark.parametrize("gate", ["settlement", "dispatch", "unknown_scope"])
async def test_recovery_scope_does_not_block_unrelated_exit(gate):
    from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
    from crypto_momentum_lab.domain.execution.evidence_models import (
        ExecutionCumulativeOrderReport,
        ExecutionEvidence,
    )
    from crypto_momentum_lab.domain.execution.execution_book import (
        Accepted,
        Blocked,
        ExecutionBook,
        ExecutionRequest,
    )
    from crypto_momentum_lab.domain.execution.order_state import (
        ExchangeOrderEvent,
        ExchangeOrderState,
    )
    from crypto_momentum_lab.domain.execution.trade_command import (
        TradeCommand,
        TradeCommandType,
    )
    from crypto_momentum_lab.domain.strategy import EntryType, StrategySide
    from tests.unit.execution.test_terminal_settlement import NOW, evidence, fill

    book = ExecutionBook()
    command = TradeCommand(
        "waiting",
        SCOPE.to_position_key(),
        TradeCommandType.ENTRY,
        StrategySide.LONG,
        EntryType.MARKET,
        Decimal(2),
        created_at=NOW,
    )
    book.register_prepared_command(command, SCOPE)
    if gate == "settlement":
        await book.observe(
            evidence(
                "terminal",
                order_event=ExchangeOrderEvent(
                    "terminal",
                    "waiting",
                    ExchangeOrderState.FILLED,
                    NOW,
                    "exchange",
                    {},
                ),
                cumulative_order=ExecutionCumulativeOrderReport(
                    "waiting", Decimal(2), Decimal(200), NOW
                ),
            )
        )
        # The memory-only observation path does not run durable settlement.
        # Install the persisted gate whose position isolation is under test.
        book._recovery_required_commands.add("waiting")
    else:
        await book.mark_dispatching("waiting")
        await book.mark_unknown("waiting", reason="response lost")
        if gate == "unknown_scope":
            book._outbox_by_command_id.pop("waiting")
    assert book.command_requires_recovery("waiting")
    scope = ExecutionScope("live", "primary", "ETHUSDT", SCOPE.position_side)
    await book.observe(
        ExecutionEvidence(
            "eth-buy",
            scope,
            NOW,
            fill=replace(fill("eth-buy", "2", entry=True), symbol="ETHUSDT"),
            coverage=FactCoverageInterval(start_at=NOW, end_at=NOW),
        )
    )
    view = await book.read(scope)
    request = ExecutionRequest(
        "eth-exit",
        scope,
        "trend",
        "1",
        "run",
        "decision",
        view.projection_version,
        TradeCommandType.EXIT,
        Decimal(1),
        reduce_only=True,
        target_batch_ids=tuple(batch.batch_id for batch in view.batches),
    )
    result = await book.act(request)
    assert isinstance(result, Blocked if gate == "unknown_scope" else Accepted)
    if gate != "unknown_scope":
        same_scope = replace(request, request_id="btc-exit", scope=SCOPE)
        assert isinstance(await book.act(same_scope), Blocked)


async def test_book_commit_invalidates_operational_cache_without_next_candle():
    result = await probe_context("cache_fact_update")
    assert result["passed"], result


async def test_rolled_back_facts_do_not_publish_context_revision():
    from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
    from tests.unit.execution.test_terminal_settlement import (
        ObservationUnitOfWork,
        evidence,
        fill,
    )

    uow = ObservationUnitOfWork()
    book = ExecutionBook(execution_unit_of_work=uow)
    book._persistence_failed = False
    await book.observe(evidence("initial"))
    revision = book.context_revision
    uow.fail_commit = True
    with pytest.raises(RuntimeError, match="not durably accepted"):
        await book.observe(evidence("rollback", fill=fill("buy", "2", entry=True)))
    assert book.context_revision == revision
    stored_view = book._books[SCOPE.to_position_key().canonical_id].get_view()
    assert stored_view.total_quantity == 0
    with pytest.raises(RuntimeError, match="durable restoration"):
        await book.read(SCOPE)
