"""A real Book version change between exit decision and queued preparation."""

from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from crypto_momentum_lab.domain.account import AccountFillEvent, AccountPositionSnapshot
from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderState,
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    FactCoverageInterval,
    FactCoverageStatus,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionResult,
)
from crypto_momentum_lab.live_rollout.exit_channels import LiveExitChannelRuntime
from crypto_momentum_lab.live_rollout.exits import (
    LiveExitOrderRequest,
    managed_live_positions_from_views,
)
from tests.fixtures.live_market import _intent, _state
from tests.unit.execution_account.orders.test_coordinator import (
    OrderExecutionCoordinator,
    _prepared,
    _submission_preparation,
)
from tests.unit.live_rollout.test_exit_processor import NOW, _context, _processor


async def test_prior_exit_settlement_does_not_block_another_exit():
    book = ExecutionBook()
    book._recovery_required_commands.add("settling-earlier-exit")
    scope = ExecutionScope("live", "primary", "BTCUSDT", FuturesPositionSide.LONG)
    view = await book.read(scope)
    backend, repository = AsyncMock(), AsyncMock()
    coordinator = OrderExecutionCoordinator(
        backend=backend,
        environment="live",
        account_label="primary",
        execution_book=book,
        reservation_repository=AsyncMock(),
    )
    coordinator.configure_submission(repository)
    plan = OrderExecutionPlan(
        "exit",
        "run-1",
        "later-exit",
        "BTCUSDT",
        "SELL",
        "MARKET",
        Decimal("1"),
        None,
        True,
        NOW,
        position_side=FuturesPositionSide.LONG,
        batch_id="target",
        projection_version=view.projection_version,
    )

    backend.submit.return_value = OrderExecutionResult(
        plan.client_order_id, ExchangeOrderState.ACKNOWLEDGED, "123"
    )
    repository.prepare_submission_in_session.side_effect = lambda session, **kwargs: _prepared(kwargs["plan"])

    class Submission:
        async def execute(self, *_args, **_kwargs):
            return await coordinator.prepare_and_execute(
                plan, preparation=_submission_preparation(plan)
            )

    processor = _processor(Submission())
    try:
        outcome = await processor.process_requests(
            (LiveExitOrderRequest(replace(_intent(), reduce_only=True), Decimal("1")),),
            state=_state(),
            context=_context(),
        )
        assert outcome == (1, 1, None)
        backend.submit.assert_awaited_once()
        repository.prepare_submission_in_session.assert_awaited_once()
        assert book.command_requires_recovery("settling-earlier-exit")
    finally:
        await coordinator.aclose()


@pytest.mark.parametrize("race_at", ["before_read", "before_act"])
async def test_candle_exit_rebuilds_quantity_after_real_book_advance(race_at):
    scope = ExecutionScope("live", "primary", "BTCUSDT", FuturesPositionSide.LONG)
    book = ExecutionBook()
    book._ensure_journal(scope.to_position_key()).set_coverage(
        FactCoverageInterval(
            start_at=NOW,
            end_at=NOW,
            status=FactCoverageStatus.CONFIRMED,
        )
    )
    opening = AccountFillEvent(
        "live",
        "primary",
        "BTCUSDT",
        "opening",
        "entry",
        "BUY",
        Decimal("100"),
        Decimal("1"),
        Decimal("0"),
        Decimal("0"),
        "USDT",
        NOW,
        {"positionSide": "LONG", "is_system": True},
    )
    snapshot = AccountPositionSnapshot(
        "live",
        "primary",
        "BTCUSDT",
        "LONG",
        Decimal("1"),
        Decimal("100"),
        Decimal("100"),
        Decimal("0"),
        Decimal("100"),
        5,
        "cross",
        NOW,
        {},
    )
    await book.observe(
        ExecutionEvidence("opening", scope, NOW, fill=opening, snapshot=snapshot)
    )
    assert (await book.read(scope)).is_ready_for_trade, (
        await book.read(scope)
    ).diagnostics
    posted = []

    async def post(plan, **kwargs):
        posted.append(plan)
        return OrderExecutionResult(
            plan.client_order_id, ExchangeOrderState.ACKNOWLEDGED, "123"
        )

    backend, repository = AsyncMock(), AsyncMock()
    backend.submit.side_effect = post
    repository.prepare_submission_in_session.side_effect = lambda session, **kwargs: _prepared(
        kwargs["plan"]
    )
    coordinator = OrderExecutionCoordinator(
        backend=backend,
        environment="live",
        account_label="primary",
        execution_book=book,
        reservation_repository=AsyncMock(),
    )
    coordinator.configure_submission(repository)
    decided = []

    async def advance_book():
        closing = replace(
            opening,
            trade_id="partial-close",
            order_id="external",
            side="SELL",
            quantity=Decimal("0.5"),
            trade_at=NOW + timedelta(seconds=1),
        )
        book._ensure_journal(scope.to_position_key()).set_coverage(
            FactCoverageInterval(
                start_at=NOW,
                end_at=closing.trade_at,
                status=FactCoverageStatus.CONFIRMED,
            )
        )
        await book.observe(
            ExecutionEvidence(
                "partial-close",
                scope,
                closing.trade_at,
                fill=closing,
                snapshot=replace(
                    snapshot,
                    position_amt=Decimal("0.5"),
                    observed_at=closing.trade_at,
                ),
            )
        )

    original_act = book.act

    async def advance_before_act(*args, **kwargs):
        if len(decided) == 1:
            await advance_book()
        return await original_act(*args, **kwargs)

    if race_at == "before_act":
        book.act = advance_before_act

    class Submission:
        async def execute(self, candidate, *, requested_quantity, **kwargs):
            plan = OrderExecutionPlan(
                "exit",
                "run-1",
                "candle-exit",
                "BTCUSDT",
                "SELL",
                "MARKET",
                requested_quantity,
                None,
                True,
                NOW,
                position_side=FuturesPositionSide.LONG,
                batch_id=candidate.features["batch_id"],
                projection_version=candidate.features["projection_version"],
            )
            decided.append(plan)
            if len(decided) == 1 and race_at == "before_read":
                await advance_book()
            return await coordinator.prepare_and_execute(
                plan, preparation=_submission_preparation(plan)
            )

    processor = _processor(Submission())
    context = _context()

    class Daemon:
        async def process_closed_candle(self, event, *, latest_quote):
            view = await book.read(scope)
            position = managed_live_positions_from_views((view,))[0].batch_views()[0]
            candidate = replace(
                _intent(),
                reduce_only=True,
                features={
                    "batch_id": position.batch_id,
                    "projection_version": view.projection_version,
                },
            )
            return (
                await processor.process_requests(
                    (LiveExitOrderRequest(candidate, position.quantity),),
                    state=_state(),
                    context=context,
                )
            )[2]

    runtime = LiveExitChannelRuntime(
        daemon=Daemon(),
        latest_market_quotes=SimpleNamespace(for_symbols=lambda symbols: ()),
        latest_market_states=SimpleNamespace(),
        is_transient_error=lambda error: False,
    )
    event = SimpleNamespace(candle=SimpleNamespace(symbol="BTCUSDT", candle_start=NOW))
    key = ("BTCUSDT", NOW)
    runtime._pending_candles[key] = event
    try:
        await runtime._evaluate_candle(key)
        assert [plan.quantity for plan in decided] == [Decimal("1"), Decimal("0.5")]
        assert decided[0].projection_version != decided[1].projection_version
        assert [plan.quantity for plan in posted] == [Decimal("0.5")]
        assert repository.prepare_submission_in_session.await_count == 1
        assert key not in runtime._pending_candles
    finally:
        await coordinator.aclose()
