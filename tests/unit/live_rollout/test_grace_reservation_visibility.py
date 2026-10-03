"""Restored order rows must retain exact Book reservation ownership."""

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
from crypto_momentum_lab.domain.strategy.position_exit import (
    ClosedCandle15m,
    PositionExitMode,
)
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionCoordinator,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionResult,
)
from crypto_momentum_lab.live_rollout.exits import (
    LiveExitCancellationRequest,
    LiveExitManager,
    LiveExitOrderRequest,
    managed_live_positions_from_views,
)
from crypto_momentum_lab.persistence.postgres.order_read_repository import (
    _persisted_order,
)
from tests.unit.execution_account.orders.test_coordinator import (
    _prepared,
    _submission_preparation,
)
from tests.unit.live_rollout.test_exit_processor import NOW, _context, _processor
from tests.unit.live_rollout.test_exits import _config
from tests.fixtures.live_market import _intent, _state


@pytest.mark.parametrize("path", ("candle", "queued_conflict"))
async def test_restored_grace_ack_blocks_next_candle_and_keeps_timeout_cancellation(
    path,
):
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

    initial = await book.read(scope)
    batch = initial.batches[0]
    plan = OrderExecutionPlan(
        "exit",
        "run-1",
        "grace",
        "BTCUSDT",
        "SELL",
        "LIMIT",
        Decimal("1"),
        Decimal("100.88"),
        True,
        NOW + timedelta(minutes=30),
        position_side=FuturesPositionSide.LONG,
        time_in_force="GTC",
        batch_id=batch.batch_id,
        projection_version=initial.projection_version,
    )
    backend, repository = AsyncMock(), AsyncMock()
    backend.submit.return_value = OrderExecutionResult(
        "grace", ExchangeOrderState.ACKNOWLEDGED, "123"
    )
    repository.prepare_submission.side_effect = lambda **kwargs: _prepared(
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
    try:
        await coordinator.prepare_and_execute(
            plan, preparation=_submission_preparation(plan)
        )
        if path == "queued_conflict":
            duplicate = replace(plan, client_order_id="later-candle", intent_id="later")

            class Submission:
                async def execute(self, *_args, **_kwargs):
                    return await coordinator.prepare_and_execute(
                        duplicate, preparation=_submission_preparation(duplicate)
                    )

            outcome = await _processor(Submission()).process_requests(
                (
                    LiveExitOrderRequest(
                        replace(_intent(), reduce_only=True), Decimal("1")
                    ),
                ),
                state=_state(),
                context=_context(),
            )
            assert outcome == (0, 0, "pending_live_context:BTCUSDT")
            backend.submit.assert_awaited_once()
            repository.prepare_submission.assert_awaited_once()
            return
        # ExchangeOrderRow has no batch/allocations columns; use its real decoder.
        row = SimpleNamespace(
            **{
                field: getattr(plan, field)
                for field in (
                    "intent_id",
                    "run_id",
                    "client_order_id",
                    "symbol",
                    "side",
                    "order_type",
                    "quantity",
                    "price",
                    "reduce_only",
                    "created_at",
                    "time_in_force",
                    "expires_at",
                )
            },
            position_side="LONG",
            state="acknowledged",
            exchange_order_id="123",
            executed_quantity=Decimal("0"),
            updated_at=plan.created_at,
        )
        restored = _persisted_order(row)
        assert restored.plan.batch_id is None
        view = await book.read(scope)
        positions = managed_live_positions_from_views(
            (view,), unresolved_orders=(restored,)
        )
        manager = LiveExitManager(
            config=_config(
                PositionExitMode.CANDLE_15M,
                candle_grace_bars=8,
                candle_grace_profit_pct=Decimal("0.0088"),
            )
        )
        candle = ClosedCandle15m(
            "BTCUSDT",
            NOW + timedelta(minutes=45),
            NOW + timedelta(minutes=60),
            Decimal("100"),
            Decimal("99"),
        )
        assert await manager.requests_for_closed_candle(candle, positions) == ()
        assert len(view.reservations) == 1
        state = SimpleNamespace(
            symbol="BTCUSDT",
            last_bid_price=Decimal("99"),
            mark_price=Decimal("99"),
            close_price=Decimal("99"),
        )
        timeouts = await manager.requests_for_grace_timeout(
            now=plan.created_at + timedelta(hours=2), state=state, positions=positions
        )
        assert len(timeouts) == 1
        assert isinstance(timeouts[0], LiveExitCancellationRequest)
        assert timeouts[0].cancel_plan.client_order_id == "grace"
        assert timeouts[0].fallback_quantity == Decimal("1")
        backend.submit.assert_awaited_once()
    finally:
        await coordinator.aclose()


async def test_startup_reconciliation_does_not_lock_submission_configuration():
    from tests.unit.execution_account.orders.test_coordinator import _plan

    backend, repository = AsyncMock(), AsyncMock()
    plan = _plan("startup-open-exit", reduce_only=True)
    backend.reconcile_order.return_value = OrderExecutionResult(
        plan.client_order_id, ExchangeOrderState.ACKNOWLEDGED, "123", plan=plan
    )
    coordinator = OrderExecutionCoordinator(
        backend=backend, environment="live", account_label="primary"
    )
    try:
        await coordinator.reconcile_order(plan)
        coordinator.configure_submission(repository)
        assert coordinator._submission_repository is repository
        backend.submit.assert_not_awaited()
    finally:
        await coordinator.aclose()


async def test_submission_configuration_stays_locked_after_first_submit():
    from tests.unit.execution_account.orders.test_coordinator import _plan

    backend = AsyncMock()
    plan = _plan("first-submit", reduce_only=True)
    backend.submit.return_value = OrderExecutionResult(
        plan.client_order_id, ExchangeOrderState.ACKNOWLEDGED, "123", plan=plan
    )
    coordinator = OrderExecutionCoordinator(
        backend=backend, environment="live", account_label="primary"
    )
    try:
        coordinator.configure_submission(AsyncMock())
        await coordinator.submit(plan)
        with pytest.raises(RuntimeError, match="cannot configure submission"):
            coordinator.configure_submission(AsyncMock())
    finally:
        await coordinator.aclose()
