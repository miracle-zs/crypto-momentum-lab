import asyncio
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import cast

import pytest

from crypto_momentum_lab.domain.execution import ExchangeOrderState
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.execution_account.orders.coordinator import (
    OrderExecutionPort,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionResult,
)
from crypto_momentum_lab.live_rollout.context import (
    LiveContextProvider,
    LiveDaemonRuntimeContext,
)
from crypto_momentum_lab.live_rollout.exit_processor import (
    ExitProcessorConfig,
    LiveExitProcessor,
)
from crypto_momentum_lab.live_rollout.exits import LiveExitOrderRequest
from crypto_momentum_lab.live_rollout.submission import LiveCandidateSubmission
from tests.unit.shadow_operation.test_service import _intent, _state

NOW = datetime(2026, 7, 4, 0, 0, 20, tzinfo=UTC)


class RecordingSubmission:
    def __init__(self, result: OrderExecutionResult | None) -> None:
        self.result = result
        self.calls: list[tuple[object, Decimal | None, MarketState15s]] = []

    async def execute(
        self,
        candidate,
        *,
        requested_quantity,
        state,
        context,
        reference_price=None,
    ):
        del context, reference_price
        self.calls.append((candidate, requested_quantity, state))
        return self.result


class BlockingSubmission(RecordingSubmission):
    def __init__(self) -> None:
        super().__init__(_acknowledged_result())
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.active = 0
        self.max_active = 0

    async def execute(self, *args, **kwargs):
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self.started.set()
        await self.release.wait()
        self.active -= 1
        return await super().execute(*args, **kwargs)


def _acknowledged_result(*, suppressed: bool = False) -> OrderExecutionResult:
    return OrderExecutionResult(
        client_order_id="cml_exit_1",
        state=ExchangeOrderState.ACKNOWLEDGED,
        exchange_order_id="exchange-1",
        suppressed=suppressed,
    )


def _context() -> LiveDaemonRuntimeContext:
    return cast(LiveDaemonRuntimeContext, SimpleNamespace())


def _processor(
    submission: object,
    *,
    context_is_current: Callable[[LiveDaemonRuntimeContext], bool] = lambda _: True,
) -> LiveExitProcessor:
    async def provide_context(_state: MarketState15s) -> LiveDaemonRuntimeContext:
        return _context()

    async def publish_positions(_context: LiveDaemonRuntimeContext) -> None:
        return None

    return LiveExitProcessor(
        config=ExitProcessorConfig(run_id="run-1"),
        exit_manager=None,
        exit_recovery_client=None,
        state_machine=cast(OrderExecutionPort, object()),
        submission=cast(LiveCandidateSubmission, submission),
        telemetry=None,
        clock=lambda: NOW,
        is_exit_enabled=lambda: True,
        context_provider=cast(LiveContextProvider, provide_context),
        sync_pending_entry_plans=lambda _context: None,
        publish_managed_position_symbols=publish_positions,
        invalidate_context_cache=lambda: None,
        context_is_current=context_is_current,
    )


def test_exit_processor_config_rejects_blank_run_id() -> None:
    with pytest.raises(ValueError, match="run_id must not be empty"):
        ExitProcessorConfig(run_id=" ")


@pytest.mark.asyncio
async def test_process_requests_delegates_one_exit_and_reports_submission_counts(
) -> None:
    submission = RecordingSubmission(_acknowledged_result())
    processor = _processor(submission)
    candidate = replace(_intent(), candidate_id="exit-1", reduce_only=True)
    state = _state()
    context = _context()

    approved, submitted, failure = await processor.process_requests(
        (LiveExitOrderRequest(candidate=candidate, quantity=Decimal("0.001")),),
        state=state,
        context=context,
    )

    assert (approved, submitted, failure) == (1, 1, None)
    assert submission.calls == [(candidate, Decimal("0.001"), state)]


@pytest.mark.asyncio
async def test_process_requests_counts_suppressed_exit_without_exchange_submission(
) -> None:
    submission = RecordingSubmission(_acknowledged_result(suppressed=True))
    processor = _processor(submission)
    candidate = replace(_intent(), candidate_id="exit-suppressed", reduce_only=True)

    approved, submitted, failure = await processor.process_requests(
        (LiveExitOrderRequest(candidate=candidate, quantity=Decimal("0.001")),),
        state=_state(),
        context=_context(),
    )

    assert (approved, submitted, failure) == (1, 0, None)


@pytest.mark.asyncio
async def test_process_requests_serializes_same_symbol_batches() -> None:
    submission = BlockingSubmission()
    processor = _processor(submission)
    state = _state()
    context = _context()

    first = asyncio.create_task(
        processor.process_requests(
            (
                LiveExitOrderRequest(
                    candidate=replace(
                        _intent(), candidate_id="exit-1", reduce_only=True
                    ),
                    quantity=Decimal("0.001"),
                ),
            ),
            state=state,
            context=context,
        )
    )
    await submission.started.wait()
    second = asyncio.create_task(
        processor.process_requests(
            (
                LiveExitOrderRequest(
                    candidate=replace(
                        _intent(), candidate_id="exit-2", reduce_only=True
                    ),
                    quantity=Decimal("0.001"),
                ),
            ),
            state=state,
            context=context,
        )
    )
    await asyncio.sleep(0)
    assert len(submission.calls) == 0
    assert submission.max_active == 1

    submission.release.set()
    assert await first == (1, 1, None)
    assert await second == (1, 1, None)
    assert len(submission.calls) == 2
