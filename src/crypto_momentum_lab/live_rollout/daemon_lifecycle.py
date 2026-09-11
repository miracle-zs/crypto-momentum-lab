"""Lifecycle coordination for the live strategy daemon."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, Awaitable, Callable

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.live_rollout.checkpoint_coordinator import (
    LiveCheckpointCoordinator,
)
from crypto_momentum_lab.live_rollout.exit_lane import (
    ExitExecutionLane,
    ExitLaneOutcome,
)
from crypto_momentum_lab.live_rollout.exits import LiveExitManager
from crypto_momentum_lab.live_rollout.market_loop import (
    LiveDaemonResult,
)
from crypto_momentum_lab.live_rollout.scheduled_controller import (
    ScheduledRiskWindowController,
)


class LiveDaemonLifecycle:
    """Start and stop the live lanes around one ordered market loop."""

    def __init__(
        self,
        *,
        run_id: str,
        checkpoint_coordinator: LiveCheckpointCoordinator,
        exit_lane: ExitExecutionLane,
        exit_manager: LiveExitManager | None,
        scheduled_controller: ScheduledRiskWindowController,
        scheduled_risk_window_enabled: bool,
        run_market_loop: Callable[
            [AsyncIterable[MarketState15s]], Awaitable[LiveDaemonResult]
        ],
        set_run_active: Callable[[bool], None],
    ) -> None:
        if not run_id.strip():
            raise ValueError("run_id must not be empty")
        self._run_id = run_id
        self._checkpoint_coordinator = checkpoint_coordinator
        self._exit_lane = exit_lane
        self._exit_manager = exit_manager
        self._scheduled_controller = scheduled_controller
        self._scheduled_risk_window_enabled = scheduled_risk_window_enabled
        self._run_market_loop = run_market_loop
        self._set_run_active = set_run_active

    async def run(
        self,
        states: AsyncIterable[MarketState15s],
    ) -> LiveDaemonResult:
        self._set_run_active(True)
        result: LiveDaemonResult | None = None
        exit_outcome = ExitLaneOutcome()
        scheduled_task: asyncio.Task[None] | None = None
        try:
            await self._checkpoint_coordinator.start()
            if self._exit_manager is not None:
                await self._exit_lane.start()
            if self._scheduled_risk_window_enabled:
                scheduled_task = asyncio.create_task(
                    self._scheduled_controller.run(),
                    name=f"live-scheduled-risk-window:{self._run_id}",
                )
            result = await self._run_market_loop(states)
        finally:
            if scheduled_task is not None:
                scheduled_task.cancel()
                await asyncio.gather(
                    scheduled_task,
                    return_exceptions=True,
                )
            if self._exit_manager is not None:
                exit_outcome = await self._exit_lane.stop()
            await self._checkpoint_coordinator.stop()
            self._set_run_active(False)
        if result is None:
            raise RuntimeError("live daemon stopped without a result")
        return _merge_lane_outcomes(
            result,
            exit_outcome=exit_outcome,
            scheduled_controller=self._scheduled_controller,
        )


def _merge_lane_outcomes(
    result: LiveDaemonResult,
    *,
    exit_outcome: ExitLaneOutcome,
    scheduled_controller: ScheduledRiskWindowController,
) -> LiveDaemonResult:
    return LiveDaemonResult(
        processed_state_count=result.processed_state_count,
        approved_intent_count=(
            result.approved_intent_count
            + exit_outcome.approved_intent_count
            + scheduled_controller.approved_intent_count
        ),
        submitted_order_count=(
            result.submitted_order_count
            + exit_outcome.submitted_order_count
            + scheduled_controller.submitted_order_count
        ),
        halt_reason=(
            result.halt_reason
            or (exit_outcome.failure if exit_outcome.fatal_failure else None)
        ),
        final_state_at=result.final_state_at,
    )


__all__ = ["LiveDaemonLifecycle"]
