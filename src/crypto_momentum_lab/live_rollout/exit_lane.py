"""Concurrent reduce-only exit lanes for the live strategy.

This module owns the queueing and lifecycle policy for market and quote-triggered exits.  The processor callbacks remain owned by the live
daemon because they need the daemon's context and recovery collaborators; the
lane itself owns only scheduling, latest-value coalescing, and outcome
aggregation.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import structlog

from crypto_momentum_lab.domain.market.models import MarketState15s, RealtimeMarketQuote
from crypto_momentum_lab.live_rollout.exit_failure_policy import (
    is_pending_exit_evaluation,
)

log = structlog.get_logger()


@dataclass(frozen=True, slots=True)
class ExitLaneOutcome:
    """Aggregated result of work executed by one exit lane."""

    approved_intent_count: int = 0
    submitted_order_count: int = 0
    failure: str | None = None
    fatal_failure: bool = False

    def merge(self, other: ExitLaneOutcome) -> ExitLaneOutcome:
        return ExitLaneOutcome(
            approved_intent_count=(
                self.approved_intent_count + other.approved_intent_count
            ),
            submitted_order_count=(
                self.submitted_order_count + other.submitted_order_count
            ),
            failure=(
                self.failure
                if self.fatal_failure
                else other.failure
                if other.fatal_failure
                else self.failure or other.failure
            ),
            fatal_failure=self.fatal_failure or other.fatal_failure,
        )


@dataclass(frozen=True, slots=True)
class _ExitLaneWork:
    state: MarketState15s


@dataclass(frozen=True, slots=True)
class _QuoteLaneWork:
    quote: RealtimeMarketQuote
    state: MarketState15s


class ExitExecutionLane:
    """Run exit decisions independently from event consumers.

    The market worker deliberately keeps only the newest pending state for a
    symbol.  A slow candle lookup for one symbol therefore cannot build a
    queue of stale exit decisions for that symbol. Account facts are already
    committed before their triggers enter these same latest-value lanes.
    """

    _MARKET_WORKER_COUNT = 4

    def __init__(
        self,
        processor: Callable[
            [MarketState15s],
            Awaitable[ExitLaneOutcome],
        ],
        quote_processor: Callable[
            [RealtimeMarketQuote, MarketState15s],
            Awaitable[ExitLaneOutcome],
        ],
        *,
        on_outcome: Callable[[str, ExitLaneOutcome], None] | None = None,
    ) -> None:
        self._on_outcome = on_outcome
        self._processor = processor
        self._quote_processor = quote_processor
        self._started = False
        self._market_queue: asyncio.Queue[str | None] | None = None
        self._quote_queue: asyncio.Queue[str | None] | None = None
        self._market_state_lock: asyncio.Lock | None = None
        self._market_latest: dict[str, _ExitLaneWork] = {}
        self._quote_latest: dict[str, _QuoteLaneWork] = {}
        self._market_workers: tuple[asyncio.Task[None], ...] = ()
        self._quote_workers: tuple[asyncio.Task[None], ...] = ()
        self._idle: asyncio.Event | None = None
        self._outstanding_work = 0
        self._outcome = ExitLaneOutcome()

    @property
    def started(self) -> bool:
        return self._started

    @property
    def failure(self) -> str | None:
        if not self._outcome.fatal_failure:
            return None
        return self._outcome.failure

    async def start(self) -> None:
        if self._started:
            return
        self._market_queue = asyncio.Queue()
        self._quote_queue = asyncio.Queue()
        self._market_state_lock = asyncio.Lock()
        self._market_latest = {}
        self._quote_latest = {}
        self._outstanding_work = 0
        self._outcome = ExitLaneOutcome()
        self._idle = asyncio.Event()
        self._idle.set()
        self._started = True
        self._market_workers = tuple(
            asyncio.create_task(
                self._run_market_worker(),
                name=f"live-exit-market-worker-{index}",
            )
            for index in range(self._MARKET_WORKER_COUNT)
        )
        self._quote_workers = tuple(
            asyncio.create_task(
                self._run_quote_worker(),
                name=f"live-exit-quote-worker-{index}",
            )
            for index in range(self._MARKET_WORKER_COUNT)
        )

    async def submit_market(
        self,
        state: MarketState15s,
    ) -> None:
        if (
            not self._started
            or self._market_queue is None
            or self._market_state_lock is None
            or self._idle is None
        ):
            raise RuntimeError("exit lane is not started")
        async with self._market_state_lock:
            new_work = state.symbol not in self._market_latest
            if new_work:
                self._outstanding_work += 1
            self._idle.clear()
            previous = self._market_latest.get(state.symbol)
            if previous is not None:
                state = max((state, previous.state), key=lambda item: item.bucket_end)
            self._market_latest[state.symbol] = _ExitLaneWork(state)
            if new_work:
                self._market_queue.put_nowait(state.symbol)

    async def submit_quote(
        self,
        quote: RealtimeMarketQuote,
        state: MarketState15s,
    ) -> None:
        if (
            not self._started
            or self._quote_queue is None
            or self._market_state_lock is None
            or self._idle is None
        ):
            raise RuntimeError("exit lane is not started")
        async with self._market_state_lock:
            new_work = quote.symbol not in self._quote_latest
            if new_work:
                self._outstanding_work += 1
            self._idle.clear()
            previous = self._quote_latest.get(quote.symbol)
            if previous is not None:
                # An account trigger can carry an older cached ticker.
                # Keep the newer price; the worker loads current context.
                quote = max((quote, previous.quote), key=lambda item: item.event_at)
                state = max((state, previous.state), key=lambda item: item.bucket_end)
            self._quote_latest[quote.symbol] = _QuoteLaneWork(quote, state)
            if new_work:
                self._quote_queue.put_nowait(quote.symbol)

    async def stop(self) -> ExitLaneOutcome:
        if not self._started:
            return ExitLaneOutcome()
        market_queue = self._market_queue
        quote_queue = self._quote_queue
        market_workers = self._market_workers
        quote_workers = self._quote_workers
        workers = market_workers + quote_workers
        try:
            await self.drain()
            if market_queue is not None:
                for _ in market_workers:
                    market_queue.put_nowait(None)
            if quote_queue is not None:
                for _ in quote_workers:
                    quote_queue.put_nowait(None)
            if workers:
                await asyncio.gather(*workers)
        except asyncio.CancelledError:
            await self._cancel_workers(workers)
            self._cancel_pending_work()
            self._reset_after_stop()
            raise
        outcome = self._outcome
        self._reset_after_stop()
        return outcome

    async def drain(self) -> None:
        if self._started and self._idle is not None:
            await self._idle.wait()

    async def _run_market_worker(self) -> None:
        if self._market_queue is None or self._market_state_lock is None:
            return
        while True:
            symbol = await self._market_queue.get()
            if symbol is None:
                return
            async with self._market_state_lock:
                work = self._market_latest.pop(symbol, None)
            if work is None:
                self._mark_idle_if_ready()
                continue
            outcome = await self._run_work(work)
            self._record_outcome(symbol, outcome)
            self._outstanding_work -= 1
            self._mark_idle_if_ready()

    async def _run_quote_worker(self) -> None:
        if self._quote_queue is None or self._market_state_lock is None:
            return
        while True:
            symbol = await self._quote_queue.get()
            if symbol is None:
                return
            async with self._market_state_lock:
                work = self._quote_latest.pop(symbol, None)
            if work is None:
                self._mark_idle_if_ready()
                continue
            outcome = await self._run_quote_work(work)
            self._record_outcome(symbol, outcome)
            self._outstanding_work -= 1
            self._mark_idle_if_ready()

    async def _run_work(self, work: _ExitLaneWork) -> ExitLaneOutcome:
        try:
            return await self._processor(work.state)
        except Exception as error:
            log.exception(
                "live_exit_lane_work_failed",
                symbol=work.state.symbol,
                error_type=type(error).__name__,
            )
            return ExitLaneOutcome(
                failure=f"exit_execution_failed:{type(error).__name__}",
                fatal_failure=True,
            )

    async def _run_quote_work(self, work: _QuoteLaneWork) -> ExitLaneOutcome:
        try:
            return await self._quote_processor(
                work.quote,
                work.state,
            )
        except Exception as error:
            log.exception(
                "live_quote_exit_lane_work_failed",
                symbol=work.quote.symbol,
                error_type=type(error).__name__,
            )
            return ExitLaneOutcome(
                failure=f"quote_exit_execution_failed:{type(error).__name__}",
                fatal_failure=True,
            )

    def record_recovery_outcome(self, symbol: str, outcome: ExitLaneOutcome) -> None:
        """Account for work completed by the existing repair worker."""
        self._record_outcome(symbol, outcome)

    def _record_outcome(self, symbol: str, outcome: ExitLaneOutcome) -> None:
        self._outcome = self._outcome.merge(outcome)
        if self._on_outcome is not None:
            try:
                self._on_outcome(symbol, outcome)
            except Exception as error:
                log.exception("live_exit_outcome_publication_failed", symbol=symbol)
                self._outcome = self._outcome.merge(
                    ExitLaneOutcome(
                        failure=f"exit_outcome_publication_failed:{type(error).__name__}",
                        fatal_failure=True,
                    )
                )
        if outcome.failure is not None and not is_pending_exit_evaluation(outcome.failure):
            if outcome.fatal_failure:
                log.error(
                    "live_exit_lane_failed_closed",
                    reason=outcome.failure,
                )
            else:
                log.warning(
                    "live_exit_lane_recoverable_failure",
                    reason=outcome.failure,
                )

    def _mark_idle_if_ready(self) -> None:
        if self._idle is not None and self._outstanding_work == 0:
            self._idle.set()

    async def _cancel_workers(
        self,
        workers: tuple[asyncio.Task[None], ...],
    ) -> None:
        for worker in workers:
            self._cancel(worker)
        if workers:
            await asyncio.gather(*workers, return_exceptions=True)

    def _cancel_pending_work(self) -> None:
        self._market_latest.clear()
        self._quote_latest.clear()
        self._outstanding_work = 0
        if self._idle is not None:
            self._idle.set()

    @staticmethod
    def _cancel(task: asyncio.Task[None] | None) -> None:
        if task is not None and not task.done():
            task.cancel()

    def _reset_after_stop(self) -> None:
        self._started = False
        self._market_workers = ()
        self._quote_workers = ()
        self._cancel_pending_work()
