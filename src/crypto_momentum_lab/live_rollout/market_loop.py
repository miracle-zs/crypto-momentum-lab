"""Ordered live market-state orchestration.

The loop owns ordering, gap handling, admission, and checkpoint progress.  It
does not construct exchange clients or persistence adapters; those are passed
in as the already-separated lanes and coordinators.
"""

from __future__ import annotations

import asyncio
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Mapping,
    Sequence,
)
from datetime import datetime, timedelta

import structlog

import crypto_momentum_lab.live_rollout.market_runtime_contracts as market_runtime_contracts
import crypto_momentum_lab.live_rollout.runtime_errors as runtime_errors
from crypto_momentum_lab.domain.execution.order_state import OrderExecutionPlan
from crypto_momentum_lab.domain.market.models import JsonValue, MarketState15s
from crypto_momentum_lab.domain.strategy import (
    StrategyDecision,
)
from crypto_momentum_lab.domain.strategy.runtime import RuntimeStrategy
from crypto_momentum_lab.live_rollout.checkpoint_coordinator import (
    LiveCheckpointCoordinator,
)
from crypto_momentum_lab.live_rollout.context import (
    LiveContextRuntime,
    LiveDaemonRuntimeContext,
)
from crypto_momentum_lab.live_rollout.context_prefetch import (
    LiveContextPrefetcher,
    PrefetchedContext,
)
from crypto_momentum_lab.live_rollout.entry_lane import EntryExecutionLane
from crypto_momentum_lab.live_rollout.exit_lane import ExitExecutionLane
from crypto_momentum_lab.live_rollout.exits import LiveExitManager
from crypto_momentum_lab.live_rollout.runtime_cache import (
    LiveRuntimeCacheMaintenance,
)
from crypto_momentum_lab.live_rollout.scheduled_controller import (
    ScheduledRiskWindowController,
)
from crypto_momentum_lab.live_rollout.telemetry import (
    LIVE_LANE_ENTRY,
    LiveTelemetrySink,
    market_state_input_fingerprint,
)

log = structlog.get_logger()


class LiveMarketLoop:
    """Run ordered market states through admission and execution lanes."""

    def __init__(
        self,
        *,
        run_id: str,
        strategy: RuntimeStrategy,
        context_prefetcher: LiveContextPrefetcher,
        runtime_cache: LiveRuntimeCacheMaintenance,
        scheduled_controller: ScheduledRiskWindowController,
        telemetry: LiveTelemetrySink | None,
        exit_lane: ExitExecutionLane,
        exit_manager: LiveExitManager | None,
        exit_enabled: Callable[[], bool],
        context_runtime: LiveContextRuntime,
        checkpoint_coordinator: LiveCheckpointCoordinator,
        entry_lane: EntryExecutionLane,
        clock: Callable[[], datetime],
        recover_market_state_gap: market_runtime_contracts.MarketStateGapRecovery
        | None = None,
        hub_cursor_provider: Callable[[], Mapping[str, str | int] | None] | None = None,
        commit_market_state_cursor: Callable[[MarketState15s], None] | None = None,
        entered_symbol_lookup: Callable[[str], bool] | None = None,
        request_order_cleanup: Callable[
            [tuple[OrderExecutionPlan, ...]], None
        ],
        decision_filter: (
            Callable[
                [StrategyDecision, MarketState15s],
                Awaitable[StrategyDecision],
            ]
            | None
        ) = None,
        decision_fact_binder: Callable[[LiveDaemonRuntimeContext | None], None]
        | None = None,
    ) -> None:
        if not run_id.strip():
            raise ValueError("run_id must not be empty")
        self._run_id = run_id
        self._strategy = strategy
        self._context_prefetcher = context_prefetcher
        self._runtime_cache = runtime_cache
        self._scheduled_controller = scheduled_controller
        self._telemetry = telemetry
        self._exit_lane = exit_lane
        self._exit_manager = exit_manager
        self._exit_enabled = exit_enabled
        self._context_runtime = context_runtime
        self._checkpoint_coordinator = checkpoint_coordinator
        self._entry_lane = entry_lane
        self._request_order_cleanup = request_order_cleanup
        self._clock = clock
        self._recover_market_state_gap = recover_market_state_gap
        self._hub_cursor_provider = hub_cursor_provider
        self._commit_market_state_cursor = commit_market_state_cursor
        self._entered_symbol_lookup = entered_symbol_lookup
        self._decision_filter = decision_filter
        self._decision_fact_binder = decision_fact_binder
        self._market_gap_generation = 0
        self._strategy_gap_reset_generation_by_symbol: dict[str, int] = {}

    def notify_market_state_gap(self, *, reason: str) -> None:
        if not reason.strip():
            raise ValueError("reason must not be empty")
        self._market_gap_generation += 1
        log.warning(
            "live_strategy_market_state_gap_detected",
            run_id=self._run_id,
            reason=reason,
            generation=self._market_gap_generation,
        )

    async def run(
        self,
        states: AsyncIterable[MarketState15s],
    ) -> market_runtime_contracts.LiveDaemonResult:
        prefetched_states = self._context_prefetcher.stream(states)
        try:
            return await self._run_prefetched(prefetched_states)
        finally:
            # async for does not close an async generator when this method
            # returns or raises from inside the loop.  Close it explicitly so
            # its producer and any lookahead context/database tasks are
            # cancelled and awaited on every exit path.
            await prefetched_states.aclose()

    async def _run_prefetched(
        self,
        prefetched_states: AsyncIterator[PrefetchedContext],
    ) -> market_runtime_contracts.LiveDaemonResult:
        self._entry_lane.reset()
        processed = approved = submitted = 0
        final_state_at: datetime | None = None
        data_requirement = self._strategy.required_data()
        max_gap_seconds = data_requirement.max_gap_seconds
        state_interval_seconds = data_requirement.base_state_interval_seconds
        async for prefetched in prefetched_states:
            state = prefetched.state
            if state.is_backfill:
                self._strategy.warm_market_state(state)
                self._record_processed_state(state, saved_at=state.bucket_end)
                processed += 1
                final_state_at = state.bucket_start
                continue
            last_processed_at = self._checkpoint_coordinator.last_processed_at(
                state.symbol
            )
            recovered_states: tuple[MarketState15s, ...] = ()
            if (
                last_processed_at is not None
                and self._entered_symbol_lookup is not None
                and self._entered_symbol_lookup(state.symbol)
            ):
                # The symbol just entered the monitored pool, so it has no
                # prior history here: its first bucket is a new baseline, not a
                # gap.  The monitored pool is deliberately wider than the set
                # the strategies trade, so a symbol warms up again from this
                # point well before it matters.  Without this the "gap" would
                # span the whole time it was out of the pool, and recovery would
                # chase buckets that never existed.
                #
                # Note this reacts to a *market*-layer signal inside the
                # strategy layer.  That is sound only while "left the pool"
                # means "its momentum premise is gone" -- which is what makes
                # dropping its rolling indicators correct rather than
                # over-eager.  It costs nothing today because reset_symbol is a
                # plain `pop(..., None)` and is a no-op for symbols the strategy
                # holds no state for.  Revisit if pool membership ever stops
                # implying that.
                self._strategy.reset_symbol(state.symbol)
                self._checkpoint_coordinator.forget_symbol(state.symbol)
                log.info(
                    "live_strategy_symbol_entry_baseline",
                    run_id=self._run_id,
                    symbol=state.symbol,
                )
                # Clearing the watermark makes the continuity check a no-op,
                # which is exactly the semantics we want for a fresh entry.
                last_processed_at = None
            try:
                _validate_market_state_continuity(
                    state=state,
                    last_processed_at=last_processed_at,
                    expected_interval_seconds=state_interval_seconds,
                )
            except market_runtime_contracts.LiveMarketStateContinuityError as error:
                recovered_states = await self._recover_gap(error)
                if not recovered_states:
                    # A MarketState15s stream is event-driven: a quiet symbol
                    # may have no row for one or more buckets even while the
                    # Hub and the other symbols remain healthy.  Reset only
                    # this symbol's rolling indicators and watermark, then
                    # process the current state as its new warm-up boundary.
                    # Treating the gap as a process-wide fatal error makes one
                    # illiquid symbol restart every live account and
                    # unnecessarily interrupts exits for unrelated symbols.
                    self._strategy.reset_symbol(state.symbol)
                    self._checkpoint_coordinator.forget_symbol(state.symbol)
                    log.warning(
                        "live_strategy_symbol_reset_after_market_state_gap",
                        run_id=self._run_id,
                        symbol=state.symbol,
                        reason=str(error),
                    )
                self._strategy_gap_reset_generation_by_symbol[state.symbol] = (
                    self._market_gap_generation
                )
            decision_details = _strategy_decision_details(
                strategy=self._strategy,
                state=state,
                last_processed_at=last_processed_at,
                recovered_bucket_count=len(recovered_states),
                hub_cursor_provider=self._hub_cursor_provider,
            )
            self._runtime_cache.prune(
                now=self._clock(),
                current_symbol=state.symbol,
                active_symbols=self._entry_lane.entry_symbols,
            )
            self._scheduled_controller.observe_state(state)
            if self._telemetry is not None:
                await self._telemetry.market_state_received(
                    state,
                    occurred_at=prefetched.received_at,
                    lane=LIVE_LANE_ENTRY,
                )
                self._telemetry.market_state_progress(
                    state,
                    occurred_at=prefetched.received_at,
                    received_at=prefetched.received_at,
                )
            gap_generation = self._market_gap_generation
            if gap_generation > self._strategy_gap_reset_generation_by_symbol.get(
                state.symbol,
                0,
            ):
                self._strategy.reset_symbol(state.symbol)
                self._checkpoint_coordinator.forget_symbol(state.symbol)
                log.info(
                    "live_strategy_symbol_reset_after_market_gap",
                    run_id=self._run_id,
                    symbol=state.symbol,
                    generation=gap_generation,
                )
                self._strategy_gap_reset_generation_by_symbol[state.symbol] = (
                    gap_generation
                )
            _reset_strategy_for_gap(
                strategy=self._strategy,
                symbol=state.symbol,
                current_at=state.bucket_start,
                last_processed_at=self._checkpoint_coordinator.last_processed_at(
                    state.symbol
                ),
                max_gap_seconds=max_gap_seconds,
            )
            admission = await self._context_runtime.prepare(prefetched)
            if admission.error is not None:
                admission_error = admission.error
                if not runtime_errors.is_transient_runtime_error(admission_error):
                    raise admission_error
                # Keep indicators moving but never authorize without a fresh
                # context. The next state retries the full context read.
                decision = self._strategy.on_market_state(state)
                self._entry_lane.record_decision(
                    decision=decision,
                    state=state,
                    recorded_at=self._clock(),
                    filter_context={
                        "context_available": False,
                        "context_error_type": type(admission_error).__name__,
                    },
                )
                if self._telemetry is not None:
                    await self._telemetry.strategy_decision(
                        state,
                        occurred_at=self._clock(),
                        signal_count=len(decision.signals),
                        candidate_count=len(decision.candidates),
                        details=decision_details,
                        empty_heartbeat_eligible=_empty_heartbeat_eligible(
                            state.symbol,
                            entry_symbols=self._entry_lane.entry_symbols,
                            open_position_symbols=None,
                        ),
                    )
                processed += 1
                final_state_at = state.bucket_start
                self._record_processed_state(state, saved_at=state.bucket_end)
                log.warning(
                    "live_runtime_context_degraded",
                    run_id=self._run_id,
                    symbol=state.symbol,
                    error_type=type(admission_error).__name__,
                )
                continue
            if admission.context is None:
                raise RuntimeError("market state admission is incomplete")
            context = admission.context
            if context.unmanaged_position_symbols:
                log.warning(
                    "live_unmanaged_positions_observed",
                    run_id=self._run_id,
                    symbols=sorted(context.unmanaged_position_symbols),
                )
            # Use the already loaded account facts to request background cleanup.
            # No REST cancellation or reconciliation owns this market event.
            position_symbols = (
                (context.open_position_symbols or frozenset())
                | context.pending_position_symbols
                | context.unmanaged_position_symbols
            )
            orphan_plans = tuple(
                order.plan
                for order in context.unresolved_orders
                if order.plan.reduce_only
                and order.plan.order_type == "LIMIT"
                and order.plan.symbol not in position_symbols
            )
            if orphan_plans:
                self._request_order_cleanup(orphan_plans)
            if (
                self._exit_manager is not None
                and self._exit_enabled()
                and self._exit_manager.uses_market_state_exit
            ):
                await self._exit_lane.submit_market(state)
                await asyncio.sleep(0)
            decision = self._strategy.on_market_state(state)
            if self._decision_fact_binder is not None:
                self._decision_fact_binder(context)
            if self._decision_filter is not None:
                decision = await self._decision_filter(decision, state)
            decision_recorded_at = self._clock()
            if self._telemetry is not None:
                await self._telemetry.strategy_decision(
                    state,
                    occurred_at=decision_recorded_at,
                    signal_count=len(decision.signals),
                    candidate_count=len(decision.candidates),
                    details=decision_details,
                    empty_heartbeat_eligible=_empty_heartbeat_eligible(
                        state.symbol,
                        entry_symbols=self._entry_lane.entry_symbols,
                        open_position_symbols=context.open_position_symbols,
                    ),
                )
            entry_outcome = await self._entry_lane.process(
                decision=decision,
                state=state,
                context=context,
                recorded_at=decision_recorded_at,
            )
            approved += entry_outcome.approved_intent_count
            submitted += entry_outcome.submitted_order_count
            processed += 1
            final_state_at = state.bucket_start
            self._record_processed_state(state, saved_at=context.now)
        await self._checkpoint_coordinator.save_final()
        return market_runtime_contracts.LiveDaemonResult(
            processed,
            approved,
            submitted,
            None,
            final_state_at,
        )

    def _record_processed_state(
        self,
        state: MarketState15s,
        *,
        saved_at: datetime,
    ) -> None:
        if self._commit_market_state_cursor is not None:
            self._commit_market_state_cursor(state)
        self._checkpoint_coordinator.record_processed_state(
            state,
            saved_at=saved_at,
        )

    async def _recover_gap(
        self,
        error: market_runtime_contracts.LiveMarketStateContinuityError,
    ) -> tuple[MarketState15s, ...]:
        loader = self._recover_market_state_gap
        if loader is None:
            return ()
        try:
            states = tuple(await loader(error))
        except Exception as recovery_error:
            log.warning(
                "live_strategy_market_state_gap_recovery_failed",
                run_id=self._run_id,
                symbol=error.symbol,
                previous_at=error.previous_at.isoformat(),
                current_at=error.current_at.isoformat(),
                error_type=type(recovery_error).__name__,
            )
            return ()
        if not _is_complete_gap_recovery(error, states, self._strategy):
            return ()
        try:
            for recovered in states:
                self._strategy.warm_market_state(recovered)
                self._checkpoint_coordinator.record_recovered_state(
                    recovered,
                    saved_at=self._clock(),
                )
        except Exception as recovery_error:
            self._strategy.reset_symbol(error.symbol)
            self._checkpoint_coordinator.forget_symbol(error.symbol)
            log.warning(
                "live_strategy_market_state_gap_recovery_failed",
                run_id=self._run_id,
                symbol=error.symbol,
                previous_at=error.previous_at.isoformat(),
                current_at=error.current_at.isoformat(),
                error_type=type(recovery_error).__name__,
            )
            return ()
        log.warning(
            "live_strategy_market_state_gap_recovered",
            run_id=self._run_id,
            symbol=error.symbol,
            previous_at=error.previous_at.isoformat(),
            current_at=error.current_at.isoformat(),
            recovered_bucket_count=len(states),
        )
        return states


def _strategy_decision_details(
    *,
    strategy: RuntimeStrategy,
    state: MarketState15s,
    last_processed_at: datetime | None,
    recovered_bucket_count: int,
    hub_cursor_provider: Callable[[], Mapping[str, str | int] | None] | None,
) -> dict[str, JsonValue]:
    details: dict[str, JsonValue] = {
        "market_state_input_fingerprint": market_state_input_fingerprint(state),
        "last_processed_at_before": (
            None if last_processed_at is None else last_processed_at.isoformat()
        ),
        "gap_recovered_bucket_count": recovered_bucket_count,
        "input_data_complete": state.data_complete,
        "input_missing_agg_trade_count": state.missing_agg_trade_count,
    }
    details["strategy_buffered_state_count"] = strategy.buffered_state_count
    details["strategy_buffered_symbol_count"] = strategy.buffered_symbol_count
    if hub_cursor_provider is None:
        return details
    try:
        cursor = hub_cursor_provider()
    except Exception as error:
        log.warning(
            "live_strategy_hub_cursor_snapshot_failed",
            error_type=type(error).__name__,
        )
        return details
    if not isinstance(cursor, Mapping):
        return details
    stream_id = cursor.get("stream_id")
    sequence = cursor.get("sequence")
    if isinstance(stream_id, str) and stream_id.strip():
        details["hub_stream_id"] = stream_id
    if isinstance(sequence, int) and not isinstance(sequence, bool) and sequence >= 0:
        details["hub_sequence"] = sequence
    return details


def _empty_heartbeat_eligible(
    symbol: str,
    *,
    entry_symbols: frozenset[str] | None,
    open_position_symbols: frozenset[str] | None,
) -> bool:
    """Whether an empty strategy-output heartbeat should be durable for ``symbol``.

    ``entry_symbols is None`` means the pool is unconfigured (legacy), so every
    monitored symbol stays eligible.  Otherwise only entry-pool members and
    symbols that currently hold a position keep the 60s empty heartbeat.
    """

    if entry_symbols is None:
        return True
    if symbol in entry_symbols:
        return True
    return symbol in (open_position_symbols or frozenset())


def _validate_market_state_continuity(
    *,
    state: MarketState15s,
    last_processed_at: datetime | None,
    expected_interval_seconds: int,
) -> None:
    if last_processed_at is None:
        return
    delta_seconds = (state.bucket_start - last_processed_at).total_seconds()
    # Multiple messages for one bucket are valid because the Hub publishes
    # one state per symbol.  A strictly later timestamp must be the next
    # canonical bucket; otherwise at least one bucket was lost or skipped.
    if delta_seconds <= 0 or delta_seconds == expected_interval_seconds:
        return
    raise market_runtime_contracts.LiveMarketStateContinuityError(
        symbol=state.symbol,
        previous_at=last_processed_at,
        current_at=state.bucket_start,
        expected_interval_seconds=expected_interval_seconds,
    )


def _reset_strategy_for_gap(
    *,
    strategy: RuntimeStrategy,
    symbol: str,
    current_at: datetime,
    last_processed_at: datetime | None,
    max_gap_seconds: int | None,
) -> None:
    if last_processed_at is None or max_gap_seconds is None:
        return
    if (current_at - last_processed_at).total_seconds() <= max_gap_seconds:
        return
    strategy.reset_symbol(symbol)


def _is_complete_gap_recovery(
    error: market_runtime_contracts.LiveMarketStateContinuityError,
    states: Sequence[MarketState15s],
    strategy: RuntimeStrategy,
) -> bool:
    if (
        error.observed_delta_seconds <= 0
        or error.observed_delta_seconds % error.expected_interval_seconds != 0
    ):
        return False
    missing_bucket_count = (
        error.observed_delta_seconds // error.expected_interval_seconds - 1
    )
    interval = timedelta(seconds=error.expected_interval_seconds)
    expected = tuple(
        error.previous_at + interval * index
        for index in range(1, missing_bucket_count + 1)
    )
    if not states or len(states) != len(expected):
        return False
    ordered = tuple(sorted(states, key=lambda state: state.bucket_start))
    if any(state.symbol != error.symbol for state in ordered):
        return False
    if tuple(state.bucket_start for state in ordered) != expected:
        return False
    if any(not state.data_complete for state in ordered):
        return False
    requirement = strategy.required_data()
    required_fields = requirement.required_fields
    return all(
        all(getattr(state, field) is not None for field in required_fields)
        for state in ordered
    )


__all__ = ["LiveMarketLoop"]
