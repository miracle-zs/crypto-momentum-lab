"""Entry decision and candidate execution lane for live rollout.

The lane owns entry-specific filtering, policy comparison, signal recording,
and candidate iteration.  Exchange submission remains an injected adapter so
the lane can enforce ordering and fail-closed rules without owning the
daemon's exchange or persistence implementations.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Protocol

import structlog

from crypto_momentum_lab.domain.execution import ExchangeOrderState
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import (
    EntryPolicyComparison,
    EntryPolicyComparisonRequest,
    EntryType,
    OrderIntentCandidate,
    StrategyDecision,
    UniverseRankingSnapshot,
    compare_entry_policy_request,
    summarize_entry_policy_comparisons,
)
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderExecutionResult,
)
from crypto_momentum_lab.live_rollout.context import (
    LiveDaemonRuntimeContext,
    LiveEntryFilterContext,
)
from crypto_momentum_lab.live_rollout.signal_recorder import (
    LiveSignalRecorderPort,
)
from crypto_momentum_lab.live_rollout.telemetry import (
    LIVE_LANE_ENTRY,
    LiveTelemetrySink,
    state_trace_id,
)

log = structlog.get_logger()


EntrySymbolLoader = Callable[[datetime], Awaitable[frozenset[str]]]
EntryFilterContextLoader = Callable[
    [MarketState15s], Awaitable[LiveEntryFilterContext | None]
]
EntryUniverseContextProvider = Callable[
    [str, datetime], Mapping[str, object] | None
]
EntryUniverseSnapshotProvider = Callable[
    [datetime], UniverseRankingSnapshot | None
]


class EntryCandidateExecutor(Protocol):
    async def __call__(
        self,
        candidate: OrderIntentCandidate,
        *,
        requested_quantity: Decimal | None,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
    ) -> OrderExecutionResult | None: ...


@dataclass(frozen=True, slots=True)
class EntryLaneConfig:
    run_id: str
    entry_symbol_loader: EntrySymbolLoader | None = None
    entry_symbol_refresh_seconds: float = 15.0
    entry_filter_context_loader: EntryFilterContextLoader | None = None
    entry_universe_context_provider: (
        EntryUniverseContextProvider | None
    ) = None
    entry_universe_snapshot_provider: (
        EntryUniverseSnapshotProvider | None
    ) = None
    entry_long_only: bool = False
    require_price_above_ema5: bool = False
    require_price_above_ema10: bool = False
    entry_policy_compare_only: bool = False
    entry_policy_enforce: bool = False
    entry_order_type: EntryType = EntryType.LIMIT
    entry_limit_ttl_seconds: int = 900

    def __post_init__(self) -> None:
        if not self.run_id.strip():
            raise ValueError("run_id must not be empty")
        if self.entry_symbol_refresh_seconds <= 0:
            raise ValueError("entry_symbol_refresh_seconds must be positive")
        if self.entry_limit_ttl_seconds < 601:
            raise ValueError("entry_limit_ttl_seconds must be at least 601")
        if not isinstance(self.entry_order_type, EntryType):
            raise TypeError("entry_order_type must be an EntryType")
        if self.entry_policy_compare_only and self.entry_policy_enforce:
            raise ValueError(
                "entry_policy_compare_only and entry_policy_enforce "
                "are mutually exclusive"
            )


@dataclass(frozen=True, slots=True)
class EntryLaneOutcome:
    approved_intent_count: int = 0
    submitted_order_count: int = 0
    pending_reconciliation: bool = False


@dataclass(frozen=True, slots=True)
class _EntryPolicyEvaluation:
    """One immutable policy evaluation shared by execution and telemetry."""

    comparisons: tuple[EntryPolicyComparison, ...] = ()
    skip_reason: str | None = None
    universe_snapshot_error: str | None = None

    def comparison_for(
        self,
        candidate_id: str,
    ) -> EntryPolicyComparison | None:
        return next(
            (
                comparison
                for comparison in self.comparisons
                if comparison.candidate_id == candidate_id
            ),
            None,
        )


class EntryExecutionLane:
    """Admit and execute entry candidates for one market decision.

    The interface is one decision in and one aggregate outcome out.  The lane
    keeps its symbol-pool refresh state, evaluates policy once, records the
    same decision that it executes, and stops after an uncertain submission so
    callers cannot continue authorizing entries from the same stale context.
    """

    def __init__(
        self,
        *,
        config: EntryLaneConfig,
        clock: Callable[[], datetime],
        entry_enabled: Callable[[], bool],
        entry_enabled_reason: Callable[[], str],
        execute_candidate: EntryCandidateExecutor,
        invalidate_context: Callable[[], None],
        telemetry: LiveTelemetrySink | None = None,
        signal_recorder: LiveSignalRecorderPort | None = None,
    ) -> None:
        self._config = config
        self._clock = clock
        self._entry_enabled = entry_enabled
        self._entry_enabled_reason = entry_enabled_reason
        self._execute_candidate = execute_candidate
        self._invalidate_context = invalidate_context
        self._telemetry = telemetry
        self._signal_recorder = signal_recorder
        self._entry_symbols: frozenset[str] | None = None
        self._entry_symbols_loaded_at: datetime | None = None

    @property
    def entry_symbols(self) -> frozenset[str] | None:
        return self._entry_symbols

    def reset(self) -> None:
        """Reset per-run entry-pool state before a new market loop."""
        self._entry_symbols = None
        self._entry_symbols_loaded_at = None

    def record_decision(
        self,
        *,
        decision: StrategyDecision,
        state: MarketState15s,
        recorded_at: datetime,
        context: LiveDaemonRuntimeContext | None = None,
        gate_reasons: tuple[str, ...] = (),
        entry_symbols: frozenset[str] | None = None,
        entry_filter_context: LiveEntryFilterContext | None = None,
        filter_context: Mapping[str, object] | None = None,
        policy_evaluation: _EntryPolicyEvaluation | None = None,
    ) -> None:
        recorder = self._signal_recorder
        if recorder is None:
            return
        candidate_filter_results: dict[str, object] = {}
        for candidate in decision.candidates:
            rejection_reason = _live_entry_candidate_rejection_reason(
                candidate,
                entry_enabled=self._entry_enabled(),
                entry_long_only=self._config.entry_long_only,
                entry_symbols=entry_symbols,
                context=entry_filter_context,
                require_price_above_ema5=self._config.require_price_above_ema5,
                require_price_above_ema10=self._config.require_price_above_ema10,
                now=recorded_at,
            )
            candidate_filter_results[candidate.candidate_id] = {
                "symbol": candidate.symbol,
                "side": _enum_text(candidate.side),
                "reduce_only": candidate.reduce_only,
                "passed": rejection_reason is None,
                "rejection_reason": rejection_reason,
            }
        if policy_evaluation is None:
            policy_evaluation = self._evaluate_entry_policy(
                decision=decision,
                state=state,
                recorded_at=recorded_at,
                context=context,
                gate_reasons=gate_reasons,
                entry_symbols=entry_symbols,
                entry_filter_context=entry_filter_context,
                filter_context=filter_context,
            )
        policy_comparisons = [
            comparison.as_details()
            for comparison in policy_evaluation.comparisons
        ]
        policy_comparison_summary: dict[str, object] | None = None
        if (
            self._config.entry_policy_compare_only
            or self._config.entry_policy_enforce
        ) and (
            policy_evaluation.comparisons
            or policy_evaluation.skip_reason is None
        ):
            policy_comparison_summary = summarize_entry_policy_comparisons(
                policy_evaluation.comparisons,
                reduce_only_skipped=sum(
                    candidate.reduce_only for candidate in decision.candidates
                ),
            ).as_details()
        policy_enforce_skip_reason = policy_evaluation.skip_reason
        if (
            policy_enforce_skip_reason is None
            and self._config.entry_policy_enforce
            and policy_evaluation.universe_snapshot_error is not None
        ):
            policy_enforce_skip_reason = "universe_snapshot_error"
        details: dict[str, object] = dict(filter_context or {})
        universe_context_provider = (
            self._config.entry_universe_context_provider
        )
        if universe_context_provider is not None:
            try:
                universe_context = universe_context_provider(
                    state.symbol,
                    state.bucket_end,
                )
            except Exception as error:
                log.warning(
                    "live_strategy_signal_universe_context_failed",
                    run_id=self._config.run_id,
                    symbol=state.symbol,
                    error_type=type(error).__name__,
                )
            else:
                if universe_context is not None:
                    details["universe"] = dict(universe_context)
        effective_entry_candidates = {
            candidate.candidate_id: _planned_entry_execution_context(
                candidate,
                state=state,
                execution_now=recorded_at,
                entry_order_type=self._config.entry_order_type,
                limit_ttl_seconds=self._config.entry_limit_ttl_seconds,
            )
            for candidate in decision.candidates
            if not candidate.reduce_only
        }
        details.update(
            {
                "entry_enabled": self._entry_enabled(),
                "entry_enabled_reason": self._entry_enabled_reason(),
                "entry_long_only": self._config.entry_long_only,
                "entry_symbol_pool_configured": entry_symbols is not None,
                "entry_symbol_pool_size": (
                    None if entry_symbols is None else len(entry_symbols)
                ),
                "require_price_above_ema5": (
                    self._config.require_price_above_ema5
                ),
                "require_price_above_ema10": (
                    self._config.require_price_above_ema10
                ),
                "gate_reasons": list(gate_reasons),
                "signal_count": len(decision.signals),
                "candidate_count": len(decision.candidates),
                "rejection_count": len(decision.rejections),
                "strategy_rejections": [
                    {
                        "reason": _enum_text(rejection.reason),
                        "symbol": rejection.symbol,
                        "bucket_start": rejection.bucket_start,
                        "details": rejection.details,
                    }
                    for rejection in decision.rejections
                ],
                "entry_filter_values": _entry_filter_values(
                    entry_filter_context
                ),
                "market_state_bucket_start": state.bucket_start,
                "market_state_bucket_end": state.bucket_end,
                "market_state_last_received_at": state.last_received_at,
                "market_state_age_seconds": round(
                    _market_state_age_seconds(state, recorded_at),
                    3,
                ),
                "candidate_filter_results": candidate_filter_results,
                "entry_policy_compare_only": (
                    self._config.entry_policy_compare_only
                ),
                "entry_policy_enforce": self._config.entry_policy_enforce,
                "entry_policy_mode": (
                    "enforce"
                    if self._config.entry_policy_enforce
                    else (
                        "compare_only"
                        if self._config.entry_policy_compare_only
                        else "legacy"
                    )
                ),
                "entry_policy_comparisons": policy_comparisons,
                "entry_policy_comparison_summary": policy_comparison_summary,
                "entry_policy_compare_skip_reason": policy_evaluation.skip_reason,
                "entry_policy_enforce_skip_reason": policy_enforce_skip_reason,
                "entry_policy_universe_snapshot_error": (
                    policy_evaluation.universe_snapshot_error
                ),
                "effective_entry_candidates": effective_entry_candidates,
            }
        )
        try:
            recorder.record_decision(
                decision=decision,
                state=state,
                recorded_at=recorded_at,
                account_context=_live_signal_account_context(
                    context,
                    gate_reasons=gate_reasons,
                ),
                filter_context=details,
            )
        except Exception as error:
            log.warning(
                "live_strategy_signal_recorder_failed",
                run_id=self._config.run_id,
                error_type=type(error).__name__,
            )

    async def process(
        self,
        *,
        decision: StrategyDecision,
        state: MarketState15s,
        context: LiveDaemonRuntimeContext,
        gate_reasons: tuple[str, ...],
        recorded_at: datetime,
    ) -> EntryLaneOutcome:
        has_entry_candidates = self._entry_enabled() and any(
            not candidate.reduce_only for candidate in decision.candidates
        )
        if has_entry_candidates and self._config.entry_symbol_loader is not None:
            if (
                self._entry_symbols_loaded_at is None
                or (
                    state.bucket_start - self._entry_symbols_loaded_at
                ).total_seconds()
                >= self._config.entry_symbol_refresh_seconds
            ):
                try:
                    self._entry_symbols = await self._config.entry_symbol_loader(
                        state.bucket_start
                    )
                except Exception:
                    # Entry-pool lookup is fail-closed. Exit handling remains
                    # independent, so an outage cannot strand open positions.
                    self._entry_symbols = frozenset()
                self._entry_symbols_loaded_at = state.bucket_start

        entry_filter_context = None
        if (
            has_entry_candidates
            and (
                self._config.require_price_above_ema5
                or self._config.require_price_above_ema10
            )
            and self._config.entry_filter_context_loader is not None
        ):
            try:
                entry_filter_context = (
                    await self._config.entry_filter_context_loader(state)
                )
            except Exception:
                # Missing or stale EMA data must not authorize a live entry.
                entry_filter_context = None
        if self._telemetry is not None:
            await self._telemetry.entry_filter_ready(
                state,
                occurred_at=self._clock(),
                candidate_count=len(decision.candidates),
                symbol_pool_loaded=(
                    not has_entry_candidates or self._entry_symbols is not None
                ),
                ema_context_loaded=(
                    not has_entry_candidates
                    or not (
                        self._config.require_price_above_ema5
                        or self._config.require_price_above_ema10
                    )
                    or entry_filter_context is not None
                ),
            )
        policy_evaluation = self._evaluate_entry_policy(
            decision=decision,
            state=state,
            recorded_at=recorded_at,
            context=context,
            gate_reasons=gate_reasons,
            entry_symbols=self._entry_symbols,
            entry_filter_context=entry_filter_context,
            filter_context={
                "context_available": True,
                "gate_approved": True,
            },
        )
        self.record_decision(
            decision=decision,
            state=state,
            recorded_at=recorded_at,
            context=context,
            gate_reasons=gate_reasons,
            entry_symbols=self._entry_symbols,
            entry_filter_context=entry_filter_context,
            policy_evaluation=policy_evaluation,
        )
        if self._telemetry is not None:
            await self._telemetry.signal_recorded(
                state,
                occurred_at=self._clock(),
                candidate_count=len(decision.candidates),
            )

        approved = submitted = 0
        pending_reconciliation = False
        for candidate in decision.candidates:
            if (
                not candidate.reduce_only
                and self._config.entry_policy_enforce
            ):
                comparison = policy_evaluation.comparison_for(
                    candidate.candidate_id
                )
                if (
                    policy_evaluation.skip_reason is not None
                    or policy_evaluation.universe_snapshot_error is not None
                    or comparison is None
                    or not comparison.policy_decision.eligible
                ):
                    continue
            elif (
                _live_entry_candidate_rejection_reason(
                    candidate,
                    entry_enabled=self._entry_enabled(),
                    entry_long_only=self._config.entry_long_only,
                    entry_symbols=self._entry_symbols,
                    context=entry_filter_context,
                    require_price_above_ema5=(
                        self._config.require_price_above_ema5
                    ),
                    require_price_above_ema10=(
                        self._config.require_price_above_ema10
                    ),
                    now=recorded_at,
                )
                is not None
            ):
                continue
            result = await self._execute_candidate(
                candidate,
                requested_quantity=None,
                state=state,
                context=context,
            )
            if result is None:
                continue
            self._invalidate_context()
            approved += 1
            submitted += int(not result.suppressed)
            if result.state is ExchangeOrderState.UNKNOWN_PENDING_RECONCILIATION:
                pending_reconciliation = True
                log.warning(
                    "live_order_outcome_pending_reconciliation",
                    run_id=self._config.run_id,
                    symbol=candidate.symbol,
                    client_order_id=result.client_order_id,
                )
                break
        return EntryLaneOutcome(
            approved_intent_count=approved,
            submitted_order_count=submitted,
            pending_reconciliation=pending_reconciliation,
        )

    def _evaluate_entry_policy(
        self,
        *,
        decision: StrategyDecision,
        state: MarketState15s,
        recorded_at: datetime,
        context: LiveDaemonRuntimeContext | None,
        gate_reasons: tuple[str, ...],
        entry_symbols: frozenset[str] | None,
        entry_filter_context: LiveEntryFilterContext | None,
        filter_context: Mapping[str, object] | None,
    ) -> _EntryPolicyEvaluation:
        if not (
            self._config.entry_policy_compare_only
            or self._config.entry_policy_enforce
        ):
            return _EntryPolicyEvaluation()
        context_available = (filter_context or {}).get(
            "context_available",
            context is not None,
        )
        if context_available is False:
            return _EntryPolicyEvaluation(skip_reason="context_unavailable")

        universe_snapshot: UniverseRankingSnapshot | None = None
        universe_snapshot_error: str | None = None
        universe_snapshot_provider = (
            self._config.entry_universe_snapshot_provider
        )
        if universe_snapshot_provider is not None:
            try:
                universe_snapshot = universe_snapshot_provider(
                    state.bucket_end
                )
            except Exception as error:
                universe_snapshot_error = type(error).__name__
                log.warning(
                    "live_strategy_signal_universe_snapshot_failed",
                    run_id=self._config.run_id,
                    symbol=state.symbol,
                    error_type=type(error).__name__,
                )

        entry_price = (
            None
            if entry_filter_context is None
            else entry_filter_context.entry_price
        )
        ema5 = (
            None if entry_filter_context is None else entry_filter_context.ema5
        )
        ema10 = (
            None if entry_filter_context is None else entry_filter_context.ema10
        )
        source_trace = state_trace_id(state, LIVE_LANE_ENTRY)
        comparisons: list[EntryPolicyComparison] = []
        for candidate in decision.candidates:
            if candidate.reduce_only:
                continue
            legacy_rejection_reason = _live_entry_candidate_rejection_reason(
                candidate,
                entry_enabled=self._entry_enabled(),
                entry_long_only=self._config.entry_long_only,
                entry_symbols=entry_symbols,
                context=entry_filter_context,
                require_price_above_ema5=self._config.require_price_above_ema5,
                require_price_above_ema10=self._config.require_price_above_ema10,
                now=recorded_at,
            )
            comparisons.append(
                compare_entry_policy_request(
                    EntryPolicyComparisonRequest(
                        candidate=candidate,
                        source_trace_id=source_trace,
                        legacy_rejection_reason=legacy_rejection_reason,
                        gate_reasons=gate_reasons,
                        entry_enabled=self._entry_enabled(),
                        entry_long_only=self._config.entry_long_only,
                        entry_symbols=entry_symbols,
                        universe_snapshot=universe_snapshot,
                        entry_price=entry_price,
                        ema5=ema5,
                        ema10=ema10,
                        require_price_above_ema5=(
                            self._config.require_price_above_ema5
                        ),
                        require_price_above_ema10=(
                            self._config.require_price_above_ema10
                        ),
                        observed_at=recorded_at,
                        ema_observed_at=(
                            None
                            if entry_filter_context is None
                            else entry_filter_context.ema_observed_at
                        ),
                        ema_snapshot_id=(
                            None
                            if entry_filter_context is None
                            else entry_filter_context.ema_snapshot_id
                        ),
                        ema_config_hash=(
                            None
                            if entry_filter_context is None
                            else entry_filter_context.ema_config_hash
                        ),
                    )
                )
            )
        return _EntryPolicyEvaluation(
            comparisons=tuple(comparisons),
            universe_snapshot_error=universe_snapshot_error,
        )


def _live_entry_candidate_passes(
    candidate: OrderIntentCandidate,
    *,
    context: LiveEntryFilterContext | None,
    require_price_above_ema5: bool,
    require_price_above_ema10: bool,
) -> bool:
    if candidate.reduce_only:
        return True
    if not require_price_above_ema5 and not require_price_above_ema10:
        return True
    if context is None or context.entry_price is None:
        return False
    if require_price_above_ema5 and (
        context.ema5 is None or context.entry_price <= context.ema5
    ):
        return False
    if require_price_above_ema10 and (
        context.ema10 is None or context.entry_price <= context.ema10
    ):
        return False
    return True


def _live_entry_candidate_rejection_reason(
    candidate: OrderIntentCandidate,
    *,
    entry_enabled: bool,
    entry_long_only: bool,
    entry_symbols: frozenset[str] | None,
    context: LiveEntryFilterContext | None,
    require_price_above_ema5: bool,
    require_price_above_ema10: bool,
    now: datetime | None = None,
) -> str | None:
    """Return the entry filter reason used by the execution lane."""

    if candidate.reduce_only:
        return None
    if not entry_enabled:
        return "entry_disabled"
    if now is not None and candidate.expires_at <= now:
        return "candidate_expired"
    if (
        entry_long_only
        and getattr(candidate.side, "value", candidate.side) != "long"
    ):
        return "short_entries_disabled"
    if entry_symbols is not None and candidate.symbol not in entry_symbols:
        return "outside_entry_symbol_pool"
    if not _live_entry_candidate_passes(
        candidate,
        context=context,
        require_price_above_ema5=require_price_above_ema5,
        require_price_above_ema10=require_price_above_ema10,
    ):
        return "ema_filter_failed"
    return None


def _market_state_age_seconds(
    state: MarketState15s,
    now: datetime,
) -> float:
    return max(0.0, (now - state.bucket_end).total_seconds())


def _entry_filter_values(
    context: LiveEntryFilterContext | None,
) -> dict[str, object]:
    if context is None:
        return {
            "entry_price": None,
            "ema5": None,
            "ema10": None,
            "ema_observed_at": None,
            "ema_snapshot_id": None,
            "ema_config_hash": None,
        }
    return {
        "entry_price": context.entry_price,
        "ema5": context.ema5,
        "ema10": context.ema10,
        "ema_observed_at": context.ema_observed_at,
        "ema_snapshot_id": context.ema_snapshot_id,
        "ema_config_hash": context.ema_config_hash,
    }


def _live_signal_account_context(
    context: LiveDaemonRuntimeContext | None,
    *,
    gate_reasons: tuple[str, ...] = (),
) -> dict[str, object]:
    if context is None:
        return {
            "context_available": False,
            "gate_reasons": list(gate_reasons),
        }
    lease = context.active_lease
    return {
        "context_available": True,
        "account_state": _enum_text(context.account_state),
        "account_observed_at": context.account_observed_at,
        "account_snapshot_version": context.account_snapshot_version,
        "realized_pnl": context.realized_pnl,
        "unrealized_pnl": context.unrealized_pnl,
        "gross_exposure": context.gross_exposure,
        "open_position_symbols": sorted(context.open_position_symbols or ()),
        "managed_position_symbols": sorted(
            position.symbol for position in context.managed_positions
        ),
        "pending_position_symbols": sorted(context.pending_position_symbols),
        "unmanaged_position_symbols": sorted(
            context.unmanaged_position_symbols
        ),
        "active_halt_count": len(context.active_halts),
        "active_halt_reasons": [halt.reason for halt in context.active_halts],
        "unresolved_order_count": len(context.unresolved_order_states),
        "unresolved_order_states": [
            _enum_text(state) for state in context.unresolved_order_states
        ],
        "active_lease_state": (
            None if lease is None else _enum_text(lease.state)
        ),
        "active_lease_expires_at": None if lease is None else lease.expires_at,
        "risk_config": {
            "max_order_notional": context.risk_config.max_order_notional,
            "max_gross_notional": context.risk_config.max_gross_notional,
            "max_daily_loss": context.risk_config.max_daily_loss,
            "max_open_positions": context.risk_config.max_open_positions,
        },
        "gate_reasons": list(gate_reasons),
    }


def _enum_text(value: object) -> str:
    return str(getattr(value, "value", value))


def _planned_entry_execution_context(
    candidate: OrderIntentCandidate,
    *,
    state: MarketState15s,
    execution_now: datetime,
    entry_order_type: EntryType,
    limit_ttl_seconds: int,
) -> dict[str, object]:
    prepared = _prepare_entry_candidate_for_observation(
        candidate,
        state=state,
        execution_now=execution_now,
        entry_order_type=entry_order_type,
        limit_ttl_seconds=limit_ttl_seconds,
    )
    if prepared.entry_type is not EntryType.LIMIT:
        price_source = None
    else:
        _, price_source = _entry_limit_price(candidate, state=state)
    return {
        "original_entry_type": _enum_text(candidate.entry_type),
        "original_limit_price": candidate.limit_price,
        "effective_entry_type": _enum_text(prepared.entry_type),
        "effective_limit_price": prepared.limit_price,
        "effective_limit_price_source": price_source,
        "effective_expires_at": prepared.expires_at,
        "desired_notional": candidate.desired_notional,
    }


def _entry_limit_price(
    candidate: OrderIntentCandidate,
    *,
    state: MarketState15s,
) -> tuple[Decimal | None, str | None]:
    if candidate.limit_price is not None:
        return candidate.limit_price, "candidate.limit_price"
    if (
        _enum_text(candidate.side) == "long"
        and state.last_ask_price is not None
        and state.close_price is not None
    ):
        return (
            min(state.last_ask_price, state.close_price),
            "min(state.last_ask_price,state.close_price)",
        )
    if _enum_text(candidate.side) == "long" and state.last_ask_price is not None:
        return state.last_ask_price, "state.last_ask_price"
    if state.close_price is not None:
        return state.close_price, "state.close_price"
    if state.mark_price is not None:
        return state.mark_price, "state.mark_price"
    if state.midpoint is not None:
        return state.midpoint, "state.midpoint"
    return None, None


def _prepare_entry_candidate_for_observation(
    candidate: OrderIntentCandidate,
    *,
    state: MarketState15s,
    execution_now: datetime,
    entry_order_type: EntryType,
    limit_ttl_seconds: int,
) -> OrderIntentCandidate:
    if candidate.reduce_only or entry_order_type is EntryType.MARKET:
        return candidate
    signal_price, _ = _entry_limit_price(candidate, state=state)
    return replace(
        candidate,
        entry_type=EntryType.LIMIT,
        limit_price=signal_price,
        expires_at=execution_now + timedelta(seconds=limit_ttl_seconds),
    )
