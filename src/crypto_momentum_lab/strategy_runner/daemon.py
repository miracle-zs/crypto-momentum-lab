import asyncio
from collections import deque
from collections.abc import Callable, Coroutine, Iterable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from inspect import Parameter, signature
from typing import Protocol
from uuid import NAMESPACE_URL, uuid5

import structlog

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import (
    EntryPolicyComparison,
    EntryPolicyComparisonRequest,
    OrderIntentCandidate,
    RejectionReason,
    RunMode,
    StrategyCheckpoint,
    StrategyDataRequirement,
    StrategyDecision,
    StrategyRejection,
    StrategyRunIdentity,
    StrategySide,
    StrategySignal,
    UniverseRankingSnapshot,
    compare_entry_policy_request,
    summarize_entry_policy_comparisons,
)
from crypto_momentum_lab.strategy_runner.candle_source import (
    ClosedCandle15mSource,
    ClosedCandleSourceError,
)
from crypto_momentum_lab.strategy_runner.fills import (
    ReplayExecutionConfig,
    SimulatedFill,
    resolve_candidate_fill_at_state,
)
from crypto_momentum_lab.strategy_runner.portfolio import (
    Candle15mAggregator,
    ClosedCandle15m,
    PaperExitConfig,
    PaperExitMode,
    PaperPosition,
    PaperPositionStatus,
    mark_positions,
)

log = structlog.get_logger()


class Clock(Protocol):
    def now(self) -> datetime:
        pass


PaperEntryPolicyComparisonObserver = Callable[
    [MarketState15s, tuple[EntryPolicyComparison, ...]],
    None,
]


class RuntimeStrategy(Protocol):
    def restore_checkpoint(self, checkpoint: StrategyCheckpoint) -> None:
        pass

    def on_market_state(self, state: MarketState15s) -> StrategyDecision:
        pass

    def checkpoint(self) -> StrategyCheckpoint:
        pass

    def warm_market_state(self, state: MarketState15s) -> None:
        pass

    def required_data(self) -> StrategyDataRequirement:
        pass


_PAPER_RECOVERY_STATE_LIMIT = 100_000
_CANDLE_SOURCE_RETRY_SECONDS = 30.0
_LEGACY_CANDLE_CURSOR_LOOKBACK = timedelta(minutes=15)


class PaperLiveDaemonRepository(Protocol):
    async def save_checkpoint(
        self,
        run_id: str,
        checkpoint: StrategyCheckpoint,
        saved_at: datetime,
    ) -> None:
        pass

    async def load_checkpoint(self, run_id: str) -> StrategyCheckpoint | None:
        pass


class PaperLiveArtifactRepository(Protocol):
    async def initialize_run(
        self,
        identity: StrategyRunIdentity,
        source_description: str,
        execution: ReplayExecutionConfig,
        portfolio: PaperExitConfig,
        entry_filter: "PaperEntryFilterConfig",
    ) -> None:
        pass

    async def load_pending_candidates(
        self,
        run_id: str,
    ) -> tuple[OrderIntentCandidate, ...]:
        pass

    async def save_decision(self, decision: StrategyDecision) -> None:
        pass

    async def save_fills(
        self,
        run_id: str,
        fills: tuple[SimulatedFill, ...],
    ) -> tuple[PaperPosition, ...]:
        pass

    async def load_open_positions(
        self,
        run_id: str,
    ) -> tuple[PaperPosition, ...]:
        pass

    async def save_portfolio(
        self,
        run_id: str,
        positions: tuple[PaperPosition, ...],
        observed_at: datetime,
        config: PaperExitConfig,
    ) -> None:
        pass


@dataclass(frozen=True, slots=True)
class PaperEntryFilterConfig:
    allow_long: bool = True
    allow_short: bool = True
    max_abs_aggressive_imbalance: Decimal | None = None
    max_cluster_trade_count: int | None = None
    require_price_above_ema5: bool = False
    require_price_above_ema10: bool = False

    def __post_init__(self) -> None:
        if not self.allow_long and not self.allow_short:
            raise ValueError("entry filter must allow at least one side")
        if (
            self.max_abs_aggressive_imbalance is not None
            and not Decimal("0")
            < self.max_abs_aggressive_imbalance
            <= Decimal("1")
        ):
            raise ValueError(
                "max_abs_aggressive_imbalance must be in (0, 1]"
            )
        if (
            self.max_cluster_trade_count is not None
            and self.max_cluster_trade_count <= 0
        ):
            raise ValueError("max_cluster_trade_count must be positive")


@dataclass(frozen=True, slots=True)
class PaperEntryFilterContext:
    entry_price: Decimal | None
    long_entry_price: Decimal | None = None
    short_entry_price: Decimal | None = None
    ema5: Decimal | None = None
    ema10: Decimal | None = None
    ema_observed_at: datetime | None = None
    ema_snapshot_id: str | None = None
    ema_config_hash: str | None = None


@dataclass(frozen=True, slots=True)
class PaperLiveDaemonConfig:
    run_id: str
    strategy_name: str
    environment: str
    checkpoint_every_states: int
    checkpoint_every_seconds: float
    max_market_state_age_seconds: float
    entry_symbol_refresh_seconds: float = 15.0
    run_identity: StrategyRunIdentity | None = None
    source_description: str = "paper-live"
    execution: ReplayExecutionConfig = field(
        default_factory=ReplayExecutionConfig
    )
    portfolio: PaperExitConfig = field(default_factory=PaperExitConfig)
    entry_filter: PaperEntryFilterConfig = field(
        default_factory=PaperEntryFilterConfig
    )
    entry_policy_compare_only: bool = False
    checkpoint_phase_seconds: float = 0.0

    def __post_init__(self) -> None:
        _require_non_empty(self.run_id, "run_id")
        _require_non_empty(self.strategy_name, "strategy_name")
        _require_non_empty(self.environment, "environment")
        if self.checkpoint_every_states <= 0:
            raise ValueError("checkpoint_every_states must be positive")
        if self.checkpoint_every_seconds <= 0:
            raise ValueError("checkpoint_every_seconds must be positive")
        if not 0 <= self.checkpoint_phase_seconds < self.checkpoint_every_seconds:
            raise ValueError(
                "checkpoint_phase_seconds must be in [0, checkpoint_every_seconds)"
            )
        if self.max_market_state_age_seconds <= 0:
            raise ValueError("max_market_state_age_seconds must be positive")
        if self.entry_symbol_refresh_seconds <= 0:
            raise ValueError("entry_symbol_refresh_seconds must be positive")
        if not isinstance(self.entry_policy_compare_only, bool):
            raise TypeError("entry_policy_compare_only must be a bool")
        if not self.source_description.strip():
            raise ValueError("source_description must not be empty")
        if self.run_identity is not None:
            if self.run_identity.run_id != self.run_id:
                raise ValueError("run identity run_id mismatch")
            if self.run_identity.strategy_name != self.strategy_name:
                raise ValueError("run identity strategy_name mismatch")
            if self.run_identity.run_mode is not RunMode.PAPER:
                raise ValueError("paper daemon run mode must be paper")


@dataclass(frozen=True, slots=True)
class PaperLiveDaemonResult:
    processed_state_count: int
    halt_reason: str | None
    final_cursor: datetime | None
    final_checkpoint_saved_at: datetime | None


@dataclass(frozen=True, slots=True)
class PairedPaperLiveAccount:
    """One account adapter in a shared-entry paper strategy run."""

    repository: PaperLiveDaemonRepository
    artifact_repository: PaperLiveArtifactRepository
    config: PaperLiveDaemonConfig

    def __post_init__(self) -> None:
        if self.config.run_identity is None:
            raise ValueError("paired paper account requires run_identity")


@dataclass(frozen=True, slots=True)
class PairedPaperLiveDaemonResult:
    account_results: tuple[PaperLiveDaemonResult, ...]


def run_paired_paper_live_daemon(
    *,
    source: Iterable[MarketState15s],
    strategy: RuntimeStrategy,
    accounts: tuple[PairedPaperLiveAccount, ...],
    clock: Clock,
    entry_symbol_loader: Callable[[datetime], frozenset[str]] | None = None,
    candle_source: ClosedCandle15mSource | None = None,
    entry_filter_context_loader: (
        Callable[[MarketState15s], PaperEntryFilterContext | None] | None
    ) = None,
    on_checkpoint_persisted: Callable[[], None] | None = None,
) -> PairedPaperLiveDaemonResult:
    """Run multiple exit-only variants from one shared strategy calculation."""
    if len(accounts) < 2:
        raise ValueError("at least two paired paper accounts are required")
    first_config = accounts[0].config
    first_identity = first_config.run_identity
    if first_identity is None:
        raise ValueError("paired paper account requires run_identity")
    for account in accounts[1:]:
        identity = account.config.run_identity
        if identity is None:
            raise ValueError("paired paper account requires run_identity")
        if account.config.environment != first_config.environment:
            raise ValueError("paired accounts must use one environment")
        if account.config.strategy_name != first_config.strategy_name:
            raise ValueError("paired accounts must use one strategy")
        if (
            identity.strategy_name != first_identity.strategy_name
            or identity.strategy_version != first_identity.strategy_version
            or identity.config_hash != first_identity.config_hash
        ):
            raise ValueError("paired accounts must share strategy identity")
        if (
            account.config.checkpoint_phase_seconds
            != first_config.checkpoint_phase_seconds
        ):
            raise ValueError("paired accounts must share checkpoint phase")

    checkpoints = _load_paired_checkpoints(accounts)
    cooldown_remaining_by_account: list[dict[str, int]] = [
        {}
        if checkpoint is None
        else dict(checkpoint.cooldown_buckets_remaining_by_symbol)
        for checkpoint in checkpoints
    ]
    available_checkpoints = tuple(
        checkpoint for checkpoint in checkpoints if checkpoint is not None
    )
    # Entry decisions are shared, so the newest checkpoint is authoritative.
    # A lagging account is resumed as-is; online paper trading never backfills
    # missed entries from an older cursor.
    restored_checkpoint = max(
        available_checkpoints,
        key=_checkpoint_progress,
        default=None,
    )
    if restored_checkpoint is not None:
        strategy.restore_checkpoint(restored_checkpoint)
        if _checkpoint_needs_market_recovery(restored_checkpoint):
            _restore_paper_strategy_from_checkpoint(
                strategy=strategy,
                source=source,
                checkpoint=restored_checkpoint,
            )

    pending_by_account: list[list[OrderIntentCandidate]] = []
    open_positions_by_account: list[dict[str, PaperPosition]] = []
    last_position_persisted_at_by_account: list[dict[str, datetime]] = []
    last_candle_end_by_account: list[dict[str, datetime]] = []
    legacy_candle_cursor_symbols_by_account: list[set[str]] = []
    candle_aggregators: list[Candle15mAggregator | None] = []
    candle_history_by_account: list[
        dict[str, deque[ClosedCandle15m]]
    ] = []
    for account in accounts:
        config = account.config
        identity = config.run_identity
        if identity is None:
            raise ValueError("paired paper account requires run_identity")
        _run_async(
            account.artifact_repository.initialize_run(
                identity,
                config.source_description,
                config.execution,
                config.portfolio,
                config.entry_filter,
            )
        )
        pending_by_account.append(
            list(
                _run_async(
                    account.artifact_repository.load_pending_candidates(
                        config.run_id
                    )
                )
            )
        )
        open_positions = _run_async(
            account.artifact_repository.load_open_positions(config.run_id)
        )
        open_positions_by_account.append(
            {position.position_id: position for position in open_positions}
        )
        last_position_persisted_at_by_account.append(
            {position.position_id: position.updated_at for position in open_positions}
        )
        last_candle_end_by_account.append(
            _initial_candle_cursors(open_positions)
        )
        legacy_candle_cursor_symbols_by_account.append(set())
        candle_aggregators.append(
            Candle15mAggregator()
            if (
                config.portfolio.exit_mode is PaperExitMode.CANDLE_15M
                and candle_source is None
            )
            else None
        )
        candle_history_by_account.append({})

    processed = 0
    processed_since_checkpoint = 0
    final_cursor: datetime | None = None
    checkpoint_dirty = False
    last_checkpoint_saved_at: datetime | None = None
    last_checkpoint_elapsed_anchor = clock.now()
    checkpoint_not_before = last_checkpoint_elapsed_anchor + timedelta(
        seconds=first_config.checkpoint_phase_seconds
    )
    last_equity_snapshot_at: list[datetime | None] = [None] * len(accounts)
    candle_retry_after_by_account: list[dict[str, datetime]] = [
        {} for _ in accounts
    ]
    entry_symbols: frozenset[str] | None = None
    entry_symbols_loaded_at: datetime | None = None
    gapped_symbols: set[str] = set()
    stale_symbols: set[str] = set()
    last_processed_at_by_symbol = (
        {}
        if restored_checkpoint is None
        else dict(restored_checkpoint.last_processed_at_by_symbol)
    )
    max_gap_seconds = _strategy_max_gap_seconds(strategy)

    for state in source:
        if state.environment != first_config.environment:
            raise ValueError("runtime state environment mismatch")
        if _already_processed(state, restored_checkpoint):
            continue

        now = clock.now()
        if (
            _state_age_seconds(now, state)
            > first_config.max_market_state_age_seconds
        ):
            if state.symbol not in stale_symbols:
                _log_stale_market_state(
                    state=state,
                    now=now,
                    max_age_seconds=first_config.max_market_state_age_seconds,
                    open_position_count=sum(
                        len(positions)
                        for positions in open_positions_by_account
                    ),
                )
                stale_symbols.add(state.symbol)
            if state.symbol not in gapped_symbols:
                _reset_strategy_symbol(strategy, state.symbol)
                for cooldown_remaining in cooldown_remaining_by_account:
                    cooldown_remaining.pop(state.symbol, None)
                gapped_symbols.add(state.symbol)
            continue

        if state.symbol in stale_symbols:
            _log_stale_market_state_recovered(
                state=state,
                now=now,
                open_position_count=sum(
                    len(positions) for positions in open_positions_by_account
                ),
            )
            stale_symbols.discard(state.symbol)

        if state.symbol not in gapped_symbols:
            gap_reset = _reset_strategy_for_gap(
                strategy=strategy,
                symbol=state.symbol,
                current_at=state.bucket_start,
                last_processed_at=last_processed_at_by_symbol.get(state.symbol),
                max_gap_seconds=max_gap_seconds,
            )
            if gap_reset:
                for cooldown_remaining in cooldown_remaining_by_account:
                    cooldown_remaining.pop(state.symbol, None)
        gapped_symbols.discard(state.symbol)

        if entry_symbol_loader is not None and (
            entry_symbols_loaded_at is None
            or (
                state.bucket_start - entry_symbols_loaded_at
            ).total_seconds()
            >= first_config.entry_symbol_refresh_seconds
        ):
            entry_symbols = entry_symbol_loader(state.bucket_start)
            entry_symbols_loaded_at = state.bucket_start
        entry_allowed = entry_symbols is None or state.symbol in entry_symbols

        position_updates_by_account: list[tuple[PaperPosition, ...]] = []
        for index, account in enumerate(accounts):
            config = account.config
            identity = config.run_identity
            if identity is None:
                raise ValueError("paired paper account requires run_identity")
            aggregator = candle_aggregators[index]
            observed_candle = (
                None if aggregator is None else aggregator.observe(state)
            )
            if aggregator is not None:
                _log_candle_gap_events(
                    aggregator=aggregator,
                    account_index=index,
                )
            closed_candles: tuple[ClosedCandle15m, ...] = (
                () if observed_candle is None else (observed_candle,)
            )
            if (
                not closed_candles
                and config.portfolio.exit_mode is PaperExitMode.CANDLE_15M
            ):
                retry_after = candle_retry_after_by_account[index].get(
                    state.symbol
                )
                if retry_after is None or now >= retry_after:
                    after = last_candle_end_by_account[index].get(state.symbol)
                    if (
                        candle_source is not None
                        and after is None
                        and state.symbol
                        not in legacy_candle_cursor_symbols_by_account[index]
                    ):
                        legacy_position_count = sum(
                            1
                            for position in open_positions_by_account[index].values()
                            if (
                                position.status is PaperPositionStatus.OPEN
                                and position.symbol == state.symbol
                                and position.last_candle_end is None
                            )
                        )
                        if legacy_position_count:
                            log.warning(
                                "paper_legacy_candle_cursor_fallback",
                                symbol=state.symbol,
                                account_index=index,
                                position_count=legacy_position_count,
                                lookback_seconds=(
                                    _LEGACY_CANDLE_CURSOR_LOOKBACK.total_seconds()
                                ),
                            )
                            legacy_candle_cursor_symbols_by_account[index].add(
                                state.symbol
                            )
                    try:
                        closed_candles = _load_closed_candles_for_positions(
                            positions=tuple(
                                open_positions_by_account[index].values()
                            ),
                            state=state,
                            source=candle_source,
                            not_before=identity.created_at,
                            after=after,
                        )
                    except ClosedCandleSourceError as error:
                        log.warning(
                            "closed_candle_source_unavailable",
                            symbol=state.symbol,
                            account_index=index,
                            error=str(error),
                        )
                        candle_retry_after_by_account[index][
                            state.symbol
                        ] = now + timedelta(
                            seconds=_CANDLE_SOURCE_RETRY_SECONDS
                        )
                    else:
                        candle_retry_after_by_account[index].pop(
                            state.symbol, None
                        )
            position_updates_by_id: dict[str, PaperPosition] = {}
            candle_events: tuple[ClosedCandle15m | None, ...] = (
                closed_candles if closed_candles else (None,)
            )
            for closed_candle in candle_events:
                candle_history: deque[ClosedCandle15m] | None = None
                if closed_candle is not None:
                    last_candle_end_by_account[index][state.symbol] = (
                        closed_candle.candle_end
                    )
                    candle_history = candle_history_by_account[index].setdefault(
                        state.symbol,
                        deque(
                            maxlen=max(
                                2,
                                config.portfolio.candle_confirmation_count,
                            )
                        ),
                    )
                    if (
                        not candle_history
                        or candle_history[-1].candle_start
                        != closed_candle.candle_start
                    ):
                        candle_history.append(closed_candle)
                candle_history = candle_history_by_account[index].get(state.symbol)
                position_updates = mark_positions(
                    positions=tuple(
                        open_positions_by_account[index].values()
                    ),
                    state=state,
                    config=config.portfolio,
                    taker_fee_rate=config.execution.taker_fee_rate,
                    closed_candle=closed_candle,
                    closed_candles=(
                        () if candle_history is None else tuple(candle_history)
                    ),
                )
                for position in position_updates:
                    position_updates_by_id[position.position_id] = position
                    if position.status is PaperPositionStatus.CLOSED:
                        open_positions_by_account[index].pop(
                            position.position_id,
                            None,
                        )
                    else:
                        open_positions_by_account[index][position.position_id] = (
                            position
                        )
            position_updates_by_account.append(tuple(position_updates_by_id.values()))

        decision = _strategy_decision_without_shared_cooldown(strategy, state)
        cooldown_buckets = _strategy_cooldown_buckets(strategy)
        entry_filter_context = (
            None
            if entry_filter_context_loader is None
            else entry_filter_context_loader(state)
        )
        last_processed_at_by_symbol[state.symbol] = state.bucket_start
        for index, account in enumerate(accounts):
            account_decision = _decision_for_account(
                decision,
                account.config.run_identity,
                account.config.entry_filter,
                context=entry_filter_context,
                state=state,
                cooldown_remaining=cooldown_remaining_by_account[index],
            )
            if entry_allowed and (
                account_decision.signals or account_decision.candidates
            ):
                if cooldown_buckets > 0:
                    cooldown_remaining_by_account[index][state.symbol] = (
                        cooldown_buckets
                    )
                else:
                    cooldown_remaining_by_account[index].pop(state.symbol, None)
                _run_async(
                    account.artifact_repository.save_decision(account_decision)
                )
                pending_by_account[index].extend(account_decision.candidates)

        # Resolve entries after the strategy decision so zero-latency paper
        # execution can use the current closed state's end-of-bucket quote.
        # The state is not eligible at bucket_start: its values are only
        # available once bucket_end has been reached.
        for index, account in enumerate(accounts):
            config = account.config
            pending_by_account[index], fills = _resolve_pending_candidates(
                pending_candidates=tuple(pending_by_account[index]),
                state=state,
                execution=config.execution,
            )
            if fills:
                opened_positions = _run_async(
                    account.artifact_repository.save_fills(
                        config.run_id,
                        tuple(fills),
                    )
                )
                open_positions_by_account[index].update(
                    {
                        position.position_id: position
                        for position in opened_positions
                        if position.status is PaperPositionStatus.OPEN
                    }
                )
                for position in opened_positions:
                    if position.status is PaperPositionStatus.OPEN:
                        last_position_persisted_at_by_account[index][
                            position.position_id
                        ] = position.updated_at
            last_snapshot_at = last_equity_snapshot_at[index]
            should_snapshot = (
                last_snapshot_at is None
                or state.bucket_end - last_snapshot_at >= timedelta(minutes=1)
            )
            persisted_position_updates = _persistable_position_updates(
                position_updates_by_account[index],
                last_position_persisted_at_by_account[index],
                state.bucket_end,
            )
            if persisted_position_updates or fills or should_snapshot:
                _run_async(
                    account.artifact_repository.save_portfolio(
                        config.run_id,
                        persisted_position_updates,
                        state.bucket_end,
                        config.portfolio,
                    )
                )
                for position in persisted_position_updates:
                    if position.status is PaperPositionStatus.CLOSED:
                        last_position_persisted_at_by_account[index].pop(
                            position.position_id, None
                        )
                    else:
                        last_position_persisted_at_by_account[index][
                            position.position_id
                        ] = position.updated_at
                if should_snapshot:
                    last_equity_snapshot_at[index] = state.bucket_end

        checkpoint_dirty = True
        processed += 1
        processed_since_checkpoint += 1
        final_cursor = state.bucket_start
        checkpoint_due = (
            now >= checkpoint_not_before
            and (
                processed_since_checkpoint
                >= first_config.checkpoint_every_states
                or (now - last_checkpoint_elapsed_anchor).total_seconds()
                >= first_config.checkpoint_every_seconds
            )
        )
        if checkpoint_due:
            checkpoint_to_save = _checkpoint_for_persistence(strategy)
            _save_paired_checkpoints(
                accounts=accounts,
                checkpoint=checkpoint_to_save,
                saved_at=now,
                cooldown_remaining_by_account=tuple(
                    cooldown_remaining_by_account
                ),
            )
            _notify_checkpoint_persisted(on_checkpoint_persisted)
            checkpoint_dirty = False
            processed_since_checkpoint = 0
            last_checkpoint_saved_at = now
            last_checkpoint_elapsed_anchor = now
            checkpoint_not_before = now

    if checkpoint_dirty:
        saved_at = clock.now()
        checkpoint_to_save = _checkpoint_for_persistence(strategy)
        _save_paired_checkpoints(
            accounts=accounts,
            checkpoint=checkpoint_to_save,
            saved_at=saved_at,
            cooldown_remaining_by_account=tuple(cooldown_remaining_by_account),
        )
        _notify_checkpoint_persisted(on_checkpoint_persisted)
        last_checkpoint_saved_at = saved_at

    return _paired_result(
        accounts=accounts,
        processed=processed,
        halt_reason=None,
        final_cursor=final_cursor,
        saved_at=last_checkpoint_saved_at,
    )


def _load_paired_checkpoints(
    accounts: tuple[PairedPaperLiveAccount, ...],
) -> tuple[StrategyCheckpoint | None, ...]:
    """Load paired cursors in one query when the repository supports it."""
    grouped: dict[int, tuple[PaperLiveDaemonRepository, list[str]]] = {}
    for account in accounts:
        key = id(account.repository)
        repository, run_ids = grouped.setdefault(
            key,
            (account.repository, []),
        )
        run_ids.append(account.config.run_id)

    checkpoints_by_run_id: dict[str, StrategyCheckpoint] = {}
    for repository, run_ids in grouped.values():
        load_checkpoints = getattr(repository, "load_checkpoints", None)
        if callable(load_checkpoints):
            loaded = _run_async(load_checkpoints(tuple(run_ids)))
            checkpoints_by_run_id.update(loaded)
            continue
        for run_id in run_ids:
            checkpoint = _run_async(repository.load_checkpoint(run_id))
            if checkpoint is not None:
                checkpoints_by_run_id[run_id] = checkpoint
    return tuple(
        checkpoints_by_run_id.get(account.config.run_id)
        for account in accounts
    )


def _save_paired_checkpoints(
    *,
    accounts: tuple[PairedPaperLiveAccount, ...],
    checkpoint: StrategyCheckpoint,
    saved_at: datetime,
    cooldown_remaining_by_account: tuple[dict[str, int], ...],
) -> None:
    """Write paired cursors in one transaction per repository."""
    if len(cooldown_remaining_by_account) != len(accounts):
        raise ValueError("paired cooldown state must match account count")
    account_checkpoints = tuple(
        replace(
            checkpoint,
            cooldown_buckets_remaining_by_symbol=dict(cooldown_remaining),
        )
        for cooldown_remaining in cooldown_remaining_by_account
    )
    grouped: dict[
        int,
        tuple[
            PaperLiveDaemonRepository,
            list[tuple[str, StrategyCheckpoint, datetime]],
        ],
    ] = {}
    for account, account_checkpoint in zip(
        accounts,
        account_checkpoints,
        strict=True,
    ):
        key = id(account.repository)
        repository, values = grouped.setdefault(
            key,
            (account.repository, []),
        )
        values.append((account.config.run_id, account_checkpoint, saved_at))

    for repository, values in grouped.values():
        save_checkpoints = getattr(repository, "save_checkpoints", None)
        if callable(save_checkpoints):
            _run_async(save_checkpoints(tuple(values)))
            continue
        for run_id, account_checkpoint, account_saved_at in values:
            _run_async(
                repository.save_checkpoint(
                    run_id,
                    account_checkpoint,
                    account_saved_at,
                )
            )


def _paired_result(
    *,
    accounts: tuple[PairedPaperLiveAccount, ...],
    processed: int,
    halt_reason: str | None,
    final_cursor: datetime | None,
    saved_at: datetime | None,
) -> PairedPaperLiveDaemonResult:
    return PairedPaperLiveDaemonResult(
        account_results=tuple(
            PaperLiveDaemonResult(
                processed_state_count=processed,
                halt_reason=halt_reason,
                final_cursor=final_cursor,
                final_checkpoint_saved_at=saved_at,
            )
            for _ in accounts
        )
    )


def _decision_for_account(
    decision: StrategyDecision,
    identity: StrategyRunIdentity | None,
    entry_filter: PaperEntryFilterConfig,
    *,
    context: PaperEntryFilterContext | None = None,
    state: MarketState15s | None = None,
    cooldown_remaining: dict[str, int] | None = None,
) -> StrategyDecision:
    if identity is None:
        raise ValueError("paired paper account requires run_identity")
    source_signal_ids = {signal.signal_id for signal in decision.signals}
    active_cooldown: dict[str, int] = {}
    if state is not None and cooldown_remaining is not None:
        remaining = cooldown_remaining.get(state.symbol, 0)
        if remaining > 0:
            active_cooldown[state.symbol] = remaining
            if remaining == 1:
                cooldown_remaining.pop(state.symbol, None)
            else:
                cooldown_remaining[state.symbol] = remaining - 1
    signal_ids: dict[str, str] = {}
    signals: list[StrategySignal] = []
    for signal in decision.signals:
        if signal.symbol in active_cooldown:
            continue
        if not _signal_passes_entry_filter(
            signal,
            entry_filter,
            context=context,
        ):
            continue
        signal_id = _paired_record_id(
            prefix="sig",
            run_id=identity.run_id,
            source_id=signal.signal_id,
        )
        signal_ids[signal.signal_id] = signal_id
        signals.append(replace(signal, signal_id=signal_id, run_id=identity.run_id))
    candidates: list[OrderIntentCandidate] = []
    for candidate in decision.candidates:
        mapped_signal_id = signal_ids.get(candidate.signal_id)
        if mapped_signal_id is None:
            if candidate.signal_id in source_signal_ids:
                continue
            raise ValueError("paired candidate references unknown signal")
        candidates.append(
            replace(
                candidate,
                candidate_id=_paired_record_id(
                    prefix="cand",
                    run_id=identity.run_id,
                    source_id=candidate.candidate_id,
                ),
                signal_id=mapped_signal_id,
                run_id=identity.run_id,
            )
        )
    cooldown_rejections = tuple(
        StrategyRejection(
            reason=RejectionReason.COOLDOWN_ACTIVE,
            symbol=symbol,
            bucket_start=(
                state.bucket_start
                if state is not None
                else next(
                    signal.source_state_at
                    for signal in decision.signals
                    if signal.symbol == symbol
                )
            ),
            details={"remaining": remaining},
        )
        for symbol, remaining in active_cooldown.items()
        if any(signal.symbol == symbol for signal in decision.signals)
    )
    return StrategyDecision(
        signals=tuple(signals),
        candidates=tuple(candidates),
        rejections=decision.rejections + cooldown_rejections,
        checkpoint=decision.checkpoint,
    )


def _filter_decision(
    decision: StrategyDecision,
    entry_filter: PaperEntryFilterConfig,
    *,
    entry_filter_context: PaperEntryFilterContext | None = None,
) -> StrategyDecision:
    signals = tuple(
        signal
        for signal in decision.signals
        if _signal_passes_entry_filter(
            signal,
            entry_filter,
            context=entry_filter_context,
        )
    )
    accepted_signal_ids = {signal.signal_id for signal in signals}
    candidates = tuple(
        candidate
        for candidate in decision.candidates
        if candidate.signal_id in accepted_signal_ids
    )
    return StrategyDecision(
        signals=signals,
        candidates=candidates,
        rejections=decision.rejections,
        checkpoint=decision.checkpoint,
    )


def _signal_passes_entry_filter(
    signal: StrategySignal,
    entry_filter: PaperEntryFilterConfig,
    *,
    context: PaperEntryFilterContext | None = None,
) -> bool:
    if signal.side is StrategySide.LONG and not entry_filter.allow_long:
        return False
    if signal.side is StrategySide.SHORT and not entry_filter.allow_short:
        return False
    max_imbalance = entry_filter.max_abs_aggressive_imbalance
    if max_imbalance is not None:
        imbalance = _decimal_feature(signal, "aggressive_imbalance")
        if imbalance is None or abs(imbalance) > max_imbalance:
            return False
    max_trade_count = entry_filter.max_cluster_trade_count
    if max_trade_count is not None:
        trade_count = _int_feature(signal, "cluster_trade_count")
        if trade_count is None or trade_count > max_trade_count:
            return False
    if entry_filter.require_price_above_ema5:
        entry_price = _entry_price_for_side(context, signal.side)
        if (
            context is None
            or entry_price is None
            or context.ema5 is None
            or entry_price <= context.ema5
        ):
            return False
    if entry_filter.require_price_above_ema10:
        entry_price = _entry_price_for_side(context, signal.side)
        if (
            context is None
            or entry_price is None
            or context.ema10 is None
            or entry_price <= context.ema10
        ):
            return False
    return True


def _paper_signal_gate_reasons(
    signal: StrategySignal | None,
    entry_filter: PaperEntryFilterConfig,
) -> tuple[str, ...]:
    """Explain non-EMA paper filters for the shared Policy adapter."""

    if signal is None:
        return ("signal_missing",)
    reasons: list[str] = []
    if signal.side is StrategySide.LONG and not entry_filter.allow_long:
        reasons.append("long_entries_disabled")
    if signal.side is StrategySide.SHORT and not entry_filter.allow_short:
        reasons.append("short_entries_disabled")
    max_imbalance = entry_filter.max_abs_aggressive_imbalance
    if max_imbalance is not None:
        imbalance = _decimal_feature(signal, "aggressive_imbalance")
        if imbalance is None:
            reasons.append("aggressive_imbalance_unavailable")
        elif abs(imbalance) > max_imbalance:
            reasons.append("aggressive_imbalance_exceeded")
    max_trade_count = entry_filter.max_cluster_trade_count
    if max_trade_count is not None:
        trade_count = _int_feature(signal, "cluster_trade_count")
        if trade_count is None:
            reasons.append("cluster_trade_count_unavailable")
        elif trade_count > max_trade_count:
            reasons.append("cluster_trade_count_exceeded")
    return tuple(reasons)


def _paper_ema_filter_passes(
    entry_filter: PaperEntryFilterConfig,
    context: PaperEntryFilterContext | None,
    *,
    side: StrategySide | None = None,
) -> bool:
    entry_price = _entry_price_for_side(context, side)
    if entry_filter.require_price_above_ema5 and (
        entry_price is None
        or context.ema5 is None
        or entry_price <= context.ema5
    ):
        return False
    if entry_filter.require_price_above_ema10 and (
        entry_price is None
        or context.ema10 is None
        or entry_price <= context.ema10
    ):
        return False
    return True


def _entry_price_for_side(
    context: PaperEntryFilterContext | None,
    side: StrategySide | None,
) -> Decimal | None:
    if context is None:
        return None
    if side is StrategySide.LONG and context.long_entry_price is not None:
        return context.long_entry_price
    if side is StrategySide.SHORT and context.short_entry_price is not None:
        return context.short_entry_price
    return context.entry_price


def _paper_policy_comparisons(
    *,
    decision: StrategyDecision,
    state: MarketState15s,
    observed_at: datetime,
    entry_filter: PaperEntryFilterConfig,
    entry_filter_context: PaperEntryFilterContext | None,
    entry_symbols: frozenset[str] | None,
    entry_allowed: bool,
    universe_snapshot_provider: (
        Callable[[datetime], UniverseRankingSnapshot | None] | None
    ),
) -> tuple[EntryPolicyComparison, ...]:
    universe_snapshot: UniverseRankingSnapshot | None = None
    if universe_snapshot_provider is not None:
        try:
            universe_snapshot = universe_snapshot_provider(state.bucket_end)
        except Exception as error:
            log.warning(
                "paper_entry_policy_universe_snapshot_failed",
                symbol=state.symbol,
                error_type=type(error).__name__,
            )
    signals_by_id = {signal.signal_id: signal for signal in decision.signals}
    source_trace_id = (
        f"paper-entry:{state.symbol}:{state.bucket_start.isoformat()}"
    )
    comparisons: list[EntryPolicyComparison] = []
    for candidate in decision.candidates:
        if candidate.reduce_only:
            continue
        signal = signals_by_id.get(candidate.signal_id)
        gate_reasons = _paper_signal_gate_reasons(signal, entry_filter)
        if gate_reasons:
            legacy_reason = gate_reasons[0]
        elif not entry_allowed:
            legacy_reason = "outside_entry_symbol_pool"
        elif not _paper_ema_filter_passes(
            entry_filter,
            entry_filter_context,
            side=None if signal is None else signal.side,
        ):
            legacy_reason = "ema_filter_failed"
        else:
            legacy_reason = None
        comparisons.append(
            compare_entry_policy_request(
                EntryPolicyComparisonRequest(
                    candidate=candidate,
                    source_trace_id=source_trace_id,
                    legacy_rejection_reason=legacy_reason,
                    gate_reasons=gate_reasons,
                    entry_enabled=True,
                    entry_long_only=not entry_filter.allow_short,
                    entry_symbols=entry_symbols,
                    universe_snapshot=universe_snapshot,
                    entry_price=(
                        None
                        if entry_filter_context is None
                        else entry_filter_context.entry_price
                    ),
                    ema5=(
                        None
                        if entry_filter_context is None
                        else entry_filter_context.ema5
                    ),
                    ema10=(
                        None
                        if entry_filter_context is None
                        else entry_filter_context.ema10
                    ),
                    require_price_above_ema5=(
                        entry_filter.require_price_above_ema5
                    ),
                    require_price_above_ema10=(
                        entry_filter.require_price_above_ema10
                    ),
                    observed_at=observed_at,
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
    return tuple(comparisons)


def _observe_paper_policy_comparisons(
    *,
    observer: PaperEntryPolicyComparisonObserver | None,
    state: MarketState15s,
    comparisons: tuple[EntryPolicyComparison, ...],
) -> None:
    if observer is not None:
        try:
            observer(state, comparisons)
        except Exception as error:
            log.warning(
                "paper_entry_policy_comparison_observer_failed",
                symbol=state.symbol,
                error_type=type(error).__name__,
            )
        return
    mismatch_count = sum(not comparison.matched for comparison in comparisons)
    comparison_summary = summarize_entry_policy_comparisons(comparisons)
    log.info(
        "paper_entry_policy_compared",
        symbol=state.symbol,
        source_trace_id=(
            comparisons[0].source_trace_id if comparisons else None
        ),
        candidate_count=len(comparisons),
        mismatch_count=mismatch_count,
        comparison_summary=comparison_summary.as_details(),
        comparisons=[comparison.as_details() for comparison in comparisons],
    )


def _decimal_feature(
    signal: StrategySignal,
    field_name: str,
) -> Decimal | None:
    value = signal.features.get(field_name)
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _int_feature(signal: StrategySignal, field_name: str) -> int | None:
    value = signal.features.get(field_name)
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if parsed != parsed.to_integral_value():
        return None
    return int(parsed)


def _paired_record_id(*, prefix: str, run_id: str, source_id: str) -> str:
    return f"{prefix}_{uuid5(NAMESPACE_URL, f'paper-pair:{run_id}:{source_id}')}"


def _checkpoint_progress(checkpoint: StrategyCheckpoint) -> float:
    return max(
        (
            processed_at.timestamp()
            for processed_at in checkpoint.last_processed_at_by_symbol.values()
        ),
        default=float("-inf"),
    )


def _load_closed_candles_for_positions(
    *,
    positions: tuple[PaperPosition, ...],
    state: MarketState15s,
    source: ClosedCandle15mSource | None,
    not_before: datetime,
    after: datetime | None,
) -> tuple[ClosedCandle15m, ...]:
    """Load candles without inventing history for legacy positions.

    Positions created before ``last_candle_end`` was persisted have no known
    replay boundary. Those positions intentionally inspect only the latest
    complete 15-minute window; the first successfully processed candle then
    becomes their durable cursor through ``mark_positions``.
    """
    if source is None:
        return ()
    candle_end = _candle_start_15m(state.bucket_start)
    if candle_end <= not_before or (
        after is not None and candle_end <= after
    ):
        return ()
    matching = tuple(
        position
        for position in positions
        if position.status is PaperPositionStatus.OPEN
        and position.symbol == state.symbol
        and position.opened_at < candle_end
    )
    if not matching:
        return ()
    candle_start = (
        after
        if after is not None
        else candle_end - _LEGACY_CANDLE_CURSOR_LOOKBACK
    )
    candle_start = max(candle_start, _candle_start_15m(not_before))
    candles = source.load_closed_candles(
        symbol=state.symbol,
        start=candle_start,
        end=candle_end,
    )
    return tuple(
        candle
        for candle in candles
        if candle.candle_end <= candle_end
        and (after is None or candle.candle_end > after)
    )


def _initial_candle_cursors(
    positions: tuple[PaperPosition, ...],
) -> dict[str, datetime]:
    """Recover a symbol cursor only when every open position has one."""

    positions_by_symbol: dict[str, list[PaperPosition]] = {}
    for position in positions:
        if position.status is PaperPositionStatus.OPEN:
            positions_by_symbol.setdefault(position.symbol, []).append(position)
    cursors: dict[str, datetime] = {}
    for symbol, symbol_positions in positions_by_symbol.items():
        values = [
            position.last_candle_end
            for position in symbol_positions
            if position.last_candle_end is not None
        ]
        if len(values) == len(symbol_positions):
            cursors[symbol] = min(values)
    return cursors


def _candle_start_15m(value: datetime) -> datetime:
    utc_value = value.astimezone(UTC)
    return utc_value.replace(
        minute=utc_value.minute - utc_value.minute % 15,
        second=0,
        microsecond=0,
    )


def run_paper_live_daemon(
    *,
    source: Iterable[MarketState15s],
    strategy: RuntimeStrategy,
    repository: PaperLiveDaemonRepository,
    artifact_repository: PaperLiveArtifactRepository | None = None,
    config: PaperLiveDaemonConfig,
    clock: Clock,
    entry_symbol_loader: Callable[[datetime], frozenset[str]] | None = None,
    candle_source: ClosedCandle15mSource | None = None,
    entry_filter_context_loader: (
        Callable[[MarketState15s], PaperEntryFilterContext | None] | None
    ) = None,
    entry_universe_snapshot_provider: (
        Callable[[datetime], UniverseRankingSnapshot | None] | None
    ) = None,
    entry_policy_comparison_observer: (
        PaperEntryPolicyComparisonObserver | None
    ) = None,
    on_checkpoint_persisted: Callable[[], None] | None = None,
) -> PaperLiveDaemonResult:
    checkpoint = _run_async(repository.load_checkpoint(config.run_id))
    if checkpoint is not None:
        strategy.restore_checkpoint(checkpoint)
        if _checkpoint_needs_market_recovery(checkpoint):
            _restore_paper_strategy_from_checkpoint(
                strategy=strategy,
                source=source,
                checkpoint=checkpoint,
            )
    pending_candidates: list[OrderIntentCandidate] = []
    open_positions: dict[str, PaperPosition] = {}
    last_position_persisted_at: dict[str, datetime] = {}
    if artifact_repository is not None:
        if config.run_identity is None:
            raise ValueError("run_identity is required for paper artifacts")
        _run_async(
            artifact_repository.initialize_run(
                config.run_identity,
                config.source_description,
                config.execution,
                config.portfolio,
                config.entry_filter,
            )
        )
        pending_candidates.extend(
            _run_async(
                artifact_repository.load_pending_candidates(config.run_id)
            )
        )
        loaded_open_positions = _run_async(
            artifact_repository.load_open_positions(config.run_id)
        )
        open_positions.update(
            {position.position_id: position for position in loaded_open_positions}
        )
        last_position_persisted_at.update(
            {
                position.position_id: position.updated_at
                for position in loaded_open_positions
            }
        )
        initial_candle_cursors = _initial_candle_cursors(
            loaded_open_positions
        )
    else:
        initial_candle_cursors = {}

    processed = 0
    processed_since_checkpoint = 0
    final_cursor: datetime | None = None
    checkpoint_dirty = False
    last_checkpoint_saved_at: datetime | None = None
    daemon_started_at = clock.now()
    last_checkpoint_elapsed_anchor = daemon_started_at
    checkpoint_not_before = daemon_started_at + timedelta(
        seconds=config.checkpoint_phase_seconds
    )
    last_equity_snapshot_at: datetime | None = None
    last_candle_end_by_symbol: dict[str, datetime] = initial_candle_cursors
    legacy_candle_cursor_symbols: set[str] = set()
    candle_retry_after_by_symbol: dict[str, datetime] = {}
    candle_history_by_symbol: dict[str, deque[ClosedCandle15m]] = {}
    entry_symbols: frozenset[str] | None = None
    entry_symbols_loaded_at: datetime | None = None
    gapped_symbols: set[str] = set()
    stale_symbols: set[str] = set()
    last_processed_at_by_symbol = (
        {}
        if checkpoint is None
        else dict(checkpoint.last_processed_at_by_symbol)
    )
    max_gap_seconds = _strategy_max_gap_seconds(strategy)
    candle_not_before = (
        daemon_started_at
        if config.run_identity is None
        else config.run_identity.created_at
    )
    candle_aggregator = (
        Candle15mAggregator()
        if (
            config.portfolio.exit_mode is PaperExitMode.CANDLE_15M
            and candle_source is None
        )
        else None
    )

    for state in source:
        if state.environment != config.environment:
            raise ValueError("runtime state environment mismatch")
        if _already_processed(state, checkpoint):
            continue

        now = clock.now()
        if (
            _state_age_seconds(now, state) > config.max_market_state_age_seconds
        ):
            if state.symbol not in stale_symbols:
                _log_stale_market_state(
                    state=state,
                    now=now,
                    max_age_seconds=config.max_market_state_age_seconds,
                    open_position_count=len(open_positions),
                )
                stale_symbols.add(state.symbol)
            if state.symbol not in gapped_symbols:
                _reset_strategy_symbol(strategy, state.symbol)
                gapped_symbols.add(state.symbol)
            continue

        if state.symbol in stale_symbols:
            _log_stale_market_state_recovered(
                state=state,
                now=now,
                open_position_count=len(open_positions),
            )
            stale_symbols.discard(state.symbol)

        if state.symbol not in gapped_symbols:
            _reset_strategy_for_gap(
                strategy=strategy,
                symbol=state.symbol,
                current_at=state.bucket_start,
                last_processed_at=last_processed_at_by_symbol.get(state.symbol),
                max_gap_seconds=max_gap_seconds,
            )
        gapped_symbols.discard(state.symbol)

        if entry_symbol_loader is not None and (
            entry_symbols_loaded_at is None
            or (
                state.bucket_start - entry_symbols_loaded_at
            ).total_seconds()
            >= config.entry_symbol_refresh_seconds
        ):
            entry_symbols = entry_symbol_loader(state.bucket_start)
            entry_symbols_loaded_at = state.bucket_start
        entry_allowed = (
            entry_symbols is None or state.symbol in entry_symbols
        )

        position_updates: tuple[PaperPosition, ...] = ()
        if artifact_repository is not None:
            observed_candle = (
                None
                if candle_aggregator is None
                else candle_aggregator.observe(state)
            )
            if candle_aggregator is not None:
                _log_candle_gap_events(aggregator=candle_aggregator)
            closed_candles: tuple[ClosedCandle15m, ...] = (
                () if observed_candle is None else (observed_candle,)
            )
            if (
                not closed_candles
                and config.portfolio.exit_mode is PaperExitMode.CANDLE_15M
            ):
                retry_after = candle_retry_after_by_symbol.get(state.symbol)
                if retry_after is None or now >= retry_after:
                    after = last_candle_end_by_symbol.get(state.symbol)
                    if (
                        candle_source is not None
                        and after is None
                        and state.symbol not in legacy_candle_cursor_symbols
                    ):
                        legacy_position_count = sum(
                            1
                            for position in open_positions.values()
                            if (
                                position.status is PaperPositionStatus.OPEN
                                and position.symbol == state.symbol
                                and position.last_candle_end is None
                            )
                        )
                        if legacy_position_count:
                            log.warning(
                                "paper_legacy_candle_cursor_fallback",
                                symbol=state.symbol,
                                position_count=legacy_position_count,
                                lookback_seconds=(
                                    _LEGACY_CANDLE_CURSOR_LOOKBACK.total_seconds()
                                ),
                            )
                            legacy_candle_cursor_symbols.add(state.symbol)
                    try:
                        closed_candles = _load_closed_candles_for_positions(
                            positions=tuple(open_positions.values()),
                            state=state,
                            source=candle_source,
                            not_before=candle_not_before,
                            after=after,
                        )
                    except ClosedCandleSourceError as error:
                        log.warning(
                            "closed_candle_source_unavailable",
                            symbol=state.symbol,
                            error=str(error),
                        )
                        candle_retry_after_by_symbol[state.symbol] = (
                            now + timedelta(seconds=_CANDLE_SOURCE_RETRY_SECONDS)
                        )
                    else:
                        candle_retry_after_by_symbol.pop(state.symbol, None)
            position_updates_by_id: dict[str, PaperPosition] = {}
            candle_events: tuple[ClosedCandle15m | None, ...] = (
                closed_candles if closed_candles else (None,)
            )
            for closed_candle in candle_events:
                candle_history: deque[ClosedCandle15m] | None = None
                if closed_candle is not None:
                    last_candle_end_by_symbol[state.symbol] = (
                        closed_candle.candle_end
                    )
                    candle_history = candle_history_by_symbol.setdefault(
                        state.symbol,
                        deque(
                            maxlen=max(
                                2,
                                config.portfolio.candle_confirmation_count,
                            )
                        ),
                    )
                    if (
                        not candle_history
                        or candle_history[-1].candle_start
                        != closed_candle.candle_start
                    ):
                        candle_history.append(closed_candle)
                candle_history = candle_history_by_symbol.get(state.symbol)
                position_updates = mark_positions(
                    positions=tuple(open_positions.values()),
                    state=state,
                    config=config.portfolio,
                    taker_fee_rate=config.execution.taker_fee_rate,
                    closed_candle=closed_candle,
                    closed_candles=(
                        () if candle_history is None else tuple(candle_history)
                    ),
                )
                for position in position_updates:
                    position_updates_by_id[position.position_id] = position
                    if position.status is PaperPositionStatus.CLOSED:
                        open_positions.pop(position.position_id, None)
                    else:
                        open_positions[position.position_id] = position
            position_updates = tuple(position_updates_by_id.values())

        raw_decision = strategy.on_market_state(state)
        entry_filter_context = None
        if raw_decision.signals and (
            config.entry_filter.require_price_above_ema5
            or config.entry_filter.require_price_above_ema10
        ):
            if entry_filter_context_loader is not None:
                entry_filter_context = entry_filter_context_loader(state)
        if config.entry_policy_compare_only:
            comparisons = _paper_policy_comparisons(
                decision=raw_decision,
                state=state,
                observed_at=now,
                entry_filter=config.entry_filter,
                entry_filter_context=entry_filter_context,
                entry_symbols=entry_symbols,
                entry_allowed=entry_allowed,
                universe_snapshot_provider=entry_universe_snapshot_provider,
            )
            _observe_paper_policy_comparisons(
                observer=entry_policy_comparison_observer,
                state=state,
                comparisons=comparisons,
            )
        decision = _filter_decision(
            raw_decision,
            config.entry_filter,
            entry_filter_context=entry_filter_context,
        )
        last_processed_at_by_symbol[state.symbol] = state.bucket_start
        if artifact_repository is not None and entry_allowed and (
            decision.signals or decision.candidates
        ):
            _run_async(artifact_repository.save_decision(decision))
            pending_candidates.extend(decision.candidates)
        if artifact_repository is not None:
            pending_candidates, fills = _resolve_pending_candidates(
                pending_candidates=tuple(pending_candidates),
                state=state,
                execution=config.execution,
            )
            if fills:
                opened_positions = _run_async(
                    artifact_repository.save_fills(config.run_id, tuple(fills))
                )
                open_positions.update(
                    {
                        position.position_id: position
                        for position in opened_positions
                        if position.status is PaperPositionStatus.OPEN
                    }
                )
                for position in opened_positions:
                    if position.status is PaperPositionStatus.OPEN:
                        last_position_persisted_at[position.position_id] = (
                            position.updated_at
                        )
            should_snapshot = (
                last_equity_snapshot_at is None
                or state.bucket_end - last_equity_snapshot_at
                >= timedelta(minutes=1)
            )
            persisted_position_updates = _persistable_position_updates(
                position_updates,
                last_position_persisted_at,
                state.bucket_end,
            )
            if persisted_position_updates or fills or should_snapshot:
                _run_async(
                    artifact_repository.save_portfolio(
                        config.run_id,
                        persisted_position_updates,
                        state.bucket_end,
                        config.portfolio,
                    )
                )
                for position in persisted_position_updates:
                    if position.status is PaperPositionStatus.CLOSED:
                        last_position_persisted_at.pop(position.position_id, None)
                    else:
                        last_position_persisted_at[position.position_id] = (
                            position.updated_at
                        )
                if should_snapshot:
                    last_equity_snapshot_at = state.bucket_end
        checkpoint_dirty = True
        processed += 1
        processed_since_checkpoint += 1
        final_cursor = state.bucket_start

        should_checkpoint_by_count = (
            processed_since_checkpoint >= config.checkpoint_every_states
        )
        should_checkpoint_by_time = (
            now - last_checkpoint_elapsed_anchor
        ).total_seconds() >= config.checkpoint_every_seconds
        if (
            now >= checkpoint_not_before
            and (should_checkpoint_by_count or should_checkpoint_by_time)
        ):
            checkpoint_to_save = _checkpoint_for_persistence(strategy)
            _run_async(
                repository.save_checkpoint(
                    config.run_id,
                    checkpoint_to_save,
                    now,
                )
            )
            _notify_checkpoint_persisted(on_checkpoint_persisted)
            last_checkpoint_saved_at = now
            checkpoint_dirty = False
            processed_since_checkpoint = 0
            last_checkpoint_elapsed_anchor = now
            checkpoint_not_before = now

    if checkpoint_dirty:
        saved_at = clock.now()
        checkpoint_to_save = _checkpoint_for_persistence(strategy)
        _run_async(
            repository.save_checkpoint(
                config.run_id,
                checkpoint_to_save,
                saved_at,
            )
        )
        _notify_checkpoint_persisted(on_checkpoint_persisted)
        last_checkpoint_saved_at = saved_at

    return PaperLiveDaemonResult(
        processed_state_count=processed,
        halt_reason=None,
        final_cursor=final_cursor,
        final_checkpoint_saved_at=last_checkpoint_saved_at,
    )


def _resolve_pending_candidates(
    *,
    pending_candidates: tuple[OrderIntentCandidate, ...],
    state: MarketState15s,
    execution: ReplayExecutionConfig,
) -> tuple[list[OrderIntentCandidate], list[SimulatedFill]]:
    remaining: list[OrderIntentCandidate] = []
    fills: list[SimulatedFill] = []
    for candidate in pending_candidates:
        if candidate.symbol != state.symbol:
            remaining.append(candidate)
            continue
        fill = resolve_candidate_fill_at_state(
            candidate=candidate,
            state=state,
            execution=execution,
        )
        if fill is None:
            remaining.append(candidate)
        else:
            fills.append(fill)
    return remaining, fills


def _persistable_position_updates(
    position_updates: tuple[PaperPosition, ...],
    last_persisted_at: dict[str, datetime],
    observed_at: datetime,
) -> tuple[PaperPosition, ...]:
    """Throttle open-position marks while keeping exits durable immediately."""
    return tuple(
        position
        for position in position_updates
        if position.status is PaperPositionStatus.CLOSED
        or position.position_id not in last_persisted_at
        or observed_at - last_persisted_at[position.position_id]
        >= timedelta(minutes=1)
    )


def _already_processed(
    state: MarketState15s,
    checkpoint: StrategyCheckpoint | None,
) -> bool:
    if checkpoint is None:
        return False
    processed_at = checkpoint.last_processed_at_by_symbol.get(state.symbol)
    return processed_at is not None and state.bucket_start <= processed_at


def _checkpoint_for_persistence(strategy: RuntimeStrategy) -> StrategyCheckpoint:
    """Build a compact checkpoint without breaking older strategy adapters."""
    checkpoint_method = strategy.checkpoint
    try:
        parameters = signature(checkpoint_method).parameters
    except (TypeError, ValueError):
        parameters = None

    checkpoint: StrategyCheckpoint
    if parameters is not None:
        buffer_parameter = parameters.get("include_market_state_buffers")
        if buffer_parameter is not None and buffer_parameter.kind in {
            Parameter.KEYWORD_ONLY,
            Parameter.POSITIONAL_OR_KEYWORD,
        }:
            checkpoint = checkpoint_method(  # type: ignore[call-arg]
                include_market_state_buffers=False
            )
        else:
            checkpoint = checkpoint_method()
    else:
        checkpoint = checkpoint_method()
    compact_payload = {
        key: value
        for key, value in checkpoint.payload.items()
        if key not in {"market_state_buffers", "signal_buffers"}
    }
    return replace(checkpoint, payload=compact_payload)


def _checkpoint_needs_market_recovery(checkpoint: StrategyCheckpoint) -> bool:
    """Identify compact checkpoints produced by the paper daemon."""
    return (
        not any(
            key in checkpoint.payload
            for key in ("market_state_buffers", "signal_buffers")
        )
        and any(
            key in checkpoint.payload
            for key in ("buffer_sizes", "signal_sequence")
        )
    )


def _restore_paper_strategy_from_checkpoint(
    *,
    strategy: RuntimeStrategy,
    source: Iterable[MarketState15s],
    checkpoint: StrategyCheckpoint,
) -> None:
    warm_market_state = getattr(strategy, "warm_market_state", None)
    if not callable(warm_market_state):
        raise RuntimeError(
            "strategy does not support compact paper checkpoint recovery"
        )
    load_recovery_window = getattr(source, "load_recovery_window", None)
    if not callable(load_recovery_window):
        raise RuntimeError(
            "paper market-state source does not support compact checkpoint "
            "recovery"
        )
    states = load_recovery_window(
        last_processed_at_by_symbol=checkpoint.last_processed_at_by_symbol,
        lookback_seconds=_strategy_recovery_lookback_seconds(strategy),
        limit=_PAPER_RECOVERY_STATE_LIMIT,
    )
    for state in states:
        warm_market_state(state)


def _strategy_recovery_lookback_seconds(strategy: RuntimeStrategy) -> int:
    required_data = strategy.required_data()
    base_interval_seconds = max(
        1,
        int(required_data.base_state_interval_seconds),
    )
    warmup_buckets = max(0, int(required_data.warmup_buckets))
    return max(
        base_interval_seconds,
        (warmup_buckets + 16) * base_interval_seconds,
    )


def _state_age_seconds(now: datetime, state: MarketState15s) -> float:
    _require_aware(now, "now")
    _require_aware(state.bucket_end, "bucket_end")
    return (now - state.bucket_end).total_seconds()


def _log_stale_market_state(
    *,
    state: MarketState15s,
    now: datetime,
    max_age_seconds: float,
    open_position_count: int,
) -> None:
    """Record why stale states defer paper exits and strategy processing."""

    log.warning(
        "paper_market_state_stale",
        symbol=state.symbol,
        state_bucket_end=state.bucket_end.isoformat(),
        observed_at=now.isoformat(),
        age_seconds=_state_age_seconds(now, state),
        max_market_state_age_seconds=max_age_seconds,
        open_position_count=open_position_count,
        exit_action="defer_until_fresh_market_state",
    )


def _log_stale_market_state_recovered(
    *,
    state: MarketState15s,
    now: datetime,
    open_position_count: int,
) -> None:
    log.info(
        "paper_market_state_recovered",
        symbol=state.symbol,
        state_bucket_end=state.bucket_end.isoformat(),
        observed_at=now.isoformat(),
        open_position_count=open_position_count,
        exit_action="resume_on_fresh_market_state",
    )


def _reset_strategy_symbol(strategy: RuntimeStrategy, symbol: str) -> None:
    reset = getattr(strategy, "reset_symbol", None)
    if callable(reset):
        reset(symbol)


def _reset_strategy_for_gap(
    *,
    strategy: RuntimeStrategy,
    symbol: str,
    current_at: datetime,
    last_processed_at: datetime | None,
    max_gap_seconds: int,
) -> bool:
    if last_processed_at is None:
        return False
    if (current_at - last_processed_at).total_seconds() > max_gap_seconds:
        _reset_strategy_symbol(strategy, symbol)
        return True
    return False


def _strategy_max_gap_seconds(strategy: RuntimeStrategy) -> int:
    return strategy.required_data().max_gap_seconds


def _strategy_decision_without_shared_cooldown(
    strategy: RuntimeStrategy,
    state: MarketState15s,
) -> StrategyDecision:
    method = getattr(strategy, "on_market_state_without_cooldown", None)
    if callable(method):
        return method(state)
    return strategy.on_market_state(state)


def _strategy_cooldown_buckets(strategy: RuntimeStrategy) -> int:
    method = getattr(strategy, "cooldown_buckets", None)
    if not callable(method):
        return 0
    value = method()
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return 0
    return value


def _log_candle_gap_events(
    *,
    aggregator: Candle15mAggregator,
    account_index: int | None = None,
) -> None:
    for gap in aggregator.drain_gap_events():
        log.warning(
            "paper_closed_candle_gap_detected",
            symbol=gap.symbol,
            account_index=account_index,
            previous_candle_start=gap.previous_candle_start,
            observed_candle_start=gap.observed_candle_start,
            dropped_minute_count=gap.dropped_minute_count,
            missing_candle_count=gap.missing_candle_count,
            cumulative_gap_count=aggregator.gap_count,
        )


def _run_async[T](awaitable: Coroutine[object, object, T]) -> T:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(awaitable)
    raise RuntimeError("run_paper_live_daemon cannot run inside an active event loop")


def _notify_checkpoint_persisted(
    callback: Callable[[], None] | None,
) -> None:
    if callback is None:
        return
    try:
        callback()
    except Exception:
        # A local readiness marker must never interrupt paper execution.
        log.exception("paper_health_marker_failed")


def _require_non_empty(value: str, field_name: str) -> None:
    if not value.strip():
        raise ValueError(f"{field_name} must not be empty")


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
