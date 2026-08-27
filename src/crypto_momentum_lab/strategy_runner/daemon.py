import asyncio
from collections import deque
from collections.abc import Callable, Coroutine, Iterable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Protocol
from uuid import NAMESPACE_URL, uuid5

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import (
    OrderIntentCandidate,
    RunMode,
    StrategyCheckpoint,
    StrategyDataRequirement,
    StrategyDecision,
    StrategyRunIdentity,
    StrategySide,
    StrategySignal,
)
from crypto_momentum_lab.strategy_runner.candle_source import (
    ClosedCandle15mSource,
)
from crypto_momentum_lab.strategy_runner.fills import (
    ReplayExecutionConfig,
    SimulatedFill,
    candidate_target_fill_at,
    simulate_candidate_fill,
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


class Clock(Protocol):
    def now(self) -> datetime:
        pass


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
    ema5: Decimal | None = None
    ema10: Decimal | None = None


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
        default_factory=lambda: ReplayExecutionConfig(latency_buckets=0)
    )
    portfolio: PaperExitConfig = field(default_factory=PaperExitConfig)
    entry_filter: PaperEntryFilterConfig = field(
        default_factory=PaperEntryFilterConfig
    )

    def __post_init__(self) -> None:
        _require_non_empty(self.run_id, "run_id")
        _require_non_empty(self.strategy_name, "strategy_name")
        _require_non_empty(self.environment, "environment")
        if self.checkpoint_every_states <= 0:
            raise ValueError("checkpoint_every_states must be positive")
        if self.checkpoint_every_seconds <= 0:
            raise ValueError("checkpoint_every_seconds must be positive")
        if self.max_market_state_age_seconds <= 0:
            raise ValueError("max_market_state_age_seconds must be positive")
        if self.entry_symbol_refresh_seconds <= 0:
            raise ValueError("entry_symbol_refresh_seconds must be positive")
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

    checkpoints = tuple(
        _run_async(
            account.repository.load_checkpoint(account.config.run_id)
        )
        for account in accounts
    )
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

    pending_by_account: list[list[OrderIntentCandidate]] = []
    open_positions_by_account: list[dict[str, PaperPosition]] = []
    last_position_persisted_at_by_account: list[dict[str, datetime]] = []
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
    last_equity_snapshot_at: list[datetime | None] = [None] * len(accounts)
    last_candle_end_by_account: list[dict[str, datetime]] = [
        {} for _ in accounts
    ]
    entry_symbols: frozenset[str] | None = None
    entry_symbols_loaded_at: datetime | None = None
    gapped_symbols: set[str] = set()
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
            if state.symbol not in gapped_symbols:
                _reset_strategy_symbol(strategy, state.symbol)
                gapped_symbols.add(state.symbol)
            continue

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
            closed_candle = (
                None if aggregator is None else aggregator.observe(state)
            )
            if (
                closed_candle is None
                and config.portfolio.exit_mode is PaperExitMode.CANDLE_15M
            ):
                closed_candle = _load_latest_closed_candle_for_positions(
                    positions=tuple(open_positions_by_account[index].values()),
                    state=state,
                    source=candle_source,
                    not_before=identity.created_at,
                    after=last_candle_end_by_account[index].get(state.symbol),
                )
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
                positions=tuple(open_positions_by_account[index].values()),
                state=state,
                config=config.portfolio,
                taker_fee_rate=config.execution.taker_fee_rate,
                closed_candle=closed_candle,
                closed_candles=(
                    () if candle_history is None else tuple(candle_history)
                ),
            )
            for position in position_updates:
                if position.status is PaperPositionStatus.CLOSED:
                    open_positions_by_account[index].pop(position.position_id, None)
                else:
                    open_positions_by_account[index][position.position_id] = position
            position_updates_by_account.append(position_updates)

        decision = strategy.on_market_state(state)
        last_processed_at_by_symbol[state.symbol] = state.bucket_start
        for index, account in enumerate(accounts):
            account_decision = _decision_for_account(
                decision,
                account.config.run_identity,
                account.config.entry_filter,
            )
            if entry_allowed and (
                account_decision.signals or account_decision.candidates
            ):
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
            processed_since_checkpoint
            >= first_config.checkpoint_every_states
            or (now - last_checkpoint_elapsed_anchor).total_seconds()
            >= first_config.checkpoint_every_seconds
        )
        if checkpoint_due:
            checkpoint_to_save = strategy.checkpoint()
            for account in accounts:
                _run_async(
                    account.repository.save_checkpoint(
                        account.config.run_id,
                        checkpoint_to_save,
                        now,
                    )
                )
            checkpoint_dirty = False
            processed_since_checkpoint = 0
            last_checkpoint_saved_at = now
            last_checkpoint_elapsed_anchor = now

    if checkpoint_dirty:
        saved_at = clock.now()
        checkpoint_to_save = strategy.checkpoint()
        for account in accounts:
            _run_async(
                account.repository.save_checkpoint(
                    account.config.run_id,
                    checkpoint_to_save,
                    saved_at,
                )
            )
        last_checkpoint_saved_at = saved_at

    return _paired_result(
        accounts=accounts,
        processed=processed,
        halt_reason=None,
        final_cursor=final_cursor,
        saved_at=last_checkpoint_saved_at,
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
) -> StrategyDecision:
    if identity is None:
        raise ValueError("paired paper account requires run_identity")
    source_signal_ids = {signal.signal_id for signal in decision.signals}
    signal_ids: dict[str, str] = {}
    signals: list[StrategySignal] = []
    for signal in decision.signals:
        if not _signal_passes_entry_filter(signal, entry_filter):
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
    return StrategyDecision(
        signals=tuple(signals),
        candidates=tuple(candidates),
        rejections=decision.rejections,
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
        if (
            context is None
            or context.entry_price is None
            or context.ema5 is None
            or context.entry_price <= context.ema5
        ):
            return False
    if entry_filter.require_price_above_ema10:
        if (
            context is None
            or context.entry_price is None
            or context.ema10 is None
            or context.entry_price <= context.ema10
        ):
            return False
    return True


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


def _load_latest_closed_candle_for_positions(
    *,
    positions: tuple[PaperPosition, ...],
    state: MarketState15s,
    source: ClosedCandle15mSource | None,
    not_before: datetime,
    after: datetime | None,
) -> ClosedCandle15m | None:
    if source is None:
        return None
    candle_end = _candle_start_15m(state.bucket_start)
    if candle_end <= not_before or (
        after is not None and candle_end <= after
    ):
        return None
    matching = tuple(
        position
        for position in positions
        if position.status is PaperPositionStatus.OPEN
        and position.symbol == state.symbol
        and position.opened_at < candle_end
    )
    if not matching:
        return None
    candle_start = candle_end - timedelta(minutes=15)
    candles = source.load_closed_candles(
        symbol=state.symbol,
        start=candle_start,
        end=candle_end,
    )
    return candles[-1] if candles else None


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
) -> PaperLiveDaemonResult:
    checkpoint = _run_async(repository.load_checkpoint(config.run_id))
    if checkpoint is not None:
        strategy.restore_checkpoint(checkpoint)
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

    processed = 0
    processed_since_checkpoint = 0
    final_cursor: datetime | None = None
    checkpoint_dirty = False
    last_checkpoint_saved_at: datetime | None = None
    daemon_started_at = clock.now()
    last_checkpoint_elapsed_anchor = daemon_started_at
    last_equity_snapshot_at: datetime | None = None
    last_candle_end_by_symbol: dict[str, datetime] = {}
    candle_history_by_symbol: dict[str, deque[ClosedCandle15m]] = {}
    entry_symbols: frozenset[str] | None = None
    entry_symbols_loaded_at: datetime | None = None
    gapped_symbols: set[str] = set()
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
            if state.symbol not in gapped_symbols:
                _reset_strategy_symbol(strategy, state.symbol)
                gapped_symbols.add(state.symbol)
            continue

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
            closed_candle = (
                None
                if candle_aggregator is None
                else candle_aggregator.observe(state)
            )
            if (
                closed_candle is None
                and config.portfolio.exit_mode is PaperExitMode.CANDLE_15M
            ):
                closed_candle = _load_latest_closed_candle_for_positions(
                    positions=tuple(open_positions.values()),
                    state=state,
                    source=candle_source,
                    not_before=candle_not_before,
                    after=last_candle_end_by_symbol.get(state.symbol),
                )
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
                if position.status is PaperPositionStatus.CLOSED:
                    open_positions.pop(position.position_id, None)
                else:
                    open_positions[position.position_id] = position

        raw_decision = strategy.on_market_state(state)
        entry_filter_context = None
        if raw_decision.signals and (
            config.entry_filter.require_price_above_ema5
            or config.entry_filter.require_price_above_ema10
        ):
            if entry_filter_context_loader is not None:
                entry_filter_context = entry_filter_context_loader(state)
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
        if should_checkpoint_by_count or should_checkpoint_by_time:
            checkpoint_to_save = strategy.checkpoint()
            _run_async(
                repository.save_checkpoint(
                    config.run_id,
                    checkpoint_to_save,
                    now,
                )
            )
            last_checkpoint_saved_at = now
            checkpoint_dirty = False
            processed_since_checkpoint = 0
            last_checkpoint_elapsed_anchor = now

    if checkpoint_dirty:
        saved_at = clock.now()
        checkpoint_to_save = strategy.checkpoint()
        _run_async(
            repository.save_checkpoint(
                config.run_id,
                checkpoint_to_save,
                saved_at,
            )
        )
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
        target_fill_at = candidate_target_fill_at(candidate, execution)
        if state.bucket_end > candidate.expires_at:
            fills.append(
                simulate_candidate_fill(
                    candidate=candidate,
                    states=(),
                    execution=execution,
                )
            )
            continue
        if state.bucket_end < target_fill_at:
            remaining.append(candidate)
            continue
        fills.append(
            simulate_candidate_fill(
                candidate=candidate,
                states=(state,),
                execution=execution,
            )
        )
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


def _state_age_seconds(now: datetime, state: MarketState15s) -> float:
    _require_aware(now, "now")
    _require_aware(state.bucket_end, "bucket_end")
    return (now - state.bucket_end).total_seconds()


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
) -> None:
    if last_processed_at is None:
        return
    if (current_at - last_processed_at).total_seconds() > max_gap_seconds:
        _reset_strategy_symbol(strategy, symbol)


def _strategy_max_gap_seconds(strategy: RuntimeStrategy) -> int:
    return strategy.required_data().max_gap_seconds


def _run_async[T](awaitable: Coroutine[object, object, T]) -> T:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(awaitable)
    raise RuntimeError("run_paper_live_daemon cannot run inside an active event loop")


def _require_non_empty(value: str, field_name: str) -> None:
    if not value.strip():
        raise ValueError(f"{field_name} must not be empty")


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
