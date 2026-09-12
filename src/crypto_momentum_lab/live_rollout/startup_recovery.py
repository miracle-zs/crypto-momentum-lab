"""Warmup and checkpoint recovery for live strategy startup."""

from collections.abc import Collection, Mapping
from datetime import UTC, datetime, timedelta
from time import perf_counter

import structlog

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import StrategyCheckpoint
from crypto_momentum_lab.live_rollout.daemon import LiveRuntimeStrategy
from crypto_momentum_lab.persistence.postgres.runtime_state_repository import (
    PostgresRuntimeMarketStateRepository,
    RuntimeStateCursor,
)

log = structlog.get_logger()

MIN_WARMUP_SECONDS = 60
WARMUP_STATE_LIMIT = 100_000
WARMUP_BATCH_SIZE = 5_000


def live_market_state_cutover(now: datetime) -> datetime:
    """Choose the last fully closed 15-second bucket for startup recovery."""

    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    epoch_seconds = int(now.timestamp()) // 15 * 15
    return datetime.fromtimestamp(epoch_seconds, tz=UTC) - timedelta(seconds=15)


def cursor_after_market_bucket(bucket_start: datetime) -> RuntimeStateCursor:
    if bucket_start.tzinfo is None or bucket_start.utcoffset() is None:
        raise ValueError("bucket_start must be timezone-aware")
    return RuntimeStateCursor(
        bucket_start=bucket_start + timedelta(microseconds=1),
        symbol="",
    )


async def load_live_warmup_symbols(
    *,
    repository: PostgresRuntimeMarketStateRepository,
    environment: str,
    observed_at: datetime,
) -> frozenset[str]:
    loader = getattr(repository, "load_symbols_at", None)
    if not callable(loader):
        return frozenset()
    symbols = await loader(
        environment=environment,
        observed_at=observed_at,
    )
    return frozenset(symbol for symbol in symbols if symbol.strip())


def strategy_last_processed_at_by_symbol(
    strategy: LiveRuntimeStrategy,
) -> Mapping[str, datetime]:
    checkpoint = strategy.checkpoint(include_market_state_buffers=False)
    return checkpoint.last_processed_at_by_symbol


def validate_live_warmup_coverage(
    *,
    strategy: LiveRuntimeStrategy,
    states: Collection[MarketState15s],
    expected_symbols: Collection[str],
    cutover_at: datetime,
) -> None:
    """Reject startup unless every target symbol has a contiguous buffer."""

    required_data = getattr(strategy, "required_data", None)
    if not callable(required_data):
        log.warning(
            "live_strategy_warmup_validation_skipped",
            reason="strategy_has_no_required_data_contract",
        )
        return
    requirement = required_data()
    warmup_buckets = int(requirement.warmup_buckets)
    interval = timedelta(
        seconds=int(getattr(requirement, "base_state_interval_seconds", 15))
    )
    required_fields = tuple(getattr(requirement, "required_fields", ()))
    states_by_symbol: dict[str, list[MarketState15s]] = {}
    for state in states:
        states_by_symbol.setdefault(state.symbol, []).append(state)

    missing: list[str] = []
    gaps: list[str] = []
    for symbol in sorted(set(expected_symbols)):
        valid_states = [
            state
            for state in states_by_symbol.get(symbol, ())
            if all(getattr(state, field, None) is not None for field in required_fields)
        ]
        valid_states.sort(
            key=lambda state: getattr(state, "bucket_start", cutover_at)
        )
        if len(valid_states) < warmup_buckets:
            missing.append(
                f"{symbol}:have={len(valid_states)},need={warmup_buckets}"
            )
            continue
        window = valid_states[-warmup_buckets:]
        if all(hasattr(state, "bucket_start") for state in window):
            if any(
                current.bucket_start - previous.bucket_start != interval
                for previous, current in zip(window, window[1:], strict=False)
            ) or window[-1].bucket_start != cutover_at:
                gaps.append(symbol)

    if missing or gaps:
        details: list[str] = []
        if missing:
            details.append("insufficient=" + ",".join(missing[:8]))
        if gaps:
            details.append("gaps=" + ",".join(gaps[:8]))
        raise RuntimeError(
            "live strategy warmup incomplete at "
            f"{cutover_at.isoformat()}: "
            + "; ".join(details)
        )


def _symbols_with_complete_warmup(
    *,
    strategy: LiveRuntimeStrategy,
    states: Collection[MarketState15s],
    expected_symbols: Collection[str],
    cutover_at: datetime,
) -> frozenset[str]:
    """Return symbols that can safely participate immediately after startup.

    The exchange universe can contain newly listed symbols or symbols whose
    durable history has a gap.  Those symbols must remain entry-ineligible
    until their own rolling window is complete; they must not prevent mature
    symbols from starting the worker.
    """

    required_data = getattr(strategy, "required_data", None)
    if not callable(required_data):
        return frozenset(expected_symbols)
    requirement = required_data()
    warmup_buckets = int(requirement.warmup_buckets)
    interval = timedelta(
        seconds=int(getattr(requirement, "base_state_interval_seconds", 15))
    )
    required_fields = tuple(getattr(requirement, "required_fields", ()))
    states_by_symbol: dict[str, list[MarketState15s]] = {}
    for state in states:
        states_by_symbol.setdefault(state.symbol, []).append(state)

    complete: set[str] = set()
    for symbol in sorted(set(expected_symbols)):
        valid_states = [
            state
            for state in states_by_symbol.get(symbol, ())
            if all(getattr(state, field, None) is not None for field in required_fields)
        ]
        valid_states.sort(key=lambda state: state.bucket_start)
        if len(valid_states) < warmup_buckets:
            continue
        window = valid_states[-warmup_buckets:]
        if window[-1].bucket_start != cutover_at:
            continue
        if any(
            current.bucket_start - previous.bucket_start != interval
            for previous, current in zip(window, window[1:], strict=False)
        ):
            continue
        complete.add(symbol)
    return frozenset(complete)


def _recovery_state_limit(
    *,
    strategy: LiveRuntimeStrategy,
    symbol_count: int,
    lookback_seconds: int,
) -> int:
    """Keep enough rows for every symbol's complete recovery window.

    The repository applies ``limit`` after combining the per-symbol ranges.
    A fixed global limit can therefore truncate the newest rows for every
    symbol when the exchange universe grows, leaving no symbol with a complete
    window even though the database contains all required history.
    """

    required_data = getattr(strategy, "required_data", None)
    interval_seconds = 15
    if callable(required_data):
        requirement = required_data()
        interval_seconds = max(
            1,
            int(getattr(requirement, "base_state_interval_seconds", 15)),
        )
    states_per_symbol = max(1, lookback_seconds // interval_seconds + 2)
    return max(WARMUP_STATE_LIMIT, symbol_count * states_per_symbol)


async def warm_live_strategy(
    *,
    strategy: LiveRuntimeStrategy,
    repository: PostgresRuntimeMarketStateRepository,
    environment: str,
    now: datetime,
    cutover_at: datetime | None = None,
) -> RuntimeStateCursor:
    warm_market_state = getattr(strategy, "warm_market_state", None)
    if not callable(warm_market_state):
        raise RuntimeError("strategy does not support warm-only startup recovery")
    warmup_seconds = live_warmup_seconds(strategy)
    warmup_end = cutover_at or live_market_state_cutover(now)
    cursor = RuntimeStateCursor(
        bucket_start=warmup_end - timedelta(seconds=warmup_seconds),
        symbol="",
    )
    expected_symbols = await load_live_warmup_symbols(
        repository=repository,
        environment=environment,
        observed_at=warmup_end,
    )
    recovery_state_limit = _recovery_state_limit(
        strategy=strategy,
        symbol_count=len(expected_symbols),
        lookback_seconds=warmup_seconds,
    )
    warmed_states: list[MarketState15s] = []
    warmed_state_count = 0
    started_at = perf_counter()
    while warmed_state_count < recovery_state_limit:
        batch_limit = min(
            WARMUP_BATCH_SIZE,
            recovery_state_limit - warmed_state_count,
        )
        batch = await repository.load_after(
            environment=environment,
            cursor=cursor,
            limit=batch_limit,
            upper_bound=warmup_end,
        )
        if not batch:
            break
        accepted_in_batch = 0
        for state in batch:
            if state.bucket_start > warmup_end:
                continue
            warm_market_state(state)
            warmed_states.append(state)
            cursor = RuntimeStateCursor(
                bucket_start=state.bucket_start,
                symbol=state.symbol,
            )
            warmed_state_count += 1
            accepted_in_batch += 1
        if accepted_in_batch == 0:
            break
        if len(batch) < batch_limit:
            break
    if not expected_symbols:
        expected_symbols = frozenset(state.symbol for state in warmed_states)
    complete_symbols = _symbols_with_complete_warmup(
        strategy=strategy,
        states=warmed_states,
        expected_symbols=expected_symbols,
        cutover_at=warmup_end,
    )
    if expected_symbols and not complete_symbols:
        raise RuntimeError(
            "live strategy warmup incomplete: no symbol has a complete window "
            f"at {warmup_end.isoformat()}"
        )
    deferred_symbols = expected_symbols - complete_symbols
    if deferred_symbols:
        log.warning(
            "live_strategy_warmup_symbols_deferred",
            deferred_count=len(deferred_symbols),
            deferred_examples=sorted(deferred_symbols)[:8],
            cutover_at=warmup_end.isoformat(),
        )
    validate_live_warmup_coverage(
        strategy=strategy,
        states=warmed_states,
        expected_symbols=complete_symbols,
        cutover_at=warmup_end,
    )
    log.info(
        "live_strategy_warmup_completed",
        environment=environment,
        state_count=warmed_state_count,
        expected_symbol_count=len(expected_symbols),
        warmed_symbol_count=len({state.symbol for state in warmed_states}),
        cutover_at=warmup_end.isoformat(),
        elapsed_ms=round((perf_counter() - started_at) * 1000, 3),
        mode="warm_only",
    )
    return cursor


async def restore_live_strategy_from_checkpoint(
    *,
    strategy: LiveRuntimeStrategy,
    checkpoint: StrategyCheckpoint,
    repository: PostgresRuntimeMarketStateRepository,
    environment: str,
    cutover_at: datetime | None = None,
) -> Mapping[str, datetime]:
    warm_market_state = getattr(strategy, "warm_market_state", None)
    if not callable(warm_market_state):
        raise RuntimeError(
            "strategy does not support compact checkpoint recovery"
        )
    recovery_cutover = cutover_at or live_market_state_cutover(
        datetime.now(tz=UTC)
    )
    expected_symbols = set(checkpoint.last_processed_at_by_symbol)
    expected_symbols.update(
        await load_live_warmup_symbols(
            repository=repository,
            environment=environment,
            observed_at=recovery_cutover,
        )
    )
    recovery_bounds = {
        symbol: checkpoint.last_processed_at_by_symbol.get(
            symbol,
            recovery_cutover,
        )
        for symbol in expected_symbols
    }
    states = await repository.load_recovery_window(
        environment=environment,
        last_processed_at_by_symbol=recovery_bounds,
        lookback_seconds=live_warmup_seconds(strategy),
        limit=_recovery_state_limit(
            strategy=strategy,
            symbol_count=len(expected_symbols),
            lookback_seconds=live_warmup_seconds(strategy),
        ),
        upper_bound=recovery_cutover,
    )
    for state in states:
        warm_market_state(state)
    complete_symbols = _symbols_with_complete_warmup(
        strategy=strategy,
        states=states,
        expected_symbols=expected_symbols,
        cutover_at=recovery_cutover,
    )
    if expected_symbols and not complete_symbols:
        raise RuntimeError(
            "live strategy warmup incomplete: no symbol has a complete window "
            f"at {recovery_cutover.isoformat()}"
        )
    deferred_symbols = expected_symbols - complete_symbols
    if deferred_symbols:
        log.warning(
            "live_strategy_warmup_symbols_deferred",
            deferred_count=len(deferred_symbols),
            deferred_examples=sorted(deferred_symbols)[:8],
            cutover_at=recovery_cutover.isoformat(),
        )
    validate_live_warmup_coverage(
        strategy=strategy,
        states=states,
        expected_symbols=complete_symbols,
        cutover_at=recovery_cutover,
    )
    compact_checkpoint = strategy.checkpoint(
        include_market_state_buffers=False
    )
    log.info(
        "live_strategy_checkpoint_recovered",
        environment=environment,
        state_count=len(states),
        symbol_count=len(compact_checkpoint.warmup_buckets_by_symbol),
        expected_symbol_count=len(expected_symbols),
        cutover_at=recovery_cutover.isoformat(),
        mode="warm_only_to_current_cutover",
    )
    return compact_checkpoint.last_processed_at_by_symbol


def checkpoint_needs_market_recovery(checkpoint: StrategyCheckpoint) -> bool:
    return not any(
        key in checkpoint.payload
        for key in ("market_state_buffers", "signal_buffers")
    )


def live_warmup_seconds(strategy: LiveRuntimeStrategy) -> int:
    """Return the minimum history window sufficient for strategy buffers."""
    required_data = getattr(strategy, "required_data", None)
    warmup_buckets = 0
    interval_seconds = 15
    if callable(required_data):
        requirement = required_data()
        warmup_buckets = int(getattr(requirement, "warmup_buckets", 0))
        interval_seconds = max(
            1,
            int(getattr(requirement, "base_state_interval_seconds", 15)),
        )
    buffer_seconds = (warmup_buckets + 16) * interval_seconds
    return max(MIN_WARMUP_SECONDS, buffer_seconds)


async def warm_live_strategy_then_start_fresh(
    *,
    strategy: LiveRuntimeStrategy,
    repository: PostgresRuntimeMarketStateRepository,
    environment: str,
    now: datetime,
    cutover_at: datetime | None = None,
) -> RuntimeStateCursor:
    """Warm historical state, then continue from the current live boundary."""
    warmup_end = cutover_at or live_market_state_cutover(now)
    await warm_live_strategy(
        strategy=strategy,
        repository=repository,
        environment=environment,
        now=now,
        cutover_at=warmup_end,
    )
    return cursor_after_market_bucket(warmup_end)


__all__ = [
    "checkpoint_needs_market_recovery",
    "cursor_after_market_bucket",
    "live_market_state_cutover",
    "live_warmup_seconds",
    "load_live_warmup_symbols",
    "restore_live_strategy_from_checkpoint",
    "strategy_last_processed_at_by_symbol",
    "validate_live_warmup_coverage",
    "warm_live_strategy",
    "warm_live_strategy_then_start_fresh",
]
