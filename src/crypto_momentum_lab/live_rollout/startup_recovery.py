"""Warmup and checkpoint recovery for live strategy startup."""

import asyncio
from collections.abc import Callable, Collection, Mapping
from datetime import UTC, datetime, timedelta
from time import monotonic, perf_counter

import structlog

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import StrategyCheckpoint
from crypto_momentum_lab.live_rollout.daemon import LiveRuntimeStrategy
from crypto_momentum_lab.live_rollout.readiness import LiveWarmupStatus
from crypto_momentum_lab.persistence.postgres.runtime_state_repository import (
    PostgresRuntimeMarketStateRepository,
    RuntimeStateCursor,
)

log = structlog.get_logger()

MIN_WARMUP_SECONDS = 60
WARMUP_STATE_LIMIT = 100_000
WARMUP_BATCH_SIZE = 5_000
_DURABLE_CUTOVER_WAIT_SECONDS = 5.0
_DURABLE_CUTOVER_POLL_SECONDS = 0.1
_MARKET_GAP_RECOVERY_WAIT_SECONDS = 0.5

WarmupStatusCallback = Callable[[LiveWarmupStatus], None]


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


async def wait_for_durable_market_state_cutover(
    *,
    repository: PostgresRuntimeMarketStateRepository,
    environment: str,
    requested_cutover: datetime,
    timeout_seconds: float = _DURABLE_CUTOVER_WAIT_SECONDS,
    poll_interval_seconds: float = _DURABLE_CUTOVER_POLL_SECONDS,
) -> datetime:
    """Return a startup boundary that is visible in the market database.

    A closed bucket is published by the Hub before the PostgreSQL writer has
    necessarily committed it.  Recovery must not warm to a wall-clock bucket
    that is still in that commit window: doing so lets one consumer receive a
    bucket from the Hub while another consumer starts from the previous
    durable watermark.  Waiting for the requested bucket (or falling back to
    the newest durable bucket on timeout) makes the recovery boundary an
    explicit database visibility fence.
    """

    if not environment.strip():
        raise ValueError("environment must not be empty")
    _require_aware(requested_cutover, "requested_cutover")
    if timeout_seconds < 0:
        raise ValueError("timeout_seconds must not be negative")
    if poll_interval_seconds <= 0:
        raise ValueError("poll_interval_seconds must be positive")

    deadline = monotonic() + timeout_seconds
    latest_durable: datetime | None = None
    while True:
        latest_durable = await repository.load_latest_bucket(
            environment=environment,
        )
        if latest_durable is not None:
            _require_aware(latest_durable, "latest_durable")
            if latest_durable >= requested_cutover:
                return requested_cutover
        if monotonic() >= deadline:
            fallback = (
                requested_cutover
                if latest_durable is None
                else min(requested_cutover, latest_durable)
            )
            log.warning(
                "live_startup_durable_cutover_timeout",
                environment=environment,
                requested_cutover=requested_cutover.isoformat(),
                latest_durable=(
                    None
                    if latest_durable is None
                    else latest_durable.isoformat()
                ),
                selected_cutover=fallback.isoformat(),
                timeout_seconds=timeout_seconds,
            )
            return fallback
        await _sleep_for_durable_cutover(
            min(poll_interval_seconds, max(0.0, deadline - monotonic()))
        )


async def load_live_market_state_gap(
    *,
    repository: PostgresRuntimeMarketStateRepository,
    environment: str,
    symbol: str,
    previous_at: datetime,
    current_at: datetime,
    interval_seconds: int = 15,
    timeout_seconds: float = _MARKET_GAP_RECOVERY_WAIT_SECONDS,
    poll_interval_seconds: float = _DURABLE_CUTOVER_POLL_SECONDS,
) -> tuple[MarketState15s, ...]:
    """Load durable intermediate buckets for one live continuity gap.

    The market stream is sparse, so an absent intermediate row is not itself
    an error.  This helper only returns a recovery set when every canonical
    intermediate bucket is present; callers can then decide whether the
    strategy may be warmed in place or must reset the symbol.
    """

    if not environment.strip():
        raise ValueError("environment must not be empty")
    if not symbol.strip():
        raise ValueError("symbol must not be empty")
    _require_aware(previous_at, "previous_at")
    _require_aware(current_at, "current_at")
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    if current_at <= previous_at:
        return ()
    if timeout_seconds < 0:
        raise ValueError("timeout_seconds must not be negative")
    if poll_interval_seconds <= 0:
        raise ValueError("poll_interval_seconds must be positive")

    delta_seconds = (current_at - previous_at).total_seconds()
    bucket_count = int(delta_seconds // interval_seconds)
    missing_count = bucket_count - 1
    if delta_seconds % interval_seconds != 0 or missing_count <= 0:
        return ()

    cursor = RuntimeStateCursor(bucket_start=previous_at, symbol="")
    upper_bound = current_at - timedelta(microseconds=1)
    started_at = monotonic()
    deadline = started_at + timeout_seconds
    while True:
        states = await repository.load_after(
            environment=environment,
            cursor=cursor,
            limit=missing_count,
            upper_bound=upper_bound,
            symbols=(symbol,),
        )
        canonical = tuple(
            sorted(
                (
                    state
                    for state in states
                    if state.symbol == symbol
                    and previous_at < state.bucket_start < current_at
                ),
                key=lambda state: state.bucket_start,
            )
        )
        expected = tuple(
            previous_at + timedelta(seconds=interval_seconds * index)
            for index in range(1, bucket_count)
        )
        if tuple(state.bucket_start for state in canonical) == expected:
            log.info(
                "live_market_state_gap_recovery_outcome",
                symbol=symbol,
                outcome="complete",
                waited_seconds=round(monotonic() - started_at, 3),
                bucket_count=bucket_count,
                timeout_seconds=timeout_seconds,
            )
            return canonical
        if monotonic() >= deadline:
            # Record the real wait so the 500ms budget can be judged from data
            # instead of guessed at.
            log.warning(
                "live_market_state_gap_recovery_outcome",
                symbol=symbol,
                outcome="timeout",
                waited_seconds=round(monotonic() - started_at, 3),
                expected_buckets=missing_count,
                observed_buckets=len(canonical),
                timeout_seconds=timeout_seconds,
            )
            return ()
        await _sleep_for_durable_cutover(
            min(poll_interval_seconds, max(0.0, deadline - monotonic()))
        )


async def _sleep_for_durable_cutover(seconds: float) -> None:
    if seconds > 0:
        await asyncio.sleep(seconds)


async def load_live_warmup_symbols(
    *,
    repository: PostgresRuntimeMarketStateRepository,
    environment: str,
    observed_at: datetime,
    symbols: Collection[str] | None = None,
) -> frozenset[str]:
    if symbols is not None:
        return frozenset(symbol.strip() for symbol in symbols if symbol.strip())
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
    warmup_symbols: Collection[str] | None = None,
    on_warmup_status: WarmupStatusCallback | None = None,
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
        symbols=warmup_symbols,
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
        load_kwargs: dict[str, object] = {
            "environment": environment,
            "cursor": cursor,
            "limit": batch_limit,
            "upper_bound": warmup_end,
        }
        if warmup_symbols is not None:
            load_kwargs["symbols"] = expected_symbols
        batch = await repository.load_after(**load_kwargs)  # type: ignore[arg-type]
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
    if not expected_symbols and warmup_symbols is None:
        expected_symbols = frozenset(state.symbol for state in warmed_states)
    complete_symbols = _symbols_with_complete_warmup(
        strategy=strategy,
        states=warmed_states,
        expected_symbols=expected_symbols,
        cutover_at=warmup_end,
    )
    deferred_symbols = expected_symbols - complete_symbols
    _notify_warmup_status(
        on_warmup_status,
        required_buckets=_required_warmup_buckets(strategy),
        expected_symbols=expected_symbols,
        complete_symbols=complete_symbols,
        cutover_at=warmup_end,
    )
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
    warmup_symbols: Collection[str] | None = None,
    on_warmup_status: WarmupStatusCallback | None = None,
) -> Mapping[str, datetime]:
    warm_market_state = getattr(strategy, "warm_market_state", None)
    if not callable(warm_market_state):
        raise RuntimeError(
            "strategy does not support compact checkpoint recovery"
        )
    clear_market_state_buffers = getattr(
        strategy,
        "clear_market_state_buffers",
        None,
    )
    if not callable(clear_market_state_buffers):
        raise RuntimeError(
            "strategy does not support forced durable market rewarm"
        )
    # A checkpoint may come from an older worker that persisted derived
    # buffers.  Never combine those buffers with a new stream epoch: discard
    # them first and rebuild from the durable market-state table below.
    clear_market_state_buffers()
    log.info(
        "live_strategy_market_buffers_discarded_before_durable_rewarm",
        environment=environment,
    )
    recovery_cutover = cutover_at or live_market_state_cutover(
        datetime.now(tz=UTC)
    )
    checkpoint_symbols = set(checkpoint.last_processed_at_by_symbol)
    if warmup_symbols is None:
        expected_symbols = set(checkpoint_symbols)
        expected_symbols.update(
            await load_live_warmup_symbols(
                repository=repository,
                environment=environment,
                observed_at=recovery_cutover,
            )
        )
        recovery_symbols = expected_symbols
    else:
        expected_symbols = set(
            symbol.strip() for symbol in warmup_symbols if symbol.strip()
        )
        # The entry universe is intentionally small, but a compact checkpoint
        # can still contain symbols that were processed before the universe
        # changed.  Replaying only the current entry symbols leaves those old
        # watermarks behind; the first post-restart state for a retained symbol
        # then looks like a continuity gap and restarts the worker.  Rewarm
        # the union while keeping readiness scoped to the current entry set.
        recovery_symbols = checkpoint_symbols | expected_symbols
    recovery_bounds = {
        symbol: checkpoint.last_processed_at_by_symbol.get(
            symbol,
            recovery_cutover,
        )
        for symbol in recovery_symbols
    }
    states = await repository.load_recovery_window(
        environment=environment,
        last_processed_at_by_symbol=recovery_bounds,
        lookback_seconds=live_warmup_seconds(strategy),
        limit=_recovery_state_limit(
            strategy=strategy,
            symbol_count=len(recovery_symbols),
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
    deferred_symbols = expected_symbols - complete_symbols
    _notify_warmup_status(
        on_warmup_status,
        required_buckets=_required_warmup_buckets(strategy),
        expected_symbols=expected_symbols,
        complete_symbols=complete_symbols,
        cutover_at=recovery_cutover,
    )
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
    """Return whether live startup must rebuild derivable state durably.

    The parameter is retained for callers that use this as a policy hook, but
    live workers must rewarm after every restart.  Persisted rolling buffers
    are not an authoritative source for a new sequence/epoch.
    """

    del checkpoint
    return True


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
    warmup_symbols: Collection[str] | None = None,
    on_warmup_status: WarmupStatusCallback | None = None,
) -> RuntimeStateCursor:
    """Warm historical state, then continue from the current live boundary."""
    warmup_end = cutover_at or live_market_state_cutover(now)
    await warm_live_strategy(
        strategy=strategy,
        repository=repository,
        environment=environment,
        now=now,
        cutover_at=warmup_end,
        warmup_symbols=warmup_symbols,
        on_warmup_status=on_warmup_status,
    )
    return cursor_after_market_bucket(warmup_end)


def _notify_warmup_status(
    callback: WarmupStatusCallback | None,
    *,
    required_buckets: int,
    expected_symbols: Collection[str],
    complete_symbols: Collection[str],
    cutover_at: datetime,
) -> None:
    if callback is None:
        return
    try:
        callback(
            LiveWarmupStatus(
                required_buckets=required_buckets,
                expected_symbols=frozenset(expected_symbols),
                complete_symbols=frozenset(complete_symbols),
                cutover_at=cutover_at,
            )
        )
    except Exception as error:
        log.warning(
            "live_warmup_status_publish_failed",
            error_type=type(error).__name__,
        )


def _required_warmup_buckets(strategy: LiveRuntimeStrategy) -> int:
    required_data = getattr(strategy, "required_data", None)
    if not callable(required_data):
        return 1
    return max(1, int(required_data().warmup_buckets))


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


__all__ = [
    "checkpoint_needs_market_recovery",
    "cursor_after_market_bucket",
    "load_live_market_state_gap",
    "live_market_state_cutover",
    "live_warmup_seconds",
    "load_live_warmup_symbols",
    "restore_live_strategy_from_checkpoint",
    "strategy_last_processed_at_by_symbol",
    "validate_live_warmup_coverage",
    "wait_for_durable_market_state_cutover",
    "WarmupStatusCallback",
    "warm_live_strategy",
    "warm_live_strategy_then_start_fresh",
]
