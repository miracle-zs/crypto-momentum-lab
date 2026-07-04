import asyncio
import json
from collections.abc import Coroutine, Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import NAMESPACE_URL, uuid5

from crypto_momentum_lab.domain.market.models import JsonValue, MarketState15s
from crypto_momentum_lab.domain.strategy import StrategyCheckpoint, StrategyDecision


class Clock(Protocol):
    def now(self) -> datetime:
        pass


class RuntimeStrategy(Protocol):
    def restore_checkpoint(self, checkpoint: StrategyCheckpoint) -> None:
        pass

    def on_market_state(self, state: MarketState15s) -> StrategyDecision:
        pass


class PaperLiveDaemonRepository(Protocol):
    async def save_runtime_event(self, event: "StrategyRuntimeEvent") -> None:
        pass

    async def save_checkpoint(
        self,
        run_id: str,
        checkpoint: StrategyCheckpoint,
        saved_at: datetime,
    ) -> None:
        pass

    async def load_checkpoint(self, run_id: str) -> StrategyCheckpoint | None:
        pass


@dataclass(frozen=True, slots=True)
class StrategyRuntimeEvent:
    event_id: str
    run_id: str
    event_type: str
    occurred_at: datetime
    symbol: str | None
    bucket_start: datetime | None
    details: dict[str, JsonValue]

    def __post_init__(self) -> None:
        _require_non_empty(self.event_id, "event_id")
        _require_non_empty(self.run_id, "run_id")
        _require_non_empty(self.event_type, "event_type")
        _require_aware(self.occurred_at, "occurred_at")
        if self.symbol is not None:
            _require_non_empty(self.symbol, "symbol")
        if self.bucket_start is not None:
            _require_aware(self.bucket_start, "bucket_start")
        _ensure_jsonable(self.details)


@dataclass(frozen=True, slots=True)
class PaperLiveDaemonConfig:
    run_id: str
    strategy_name: str
    environment: str
    checkpoint_every_states: int
    checkpoint_every_seconds: float
    max_market_state_age_seconds: float
    continue_while_halted: bool = False

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


@dataclass(frozen=True, slots=True)
class PaperLiveDaemonResult:
    processed_state_count: int
    halt_reason: str | None
    final_cursor: datetime | None
    final_checkpoint_saved_at: datetime | None


def run_paper_live_daemon(
    *,
    source: Iterable[MarketState15s],
    strategy: RuntimeStrategy,
    repository: PaperLiveDaemonRepository,
    config: PaperLiveDaemonConfig,
    clock: Clock,
) -> PaperLiveDaemonResult:
    checkpoint = _run_async(repository.load_checkpoint(config.run_id))
    if checkpoint is not None:
        strategy.restore_checkpoint(checkpoint)

    processed = 0
    processed_since_checkpoint = 0
    final_cursor: datetime | None = None
    latest_checkpoint: StrategyCheckpoint | None = None
    latest_checkpoint_dirty = False
    last_checkpoint_saved_at: datetime | None = None
    last_checkpoint_elapsed_anchor = clock.now()

    for state in source:
        if state.environment != config.environment:
            raise ValueError("runtime state environment mismatch")
        if _already_processed(state, checkpoint):
            continue

        now = clock.now()
        if _state_age_seconds(now, state) > config.max_market_state_age_seconds:
            _run_async(
                repository.save_runtime_event(
                    _event(
                        run_id=config.run_id,
                        event_type="halted",
                        occurred_at=now,
                        symbol=state.symbol,
                        bucket_start=state.bucket_start,
                        details={
                            "reason": "stale_market_state",
                            "bucket_end": state.bucket_end.isoformat(),
                        },
                    )
                )
            )
            return PaperLiveDaemonResult(
                processed_state_count=processed,
                halt_reason="stale_market_state",
                final_cursor=final_cursor,
                final_checkpoint_saved_at=last_checkpoint_saved_at,
            )

        decision = strategy.on_market_state(state)
        latest_checkpoint = decision.checkpoint
        latest_checkpoint_dirty = True
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
            _save_checkpoint_and_event(
                repository=repository,
                run_id=config.run_id,
                checkpoint=decision.checkpoint,
                saved_at=now,
                symbol=state.symbol,
                bucket_start=state.bucket_start,
                processed_state_count=processed,
            )
            last_checkpoint_saved_at = now
            latest_checkpoint_dirty = False
            processed_since_checkpoint = 0
            last_checkpoint_elapsed_anchor = now

    if latest_checkpoint is not None and latest_checkpoint_dirty:
        saved_at = clock.now()
        _save_checkpoint_and_event(
            repository=repository,
            run_id=config.run_id,
            checkpoint=latest_checkpoint,
            saved_at=saved_at,
            symbol=None,
            bucket_start=final_cursor,
            processed_state_count=processed,
        )
        last_checkpoint_saved_at = saved_at

    return PaperLiveDaemonResult(
        processed_state_count=processed,
        halt_reason=None,
        final_cursor=final_cursor,
        final_checkpoint_saved_at=last_checkpoint_saved_at,
    )


def _save_checkpoint_and_event(
    *,
    repository: PaperLiveDaemonRepository,
    run_id: str,
    checkpoint: StrategyCheckpoint,
    saved_at: datetime,
    symbol: str | None,
    bucket_start: datetime | None,
    processed_state_count: int,
) -> None:
    _run_async(repository.save_checkpoint(run_id, checkpoint, saved_at))
    _run_async(
        repository.save_runtime_event(
            _event(
                run_id=run_id,
                event_type="checkpoint_saved",
                occurred_at=saved_at,
                symbol=symbol,
                bucket_start=bucket_start,
                details={"processed_state_count": processed_state_count},
            )
        )
    )


def _event(
    *,
    run_id: str,
    event_type: str,
    occurred_at: datetime,
    symbol: str | None,
    bucket_start: datetime | None,
    details: dict[str, JsonValue],
) -> StrategyRuntimeEvent:
    encoded = json.dumps(
        {
            "run_id": run_id,
            "event_type": event_type,
            "occurred_at": occurred_at.isoformat(),
            "symbol": symbol,
            "bucket_start": None
            if bucket_start is None
            else bucket_start.isoformat(),
            "details": details,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return StrategyRuntimeEvent(
        event_id=str(uuid5(NAMESPACE_URL, f"strategy-runtime-event:{encoded}")),
        run_id=run_id,
        event_type=event_type,
        occurred_at=occurred_at,
        symbol=symbol,
        bucket_start=bucket_start,
        details=details,
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


def _ensure_jsonable(value: JsonValue) -> None:
    json.dumps(value, allow_nan=False)
