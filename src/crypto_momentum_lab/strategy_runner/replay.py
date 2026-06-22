import json
from collections import Counter
from collections.abc import Iterable
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any, cast

from crypto_momentum_lab.domain.market.models import JsonValue, MarketState15s
from crypto_momentum_lab.domain.strategy import (
    OrderIntentCandidate,
    RunMode,
    StrategyCheckpoint,
    StrategyRejection,
    StrategyRunIdentity,
    StrategySignal,
    deterministic_config_hash,
)
from crypto_momentum_lab.persistence.parquet import read_market_states_15s_dataset
from crypto_momentum_lab.strategies.compression_breakout import (
    CompressionBreakoutConfig,
    CompressionBreakoutRuntimeConfig,
    CompressionBreakoutRuntimeStrategy,
)


class ReplayError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ReplayConfig:
    strategy_name: str
    run_id: str
    code_commit: str
    generated_at: datetime
    compression_breakout: CompressionBreakoutConfig
    candidate_notional: Decimal | None
    candidate_ttl_buckets: int

    def __post_init__(self) -> None:
        if not self.strategy_name:
            raise ValueError("strategy_name must not be empty")
        if not self.run_id:
            raise ValueError("run_id must not be empty")
        if not self.code_commit:
            raise ValueError("code_commit must not be empty")
        if not _is_aware(self.generated_at):
            raise ValueError("generated_at must be timezone-aware")
        if self.candidate_notional is not None and self.candidate_notional <= 0:
            raise ValueError("candidate_notional must be positive")
        if self.candidate_ttl_buckets <= 0:
            raise ValueError("candidate_ttl_buckets must be positive")


@dataclass(frozen=True, slots=True)
class StrategyReplayReport:
    schema_version: int
    generated_at: datetime
    run: StrategyRunIdentity
    source_paths: tuple[str, ...]
    input_state_count: int
    processed_symbol_count: int
    signals: tuple[StrategySignal, ...]
    candidates: tuple[OrderIntentCandidate, ...]
    rejection_summary: dict[str, dict[str, int]]
    final_checkpoint: StrategyCheckpoint
    summary_counts: dict[str, dict[str, int]]


def build_strategy_replay_report(
    *,
    state_paths: tuple[Path, ...],
    config: ReplayConfig,
) -> StrategyReplayReport:
    states = read_market_states_15s_dataset(state_paths)
    return run_strategy_replay(
        states=states,
        source_paths=tuple(path.as_posix() for path in state_paths),
        config=config,
    )


def run_strategy_replay(
    *,
    states: Iterable[MarketState15s],
    source_paths: tuple[str, ...],
    config: ReplayConfig,
) -> StrategyReplayReport:
    if config.strategy_name != "compression_breakout":
        raise ReplayError(f"unsupported strategy: {config.strategy_name}")
    state_tuple = tuple(states)
    if not state_tuple:
        raise ReplayError("no market states to replay")
    for state in state_tuple:
        _validate_state_timestamps(state)

    ordered_states = tuple(
        sorted(state_tuple, key=lambda item: (item.bucket_start, item.symbol))
    )
    runtime_config = CompressionBreakoutRuntimeConfig(
        event_config=config.compression_breakout,
        candidate_notional=config.candidate_notional,
        candidate_ttl_buckets=config.candidate_ttl_buckets,
    )
    identity = StrategyRunIdentity(
        run_id=config.run_id,
        strategy_name=config.strategy_name,
        strategy_version="v0",
        config_hash=deterministic_config_hash(runtime_config),
        run_mode=RunMode.REPLAY,
        code_commit=config.code_commit,
        created_at=config.generated_at,
        source_paths=source_paths,
    )
    strategy = CompressionBreakoutRuntimeStrategy(
        config=runtime_config,
        identity=identity,
    )

    signals: list[StrategySignal] = []
    candidates: list[OrderIntentCandidate] = []
    rejections: list[StrategyRejection] = []
    checkpoint: StrategyCheckpoint | None = None
    for state in ordered_states:
        decision = strategy.on_market_state(state)
        signals.extend(decision.signals)
        candidates.extend(decision.candidates)
        rejections.extend(decision.rejections)
        checkpoint = decision.checkpoint
    if checkpoint is None:
        raise ReplayError("strategy produced no checkpoint")
    signal_tuple = tuple(signals)
    candidate_tuple = tuple(candidates)
    _validate_unique_ids(signal_tuple, candidate_tuple)
    _validate_candidate_references(signal_tuple, candidate_tuple)
    return StrategyReplayReport(
        schema_version=1,
        generated_at=config.generated_at,
        run=identity,
        source_paths=source_paths,
        input_state_count=len(ordered_states),
        processed_symbol_count=len({state.symbol for state in ordered_states}),
        signals=signal_tuple,
        candidates=candidate_tuple,
        rejection_summary=_rejection_summary(tuple(rejections)),
        final_checkpoint=checkpoint,
        summary_counts=_summary_counts(signal_tuple),
    )


def write_strategy_replay_report(
    report: StrategyReplayReport,
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(_jsonable(report), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _validate_state_timestamps(state: MarketState15s) -> None:
    if not _is_aware(state.bucket_start):
        raise ReplayError("bucket_start must be timezone-aware")
    if not _is_aware(state.bucket_end):
        raise ReplayError("bucket_end must be timezone-aware")


def _validate_unique_ids(
    signals: tuple[StrategySignal, ...],
    candidates: tuple[OrderIntentCandidate, ...],
) -> None:
    signal_ids = tuple(signal.signal_id for signal in signals)
    candidate_ids = tuple(candidate.candidate_id for candidate in candidates)
    if len(signal_ids) != len(set(signal_ids)):
        raise ReplayError("duplicate signal_id produced")
    if len(candidate_ids) != len(set(candidate_ids)):
        raise ReplayError("duplicate candidate_id produced")


def _validate_candidate_references(
    signals: tuple[StrategySignal, ...],
    candidates: tuple[OrderIntentCandidate, ...],
) -> None:
    signal_ids = {signal.signal_id for signal in signals}
    for candidate in candidates:
        if candidate.signal_id not in signal_ids:
            raise ReplayError("candidate references unknown signal_id")


def _rejection_summary(
    rejections: tuple[StrategyRejection, ...],
) -> dict[str, dict[str, int]]:
    counts: dict[str, Counter[str]] = {}
    for rejection in rejections:
        counts.setdefault(rejection.reason.value, Counter())
        counts[rejection.reason.value][rejection.symbol] += 1
    return {
        reason: dict(sorted(symbol_counts.items()))
        for reason, symbol_counts in sorted(counts.items())
    }


def _summary_counts(
    signals: tuple[StrategySignal, ...],
) -> dict[str, dict[str, int]]:
    by_side = Counter(signal.side.value for signal in signals)
    by_symbol = Counter(signal.symbol for signal in signals)
    return {
        "signals_by_side": dict(sorted(by_side.items())),
        "signals_by_symbol": dict(sorted(by_symbol.items())),
    }


def _jsonable(value: object) -> JsonValue:
    if is_dataclass(value) and not isinstance(value, type):
        return _jsonable(asdict(cast(Any, value)))
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    return str(value)


def _is_aware(value: datetime) -> bool:
    return value.tzinfo is not None and value.utcoffset() is not None
