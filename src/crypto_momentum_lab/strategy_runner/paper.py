import json
from collections import Counter
from collections.abc import Iterator
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol, cast

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
from crypto_momentum_lab.strategies.compression_breakout import (
    CompressionBreakoutConfig,
)
from crypto_momentum_lab.strategies.liquidation_cascade import (
    LiquidationCascadeConfig,
)
from crypto_momentum_lab.strategies.order_flow_impulse import OrderFlowImpulseConfig
from crypto_momentum_lab.strategy_runner.fills import (
    FillSummaryValue,
    ReplayExecutionConfig,
    SimulatedFill,
    SimulatedFillStatus,
    candidate_target_fill_at,
    fill_summary,
    pending_candidate_fill,
    simulate_candidate_fill,
)
from crypto_momentum_lab.strategy_runner.registry import (
    StrategyRegistryError,
    build_runtime_config,
    build_runtime_strategy,
)


class PaperRunnerError(RuntimeError):
    pass


class PaperMarketStateSource(Protocol):
    @property
    def description(self) -> str:
        pass

    def __iter__(self) -> Iterator[MarketState15s]:
        pass


@dataclass(frozen=True, slots=True)
class InMemoryPaperMarketStateSource:
    states: tuple[MarketState15s, ...]
    description: str = "memory"

    def __iter__(self) -> Iterator[MarketState15s]:
        return iter(self.states)


@dataclass(frozen=True, slots=True)
class PaperRunnerConfig:
    strategy_name: str
    run_id: str
    code_commit: str
    generated_at: datetime
    compression_breakout: CompressionBreakoutConfig
    candidate_notional: Decimal | None
    candidate_ttl_buckets: int
    order_flow_impulse: OrderFlowImpulseConfig | None = None
    liquidation_cascade: LiquidationCascadeConfig | None = None
    execution: ReplayExecutionConfig = field(default_factory=ReplayExecutionConfig)
    max_states: int | None = None

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
        if self.max_states is not None and self.max_states <= 0:
            raise ValueError("max_states must be positive")


@dataclass(frozen=True, slots=True)
class PaperTradingRunReport:
    schema_version: int
    generated_at: datetime
    run: StrategyRunIdentity
    execution_config: ReplayExecutionConfig
    source_description: str
    input_state_count: int
    processed_symbol_count: int
    signals: tuple[StrategySignal, ...]
    candidates: tuple[OrderIntentCandidate, ...]
    paper_fills: tuple[SimulatedFill, ...]
    pending_candidate_count: int
    rejection_summary: dict[str, dict[str, int]]
    final_checkpoint: StrategyCheckpoint
    summary_counts: dict[str, dict[str, int]]
    fill_summary: dict[str, dict[str, FillSummaryValue]]


def run_paper_trading(
    *,
    source: PaperMarketStateSource,
    config: PaperRunnerConfig,
) -> PaperTradingRunReport:
    try:
        runtime_config = build_runtime_config(
            config.strategy_name,
            config=_runtime_config_payload(config),
        )
    except StrategyRegistryError as error:
        raise PaperRunnerError(str(error)) from error
    identity = StrategyRunIdentity(
        run_id=config.run_id,
        strategy_name=config.strategy_name,
        strategy_version="v0",
        config_hash=deterministic_config_hash(runtime_config),
        run_mode=RunMode.PAPER,
        code_commit=config.code_commit,
        created_at=config.generated_at,
        source_paths=(source.description,),
    )
    strategy = build_runtime_strategy(
        config.strategy_name,
        config=_runtime_config_payload(config),
        identity=identity,
    )

    signals: list[StrategySignal] = []
    candidates: list[OrderIntentCandidate] = []
    rejections: list[StrategyRejection] = []
    paper_fills: list[SimulatedFill] = []
    pending_candidates: list[OrderIntentCandidate] = []
    last_processed_at_by_symbol: dict[str, datetime] = {}
    checkpoint: StrategyCheckpoint | None = None
    input_state_count = 0

    for state in source:
        if config.max_states is not None and input_state_count >= config.max_states:
            break
        _validate_state(state, last_processed_at_by_symbol)
        input_state_count += 1
        pending_candidates, fills = _resolve_pending_candidates(
            pending_candidates=tuple(pending_candidates),
            state=state,
            execution=config.execution,
        )
        paper_fills.extend(fills)

        decision = strategy.on_market_state(state)
        signals.extend(decision.signals)
        candidates.extend(decision.candidates)
        pending_candidates.extend(decision.candidates)
        rejections.extend(decision.rejections)
        checkpoint = decision.checkpoint
        last_processed_at_by_symbol[state.symbol] = state.bucket_start

    if input_state_count == 0:
        raise PaperRunnerError("no market states to paper trade")
    if checkpoint is None:
        raise PaperRunnerError("strategy produced no checkpoint")

    shutdown_fills = _finalize_pending_candidates(
        pending_candidates=tuple(pending_candidates),
        last_processed_at_by_symbol=last_processed_at_by_symbol,
        execution=config.execution,
    )
    paper_fills.extend(shutdown_fills)

    signal_tuple = tuple(signals)
    candidate_tuple = tuple(candidates)
    fill_tuple = tuple(paper_fills)
    _validate_unique_ids(signal_tuple, candidate_tuple, fill_tuple)
    _validate_candidate_references(signal_tuple, candidate_tuple)
    return PaperTradingRunReport(
        schema_version=1,
        generated_at=config.generated_at,
        run=identity,
        execution_config=config.execution,
        source_description=source.description,
        input_state_count=input_state_count,
        processed_symbol_count=len(last_processed_at_by_symbol),
        signals=signal_tuple,
        candidates=candidate_tuple,
        paper_fills=fill_tuple,
        pending_candidate_count=sum(
            1 for fill in fill_tuple if fill.status is SimulatedFillStatus.PENDING
        ),
        rejection_summary=_rejection_summary(tuple(rejections)),
        final_checkpoint=checkpoint,
        summary_counts=_summary_counts(signal_tuple),
        fill_summary=fill_summary(fill_tuple),
    )


def write_paper_trading_report(
    report: PaperTradingRunReport,
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(_jsonable(report), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
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
        if state.bucket_start > candidate.expires_at:
            fills.append(
                simulate_candidate_fill(
                    candidate=candidate,
                    states=(),
                    execution=execution,
                )
            )
            continue
        if target_fill_at <= state.bucket_start <= candidate.expires_at:
            fills.append(
                simulate_candidate_fill(
                    candidate=candidate,
                    states=(state,),
                    execution=execution,
                )
            )
            continue
        remaining.append(candidate)
    return remaining, fills


def _finalize_pending_candidates(
    *,
    pending_candidates: tuple[OrderIntentCandidate, ...],
    last_processed_at_by_symbol: dict[str, datetime],
    execution: ReplayExecutionConfig,
) -> tuple[SimulatedFill, ...]:
    fills: list[SimulatedFill] = []
    for candidate in pending_candidates:
        last_processed_at = last_processed_at_by_symbol.get(candidate.symbol)
        if last_processed_at is not None and last_processed_at >= candidate.expires_at:
            fills.append(
                simulate_candidate_fill(
                    candidate=candidate,
                    states=(),
                    execution=execution,
                )
            )
            continue
        fills.append(
            pending_candidate_fill(
                candidate=candidate,
                execution=execution,
                reason="source_ended_before_fill",
            )
        )
    return tuple(fills)


def _validate_state(
    state: MarketState15s,
    last_processed_at_by_symbol: dict[str, datetime],
) -> None:
    if not _is_aware(state.bucket_start):
        raise PaperRunnerError("bucket_start must be timezone-aware")
    if not _is_aware(state.bucket_end):
        raise PaperRunnerError("bucket_end must be timezone-aware")
    previous = last_processed_at_by_symbol.get(state.symbol)
    if previous is not None and state.bucket_start < previous:
        raise PaperRunnerError("state moved backward for symbol")


def _runtime_config_payload(config: PaperRunnerConfig) -> dict[str, object]:
    payload: dict[str, object] = {
        "candidate_notional": config.candidate_notional,
        "candidate_ttl_buckets": config.candidate_ttl_buckets,
        "compression_breakout": config.compression_breakout,
    }
    if config.order_flow_impulse is not None:
        payload["order_flow_impulse"] = config.order_flow_impulse
    if config.liquidation_cascade is not None:
        payload["liquidation_cascade"] = config.liquidation_cascade
    return payload


def _validate_unique_ids(
    signals: tuple[StrategySignal, ...],
    candidates: tuple[OrderIntentCandidate, ...],
    fills: tuple[SimulatedFill, ...],
) -> None:
    signal_ids = tuple(signal.signal_id for signal in signals)
    candidate_ids = tuple(candidate.candidate_id for candidate in candidates)
    fill_ids = tuple(fill.fill_id for fill in fills)
    if len(signal_ids) != len(set(signal_ids)):
        raise PaperRunnerError("duplicate signal_id produced")
    if len(candidate_ids) != len(set(candidate_ids)):
        raise PaperRunnerError("duplicate candidate_id produced")
    if len(fill_ids) != len(set(fill_ids)):
        raise PaperRunnerError("duplicate fill_id produced")


def _validate_candidate_references(
    signals: tuple[StrategySignal, ...],
    candidates: tuple[OrderIntentCandidate, ...],
) -> None:
    signal_ids = {signal.signal_id for signal in signals}
    for candidate in candidates:
        if candidate.signal_id not in signal_ids:
            raise PaperRunnerError("candidate references unknown signal_id")


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
