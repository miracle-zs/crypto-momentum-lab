import json
from collections import Counter
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timedelta
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
    StrategySide,
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


class SimulatedFillStatus(StrEnum):
    FILLED = "filled"
    EXPIRED = "expired"
    REJECTED = "rejected"


@dataclass(frozen=True, slots=True)
class ReplayExecutionConfig:
    latency_buckets: int = 1
    state_interval_seconds: int = 15
    taker_fee_rate: Decimal = Decimal("0.0004")
    slippage_bps: Decimal = Decimal("0")

    def __post_init__(self) -> None:
        if self.latency_buckets < 0:
            raise ValueError("latency_buckets must be non-negative")
        if self.state_interval_seconds <= 0:
            raise ValueError("state_interval_seconds must be positive")
        if self.taker_fee_rate < 0:
            raise ValueError("taker_fee_rate must be non-negative")
        if self.slippage_bps < 0:
            raise ValueError("slippage_bps must be non-negative")
        if self.slippage_bps >= Decimal("10000"):
            raise ValueError("slippage_bps must be less than 10000")


@dataclass(frozen=True, slots=True)
class ReplayConfig:
    strategy_name: str
    run_id: str
    code_commit: str
    generated_at: datetime
    compression_breakout: CompressionBreakoutConfig
    candidate_notional: Decimal | None
    candidate_ttl_buckets: int
    execution: ReplayExecutionConfig | None = field(
        default_factory=ReplayExecutionConfig
    )

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
class SimulatedFill:
    candidate_id: str
    signal_id: str
    symbol: str
    side: StrategySide
    status: SimulatedFillStatus
    target_fill_at: datetime
    filled_at: datetime | None
    requested_notional: Decimal | None
    filled_notional: Decimal | None
    quantity: Decimal | None
    reference_midpoint: Decimal | None
    spread: Decimal | None
    fill_price: Decimal | None
    fee: Decimal
    total_cost: Decimal
    cost_bps: Decimal | None
    reason: str | None

    def __post_init__(self) -> None:
        if not self.candidate_id:
            raise ValueError("candidate_id must not be empty")
        if not self.signal_id:
            raise ValueError("signal_id must not be empty")
        if not self.symbol:
            raise ValueError("symbol must not be empty")
        if not _is_aware(self.target_fill_at):
            raise ValueError("target_fill_at must be timezone-aware")
        if self.filled_at is not None and not _is_aware(self.filled_at):
            raise ValueError("filled_at must be timezone-aware")
        if self.fee < 0:
            raise ValueError("fee must be non-negative")
        if self.total_cost < 0:
            raise ValueError("total_cost must be non-negative")


type FillSummaryValue = int | Decimal


@dataclass(frozen=True, slots=True)
class StrategyReplayReport:
    schema_version: int
    generated_at: datetime
    run: StrategyRunIdentity
    execution_config: ReplayExecutionConfig | None
    source_paths: tuple[str, ...]
    input_state_count: int
    processed_symbol_count: int
    signals: tuple[StrategySignal, ...]
    candidates: tuple[OrderIntentCandidate, ...]
    simulated_fills: tuple[SimulatedFill, ...]
    rejection_summary: dict[str, dict[str, int]]
    final_checkpoint: StrategyCheckpoint
    summary_counts: dict[str, dict[str, int]]
    fill_summary: dict[str, dict[str, FillSummaryValue]]


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
    simulated_fills = _simulate_candidate_fills(
        candidates=candidate_tuple,
        ordered_states=ordered_states,
        execution=config.execution,
    )
    return StrategyReplayReport(
        schema_version=2,
        generated_at=config.generated_at,
        run=identity,
        execution_config=config.execution,
        source_paths=source_paths,
        input_state_count=len(ordered_states),
        processed_symbol_count=len({state.symbol for state in ordered_states}),
        signals=signal_tuple,
        candidates=candidate_tuple,
        simulated_fills=simulated_fills,
        rejection_summary=_rejection_summary(tuple(rejections)),
        final_checkpoint=checkpoint,
        summary_counts=_summary_counts(signal_tuple),
        fill_summary=_fill_summary(simulated_fills),
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


def _simulate_candidate_fills(
    *,
    candidates: tuple[OrderIntentCandidate, ...],
    ordered_states: tuple[MarketState15s, ...],
    execution: ReplayExecutionConfig | None,
) -> tuple[SimulatedFill, ...]:
    if execution is None:
        return ()
    states_by_symbol: dict[str, list[MarketState15s]] = {}
    for state in ordered_states:
        states_by_symbol.setdefault(state.symbol, []).append(state)
    return tuple(
        _simulate_candidate_fill(
            candidate=candidate,
            states=tuple(states_by_symbol.get(candidate.symbol, ())),
            execution=execution,
        )
        for candidate in candidates
    )


def _simulate_candidate_fill(
    *,
    candidate: OrderIntentCandidate,
    states: tuple[MarketState15s, ...],
    execution: ReplayExecutionConfig,
) -> SimulatedFill:
    target_fill_at = candidate.created_at + timedelta(
        seconds=execution.latency_buckets * execution.state_interval_seconds
    )
    if target_fill_at > candidate.expires_at:
        return _unfilled(
            candidate=candidate,
            status=SimulatedFillStatus.EXPIRED,
            target_fill_at=target_fill_at,
            reason="candidate_expired",
        )
    fill_state = next(
        (
            state
            for state in states
            if target_fill_at <= state.bucket_start <= candidate.expires_at
        ),
        None,
    )
    if fill_state is None:
        return _unfilled(
            candidate=candidate,
            status=SimulatedFillStatus.EXPIRED,
            target_fill_at=target_fill_at,
            reason="no_market_state_before_expiry",
        )
    if candidate.desired_notional is None:
        return _unfilled(
            candidate=candidate,
            status=SimulatedFillStatus.REJECTED,
            target_fill_at=target_fill_at,
            reason="missing_desired_notional",
        )
    quote = _marketable_quote(fill_state, candidate.side)
    if quote is None:
        return _unfilled(
            candidate=candidate,
            status=SimulatedFillStatus.REJECTED,
            target_fill_at=target_fill_at,
            reason="missing_fill_price",
        )
    fill_price, midpoint, spread = quote
    fill_price = _apply_slippage(
        fill_price,
        side=candidate.side,
        slippage_bps=execution.slippage_bps,
    )
    if fill_price <= 0:
        return _unfilled(
            candidate=candidate,
            status=SimulatedFillStatus.REJECTED,
            target_fill_at=target_fill_at,
            reason="invalid_fill_price",
        )

    requested_notional = candidate.desired_notional
    quantity = requested_notional / fill_price
    fee = requested_notional * execution.taker_fee_rate
    market_cost = _market_cost(
        fill_price=fill_price,
        midpoint=midpoint,
        quantity=quantity,
        side=candidate.side,
    )
    total_cost = fee + market_cost
    return SimulatedFill(
        candidate_id=candidate.candidate_id,
        signal_id=candidate.signal_id,
        symbol=candidate.symbol,
        side=candidate.side,
        status=SimulatedFillStatus.FILLED,
        target_fill_at=target_fill_at,
        filled_at=fill_state.bucket_start,
        requested_notional=requested_notional,
        filled_notional=requested_notional,
        quantity=quantity,
        reference_midpoint=midpoint,
        spread=spread,
        fill_price=fill_price,
        fee=fee,
        total_cost=total_cost,
        cost_bps=(total_cost / requested_notional) * Decimal("10000"),
        reason="filled",
    )


def _unfilled(
    *,
    candidate: OrderIntentCandidate,
    status: SimulatedFillStatus,
    target_fill_at: datetime,
    reason: str,
) -> SimulatedFill:
    return SimulatedFill(
        candidate_id=candidate.candidate_id,
        signal_id=candidate.signal_id,
        symbol=candidate.symbol,
        side=candidate.side,
        status=status,
        target_fill_at=target_fill_at,
        filled_at=None,
        requested_notional=candidate.desired_notional,
        filled_notional=None,
        quantity=None,
        reference_midpoint=None,
        spread=None,
        fill_price=None,
        fee=Decimal("0"),
        total_cost=Decimal("0"),
        cost_bps=None,
        reason=reason,
    )


def _marketable_quote(
    state: MarketState15s,
    side: StrategySide,
) -> tuple[Decimal, Decimal, Decimal | None] | None:
    bid = state.last_bid_price
    ask = state.last_ask_price
    spread = state.spread
    midpoint = state.midpoint
    if midpoint is None and bid is not None and ask is not None:
        midpoint = (bid + ask) / Decimal("2")
    if spread is None and bid is not None and ask is not None:
        spread = ask - bid
    if midpoint is not None and spread is not None:
        half_spread = spread / Decimal("2")
        if bid is None:
            bid = midpoint - half_spread
        if ask is None:
            ask = midpoint + half_spread
    if midpoint is None or midpoint <= 0:
        return None
    if side is StrategySide.LONG:
        if ask is None or ask <= 0:
            return None
        return ask, midpoint, spread
    if bid is None or bid <= 0:
        return None
    return bid, midpoint, spread


def _apply_slippage(
    price: Decimal,
    *,
    side: StrategySide,
    slippage_bps: Decimal,
) -> Decimal:
    multiplier = Decimal("1") + (slippage_bps / Decimal("10000"))
    if side is StrategySide.SHORT:
        multiplier = Decimal("1") - (slippage_bps / Decimal("10000"))
    return price * multiplier


def _market_cost(
    *,
    fill_price: Decimal,
    midpoint: Decimal,
    quantity: Decimal,
    side: StrategySide,
) -> Decimal:
    if side is StrategySide.LONG:
        raw_cost = (fill_price - midpoint) * quantity
    else:
        raw_cost = (midpoint - fill_price) * quantity
    return max(raw_cost, Decimal("0"))


def _fill_summary(
    simulated_fills: tuple[SimulatedFill, ...],
) -> dict[str, dict[str, FillSummaryValue]]:
    by_status = Counter(fill.status.value for fill in simulated_fills)
    filled_notional_by_symbol: dict[str, Decimal] = {}
    fee_by_symbol: dict[str, Decimal] = {}
    cost_by_symbol: dict[str, Decimal] = {}
    for fill in simulated_fills:
        if fill.status is not SimulatedFillStatus.FILLED:
            continue
        filled_notional_by_symbol[fill.symbol] = (
            filled_notional_by_symbol.get(fill.symbol, Decimal("0"))
            + (fill.filled_notional or Decimal("0"))
        )
        fee_by_symbol[fill.symbol] = (
            fee_by_symbol.get(fill.symbol, Decimal("0")) + fill.fee
        )
        cost_by_symbol[fill.symbol] = (
            cost_by_symbol.get(fill.symbol, Decimal("0")) + fill.total_cost
        )
    return {
        "fills_by_status": dict(sorted(by_status.items())),
        "filled_notional_by_symbol": dict(sorted(filled_notional_by_symbol.items())),
        "fee_by_symbol": dict(sorted(fee_by_symbol.items())),
        "cost_by_symbol": dict(sorted(cost_by_symbol.items())),
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
