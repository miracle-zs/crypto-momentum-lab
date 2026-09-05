import json
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path
from typing import Any, cast

from crypto_momentum_lab.domain.market.models import JsonValue, MarketState15s
from crypto_momentum_lab.domain.strategy import (
    EntryPolicyComparison,
    EntryPolicyComparisonRequest,
    OrderIntentCandidate,
    RunMode,
    StrategyCheckpoint,
    StrategyRejection,
    StrategyRunIdentity,
    StrategySide,
    StrategySignal,
    UniverseRankingEntry,
    UniverseRankingSnapshot,
    compare_entry_policy_request,
    deterministic_config_hash,
    summarize_entry_policy_comparisons,
)
from crypto_momentum_lab.persistence.parquet import read_market_states_15s_dataset
from crypto_momentum_lab.strategies.compression_breakout import (
    CompressionBreakoutConfig,
    CompressionBreakoutRuntimeConfig,
    CompressionBreakoutRuntimeStrategy,
)
from crypto_momentum_lab.strategy_runner.fills import (
    FillSummaryValue,
    ReplayExecutionConfig,
    SimulatedFill,
    fill_summary,
    simulate_candidate_fills,
)

_COMPARISON_INPUT_FIELDS = frozenset(("schema_version", "requests"))
_COMPARISON_REQUEST_FIELDS = frozenset(
    (
        "candidate_id",
        "source_trace_id",
        "legacy_rejection_reason",
        "gate_reasons",
        "entry_enabled",
        "entry_long_only",
        "entry_symbols",
        "entry_price",
        "ema5",
        "ema10",
        "require_price_above_ema5",
        "require_price_above_ema10",
        "observed_at",
        "universe_snapshot",
        "ema_observed_at",
        "ema_snapshot_id",
        "ema_config_hash",
    )
)
_UNIVERSE_SNAPSHOT_FIELDS = frozenset(
    ("snapshot_id", "observed_at", "config_hash", "entries")
)
_UNIVERSE_ENTRY_FIELDS = frozenset(("symbol", "rank", "direction"))


class ReplayError(RuntimeError):
    pass


class EntryPolicyReplayError(ReplayError):
    """The comparison input does not match the replay candidate set."""


@dataclass(frozen=True, slots=True)
class ReplayConfig:
    strategy_name: str
    run_id: str
    code_commit: str
    generated_at: datetime
    compression_breakout: CompressionBreakoutConfig
    candidate_notional: Decimal | None
    candidate_ttl_buckets: int
    signal_interval_seconds: int = 300
    execution: ReplayExecutionConfig | None = field(
        default_factory=ReplayExecutionConfig
    )
    reset_on_gap: bool = True

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
        if self.signal_interval_seconds <= 0:
            raise ValueError("signal_interval_seconds must be positive")
        if not isinstance(self.reset_on_gap, bool):
            raise TypeError("reset_on_gap must be a bool")


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
    replay_options: dict[str, bool | int] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class EntryPolicyReplayReport:
    """Serializable, read-only comparison results for one replay run."""

    schema_version: int
    generated_at: datetime
    source_run_id: str
    source_paths: tuple[str, ...]
    candidate_count: int
    comparisons: tuple[EntryPolicyComparison, ...]
    summary: dict[str, int]
    policy_reasons: dict[str, int]
    mismatch_reasons: dict[str, int]
    replay_options: dict[str, bool | int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.schema_version <= 0:
            raise ValueError("schema_version must be positive")
        if not _is_aware(self.generated_at):
            raise ValueError("generated_at must be timezone-aware")
        if not self.source_run_id.strip():
            raise ValueError("source_run_id must not be empty")
        if not self.source_paths:
            raise ValueError("source_paths must not be empty")
        if self.candidate_count != len(self.comparisons):
            raise ValueError("candidate_count must match comparisons")
        candidate_ids = tuple(
            comparison.candidate_id for comparison in self.comparisons
        )
        if len(candidate_ids) != len(set(candidate_ids)):
            raise ValueError("comparison candidate IDs must be unique")

    def as_details(self) -> dict[str, object]:
        """Return a bounded JSON-friendly report representation."""

        return {
            "schema_version": self.schema_version,
            "generated_at": self.generated_at.isoformat(),
            "source_run_id": self.source_run_id,
            "source_paths": list(self.source_paths),
            "candidate_count": self.candidate_count,
            "comparisons": [
                comparison.as_details() for comparison in self.comparisons
            ],
            "summary": dict(self.summary),
            "policy_reasons": dict(self.policy_reasons),
            "mismatch_reasons": dict(self.mismatch_reasons),
            "replay_options": dict(self.replay_options),
        }


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
        signal_interval_seconds=config.signal_interval_seconds,
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
    last_processed_at_by_symbol: dict[str, datetime] = {}
    max_gap_seconds = strategy.required_data().max_gap_seconds
    for state in ordered_states:
        last_processed_at = last_processed_at_by_symbol.get(state.symbol)
        if (
            config.reset_on_gap
            and last_processed_at is not None
            and (state.bucket_start - last_processed_at).total_seconds()
            > max_gap_seconds
        ):
            strategy.reset_symbol(state.symbol)
        decision = strategy.on_market_state(state)
        signals.extend(decision.signals)
        candidates.extend(decision.candidates)
        rejections.extend(decision.rejections)
        last_processed_at_by_symbol[state.symbol] = state.bucket_start
    checkpoint = strategy.checkpoint()
    signal_tuple = tuple(signals)
    candidate_tuple = tuple(candidates)
    _validate_unique_ids(signal_tuple, candidate_tuple)
    _validate_candidate_references(signal_tuple, candidate_tuple)
    simulated_fills = simulate_candidate_fills(
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
        fill_summary=fill_summary(simulated_fills),
        replay_options={
            "reset_on_gap": config.reset_on_gap,
            "max_gap_seconds": max_gap_seconds,
        },
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


def build_entry_policy_replay_report(
    *,
    replay_report: StrategyReplayReport,
    requests: Iterable[EntryPolicyComparisonRequest],
) -> EntryPolicyReplayReport:
    """Evaluate prepared Policy inputs against one deterministic replay.

    Every non-reduce-only candidate must have exactly one request.  Refusing
    missing or unknown requests prevents a partial comparison from looking
    like a clean run.  The returned report is independent of the execution
    fills in ``replay_report`` and cannot submit or mutate an order.
    """

    entry_candidates = tuple(
        candidate
        for candidate in replay_report.candidates
        if not candidate.reduce_only
    )

    request_tuple = tuple(requests)
    request_by_id: dict[str, EntryPolicyComparisonRequest] = {}
    for request in request_tuple:
        candidate_id = request.candidate.candidate_id
        if candidate_id in request_by_id:
            raise EntryPolicyReplayError(
                f"duplicate comparison request for candidate {candidate_id}"
            )
        request_by_id[candidate_id] = request

    candidate_by_id = {
        candidate.candidate_id: candidate for candidate in entry_candidates
    }
    unknown_ids = sorted(set(request_by_id) - set(candidate_by_id))
    if unknown_ids:
        raise EntryPolicyReplayError(
            "comparison request references unknown candidate(s): "
            + ", ".join(unknown_ids)
        )
    missing_ids = [
        candidate.candidate_id
        for candidate in entry_candidates
        if candidate.candidate_id not in request_by_id
    ]
    if missing_ids:
        raise EntryPolicyReplayError(
            "comparison request missing candidate(s): " + ", ".join(missing_ids)
        )

    comparisons: list[EntryPolicyComparison] = []
    for candidate in entry_candidates:
        request = request_by_id[candidate.candidate_id]
        if request.candidate != candidate:
            raise EntryPolicyReplayError(
                "comparison request candidate payload mismatch: "
                f"{candidate.candidate_id}"
            )
        try:
            comparisons.append(compare_entry_policy_request(request))
        except (TypeError, ValueError) as error:
            raise EntryPolicyReplayError(
                "invalid Policy input for candidate "
                f"{candidate.candidate_id}: {error}"
            ) from error

    comparison_tuple = tuple(comparisons)
    comparison_summary = summarize_entry_policy_comparisons(
        comparison_tuple,
        reduce_only_skipped=len(replay_report.candidates) - len(entry_candidates),
    )
    return EntryPolicyReplayReport(
        schema_version=1,
        generated_at=replay_report.generated_at,
        source_run_id=replay_report.run.run_id,
        source_paths=replay_report.source_paths,
        candidate_count=len(comparison_tuple),
        comparisons=comparison_tuple,
        summary=comparison_summary.as_summary(),
        policy_reasons=comparison_summary.policy_reasons,
        mismatch_reasons=comparison_summary.mismatch_reasons,
        replay_options=dict(replay_report.replay_options),
    )


def read_entry_policy_comparison_requests(
    *,
    input_path: Path,
    candidates: tuple[OrderIntentCandidate, ...],
) -> tuple[EntryPolicyComparisonRequest, ...]:
    """Read strict, candidate-keyed Policy inputs for a Replay run.

    The file deliberately contains only policy inputs; candidate identity and
    order fields come from the freshly generated replay report.  This keeps a
    stale or edited candidate payload from being silently compared.
    """

    try:
        payload = json.loads(input_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise EntryPolicyReplayError(
            f"unable to read comparison input {input_path}: {error}"
        ) from error
    root = _mapping(payload, "comparison input root")
    _ensure_keys(root, _COMPARISON_INPUT_FIELDS, "comparison input root")
    if root.get("schema_version") != 1:
        raise EntryPolicyReplayError(
            "comparison input schema_version must be 1"
        )
    raw_requests = root.get("requests")
    if not isinstance(raw_requests, list):
        raise EntryPolicyReplayError("comparison input requests must be a list")
    candidates_by_id = {candidate.candidate_id: candidate for candidate in candidates}
    requests: list[EntryPolicyComparisonRequest] = []
    for index, raw_request in enumerate(raw_requests):
        try:
            row = _mapping(raw_request, f"requests[{index}]")
            _ensure_keys(
                row,
                _COMPARISON_REQUEST_FIELDS,
                f"requests[{index}]",
            )
            candidate_id = _required_text(row, "candidate_id")
            candidate = candidates_by_id.get(candidate_id)
            if candidate is None:
                raise EntryPolicyReplayError(
                    f"requests[{index}] references unknown candidate {candidate_id}"
                )
            requests.append(
                EntryPolicyComparisonRequest(
                    candidate=candidate,
                    source_trace_id=_required_text(row, "source_trace_id"),
                    legacy_rejection_reason=_optional_text(
                        row, "legacy_rejection_reason"
                    ),
                    gate_reasons=_text_list(row, "gate_reasons"),
                    entry_enabled=_required_bool(row, "entry_enabled"),
                    entry_long_only=_required_bool(row, "entry_long_only"),
                    entry_symbols=_optional_symbol_set(row, "entry_symbols"),
                    entry_price=_optional_decimal(row, "entry_price"),
                    ema5=_optional_decimal(row, "ema5"),
                    ema10=_optional_decimal(row, "ema10"),
                    require_price_above_ema5=_required_bool(
                        row, "require_price_above_ema5"
                    ),
                    require_price_above_ema10=_required_bool(
                        row, "require_price_above_ema10"
                    ),
                    observed_at=_required_datetime(row, "observed_at"),
                    universe_snapshot=_optional_universe_snapshot(
                        row.get("universe_snapshot"),
                        field_name="universe_snapshot",
                    ),
                    ema_observed_at=_optional_datetime(
                        row, "ema_observed_at"
                    ),
                    ema_snapshot_id=_optional_text(row, "ema_snapshot_id"),
                    ema_config_hash=_optional_text(row, "ema_config_hash"),
                )
            )
        except EntryPolicyReplayError:
            raise
        except (KeyError, TypeError, ValueError) as error:
            raise EntryPolicyReplayError(
                f"invalid comparison input requests[{index}]: {error}"
            ) from error
    return tuple(requests)


def write_entry_policy_replay_report(
    report: EntryPolicyReplayReport,
    output_path: Path,
) -> None:
    """Write a deterministic JSON comparison artifact."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(_jsonable(report.as_details()), indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )


def _mapping(value: object, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise EntryPolicyReplayError(f"{field_name} must be an object")
    return value


def _ensure_keys(
    mapping: Mapping[str, object],
    allowed: frozenset[str],
    field_name: str,
) -> None:
    actual = set(mapping)
    missing = sorted(allowed - actual)
    extra = sorted(actual - allowed)
    if missing or extra:
        details: list[str] = []
        if missing:
            details.append("missing=" + ",".join(missing))
        if extra:
            details.append("unknown=" + ",".join(extra))
        raise ValueError(f"{field_name} keys invalid ({'; '.join(details)})")


def _required_text(mapping: Mapping[str, object], field_name: str) -> str:
    value = mapping[field_name]
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _optional_text(
    mapping: Mapping[str, object],
    field_name: str,
) -> str | None:
    value = mapping[field_name]
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be null or a non-empty string")
    return value


def _required_bool(mapping: Mapping[str, object], field_name: str) -> bool:
    value = mapping[field_name]
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be a boolean")
    return value


def _text_list(mapping: Mapping[str, object], field_name: str) -> tuple[str, ...]:
    value = mapping[field_name]
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    result: list[str] = []
    for index, item in enumerate(value):
        if not isinstance(item, str) or not item.strip():
            raise ValueError(f"{field_name}[{index}] must be a non-empty string")
        result.append(item)
    return tuple(result)


def _optional_symbol_set(
    mapping: Mapping[str, object],
    field_name: str,
) -> frozenset[str] | None:
    value = mapping[field_name]
    if value is None:
        return None
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be null or a list")
    symbols: list[str] = []
    for index, item in enumerate(value):
        if not isinstance(item, str) or not item.strip():
            raise ValueError(f"{field_name}[{index}] must be a non-empty string")
        symbols.append(item)
    return frozenset(symbols)


def _optional_decimal(
    mapping: Mapping[str, object],
    field_name: str,
) -> Decimal | None:
    value = mapping[field_name]
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be null or a decimal string")
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise ValueError(f"{field_name} must be a decimal string") from error
    if not parsed.is_finite():
        raise ValueError(f"{field_name} must be a finite decimal string")
    return parsed


def _required_datetime(mapping: Mapping[str, object], field_name: str) -> datetime:
    value = mapping[field_name]
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be an ISO datetime string")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _optional_datetime(
    mapping: Mapping[str, object],
    field_name: str,
) -> datetime | None:
    value = mapping[field_name]
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be null or an ISO datetime string")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _optional_universe_snapshot(
    value: object,
    *,
    field_name: str,
) -> UniverseRankingSnapshot | None:
    if value is None:
        return None
    mapping = _mapping(value, field_name)
    _ensure_keys(mapping, _UNIVERSE_SNAPSHOT_FIELDS, field_name)
    entries_value = mapping["entries"]
    if not isinstance(entries_value, list):
        raise ValueError(f"{field_name}.entries must be a list")
    entries: list[UniverseRankingEntry] = []
    for index, raw_entry in enumerate(entries_value):
        entry = _mapping(raw_entry, f"{field_name}.entries[{index}]")
        _ensure_keys(
            entry,
            _UNIVERSE_ENTRY_FIELDS,
            f"{field_name}.entries[{index}]",
        )
        direction = _required_text(entry, "direction")
        rank = entry["rank"]
        if not isinstance(rank, int) or isinstance(rank, bool):
            raise ValueError(
                f"{field_name}.entries[{index}].rank must be an integer"
            )
        entries.append(
            UniverseRankingEntry(
                symbol=_required_text(entry, "symbol"),
                rank=rank,
                direction=StrategySide(direction),
            )
        )
    return UniverseRankingSnapshot(
        snapshot_id=_required_text(mapping, "snapshot_id"),
        observed_at=_required_datetime(mapping, "observed_at"),
        entries=tuple(entries),
        config_hash=_optional_text(mapping, "config_hash"),
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
