import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, cast
from uuid import NAMESPACE_URL, uuid5

from crypto_momentum_lab.domain.market.models import JsonValue


class RunMode(StrEnum):
    REPLAY = "replay"
    PAPER = "paper"
    LIVE = "live"


class StrategySide(StrEnum):
    LONG = "long"
    SHORT = "short"


class EntryType(StrEnum):
    MARKET = "market"


class RejectionReason(StrEnum):
    INSUFFICIENT_WARMUP = "insufficient_warmup"
    MISSING_REQUIRED_PRICE = "missing_required_price"
    MISSING_REQUIRED_FIELD = "missing_required_field"
    COOLDOWN_ACTIVE = "cooldown_active"
    NO_SIGNAL = "no_signal"
    CANDIDATE_EXPIRED = "candidate_expired"


@dataclass(frozen=True, slots=True)
class StrategyMetadata:
    name: str
    version: str

    def __post_init__(self) -> None:
        _require_non_empty(self.name, "strategy name")
        _require_non_empty(self.version, "strategy version")


@dataclass(frozen=True, slots=True)
class StrategyRunIdentity:
    run_id: str
    strategy_name: str
    strategy_version: str
    config_hash: str
    run_mode: RunMode
    code_commit: str
    created_at: datetime
    source_paths: tuple[str, ...]

    def __post_init__(self) -> None:
        _require_non_empty(self.run_id, "run_id")
        _require_non_empty(self.strategy_name, "strategy_name")
        _require_non_empty(self.strategy_version, "strategy_version")
        _require_non_empty(self.config_hash, "config_hash")
        _require_non_empty(self.code_commit, "code_commit")
        if not _is_aware(self.created_at):
            raise ValueError("created_at must be timezone-aware")
        if not self.source_paths:
            raise ValueError("source_paths must not be empty")
        for source_path in self.source_paths:
            _require_non_empty(source_path, "source_path")


@dataclass(frozen=True, slots=True)
class StrategyDataRequirement:
    base_state_interval_seconds: int
    warmup_buckets: int
    required_fields: tuple[str, ...]
    max_gap_seconds: int
    allow_entries_before_warmup: bool

    def __post_init__(self) -> None:
        if self.base_state_interval_seconds <= 0:
            raise ValueError("base_state_interval_seconds must be positive")
        if self.warmup_buckets <= 0:
            raise ValueError("warmup_buckets must be positive")
        if self.max_gap_seconds <= 0:
            raise ValueError("max_gap_seconds must be positive")
        if not self.required_fields:
            raise ValueError("required_fields must not be empty")
        for required_field in self.required_fields:
            _require_non_empty(required_field, "required_field")


@dataclass(frozen=True, slots=True)
class StrategySignal:
    signal_id: str
    run_id: str
    strategy_name: str
    strategy_version: str
    config_hash: str
    symbol: str
    side: StrategySide
    detected_at: datetime
    source_state_at: datetime
    reason: str
    features: dict[str, JsonValue]
    reference_prices: dict[str, JsonValue]

    def __post_init__(self) -> None:
        _require_non_empty(self.signal_id, "signal_id")
        _require_common_record_fields(
            run_id=self.run_id,
            strategy_name=self.strategy_name,
            strategy_version=self.strategy_version,
            config_hash=self.config_hash,
            symbol=self.symbol,
        )
        _require_non_empty(self.reason, "reason")
        if not _is_aware(self.detected_at):
            raise ValueError("detected_at must be timezone-aware")
        if not _is_aware(self.source_state_at):
            raise ValueError("source_state_at must be timezone-aware")
        object.__setattr__(
            self,
            "features",
            _normalize_json_mapping(self.features, "features"),
        )
        object.__setattr__(
            self,
            "reference_prices",
            _normalize_json_mapping(self.reference_prices, "reference_prices"),
        )


@dataclass(frozen=True, slots=True)
class OrderIntentCandidate:
    candidate_id: str
    signal_id: str
    run_id: str
    strategy_name: str
    strategy_version: str
    config_hash: str
    symbol: str
    side: StrategySide
    entry_type: EntryType
    limit_price: Decimal | None
    desired_notional: Decimal | None
    reduce_only: bool
    expires_at: datetime
    created_at: datetime
    reason: str
    features: dict[str, JsonValue]

    def __post_init__(self) -> None:
        _require_non_empty(self.candidate_id, "candidate_id")
        _require_non_empty(self.signal_id, "signal_id")
        _require_common_record_fields(
            run_id=self.run_id,
            strategy_name=self.strategy_name,
            strategy_version=self.strategy_version,
            config_hash=self.config_hash,
            symbol=self.symbol,
        )
        _require_non_empty(self.reason, "reason")
        if not _is_aware(self.created_at):
            raise ValueError("created_at must be timezone-aware")
        if not _is_aware(self.expires_at):
            raise ValueError("expires_at must be timezone-aware")
        if self.expires_at <= self.created_at:
            raise ValueError("expires_at must be after created_at")
        if self.desired_notional is not None and self.desired_notional <= 0:
            raise ValueError("desired_notional must be positive")
        object.__setattr__(
            self,
            "features",
            _normalize_json_mapping(self.features, "features"),
        )


@dataclass(frozen=True, slots=True)
class StrategyRejection:
    reason: RejectionReason
    symbol: str
    bucket_start: datetime
    details: dict[str, JsonValue]

    def __post_init__(self) -> None:
        _require_non_empty(self.symbol, "symbol")
        if not _is_aware(self.bucket_start):
            raise ValueError("bucket_start must be timezone-aware")
        object.__setattr__(
            self,
            "details",
            _normalize_json_mapping(self.details, "details"),
        )


@dataclass(frozen=True, slots=True)
class StrategyCheckpoint:
    last_processed_at_by_symbol: dict[str, datetime]
    warmup_buckets_by_symbol: dict[str, int]
    cooldown_buckets_remaining_by_symbol: dict[str, int]
    payload: dict[str, JsonValue]

    def __post_init__(self) -> None:
        for symbol, processed_at in self.last_processed_at_by_symbol.items():
            _require_non_empty(symbol, "symbol")
            if not _is_aware(processed_at):
                raise ValueError(
                    "last_processed_at_by_symbol values must be timezone-aware"
                )
        _require_non_negative_counts(
            self.warmup_buckets_by_symbol,
            "warmup_buckets_by_symbol",
        )
        _require_non_negative_counts(
            self.cooldown_buckets_remaining_by_symbol,
            "cooldown_buckets_remaining_by_symbol",
        )
        object.__setattr__(
            self,
            "payload",
            _normalize_json_mapping(self.payload, "payload"),
        )
        _ensure_json_normalizable(asdict(self), "checkpoint")


@dataclass(frozen=True, slots=True)
class StrategyDecision:
    signals: tuple[StrategySignal, ...]
    candidates: tuple[OrderIntentCandidate, ...]
    rejections: tuple[StrategyRejection, ...]
    checkpoint: StrategyCheckpoint

    def __post_init__(self) -> None:
        signals_by_id = {signal.signal_id: signal for signal in self.signals}
        for candidate in self.candidates:
            signal = signals_by_id.get(candidate.signal_id)
            if signal is None:
                raise ValueError(
                    "candidate signal_id must reference a decision signal"
                )
            if not _candidate_matches_signal(candidate, signal):
                raise ValueError("candidate must match source signal identity")


def deterministic_config_hash(config: object) -> str:
    normalized = _normalize_json_value(config, canonical_decimals=True)
    encoded = json.dumps(
        normalized,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def deterministic_signal_id(
    *,
    identity: StrategyRunIdentity,
    symbol: str,
    side: StrategySide,
    detected_at: datetime,
    sequence: int,
) -> str:
    if sequence <= 0:
        raise ValueError("sequence must be positive")
    _require_non_empty(symbol, "symbol")
    if not _is_aware(detected_at):
        raise ValueError("detected_at must be timezone-aware")
    namespace_value = "|".join(
        (
            identity.run_id,
            identity.strategy_name,
            symbol,
            side.value,
            detected_at.isoformat(),
            str(sequence),
        )
    )
    return f"sig_{uuid5(NAMESPACE_URL, namespace_value)}"


def deterministic_candidate_id(*, signal_id: str, sequence: int) -> str:
    _require_non_empty(signal_id, "signal_id")
    if sequence <= 0:
        raise ValueError("sequence must be positive")
    return f"cand_{uuid5(NAMESPACE_URL, f'{signal_id}|{sequence}')}"


def _normalize_json_value(
    value: object,
    *,
    canonical_decimals: bool = False,
) -> JsonValue:
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, Decimal):
        if canonical_decimals:
            return format(value.normalize(), "f")
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, float):
        if not math.isfinite(value):
            raise TypeError("JSON numbers must be finite")
        return value
    if value is None or isinstance(value, str | int | bool):
        return value
    if is_dataclass(value) and not isinstance(value, type):
        return _normalize_json_value(
            asdict(cast(Any, value)),
            canonical_decimals=canonical_decimals,
        )
    if isinstance(value, Mapping):
        normalized: dict[str, JsonValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("JSON object keys must be strings")
            normalized[key] = _normalize_json_value(
                item,
                canonical_decimals=canonical_decimals,
            )
        return normalized
    if isinstance(value, list | tuple):
        return [
            _normalize_json_value(
                item,
                canonical_decimals=canonical_decimals,
            )
            for item in value
        ]
    raise TypeError(f"Unsupported JSON value: {type(value).__name__}")


def _ensure_json_normalizable(value: object, field_name: str) -> None:
    try:
        _normalize_json_value(value)
    except TypeError as exc:
        raise TypeError(f"{field_name} must be JSON-normalizable") from exc


def _normalize_json_mapping(value: object, field_name: str) -> dict[str, JsonValue]:
    try:
        normalized = _normalize_json_value(value)
    except TypeError as exc:
        raise TypeError(f"{field_name} must be JSON-normalizable") from exc
    if not isinstance(normalized, dict):
        raise TypeError(f"{field_name} must be JSON-normalizable")
    return normalized


def _candidate_matches_signal(
    candidate: OrderIntentCandidate,
    signal: StrategySignal,
) -> bool:
    return (
        candidate.run_id == signal.run_id
        and candidate.strategy_name == signal.strategy_name
        and candidate.strategy_version == signal.strategy_version
        and candidate.config_hash == signal.config_hash
        and candidate.symbol == signal.symbol
        and candidate.side == signal.side
    )


def _require_common_record_fields(
    *,
    run_id: str,
    strategy_name: str,
    strategy_version: str,
    config_hash: str,
    symbol: str,
) -> None:
    _require_non_empty(run_id, "run_id")
    _require_non_empty(strategy_name, "strategy_name")
    _require_non_empty(strategy_version, "strategy_version")
    _require_non_empty(config_hash, "config_hash")
    _require_non_empty(symbol, "symbol")


def _require_non_negative_counts(
    values: dict[str, int],
    field_name: str,
) -> None:
    for symbol, value in values.items():
        _require_non_empty(symbol, "symbol")
        if value < 0:
            raise ValueError(f"{field_name} values must be non-negative")


def _require_non_empty(value: str, field_name: str) -> None:
    if not value.strip():
        raise ValueError(f"{field_name} must not be empty")


def _is_aware(value: datetime) -> bool:
    return value.tzinfo is not None and value.utcoffset() is not None
