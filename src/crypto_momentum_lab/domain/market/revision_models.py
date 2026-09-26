"""Domain models for market data dual-revision, envelopes, and datasets.

Obeys Astra Architecture Blueprint 2026-09-25:
- RevisionRef separates business bucket time from immutable content revision;
- Distinct visibility modes: decision_visible vs canonical;
- DatasetManifest with explicit interval coverage and holes proof;
- DecisionTrace pinning input revision references.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from crypto_momentum_lab.domain.market.models import MarketState15s


def _require_aware(dt: datetime, name: str) -> datetime:
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware (UTC)")
    return dt.astimezone(UTC)


class MarketVisibilityMode(StrEnum):
    """Visibility mode for market data queries and datasets."""

    DECISION_VISIBLE = "decision_visible"
    CANONICAL = "canonical"


@dataclass(frozen=True, slots=True)
class MarketRevisionRef:
    """Immutable pointer to a specific content revision of a market state."""

    scope: str
    symbol: str
    interval: str
    bucket_start: datetime
    bucket_end: datetime
    revision_id: str
    content_hash: str
    published_at: datetime
    source_epoch: str
    visibility_mode: MarketVisibilityMode
    observed_at: datetime | None = None

    def __post_init__(self) -> None:
        if not self.scope.strip():
            raise ValueError("scope must not be empty")
        if not self.symbol.strip():
            raise ValueError("symbol must not be empty")
        if not self.interval.strip():
            raise ValueError("interval must not be empty")
        object.__setattr__(
            self, "bucket_start", _require_aware(self.bucket_start, "bucket_start")
        )
        object.__setattr__(
            self, "bucket_end", _require_aware(self.bucket_end, "bucket_end")
        )
        object.__setattr__(
            self, "published_at", _require_aware(self.published_at, "published_at")
        )
        if self.observed_at is not None:
            object.__setattr__(
                self, "observed_at", _require_aware(self.observed_at, "observed_at")
            )
        if self.bucket_end <= self.bucket_start:
            raise ValueError("bucket_end must be strictly greater than bucket_start")
        if not self.revision_id.strip():
            raise ValueError("revision_id must not be empty")
        if not self.content_hash.strip():
            raise ValueError("content_hash must not be empty")
        if not self.source_epoch.strip():
            raise ValueError("source_epoch must not be empty")

    @property
    def canonical_bucket_key(self) -> tuple[str, str, str, datetime]:
        """Natural key for the time bucket across revisions."""
        return (self.scope, self.symbol, self.interval, self.bucket_start)


@dataclass(frozen=True, slots=True)
class MarketEnvelope:
    """Immutable envelope enclosing market state with its lineage."""

    ref: MarketRevisionRef
    state: MarketState15s
    lineage: dict[str, Any] = field(default_factory=dict)
    data_complete: bool = True
    missing_count: int = 0

    def __post_init__(self) -> None:
        if self.missing_count < 0:
            raise ValueError("missing_count must be non-negative")
        # Validate ref matches state
        if self.ref.symbol != self.state.symbol:
            raise ValueError(
                f"Envelope ref symbol {self.ref.symbol} != "
                f"state symbol {self.state.symbol}"
            )
        if self.ref.bucket_start != self.state.bucket_start:
            raise ValueError(
                f"Envelope ref bucket_start {self.ref.bucket_start} != "
                f"state bucket_start {self.state.bucket_start}"
            )


@dataclass(frozen=True, slots=True)
class DatasetManifest:
    """Immutable manifest establishing a reproducible dataset of market revisions."""

    manifest_id: str
    scope: str
    symbols: tuple[str, ...]
    interval: str
    start_time: datetime
    end_time: datetime
    visibility_mode: MarketVisibilityMode
    revision_refs: tuple[MarketRevisionRef, ...]
    schema_version: int = 1
    feature_algorithm_version: str = "v1"
    manifest_hash: str = ""
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    coverage_ratio: Decimal = Decimal("1.0")
    holes: tuple[tuple[datetime, datetime], ...] = ()

    def __post_init__(self) -> None:
        if not self.manifest_id.strip():
            raise ValueError("manifest_id must not be empty")
        if not self.scope.strip():
            raise ValueError("scope must not be empty")
        if not self.symbols:
            raise ValueError("symbols must not be empty")
        object.__setattr__(
            self, "start_time", _require_aware(self.start_time, "start_time")
        )
        object.__setattr__(
            self, "end_time", _require_aware(self.end_time, "end_time")
        )
        object.__setattr__(
            self, "created_at", _require_aware(self.created_at, "created_at")
        )
        if self.end_time <= self.start_time:
            raise ValueError("end_time must be greater than start_time")
        if self.coverage_ratio < Decimal("0") or self.coverage_ratio > Decimal("1"):
            raise ValueError("coverage_ratio must be between 0 and 1")
        if not self.manifest_hash:
            object.__setattr__(self, "manifest_hash", self.compute_manifest_hash())

    def compute_manifest_hash(self) -> str:
        payload = {
            "manifest_id": self.manifest_id,
            "scope": self.scope,
            "symbols": sorted(self.symbols),
            "interval": self.interval,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "visibility_mode": self.visibility_mode.value,
            "schema_version": self.schema_version,
            "feature_algorithm_version": self.feature_algorithm_version,
            "coverage_ratio": str(self.coverage_ratio),
            "revision_ids": [r.revision_id for r in self.revision_refs],
            "holes": [
                [h[0].isoformat(), h[1].isoformat()] for h in self.holes
            ],
        }
        dumped = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(dumped.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class RunManifest:
    """Immutable manifest establishing an authoritative strategy run or backtest."""

    run_id: str
    strategy_name: str
    strategy_policy_version: str
    dataset_manifest_id: str | None = None
    policy_code_digest: str = ""
    policy_parameters_digest: str = ""
    risk_plan_digest: str = ""
    initial_equity: Decimal = Decimal("0")
    simulation_model: str = "live"
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    run_manifest_hash: str = ""
    tags: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.run_id.strip():
            raise ValueError("run_id must not be empty")
        if not self.strategy_name.strip():
            raise ValueError("strategy_name must not be empty")
        if not self.strategy_policy_version.strip():
            raise ValueError("strategy_policy_version must not be empty")
        object.__setattr__(
            self, "created_at", _require_aware(self.created_at, "created_at")
        )
        if not self.run_manifest_hash:
            object.__setattr__(self, "run_manifest_hash", self.compute_manifest_hash())

    def compute_manifest_hash(self) -> str:
        payload = {
            "run_id": self.run_id,
            "strategy_name": self.strategy_name,
            "strategy_policy_version": self.strategy_policy_version,
            "dataset_manifest_id": self.dataset_manifest_id,
            "policy_code_digest": self.policy_code_digest,
            "policy_parameters_digest": self.policy_parameters_digest,
            "risk_plan_digest": self.risk_plan_digest,
            "initial_equity": str(self.initial_equity),
            "simulation_model": self.simulation_model,
            "created_at": self.created_at.isoformat(),
            "tags": dict(sorted(self.tags.items())),
        }
        dumped = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(dumped.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class DecisionTrace:
    """Record pinning exact market revision inputs and policy outputs."""

    decision_id: str
    strategy_name: str
    account_label: str
    decision_time: datetime
    evaluated_market_refs: tuple[MarketRevisionRef, ...]
    intent_produced: bool
    intent_id: str | None = None
    rejection_reason: str | None = None
    input_hash: str = ""
    frame_digest: str = ""
    trace_payload: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.decision_id.strip():
            raise ValueError("decision_id must not be empty")
        if not self.strategy_name.strip():
            raise ValueError("strategy_name must not be empty")
        if not self.account_label.strip():
            raise ValueError("account_label must not be empty")
        object.__setattr__(
            self, "decision_time", _require_aware(self.decision_time, "decision_time")
        )
        if not self.evaluated_market_refs:
            raise ValueError("evaluated_market_refs must not be empty")
