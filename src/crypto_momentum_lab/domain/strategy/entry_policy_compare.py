"""Pure, read-only comparison between a legacy entry rule and the Policy.

The adapter accepts already-loaded values and never executes an order.  Live
and Paper hosts can use the same comparison contract without depending on one
another's runtime modules.
"""

from __future__ import annotations

import hashlib
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from crypto_momentum_lab.domain.strategy.entry_policy import (
    EmaPolicyState,
    EmaSnapshot,
    EntryEligibilityDecision,
    EntryEligibilityPolicy,
    EntryGateResult,
    PolicyInputSnapshot,
    UniverseRankingEntry,
    UniverseRankingSnapshot,
)
from crypto_momentum_lab.domain.strategy.models import (
    OrderIntentCandidate,
    StrategySide,
)


@dataclass(frozen=True, slots=True)
class EntryPolicyComparison:
    """One candidate's legacy and Policy outcomes."""

    source_trace_id: str
    candidate_id: str
    legacy_rejection_reason: str | None
    policy_decision: EntryEligibilityDecision

    def __post_init__(self) -> None:
        if not self.source_trace_id.strip():
            raise ValueError("source_trace_id must not be empty")
        if not self.candidate_id.strip():
            raise ValueError("candidate_id must not be empty")

    @property
    def legacy_eligible(self) -> bool:
        return self.legacy_rejection_reason is None

    @property
    def matched(self) -> bool:
        """Whether both rules agree on whether the candidate is eligible."""

        return self.legacy_eligible == self.policy_decision.eligible

    def as_details(self) -> dict[str, object]:
        """Return a JSON-friendly, bounded telemetry representation."""

        return {
            "source_trace_id": self.source_trace_id,
            "candidate_id": self.candidate_id,
            "legacy_eligible": self.legacy_eligible,
            "legacy_rejection_reason": self.legacy_rejection_reason,
            "policy_eligible": self.policy_decision.eligible,
            "policy_rejection_reasons": list(self.policy_decision.reasons),
            "matched": self.matched,
        }


@dataclass(frozen=True, slots=True)
class EntryPolicyComparisonSummary:
    """Bounded aggregate for one compare-only observation batch.

    The same shape is used by Replay and by the runtime adapters.  Candidate
    IDs and source traces stay in the individual comparisons; this aggregate
    contains only low-cardinality counts and Policy reason labels.
    """

    candidates: int
    matched: int
    mismatched: int
    legacy_eligible: int
    policy_eligible: int
    reduce_only_skipped: int
    policy_reasons: dict[str, int]
    mismatch_reasons: dict[str, int]

    def __post_init__(self) -> None:
        for field_name in (
            "candidates",
            "matched",
            "mismatched",
            "legacy_eligible",
            "policy_eligible",
            "reduce_only_skipped",
        ):
            value = getattr(self, field_name)
            if value < 0:
                raise ValueError(f"{field_name} must not be negative")
        if self.matched + self.mismatched != self.candidates:
            raise ValueError("matched and mismatched must equal candidates")

    def as_summary(self) -> dict[str, int]:
        """Return the Replay-compatible count-only summary."""

        return {
            "candidates": self.candidates,
            "matched": self.matched,
            "mismatched": self.mismatched,
            "legacy_eligible": self.legacy_eligible,
            "policy_eligible": self.policy_eligible,
            "reduce_only_skipped": self.reduce_only_skipped,
        }

    def as_details(self) -> dict[str, object]:
        """Return a JSON-friendly bounded runtime/replay summary."""

        return {
            **self.as_summary(),
            "policy_reasons": dict(self.policy_reasons),
            "mismatch_reasons": dict(self.mismatch_reasons),
        }


@dataclass(frozen=True, slots=True)
class EntryPolicyComparisonRequest:
    """Immutable host-to-Policy comparison input.

    Hosts prepare this request from their own source adapters.  The Policy
    implementation only sees this value and never loads candles, rankings, or
    account state itself.  Keeping the request explicit also gives Replay a
    stable seam for serializing the exact inputs used by a comparison.
    """

    candidate: OrderIntentCandidate
    source_trace_id: str
    legacy_rejection_reason: str | None
    gate_reasons: tuple[str, ...]
    entry_enabled: bool
    entry_long_only: bool
    entry_symbols: frozenset[str] | None
    entry_price: Decimal | None
    ema5: Decimal | None
    ema10: Decimal | None
    require_price_above_ema5: bool
    require_price_above_ema10: bool
    observed_at: datetime
    universe_snapshot: UniverseRankingSnapshot | None = None
    ema_observed_at: datetime | None = None
    ema_snapshot_id: str | None = None
    ema_config_hash: str | None = None

    def __post_init__(self) -> None:
        if not self.source_trace_id.strip():
            raise ValueError("source_trace_id must not be empty")
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        for field_name in (
            "entry_enabled",
            "entry_long_only",
            "require_price_above_ema5",
            "require_price_above_ema10",
        ):
            if not isinstance(getattr(self, field_name), bool):
                raise TypeError(f"{field_name} must be a bool")


def compare_entry_policy_request(
    request: EntryPolicyComparisonRequest,
) -> EntryPolicyComparison:
    """Evaluate one prepared comparison request through the shared Policy."""

    return _compare_entry_candidate(
        request.candidate,
        source_trace_id=request.source_trace_id,
        legacy_rejection_reason=request.legacy_rejection_reason,
        gate_reasons=request.gate_reasons,
        entry_enabled=request.entry_enabled,
        entry_long_only=request.entry_long_only,
        entry_symbols=request.entry_symbols,
        entry_price=request.entry_price,
        ema5=request.ema5,
        ema10=request.ema10,
        require_price_above_ema5=request.require_price_above_ema5,
        require_price_above_ema10=request.require_price_above_ema10,
        observed_at=request.observed_at,
        universe_snapshot=request.universe_snapshot,
        ema_observed_at=request.ema_observed_at,
        ema_snapshot_id=request.ema_snapshot_id,
        ema_config_hash=request.ema_config_hash,
    )


def compare_entry_candidate(
    candidate: OrderIntentCandidate,
    *,
    source_trace_id: str,
    legacy_rejection_reason: str | None,
    gate_reasons: tuple[str, ...],
    entry_enabled: bool,
    entry_long_only: bool,
    entry_symbols: frozenset[str] | None,
    entry_price: Decimal | None,
    ema5: Decimal | None,
    ema10: Decimal | None,
    require_price_above_ema5: bool,
    require_price_above_ema10: bool,
    observed_at: datetime,
    universe_snapshot: UniverseRankingSnapshot | None = None,
    ema_observed_at: datetime | None = None,
    ema_snapshot_id: str | None = None,
    ema_config_hash: str | None = None,
) -> EntryPolicyComparison:
    """Evaluate the new Policy without changing the legacy result.

    ``entry_symbols=None`` represents the legacy *unconfigured* pool and is
    therefore different from an empty, successfully loaded pool.  When an EMA
    filter is enabled, missing snapshot metadata is fail-closed so compare-only
    exposes adapters that have not yet migrated to the immutable input
    contract.
    """

    return _compare_entry_candidate(
        candidate,
        source_trace_id=source_trace_id,
        legacy_rejection_reason=legacy_rejection_reason,
        gate_reasons=gate_reasons,
        entry_enabled=entry_enabled,
        entry_long_only=entry_long_only,
        entry_symbols=entry_symbols,
        entry_price=entry_price,
        ema5=ema5,
        ema10=ema10,
        require_price_above_ema5=require_price_above_ema5,
        require_price_above_ema10=require_price_above_ema10,
        observed_at=observed_at,
        universe_snapshot=universe_snapshot,
        ema_observed_at=ema_observed_at,
        ema_snapshot_id=ema_snapshot_id,
        ema_config_hash=ema_config_hash,
    )


def _compare_entry_candidate(
    candidate: OrderIntentCandidate,
    *,
    source_trace_id: str,
    legacy_rejection_reason: str | None,
    gate_reasons: tuple[str, ...],
    entry_enabled: bool,
    entry_long_only: bool,
    entry_symbols: frozenset[str] | None,
    entry_price: Decimal | None,
    ema5: Decimal | None,
    ema10: Decimal | None,
    require_price_above_ema5: bool,
    require_price_above_ema10: bool,
    observed_at: datetime,
    universe_snapshot: UniverseRankingSnapshot | None = None,
    ema_observed_at: datetime | None = None,
    ema_snapshot_id: str | None = None,
    ema_config_hash: str | None = None,
) -> EntryPolicyComparison:
    """Implementation for both the compatibility and request interfaces."""

    if candidate.reduce_only:
        raise ValueError("entry policy comparison only accepts entry candidates")

    policy_universe_snapshot: UniverseRankingSnapshot | None
    universe_required: bool
    if entry_symbols is None:
        policy_universe_snapshot = None
        universe_required = False
    elif universe_snapshot is None:
        policy_universe_snapshot = universe_snapshot_for_symbols(
            entry_symbols,
            observed_at=observed_at,
        )
        universe_required = True
    else:
        policy_universe_snapshot = universe_snapshot
        universe_required = True

    ema_required = require_price_above_ema5 or require_price_above_ema10
    if not ema_required:
        ema_state = EmaPolicyState.disabled()
    elif entry_price is None or ema_observed_at is None:
        ema_state = EmaPolicyState.unavailable()
    else:
        ema_state = EmaPolicyState.valid(
            EmaSnapshot(
                symbol=candidate.symbol,
                observed_at=ema_observed_at,
                entry_price=entry_price,
                ema5=ema5,
                ema10=ema10,
                snapshot_id=ema_snapshot_id,
                config_hash=ema_config_hash,
            ),
            require_price_above_ema5=require_price_above_ema5,
            require_price_above_ema10=require_price_above_ema10,
        )

    policy_decision = EntryEligibilityPolicy.evaluate(
        PolicyInputSnapshot(
            symbol=candidate.symbol,
            observed_at=observed_at,
            candidate_expiry=candidate.expires_at,
            entry_gate_result=EntryGateResult(
                approved=not gate_reasons,
                reasons=gate_reasons,
            ),
            direction=StrategySide(candidate.side),
            universe_snapshot=policy_universe_snapshot,
            ema_state=ema_state,
            entry_enabled=entry_enabled,
            allow_short=not entry_long_only,
            universe_required=universe_required,
        )
    )
    return EntryPolicyComparison(
        source_trace_id=source_trace_id,
        candidate_id=candidate.candidate_id,
        legacy_rejection_reason=legacy_rejection_reason,
        policy_decision=policy_decision,
    )


def universe_snapshot_for_symbols(
    symbols: frozenset[str],
    *,
    observed_at: datetime,
    snapshot_id: str | None = None,
    config_hash: str | None = None,
) -> UniverseRankingSnapshot:
    normalized_symbols = tuple(sorted(symbol.strip().upper() for symbol in symbols))
    entries = tuple(
        entry
        for rank, symbol in enumerate(normalized_symbols, start=1)
        for entry in (
            UniverseRankingEntry(symbol, rank, StrategySide.LONG),
            UniverseRankingEntry(symbol, rank, StrategySide.SHORT),
        )
    )
    digest = hashlib.sha256(
        "\x1f".join(normalized_symbols).encode("utf-8")
    ).hexdigest()[:16]
    return UniverseRankingSnapshot(
        snapshot_id=snapshot_id or f"legacy-symbol-pool-{digest}",
        observed_at=observed_at,
        entries=entries,
        config_hash=config_hash,
    )


def summarize_entry_policy_comparisons(
    comparisons: Iterable[EntryPolicyComparison],
    *,
    reduce_only_skipped: int = 0,
) -> EntryPolicyComparisonSummary:
    """Aggregate compare-only results without retaining unbounded detail."""

    comparison_tuple = tuple(comparisons)
    mismatches = tuple(
        comparison
        for comparison in comparison_tuple
        if not comparison.matched
    )
    policy_reasons = Counter(
        reason
        for comparison in comparison_tuple
        for reason in comparison.policy_decision.reasons
    )
    mismatch_reasons = Counter(
        reason
        for comparison in mismatches
        for reason in comparison.policy_decision.reasons
    )
    return EntryPolicyComparisonSummary(
        candidates=len(comparison_tuple),
        matched=len(comparison_tuple) - len(mismatches),
        mismatched=len(mismatches),
        legacy_eligible=sum(
            comparison.legacy_eligible for comparison in comparison_tuple
        ),
        policy_eligible=sum(
            comparison.policy_decision.eligible
            for comparison in comparison_tuple
        ),
        reduce_only_skipped=reduce_only_skipped,
        policy_reasons=dict(sorted(policy_reasons.items())),
        mismatch_reasons=dict(sorted(mismatch_reasons.items())),
    )


__all__ = [
    "EntryPolicyComparison",
    "EntryPolicyComparisonRequest",
    "EntryPolicyComparisonSummary",
    "compare_entry_candidate",
    "compare_entry_policy_request",
    "summarize_entry_policy_comparisons",
    "universe_snapshot_for_symbols",
]
