"""Current entry policy evaluation from explicit runtime facts."""

from __future__ import annotations

import hashlib
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
class CandidatePolicyDecision:
    source_trace_id: str
    candidate_id: str
    policy_decision: EntryEligibilityDecision

    def as_details(self) -> dict[str, object]:
        return {
            "source_trace_id": self.source_trace_id,
            "candidate_id": self.candidate_id,
            "policy_eligible": self.policy_decision.eligible,
            "policy_rejection_reasons": list(self.policy_decision.reasons),
        }


def evaluate_entry_candidate(
    candidate: OrderIntentCandidate,
    *,
    source_trace_id: str,
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
) -> CandidatePolicyDecision:
    """Evaluate current entry eligibility from already-loaded facts."""

    if candidate.reduce_only:
        raise ValueError("entry policy only accepts entry candidates")

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
    return CandidatePolicyDecision(
        source_trace_id=source_trace_id,
        candidate_id=candidate.candidate_id,
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
        snapshot_id=snapshot_id or f"symbol-pool-{digest}",
        observed_at=observed_at,
        entries=entries,
        config_hash=config_hash,
    )
