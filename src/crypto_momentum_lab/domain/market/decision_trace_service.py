"""DecisionTraceService for reproducible decision auditing and multi-mode replay.

Obeys Astra Architecture Blueprint 2026-09-25:
- DecisionTrace pins exact input revisions;
- Decision-visible replay strictly reproduces original live decision;
- Canonical replay allows counterfactual evaluation with explicit explanations;
- Missing original revision fails closed with UnreproducibleError.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from crypto_momentum_lab.domain.market.market_book import (
    MarketBook,
    RevisionNotFoundError,
    UnreproducibleError,
)
from crypto_momentum_lab.domain.market.revision_models import (
    DecisionTrace,
    MarketEnvelope,
    MarketRevisionRef,
    MarketVisibilityMode,
)
from crypto_momentum_lab.domain.operational.retention_authority import (
    RetentionAuthority,
)
from crypto_momentum_lab.domain.operational.retention_models import RecoverySpec


@dataclass(frozen=True, slots=True)
class ReplayResult:
    """Outcome of replaying a historic strategy decision."""

    decision_id: str
    replay_mode: MarketVisibilityMode
    reproduced: bool
    original_intent_produced: bool
    replayed_intent_produced: bool
    original_rejection_reason: str | None
    replayed_rejection_reason: str | None
    evaluated_revisions: tuple[MarketRevisionRef, ...]
    divergence_explanation: str | None = None
    replayed_at: datetime = field(default_factory=lambda: datetime.now(UTC))


class DecisionTraceService:
    """Service orchestrating immutable decision traces and verifiable replay."""

    def __init__(
        self,
        market_book: MarketBook,
        retention_authority: RetentionAuthority | None = None,
    ) -> None:
        self._book = market_book
        self._authority = retention_authority

    def record_decision(
        self,
        *,
        decision_id: str,
        strategy_name: str,
        account_label: str,
        decision_time: datetime,
        evaluated_market_refs: tuple[MarketRevisionRef, ...],
        intent_produced: bool,
        intent_id: str | None = None,
        rejection_reason: str | None = None,
        trace_payload: dict[str, Any] | None = None,
    ) -> DecisionTrace:
        """Records an immutable DecisionTrace and registers retention dependencies."""
        trace = DecisionTrace(
            decision_id=decision_id,
            strategy_name=strategy_name,
            account_label=account_label,
            decision_time=decision_time,
            evaluated_market_refs=evaluated_market_refs,
            intent_produced=intent_produced,
            intent_id=intent_id,
            rejection_reason=rejection_reason,
            trace_payload=trace_payload or {},
        )
        self._book._repo.save_decision_trace(trace)

        # Register retention dependency for earliest evaluated revision bucket
        if self._authority is not None and evaluated_market_refs:
            earliest_bucket = min(r.bucket_start for r in evaluated_market_refs)
            spec = RecoverySpec(
                source_dataset="market_revisions",
                earliest_needed_watermark=earliest_bucket,
                earliest_checkpoint_id=decision_id,
                cold_recovery_supported=True,
            )
            self._authority.register_dependency(
                consumer_id=f"decision_{decision_id}",
                generation=1,
                recovery_spec=spec,
            )

        return trace

    def replay_decision(
        self,
        decision_id: str,
        *,
        replay_mode: MarketVisibilityMode = MarketVisibilityMode.DECISION_VISIBLE,
        policy_evaluator: Callable[
            [tuple[MarketEnvelope, ...]], tuple[bool, str | None]
        ],
    ) -> ReplayResult:
        """Replays historic decision using visible or canonical revisions.

        Raises:
            UnreproducibleError: If an original revision is missing from storage in
            DECISION_VISIBLE mode.
        """
        trace = self._book._repo.load_decision_trace(decision_id)
        if trace is None:
            raise KeyError(f"DecisionTrace {decision_id} not found")

        revisions_to_use: list[MarketRevisionRef] = []
        envelopes: list[MarketEnvelope] = []

        if replay_mode == MarketVisibilityMode.DECISION_VISIBLE:
            # Strictly load the exact pinned revisions
            for r in trace.evaluated_market_refs:
                try:
                    env = self._book.read(r)
                    revisions_to_use.append(r)
                    envelopes.append(env)
                except RevisionNotFoundError as exc:
                    raise UnreproducibleError(
                        f"Cannot reproduce decision {decision_id}: original revision "
                        f"{r.revision_id} for {r.symbol} is missing from MarketBook!"
                    ) from exc
        else:
            # Canonical counterfactual mode: resolve latest canonical for each bucket
            for r in trace.evaluated_market_refs:
                can_ref = self._book.get_canonical_ref(
                    r.scope, r.symbol, r.interval, r.bucket_start
                )
                ref = can_ref if can_ref is not None else r
                revisions_to_use.append(ref)
                envelopes.append(self._book.read(ref))

        # Evaluate strategy policy on the chosen envelopes
        intent_produced, rejection = policy_evaluator(tuple(envelopes))

        reproduced = (
            intent_produced == trace.intent_produced
            and rejection == trace.rejection_reason
        )

        divergence = None
        if not reproduced:
            divergence = (
                f"Divergence in {replay_mode.value} replay: original intent="
                f"{trace.intent_produced} (reason={trace.rejection_reason}), "
                f"replayed intent={intent_produced} (reason={rejection})"
            )

        return ReplayResult(
            decision_id=decision_id,
            replay_mode=replay_mode,
            reproduced=reproduced,
            original_intent_produced=trace.intent_produced,
            replayed_intent_produced=intent_produced,
            original_rejection_reason=trace.rejection_reason,
            replayed_rejection_reason=rejection,
            evaluated_revisions=tuple(revisions_to_use),
            divergence_explanation=divergence,
        )
