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
        input_hash: str = "",
        frame_digest: str = "",
        trace_payload: dict[str, Any] | None = None,
    ) -> DecisionTrace:
        """Records an immutable DecisionTrace and registers retention dependencies."""
        payload = dict(trace_payload or {})
        if frame_digest:
            payload["frame_digest"] = frame_digest
        if input_hash:
            payload["input_hash"] = input_hash
        trace = DecisionTrace(
            decision_id=decision_id,
            strategy_name=strategy_name,
            account_label=account_label,
            decision_time=decision_time,
            evaluated_market_refs=evaluated_market_refs,
            intent_produced=intent_produced,
            intent_id=intent_id,
            rejection_reason=rejection_reason,
            input_hash=input_hash,
            frame_digest=frame_digest,
            trace_payload=payload,
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
            [tuple[MarketEnvelope, ...]], Any
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
        raw_result = policy_evaluator(tuple(envelopes))

        intent_produced: bool
        rejection: str | None
        replayed_notional: str | None = None
        replayed_input_hash: str | None = None
        replayed_next_version: int | None = None

        if hasattr(raw_result, "intent"):  # DecisionResult
            intent_produced = raw_result.intent is not None
            rejection = raw_result.rejection_reason
            if raw_result.intent is not None:
                n = getattr(raw_result.intent, "desired_notional", None)
                if n is None:
                    n = getattr(raw_result.intent, "target_notional", None)
                replayed_notional = str(n) if n is not None else None
            replayed_input_hash = getattr(raw_result, "input_hash", None)
            if hasattr(raw_result, "next_policy_state"):
                replayed_next_version = raw_result.next_policy_state.policy_version
        elif isinstance(raw_result, tuple):
            intent_produced, rejection = raw_result[:2]
            if len(raw_result) > 2:
                replayed_notional = str(raw_result[2])
        else:
            intent_produced = bool(raw_result)
            rejection = None

        divergence: str | None = None
        reproduced = (
            intent_produced == trace.intent_produced
            and rejection == trace.rejection_reason
        )
        if not reproduced:
            divergence = (
                f"Divergence in {replay_mode.value} replay: original intent="
                f"{trace.intent_produced} (reason={trace.rejection_reason}), "
                f"replayed intent={intent_produced} (reason={rejection})"
            )
        elif trace.trace_payload:
            orig_intent = trace.trace_payload.get("output_intent")
            if orig_intent is not None and replayed_notional is not None:
                expected_notional = str(
                    orig_intent.get("desired_notional")
                    or orig_intent.get("target_notional")
                )
                if replayed_notional != expected_notional:
                    reproduced = False
                    divergence = (
                        f"Divergence in {replay_mode.value} replay: "
                        f"target notional mismatch "
                        f"(original={expected_notional}, replayed={replayed_notional})"
                    )
            orig_state = trace.trace_payload.get("next_policy_state")
            if (
                reproduced
                and orig_state is not None
                and replayed_next_version is not None
            ):
                expected_v = orig_state.get("policy_version")
                if expected_v is not None and replayed_next_version != expected_v:
                    reproduced = False
                    divergence = (
                        f"Divergence in {replay_mode.value} replay: "
                        f"next policy version mismatch "
                        f"(original={expected_v}, replayed={replayed_next_version})"
                    )
            if (
                reproduced
                and trace.input_hash
                and replayed_input_hash
                and replayed_input_hash != trace.input_hash
            ):
                reproduced = False
                divergence = (
                    f"Divergence in {replay_mode.value} replay: input hash mismatch "
                    f"(original={trace.input_hash}, replayed={replayed_input_hash})"
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
