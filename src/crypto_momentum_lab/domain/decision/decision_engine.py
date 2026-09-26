"""Pure DecisionEngine domain service.

Obeys Astra Architecture Blueprint 2026-09-25:
- decide(DecisionInput, PolicyState, EffectivePolicy) -> DecisionResult
- Pure function: no DB, no network, no implicit now(), no random IDs, no os.environ.
- Explicit ClockEvent controls time.
- Deterministic decision_id derived from input hash.
- Pointers to MarketRevisionRef and PositionView version.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from crypto_momentum_lab.domain.decision.decision_frame import (
    ClockEvent,
    DecisionFrame,
)
from crypto_momentum_lab.domain.decision.policy_transition import (
    PolicyTransition,
    StrategyPositionMode,
    execute_policy_transition,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    PositionHealthStatus,
    PositionView,
)
from crypto_momentum_lab.domain.execution.trade_command import TradeCommand
from crypto_momentum_lab.domain.market.market_book import compute_market_state_hash
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.market.revision_models import (
    MarketEnvelope,
    MarketRevisionRef,
    MarketVisibilityMode,
)
from crypto_momentum_lab.domain.strategy.models import (
    EntryType,
    OrderIntentCandidate,
    RejectionReason,
    StrategyDecision,
    StrategyRejection,
)
from crypto_momentum_lab.domain.strategy.position_exit import PositionExitPolicy


def _require_aware(dt: datetime, name: str) -> datetime:
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware (UTC)")
    return dt.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class DecisionInput:
    """Immutable input contract for a single strategy evaluation tick."""

    symbol: str
    market_ref: MarketRevisionRef
    market_envelope: MarketEnvelope
    position_view: PositionView
    universe_version: str
    clock_event: ClockEvent
    cash_balance: Decimal
    risk_config_version: str
    frame: DecisionFrame | None = None

    def __post_init__(self) -> None:
        if not self.symbol.strip():
            raise ValueError("symbol must not be empty")
        if self.market_ref.symbol != self.symbol:
            raise ValueError(
                f"Market ref symbol {self.market_ref.symbol} != {self.symbol}"
            )
        if self.market_envelope.ref != self.market_ref:
            raise ValueError(
                f"market_envelope.ref ({self.market_envelope.ref.revision_id}) "
                "must match market_ref "
                f"({self.market_ref.revision_id})"
            )
        computed_hash = compute_market_state_hash(self.market_envelope.state)
        if computed_hash != self.market_ref.content_hash:
            raise ValueError(
                f"market_envelope state content hash ({computed_hash}) does not match "
                f"market_ref.content_hash ({self.market_ref.content_hash})"
            )
        if self.market_envelope.state.symbol != self.symbol:
            raise ValueError(
                f"market_envelope state symbol "
                f"{self.market_envelope.state.symbol} != {self.symbol}"
            )
        if self.position_view.key.symbol != self.symbol:
            raise ValueError(
                f"Position view symbol {self.position_view.key.symbol} != {self.symbol}"
            )
        if self.cash_balance < Decimal("0"):
            raise ValueError("cash_balance must not be negative")

    @property
    def frame_digest(self) -> str:
        return self.frame.frame_digest if self.frame is not None else ""


@dataclass(frozen=True, slots=True)
class PolicyState:
    """Versioned, mutable-free strategy state preserving cooldowns and anchors."""

    policy_version: int = 1
    cooldown_until_by_symbol: dict[str, datetime] = field(default_factory=dict)
    anchor_prices_by_symbol: dict[str, Decimal] = field(default_factory=dict)
    active_intent_ids_by_symbol: dict[str, str] = field(default_factory=dict)
    custom_state: dict[str, Any] = field(default_factory=dict)
    signal_memory: dict[str, Any] = field(default_factory=dict)
    warmup_status: dict[str, bool] = field(default_factory=dict)
    grace_until_by_symbol: dict[str, datetime] = field(default_factory=dict)
    holding_deadline_by_symbol: dict[str, datetime] = field(default_factory=dict)
    sizing_state_by_symbol: dict[str, Any] = field(default_factory=dict)

    def is_in_cooldown(self, symbol: str, current_time: datetime) -> bool:
        until = self.cooldown_until_by_symbol.get(symbol)
        return until is not None and current_time < until

    def with_cooldown(self, symbol: str, until: datetime) -> PolicyState:
        new_cd = dict(self.cooldown_until_by_symbol)
        new_cd[symbol] = until
        return PolicyState(
            policy_version=self.policy_version + 1,
            cooldown_until_by_symbol=new_cd,
            anchor_prices_by_symbol=dict(self.anchor_prices_by_symbol),
            active_intent_ids_by_symbol=dict(self.active_intent_ids_by_symbol),
            custom_state=dict(self.custom_state),
            signal_memory=dict(self.signal_memory),
            warmup_status=dict(self.warmup_status),
            grace_until_by_symbol=dict(self.grace_until_by_symbol),
            holding_deadline_by_symbol=dict(self.holding_deadline_by_symbol),
            sizing_state_by_symbol=dict(self.sizing_state_by_symbol),
        )

    def with_anchor_and_intent(
        self, symbol: str, anchor_price: Decimal, intent_id: str
    ) -> PolicyState:
        new_anchors = dict(self.anchor_prices_by_symbol)
        new_anchors[symbol] = anchor_price
        new_intents = dict(self.active_intent_ids_by_symbol)
        new_intents[symbol] = intent_id
        return PolicyState(
            policy_version=self.policy_version + 1,
            cooldown_until_by_symbol=dict(self.cooldown_until_by_symbol),
            anchor_prices_by_symbol=new_anchors,
            active_intent_ids_by_symbol=new_intents,
            custom_state=dict(self.custom_state),
            signal_memory=dict(self.signal_memory),
            warmup_status=dict(self.warmup_status),
            grace_until_by_symbol=dict(self.grace_until_by_symbol),
            holding_deadline_by_symbol=dict(self.holding_deadline_by_symbol),
            sizing_state_by_symbol=dict(self.sizing_state_by_symbol),
        )

    def with_holding_deadline(self, symbol: str, deadline: datetime) -> PolicyState:
        new_deadline = dict(self.holding_deadline_by_symbol)
        new_deadline[symbol] = deadline
        return PolicyState(
            policy_version=self.policy_version + 1,
            cooldown_until_by_symbol=dict(self.cooldown_until_by_symbol),
            anchor_prices_by_symbol=dict(self.anchor_prices_by_symbol),
            active_intent_ids_by_symbol=dict(self.active_intent_ids_by_symbol),
            custom_state=dict(self.custom_state),
            signal_memory=dict(self.signal_memory),
            warmup_status=dict(self.warmup_status),
            grace_until_by_symbol=dict(self.grace_until_by_symbol),
            holding_deadline_by_symbol=new_deadline,
            sizing_state_by_symbol=dict(self.sizing_state_by_symbol),
        )

    def with_signal_memory(self, key: str, value: Any) -> PolicyState:
        new_mem = dict(self.signal_memory)
        new_mem[key] = value
        return PolicyState(
            policy_version=self.policy_version + 1,
            cooldown_until_by_symbol=dict(self.cooldown_until_by_symbol),
            anchor_prices_by_symbol=dict(self.anchor_prices_by_symbol),
            active_intent_ids_by_symbol=dict(self.active_intent_ids_by_symbol),
            custom_state=dict(self.custom_state),
            signal_memory=new_mem,
            warmup_status=dict(self.warmup_status),
            grace_until_by_symbol=dict(self.grace_until_by_symbol),
            holding_deadline_by_symbol=dict(self.holding_deadline_by_symbol),
            sizing_state_by_symbol=dict(self.sizing_state_by_symbol),
        )

    def with_cleared_symbol(self, symbol: str) -> PolicyState:
        new_cd = dict(self.cooldown_until_by_symbol)
        new_cd.pop(symbol, None)
        new_anchors = dict(self.anchor_prices_by_symbol)
        new_anchors.pop(symbol, None)
        new_intents = dict(self.active_intent_ids_by_symbol)
        new_intents.pop(symbol, None)
        new_grace = dict(self.grace_until_by_symbol)
        new_grace.pop(symbol, None)
        new_deadline = dict(self.holding_deadline_by_symbol)
        new_deadline.pop(symbol, None)
        new_sizing = dict(self.sizing_state_by_symbol)
        new_sizing.pop(symbol, None)
        return PolicyState(
            policy_version=self.policy_version + 1,
            cooldown_until_by_symbol=new_cd,
            anchor_prices_by_symbol=new_anchors,
            active_intent_ids_by_symbol=new_intents,
            custom_state=dict(self.custom_state),
            signal_memory=dict(self.signal_memory),
            warmup_status=dict(self.warmup_status),
            grace_until_by_symbol=new_grace,
            holding_deadline_by_symbol=new_deadline,
            sizing_state_by_symbol=new_sizing,
        )


@dataclass(frozen=True, slots=True)
class EffectivePolicy:
    """Immutable parameters governing entry, exit, and sizing rules."""

    policy_id: str
    strategy_name: str
    policy_version: int = 1
    entry_threshold: Decimal = Decimal("65000.00")
    order_type: EntryType = EntryType.MARKET
    target_notional: Decimal = Decimal("500.00")
    max_open_positions: int = 4
    exit_policy: PositionExitPolicy = field(default_factory=PositionExitPolicy)
    cooldown_duration: timedelta = timedelta(minutes=15)
    position_mode: StrategyPositionMode = StrategyPositionMode.LONG_ONLY
    grace_period: timedelta = timedelta(0)
    candidate_generator: Any | None = None


@dataclass(frozen=True, slots=True)
class DecisionResult:
    """Deterministic outcome of strategy decision evaluation."""

    decision_id: str
    input_hash: str
    intent: OrderIntentCandidate | None
    exit_command: TradeCommand | None
    next_policy_state: PolicyState
    rejection_reason: str | None
    evaluated_at: datetime
    frame_digest: str = ""
    transition: PolicyTransition | None = None


def compute_decision_input_hash(
    decision_input: DecisionInput,
    policy: EffectivePolicy,
    state: PolicyState,
) -> str:
    """Calculates a deterministic cryptographic hash of all decision inputs."""
    payload = {
        "symbol": decision_input.symbol,
        "market_revision_id": decision_input.market_ref.revision_id,
        "market_content_hash": decision_input.market_ref.content_hash,
        "position_version": decision_input.position_view.projection_version,
        "position_quantity": str(decision_input.position_view.total_quantity),
        "universe_version": decision_input.universe_version,
        "clock_time": decision_input.clock_event.timestamp.isoformat(),
        "clock_sequence": decision_input.clock_event.sequence,
        "cash_balance": str(decision_input.cash_balance),
        "risk_config_version": decision_input.risk_config_version,
        "policy_id": policy.policy_id,
        "policy_version": policy.policy_version,
        "policy_state_version": state.policy_version,
    }
    if decision_input.frame is not None:
        payload["frame_digest"] = decision_input.frame.frame_digest
    dumped = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(dumped.encode("utf-8")).hexdigest()


def decide(
    decision_input: DecisionInput,
    state: PolicyState,
    policy: EffectivePolicy,
) -> DecisionResult:
    """Pure strategy decision function.

    Guarantees:
    - Zero side effects: no I/O, no DB, no network, no datetime.now();
    - Fully reproducible from frozen DecisionInput;
    - Returns updated PolicyState and deterministic decision ID;
    - Executes authoritative state transition via execute_policy_transition.
    """
    frame = decision_input.frame
    if frame is None:
        clock_event = decision_input.clock_event
        frame = DecisionFrame(
            scope="decision",
            symbol=decision_input.symbol,
            market_refs=(decision_input.market_ref,),
            position_view_token=decision_input.position_view.projection_version,
            universe_version=decision_input.universe_version,
            risk_config_version=decision_input.risk_config_version,
            policy_code_digest=f"policy_{state.policy_version}",
            policy_parameters_digest=f"param_{state.policy_version}",
            policy_state_digest=f"state_{state.policy_version}",
            risk_plan_digest=decision_input.risk_config_version,
            clock_event=clock_event,
            cash_balance=decision_input.cash_balance,
        )

    transition = execute_policy_transition(
        frame=frame,
        prior_state=state,
        policy_artifact=policy,
        market_envelope=decision_input.market_envelope,
        position_view=decision_input.position_view,
        decision_input=decision_input,
    )

    return DecisionResult(
        decision_id=transition.decision_id,
        input_hash=transition.input_hash,
        intent=transition.entry_candidate,
        exit_command=transition.exit_command,
        next_policy_state=transition.next_state,
        rejection_reason=transition.rejection_reason,
        evaluated_at=transition.transition_time,
        frame_digest=transition.frame_digest,
        transition=transition,
    )


class DecisionEngine:
    """Authoritative pure domain DecisionEngine service."""

    @staticmethod
    def evaluate(
        decision_input: DecisionInput,
        state: PolicyState,
        policy: EffectivePolicy,
    ) -> DecisionResult:
        """Evaluates pure strategy decision."""
        return decide(decision_input, state, policy)


@dataclass(frozen=True, slots=True)
class FrozenDecisionInputs:
    """Real, version-pinned facts a live decision must be evaluated against.

    Callers supply these from the shared frozen input set (position
    projection, cash, policy state). The filter never invents them.
    """

    position_view: PositionView
    cash_balance: Decimal
    policy_state: PolicyState
    universe_version: str
    risk_config_version: str

    def __post_init__(self) -> None:
        if self.cash_balance < Decimal("0"):
            raise ValueError("cash_balance must not be negative")
        if not self.universe_version.strip():
            raise ValueError("universe_version must not be empty")
        if not self.risk_config_version.strip():
            raise ValueError("risk_config_version must not be empty")


def build_decision_input(
    *,
    state: MarketState15s,
    frozen: FrozenDecisionInputs,
    clock_sequence: int = 1,
    scope: str = "decision",
    source_epoch: str = "ep_decision",
    market_ref: MarketRevisionRef | None = None,
    market_envelope: MarketEnvelope | None = None,
    frame: DecisionFrame | None = None,
) -> DecisionInput:
    """Shared DecisionInput assembly for live, paper, and research paths.

    Every runner freezes the same kind of facts; only the epoch/scope
    labels differ. Do not reassemble DecisionInput ad hoc in runners.
    """
    if market_ref is None:
        pub_time = state.last_received_at or state.bucket_end
        content_hash = compute_market_state_hash(state)
        b_epoch = int(state.bucket_start.timestamp())
        market_ref = MarketRevisionRef(
            scope=scope,
            symbol=state.symbol,
            interval="15s",
            bucket_start=state.bucket_start,
            bucket_end=state.bucket_end,
            revision_id=f"{scope}:{state.symbol}:15s:{b_epoch}:{content_hash[:10]}",
            content_hash=content_hash,
            published_at=pub_time,
            source_epoch=source_epoch,
            visibility_mode=MarketVisibilityMode.DECISION_VISIBLE,
            observed_at=state.first_received_at or pub_time,
        )

    if market_envelope is None:
        market_envelope = MarketEnvelope(ref=market_ref, state=state)

    clock_event = ClockEvent(timestamp=state.bucket_end, sequence=clock_sequence)
    if frame is None:
        frame = DecisionFrame(
            scope=scope,
            symbol=state.symbol,
            market_refs=(market_ref,),
            position_view_token=frozen.position_view.projection_version,
            universe_version=frozen.universe_version,
            risk_config_version=frozen.risk_config_version,
            policy_code_digest=f"policy_{frozen.policy_state.policy_version}",
            policy_parameters_digest=f"param_{frozen.policy_state.policy_version}",
            policy_state_digest=f"state_{frozen.policy_state.policy_version}",
            risk_plan_digest=frozen.risk_config_version,
            clock_event=clock_event,
            cash_balance=frozen.cash_balance,
        )

    return DecisionInput(
        symbol=state.symbol,
        market_ref=market_ref,
        market_envelope=market_envelope,
        position_view=frozen.position_view,
        universe_version=frozen.universe_version,
        clock_event=clock_event,
        cash_balance=frozen.cash_balance,
        risk_config_version=frozen.risk_config_version,
        frame=frame,
    )


def map_decision_rejection_reason(raw_reason: str | None) -> str:
    """Canonical rejection reason label shared by live and paper runners."""
    if raw_reason == "cooldown_active":
        return "COOLDOWN_ACTIVE"
    if raw_reason == "holding_position_no_exit":
        return "HOLDING_POSITION"
    if raw_reason == "below_entry_threshold":
        return "BELOW_ENTRY_THRESHOLD"
    return "NO_SIGNAL"


def create_authoritative_decision_filter(
    strategy_name: str,
    target_notional: Decimal | None = None,
    fact_provider: Callable[[MarketState15s], FrozenDecisionInputs | None]
    | None = None,
    on_decision_result: Callable[[DecisionResult], None] | None = None,
) -> Callable[[StrategyDecision, MarketState15s], StrategyDecision]:
    """Authoritative decision filter wrapping DecisionEngine for runtime loops.

    Refuses to evaluate against synthetic facts. Without a fact provider, or
    when the frozen position is not READY, candidates are rejected with an
    explicit reason instead of being approved on an empty READY view.
    """
    engine = DecisionEngine()
    notional = target_notional or Decimal("500.00")

    def _reject_all(
        decision: StrategyDecision,
        state: MarketState15s,
        reason: str,
    ) -> StrategyDecision:
        details = {
            "raw_reason": reason,
            "strategy_name": strategy_name,
        }
        new_rejections = list(decision.rejections)
        for cand in decision.candidates:
            new_rejections.append(
                StrategyRejection(
                    reason=RejectionReason.NO_SIGNAL,
                    symbol=state.symbol,
                    bucket_start=state.bucket_start,
                    details={**details, "candidate_id": cand.candidate_id},
                )
            )
        return StrategyDecision(
            signals=decision.signals,
            candidates=(),
            rejections=tuple(new_rejections),
            checkpoint=decision.checkpoint,
        )

    def _filter(
        decision: StrategyDecision, state: MarketState15s
    ) -> StrategyDecision:
        if not decision.candidates:
            return decision

        if fact_provider is None:
            return _reject_all(
                decision, state, "missing_frozen_decision_inputs"
            )
        frozen = fact_provider(state)
        if frozen is None:
            return _reject_all(
                decision, state, "frozen_decision_inputs_unavailable"
            )
        pos_view = frozen.position_view
        if pos_view.key.symbol != state.symbol:
            return _reject_all(
                decision, state, "frozen_inputs_symbol_mismatch"
            )
        if pos_view.health_status != PositionHealthStatus.READY:
            return _reject_all(
                decision,
                state,
                f"position_health_{pos_view.health_status.value.lower()}",
            )
        if not pos_view.is_ready_for_trade:
            return _reject_all(
                decision,
                state,
                "position_not_ready_for_trade",
            )

        dec_input = build_decision_input(
            state=state,
            frozen=frozen,
            clock_sequence=1,
            scope="decision",
            source_epoch="ep_decision",
        )

        filtered_candidates: list[OrderIntentCandidate] = []
        new_rejections: list[StrategyRejection] = list(decision.rejections)

        for cand in decision.candidates:
            policy = EffectivePolicy(
                policy_id=f"policy_{strategy_name}",
                strategy_name=strategy_name,
                target_notional=notional,
                candidate_generator=lambda inp, st, _c=cand: _c,
            )
            # Shared starting PolicyState — never a fresh empty state.
            dec_res = engine.evaluate(
                dec_input, frozen.policy_state, policy
            )
            if on_decision_result is not None:
                on_decision_result(dec_res)
            if dec_res.intent is not None:
                filtered_candidates.append(cand)
            else:
                raw_reason = dec_res.rejection_reason or (
                    "decision_engine_filtered"
                )
                rej_reason = (
                    RejectionReason.COOLDOWN_ACTIVE
                    if raw_reason == "cooldown_active"
                    else (
                        RejectionReason.HOLDING_POSITION
                        if raw_reason == "holding_position_no_exit"
                        else (
                            RejectionReason.BELOW_ENTRY_THRESHOLD
                            if raw_reason == "below_entry_threshold"
                            else RejectionReason.NO_SIGNAL
                        )
                    )
                )
                new_rejections.append(
                    StrategyRejection(
                        reason=rej_reason,
                        symbol=state.symbol,
                        bucket_start=state.bucket_start,
                        details={
                            "decision_id": dec_res.decision_id,
                            "raw_reason": raw_reason,
                            "candidate_id": cand.candidate_id,
                            "policy_state_version": (
                                frozen.policy_state.policy_version
                            ),
                        },
                    )
                )

        return StrategyDecision(
            signals=decision.signals,
            candidates=tuple(filtered_candidates),
            rejections=tuple(new_rejections),
            checkpoint=decision.checkpoint,
        )

    return _filter
