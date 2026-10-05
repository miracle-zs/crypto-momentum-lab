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
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol

from crypto_momentum_lab.domain.decision.decision_frame import (
    ClockEvent as ClockEvent,
)
from crypto_momentum_lab.domain.decision.decision_frame import (
    DecisionFrame as DecisionFrame,
)
from crypto_momentum_lab.domain.decision.policy_transition import (
    PolicyTransition,
    StrategyPositionMode,
    canonicalize_policy_value,
    compute_policy_parameters_digest,
    compute_policy_state_digest,
    execute_policy_transition,
    serialize_policy_parameters,
    serialize_policy_state,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    PositionView,
)
from crypto_momentum_lab.domain.execution.trade_command import TradeCommand
from crypto_momentum_lab.domain.market.closed_candle import ClosedCandle15m
from crypto_momentum_lab.domain.market.market_book import compute_market_state_hash
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.market.revision_models import (
    DecisionTrace,
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
    StrategySide,
)
from crypto_momentum_lab.domain.strategy.position_exit import PositionExitPolicy
from crypto_momentum_lab.domain.strategy.sizing import SizingModel, SymbolLotRules

log = logging.getLogger(__name__)


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
    closed_candles: tuple[ClosedCandle15m, ...] = ()

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
        self,
        symbol: str,
        anchor_price: Decimal,
        intent_id: str,
        grace_until: datetime | None = None,
    ) -> PolicyState:
        new_anchors = dict(self.anchor_prices_by_symbol)
        new_anchors[symbol] = anchor_price
        new_intents = dict(self.active_intent_ids_by_symbol)
        new_intents[symbol] = intent_id
        new_grace = dict(self.grace_until_by_symbol)
        if grace_until is not None:
            new_grace[symbol] = grace_until
        return PolicyState(
            policy_version=self.policy_version + 1,
            cooldown_until_by_symbol=dict(self.cooldown_until_by_symbol),
            anchor_prices_by_symbol=new_anchors,
            active_intent_ids_by_symbol=new_intents,
            custom_state=dict(self.custom_state),
            signal_memory=dict(self.signal_memory),
            warmup_status=dict(self.warmup_status),
            grace_until_by_symbol=new_grace,
            holding_deadline_by_symbol=dict(self.holding_deadline_by_symbol),
            sizing_state_by_symbol=dict(self.sizing_state_by_symbol),
        )

    def with_grace_until(self, symbol: str, until: datetime) -> PolicyState:
        new_grace = dict(self.grace_until_by_symbol)
        new_grace[symbol] = until
        return PolicyState(
            policy_version=self.policy_version + 1,
            cooldown_until_by_symbol=dict(self.cooldown_until_by_symbol),
            anchor_prices_by_symbol=dict(self.anchor_prices_by_symbol),
            active_intent_ids_by_symbol=dict(self.active_intent_ids_by_symbol),
            custom_state=dict(self.custom_state),
            signal_memory=dict(self.signal_memory),
            warmup_status=dict(self.warmup_status),
            grace_until_by_symbol=new_grace,
            holding_deadline_by_symbol=dict(self.holding_deadline_by_symbol),
            sizing_state_by_symbol=dict(self.sizing_state_by_symbol),
        )

    def with_exit(self, symbol: str, cooldown_until: datetime) -> PolicyState:
        """Atomically set cooldown and clear symbol-scoped position state."""
        new_cd = dict(self.cooldown_until_by_symbol)
        new_cd[symbol] = cooldown_until
        new_anchors = dict(self.anchor_prices_by_symbol)
        new_anchors.pop(symbol, None)
        new_intents = dict(self.active_intent_ids_by_symbol)
        new_intents.pop(symbol, None)
        new_grace = dict(self.grace_until_by_symbol)
        new_grace.pop(symbol, None)
        new_deadlines = dict(self.holding_deadline_by_symbol)
        new_deadlines.pop(symbol, None)
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
            holding_deadline_by_symbol=new_deadlines,
            sizing_state_by_symbol=new_sizing,
        )

    def reset_for_split(self, carry_sizing: bool = False) -> PolicyState:
        """Clean slate reset across walk-forward train/eval split boundaries."""
        return PolicyState(
            policy_version=self.policy_version + 1,
            cooldown_until_by_symbol={},
            anchor_prices_by_symbol={},
            active_intent_ids_by_symbol={},
            custom_state={},
            signal_memory={},
            warmup_status={},
            grace_until_by_symbol={},
            holding_deadline_by_symbol={},
            sizing_state_by_symbol=(
                dict(self.sizing_state_by_symbol) if carry_sizing else {}
            ),
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

    def with_sizing_state(self, symbol: str, sizing_state: Any) -> PolicyState:
        new_sizing = dict(self.sizing_state_by_symbol)
        new_sizing[symbol] = sizing_state
        return PolicyState(
            policy_version=self.policy_version + 1,
            cooldown_until_by_symbol=dict(self.cooldown_until_by_symbol),
            anchor_prices_by_symbol=dict(self.anchor_prices_by_symbol),
            active_intent_ids_by_symbol=dict(self.active_intent_ids_by_symbol),
            custom_state=dict(self.custom_state),
            signal_memory=dict(self.signal_memory),
            warmup_status=dict(self.warmup_status),
            grace_until_by_symbol=dict(self.grace_until_by_symbol),
            holding_deadline_by_symbol=dict(self.holding_deadline_by_symbol),
            sizing_state_by_symbol=new_sizing,
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
    entry_threshold: Decimal | None = None
    short_entry_threshold: Decimal | None = None
    order_type: EntryType = EntryType.MARKET
    target_notional: Decimal = Decimal("100.00")
    max_open_positions: int | None = None
    exit_policy: PositionExitPolicy = field(default_factory=PositionExitPolicy)
    cooldown_duration: timedelta = timedelta(minutes=15)
    position_mode: StrategyPositionMode = StrategyPositionMode.LONG_ONLY
    grace_period: timedelta = timedelta(0)
    sizing_model: SizingModel | None = None
    symbol_lot_rules: SymbolLotRules | None = None
    candidate_generator: (
        Callable[
            [DecisionInput | MarketEnvelope, PolicyState], OrderIntentCandidate | None
        ]
        | None
    ) = None


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
    decision_frame: DecisionFrame | None = None


def compute_decision_input_hash(
    decision_input: DecisionInput,
    policy: EffectivePolicy,
    state: PolicyState,
) -> str:
    """Calculates a deterministic cryptographic hash of all decision inputs.

    Covers symbol, market revision identity and content, position scope, batches,
    universe, clock, risk parameters, full policy parameters, and complete policy state.
    """
    pos_key = decision_input.position_view.key
    batches_summary = []
    for batch in decision_input.position_view.batches:
        batches_summary.append(
            (
                batch.batch_id,
                batch.episode_id,
                str(batch.original_quantity),
                str(batch.quantity),
                str(batch.entry_price),
                batch.opened_at.isoformat(),
            )
        )
    payload: dict[str, Any] = {
        "symbol": decision_input.symbol,
        "market_revision_id": decision_input.market_ref.revision_id,
        "market_content_hash": decision_input.market_ref.content_hash,
        "position_scope": {
            "environment": pos_key.environment,
            "account_label": pos_key.account_label,
            "position_side": pos_key.position_side.value,
        },
        "position_version": decision_input.position_view.projection_version,
        "position_quantity": str(decision_input.position_view.total_quantity),
        "position_health": decision_input.position_view.health_status.value,
        "position_batches": sorted(batches_summary),
        "universe_version": decision_input.universe_version,
        "clock_time": decision_input.clock_event.timestamp.isoformat(),
        "clock_sequence": decision_input.clock_event.sequence,
        "cash_balance": str(decision_input.cash_balance),
        "risk_config_version": decision_input.risk_config_version,
        "policy_parameters": serialize_policy_parameters(policy),
        "policy_state": serialize_policy_state(state),
    }
    if decision_input.frame is not None:
        payload["frame_digest"] = decision_input.frame.frame_digest
    dumped = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(dumped.encode("utf-8")).hexdigest()


def decide(
    decision_input: DecisionInput,
    state: PolicyState,
    policy: EffectivePolicy,
    closed_candles: tuple[ClosedCandle15m, ...] | None = None,
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
        scope_to_use = decision_input.market_ref.scope
        if not scope_to_use:
            raise ValueError("Decision frame requires an explicit environment/scope")
        frame = DecisionFrame(
            scope=scope_to_use,
            symbol=decision_input.symbol,
            market_refs=(decision_input.market_ref,),
            position_view_token=decision_input.position_view.projection_version,
            universe_version=decision_input.universe_version,
            risk_config_version=decision_input.risk_config_version,
            policy_code_digest=f"policy_{policy.strategy_name}_{state.policy_version}",
            policy_parameters_digest=compute_policy_parameters_digest(policy),
            policy_state_digest=compute_policy_state_digest(state),
            risk_plan_digest=decision_input.risk_config_version,
            clock_event=clock_event,
            cash_balance=decision_input.cash_balance,
        )

    candles = (
        closed_candles if closed_candles is not None else decision_input.closed_candles
    )
    transition = execute_policy_transition(
        frame=frame,
        prior_state=state,
        policy_artifact=policy,
        market_envelope=decision_input.market_envelope,
        position_view=decision_input.position_view,
        closed_candles=candles,
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
        decision_frame=frame,
    )


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
    scope: str | None = None,
    source_epoch: str | None = None,
    market_ref: MarketRevisionRef | None = None,
    market_envelope: MarketEnvelope | None = None,
    frame: DecisionFrame | None = None,
    policy: EffectivePolicy | None = None,
    closed_candles: tuple[ClosedCandle15m, ...] = (),
) -> DecisionInput:
    """Shared DecisionInput assembly for live, paper, and research paths.

    Every runner freezes the same kind of facts; only the epoch/scope
    labels differ. Do not reassemble DecisionInput ad hoc in runners.
    """
    effective_scope = scope or state.environment
    if not effective_scope:
        raise ValueError(
            "Decision input assembly requires an explicit environment/scope"
        )
    effective_source_epoch = source_epoch or f"seq_{state.trade_count}"

    if market_ref is None:
        pub_time = state.last_received_at or state.bucket_end
        content_hash = compute_market_state_hash(state)
        b_epoch = int(state.bucket_start.timestamp())
        market_ref = MarketRevisionRef(
            scope=effective_scope,
            symbol=state.symbol,
            interval="15s",
            bucket_start=state.bucket_start,
            bucket_end=state.bucket_end,
            revision_id=f"{effective_scope}:{state.symbol}:15s:{b_epoch}:{content_hash[:10]}",
            content_hash=content_hash,
            published_at=pub_time,
            source_epoch=effective_source_epoch,
            visibility_mode=MarketVisibilityMode.DECISION_VISIBLE,
            observed_at=state.first_received_at or pub_time,
        )

    if market_envelope is None:
        market_envelope = MarketEnvelope(ref=market_ref, state=state)

    clock_event = ClockEvent(timestamp=state.bucket_end, sequence=clock_sequence)
    if frame is None:
        p_code = (
            f"policy_{policy.strategy_name}_{frozen.policy_state.policy_version}"
            if policy is not None
            else f"policy_{frozen.policy_state.policy_version}"
        )
        p_params = (
            compute_policy_parameters_digest(policy)
            if policy is not None
            else f"param_{frozen.policy_state.policy_version}"
        )
        frame = DecisionFrame(
            scope=effective_scope,
            symbol=state.symbol,
            market_refs=(market_ref,),
            position_view_token=frozen.position_view.projection_version,
            universe_version=frozen.universe_version,
            risk_config_version=frozen.risk_config_version,
            policy_code_digest=p_code,
            policy_parameters_digest=p_params,
            policy_state_digest=compute_policy_state_digest(frozen.policy_state),
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
        closed_candles=closed_candles,
    )


def decision_trace_from_result(
    result: DecisionResult,
    decision_input: DecisionInput,
    strategy_name: str = "strategy",
    account_label: str = "primary",
    prior_policy_state: PolicyState | None = None,
    policy: EffectivePolicy | None = None,
    input_candidate: OrderIntentCandidate | None = None,
) -> DecisionTrace:
    """Builds an immutable DecisionTrace with semantic outputs and next state."""
    frame = result.decision_frame or decision_input.frame
    payload: dict[str, Any] = {
        "trace_schema_version": 1,
        "input_hash": result.input_hash,
        "frame_digest": result.frame_digest,
        "next_policy_state": serialize_policy_state(result.next_policy_state),
        "clock_event": {
            "timestamp": decision_input.clock_event.timestamp.isoformat(),
            "sequence": decision_input.clock_event.sequence,
            "event_type": decision_input.clock_event.event_type,
        },
    }
    if prior_policy_state is not None:
        payload["prior_policy_state"] = serialize_policy_state(prior_policy_state)
    if policy is not None:
        payload["policy_parameters"] = serialize_policy_parameters(policy)
        if callable(
            policy.candidate_generator
        ) and decision_input.position_view.total_quantity == Decimal("0"):
            payload["candidate_generation_mode"] = "injected_candidate"
            if input_candidate is not None:
                payload["input_candidate"] = _serialize_intent_candidate(
                    input_candidate
                )
        else:
            payload["candidate_generation_mode"] = "policy"

    if frame is not None:
        payload["decision_frame"] = {
            "scope": frame.scope,
            "symbol": frame.symbol,
            "market_revision_ids": [ref.revision_id for ref in frame.market_refs],
            "position_view_token": frame.position_view_token,
            "clock_event": {
                "timestamp": frame.clock_event.timestamp.isoformat(),
                "sequence": frame.clock_event.sequence,
                "event_type": frame.clock_event.event_type,
            },
            "universe_version": frame.universe_version,
            "risk_config_version": frame.risk_config_version,
            "policy_code_digest": frame.policy_code_digest,
            "policy_parameters_digest": frame.policy_parameters_digest,
            "policy_state_digest": frame.policy_state_digest,
            "risk_plan_digest": frame.risk_plan_digest,
            "cash_balance": str(frame.cash_balance),
            "max_clock_skew_seconds": str(frame.max_clock_skew.total_seconds()),
        }

    pv = decision_input.position_view
    batches_data = []
    for batch in pv.batches:
        batches_data.append(
            {
                "batch_id": batch.batch_id,
                "episode_id": batch.episode_id,
                "symbol": pv.key.symbol,
                "side": pv.key.position_side.value,
                "quantity": str(batch.quantity),
                "remaining_quantity": str(batch.quantity),
                "allocated_quantity": str(batch.quantity),
                "original_quantity": str(batch.original_quantity),
                "entry_price": str(batch.entry_price),
                "opened_at": batch.opened_at.isoformat(),
                "exit_order_submitted_at": (
                    batch.exit_order_submitted_at.isoformat()
                    if batch.exit_order_submitted_at is not None
                    else None
                ),
            }
        )

    active_episode = pv.active_episode
    active_episode_data = None
    if active_episode is not None:
        active_episode_data = {
            "episode_id": active_episode.episode_id,
            "side": active_episode.side.value,
            "opened_at": active_episode.opened_at.isoformat(),
            "closed_at": (
                active_episode.closed_at.isoformat()
                if active_episode.closed_at is not None
                else None
            ),
            "is_active": active_episode.is_active,
            "cumulative_bought": str(active_episode.cumulative_bought),
            "cumulative_sold": str(active_episode.cumulative_sold),
            "peak_quantity": str(active_episode.peak_quantity),
        }
    key = pv.key
    payload["decision_context"] = {
        "cash_balance": str(decision_input.cash_balance),
        "universe_version": decision_input.universe_version,
        "risk_config_version": decision_input.risk_config_version,
        "position_view": {
            "position_key": {
                "environment": key.environment,
                "account_label": key.account_label,
                "symbol": key.symbol,
                "position_side": key.position_side.value,
            },
            "symbol": pv.key.symbol,
            "position_side": pv.key.position_side.value,
            "total_quantity": str(pv.total_quantity),
            "unallocated_quantity": str(pv.unallocated_quantity),
            "health_status": pv.health_status.value,
            "projection_version": pv.projection_version,
            "input_revision": pv.input_revision,
            "event_cut": pv.event_cut.isoformat() if pv.event_cut else None,
            "policy_version": pv.policy_version,
            "schema_version": pv.schema_version,
            "reconciliation_gap": (
                str(pv.reconciliation_gap)
                if pv.reconciliation_gap is not None
                else None
            ),
            "is_comparable": pv.is_comparable,
            "active_episode": active_episode_data,
            "batches": batches_data,
        },
    }
    payload["closed_candles"] = [
        {
            "symbol": candle.symbol,
            "candle_start": candle.candle_start.isoformat(),
            "candle_end": candle.candle_end.isoformat(),
            "open_price": str(candle.open_price),
            "close_price": str(candle.close_price),
        }
        for candle in decision_input.closed_candles
    ]

    if decision_input.market_envelope is not None:
        try:
            from crypto_momentum_lab.domain.market.state_codec import (
                market_state_to_payload,
            )

            payload["market_state"] = market_state_to_payload(
                decision_input.market_envelope.state
            )
        except Exception:
            log.exception("decision_trace_market_state_serialization_failed")
    if result.intent is not None:
        payload["output_intent"] = _serialize_intent_candidate(result.intent)
    if result.exit_command is not None:
        payload["output_exit_command"] = canonicalize_policy_value(result.exit_command)
    return DecisionTrace(
        decision_id=result.decision_id,
        strategy_name=strategy_name,
        account_label=account_label,
        decision_time=result.evaluated_at,
        evaluated_market_refs=(
            frame.market_refs if frame is not None else (decision_input.market_ref,)
        ),
        intent_produced=result.intent is not None,
        intent_id=result.intent.candidate_id if result.intent is not None else None,
        rejection_reason=result.rejection_reason,
        input_hash=result.input_hash,
        frame_digest=result.frame_digest,
        trace_payload=payload,
    )


def _serialize_intent_candidate(candidate: OrderIntentCandidate) -> dict[str, Any]:
    notional = candidate.desired_notional
    features = canonicalize_policy_value(candidate.features)
    if isinstance(features, dict) and features.get("sizing_model"):
        for key in (
            "quantized_quantity",
            "lot_remainder",
            "target_notional",
            "actual_notional",
            "step_size",
            "margin_required",
        ):
            value = features.get(key)
            if isinstance(value, str):
                try:
                    features[key] = canonicalize_policy_value(Decimal(value))
                except InvalidOperation:
                    continue
    return {
        "candidate_id": candidate.candidate_id,
        "signal_id": candidate.signal_id,
        "run_id": candidate.run_id,
        "strategy_name": candidate.strategy_name,
        "strategy_version": candidate.strategy_version,
        "config_hash": candidate.config_hash,
        "symbol": candidate.symbol,
        "side": candidate.side.value,
        "entry_type": candidate.entry_type.value,
        "limit_price": (
            canonicalize_policy_value(candidate.limit_price)
            if candidate.limit_price is not None
            else None
        ),
        "desired_notional": (
            canonicalize_policy_value(notional) if notional is not None else None
        ),
        "target_notional": (
            canonicalize_policy_value(notional) if notional is not None else None
        ),
        "reduce_only": candidate.reduce_only,
        "expires_at": candidate.expires_at.isoformat(),
        "created_at": candidate.created_at.isoformat(),
        "reason": candidate.reason,
        "features": features,
    }


class DecisionReplayReceipt(Protocol):
    """Commit result required to suppress repeated decision effects."""

    @property
    def is_replay(self) -> bool: ...


def create_authoritative_async_decision_filter(
    strategy_name: str,
    *,
    fact_provider: Callable[
        [MarketState15s, StrategySide | None], Awaitable[FrozenDecisionInputs | None]
    ],
    durable_decision_commit: Callable[
        [DecisionTrace, DecisionResult, DecisionInput], Awaitable[DecisionReplayReceipt]
    ],
    effective_policy_provider: Callable[[MarketState15s], EffectivePolicy],
    clock_sequence_provider: Callable[[MarketState15s], int] | None = None,
    source_epoch_provider: Callable[[MarketState15s], str] | None = None,
) -> Callable[[StrategyDecision, MarketState15s], Awaitable[StrategyDecision]]:
    """Evaluate each candidate against fresh facts and commit its result directly."""

    def reject(
        decision: StrategyDecision,
        state: MarketState15s,
        reason: str,
    ) -> StrategyDecision:
        return replace(
            decision,
            candidates=(),
            rejections=decision.rejections
            + tuple(
                StrategyRejection(
                    reason=RejectionReason.NO_SIGNAL,
                    symbol=state.symbol,
                    bucket_start=state.bucket_start,
                    details={
                        "raw_reason": reason,
                        "strategy_name": strategy_name,
                        "candidate_id": candidate.candidate_id,
                    },
                )
                for candidate in decision.candidates
            ),
        )

    async def evaluate_one(
        decision: StrategyDecision,
        state: MarketState15s,
        candidate: OrderIntentCandidate | None,
    ) -> StrategyDecision:
        frozen = await fact_provider(
            state, None if candidate is None else candidate.side
        )
        if frozen is None:
            return reject(decision, state, "frozen_decision_inputs_unavailable")
        view = frozen.position_view
        if view.key.symbol != state.symbol:
            return reject(decision, state, "frozen_inputs_symbol_mismatch")
        if candidate is None and view.total_quantity <= 0:
            return decision
        if not state.environment:
            raise ValueError("MarketState15s requires an explicit environment")
        sequence = (
            1 if clock_sequence_provider is None else clock_sequence_provider(state)
        )
        if sequence <= 0:
            raise ValueError("decision clock sequence must be positive")
        epoch = (
            f"ep_{state.environment}"
            if source_epoch_provider is None
            else source_epoch_provider(state)
        )
        if not epoch:
            raise ValueError("decision source epoch must be explicit")
        if candidate is not None:
            candidate = replace(
                candidate,
                features={
                    **candidate.features,
                    "projection_version": view.projection_version,
                    "position_side": view.key.position_side.value,
                },
            )
        policy = replace(
            effective_policy_provider(state),
            candidate_generator=lambda _input, _state: candidate,
        )
        if policy.target_notional <= 0:
            raise ValueError(
                "target_notional must be explicitly configured and positive"
            )
        decision_input = build_decision_input(
            state=state,
            frozen=frozen,
            clock_sequence=sequence,
            scope=state.environment,
            source_epoch=epoch,
            policy=policy,
        )
        result = decide(decision_input, frozen.policy_state, policy)
        trace = decision_trace_from_result(
            result,
            decision_input,
            strategy_name=strategy_name,
            account_label=view.key.account_label,
            prior_policy_state=frozen.policy_state,
            policy=policy,
            input_candidate=candidate,
        )
        receipt = await durable_decision_commit(trace, result, decision_input)
        if candidate is None:
            return decision
        if receipt.is_replay:
            return replace(decision, candidates=())
        if result.intent is not None:
            return replace(decision, candidates=(result.intent,))
        reason = result.rejection_reason or "decision_engine_filtered"
        rejection = StrategyRejection(
            reason={
                "cooldown_active": RejectionReason.COOLDOWN_ACTIVE,
                "holding_position_no_exit": RejectionReason.HOLDING_POSITION,
                "below_entry_threshold": RejectionReason.BELOW_ENTRY_THRESHOLD,
            }.get(reason, RejectionReason.NO_SIGNAL),
            symbol=state.symbol,
            bucket_start=state.bucket_start,
            details={
                "decision_id": result.decision_id,
                "raw_reason": reason,
                "candidate_id": candidate.candidate_id,
                "policy_state_version": frozen.policy_state.policy_version,
            },
        )
        return replace(
            decision, candidates=(), rejections=decision.rejections + (rejection,)
        )

    async def _filter(
        decision: StrategyDecision,
        state: MarketState15s,
    ) -> StrategyDecision:
        if not decision.candidates:
            return await evaluate_one(decision, state, None)

        filtered_candidates: list[OrderIntentCandidate] = []
        rejections: list[StrategyRejection] = list(decision.rejections)
        for candidate in decision.candidates:
            single_candidate = replace(
                decision,
                candidates=(candidate,),
                rejections=(),
            )
            filtered = await evaluate_one(single_candidate, state, candidate)
            filtered_candidates.extend(filtered.candidates)
            rejections.extend(filtered.rejections)
        return StrategyDecision(
            signals=decision.signals,
            candidates=tuple(filtered_candidates),
            rejections=tuple(rejections),
            checkpoint=decision.checkpoint,
        )

    return _filter
