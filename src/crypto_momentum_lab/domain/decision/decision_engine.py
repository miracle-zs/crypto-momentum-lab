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
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

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
from crypto_momentum_lab.domain.market.revision_models import (
    DecisionTrace,
    MarketEnvelope,
    MarketRevisionRef,
)
from crypto_momentum_lab.domain.strategy.models import (
    EntryType,
    OrderIntentCandidate,
)
from crypto_momentum_lab.domain.strategy.position_exit import PositionExitPolicy
from crypto_momentum_lab.domain.strategy.sizing import SizingModel, SymbolLotRules

log = logging.getLogger(__name__)


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
    max_concurrency_per_symbol: int | None = 1
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

    def __post_init__(self) -> None:
        if self.max_concurrency_per_symbol is not None and (
            type(self.max_concurrency_per_symbol) is not int
            or self.max_concurrency_per_symbol <= 0
        ):
            raise ValueError("max_concurrency_per_symbol must be a positive integer")


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
                batch.entry_order_ids,
                batch.entry_client_order_ids,
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
                "entry_order_ids": list(batch.entry_order_ids),
                "entry_client_order_ids": list(batch.entry_client_order_ids),
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
