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
    ClockEvent as ClockEvent,
)
from crypto_momentum_lab.domain.decision.decision_frame import (
    DecisionFrame as DecisionFrame,
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
    entry_threshold: Decimal = Decimal("65000.00")
    short_entry_threshold: Decimal | None = None
    order_type: EntryType = EntryType.MARKET
    target_notional: Decimal = Decimal("500.00")
    max_open_positions: int = 4
    exit_policy: PositionExitPolicy = field(default_factory=PositionExitPolicy)
    cooldown_duration: timedelta = timedelta(minutes=15)
    position_mode: StrategyPositionMode = StrategyPositionMode.LONG_ONLY
    grace_period: timedelta = timedelta(0)
    sizing_model: Any | None = None
    symbol_lot_rules: Any | None = None
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
    """Calculates a deterministic cryptographic hash of all decision inputs.

    Covers symbol, market revision identity and content, position scope, batches,
    universe, clock, risk parameters, full policy parameters, and complete policy state.
    """
    pos_key = decision_input.position_view.key
    batches_summary = [
        (
            b.batch_id,
            str(b.original_quantity),
            str(
                getattr(b, "remaining_quantity", None)
                or getattr(b, "quantity", None)
                or "0"
            ),
            str(b.entry_price),
            b.opened_at.isoformat(),
        )
        for b in decision_input.position_view.batches
    ]
    payload: dict[str, Any] = {
        "symbol": decision_input.symbol,
        "market_revision_id": decision_input.market_ref.revision_id,
        "market_content_hash": decision_input.market_ref.content_hash,
        "position_scope": {
            "environment": pos_key.environment,
            "account_label": pos_key.account_label,
            "position_side": (
                pos_key.position_side.value
                if hasattr(pos_key.position_side, "value")
                else str(pos_key.position_side)
            ),
        },
        "position_version": decision_input.position_view.projection_version,
        "position_quantity": str(decision_input.position_view.total_quantity),
        "position_health": (
            decision_input.position_view.health_status.value
            if hasattr(decision_input.position_view.health_status, "value")
            else str(decision_input.position_view.health_status)
        ),
        "position_batches": sorted(batches_summary),
        "universe_version": decision_input.universe_version,
        "clock_time": decision_input.clock_event.timestamp.isoformat(),
        "clock_sequence": decision_input.clock_event.sequence,
        "cash_balance": str(decision_input.cash_balance),
        "risk_config_version": decision_input.risk_config_version,
        "policy_parameters": {
            "policy_id": policy.policy_id,
            "strategy_name": policy.strategy_name,
            "policy_version": policy.policy_version,
            "entry_threshold": str(policy.entry_threshold),
            "short_entry_threshold": (
                str(policy.short_entry_threshold)
                if policy.short_entry_threshold is not None
                else None
            ),
            "order_type": (
                policy.order_type.value
                if hasattr(policy.order_type, "value")
                else str(policy.order_type)
            ),
            "target_notional": str(policy.target_notional),
            "max_open_positions": policy.max_open_positions,
            "cooldown_duration_seconds": int(policy.cooldown_duration.total_seconds()),
            "position_mode": (
                policy.position_mode.value
                if hasattr(policy.position_mode, "value")
                else str(policy.position_mode)
            ),
            "grace_period_seconds": int(policy.grace_period.total_seconds()),
        },
        "policy_state": {
            "policy_version": state.policy_version,
            "cooldown_until_by_symbol": {
                k: v.isoformat()
                for k, v in sorted(state.cooldown_until_by_symbol.items())
            },
            "anchor_prices_by_symbol": {
                k: str(v) for k, v in sorted(state.anchor_prices_by_symbol.items())
            },
            "active_intent_ids_by_symbol": dict(
                sorted(state.active_intent_ids_by_symbol.items())
            ),
            "warmup_status": dict(sorted(state.warmup_status.items())),
            "grace_until_by_symbol": {
                k: v.isoformat() for k, v in sorted(state.grace_until_by_symbol.items())
            },
            "holding_deadline_by_symbol": {
                k: v.isoformat()
                for k, v in sorted(state.holding_deadline_by_symbol.items())
            },
        },
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
        scope_to_use = (
            getattr(decision_input.market_ref, "scope", None)
            or getattr(decision_input.position_view.key, "environment", None)
            or "live"
        )
        frame = DecisionFrame(
            scope=scope_to_use,
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
    scope: str | None = None,
    source_epoch: str | None = None,
    market_ref: MarketRevisionRef | None = None,
    market_envelope: MarketEnvelope | None = None,
    frame: DecisionFrame | None = None,
) -> DecisionInput:
    """Shared DecisionInput assembly for live, paper, and research paths.

    Every runner freezes the same kind of facts; only the epoch/scope
    labels differ. Do not reassemble DecisionInput ad hoc in runners.
    """
    effective_scope = scope or getattr(state, "environment", None) or "live"
    effective_source_epoch = source_epoch or f"seq_{getattr(state, 'trade_count', 0)}"

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
        frame = DecisionFrame(
            scope=effective_scope,
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


def decision_trace_from_result(
    result: DecisionResult,
    decision_input: DecisionInput,
    strategy_name: str,
    account_label: str,
) -> DecisionTrace:
    """Builds an immutable DecisionTrace with semantic outputs and next state."""
    cd_items = result.next_policy_state.cooldown_until_by_symbol.items()
    anchor_items = result.next_policy_state.anchor_prices_by_symbol.items()
    intent_items = result.next_policy_state.active_intent_ids_by_symbol.items()
    warmup_items = result.next_policy_state.warmup_status.items()
    payload: dict[str, Any] = {
        "input_hash": result.input_hash,
        "frame_digest": result.frame_digest,
        "next_policy_state": {
            "policy_version": result.next_policy_state.policy_version,
            "cooldown_until": {k: v.isoformat() for k, v in sorted(cd_items)},
            "anchor_prices": {k: str(v) for k, v in sorted(anchor_items)},
            "active_intent_ids": dict(sorted(intent_items)),
            "warmup_status": dict(sorted(warmup_items)),
        },
    }
    if (
        hasattr(decision_input, "market_envelope")
        and decision_input.market_envelope is not None
    ):
        try:
            from crypto_momentum_lab.market_data.hub import market_state_to_payload

            payload["market_state"] = market_state_to_payload(
                decision_input.market_envelope.state
            )
        except Exception:
            pass
    if result.intent is not None:
        notional = getattr(result.intent, "desired_notional", None)
        if notional is None:
            notional = getattr(result.intent, "target_notional", None)
        lim = result.intent.limit_price
        payload["output_intent"] = {
            "candidate_id": result.intent.candidate_id,
            "symbol": result.intent.symbol,
            "desired_notional": str(notional) if notional is not None else None,
            "target_notional": str(notional) if notional is not None else None,
            "limit_price": str(lim) if lim is not None else None,
        }
    if result.exit_command is not None:
        cmd = result.exit_command
        payload["output_exit_command"] = {
            "command_id": cmd.command_id,
            "symbol": cmd.position_key.symbol,
            "quantity": str(cmd.requested_quantity),
            "side": (cmd.side.value if hasattr(cmd.side, "value") else str(cmd.side)),
        }
    return DecisionTrace(
        decision_id=result.decision_id,
        strategy_name=strategy_name,
        account_label=account_label,
        decision_time=result.evaluated_at,
        evaluated_market_refs=(decision_input.market_ref,),
        intent_produced=result.intent is not None,
        intent_id=result.intent.candidate_id if result.intent is not None else None,
        rejection_reason=result.rejection_reason,
        input_hash=result.input_hash,
        frame_digest=result.frame_digest,
        trace_payload=payload,
    )


build_decision_trace = decision_trace_from_result


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
    on_decision_result: Callable[..., None] | None = None,
    trace_recorder: Callable[[DecisionTrace], None] | None = None,
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

    def _filter(decision: StrategyDecision, state: MarketState15s) -> StrategyDecision:
        if not decision.candidates:
            if fact_provider is not None:
                frozen = fact_provider(state)
                if (
                    frozen is not None
                    and frozen.position_view.key.symbol == state.symbol
                    and frozen.position_view.total_quantity > Decimal("0")
                ):
                    scope_to_use = getattr(state, "environment", None) or "live"
                    dec_input = build_decision_input(
                        state=state,
                        frozen=frozen,
                        clock_sequence=1,
                        scope=scope_to_use,
                        source_epoch=f"ep_{scope_to_use}",
                    )
                    policy = EffectivePolicy(
                        policy_id=f"policy_{strategy_name}",
                        strategy_name=strategy_name,
                        target_notional=notional,
                        candidate_generator=lambda inp, st: None,
                    )
                    dec_res = engine.evaluate(dec_input, frozen.policy_state, policy)
                    if trace_recorder is not None:
                        try:
                            trace = build_decision_trace(
                                dec_res,
                                dec_input,
                                strategy_name=strategy_name,
                                account_label=frozen.position_view.key.account_label,
                            )
                            trace_recorder(trace)
                        except Exception:
                            pass
                    if on_decision_result is not None:
                        try:
                            on_decision_result(dec_res, dec_input)
                        except TypeError:
                            on_decision_result(dec_res)
            return decision

        if fact_provider is None:
            return _reject_all(decision, state, "missing_frozen_decision_inputs")
        frozen = fact_provider(state)
        if frozen is None:
            return _reject_all(decision, state, "frozen_decision_inputs_unavailable")
        pos_view = frozen.position_view
        if pos_view.key.symbol != state.symbol:
            return _reject_all(decision, state, "frozen_inputs_symbol_mismatch")
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

        scope_to_use = getattr(state, "environment", None) or "live"
        dec_input = build_decision_input(
            state=state,
            frozen=frozen,
            clock_sequence=1,
            scope=scope_to_use,
            source_epoch=f"ep_{scope_to_use}",
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
            dec_res = engine.evaluate(dec_input, frozen.policy_state, policy)
            if trace_recorder is not None:
                try:
                    trace = build_decision_trace(
                        dec_res,
                        dec_input,
                        strategy_name=strategy_name,
                        account_label=frozen.position_view.key.account_label,
                    )
                    trace_recorder(trace)
                except Exception:
                    pass
            if on_decision_result is not None:
                try:
                    on_decision_result(dec_res, dec_input)
                except TypeError:
                    on_decision_result(dec_res)
            if dec_res.intent is not None:
                filtered_candidates.append(cand)
            else:
                raw_reason = dec_res.rejection_reason or ("decision_engine_filtered")
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
