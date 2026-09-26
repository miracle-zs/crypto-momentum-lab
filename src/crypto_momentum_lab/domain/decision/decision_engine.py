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

from crypto_momentum_lab.domain.execution.position_ledger_models import (
    PositionHealthStatus,
    PositionView,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    ExitAllocation,
    ExitAllocationPlan,
    ExitPolicyMode,
    TradeCommand,
    TradeCommandType,
)
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
    StrategySide,
)
from crypto_momentum_lab.strategy_runner.position_exit import (
    ClosedCandle15m,
    PositionExitPolicy,
    position_exit_reason,
)


def _require_aware(dt: datetime, name: str) -> datetime:
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware (UTC)")
    return dt.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class ClockEvent:
    """Explicit external clock tick driving deterministic strategy evaluation."""

    timestamp: datetime
    sequence: int
    event_type: str = "bucket_close"

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "timestamp", _require_aware(self.timestamp, "timestamp")
        )
        if self.sequence < 0:
            raise ValueError("sequence must be non-negative")


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


@dataclass(frozen=True, slots=True)
class PolicyState:
    """Versioned, mutable-free strategy state preserving cooldowns and anchors."""

    policy_version: int = 1
    cooldown_until_by_symbol: dict[str, datetime] = field(default_factory=dict)
    anchor_prices_by_symbol: dict[str, Decimal] = field(default_factory=dict)
    active_intent_ids_by_symbol: dict[str, str] = field(default_factory=dict)
    custom_state: dict[str, Any] = field(default_factory=dict)

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
        )

    def with_cleared_symbol(self, symbol: str) -> PolicyState:
        new_cd = dict(self.cooldown_until_by_symbol)
        new_cd.pop(symbol, None)
        new_anchors = dict(self.anchor_prices_by_symbol)
        new_anchors.pop(symbol, None)
        new_intents = dict(self.active_intent_ids_by_symbol)
        new_intents.pop(symbol, None)
        return PolicyState(
            policy_version=self.policy_version + 1,
            cooldown_until_by_symbol=new_cd,
            anchor_prices_by_symbol=new_anchors,
            active_intent_ids_by_symbol=new_intents,
            custom_state=dict(self.custom_state),
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
    - Returns updated PolicyState and deterministic decision ID.
    """
    input_hash = compute_decision_input_hash(decision_input, policy, state)
    decision_id = f"dec_{decision_input.symbol}_{input_hash[:16]}"
    clock_time = decision_input.clock_event.timestamp
    state_15s = decision_input.market_envelope.state
    pos_view = decision_input.position_view

    # 1. Check Exit condition if position is currently open
    if pos_view.total_quantity > Decimal("0"):
        closed_candle = None
        if state_15s.open_price is not None and state_15s.close_price is not None:
            closed_candle = ClosedCandle15m(
                symbol=decision_input.symbol,
                candle_start=state_15s.bucket_start,
                candle_end=state_15s.bucket_end,
                open_price=state_15s.open_price,
                close_price=state_15s.close_price,
            )

        earliest_open = min(
            (b.opened_at for b in pos_view.batches),
            default=clock_time,
        )

        exit_reason = position_exit_reason(
            held_until=clock_time,
            opened_at=earliest_open,
            symbol=decision_input.symbol,
            side=StrategySide.LONG,
            policy=policy.exit_policy,
            closed_candle=closed_candle,
        )

        if exit_reason is not None:
            allocations = tuple(
                ExitAllocation(
                    batch_id=b.batch_id,
                    allocated_quantity=b.quantity,
                    entry_price=b.entry_price,
                )
                for b in pos_view.batches
                if b.quantity > Decimal("0")
            )
            total_qty = sum(
                (a.allocated_quantity for a in allocations), start=Decimal("0")
            )
            if total_qty > Decimal("0"):
                alloc_plan = ExitAllocationPlan(
                    position_key=pos_view.key,
                    allocations=allocations,
                    total_allocated_quantity=total_qty,
                    policy=ExitPolicyMode.FULL_POSITION_CLOSE,
                    reason=exit_reason,
                    projection_version=pos_view.projection_version,
                )
                exit_cmd = TradeCommand(
                    command_id=f"cmd_exit_{decision_id}",
                    position_key=pos_view.key,
                    command_type=TradeCommandType.EXIT,
                    side=StrategySide.SHORT,
                    order_type=EntryType.MARKET,
                    requested_quantity=total_qty,
                    reduce_only=True,
                    allocation_plan=alloc_plan,
                    reason=exit_reason,
                    created_at=clock_time,
                    expected_projection_version=pos_view.projection_version,
                )
                next_state = state.with_cooldown(
                    decision_input.symbol, clock_time + policy.cooldown_duration
                )
                return DecisionResult(
                    decision_id=decision_id,
                    input_hash=input_hash,
                    intent=None,
                    exit_command=exit_cmd,
                    next_policy_state=next_state,
                    rejection_reason=None,
                    evaluated_at=clock_time,
                )

    # 2. Check Cooldown
    if state.is_in_cooldown(decision_input.symbol, clock_time):
        return DecisionResult(
            decision_id=decision_id,
            input_hash=input_hash,
            intent=None,
            exit_command=None,
            next_policy_state=state,
            rejection_reason="cooldown_active",
            evaluated_at=clock_time,
        )

    # 3. Check Entry Eligibility (when position is flat)
    if pos_view.total_quantity == Decimal("0"):
        if policy.candidate_generator is not None:
            cand = policy.candidate_generator(decision_input, state)
            if cand is not None:
                return DecisionResult(
                    decision_id=decision_id,
                    input_hash=input_hash,
                    intent=cand,
                    exit_command=None,
                    next_policy_state=state,
                    rejection_reason=None,
                    evaluated_at=clock_time,
                )
            else:
                return DecisionResult(
                    decision_id=decision_id,
                    input_hash=input_hash,
                    intent=None,
                    exit_command=None,
                    next_policy_state=state,
                    rejection_reason="no_candidate",
                    evaluated_at=clock_time,
                )

        close = state_15s.close_price or Decimal("0")
        if close > policy.entry_threshold:
            intent = OrderIntentCandidate(
                candidate_id=f"intent_{decision_id}",
                signal_id=f"sig_{decision_id}",
                run_id="run_deterministic",
                strategy_name=policy.strategy_name,
                strategy_version=f"v{policy.policy_version}",
                config_hash=policy.policy_id,
                symbol=decision_input.symbol,
                side=StrategySide.LONG,
                entry_type=policy.order_type,
                limit_price=close if policy.order_type == EntryType.LIMIT else None,
                desired_notional=policy.target_notional,
                reduce_only=False,
                expires_at=clock_time + timedelta(minutes=5),
                created_at=clock_time,
                reason="breakout_above_threshold",
                features={"close_price": str(close)},
            )
            return DecisionResult(
                decision_id=decision_id,
                input_hash=input_hash,
                intent=intent,
                exit_command=None,
                next_policy_state=state,
                rejection_reason=None,
                evaluated_at=clock_time,
            )
        else:
            return DecisionResult(
                decision_id=decision_id,
                input_hash=input_hash,
                intent=None,
                exit_command=None,
                next_policy_state=state,
                rejection_reason="below_entry_threshold",
                evaluated_at=clock_time,
            )

    return DecisionResult(
        decision_id=decision_id,
        input_hash=input_hash,
        intent=None,
        exit_command=None,
        next_policy_state=state,
        rejection_reason="holding_position_no_exit",
        evaluated_at=clock_time,
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
) -> DecisionInput:
    """Shared DecisionInput assembly for live, paper, and research paths.

    Every runner freezes the same kind of facts; only the epoch/scope
    labels differ. Do not reassemble DecisionInput ad hoc in runners.
    """
    market_ref = MarketRevisionRef(
        scope=scope,
        symbol=state.symbol,
        interval="15s",
        bucket_start=state.bucket_start,
        bucket_end=state.bucket_end,
        revision_id=f"rev_{state.symbol}_{int(state.bucket_start.timestamp())}",
        content_hash=compute_market_state_hash(state),
        published_at=state.bucket_end,
        source_epoch=source_epoch,
        visibility_mode=MarketVisibilityMode.DECISION_VISIBLE,
    )
    envelope = MarketEnvelope(ref=market_ref, state=state)
    return DecisionInput(
        symbol=state.symbol,
        market_ref=market_ref,
        market_envelope=envelope,
        position_view=frozen.position_view,
        universe_version=frozen.universe_version,
        clock_event=ClockEvent(timestamp=state.bucket_end, sequence=clock_sequence),
        cash_balance=frozen.cash_balance,
        risk_config_version=frozen.risk_config_version,
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
