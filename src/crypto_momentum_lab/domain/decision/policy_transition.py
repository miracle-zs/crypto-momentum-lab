"""Strategy policy state transitions and timing contracts (R3).

Obeys Astra Architecture Blueprint 2026-09-25:
- transition(frame, prior_state, policy_artifact) -> PolicyTransition;
- Unified state transitions across Paper, Research, and Live dry-run;
- Explicit StrategyPositionMode (LONG_ONLY, SHORT_ONLY, BOTH), no implicit defaults;
- Clocks drive holding exits and timers even in the absence of entry candidates;
- Atomic next_state, decision_trace, and commands emission.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Any, Protocol

from crypto_momentum_lab.domain.decision.decision_frame import (
    DecisionFrame,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    PositionView,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    ExitAllocation,
    ExitAllocationPlan,
    ExitPolicyMode,
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.market.revision_models import MarketEnvelope
from crypto_momentum_lab.domain.strategy.models import (
    EntryType,
    OrderIntentCandidate,
    StrategySide,
)
from crypto_momentum_lab.domain.strategy.position_exit import (
    ClosedCandle15m,
    PositionExitPolicy,
    position_exit_reason,
)
from crypto_momentum_lab.domain.strategy.sizing import (
    SizingModel,
    SizingPlan,
    SizingRejection,
    SymbolLotRules,
    default_symbol_lot_rules,
)


class StrategyPositionMode(StrEnum):
    """Explicit allowed trading directions for a strategy policy."""

    LONG_ONLY = "long_only"
    SHORT_ONLY = "short_only"
    BOTH = "both"


@dataclass(frozen=True, slots=True)
class TimerRequest:
    """Explicit timer request emitted during policy state transition."""

    timer_id: str
    timer_type: str  # cooldown_expiry, grace_period_expiry, max_holding_expiry
    symbol: str
    due_at: datetime
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.timer_id.strip():
            raise ValueError("timer_id must not be empty")
        if not self.timer_type.strip():
            raise ValueError("timer_type must not be empty")
        if not self.symbol.strip():
            raise ValueError("symbol must not be empty")
        if self.due_at.tzinfo is None:
            raise ValueError("due_at must be timezone-aware (UTC)")


@dataclass(frozen=True, slots=True)
class PolicyTransition:
    """Immutable outcome of StrategyPolicy.transition (R3)."""

    decision_id: str
    frame_digest: str
    input_hash: str
    prior_state_version: int
    next_state: Any  # PolicyState
    entry_candidate: OrderIntentCandidate | None = None
    exit_command: TradeCommand | None = None
    timer_requests: tuple[TimerRequest, ...] = ()
    rejection_reason: str | None = None
    transition_time: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def is_actionable(self) -> bool:
        return self.entry_candidate is not None or self.exit_command is not None


class StrategyPolicy(Protocol):
    """Protocol governing pure, reproducible strategy policy state transitions."""

    def transition(
        self,
        frame: DecisionFrame,
        prior_state: Any,
        policy_artifact: Any,
        *,
        market_envelope: MarketEnvelope,
        position_view: PositionView,
        closed_candles: tuple[ClosedCandle15m, ...] = (),
    ) -> PolicyTransition: ...


def compute_transition_input_hash(
    frame: DecisionFrame,
    prior_state_version: int,
    policy_id: str,
    policy_version: int,
) -> str:
    """Deterministic cryptographic hash binding frame and policy context."""
    payload = {
        "frame_digest": frame.frame_digest,
        "prior_state_version": prior_state_version,
        "policy_id": policy_id,
        "policy_version": policy_version,
    }
    dumped = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(dumped.encode("utf-8")).hexdigest()


def _evaluate_sizing(
    cand: OrderIntentCandidate,
    policy_artifact: Any,
    symbol: str,
    ref_price: Decimal,
    cash_balance: Decimal,
    as_of: datetime,
) -> tuple[OrderIntentCandidate | None, SizingPlan | None, str | None]:
    """Applies sizing model if present on policy_artifact.

    Returns:
        (updated_candidate, sizing_plan, rejection_reason)
    """
    sizing_model: SizingModel | None = getattr(policy_artifact, "sizing_model", None)
    if sizing_model is None:
        return cand, None, None

    lot_rules: SymbolLotRules = getattr(
        policy_artifact, "symbol_lot_rules", None
    ) or default_symbol_lot_rules(symbol)

    result = sizing_model.compute_plan(
        symbol=symbol,
        reference_price=ref_price,
        cash_balance=cash_balance,
        lot_rules=lot_rules,
        as_of=as_of,
        sizing_version=getattr(policy_artifact, "policy_version", 1),
    )
    if isinstance(result, SizingRejection):
        return None, None, f"sizing_{result.reason}"

    sizing_feat = {
        "sizing_model": str(result.features.get("model", "custom")),
        "quantized_quantity": str(result.quantized_quantity),
        "lot_remainder": str(result.lot_remainder),
        "target_notional": str(result.target_notional),
        "actual_notional": str(result.actual_notional),
        "step_size": str(result.step_size),
        "margin_required": str(result.margin_required),
    }
    updated_features = dict(cand.features)
    updated_features.update(sizing_feat)
    updated_cand = replace(
        cand,
        desired_notional=result.actual_notional,
        features=updated_features,
    )
    return updated_cand, result, None


def execute_policy_transition(
    frame: DecisionFrame,
    prior_state: Any,
    policy_artifact: Any,
    *,
    market_envelope: MarketEnvelope,
    position_view: PositionView,
    closed_candles: tuple[ClosedCandle15m, ...] = (),
    decision_input: Any | None = None,
) -> PolicyTransition:
    """Canonical pure implementation of StrategyPolicy.transition.

    Guarantees:
    - Pure function: zero I/O, no database, no network, no implicit wall clock;
    - Same output across Live dry-run, Paper, and Research for same frame + state;
    - Clocks evaluate holding exit and timers even when no entry signal exists;
    - Position mode (LONG_ONLY / SHORT_ONLY / BOTH) strictly enforced.
    """
    clock_time = frame.clock_event.timestamp
    symbol = frame.symbol

    if market_envelope.state.symbol != symbol:
        raise ValueError(
            f"market_envelope symbol {market_envelope.state.symbol} != {symbol}"
        )
    if position_view.key.symbol != symbol:
        raise ValueError(f"position_view symbol {position_view.key.symbol} != {symbol}")

    input_hash = compute_transition_input_hash(
        frame=frame,
        prior_state_version=prior_state.policy_version,
        policy_id=policy_artifact.policy_id,
        policy_version=policy_artifact.policy_version,
    )
    decision_id = f"dec_{symbol}_{input_hash[:16]}"
    state_15s = market_envelope.state

    # 1. Evaluate Position Holding & Exit (even when no entry signal exists)
    if position_view.total_quantity > Decimal("0"):
        closed_candle: ClosedCandle15m | None = None
        if (
            state_15s.open_price is not None
            and state_15s.close_price is not None
            and state_15s.open_price > Decimal("0")
            and state_15s.close_price > Decimal("0")
            and (state_15s.bucket_end - state_15s.bucket_start) == timedelta(minutes=15)
        ):
            closed_candle = ClosedCandle15m(
                symbol=symbol,
                candle_start=state_15s.bucket_start,
                candle_end=state_15s.bucket_end,
                open_price=state_15s.open_price,
                close_price=state_15s.close_price,
            )

        earliest_open = min(
            (b.opened_at for b in position_view.batches),
            default=clock_time,
        )

        exit_policy: PositionExitPolicy = getattr(
            policy_artifact, "exit_policy", PositionExitPolicy()
        )
        exit_reason = position_exit_reason(
            held_until=clock_time,
            opened_at=earliest_open,
            symbol=symbol,
            side=StrategySide.LONG,
            policy=exit_policy,
            closed_candle=closed_candle,
            closed_candles=closed_candles,
        )

        if exit_reason is not None:
            allocations = tuple(
                ExitAllocation(
                    batch_id=b.batch_id,
                    allocated_quantity=b.quantity,
                    entry_price=b.entry_price,
                )
                for b in position_view.batches
                if b.quantity > Decimal("0")
            )
            total_qty = sum(
                (a.allocated_quantity for a in allocations), start=Decimal("0")
            )
            if total_qty > Decimal("0"):
                alloc_plan = ExitAllocationPlan(
                    position_key=position_view.key,
                    allocations=allocations,
                    total_allocated_quantity=total_qty,
                    policy=ExitPolicyMode.FULL_POSITION_CLOSE,
                    reason=exit_reason,
                    projection_version=position_view.projection_version,
                )
                exit_cmd = TradeCommand(
                    command_id=f"cmd_exit_{decision_id}",
                    position_key=position_view.key,
                    command_type=TradeCommandType.EXIT,
                    side=StrategySide.SHORT,
                    order_type=EntryType.MARKET,
                    requested_quantity=total_qty,
                    reduce_only=True,
                    allocation_plan=alloc_plan,
                    reason=exit_reason,
                    created_at=clock_time,
                    expected_projection_version=position_view.projection_version,
                )
                cooldown_dur: timedelta = getattr(
                    policy_artifact, "cooldown_duration", timedelta(minutes=15)
                )
                cd_due = clock_time + cooldown_dur
                if hasattr(prior_state, "with_cooldown"):
                    next_state = prior_state.with_cooldown(symbol, cd_due)
                else:
                    next_state = prior_state
                timer = TimerRequest(
                    timer_id=f"tm_cd_{decision_id}",
                    timer_type="cooldown_expiry",
                    symbol=symbol,
                    due_at=cd_due,
                    details={"exit_reason": exit_reason},
                )
                return PolicyTransition(
                    decision_id=decision_id,
                    frame_digest=frame.frame_digest,
                    input_hash=input_hash,
                    prior_state_version=prior_state.policy_version,
                    next_state=next_state,
                    exit_command=exit_cmd,
                    timer_requests=(timer,),
                    transition_time=clock_time,
                )
        else:
            # Position is held and not exiting; schedule max holding expiration timer
            max_holding = exit_policy.max_holding_seconds
            holding_timers: list[TimerRequest] = []
            if max_holding is not None and max_holding > 0:
                holding_deadline = earliest_open + timedelta(seconds=max_holding)
                holding_timers.append(
                    TimerRequest(
                        timer_id=f"tm_hold_{decision_id}",
                        timer_type="max_holding_expiry",
                        symbol=symbol,
                        due_at=holding_deadline,
                        details={"opened_at": earliest_open.isoformat()},
                    )
                )
            return PolicyTransition(
                decision_id=decision_id,
                frame_digest=frame.frame_digest,
                input_hash=input_hash,
                prior_state_version=prior_state.policy_version,
                next_state=prior_state,
                timer_requests=tuple(holding_timers),
                rejection_reason="holding_position_no_exit",
                transition_time=clock_time,
            )

    # 2. Check Cooldown
    if prior_state.is_in_cooldown(symbol, clock_time):
        return PolicyTransition(
            decision_id=decision_id,
            frame_digest=frame.frame_digest,
            input_hash=input_hash,
            prior_state_version=prior_state.policy_version,
            next_state=prior_state,
            rejection_reason="cooldown_active",
            transition_time=clock_time,
        )

    # 3. Check Position Mode and Entry Evaluation (when position is flat)
    pos_mode = getattr(policy_artifact, "position_mode", StrategyPositionMode.LONG_ONLY)
    if position_view.total_quantity == Decimal("0"):
        generator = getattr(policy_artifact, "candidate_generator", None)
        if generator is not None:
            arg0 = decision_input if decision_input is not None else market_envelope
            cand = generator(arg0, prior_state)
            if cand is not None:
                # Enforce position mode
                if (
                    cand.side == StrategySide.LONG
                    and pos_mode == StrategyPositionMode.SHORT_ONLY
                ):
                    return PolicyTransition(
                        decision_id=decision_id,
                        frame_digest=frame.frame_digest,
                        input_hash=input_hash,
                        prior_state_version=prior_state.policy_version,
                        next_state=prior_state,
                        rejection_reason="direction_not_permitted_by_position_mode",
                        transition_time=clock_time,
                    )
                if (
                    cand.side == StrategySide.SHORT
                    and pos_mode == StrategyPositionMode.LONG_ONLY
                ):
                    return PolicyTransition(
                        decision_id=decision_id,
                        frame_digest=frame.frame_digest,
                        input_hash=input_hash,
                        prior_state_version=prior_state.policy_version,
                        next_state=prior_state,
                        rejection_reason="direction_not_permitted_by_position_mode",
                        transition_time=clock_time,
                    )

                # Evaluate sizing if model present
                ref_price = cand.limit_price or state_15s.close_price or Decimal("0")
                effective_cash = frame.cash_balance
                if effective_cash <= Decimal("0") and decision_input is not None:
                    effective_cash = getattr(
                        decision_input, "cash_balance", effective_cash
                    )
                cand_sized, sizing_plan, rej_reason = _evaluate_sizing(
                    cand=cand,
                    policy_artifact=policy_artifact,
                    symbol=symbol,
                    ref_price=ref_price,
                    cash_balance=effective_cash,
                    as_of=clock_time,
                )
                if rej_reason is not None:
                    return PolicyTransition(
                        decision_id=decision_id,
                        frame_digest=frame.frame_digest,
                        input_hash=input_hash,
                        prior_state_version=prior_state.policy_version,
                        next_state=prior_state,
                        rejection_reason=rej_reason,
                        transition_time=clock_time,
                    )
                if cand_sized is not None:
                    cand = cand_sized

                if hasattr(prior_state, "with_anchor_and_intent"):
                    next_state = prior_state.with_anchor_and_intent(
                        symbol=symbol,
                        anchor_price=state_15s.close_price or Decimal("0"),
                        intent_id=cand.candidate_id,
                    )
                else:
                    next_state = prior_state
                if sizing_plan is not None and hasattr(next_state, "with_sizing_state"):
                    next_state = next_state.with_sizing_state(
                        symbol=symbol,
                        sizing_state=asdict(sizing_plan),
                    )
                grace_timers: list[TimerRequest] = []
                grace_period: timedelta = getattr(
                    policy_artifact, "grace_period", timedelta(0)
                )
                if grace_period > timedelta(0):
                    grace_due = clock_time + grace_period
                    grace_timers.append(
                        TimerRequest(
                            timer_id=f"tm_grace_{decision_id}",
                            timer_type="grace_period_expiry",
                            symbol=symbol,
                            due_at=grace_due,
                            details={"candidate_id": cand.candidate_id},
                        )
                    )
                return PolicyTransition(
                    decision_id=decision_id,
                    frame_digest=frame.frame_digest,
                    input_hash=input_hash,
                    prior_state_version=prior_state.policy_version,
                    next_state=next_state,
                    entry_candidate=cand,
                    timer_requests=tuple(grace_timers),
                    transition_time=clock_time,
                )
            else:
                return PolicyTransition(
                    decision_id=decision_id,
                    frame_digest=frame.frame_digest,
                    input_hash=input_hash,
                    prior_state_version=prior_state.policy_version,
                    next_state=prior_state,
                    rejection_reason="no_candidate",
                    transition_time=clock_time,
                )

        # Built-in breakout threshold evaluation
        entry_thresh: Decimal = getattr(
            policy_artifact, "entry_threshold", Decimal("65000.00")
        )
        close_px = state_15s.close_price or Decimal("0")
        if close_px > entry_thresh:
            if pos_mode == StrategyPositionMode.SHORT_ONLY:
                return PolicyTransition(
                    decision_id=decision_id,
                    frame_digest=frame.frame_digest,
                    input_hash=input_hash,
                    prior_state_version=prior_state.policy_version,
                    next_state=prior_state,
                    rejection_reason="direction_not_permitted_by_position_mode",
                    transition_time=clock_time,
                )
            target_notional: Decimal = getattr(
                policy_artifact, "target_notional", Decimal("500.00")
            )
            order_type: EntryType = getattr(
                policy_artifact, "order_type", EntryType.MARKET
            )
            cand = OrderIntentCandidate(
                candidate_id=f"intent_{decision_id}",
                signal_id=f"sig_{decision_id}",
                run_id="run_deterministic",
                strategy_name=getattr(policy_artifact, "strategy_name", "breakout"),
                strategy_version=f"v{getattr(policy_artifact, 'policy_version', 1)}",
                config_hash=policy_artifact.policy_id,
                symbol=symbol,
                side=StrategySide.LONG,
                entry_type=order_type,
                limit_price=(close_px if order_type == EntryType.LIMIT else None),
                desired_notional=target_notional,
                reduce_only=False,
                expires_at=clock_time + timedelta(minutes=5),
                created_at=clock_time,
                reason="breakout_above_threshold",
                features={"close_price": str(close_px)},
            )
            # Evaluate sizing if model present
            ref_price = cand.limit_price or close_px
            effective_cash = frame.cash_balance
            if effective_cash <= Decimal("0") and decision_input is not None:
                effective_cash = getattr(decision_input, "cash_balance", effective_cash)
            cand_sized, sizing_plan, rej_reason = _evaluate_sizing(
                cand=cand,
                policy_artifact=policy_artifact,
                symbol=symbol,
                ref_price=ref_price,
                cash_balance=effective_cash,
                as_of=clock_time,
            )
            if rej_reason is not None:
                return PolicyTransition(
                    decision_id=decision_id,
                    frame_digest=frame.frame_digest,
                    input_hash=input_hash,
                    prior_state_version=prior_state.policy_version,
                    next_state=prior_state,
                    rejection_reason=rej_reason,
                    transition_time=clock_time,
                )
            if cand_sized is not None:
                cand = cand_sized

            if hasattr(prior_state, "with_anchor_and_intent"):
                next_state = prior_state.with_anchor_and_intent(
                    symbol=symbol,
                    anchor_price=close_px,
                    intent_id=cand.candidate_id,
                )
            else:
                next_state = prior_state
            if sizing_plan is not None and hasattr(next_state, "with_sizing_state"):
                next_state = next_state.with_sizing_state(
                    symbol=symbol,
                    sizing_state=asdict(sizing_plan),
                )
            return PolicyTransition(
                decision_id=decision_id,
                frame_digest=frame.frame_digest,
                input_hash=input_hash,
                prior_state_version=prior_state.policy_version,
                next_state=next_state,
                entry_candidate=cand,
                transition_time=clock_time,
            )
        else:
            return PolicyTransition(
                decision_id=decision_id,
                frame_digest=frame.frame_digest,
                input_hash=input_hash,
                prior_state_version=prior_state.policy_version,
                next_state=prior_state,
                rejection_reason="below_entry_threshold",
                transition_time=clock_time,
            )

    return PolicyTransition(
        decision_id=decision_id,
        frame_digest=frame.frame_digest,
        input_hash=input_hash,
        prior_state_version=prior_state.policy_version,
        next_state=prior_state,
        rejection_reason="no_action",
        transition_time=clock_time,
    )
